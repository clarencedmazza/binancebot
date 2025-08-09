#!/usr/bin/env python3
# Binance.US Momentum Bot (Ross-style) — multi-coin scanner (USDT), partial @ +1R, ATR trail, SQLite
import os, time, hmac, hashlib, json, sqlite3, argparse, math, threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd

BASE = "https://api.binance.us"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent":"momo-bus/1.0"})

BUSA_API_KEY   = os.getenv("BUSA_API_KEY")
BUSA_API_SECRET= os.getenv("BUSA_API_SECRET")
DB_PATH        = os.getenv("DB_PATH","momo.sqlite3")

RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE","0.01"))
KILL_SWITCH    = os.getenv("KILL_SWITCH","1") == "1"

NEWS_ENABLED   = os.getenv("NEWS_ENABLED","0") == "1"
CRYPTOPANIC_KEY= os.getenv("CRYPTOPANIC_KEY","")

# Momentum scan params
SCN_TOP_N      = int(os.getenv("SCN_TOP_N","8"))       # rank candidates top-N before triggers
SCN_WIN_MIN    = int(os.getenv("SCN_WIN_MIN","30"))    # recent window minutes
RVOL_BASE_HRS  = int(os.getenv("RVOL_BASE_HRS","24"))  # baseline window hours
HOD_TOLERANCE  = float(os.getenv("HOD_TOLERANCE","0.006")) # within 0.6% of HOD
VOL_SPIKE_K    = float(os.getenv("VOL_SPIKE_K","1.5"))     # 1m volume spike vs 20 SMA
ATR_MULT_TRAIL = float(os.getenv("ATR_MULT_TRAIL","1.5"))  # trailing ATR(1m) multiplier
PARTIAL_AT_R   = float(os.getenv("PARTIAL_AT_R","1.0"))    # partial @ +1R
PARTIAL_RATIO  = float(os.getenv("PARTIAL_RATIO","0.5"))   # 50%
MAX_OPEN_TRADES= int(os.getenv("MAX_OPEN_TRADES","2"))     # concurrency cap
DAILY_MAX_DD   = float(os.getenv("DAILY_MAX_DD","0.05"))   # halt after -5% equity on the day
COOLDOWN_MIN   = int(os.getenv("COOLDOWN_MIN","5"))        # after stop-out

# ---------- SQLite ----------
SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS positions (
  symbol TEXT PRIMARY KEY,
  size REAL, entry REAL, stop REAL, take REAL,
  opened_at TEXT, partial_taken INTEGER DEFAULT 0, trailing INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS trades (
  id TEXT PRIMARY KEY, symbol TEXT, side TEXT,
  entry REAL, exit REAL, size REAL, pnl REAL,
  opened_at TEXT, closed_at TEXT
);
CREATE TABLE IF NOT EXISTS equity_log (ts TEXT PRIMARY KEY, equity REAL);
"""
class DB:
    def __init__(self, path=DB_PATH):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.executescript(SCHEMA); self.conn.commit()
    def set(self,k,v): self.conn.execute("REPLACE INTO settings(k,v) VALUES(?,?)",(k,str(v))); self.conn.commit()
    def get(self,k,default=None): cur=self.conn.execute("SELECT v FROM settings WHERE k=?",(k,)); r=cur.fetchone(); return r[0] if r else default
    def upsert_pos(self, sym, size, entry, stop, take, opened_at, partial, trailing):
        self.conn.execute("""REPLACE INTO positions(symbol,size,entry,stop,take,opened_at,partial_taken,trailing)
                             VALUES(?,?,?,?,?,?,?,?)""",(sym,size,entry,stop,take,opened_at,1 if partial else 0,1 if trailing else 0)); self.conn.commit()
    def get_pos(self, sym):
        cur=self.conn.execute("SELECT symbol,size,entry,stop,take,opened_at,partial_taken,trailing FROM positions WHERE symbol=?", (sym,))
        r=cur.fetchone()
        if not r: return None
        return {"symbol":r[0],"size":r[1],"entry":r[2],"stop":r[3],"take":r[4],
                "opened_at":r[5],"partial_taken":bool(r[6]),"trailing":bool(r[7])}
    def del_pos(self,sym): self.conn.execute("DELETE FROM positions WHERE symbol=?",(sym,)); self.conn.commit()
    def ins_trade(self, tid, sym, side, entry, exitp, size, pnl, opened, closed):
        self.conn.execute("""INSERT OR REPLACE INTO trades(id,symbol,side,entry,exit,size,pnl,opened_at,closed_at)
                             VALUES(?,?,?,?,?,?,?,?,?)""",(tid,sym,side,entry,exitp,size,pnl,opened,closed)); self.conn.commit()
    def log_equity(self, val): self.conn.execute("REPLACE INTO equity_log(ts,equity) VALUES(?,?)",(datetime.utcnow().isoformat(), val)); self.conn.commit()

# ---------- Binance.US helpers ----------
def tsms(): return int(time.time()*1000)

def sign_params(params: Dict[str,str]) -> str:
    q = "&".join([f"{k}={params[k]}" for k in sorted(params.keys()) if params[k] is not None])
    sig = hmac.new(BUSA_API_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()
    return q + f"&signature={sig}"

def priv_get(path: str, params: Dict[str,str]) -> Tuple[int,Any]:
    params = {**params, "timestamp": str(tsms()), "recvWindow":"5000"}
    qsig = sign_params(params)
    r = SESSION.get(BASE+path+"?"+qsig, headers={"X-MBX-APIKEY":BUSA_API_KEY}, timeout=15)
    return r.status_code, (r.json() if "application/json" in r.headers.get("Content-Type","") else r.text)

def priv_post(path: str, params: Dict[str,str]) -> Tuple[int,Any]:
    params = {**params, "timestamp": str(tsms()), "recvWindow":"5000"}
    qsig = sign_params(params)
    r = SESSION.post(BASE+path, headers={"X-MBX-APIKEY":BUSA_API_KEY, "Content-Type":"application/x-www-form-urlencoded"}, data=qsig, timeout=15)
    return r.status_code, (r.json() if "application/json" in r.headers.get("Content-Type","") else r.text)

def pub_get(url: str, params: Dict[str,str]=None) -> Any:
    r = SESSION.get(url, params=params, timeout=15)
    return r.json()

# ---------- Market meta ----------
def exchange_info() -> Dict[str,Any]:
    return pub_get(BASE+"/api/v3/exchangeInfo")

def usdt_symbols(info: Dict[str,Any]) -> Dict[str, Dict[str,float]]:
    out={}
    for s in info.get("symbols",[]):
        if s.get("status")!="TRADING": continue
        if not s.get("symbol","").endswith("USDT"): continue
        # filters for lot/min notional
        lot=None; notional=None; minQty=0.0; step=0.0
        for f in s.get("filters",[]):
            if f["filterType"]=="LOT_SIZE":
                step=float(f["stepSize"]); minQty=float(f["minQty"])
            if f["filterType"]=="NOTIONAL":
                notional=float(f["minNotional"])
        out[s["symbol"]]={"step":step or 0.000001, "minQty":minQty or 0.0, "minNotional":notional or 10.0}
    return out

def klines(symbol:str, interval:str, limit:int=100):
    return pub_get(BASE+"/api/v3/klines", {"symbol":symbol, "interval":interval, "limit":limit})

def ticker24(symbol:str):
    return pub_get(BASE+"/api/v3/ticker/24hr", {"symbol":symbol})

# ---------- Indicators ----------
def df_from_klines(rows: List[List]) -> pd.DataFrame:
    if not rows: return pd.DataFrame()
    cols=["open_time","open","high","low","close","volume","close_time","q","n","taker_base","taker_quote","ignore"]
    df=pd.DataFrame(rows, columns=cols)
    for c in ["open","high","low","close","volume"]:
        df[c]=pd.to_numeric(df[c], errors="coerce")
    df["ts"]=pd.to_datetime(df["close_time"], unit="ms")
    df.set_index("ts", inplace=True)
    return df[["open","high","low","close","volume"]]

def SMA(s: pd.Series, n:int): return s.rolling(n).mean()
def ATR(df: pd.DataFrame, n:int=14):
    tr = pd.concat([
        df["high"]-df["low"],
        (df["high"]-df["close"].shift()).abs(),
        (df["low"]-df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    return SMA(tr, n)

# ---------- Momentum scan ----------
def rvol_recent(symbol:str) -> Tuple[float,float,float]:
    # RVOL = sum vol last 30m / avg(sum vol 30m across last 24h windows)
    k1 = df_from_klines(klines(symbol, "1m", limit= max(60*RVOL_BASE_HRS, 120)))
    if k1.empty or len(k1)<60: return 0.0, 0.0, 0.0
    recent = k1.tail(SCN_WIN_MIN)["volume"].sum()
    # baseline: average 30m volume across past 24h slices
    blocks=[]
    arr=k1["volume"].values
    win=SCN_WIN_MIN
    for i in range(win, len(arr)-win, win):
        blocks.append(arr[i-win:i].sum())
    base = (sum(blocks)/len(blocks)) if blocks else 1e-9
    return (recent/max(base,1e-9), recent, base)

def hod_proximity(symbol:str) -> Tuple[float,float,float]:
    k5 = df_from_klines(klines(symbol,"5m", limit=288))
    if k5.empty: return 1.0, 0.0, 0.0
    hod = k5["high"].max()
    last = k5["close"].iloc[-1]
    return (abs(hod-last)/hod, last, hod)

def vol_spike(symbol:str) -> bool:
    k1 = df_from_klines(klines(symbol,"1m",limit=60))
    if k1.empty or len(k1)<21: return False
    avg = SMA(k1["volume"],20).iloc[-2]
    return (k1["volume"].iloc[-1] > VOL_SPIKE_K * max(avg,1e-9))

def news_boost(symbol:str) -> bool:
    if not NEWS_ENABLED or not CRYPTOPANIC_KEY: return False
    try:
        r = SESSION.get("https://cryptopanic.com/api/v1/posts/",
                        params={"auth_token":CRYPTOPANIC_KEY,"public":"true","filter":"rising,hot,important","currencies":symbol.replace("USDT","")},
                        timeout=10)
        j=r.json()
        now=datetime.utcnow()
        for p in j.get("results",[]):
            ts = datetime.fromisoformat(p["published_at"].replace("Z","+00:00")).replace(tzinfo=None)
            if (now-ts) <= timedelta(hours=2):
                return True
    except Exception:
        return False
    return False

@dataclass
class Pos:
    size: float=0.0
    entry: float=0.0
    stop: float=0.0
    take: float=0.0
    opened_at: Optional[datetime]=None
    partial_taken: bool=False
    trailing: bool=False

@dataclass
class Account:
    equity: float= float(os.getenv("START_CAPITAL","100"))
    day_start: float= float(os.getenv("START_CAPITAL","100"))
    last_stop: Optional[datetime]=None
    open: Dict[str,Pos]=field(default_factory=dict)
    def daily_dd(self): return max(0.0, (self.day_start - self.equity)/max(self.day_start,1e-9))

def round_step(qty: float, step: float) -> float:
    if step<=0: return qty
    precision = int(round(-math.log10(step))) if step<1 else 0
    return max(step, float(f"{math.floor(qty/step)*step:.{precision}f}"))

def current_R(entry: float, stop: float, price: float) -> float:
    risk = abs(entry-stop)
    if risk<=1e-9: return 0.0
    move = price-entry
    return move/risk

# ---------- Execution ----------
def price(symbol:str) -> float:
    t = ticker24(symbol)
    try: return float(t["lastPrice"])
    except: return 0.0

def place_market_buy(symbol:str, qty: float) -> Tuple[bool,Any]:
    if KILL_SWITCH: return True, {"dry":True}
    code, data = priv_post("/api/v3/order", {
        "symbol":symbol, "side":"BUY", "type":"MARKET", "quoteOrderQty": None, "quantity": f"{qty:.8f}"
    })
    return code==200, data

def place_market_sell(symbol:str, qty: float) -> Tuple[bool,Any]:
    if KILL_SWITCH: return True, {"dry":True}
    code, data = priv_post("/api/v3/order", {
        "symbol":symbol, "side":"SELL", "type":"MARKET", "quantity": f"{qty:.8f}"
    })
    return code==200, data

# ---------- Strategy ----------
def candidate_symbols(meta) -> List[str]:
    # Pull all USDT pairs, rank by % change 24h + recent spike + RVOL/HOD.
    syms=list(meta.keys())
    rows=[]
    for s in syms:
        t = ticker24(s)
        try:
            chg=float(t.get("priceChangePercent","0"))
            vol=float(t.get("volume","0"))
        except: 
            chg,vol=0.0,0.0
        rvol,_,_= rvol_recent(s)
        hod_d, last, hod = hod_proximity(s)
        news = news_boost(s)
        score = (chg/10.0) + (max(0,1.0 - hod_d/HOD_TOLERANCE)) + (rvol) + (1.0 if news else 0.0)
        rows.append((score, s, rvol, hod_d, last, hod, chg, vol, news))
    rows.sort(reverse=True)
    return [r[1] for r in rows[:SCN_TOP_N]]

def trigger_long(symbol:str) -> Optional[Dict[str,Any]]:
    # Require HOD proximity, 1m volume spike, 1m close breakout above last 10 highs
    k1 = df_from_klines(klines(symbol,"1m",limit=60))
    if k1.empty or len(k1)<20: return None
    last_close = k1["close"].iloc[-1]
    prev_high = k1["high"].iloc[-10:-1].max()
    hod_d, _, _ = hod_proximity(symbol)
    if hod_d > HOD_TOLERANCE: return None
    if not vol_spike(symbol): return None
    if last_close <= prev_high: return None
    atr = ATR(k1,14).iloc[-1]
    flag_low = k1["low"].iloc[-5:-1].min()
    return {"entry":last_close, "stop": min(flag_low, last_close - 1.2*atr), "atr": float(atr)}

def size_from_risk(acct: Account, sym: str, entry: float, stop: float, step: float, minNotional: float) -> float:
    risk_usd = max(1.0, acct.equity * RISK_PER_TRADE)
    risk_per_unit = max(1e-9, entry - stop)
    qty = risk_usd / risk_per_unit
    qty = round_step(qty, step)
    # obey min notional
    if qty*entry < max(minNotional, 10.0):
        qty = round_step( (max(minNotional,10.0))/entry, step)
    return qty

def open_long(db: DB, acct: Account, meta, symbol:str, trig: Dict[str,Any]):
    if len([1 for p in acct.open.values() if p.size>0]) >= MAX_OPEN_TRADES: return
    if acct.daily_dd() >= DAILY_MAX_DD: return
    entry = price(symbol)
    stop = trig["stop"]
    if stop>=entry: return
    qty = size_from_risk(acct, symbol, entry, stop, meta[symbol]["step"], meta[symbol]["minNotional"])
    if qty<=0: return
    ok, data = place_market_buy(symbol, qty)
    if not ok: return
    pos=Pos(size=qty, entry=entry, stop=stop, take=entry + (entry-stop)*2.0, opened_at=datetime.utcnow())
    acct.open[symbol]=pos; db.upsert_pos(symbol, pos.size, pos.entry, pos.stop, pos.take, pos.opened_at.isoformat(), False, False)
    print(f"[OPEN] {symbol} qty={qty} entry={entry:.4f} SL={stop:.4f} TP={pos.take:.4f}")

def close_any(db: DB, acct: Account, symbol:str, reason:str):
    pos=acct.open.get(symbol); if not pos: return
    px=price(symbol)
    qty=pos.size
    if qty>0:
        ok,_=place_market_sell(symbol, qty)
        if not ok: return
    pnl=(px - pos.entry)*qty
    acct.equity += pnl
    db.ins_trade(f"{symbol}-{int(time.time()*1000)}", symbol, "long", pos.entry, px, qty, pnl, pos.opened_at.isoformat(), datetime.utcnow().isoformat())
    db.log_equity(acct.equity)
    db.del_pos(symbol); acct.open.pop(symbol,None)
    print(f"[CLOSE] {symbol} {reason} exit={px:.4f} pnl={pnl:.2f} equity={acct.equity:.2f}")

def close_partial(db: DB, acct: Account, symbol:str, frac: float):
    pos=acct.open.get(symbol); if not pos or pos.size<=0: return False
    part = max(0.0, round_step(pos.size*frac, 0.000001))
    if part<=0: return False
    px=price(symbol)
    ok,_=place_market_sell(symbol, part)
    if not ok: return False
    pnl=(px - pos.entry)*part; acct.equity += pnl; db.log_equity(acct.equity)
    pos.size = round_step(pos.size - part, 0.000001)
    db.upsert_pos(symbol, pos.size, pos.entry, pos.stop, pos.take, pos.opened_at.isoformat(), True, pos.trailing)
    print(f"[PARTIAL] {symbol} qty={part} px={px:.4f} pnl={pnl:.2f} remain={pos.size}")
    return True

def manage_open(db: DB, acct: Account, symbol:str):
    pos=acct.open.get(symbol); if not pos: return
    px=price(symbol)
    # partial @ +1R
    if (not pos.partial_taken) and current_R(pos.entry, pos.stop, px) >= PARTIAL_AT_R:
        if close_partial(db, acct, symbol, PARTIAL_RATIO):
            pos.partial_taken=True; pos.stop = pos.entry; pos.trailing=True
            db.upsert_pos(symbol, pos.size, pos.entry, pos.stop, pos.take, pos.opened_at.isoformat(), True, True)
    # trailing
    if pos.trailing and pos.size>0:
        k1 = df_from_klines(klines(symbol,"1m",limit=60))
        if not k1.empty:
            atr = ATR(k1,14).iloc[-1]
            trail = px - ATR_MULT_TRAIL*float(atr)
            if trail > pos.stop: pos.stop = trail; db.upsert_pos(symbol, pos.size, pos.entry, pos.stop, pos.take, pos.opened_at.isoformat(), pos.partial_taken, True)
    # exits
    if px <= pos.stop: return close_any(db, acct, symbol, "stop")
    if (not pos.trailing) and px >= pos.take: return close_any(db, acct, symbol, "target")

# ---------- Main loop ----------
def run_scan_and_trade():
    db=DB()
    acct=Account(equity=float(db.get("equity", db.get("start_capital","100"))), day_start=float(db.get("day_start", db.get("start_capital","100"))))
    info=exchange_info(); meta=usdt_symbols(info)
    day = datetime.utcnow().date()
    print("[BOOT]", len(meta), "USDT pairs")
    while True:
        try:
            if datetime.utcnow().date()!=day:
                day=datetime.utcnow().date(); acct.day_start=acct.equity; db.set("day_start", acct.day_start); print("[DAY] reset")
            # manage open first
            for sym in list(acct.open.keys()):
                manage_open(db, acct, sym)
            # scan for new entries
            cands = candidate_symbols(meta)
            for sym in cands:
                if sym in acct.open: continue
                t = trigger_long(sym)
                if t: open_long(db, acct, meta, sym, t)
            db.set("equity", acct.equity)
            time.sleep(15)  # fast scan cadence for momentum
        except Exception as e:
            print("[LOOP]", e); time.sleep(5)

def run_status():
    db=DB()
    c=db.conn.execute("SELECT symbol,size,entry,stop,take,opened_at,partial_taken,trailing FROM positions")
    rows=c.fetchall()
    if not rows: print("No open positions.")
    for r in rows: print({"symbol":r[0],"size":r[1],"entry":r[2],"stop":r[3],"take":r[4],"opened_at":r[5],"partial":bool(r[6]),"trailing":bool(r[7])})
    cur=db.conn.execute("SELECT ts,equity FROM equity_log ORDER BY ts DESC LIMIT 5")
    for r in cur.fetchall(): print("equity", r)

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--status", action="store_true")
    args=ap.parse_args()
    if args.status: run_status()
    else: run_scan_and_trade()
