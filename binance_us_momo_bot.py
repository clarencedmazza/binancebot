#!/usr/bin/env python3
# Ross Momentum 2.0 — Binance.US spot USDT momentum breakout bot
# - Scans all USDT pairs on Binance.US (TRADING)
# - Market filter: BTC trend (EMA9>EMA20) and price>VWAP
# - Candidate filter: min dollar volume, max spread
# - Entry: 1m breakout w/ volume spike + 5m/15m EMA alignment + not overextended vs VWAP
# - Risk: stop at flag low or ATR; size by $risk
# - Management: partial 1 @ +1R, partial 2 @ +2R, adaptive ATR trail (1.5->1.0 by +3R)
# - Safety: daily DD cooldown, 3-loser stop, kill switch, robust error handling
# - Timezone-safe UTC everywhere

import os, time, hmac, hashlib, sqlite3, math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd

print("[BOOT] starting bot v2...")

# ===== Config / Env =====
BASE = "https://api.binance.us"
MAX_KLINE_LIMIT = 1000

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "ross-momo-2.0/1.0"})

BUSA_API_KEY    = os.getenv("BUSA_API_KEY")
BUSA_API_SECRET = os.getenv("BUSA_API_SECRET")
DB_PATH         = os.getenv("DB_PATH", "momo_v2.sqlite3")

# Risk & behavior
RISK_PER_TRADE   = float(os.getenv("RISK_PER_TRADE", "0.01"))    # 1% base
RISK_BOOST_APLUS = float(os.getenv("RISK_BOOST_APLUS", "1.5"))   # 1.5x for A+ (MTF+news)
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES", "2"))
DAILY_MAX_DD     = float(os.getenv("DAILY_MAX_DD", "0.05"))      # 5%
COOLDOWN_MIN     = int(os.getenv("COOLDOWN_MIN", "5"))
LOSER_STOP_N     = int(os.getenv("LOSER_STOP_N", "3"))           # stop after 3 losers

# Scanning
SCN_TOP_N        = int(os.getenv("SCN_TOP_N", "12"))
SCN_WIN_MIN      = int(os.getenv("SCN_WIN_MIN", "30"))
MIN_DOLLAR_VOL   = float(os.getenv("MIN_DOLLAR_VOL", "50000"))   # $50k avg 1m over 30m
MAX_SPREAD_FRAC  = float(os.getenv("MAX_SPREAD_FRAC", "0.002"))  # 0.2%
HOD_TOLERANCE    = float(os.getenv("HOD_TOLERANCE", "0.01"))     # within 1% of HOD
VOL_SPIKE_K      = float(os.getenv("VOL_SPIKE_K", "1.8"))        # 1m vol > 1.8x 20SMA
MAX_VWAP_EXT     = float(os.getenv("MAX_VWAP_EXT", "0.03"))      # not >3% over VWAP
ATR_MULT_TRAIL0  = float(os.getenv("ATR_MULT_TRAIL0", "1.5"))    # trail starts 1.5 ATR
ATR_MULT_TRAIL1  = float(os.getenv("ATR_MULT_TRAIL1", "1.0"))    # by +3R reduce to 1.0 ATR

NEWS_ENABLED     = os.getenv("NEWS_ENABLED", "0") == "1"         # optional
CRYPTOPANIC_KEY  = os.getenv("CRYPTOPANIC_KEY", "")

KILL_SWITCH      = os.getenv("KILL_SWITCH", "1") == "1"          # 1=dry run
START_CAPITAL    = float(os.getenv("START_CAPITAL", "100"))
LOOP_SLEEP       = int(os.getenv("LOOP_SLEEP", "10"))

# ===== SQLite =====
SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS positions (
  symbol TEXT PRIMARY KEY,
  size REAL, entry REAL, stop REAL, take1 REAL, take2 REAL,
  opened_at TEXT, partial1 INTEGER DEFAULT 0, partial2 INTEGER DEFAULT 0, trailing INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS trades (
  id TEXT PRIMARY KEY, symbol TEXT, side TEXT,
  entry REAL, exit REAL, size REAL, pnl REAL,
  opened_at TEXT, closed_at TEXT
);
CREATE TABLE IF NOT EXISTS equity_log (ts TEXT PRIMARY KEY, equity REAL);
"""

class DB:
    def __init__(self, path: str = DB_PATH):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.executescript(SCHEMA); self.conn.commit()
    def set(self, k: str, v: Any) -> None:
        self.conn.execute("REPLACE INTO settings(k,v) VALUES(?,?)", (k, str(v))); self.conn.commit()
    def get(self, k: str, default: Optional[str] = None) -> Optional[str]:
        cur = self.conn.execute("SELECT v FROM settings WHERE k=?", (k,)); r = cur.fetchone()
        return r[0] if r else default
    def upsert_pos(self, sym: str, size: float, entry: float, stop: float, take1: float, take2: float,
                   opened_at: str, p1: bool, p2: bool, trailing: bool) -> None:
        self.conn.execute(
            """REPLACE INTO positions(symbol,size,entry,stop,take1,take2,opened_at,partial1,partial2,trailing)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (sym, size, entry, stop, take1, take2, opened_at, 1 if p1 else 0, 1 if p2 else 0, 1 if trailing else 0)
        ); self.conn.commit()
    def get_pos(self, sym: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT symbol,size,entry,stop,take1,take2,opened_at,partial1,partial2,trailing FROM positions WHERE symbol=?",
            (sym,)
        ); r = cur.fetchone()
        if not r: return None
        return {"symbol": r[0], "size": r[1], "entry": r[2], "stop": r[3], "take1": r[4], "take2": r[5],
                "opened_at": r[6], "partial1": bool(r[7]), "partial2": bool(r[8]), "trailing": bool(r[9])}
    def del_pos(self, sym: str) -> None:
        self.conn.execute("DELETE FROM positions WHERE symbol=?", (sym,)); self.conn.commit()
    def ins_trade(self, tid: str, sym: str, side: str, entry: float, exitp: float, size: float,
                  pnl: float, opened: str, closed: str) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO trades(id,symbol,side,entry,exit,size,pnl,opened_at,closed_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (tid, sym, side, entry, exitp, size, pnl, opened, closed)
        ); self.conn.commit()
    def log_equity(self, val: float) -> None:
        self.conn.execute("REPLACE INTO equity_log(ts,equity) VALUES(?,?)", (datetime.now(timezone.utc).isoformat(), val))
        self.conn.commit()

# ===== Binance.US helpers =====
def tsms() -> int: return int(time.time() * 1000)

def sign_params(params: Dict[str, str]) -> str:
    if not BUSA_API_SECRET: raise RuntimeError("Missing BUSA_API_SECRET")
    q = "&".join([f"{k}={params[k]}" for k in sorted(params.keys()) if params[k] is not None])
    sig = hmac.new(BUSA_API_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()
    return q + f"&signature={sig}"

def priv_post(path: str, params: Dict[str, str]) -> Tuple[int, Any]:
    if not BUSA_API_KEY or not BUSA_API_SECRET:
        return 400, {"error": "Missing API keys"}
    params = {**params, "timestamp": str(tsms()), "recvWindow": "5000"}
    qsig = sign_params(params)
    r = SESSION.post(BASE + path,
        headers={"X-MBX-APIKEY": BUSA_API_KEY, "Content-Type":"application/x-www-form-urlencoded"},
        data=qsig, timeout=15)
    ct = r.headers.get("Content-Type",""); 
    try: body = r.json() if ct and "application/json" in ct else r.text
    except Exception: body = r.text
    return r.status_code, body

def pub_get(url: str, params: Optional[Dict[str, str]] = None) -> Any:
    r = SESSION.get(url, params=params, timeout=15); r.raise_for_status(); return r.json()

def exchange_info() -> Dict[str, Any]:
    try: return pub_get(BASE + "/api/v3/exchangeInfo")
    except Exception as e: print(f"[WARN] exchange_info failed: {e}"); return {"symbols":[]}

def usdt_symbols(info: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for s in info.get("symbols", []):
        if s.get("status") != "TRADING": continue
        sym = s.get("symbol","")
        if not sym.endswith("USDT"): continue
        step, min_qty, min_notional = 0.000001, 0.0, 10.0
        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "LOT_SIZE":
                try:
                    step = float(f.get("stepSize", step)) or step
                    min_qty = float(f.get("minQty", min_qty)) or min_qty
                except Exception: pass
            if t in ("NOTIONAL","MIN_NOTIONAL"):
                try:
                    mn = float(f.get("minNotional", min_notional)) if f.get("minNotional") else min_notional
                    if mn > 0: min_notional = mn
                except Exception: pass
        out[sym] = {"step": step, "minQty": min_qty, "minNotional": min_notional}
    return out

def klines(symbol: str, interval: str, limit: int = 100) -> Any:
    limit = max(1, min(limit, MAX_KLINE_LIMIT))
    return pub_get(BASE + "/api/v3/klines", {"symbol":symbol,"interval":interval,"limit":limit})

def ticker24(symbol: str) -> Any: return pub_get(BASE + "/api/v3/ticker/24hr", {"symbol":symbol})
def book_ticker(symbol: str) -> Any: return pub_get(BASE + "/api/v3/ticker/bookTicker", {"symbol":symbol})

# ===== Indicators =====
def df_from_klines(rows: List[List]) -> pd.DataFrame:
    if not rows: return pd.DataFrame()
    cols=["open_time","open","high","low","close","volume","close_time","q","n","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(rows, columns=cols)
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df.set_index("ts", inplace=True)
    return df[["open","high","low","close","volume"]]

def EMA(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def SMA(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def ATR(df: pd.DataFrame, n: int = 14) -> pd.Series:
    tr = pd.concat([df["high"]-df["low"], (df["high"]-df["close"].shift()).abs(), (df["low"]-df["close"].shift()).abs()], axis=1).max(axis=1)
    return SMA(tr, n)

def last_atr(df: pd.DataFrame, n: int = 14) -> Optional[float]:
    if df is None or df.empty or len(df) < n+1: return None
    v = ATR(df,n).iloc[-1]
    if pd.isna(v): return None
    try: return float(v)
    except Exception: return None

def vwap(df: pd.DataFrame) -> pd.Series:
    # VWAP over the session (from df start)
    pv = df["close"] * df["volume"]
    cum_pv = pv.cumsum(); cum_vol = df["volume"].cumsum().replace(0, pd.NA)
    vw = (cum_pv / cum_vol).ffill()
    return vw

# ===== Momentum scan helpers =====
def rvol_recent(symbol: str) -> float:
    try:
        limit = max(min(60*24, MAX_KLINE_LIMIT), 120)  # up to 24h baseline
        k1 = df_from_klines(klines(symbol, "1m", limit=limit))
        if k1.empty or len(k1) < 60: return 0.0
        recent = k1.tail(SCN_WIN_MIN)["volume"].sum()
        arr = k1["volume"].values; win = SCN_WIN_MIN
        blocks = [arr[i-win:i].sum() for i in range(win, len(arr)-win, win)]
        base = (sum(blocks)/len(blocks)) if blocks else 1e-9
        return float(recent) / max(float(base), 1e-9)
    except Exception:
        return 0.0

def hod_proximity(symbol: str) -> Tuple[float,float,float]:
    try:
        k5 = df_from_klines(klines(symbol, "5m", limit=288))
        if k5.empty: return 1.0, 0.0, 0.0
        hod = float(k5["high"].max()); last = float(k5["close"].iloc[-1])
        diff = abs(hod - last) / hod if hod > 0 else 1.0
        return diff, last, hod
    except Exception:
        return 1.0, 0.0, 0.0

def news_boost(symbol: str) -> bool:
    if not NEWS_ENABLED or not CRYPTOPANIC_KEY: return False
    try:
        r = SESSION.get("https://cryptopanic.com/api/v1/posts/",
                        params={"auth_token": CRYPTOPANIC_KEY, "public":"true",
                                "filter":"rising,hot,important", "currencies":symbol.replace("USDT","")},
                        timeout=10)
        j = r.json(); now = datetime.now(timezone.utc)
        for p in j.get("results", []):
            ts = datetime.fromisoformat(p["published_at"].replace("Z","+00:00")).astimezone(timezone.utc)
            if (now - ts) <= timedelta(hours=2): return True
    except Exception: return False
    return False

# ===== State =====
@dataclass
class Pos:
    size: float = 0.0
    entry: float = 0.0
    stop: float = 0.0
    take1: float = 0.0
    take2: float = 0.0
    opened_at: Optional[datetime] = None
    partial1: bool = False
    partial2: bool = False
    trailing: bool = False

@dataclass
class Account:
    equity: float = START_CAPITAL
    day_start: float = START_CAPITAL
    open: Dict[str, Pos] = field(default_factory=dict)
    loser_streak: int = 0
    def daily_dd(self) -> float:
        return max(0.0, (self.day_start - self.equity) / max(self.day_start, 1e-9))

# ===== Utils =====
def round_step(qty: float, step: float) -> float:
    if step <= 0: return qty
    precision = int(round(-math.log10(step))) if step < 1 else 0
    steps = math.floor(qty / step)
    return float(f"{steps * step:.{precision}f}")

def current_R(entry: float, stop: float, price_now: float) -> float:
    risk = abs(entry - stop); 
    if risk <= 1e-9: return 0.0
    return (price_now - entry) / risk

def price(symbol: str) -> float:
    try:
        t = ticker24(symbol)
        lp = t.get("lastPrice") or t.get("last_price") or t.get("weightedAvgPrice")
        return float(lp) if lp is not None else 0.0
    except Exception:
        return 0.0

def place_market_buy(symbol: str, qty: float) -> Tuple[bool, Any]:
    if KILL_SWITCH: return True, {"dry": True}
    code, data = priv_post("/api/v3/order", {"symbol":symbol,"side":"BUY","type":"MARKET","quantity":f"{qty:.8f}"})
    return code == 200, data

def place_market_sell(symbol: str, qty: float) -> Tuple[bool, Any]:
    if KILL_SWITCH: return True, {"dry": True}
    code, data = priv_post("/api/v3/order", {"symbol":symbol,"side":"SELL","type":"MARKET","quantity":f"{qty:.8f}"})
    return code == 200, data

# ===== Filters & Strategy =====
def market_ok() -> bool:
    # BTC 5m EMA alignment + price above VWAP
    try:
        df5 = df_from_klines(klines("BTCUSDT", "5m", limit=300))
        if df5.empty or len(df5) < 50: return True  # fail-open if no data
        ema9, ema20 = EMA(df5["close"], 9), EMA(df5["close"], 20)
        cond_ema = bool(ema9.iloc[-1] > ema20.iloc[-1])
        df1 = df_from_klines(klines("BTCUSDT", "1m", limit=200))
        if df1.empty: return cond_ema
        vw = vwap(df1); cond_vwap = bool(df1["close"].iloc[-1] > vw.iloc[-1])
        return cond_ema and cond_vwap
    except Exception:
        return True

def candidate_symbols(meta: Dict[str, Dict[str, float]]) -> List[str]:
    syms = list(meta.keys()); scored: List[Tuple[float,str]] = []
    for s in syms:
        try:
            # Liquidity / spread screen
            k1 = df_from_klines(klines(s, "1m", limit=60))
            if k1.empty or len(k1) < 30: continue
            dollar_vol = float((k1["close"].tail(30) * k1["volume"].tail(30)).mean())
            if dollar_vol < MIN_DOLLAR_VOL: continue
            bt = book_ticker(s); bid = float(bt.get("bidPrice", 0) or 0); ask = float(bt.get("askPrice", 0) or 0)
            if bid <= 0 or ask <= 0: continue
            spread = (ask - bid) / max((ask + bid) / 2.0, 1e-9)
            if spread > MAX_SPREAD_FRAC: continue

            # Score: pct change, rvol, HOD proximity (+ optional news)
            t24 = ticker24(s)
            chg = float(t24.get("priceChangePercent", 0) or 0.0)
            rvol = rvol_recent(s)
            hod_d, _, _ = hod_proximity(s)
            hod_component = max(0.0, 1.0 - (hod_d / max(HOD_TOLERANCE, 1e-9)))
            boost = 1.0 if news_boost(s) else 0.0
            score = (0.25*chg/10.0) + (0.40*rvol) + (0.25*hod_component) + (0.10*boost)
            scored.append((score, s))
        except Exception:
            continue
    scored.sort(key=lambda x: x[0], reverse=True)
    return [sym for _, sym in scored[:SCN_TOP_N]]

def mtf_ok(symbol: str) -> bool:
    try:
        df5 = df_from_klines(klines(symbol, "5m", limit=200))
        df15 = df_from_klines(klines(symbol, "15m", limit=200))
        if df5.empty or df15.empty: return False
        e5a, e5b = EMA(df5["close"], 9).iloc[-1], EMA(df5["close"], 20).iloc[-1]
        e15a, e15b = EMA(df15["close"], 9).iloc[-1], EMA(df15["close"], 20).iloc[-1]
        return (e5a > e5b) and (e15a > e15b)
    except Exception:
        return False

def vol_spike_ok(df1: pd.DataFrame) -> bool:
    if df1.empty or len(df1) < 21: return False
    avg = SMA(df1["volume"], 20).iloc[-2]
    last_v = df1["volume"].iloc[-1]
    if pd.isna(avg) or pd.isna(last_v): return False
    return float(last_v) > VOL_SPIKE_K * float(max(avg, 1e-9))

def trigger_long(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        # 1m breakout + vol spike + not too extended vs VWAP + MTF alignment
        df1 = df_from_klines(klines(symbol, "1m", limit=80))
        if df1.empty or len(df1) < 30: return None
        last_close = float(df1["close"].iloc[-1])
        prev_high = float(df1["high"].iloc[-10:-1].max())
        if last_close <= prev_high: return None
        if not vol_spike_ok(df1): return None

        vw = vwap(df1); if_over = (last_close - float(vw.iloc[-1])) / max(float(vw.iloc[-1]), 1e-9)
        if if_over > MAX_VWAP_EXT: return None
        if not mtf_ok(symbol): return None

        # Stop via flag low or ATR
        atr = last_atr(df1, 14)
        if atr is None or atr <= 0: return None
        flag_low = float(df1["low"].iloc[-5:-1].min())
        stop = min(flag_low, last_close - 1.2*atr)
        if stop <= 0 or stop >= last_close: return None

        aplus = news_boost(symbol)  # tiny boost flag (optional)
        return {"entry": last_close, "stop": stop, "atr": atr, "apl": aplus}
    except Exception:
        return None

def size_from_risk(acct: Account, entry: float, stop: float, step: float, minNotional: float, aplus: bool) -> float:
    risk_frac = RISK_PER_TRADE * (RISK_BOOST_APLUS if aplus else 1.0)
    risk_usd = max(1.0, acct.equity * risk_frac)
    risk_per_unit = max(1e-9, entry - stop)
    qty = risk_usd / risk_per_unit
    qty = round_step(qty, step)
    if qty * entry < minNotional:
        qty = math.ceil(minNotional / entry / step) * step
    if qty * entry < minNotional: return 0.0
    return qty

# ===== Position management =====
def adaptive_trail_mult(R_now: float) -> float:
    # Linear from ATR_MULT_TRAIL0 at 1R down to ATR_MULT_TRAIL1 at 3R+
    if R_now <= 1.0: return ATR_MULT_TRAIL0
    if R_now >= 3.0: return ATR_MULT_TRAIL1
    f = (R_now - 1.0) / 2.0
    return ATR_MULT_TRAIL0 - f * (ATR_MULT_TRAIL0 - ATR_MULT_TRAIL1)

def manage_positions(db: DB, acct: Account, meta: Dict[str, Dict[str, float]]) -> None:
    to_close: List[str] = []
    for sym, pos in list(acct.open.items()):
        px = price(sym)
        if px <= 0: continue

        # Hard stop
        if px <= pos.stop:
            sz = pos.size; ok, resp = place_market_sell(sym, sz)
            pnl = (px - pos.entry) * sz
            acct.equity += pnl; acct.loser_streak = (acct.loser_streak + 1) if pnl < 0 else 0
            print(f"[EXIT-STOP] {sym} qty={sz} @~{px:.8f} pnl={pnl:.2f} {'DRY' if KILL_SWITCH else ''} resp={resp}")
            db.ins_trade(f"{sym}-{int(time.time())}", sym, "sell", pos.entry, px, sz, pnl,
                         pos.opened_at.isoformat() if pos.opened_at else "",
                         datetime.now(timezone.utc).isoformat())
            db.del_pos(sym); to_close.append(sym); continue

        # Partial 1 at +1R
        Rnow = current_R(pos.entry, pos.stop, px)
        if (not pos.partial1) and Rnow >= 1.0:
            sell_sz = pos.size * 0.33
            ok, resp = place_market_sell(sym, sell_sz)
            pnl = (px - pos.entry) * sell_sz
            acct.equity += pnl
            pos.size -= sell_sz; pos.partial1 = True
            pos.stop = max(pos.stop, pos.entry)  # move to breakeven
            print(f"[PARTIAL1] {sym} sold={sell_sz} @~{px:.8f} R={Rnow:.2f} +{pnl:.2f} {'DRY' if KILL_SWITCH else ''}")
            db.upsert_pos(sym, pos.size, pos.entry, pos.stop, pos.take1, pos.take2,
                          pos.opened_at.isoformat() if pos.opened_at else datetime.now(timezone.utc).isoformat(),
                          pos.partial1, pos.partial2, pos.trailing)

        # Partial 2 at +2R
        if (not pos.partial2) and Rnow >= 2.0:
            sell_sz = pos.size * 0.5
            ok, resp = place_market_sell(sym, sell_sz)
            pnl = (px - pos.entry) * sell_sz
            acct.equity += pnl
            pos.size -= sell_sz; pos.partial2 = True
            print(f"[PARTIAL2] {sym} sold={sell_sz} @~{px:.8f} R={Rnow:.2f} +{pnl:.2f} {'DRY' if KILL_SWITCH else ''}")
            db.upsert_pos(sym, pos.size, pos.entry, pos.stop, pos.take1, pos.take2,
                          pos.opened_at.isoformat() if pos.opened_at else datetime.now(timezone.utc).isoformat(),
                          pos.partial1, pos.partial2, pos.trailing)

        # Adaptive ATR trailing after +1R
        if pos.partial1:
            k1 = df_from_klines(klines(sym, "1m", limit=60))
            atr = last_atr(k1, 14)
            if atr and atr > 0:
                mult = adaptive_trail_mult(Rnow)
                new_stop = max(pos.stop, px - mult * atr)
                if new_stop > pos.stop:
                    pos.stop = new_stop; pos.trailing = True
                    db.upsert_pos(sym, pos.size, pos.entry, pos.stop, pos.take1, pos.take2,
                                  pos.opened_at.isoformat() if pos.opened_at else datetime.now(timezone.utc).isoformat(),
                                  pos.partial1, pos.partial2, pos.trailing)

    for s in to_close: acct.open.pop(s, None)

# ===== Main loop =====
def run_scan_and_trade():
    db = DB()
    equity = float(db.get("equity", START_CAPITAL))
    day_start = float(db.get("day_start", START_CAPITAL))
    acct = Account(equity=equity, day_start=day_start)

    info = exchange_info(); meta = usdt_symbols(info)
    print(f"[INIT] v2 symbols={len(meta)}  kill={KILL_SWITCH}  risk={RISK_PER_TRADE:.2%} minNotional~10")

    cooldown_until: Optional[datetime] = None
    last_day = datetime.now(timezone.utc).date()

    while True:
        try:
            # Day rollover
            now_day = datetime.now(timezone.utc).date()
            if now_day != last_day:
                acct.day_start = acct.equity; db.set("day_start", acct.day_start)
                acct.loser_streak = 0
                last_day = now_day
                print(f"[ROLLOVER] New UTC day. day_start={acct.day_start:.2f}")

            db.log_equity(acct.equity)

            # Risk guards
            if acct.daily_dd() >= DAILY_MAX_DD or acct.loser_streak >= LOSER_STOP_N:
                msg = "[RISK] Daily DD hit" if acct.daily_dd() >= DAILY_MAX_DD else "[RISK] Loser streak cap"
                if not cooldown_until or datetime.now(timezone.utc) >= cooldown_until:
                    cooldown_until = datetime.now(timezone.utc) + timedelta(minutes=COOLDOWN_MIN)
                    print(f"{msg}. Cooling until {cooldown_until.isoformat()}")
                time.sleep(1); continue

            if cooldown_until and datetime.now(timezone.utc) < cooldown_until:
                time.sleep(1); continue
            else:
                cooldown_until = None

            # Market regime filter
            if not market_ok():
                print("[MARKET] Conditions not favorable (BTC trend/VWAP). Waiting…")
                time.sleep(LOOP_SLEEP); continue

            # Scan candidates
            picks = candidate_symbols(meta)
            print(f"[SCAN] top={', '.join(picks)}")

            # Try entries
            for sym in picks:
                if len(acct.open) >= MAX_OPEN_TRADES: break
                if sym in acct.open: continue
                trig = trigger_long(sym)
                if not trig: continue

                entry, stop, atr = trig["entry"], trig["stop"], trig["atr"]
                aplus = bool(trig.get("apl", False))
                s_meta = meta.get(sym, {"step":0.000001, "minNotional":10.0})
                qty = size_from_risk(acct, entry, stop, s_meta["step"], s_meta["minNotional"], aplus)
                if qty <= 0: continue

                ok, resp = place_market_buy(sym, qty)
                print(f"[BUY] {sym} qty={qty} @~{entry:.8f} stop={stop:.8f} {'APLUS ' if aplus else ''}{'DRY' if KILL_SWITCH else ''} resp={resp}")

                # Precompute +1R / +2R reference
                R = entry - stop
                take1, take2 = entry + 1.0*R, entry + 2.0*R
                pos = Pos(size=qty, entry=entry, stop=stop, take1=take1, take2=take2, opened_at=datetime.now(timezone.utc))
                acct.open[sym] = pos
                db.upsert_pos(sym, pos.size, pos.entry, pos.stop, pos.take1, pos.take2, pos.opened_at.isoformat(), False, False, False)

            # Manage positions
            manage_positions(db, acct, meta)
            db.set("equity", acct.equity)

            time.sleep(LOOP_SLEEP)

        except requests.HTTPError as e:
            print(f"[HTTP] {e}"); time.sleep(3)
        except Exception as e:
            import traceback
            print(f"[LOOP-ERR] {type(e).__name__}: {e}"); traceback.print_exc(); time.sleep(3)

# ===== Entrypoint =====
if __name__ == "__main__":
    try:
        run_scan_and_trade()
    except KeyboardInterrupt:
        print("\n[EXIT] KeyboardInterrupt")
    except Exception as e:
        import traceback
        print(f"[FATAL] {type(e).__name__}: {e}"); traceback.print_exc()


