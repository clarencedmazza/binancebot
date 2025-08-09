#!/usr/bin/env python3
# Binance.US Momentum Bot (Ross-style) — multi-coin USDT scanner, partial @ +1R, ATR trail, SQLite
# Fixed version: entrypoint + loop, kline limit guard, ATR/NaN guards, safer qty calc, missing-keys handling, robust logging.

import os
import time
import hmac
import hashlib
import sqlite3
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd

print("[BOOT] starting bot...")

# ===== Config / Env =====
BASE = "https://api.binance.us"
MAX_KLINE_LIMIT = 1000

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "momo-bus/1.2"})

BUSA_API_KEY = os.getenv("BUSA_API_KEY")
BUSA_API_SECRET = os.getenv("BUSA_API_SECRET")
DB_PATH = os.getenv("DB_PATH", "momo.sqlite3")

RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.01"))           # 1% default
KILL_SWITCH = os.getenv("KILL_SWITCH", "1") == "1"                    # "1" => DRY-RUN only
NEWS_ENABLED = os.getenv("NEWS_ENABLED", "0") == "1"
CRYPTOPANIC_KEY = os.getenv("CRYPTOPANIC_KEY", "")

SCN_TOP_N = int(os.getenv("SCN_TOP_N", "8"))                          # number of top symbols to consider
SCN_WIN_MIN = int(os.getenv("SCN_WIN_MIN", "30"))                     # momentum window (minutes)
RVOL_BASE_HRS = int(os.getenv("RVOL_BASE_HRS", "24"))                 # rvol baseline (hours)
HOD_TOLERANCE = float(os.getenv("HOD_TOLERANCE", "0.006"))            # within 0.6% of HOD
VOL_SPIKE_K = float(os.getenv("VOL_SPIKE_K", "1.5"))                  # 1m vol spike vs 20 SMA
ATR_MULT_TRAIL = float(os.getenv("ATR_MULT_TRAIL", "1.5"))            # trail multiplier
PARTIAL_AT_R = float(os.getenv("PARTIAL_AT_R", "1.0"))                # partial threshold in R
PARTIAL_RATIO = float(os.getenv("PARTIAL_RATIO", "0.5"))              # take 50% at +1R
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "2"))
DAILY_MAX_DD = float(os.getenv("DAILY_MAX_DD", "0.05"))               # 5% max daily drawdown
COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN", "5"))
START_CAPITAL = float(os.getenv("START_CAPITAL", "100"))
LOOP_SLEEP = int(os.getenv("LOOP_SLEEP", "10"))                       # seconds between scans

# ===== SQLite =====
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
    def __init__(self, path: str = DB_PATH):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def set(self, k: str, v: Any) -> None:
        self.conn.execute("REPLACE INTO settings(k,v) VALUES(?,?)", (k, str(v)))
        self.conn.commit()

    def get(self, k: str, default: Optional[str] = None) -> Optional[str]:
        cur = self.conn.execute("SELECT v FROM settings WHERE k=?", (k,))
        r = cur.fetchone()
        return r[0] if r else default

    def upsert_pos(self, sym: str, size: float, entry: float, stop: float, take: float,
                   opened_at: str, partial: bool, trailing: bool) -> None:
        self.conn.execute(
            """REPLACE INTO positions(symbol,size,entry,stop,take,opened_at,partial_taken,trailing)
               VALUES(?,?,?,?,?,?,?,?)""",
            (sym, size, entry, stop, take, opened_at, 1 if partial else 0, 1 if trailing else 0),
        )
        self.conn.commit()

    def get_pos(self, sym: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT symbol,size,entry,stop,take,opened_at,partial_taken,trailing FROM positions WHERE symbol=?",
            (sym,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {
            "symbol": r[0],
            "size": r[1],
            "entry": r[2],
            "stop": r[3],
            "take": r[4],
            "opened_at": r[5],
            "partial_taken": bool(r[6]),
            "trailing": bool(r[7]),
        }

    def del_pos(self, sym: str) -> None:
        self.conn.execute("DELETE FROM positions WHERE symbol=?", (sym,))
        self.conn.commit()

    def ins_trade(self, tid: str, sym: str, side: str, entry: float, exitp: float, size: float,
                  pnl: float, opened: str, closed: str) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO trades(id,symbol,side,entry,exit,size,pnl,opened_at,closed_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (tid, sym, side, entry, exitp, size, pnl, opened, closed),
        )
        self.conn.commit()

    def log_equity(self, val: float) -> None:
        self.conn.execute(
            "REPLACE INTO equity_log(ts,equity) VALUES(?,?)",
            (datetime.utcnow().isoformat(), val),
        )
        self.conn.commit()

# ===== Binance.US helpers =====
def tsms() -> int:
    return int(time.time() * 1000)

def sign_params(params: Dict[str, str]) -> str:
    if not BUSA_API_SECRET:
        raise RuntimeError("Missing BUSA_API_SECRET")
    q = "&".join([f"{k}={params[k]}" for k in sorted(params.keys()) if params[k] is not None])
    sig = hmac.new(BUSA_API_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()
    return q + f"&signature={sig}"

def priv_post(path: str, params: Dict[str, str]) -> Tuple[int, Any]:
    if not BUSA_API_KEY or not BUSA_API_SECRET:
        return 400, {"error": "Missing API keys"}
    params = {**params, "timestamp": str(tsms()), "recvWindow": "5000"}
    qsig = sign_params(params)
    r = SESSION.post(
        BASE + path,
        headers={"X-MBX-APIKEY": BUSA_API_KEY, "Content-Type": "application/x-www-form-urlencoded"},
        data=qsig,
        timeout=15,
    )
    ct = r.headers.get("Content-Type", "")
    try:
        body = r.json() if ct and "application/json" in ct else r.text
    except Exception:
        body = r.text
    return r.status_code, body

def pub_get(url: str, params: Optional[Dict[str, str]] = None) -> Any:
    r = SESSION.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

# ===== Market meta =====
def exchange_info() -> Dict[str, Any]:
    try:
        return pub_get(BASE + "/api/v3/exchangeInfo")
    except Exception as e:
        print(f"[WARN] exchange_info failed: {e}")
        return {"symbols": []}

def usdt_symbols(info: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for s in info.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        sym = s.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        step = 0.000001
        min_qty = 0.0
        min_notional = 10.0
        for f in s.get("filters", []):
            ftype = f.get("filterType")
            if ftype == "LOT_SIZE":
                try:
                    step = float(f.get("stepSize", step)) or step
                    min_qty = float(f.get("minQty", min_qty)) or min_qty
                except Exception:
                    pass
            if ftype in ("NOTIONAL", "MIN_NOTIONAL"):
                try:
                    mn = float(f.get("minNotional", min_notional)) if f.get("minNotional") else min_notional
                    if mn > 0:
                        min_notional = mn
                except Exception:
                    pass
        out[sym] = {"step": step, "minQty": min_qty, "minNotional": min_notional}
    return out

def klines(symbol: str, interval: str, limit: int = 100) -> Any:
    limit = max(1, min(limit, MAX_KLINE_LIMIT))
    return pub_get(BASE + "/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})

def ticker24(symbol: str) -> Any:
    return pub_get(BASE + "/api/v3/ticker/24hr", {"symbol": symbol})

# ===== Indicators =====
def df_from_klines(rows: List[List]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "q", "n", "taker_base", "taker_quote", "ignore"
    ]
    df = pd.DataFrame(rows, columns=cols)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts"] = pd.to_datetime(df["close_time"], unit="ms")
    df.set_index("ts", inplace=True)
    return df[["open", "high", "low", "close", "volume"]]

def SMA(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def ATR(df: pd.DataFrame, n: int = 14) -> pd.Series:
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return SMA(tr, n)

def safe_last_atr(df: pd.DataFrame, n: int = 14) -> Optional[float]:
    if df is None or df.empty or len(df) < n + 1:
        return None
    a = ATR(df, n).iloc[-1]
    if pd.isna(a):
        return None
    try:
        return float(a)
    except Exception:
        return None

# ===== Momentum scan =====
def rvol_recent(symbol: str) -> Tuple[float, float, float]:
    try:
        limit = max(min(60 * RVOL_BASE_HRS, MAX_KLINE_LIMIT), 120)
        k1 = df_from_klines(klines(symbol, "1m", limit=limit))
        if k1.empty or len(k1) < 60:
            return 0.0, 0.0, 0.0
        recent = k1.tail(SCN_WIN_MIN)["volume"].sum()
        arr = k1["volume"].values
        win = SCN_WIN_MIN
        blocks: List[float] = [arr[i - win:i].sum() for i in range(win, len(arr) - win, win)]
        base = (sum(blocks) / len(blocks)) if blocks else 1e-9
        rvol = float(recent) / max(float(base), 1e-9)
        return rvol, float(recent), float(base)
    except Exception:
        return 0.0, 0.0, 0.0

def hod_proximity(symbol: str) -> Tuple[float, float, float]:
    try:
        k5 = df_from_klines(klines(symbol, "5m", limit=288))
        if k5.empty:
            return 1.0, 0.0, 0.0
        hod = float(k5["high"].max())
        last = float(k5["close"].iloc[-1])
        diff = abs(hod - last) / hod if hod > 0 else 1.0
        return diff, last, hod
    except Exception:
        return 1.0, 0.0, 0.0

def vol_spike(symbol: str) -> bool:
    try:
        k1 = df_from_klines(klines(symbol, "1m", limit=60))
        if k1.empty or len(k1) < 21:
            return False
        avg = SMA(k1["volume"], 20).iloc[-2]
        last_v = k1["volume"].iloc[-1]
        if pd.isna(avg) or pd.isna(last_v):
            return False
        return float(last_v) > VOL_SPIKE_K * float(max(avg, 1e-9))
    except Exception:
        return False

def news_boost(symbol: str) -> bool:
    if not NEWS_ENABLED or not CRYPTOPANIC_KEY:
        return False
    try:
        r = SESSION.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={
                "auth_token": CRYPTOPANIC_KEY,
                "public": "true",
                "filter": "rising,hot,important",
                "currencies": symbol.replace("USDT", ""),
            },
            timeout=10,
        )
        j = r.json()
        now = datetime.utcnow()
        for p in j.get("results", []):
            ts = datetime.fromisoformat(p["published_at"].replace("Z", "+00:00")).replace(tzinfo=None)
            if (now - ts) <= timedelta(hours=2):
                return True
    except Exception:
        return False
    return False

# ===== State =====
@dataclass
class Pos:
    size: float = 0.0
    entry: float = 0.0
    stop: float = 0.0
    take: float = 0.0
    opened_at: Optional[datetime] = None
    partial_taken: bool = False
    trailing: bool = False

@dataclass
class Account:
    equity: float = START_CAPITAL
    day_start: float = START_CAPITAL
    last_stop: Optional[datetime] = None
    open: Dict[str, Pos] = field(default_factory=dict)

    def daily_dd(self) -> float:
        return max(0.0, (self.day_start - self.equity) / max(self.day_start, 1e-9))

# ===== Utils =====
def round_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    precision = int(round(-math.log10(step))) if step < 1 else 0
    steps = math.floor(qty / step)
    return float(f"{steps * step:.{precision}f}")

def current_R(entry: float, stop: float, price_now: float) -> float:
    risk = abs(entry - stop)
    if risk <= 1e-9:
        return 0.0
    move = price_now - entry
    return move / risk

# ===== Execution =====
def price(symbol: str) -> float:
    try:
        t = ticker24(symbol)
        lp = t.get("lastPrice") or t.get("last_price") or t.get("weightedAvgPrice")
        return float(lp) if lp is not None else 0.0
    except Exception:
        return 0.0

def place_market_buy(symbol: str, qty: float) -> Tuple[bool, Any]:
    if KILL_SWITCH:
        return True, {"dry": True}
    code, data = priv_post(
        "/api/v3/order",
        {"symbol": symbol, "side": "BUY", "type": "MARKET", "quantity": f"{qty:.8f}"},
    )
    return code == 200, data

def place_market_sell(symbol: str, qty: float) -> Tuple[bool, Any]:
    if KILL_SWITCH:
        return True, {"dry": True}
    code, data = priv_post(
        "/api/v3/order",
        {"symbol": symbol, "side": "SELL", "type": "MARKET", "quantity": f"{qty:.8f}"},
    )
    return code == 200, data

# ===== Strategy =====
def candidate_symbols(meta: Dict[str, Dict[str, float]]) -> List[str]:
    syms = list(meta.keys())
    rows: List[Tuple[float, str]] = []
    for s in syms:
        try:
            t = ticker24(s)
            chg = float(t.get("priceChangePercent", "0") or 0.0)
            vol = float(t.get("quoteVolume") or t.get("volume") or 0.0)
        except Exception:
            chg, vol = 0.0, 0.0
        rvol, _, _ = rvol_recent(s)
        hod_d, _, _ = hod_proximity(s)
        news = news_boost(s)
        hod_component = max(0.0, 1.0 - (hod_d / max(HOD_TOLERANCE, 1e-9)))
        score = (chg / 10.0) + rvol + hod_component + (1.0 if news else 0.0) + (math.log10(vol + 1.0) * 0.05)
        rows.append((score, s))
    rows.sort(key=lambda x: x[0], reverse=True)
    picks = [r[1] for r in rows[:SCN_TOP_N]]
    return picks

def trigger_long(symbol: str) -> Optional[Dict[str, Any]]:
    k1 = df_from_klines(klines(symbol, "1m", limit=60))
    if k1.empty or len(k1) < 20:
        return None
    last_close = float(k1["close"].iloc[-1])
    prev_high = float(k1["high"].iloc[-10:-1].max())
    hod_d, _, _ = hod_proximity(symbol)
    if hod_d > HOD_TOLERANCE:
        return None
    if not vol_spike(symbol):
        return None
    if last_close <= prev_high:
        return None
    atr = safe_last_atr(k1, 14)
    if atr is None or atr <= 0:
        return None
    flag_low = float(k1["low"].iloc[-5:-1].min())
    stop = min(flag_low, last_close - 1.2 * atr)
    if stop <= 0 or stop >= last_close:
        return None
    return {"entry": last_close, "stop": stop, "atr": atr}

def size_from_risk(acct: Account, entry: float, stop: float, step: float, minNotional: float) -> float:
    risk_usd = max(1.0, acct.equity * RISK_PER_TRADE)
    risk_per_unit = max(1e-9, entry - stop)
    qty = risk_usd / risk_per_unit
    qty = round_step(qty, step)
    if qty * entry < minNotional:
        qty = math.ceil(minNotional / entry / step) * step
    if qty * entry < minNotional:
        return 0.0
    return qty

# ===== Position management =====
@dataclass
class _DummyMeta:  # (placeholder in case meta is missing keys)
    step: float = 0.000001
    minNotional: float = 10.0

def manage_positions(db: DB, acct: Account, meta: Dict[str, Dict[str, float]]) -> None:
    to_close: List[str] = []
    for sym, pos in list(acct.open.items()):
        px = price(sym)
        if px <= 0:
            continue

        # Stop loss
        if px <= pos.stop:
            sz = pos.size
            ok, resp = place_market_sell(sym, sz)
            pnl = (px - pos.entry) * sz
            print(f"[EXIT-STOP] {sym} qty={sz} @~{px:.8f} pnl={pnl:.2f} {'DRY' if KILL_SWITCH else ''} resp={resp}")
            acct.equity += pnl
            db.ins_trade(f"{sym}-{int(time.time())}", sym, "sell", pos.entry, px, sz, pnl,
                         pos.opened_at.isoformat() if pos.opened_at else "", datetime.utcnow().isoformat())
            db.del_pos(sym)
            to_close.append(sym)
            continue

        # Take partial at +1R
        Rnow = current_R(pos.entry, pos.stop, px)
        if not pos.partial_taken and Rnow >= PARTIAL_AT_R:
            sell_sz = pos.size * PARTIAL_RATIO
            ok, resp = place_market_sell(sym, sell_sz)
            pnl = (px - pos.entry) * sell_sz
            print(f"[PARTIAL] {sym} sold={sell_sz} @~{px:.8f} R={Rnow:.2f} pnl+={pnl:.2f} {'DRY' if KILL_SWITCH else ''} resp={resp}")
            acct.equity += pnl
            pos.size -= sell_sz
            pos.partial_taken = True
            pos.stop = max(pos.stop, pos.entry)
            db.upsert_pos(sym, pos.size, pos.entry, pos.stop, pos.take,
                          pos.opened_at.isoformat() if pos.opened_at else datetime.utcnow().isoformat(),
                          pos.partial_taken, pos.trailing)

        # Begin/adjust trailing after partial
        if pos.partial_taken:
            k1 = df_from_klines(klines(sym, "1m", limit=60))
            atr = safe_last_atr(k1, 14)
            if atr and atr > 0:
                new_stop = max(pos.stop, px - ATR_MULT_TRAIL * atr)
                if new_stop > pos.stop:
                    pos.stop = new_stop
                    pos.trailing = True
                    db.upsert_pos(sym, pos.size, pos.entry, pos.stop, pos.take,
                                  pos.opened_at.isoformat() if pos.opened_at else datetime.utcnow().isoformat(),
                                  pos.partial_taken, pos.trailing)

    for sym in to_close:
        acct.open.pop(sym, None)

# ===== Main loop =====
def run_scan_and_trade():
    db = DB()
    equity = float(db.get("equity", START_CAPITAL))
    day_start = float(db.get("day_start", START_CAPITAL))
    acct = Account(equity=equity, day_start=day_start)

    info = exchange_info()
    meta = usdt_symbols(info)
    print(f"[INIT] symbols={len(meta)}  kill={KILL_SWITCH}  risk={RISK_PER_TRADE:.2%} minNotional~10")

    cooldown_until: Optional[datetime] = None
    last_day = datetime.utcnow().date()

    while True:
        try:
            now_day = datetime.utcnow().date()
            if now_day != last_day:
                acct.day_start = acct.equity
                db.set("day_start", acct.day_start)
                last_day = now_day
                print(f"[ROLLOVER] New UTC day. day_start={acct.day_start:.2f}")

            db.log_equity(acct.equity)

            if acct.daily_dd() >= DAILY_MAX_DD:
                if not cooldown_until or datetime.utcnow() >= cooldown_until:
                    cooldown_until = datetime.utcnow() + timedelta(minutes=COOLDOWN_MIN)
                    print(f"[RISK] daily DD {acct.daily_dd():.2%} reached. Cooling until {cooldown_until} UTC")
                time.sleep(1)
                continue

            if cooldown_until and datetime.utcnow() < cooldown_until:
                time.sleep(1)
                continue
            else:
                cooldown_until = None

            picks = candidate_symbols(meta)
            print(f"[SCAN] top={', '.join(picks)}")

            for sym in picks:
                if len(acct.open) >= MAX_OPEN_TRADES:
                    break
                if sym in acct.open:
                    continue

                trig = trigger_long(sym)
                if not trig:
                    continue

                entry, stop = trig["entry"], trig["stop"]
                s_meta = meta.get(sym, {"step": 0.000001, "minNotional": 10.0})
                qty = size_from_risk(acct, entry, stop, s_meta["step"], s_meta["minNotional"])
                if qty <= 0:
                    continue

                ok, resp = place_market_buy(sym, qty)
                print(f"[BUY] {sym} qty={qty} @~{entry:.8f} stop={stop:.8f} {'DRY' if KILL_SWITCH else ''} resp={resp}")

                take = entry + PARTIAL_AT_R * (entry - stop)
                pos = Pos(size=qty, entry=entry, stop=stop, take=take, opened_at=datetime.utcnow())
                acct.open[sym] = pos
                db.upsert_pos(sym, pos.size, pos.entry, pos.stop, pos.take, pos.opened_at.isoformat(), False, False)

            manage_positions(db, acct, meta)
            db.set("equity", acct.equity)
            time.sleep(LOOP_SLEEP)

        except requests.HTTPError as e:
            print(f"[HTTP] {e}")
            time.sleep(3)
        except Exception as e:
            import traceback
            print(f"[LOOP-ERR] {type(e).__name__}: {e}")
            traceback.print_exc()
            time.sleep(3)

# ===== Entrypoint =====
if __name__ == "__main__":
    try:
        run_scan_and_trade()
    except KeyboardInterrupt:
        print("\n[EXIT] KeyboardInterrupt")
    except Exception as e:
        import traceback
        print(f"[FATAL] {type(e).__name__}: {e}")
        traceback.print_exc()

