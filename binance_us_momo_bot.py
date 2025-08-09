#!/usr/bin/env python3
# Binance.US Momentum Bot (Ross-style) — multi-coin USDT scanner, partial @ +1R, ATR trail, SQLite

import os
import time
import hmac
import hashlib
import json
import sqlite3
import argparse
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd

# ----- Config / Env -----
BASE = "https://api.binance.us"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "momo-bus/1.1"})

BUSA_API_KEY = os.getenv("BUSA_API_KEY")
BUSA_API_SECRET = os.getenv("BUSA_API_SECRET")
DB_PATH = os.getenv("DB_PATH", "momo.sqlite3")

RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.01"))
KILL_SWITCH = os.getenv("KILL_SWITCH", "1") == "1"

NEWS_ENABLED = os.getenv("NEWS_ENABLED", "0") == "1"
CRYPTOPANIC_KEY = os.getenv("CRYPTOPANIC_KEY", "")

SCN_TOP_N = int(os.getenv("SCN_TOP_N", "8"))          # number of top symbols to consider
SCN_WIN_MIN = int(os.getenv("SCN_WIN_MIN", "30"))     # momentum window (minutes)
RVOL_BASE_HRS = int(os.getenv("RVOL_BASE_HRS", "24")) # rvol baseline (hours)
HOD_TOLERANCE = float(os.getenv("HOD_TOLERANCE", "0.006")) # within 0.6% of HOD
VOL_SPIKE_K = float(os.getenv("VOL_SPIKE_K", "1.5"))       # 1m vol spike vs 20 SMA
ATR_MULT_TRAIL = float(os.getenv("ATR_MULT_TRAIL", "1.5")) # trail multiplier
PARTIAL_AT_R = float(os.getenv("PARTIAL_AT_R", "1.0"))     # partial threshold in R
PARTIAL_RATIO = float(os.getenv("PARTIAL_RATIO", "0.5"))   # 50% partial
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "2"))
DAILY_MAX_DD = float(os.getenv("DAILY_MAX_DD", "0.05"))
COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN", "5"))
START_CAPITAL = float(os.getenv("START_CAPITAL", "100"))

# ----- SQLite -----
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

# ----- Binance.US helpers -----
def tsms() -> int:
    return int(time.time() * 1000)

def sign_params(params: Dict[str, str]) -> str:
    q = "&".join([f"{k}={params[k]}" for k in sorted(params.keys()) if params[k] is not None])
    sig = hmac.new(BUSA_API_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()
    return q + f"&signature={sig}"

def priv_post(path: str, params: Dict[str, str]) -> Tuple[int, Any]:
    params = {**params, "timestamp": str(tsms()), "recvWindow": "5000"}
    qsig = sign_params(params)
    r = SESSION.post(
        BASE + path,
        headers={"X-MBX-APIKEY": BUSA_API_KEY, "Content-Type": "application/x-www-form-urlencoded"},
        data=qsig,
        timeout=15,
    )
    ct = r.headers.get("Content-Type", "")
    return r.status_code, (r.json() if "application/json" in ct else r.text)

def pub_get(url: str, params: Optional[Dict[str, str]] = None) -> Any:
    r = SESSION.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

# ----- Market meta -----
def exchange_info() -> Dict[str, Any]:
    return pub_get(BASE + "/api/v3/exchangeInfo")

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
                    step = float(f.get("stepSize", step))
                    min_qty = float(f.get("minQty", min_qty))
                except Exception:
                    pass
            if ftype in ("NOTIONAL", "MIN_NOTIONAL"):
                try:
                    mn = float(f.get("minNotional", min_notional))
                    if mn > 0:
                        min_notional = mn
                except Exception:
                    pass
        out[sym] = {"step": step, "minQty": min_qty, "minNotional": min_notional}
    return out

def klines(symbol: str, interval: str, limit: int = 100) -> Any:
    return pub_get(BASE + "/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})

def ticker24(symbol: str) -> Any:
    return pub_get(BASE + "/api/v3/ticker/24hr", {"symbol": symbol})

# ----- Indicators -----
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

# ----- Momentum scan -----
def rvol_recent(symbol: str) -> Tuple[float, float, float]:
    limit = max(60 * RVOL_BASE_HRS, 120)
    k1 = df_from_klines(klines(symbol, "1m", limit=limit))
    if k1.empty or len(k1) < 60:
        return 0.0, 0.0, 0.0
    recent = k1.tail(SCN_WIN_MIN)["volume"].sum()
    blocks: List[float] = []
    arr = k1["volume"].values
    win = SCN_WIN_MIN
    for i in range(win, len(arr) - win, win):
        blocks.append(arr[i - win : i].sum())
    base = (sum(blocks) / len(blocks)) if blocks else 1e-9
    rvol = recent / max(base, 1e-9)
    return rvol, float(recent), float(base)

def hod_proximity(symbol: str) -> Tuple[float, float, float]:
    k5 = df_from_klines(klines(symbol, "5m", limit=288))
    if k5.empty:
        return 1.0, 0.0, 0.0
    hod = float(k5["high"].max())
    last = float(k5["close"].iloc[-1])
    diff = abs(hod - last) / hod if hod > 0 else 1.0
    return diff, last, hod

def vol_spike(symbol: str) -> bool:
    k1 = df_from_klines(klines(symbol, "1m", limit=60))
    if k1.empty or len(k1) < 21:
        return False
    avg = SMA(k1["volume"], 20).iloc[-2]
    try:
        return k1["volume"].iloc[-1] > VOL_SPIKE_K * float(max(avg, 1e-9))
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

# ----- State -----
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

# ----- Utils -----
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

# ----- Execution -----
def price(symbol: str) -> float:
    try:
        t = ticker24(symbol)
        return float(t.get("lastPrice"))
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

# ----- Strategy -----
def candidate_symbols(meta: Dict[str, Dict[str, float]]) -> List[str]:
    syms = list(meta.keys())
    rows: List[Tuple[float, str]] = []
    for s in syms:
        try:
            t = ticker24(s)
            chg = float(t.get("priceChangePercent", "0"))
            vol = float(t.get("volume", "0"))
        except Exception:
            chg, vol = 0.0, 0.0
        rvol, _, _ = rvol_recent(s)
        hod_d, _, _ = hod_proximity(s)
        news = news_boost(s)
        hod_component = max(0.0, 1.0 - (hod_d / max(HOD_TOLERANCE, 1e-9)))
        score = (chg / 10.0) + rvol + hod_component + (1.0 if news else 0.0)
        rows.append((score, s))
    rows.sort(key=lambda x: x[0], reverse=True)
    return [r[1] for r in rows[:SCN_TOP_N]]

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
    atr = float(ATR(k1, 14).iloc[-1])
    flag_low = float(k1["low"].iloc[-5:-1].min())
    stop = min(flag_low, last_close - 1.2 * atr)
    return {"entry": last_close, "stop": stop, "atr": atr}

def size_from_risk(acct: Account, entry: float, stop: float, step: float, minNotional: float) -> float:
    risk_usd = max(1.0, acct.equity * RISK_PER_TRADE)
    risk_per_unit = max(1e-9, entry - stop)
    qty = risk_usd / risk_per_unit
    qty = round_step(qty, step)
    if qty * entry < minNotional:
        qty = math.ceil(minNotional / entry / step) * step
    return qty



