#!/usr/bin/env python3
# GPT-driven Binance.US trader — BTCUSDT & ETHUSDT, long-only, spot.
# Dry by default (KILL_SWITCH=1). Flip to 0 when ready.

import os, time, hmac, hashlib, json, math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import requests

BASE = "https://api.binance.us"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "gpt-dual-trader/1.1"})

# ---- Env ----
BUSA_API_KEY    = os.getenv("BUSA_API_KEY")
BUSA_API_SECRET = os.getenv("BUSA_API_SECRET")
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY")
LLM_MODEL       = os.getenv("LLM_MODEL", "gpt-4o-mini")
def load_strategy_prompt(path="strategy_prompt.txt") -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        print(f"[FATAL] Could not read strategy prompt from {path}: {e}")
        return ""
STRATEGY_PROMPT = load_strategy_prompt()
KILL_SWITCH     = os.getenv("KILL_SWITCH", "1") == "1"
CONF_THRESHOLD  = float(os.getenv("CONF_THRESHOLD", "0.68"))
LOOP_SEC        = int(os.getenv("LOOP_SEC", "900"))   # 15min
USDT_CAP        = float(os.getenv("USDT_CAP", "15000"))
MAX_RISK_PCT    = float(os.getenv("MAX_RISK_PCT", "2.0"))
SYMBOLS         = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]

# ---- Utils ----
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def tsms() -> int:
    return int(time.time() * 1000)

def sign(params: Dict[str, str]) -> str:
    if not BUSA_API_SECRET:
        raise RuntimeError("Missing BUSA_API_SECRET")
    q = "&".join(f"{k}={params[k]}" for k in sorted(params.keys()) if params[k] is not None)
    sig = hmac.new(BUSA_API_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()
    return q + f"&signature={sig}"

# ---- Exchange public ----
def pub_get(path: str, params: Optional[Dict[str, str]] = None) -> Any:
    r = SESSION.get(BASE + path, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def ticker24(symbol: str) -> Dict[str, float]:
    t = pub_get("/api/v3/ticker/24hr", {"symbol": symbol})
    return {
        "last": float(t.get("lastPrice") or 0),
        "high": float(t.get("highPrice") or 0),
        "low":  float(t.get("lowPrice") or 0),
        "pct":  float(t.get("priceChangePercent") or 0),
        "qvol": float(t.get("quoteVolume") or 0),
    }

def klines(symbol: str, interval: str, limit: int = 200) -> List[List]:
    limit = max(1, min(limit, 1000))
    return pub_get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})

# ---- Indicators (no pandas) ----
def ema(vals: List[float], span: int) -> Optional[float]:
    if len(vals) < span or span <= 1: return None
    k = 2.0 / (span + 1.0)
    e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1 - k)
    return float(e)

def atr_ohlc(h: List[float], l: List[float], c: List[float], n: int = 14) -> Optional[float]:
    if len(h) < n + 1 or len(l) < n + 1 or len(c) < n + 1: return None
    trs = []
    for i in range(1, len(h)):
        trs.append(max(h[i] - l[i], abs(h[i] - c[i-1]), abs(l[i] - c[i-1])))
    if len(trs) < n: return None
    return sum(trs[-n:]) / n

def vwap_from_klines(kl: List[List], use_len: int = 120) -> Optional[float]:
    closes = [float(r[4]) for r in kl][-use_len:]
    vols   = [float(r[5]) for r in kl][-use_len:]
    if not closes or not vols or len(closes) != len(vols): return None
    num = sum(c*v for c,v in zip(closes, vols))
    den = sum(vols)
    return (num / den) if den > 0 else None

# ---- Exchange private ----
def priv_request(method: str, path: str, params: Dict[str, str]) -> Tuple[int, Any]:
    if not BUSA_API_KEY or not BUSA_API_SECRET:
        return 400, {"error": "Missing API keys"}
    p = {**params, "timestamp": str(tsms()), "recvWindow": "5000"}
    qsig = sign(p)
    hdr = {"X-MBX-APIKEY": BUSA_API_KEY, "Content-Type": "application/x-www-form-urlencoded"}
    url = BASE + path
    if method == "GET":
        r = SESSION.get(url + "?" + qsig, headers=hdr, timeout=15)
    elif method == "POST":
        r = SESSION.post(url, headers=hdr, data=qsig, timeout=15)
    else:
        r = SESSION.delete(url, headers=hdr, data=qsig, timeout=15)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text

def get_account() -> Dict[str, Any]:
    code, data = priv_request("GET", "/api/v3/account", {})
    if code != 200:
        raise RuntimeError(f"account error: {data}")
    return data

def get_balance(asset: str) -> float:
    try:
        acct = get_account()
        for b in acct.get("balances", []):
            if b.get("asset") == asset:
                return float(b.get("free") or 0)
    except Exception as e:
        print(f"[WARN] get_balance({asset}) failed: {e}")
    return 0.0

def market_buy(symbol: str, qty: float) -> Tuple[bool, Any]:
    if KILL_SWITCH: return True, {"dry": True}
    code, data = priv_request("POST", "/api/v3/order", {"symbol": symbol, "side":"BUY","type":"MARKET","quantity": f"{qty:.8f}"})
    return code == 200, data

def market_sell(symbol: str, qty: float) -> Tuple[bool, Any]:
    if KILL_SWITCH: return True, {"dry": True}
    code, data = priv_request("POST", "/api/v3/order", {"symbol": symbol, "side":"SELL","type":"MARKET","quantity": f"{qty:.8f}"})
    return code == 200, data

def exchange_info() -> Dict[str, Any]:
    try: return pub_get("/api/v3/exchangeInfo")
    except Exception as e:
        print(f"[WARN] exchange_info: {e}")
        return {"symbols":[]}

def fetch_symbol_meta(symbols: List[str]) -> Dict[str, Dict[str, float]]:
    info = exchange_info()
    out: Dict[str, Dict[str, float]] = {}
    for s in info.get("symbols", []):
        sym = s.get("symbol")
        if sym not in symbols: continue
        step, min_notional = 0.000001, 10.0
        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "LOT_SIZE":
                try: step = float(f.get("stepSize", step)) or step
                except: pass
            if t in ("NOTIONAL","MIN_NOTIONAL"):
                try:
                    mn = float(f.get("minNotional", min_notional))
                    if mn > 0: min_notional = mn
                except: pass
        out[sym] = {"step": step, "minNotional": min_notional}
    return out

def round_step(qty: float, step: float) -> float:
    if step <= 0: return qty
    precision = int(round(-math.log10(step))) if step < 1 else 0
    steps = math.floor(qty / step)
    return float(f"{steps * step:.{precision}f}")

# ---- LLM ----
def call_llm(system_prompt: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not OPENAI_API_KEY:
        print("[LLM] missing OPENAI_API_KEY")
        return None

    # Clean None values before sending
    for k, v in list(context.items()):
        if v is None:
            context[k] = "null"

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    body = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(context)}
        ],
        "temperature": 0.2
    }
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=body, timeout=40)
        r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"].strip()
        # Extract only the JSON block if extra text appears
        raw_json = raw[raw.find("{"): raw.rfind("}")+1]
        data = json.loads(raw_json)
        if not isinstance(data, dict):
            return None
        return data
    except Exception as e:
        print(f"[LLM] parse error: {e} raw={raw if 'raw' in locals() else 'N/A'}")
        return None

# ---- Context builder ----
def build_context(symbol: str) -> Dict[str, Any]:
    t24 = ticker24(symbol)
    k1h = klines(symbol, "1h", 200)
    k4h = klines(symbol, "4h", 200)

    c1 = [float(r[4]) for r in k1h]
    h1 = [float(r[2]) for r in k1h]
    l1 = [float(r[3]) for r in k1h]
    c4 = [float(r[4]) for r in k4h]

    ema50_1h  = ema(c1, 50)
    ema200_1h = ema(c1, 200)
    ema50_4h  = ema(c4, 50)
    ema200_4h = ema(c4, 200)
    vwap_1h   = vwap_from_klines(k1h, 120)
    atr1h     = atr_ohlc(h1, l1, c1, 14)

    have_usdt = get_balance("USDT") if not KILL_SWITCH else USDT_CAP
    base_asset = "BTC" if symbol == "BTCUSDT" else "ETH"
    have_asset = get_balance(base_asset) if not KILL_SWITCH else 0.0

    return {
        "symbol": symbol,
        "price": t24["last"], "pct_24h": t24["pct"], "high_24h": t24["high"], "low_24h": t24["low"], "qvol_24h": t24["qvol"],
        "ema50_1h": ema50_1h, "ema200_1h": ema200_1h, "ema50_4h": ema50_4h, "ema200_4h": ema200_4h,
        "vwap_1h": vwap_1h, "atr_1h": atr1h,
        "have_asset": have_asset, "have_usdt": have_usdt,
        "dry_run": KILL_SWITCH, "ts": utcnow_iso()
    }

# ---- Validation & execution prep ----
def prepare_execution(dec: Dict[str, Any], meta: Dict[str, Dict[str, float]], ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # required keys
    req = ["action","symbol","entry_type","entry_price","amount_usdt","risk_pct","stop_loss","take_profit","confidence","reason"]
    if any(k not in dec for k in req):
        print("[GUARD] missing keys")
        return None

    action = str(dec["action"]).lower()
    symbol = str(dec["symbol"]).upper()
    if symbol not in SYMBOLS:
        print("[GUARD] symbol not allowed")
        return None
    if action not in ("buy","sell","hold"):
        print("[GUARD] invalid action")
        return None

    conf = float(dec.get("confidence", 0.0))
    if conf < CONF_THRESHOLD:
        print(f"[GUARD] confidence {conf:.2f} < {CONF_THRESHOLD:.2f}")
        return None

    price = float(ctx["price"])
    step = meta.get(symbol, {}).get("step", 0.000001)
    min_notional = meta.get(symbol, {}).get("minNotional", 10.0)

    if action == "sell":
        if (ctx["have_asset"] or 0.0) <= 0.0:
            print("[GUARD] no holdings to sell")
            return None
        qty = round_step(float(ctx["have_asset"]), step)
        if qty * price < min_notional or qty <= 0:
            print("[GUARD] notional too small to sell")
            return None
        return {"action":"sell","symbol":symbol,"qty":qty}

    if action == "hold":
        return {"action":"hold","symbol":symbol}

    # BUY checks
    stop  = float(dec["stop_loss"])
    take  = float(dec["take_profit"])
    if stop >= price:
        print("[GUARD] stop not below entry")
        return None
    stop_dist_pct = (price - stop) / max(price, 1e-9) * 100.0
    if stop_dist_pct > 3.0:
        print(f"[GUARD] stop distance {stop_dist_pct:.2f}% > 3%")
        return None
    rr = (take - price) / max(price - stop, 1e-9)
    if rr < 1.8:
        print(f"[GUARD] RR {rr:.2f} < 1.8")
        return None

    # bullish regime
    if not (ctx["ema50_1h"] and ctx["ema200_1h"] and ctx["ema50_4h"] and ctx["ema200_4h"] and ctx["vwap_1h"]):
        print("[GUARD] missing regime indicators")
        return None
    if not (ctx["ema50_1h"] > ctx["ema200_1h"] and price >= ctx["vwap_1h"] and ctx["ema50_4h"] > ctx["ema200_4h"]):
        print("[GUARD] regime not bullish")
        return None

    # sizing
    avail_usdt = float(ctx["have_usdt"])
    max_usdt = min(avail_usdt, USDT_CAP)
    risk_pct = max(0.2, min(float(dec["risk_pct"]), MAX_RISK_PCT))  # percentage
    amt_usdt = dec["amount_usdt"]

    if amt_usdt is None:
        risk_usdt = max_usdt * (risk_pct / 100.0)
        risk_per_unit = price - stop
        if risk_per_unit <= 0:
            print("[GUARD] invalid risk per unit")
            return None
        qty = risk_usdt / risk_per_unit
        # cap by max_usdt
        qty = min(qty, max_usdt / max(price, 1e-9))
    else:
        qty = float(amt_usdt) / max(price, 1e-9)
        qty = min(qty, max_usdt / max(price, 1e-9))

    qty = round_step(qty, step)
    if qty * price < min_notional or qty <= 0:
        print(f"[GUARD] notional too small: qty={qty} notional={qty*price:.2f} < {min_notional}")
        return None

    return {"action":"buy","symbol":symbol,"qty":qty}

# ---- Main loop ----
def main():
    print(f"[BOOT] GPT trader starting — symbols={SYMBOLS} dry={KILL_SWITCH} conf>={CONF_THRESHOLD} loop={LOOP_SEC}s")
    if not STRATEGY_PROMPT:
        print("[FATAL] STRATEGY_PROMPT missing"); return
    if not OPENAI_API_KEY:
        print("[WARN] OPENAI_API_KEY missing (decisions will fail)")
    meta = fetch_symbol_meta(SYMBOLS)
    if not meta:
        print("[WARN] exchange info/meta empty; will still attempt trading")

    while True:
        try:
            for sym in SYMBOLS:
                ctx = build_context(sym)
                dec = call_llm(STRATEGY_PROMPT, ctx)
                if not dec:
                    print(f"[LLM] no/invalid decision for {sym}")
                    continue

                prep = prepare_execution(dec, meta, ctx)
                if not prep:
                    # guardrails rejected; already logged why
                    continue

                if prep["action"] == "hold":
                    print(f"[HOLD] {sym} price={ctx['price']:.2f}")
                    continue

                if prep["action"] == "sell":
                    ok, resp = market_sell(sym, prep["qty"])
                    print(f"[SELL] {sym} qty={prep['qty']} price=~{ctx['price']:.2f} dry={KILL_SWITCH} resp={resp}")
                    continue

                if prep["action"] == "buy":
                    ok, resp = market_buy(sym, prep["qty"])
                    print(f"[BUY]  {sym} qty={prep['qty']} price=~{ctx['price']:.2f} dry={KILL_SWITCH} resp={resp}")
                    continue

            time.sleep(LOOP_SEC)
        except requests.HTTPError as e:
            print(f"[HTTP] {e}"); time.sleep(5)
        except Exception as e:
            print(f"[ERR] {type(e).__name__}: {e}"); time.sleep(5)

if __name__ == "__main__":
    main()



