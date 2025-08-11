#!/usr/bin/env python3
# GPT-signal executor for Binance.US — BTC/ETH, long-only.
# Now with OCO bracket, breakeven at +1R, 50% PT at +1R, 0.7R trailing on remainder.

import os, time, hmac, hashlib, json, math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import requests

BASE = "https://api.binance.us"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "gpt-signal-executor/1.2"})

# ---------- Env ----------
BUSA_API_KEY    = os.getenv("BUSA_API_KEY")
BUSA_API_SECRET = os.getenv("BUSA_API_SECRET")
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY")
LLM_MODEL       = (os.getenv("LLM_MODEL") or "gpt-4o-mini").strip()
KILL_SWITCH     = os.getenv("KILL_SWITCH", "1") == "1"   # 1=dry-run
CONF_THRESHOLD  = float(os.getenv("CONF_THRESHOLD", "0.68"))
LOOP_SEC        = int(os.getenv("LOOP_SEC", "900"))      # 15 min
USDT_CAP        = float(os.getenv("USDT_CAP", "15000"))
MAX_RISK_PCT    = float(os.getenv("MAX_RISK_PCT", "2.0"))
SYMBOLS         = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]

# trailing config
TRAIL_R_MULT    = float(os.getenv("TRAIL_R_MULT", "0.7"))    # 0.7R trail distance
PARTIAL_PCT     = float(os.getenv("PARTIAL_PCT", "0.5"))     # 50% take at +1R
OCO_STOP_OFFSET = float(os.getenv("OCO_STOP_OFFSET", "0.001"))  # stopLimitPrice = stop*(1-0.001)

def load_strategy_prompt(path="strategy_prompt.txt") -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        print(f"[FATAL] Could not read strategy prompt: {e}")
        return ""
STRATEGY_PROMPT = load_strategy_prompt()

# ---------- Utils ----------
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

# ---------- Public API ----------
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

# ---------- Indicators (no pandas) ----------
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

# --- Extra indicators / prefilter ---
def rsi_last_two(closes: List[float], n: int = 14) -> Tuple[Optional[float], Optional[float]]:
    if len(closes) < n + 2: return None, None
    gains, losses = [], []
    for i in range(1, len(closes)):
        chg = closes[i] - closes[i-1]
        gains.append(max(chg, 0.0))
        losses.append(max(-chg, 0.0))
    avg_gain = sum(gains[:n]) / n
    avg_loss = sum(losses[:n]) / n
    rsis = []
    rsis.append(100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + (avg_gain / avg_loss)))
    for i in range(n, len(gains)):
        avg_gain = (avg_gain * (n-1) + gains[i]) / n
        avg_loss = (avg_loss * (n-1) + losses[i]) / n
        rsis.append(100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + (avg_gain / avg_loss)))
    return (rsis[-1], rsis[-2] if len(rsis) >= 2 else None)

def tight_base_pct(closes: List[float], lookback: int = 5) -> Optional[float]:
    if len(closes) < lookback: return None
    window = closes[-lookback:]
    lo, hi = min(window), max(window)
    if lo <= 0: return None
    return (hi - lo) / lo * 100.0

def volatility_24h_pct(high_24h: float, low_24h: float) -> Optional[float]:
    if not low_24h or low_24h <= 0: return None
    return (high_24h - low_24h) / low_24h * 100.0

def passes_pre_filter(ctx: Dict[str, Any]) -> bool:
    price = float(ctx.get("price") or 0)
    vwap = ctx.get("vwap_1h")
    ema50_1h, ema200_1h = ctx.get("ema50_1h"), ctx.get("ema200_1h")
    ema50_4h, ema200_4h = ctx.get("ema50_4h"), ctx.get("ema200_4h")
    if not all([price, vwap, ema50_1h, ema200_1h, ema50_4h, ema200_4h]): return False
    if not (ema50_1h > ema200_1h and ema50_4h > ema200_4h and price >= vwap): return False
    rsi_last, rsi_prev = ctx.get("rsi14_1h"), ctx.get("rsi14_1h_prev")
    if rsi_last is None or rsi_last <= 55: return False
    if rsi_prev is not None and (rsi_last - rsi_prev) < -5.0: return False
    if rsi_last > 80: return False
    vol24 = ctx.get("volatility_24h_pct")
    if vol24 and vol24 > 3.0:
        if rsi_prev is None or rsi_last < rsi_prev: return False
    pv_ratio = ctx.get("price_vwap_ratio")
    sym = str(ctx.get("symbol") or "")
    chase_cap = 1.015 if sym == "BTCUSDT" else 1.02
    if pv_ratio and pv_ratio > chase_cap: return False
    base_cap = 1.0 if sym == "BTCUSDT" else 1.2
    tb = ctx.get("base_tight_pct")
    if tb is None or tb > base_cap: return False
    return True

# ---------- Private API ----------
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

# Convenience wrappers
def new_order(symbol: str, side: str, type_: str, **extra) -> Tuple[int, Any]:
    params = {"symbol": symbol, "side": side.upper(), "type": type_.upper(), **{k:v for k,v in extra.items() if v is not None}}
    return priv_request("POST", "/api/v3/order", params)

def oco_new_sell(symbol: str, quantity: float, limit_price: float, stop_price: float, stop_limit_price: float, timeInForce: str = "GTC") -> Tuple[int, Any]:
    """
    Create SELL-side OCO (TP + SL). Uses the newer OCO endpoint.
    """
    params = {
        "symbol": symbol,
        "side": "SELL",
        "quantity": f"{quantity:.8f}",
        "price": f"{limit_price:.8f}",              # take-profit LIMIT price
        "stopPrice": f"{stop_price:.8f}",           # stop trigger
        "stopLimitPrice": f"{stop_limit_price:.8f}",# stop-limit execute price
        "stopLimitTimeInForce": timeInForce
    }
    # New endpoint per Spot docs: /api/v3/orderList/oco
    return priv_request("POST", "/api/v3/orderList/oco", params)

def oco_cancel(symbol: str, orderListId: Optional[int] = None, listClientOrderId: Optional[str] = None) -> Tuple[int, Any]:
    params: Dict[str, str] = {"symbol": symbol}
    if orderListId is not None: params["orderListId"] = str(orderListId)
    if listClientOrderId is not None: params["listClientOrderId"] = listClientOrderId
    return priv_request("DELETE", "/api/v3/orderList", params)

def open_order_lists(symbol: str) -> Any:
    return priv_request("GET", "/api/v3/openOrderLists", {"symbol": symbol})

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
    code, data = new_order(symbol, "BUY", "MARKET", quantity=f"{qty:.8f}")
    return code == 200, data

def market_sell(symbol: str, qty: float) -> Tuple[bool, Any]:
    if KILL_SWITCH: return True, {"dry": True}
    code, data = new_order(symbol, "SELL", "MARKET", quantity=f"{qty:.8f}")
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

# ---------- LLM ----------
def call_llm(system_prompt: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not OPENAI_API_KEY:
        print("[LLM] missing OPENAI_API_KEY")
        return None

    for k, v in list(context.items()):
        if v is None or (isinstance(v, float) and (v != v)):
            context[k] = "null"

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
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
        raw_json = raw[raw.find("{"): raw.rfind("}")+1]
        data = json.loads(raw_json)
        if not isinstance(data, dict): return None
        return data
    except Exception as e:
        print(f"[LLM] parse/request error: {e}")
        return None

# ---------- Context ----------
def build_context(symbol: str) -> Dict[str, Any]:
    t24 = ticker24(symbol)
    k1h = klines(symbol, "1h", 200); k4h = klines(symbol, "4h", 200)
    c1 = [float(r[4]) for r in k1h]; h1 = [float(r[2]) for r in k1h]; l1 = [float(r[3]) for r in k1h]
    c4 = [float(r[4]) for r in k4h]
    ema50_1h  = ema(c1, 50); ema200_1h = ema(c1, 200); ema50_4h = ema(c4, 50); ema200_4h = ema(c4, 200)
    vwap_1h   = vwap_from_klines(k1h, 120); atr1h = atr_ohlc(h1, l1, c1, 14)
    rsi_last, rsi_prev = rsi_last_two(c1, 14)
    vol24 = volatility_24h_pct(t24["high"], t24["low"])
    base_tight = tight_base_pct(c1, 5)
    price_vwap_ratio = (t24["last"] / vwap_1h) if vwap_1h else None
    have_usdt = get_balance("USDT") if not KILL_SWITCH else USDT_CAP
    base_asset = "BTC" if symbol == "BTCUSDT" else "ETH"
    have_asset = get_balance(base_asset) if not KILL_SWITCH else 0.0
    return {
        "symbol": symbol,
        "price": t24["last"], "pct_24h": t24["pct"], "high_24h": t24["high"], "low_24h": t24["low"], "qvol_24h": t24["qvol"],
        "ema50_1h": ema50_1h, "ema200_1h": ema200_1h, "ema50_4h": ema50_4h, "ema200_4h": ema200_4h,
        "vwap_1h": vwap_1h, "atr_1h": atr1h,
        "rsi14_1h": rsi_last, "rsi14_1h_prev": rsi_prev,
        "volatility_24h_pct": vol24, "base_tight_pct": base_tight, "price_vwap_ratio": price_vwap_ratio,
        "have_asset": have_asset, "have_usdt": have_usdt, "ts_utc": utcnow_iso()
    }

# ---------- Guardrails & Sizing ----------
def prepare_execution(dec: Dict[str, Any], meta: Dict[str, Dict[str, float]], ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    req = ["action","symbol","entry_type","entry_price","stop_loss","take_profit","risk_pct","confidence","reason"]
    if any(k not in dec for k in req):
        print("[GUARD] missing keys"); return None
    action = str(dec["action"]).lower(); symbol = str(dec["symbol"]).upper()
    if symbol not in SYMBOLS: print("[GUARD] symbol not allowed"); return None
    if action not in ("buy","sell","hold"): print("[GUARD] invalid action"); return None

    conf = float(dec.get("confidence", 0.0))
    if conf < CONF_THRESHOLD:
        print(f"[GUARD] confidence {conf:.2f} < {CONF_THRESHOLD:.2f}"); return None

    price = float(ctx["price"])
    step = meta.get(symbol, {}).get("step", 0.000001)
    min_notional = meta.get(symbol, {}).get("minNotional", 10.0)

    if action == "sell":
        if (ctx["have_asset"] or 0.0) <= 0.0: print("[GUARD] no holdings to sell"); return None
        qty = round_step(float(ctx["have_asset"]), step)
        if qty * price < min_notional or qty <= 0: print("[GUARD] notional too small to sell"); return None
        return {"action":"sell","symbol":symbol,"qty":qty}

    if action == "hold":
        return {"action":"hold","symbol":symbol}

    # BUY checks
    stop  = float(dec["stop_loss"]); take = float(dec["take_profit"])
    if stop >= price: print("[GUARD] stop not below entry"); return None
    stop_dist_pct = (price - stop) / max(price, 1e-9) * 100.0
    if stop_dist_pct > 4.0: print(f"[GUARD] stop distance {stop_dist_pct:.2f}% > 4%"); return None
    rr = (take - price) / max(price - stop, 1e-9)
    min_rr = 1.5 if conf >= 0.75 else 1.8
    if rr < min_rr: print(f"[GUARD] RR {rr:.2f} < {min_rr:.1f} (conf={conf:.2f})"); return None

    avail_usdt = float(ctx["have_usdt"]); max_usdt = min(avail_usdt, USDT_CAP)
    risk_pct = max(0.2, min(float(dec["risk_pct"]), MAX_RISK_PCT))
    risk_usdt = max_usdt * (risk_pct / 100.0)
    risk_per_unit = price - stop
    if risk_per_unit <= 0: print("[GUARD] invalid risk per unit"); return None
    qty = risk_usdt / risk_per_unit
    qty = min(qty, max_usdt / max(price, 1e-9))
    qty = round_step(qty, step)
    if qty * price < min_notional or qty <= 0:
        print(f"[GUARD] notional too small: qty={qty} value={qty*price:.2f} < {min_notional}"); return None

    return {"action":"buy","symbol":symbol,"qty":qty, "entry":price, "stop":stop, "take":take, "conf":conf}

# ---------- Position / Profit Security ----------
# We track a tiny in-memory state for each symbol to manage partials & trailing.
POSITION: Dict[str, Dict[str, Any]] = {}  # symbol -> {entry, stop, take, qty, r, partial_done, oco_id, remainder_qty, trail_stop}

def place_oco_for(symbol: str, qty: float, entry: float, stop: float, take: float) -> Optional[int]:
    stop_limit = stop * (1.0 - OCO_STOP_OFFSET)
    if KILL_SWITCH:
        print(f"[OCO] DRY RUN create OCO SELL {symbol} qty={qty:.8f} TP={take:.2f} SL={stop:.2f}/{stop_limit:.2f}")
        return 0
    code, resp = oco_new_sell(symbol, qty, take, stop, stop_limit)
    if code != 200:
        print(f"[OCO] create failed {code}: {resp}")
        return None
    oid = int(resp.get("orderListId") or 0)
    print(f"[OCO] created orderListId={oid} for {symbol}")
    return oid

def replace_oco(symbol: str, oco_id: Optional[int], qty: float, entry: float, new_stop: float, new_take: float) -> Optional[int]:
    if oco_id and not KILL_SWITCH:
        oco_cancel(symbol, orderListId=oco_id)
        time.sleep(0.5)
    return place_oco_for(symbol, qty, entry, new_stop, new_take)

# ---------- Main ----------
def main():
    print(f"[BOOT] GPT signal executor — symbols={SYMBOLS} dry={KILL_SWITCH} conf>={CONF_THRESHOLD} loop={LOOP_SEC}s model={LLM_MODEL}")
    if not STRATEGY_PROMPT: print("[FATAL] Empty strategy prompt"); return
    if not OPENAI_API_KEY: print("[WARN] OPENAI_API_KEY missing (LLM will fail)")

    meta = fetch_symbol_meta(SYMBOLS)
    if not meta: print("[WARN] exchange meta empty")

    while True:
        try:
            for sym in SYMBOLS:
                ctx = build_context(sym)

                # PREFILTER: only wake LLM on quality setups
                if not passes_pre_filter(ctx):
                    print(f"[SKIP] {sym} prefilter blocked — regime/momentum/vol/base not ideal")
                    # If already in position, manage trailing anyway
                    manage_open_position(sym, ctx)
                    continue

                dec = call_llm(STRATEGY_PROMPT, ctx)
                if not dec:
                    print(f"[LLM] no/invalid decision for {sym}")
                    manage_open_position(sym, ctx)
                    continue

                prep = prepare_execution(dec, meta, ctx)
                if not prep:
                    manage_open_position(sym, ctx)
                    continue

                if prep["action"] == "hold":
                    print(f"[HOLD] {sym} price={ctx['price']:.2f}")
                    manage_open_position(sym, ctx)

                elif prep["action"] == "sell":
                    ok, resp = market_sell(sym, prep["qty"])
                    print(f"[SELL] {sym} qty={prep['qty']} ~{ctx['price']:.2f} dry={KILL_SWITCH} resp={resp}")
                    POSITION.pop(sym, None)  # reset state after full exit

                elif prep["action"] == "buy":
                    qty = prep["qty"]; entry = float(ctx["price"])
                    ok, resp = market_buy(sym, qty)
                    print(f"[BUY ] {sym} qty={qty} ~{entry:.2f} dry={KILL_SWITCH} resp={resp}")

                    # record position & R
                    stop = float(dec["stop_loss"]); take = float(dec["take_profit"])
                    R = entry - stop
                    POSITION[sym] = {
                        "entry": entry, "stop": stop, "take": take, "qty": qty,
                        "r": R, "partial_done": False, "oco_id": None,
                        "remainder_qty": qty, "trail_stop": stop
                    }

                    # place OCO bracket for full qty
                    oid = place_oco_for(sym, qty, entry, stop, take)
                    POSITION[sym]["oco_id"] = oid

                # manage open after decision
                manage_open_position(sym, ctx)

            time.sleep(LOOP_SEC)
        except requests.HTTPError as e:
            print(f"[HTTP] {e}"); time.sleep(5)
        except Exception as e:
            print(f"[ERR] {type(e).__name__}: {e}"); time.sleep(5)

# ---------- Profit Management Logic ----------
def manage_open_position(symbol: str, ctx: Dict[str, Any]) -> None:
    st = POSITION.get(symbol)
    if not st:
        return
    price = float(ctx["price"])
    entry, stop, take = st["entry"], st["stop"], st["take"]
    qty = st["remainder_qty"]
    R = st["r"]
    if R <= 0 or qty <= 0:
        return

    # 1) Breakeven & Partial at +1R
    if price >= entry + R:
        # Partial take once
        if not st["partial_done"]:
            sell_qty = max(0.0, qty * PARTIAL_PCT)
            if sell_qty > 0:
                if KILL_SWITCH:
                    print(f"[PT  ] DRY RUN {symbol} sell {sell_qty:.8f} at ~{price:.2f} (+1R partial)")
                else:
                    ok, resp = market_sell(symbol, sell_qty)
                    print(f"[PT  ] {symbol} sell {sell_qty:.8f} at ~{price:.2f} resp={resp}")
                st["remainder_qty"] = max(0.0, qty - sell_qty)
                st["partial_done"] = True

        # Move stop to breakeven for remainder via OCO replace
        if st["remainder_qty"] > 0:
            new_stop = max(entry, st.get("trail_stop", stop))
            new_take = take  # keep original TP
            if new_stop > stop + 1e-9:  # update only if changed
                oid = replace_oco(symbol, st.get("oco_id"), st["remainder_qty"], entry, new_stop, new_take)
                st["oco_id"] = oid
                st["stop"] = new_stop
                st["trail_stop"] = new_stop
                print(f"[BE  ] {symbol} moved stop -> breakeven {new_stop:.2f}")

    # 2) Trail stop by 0.7R once > +1R
    if price > entry + R and st["remainder_qty"] > 0:
        desired_trail = price - TRAIL_R_MULT * R
        if desired_trail > st.get("trail_stop", stop) + (0.05 * R):  # update with small hysteresis
            new_stop = desired_trail
            new_take = take
            oid = replace_oco(symbol, st.get("oco_id"), st["remainder_qty"], entry, new_stop, new_take)
            st["oco_id"] = oid
            st["trail_stop"] = new_stop
            print(f"[TRL ] {symbol} trail stop -> {new_stop:.2f} (price {price:.2f})")

    # 3) If holdings gone (e.g., TP/SL filled on exchange), clear state
    # We infer via balance check or open order lists; cheap check: if asset balance < tiny threshold, nuke state.
    base_asset = "BTC" if symbol == "BTCUSDT" else "ETH"
    bal = get_balance(base_asset) if not KILL_SWITCH else st["remainder_qty"]  # in dry-run, use local qty
    if not KILL_SWITCH and bal * price < 5.0:  # ~flat
        print(f"[FLAT] {symbol} position closed on exchange; clearing local state.")
        POSITION.pop(symbol, None)

if __name__ == "__main__":
    main()



