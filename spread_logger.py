#!/usr/bin/env python3
"""
Bybit vs Hyperliquid funding-spread LOGGER. READ-ONLY. Trades nothing.

Runs hourly (cron). Each run snapshots current funding on both venues, normalized to an
8h-equivalent rate, and appends one row per tracked symbol to a CSV.

Sources per run:
  Bybit    GET  https://api.bybit.com/v5/market/tickers?category=linear   -> fundingRate (per interval), markPrice
           GET  .../v5/market/instruments-info?category=linear            -> fundingInterval (min), cached
  Hyperliquid POST https://api.hyperliquid.xyz/info {"type":"metaAndAssetCtxs"}
           -> funding is HOURLY -> 8h-equiv = funding * 8 ; markPx

Normalization:
  bybit_rate_8h    = fundingRate * (480 / funding_interval_min)   # 8h->x1, 4h->x2, 1h->x8
  hl_rate_8h_equiv = hl_funding_hourly * 8

Symbols: BTC, ETH, SOL + top-10 Bybit linear-USDT by 24h turnover that also have a HL perp.
Resolved once on first run and cached to data/spread/symbols.json (stable across the 30d window).

CSV: data/spread/bybit_hl_spread.csv
  cols: ts_iso, ts_ms, symbol, bybit_rate_8h, hl_rate_8h_equiv, bybit_mark, hl_mark,
        bybit_interval_min
"""
import os
import csv
import json
import time
from datetime import datetime, timezone

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE, "data", "spread")
CSV_PATH = os.path.join(OUT_DIR, "bybit_hl_spread.csv")
SYMS_PATH = os.path.join(OUT_DIR, "symbols.json")
IVCACHE_PATH = os.path.join(OUT_DIR, "bybit_intervals.json")

PROXIES = {"https": "http://31.70.81.103:3128", "http": "http://31.70.81.103:3128"}
BYBIT = "https://api.bybit.com"
HL = "https://api.hyperliquid.xyz/info"
CORE = ["BTC", "ETH", "SOL"]
TOP_N = 10
CSV_COLS = ["ts_iso", "ts_ms", "symbol", "bybit_rate_8h", "hl_rate_8h_equiv",
            "bybit_mark", "hl_mark", "bybit_interval_min"]


def log(*a):
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}]", *a, flush=True)


def bybit_get(path, params):
    r = requests.get(BYBIT + path, params=params, proxies=PROXIES, timeout=30)
    r.raise_for_status()
    d = r.json()
    if d.get("retCode") not in (0, None):
        raise RuntimeError(f"bybit {path} retCode={d.get('retCode')} {d.get('retMsg')}")
    return d["result"]["list"]


def bybit_get_paged(path, params):
    """Follow nextPageCursor (instruments-info returns ~500/page, >800 linear symbols)."""
    out, cur = [], None
    while True:
        p = dict(params, limit=1000)
        if cur:
            p["cursor"] = cur
        r = requests.get(BYBIT + path, params=p, proxies=PROXIES, timeout=30)
        r.raise_for_status()
        d = r.json()
        if d.get("retCode") not in (0, None):
            raise RuntimeError(f"bybit {path} retCode={d.get('retCode')} {d.get('retMsg')}")
        res = d["result"]
        out.extend(res.get("list") or [])
        cur = res.get("nextPageCursor")
        if not cur:
            break
    return out


def hl_post(body):
    r = requests.post(HL, json=body, timeout=30)
    r.raise_for_status()
    return r.json()


def bybit_tickers():
    """symbol -> {fundingRate, markPrice, turnover24h} for USDT linear."""
    out = {}
    for x in bybit_get("/v5/market/tickers", {"category": "linear"}):
        s = x["symbol"]
        if not s.endswith("USDT"):
            continue
        try:
            out[s] = {
                "fundingRate": float(x["fundingRate"]) if x.get("fundingRate") not in ("", None) else None,
                "markPrice": float(x["markPrice"]) if x.get("markPrice") not in ("", None) else None,
                "turnover24h": float(x.get("turnover24h") or 0),
            }
        except (TypeError, ValueError):
            continue
    return out


def bybit_intervals():
    """symbol -> fundingInterval (minutes). Cached; refreshed if cache missing."""
    if os.path.exists(IVCACHE_PATH):
        try:
            return json.load(open(IVCACHE_PATH))
        except Exception:
            pass
    out = {}
    for x in bybit_get_paged("/v5/market/instruments-info", {"category": "linear"}):
        iv = x.get("fundingInterval")
        if iv is not None:
            out[x["symbol"]] = int(iv)
    os.makedirs(OUT_DIR, exist_ok=True)
    json.dump(out, open(IVCACHE_PATH, "w"), indent=0)
    return out


def hl_ctx():
    """HL coin name -> {funding_hourly, markPx}."""
    meta, ctxs = hl_post({"type": "metaAndAssetCtxs"})
    uni = meta["universe"]
    out = {}
    for i, u in enumerate(uni):
        a = ctxs[i]
        try:
            out[u["name"]] = {
                "funding": float(a["funding"]) if a.get("funding") not in ("", None) else None,
                "markPx": float(a["markPx"]) if a.get("markPx") not in ("", None) else None,
            }
        except (TypeError, ValueError, KeyError):
            continue
    return out


def base_of(sym):
    return sym[:-4] if sym.endswith("USDT") else sym


def to_hl_name(base, hl_names):
    if base in hl_names:
        return base
    if base.startswith("1000"):
        for cand in ("k" + base[4:], base[4:]):
            if cand in hl_names:
                return cand
    return None


def resolve_symbols(tk, hl):
    """[ (bybit_symbol, base, hl_name), ... ] = CORE + top-10 turnover with HL perp."""
    if os.path.exists(SYMS_PATH):
        return [tuple(x) for x in json.load(open(SYMS_PATH))]
    hl_names = set(hl.keys())
    picked = []
    seen = set()
    # core first
    for b in CORE:
        s = b + "USDT"
        h = to_hl_name(b, hl_names)
        if s in tk and h:
            picked.append([s, b, h])
            seen.add(b)
    # top-10 by turnover
    ranked = sorted(tk.items(), key=lambda kv: -kv[1]["turnover24h"])
    n = 0
    for s, v in ranked:
        b = base_of(s)
        if b in seen:
            continue
        h = to_hl_name(b, hl_names)
        if not h:
            continue
        picked.append([s, b, h])
        seen.add(b)
        n += 1
        if n >= TOP_N:
            break
    os.makedirs(OUT_DIR, exist_ok=True)
    json.dump(picked, open(SYMS_PATH, "w"), indent=2)
    log(f"resolved symbols -> {SYMS_PATH}: {[p[0] for p in picked]}")
    return [tuple(x) for x in picked]


WINDOW_DAYS = 30
START_PATH = os.path.join(OUT_DIR, "started_at.txt")


def window_active():
    """First run stamps start time; after WINDOW_DAYS the logger goes dormant (cron no-ops)."""
    os.makedirs(OUT_DIR, exist_ok=True)
    now = time.time()
    if not os.path.exists(START_PATH):
        with open(START_PATH, "w") as f:
            f.write(str(now))
        return True
    try:
        start = float(open(START_PATH).read().strip())
    except Exception:
        start = now
    if now - start > WINDOW_DAYS * 86_400:
        log(f"window complete ({WINDOW_DAYS}d since {datetime.fromtimestamp(start, timezone.utc).isoformat()}); dormant")
        return False
    return True


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if not window_active():
        return
    tk = bybit_tickers()
    ivs = bybit_intervals()
    hl = hl_ctx()
    symbols = resolve_symbols(tk, hl)

    now = datetime.now(timezone.utc)
    ts_ms = int(now.timestamp() * 1000)
    ts_iso = now.replace(microsecond=0).isoformat()

    rows = []
    for bsym, base, hname in symbols:
        t = tk.get(bsym)
        h = hl.get(hname)
        iv = ivs.get(bsym)
        if not t or not h or iv is None or t["fundingRate"] is None or h["funding"] is None:
            log(f"  skip {bsym}/{hname}: missing data (t={bool(t)} h={bool(h)} iv={iv})")
            continue
        bybit_8h = t["fundingRate"] * (480.0 / iv)
        hl_8h = h["funding"] * 8.0
        rows.append({
            "ts_iso": ts_iso, "ts_ms": ts_ms, "symbol": base,
            "bybit_rate_8h": f"{bybit_8h:.10f}",
            "hl_rate_8h_equiv": f"{hl_8h:.10f}",
            "bybit_mark": t["markPrice"], "hl_mark": h["markPx"],
            "bybit_interval_min": iv,
        })

    new_file = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow(r)
    log(f"logged {len(rows)} rows @ {ts_iso} -> {CSV_PATH}")


if __name__ == "__main__":
    main()
