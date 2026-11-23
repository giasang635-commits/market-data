#!/usr/bin/env python3
"""
OHLCV / funding dumper for the backtest lab. READ-ONLY to exchanges.

- Bybit linear perps  -> via Squid proxy chain (Second primary, First fallback)
- Binance spot        -> direct via data-api.binance.vision public mirror
- Timeframes: 1h / 4h / 1d, full history via `since` pagination, incremental append + resume.

Layout:
  data/{exchange}/{SYMBOL}_{tf}.parquet   cols: ts(UTC ms int64), open, high, low, close, volume (float64)
  funding/{exchange}/{SYMBOL}.parquet     cols: ts(UTC ms int64), rate(float64)   [perp only]
  meta/{exchange}_markets.json            SLIM dump of SELECTED symbols only (tracked in git, <200KB)
  meta/{exchange}_markets.full.json       full ccxt load_markets() (gitignored, local only)
  manifest.json                           files, ranges, selected_symbols, updated_at

Symbol naming:
  Bybit linear perp  BTC/USDT:USDT -> BTC-USDT-PERP
  Binance spot       BTC/USDT      -> BTC-USDT

Selection: quote=USDT, active, NOT stable-base, NOT leveraged token; top-N by 24h quoteVolume.
"""
import os
import re
import sys
import json
import time
import argparse
from datetime import datetime, timezone

import ccxt
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
FUND = os.path.join(BASE, "funding")
META = os.path.join(BASE, "meta")
MANIFEST = os.path.join(BASE, "manifest.json")

# Squid proxy chain of the Kit bot (Second/Berlin primary, First/France fallback)
PROXIES = ["http://31.70.81.103:3128", "http://46.8.224.186:3128"]

TF_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
OHLCV_LIMIT = 1000
FUNDING_LIMIT = 200
MAX_RETRY = 4
# full-history floor (2017-01-01). since=0 makes Bybit return only the latest page, not history.
FULL_SINCE = 1_483_228_800_000

# base assets to exclude (stablecoins / pegged)
STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDE", "USD1", "PYUSD", "EURC",
    "USDP", "FRAX", "USDT", "USDD", "GUSD", "LUSD", "SUSD", "EURI", "EUR",
    "USTC", "UST", "XUSD", "AEUR", "USDR", "RLUSD",
}
# leveraged tokens: ...UP / ...DOWN / ...BULL / ...BEAR / ...3L / ...3S etc.
LEV_RE = re.compile(r"(UP|DOWN|BULL|BEAR)$|[2-5][LS]$")

# Bybit: info.symbolType of non-crypto instruments (tokenized stocks / ETFs / commodities)
NONCRYPTO_SYMBOLTYPES = {"stock", "ETF", "commodity"}

# Binance: tokenized-stock bases (all end in 'B', ticker cross-checked vs Bybit stock/ETF/commodity).
# Binance spot has no clean symbolType field, so this static (hand-reviewed) blocklist is used.
BINANCE_STOCK_BASES = {
    "AAOIB", "AAPLB", "ALABB", "AMATB", "AMZNB", "ARMB", "ASMLB", "ASTSB", "AVGOB",
    "AXTIB", "BABAB", "BEB", "BMNRB", "CBRSB", "COHRB", "COINB", "CRCLB", "CRDOB",
    "CRWVB", "DELLB", "DRAMB", "EWYB", "FLNCB", "GLWB", "GOOGLB", "GSB", "HOODB",
    "IBMB", "INTCB", "INTWB", "IRENB", "KORUB", "LITEB", "METAB", "MRVLB", "MSFTB",
    "MSTRB", "MUB", "MUUB", "MVLLB", "NBISB", "NFLXB", "NVDAB", "ORCLB", "PLTRB",
    "PYPLB", "QCOMB", "QQQB", "RKLBB", "SKHYB", "SMCIB", "SMHB", "SNDKB", "SNXXB",
    "SOXLB", "SOXSB", "SPCXB", "SPYB", "TQQQB", "TSLAB", "TSMB", "USARB", "WDCB",
}


def log(*a):
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}]", *a, flush=True)


def now_ms():
    return int(time.time() * 1000)


def g(d, *keys):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


# --------------------------------------------------------------------------- #
# exchange factory
# --------------------------------------------------------------------------- #
def make_exchange(name):
    if name == "bybit":
        ex = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
        ex.httpsProxy = PROXIES[0]
        ex._uses_proxy = True
    elif name == "binance":
        ex = ccxt.binance({
            "enableRateLimit": True,
            "options": {"defaultType": "spot", "fetchMarkets": ["spot"]},
        })
        # spot-only load_markets avoids fapi/dapi/eapi (451); route public /api/ to vision mirror
        for k, v in list(ex.urls["api"].items()):
            if isinstance(v, str):
                ex.urls["api"][k] = v.replace(
                    "https://api.binance.com/api/", "https://data-api.binance.vision/api/"
                )
        ex._uses_proxy = False
    else:
        raise ValueError(f"unknown exchange {name}")
    return ex


def call(ex, fn, *args, **kwargs):
    """Retry wrapper. For proxied exchanges cycles the proxy chain on network errors."""
    last = None
    for attempt in range(MAX_RETRY):
        try:
            return fn(*args, **kwargs)
        except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout,
                ccxt.DDoSProtection) as e:
            last = e
            if getattr(ex, "_uses_proxy", False):
                nxt = PROXIES[(attempt + 1) % len(PROXIES)]
                ex.httpsProxy = nxt
                log(f"    net err ({type(e).__name__}); switch proxy -> {nxt}")
            time.sleep(1.5 * (attempt + 1))
    raise last


# --------------------------------------------------------------------------- #
# symbol helpers
# --------------------------------------------------------------------------- #
def norm_symbol(name, market):
    base, quote = market["base"], market["quote"]
    if name == "bybit" and market.get("swap") and market.get("linear"):
        return f"{base}-{quote}-PERP"
    return f"{base}-{quote}"


def eligible(name, m):
    if not m.get("active"):
        return False
    if m.get("quote") != "USDT":
        return False
    base = m.get("base") or ""
    if base in STABLE_BASES or LEV_RE.search(base):
        return False
    if name == "bybit":
        st = (m.get("info") or {}).get("symbolType")
        if st in NONCRYPTO_SYMBOLTYPES:
            return False
        return bool(m.get("swap") and m.get("linear"))
    # binance spot
    if base in BINANCE_STOCK_BASES:
        return False
    return bool(m.get("spot"))


def resolve_symbol(name, markets, spec):
    """Resolve an extra-symbol spec to a ccxt unified symbol (type-aware).
    Bybit -> linear swap, Binance -> spot. Accepts ccxt symbol | market id | base."""
    def right_type(m):
        if name == "bybit":
            return bool(m.get("swap") and m.get("linear"))
        return bool(m.get("spot"))

    if spec in markets and right_type(markets[spec]):
        return spec
    cands = []
    for s, m in markets.items():
        if not (m.get("quote") == "USDT" and m.get("active") and right_type(m)):
            continue
        if s == spec or m.get("id") == spec or m.get("base") == spec:
            cands.append(s)
    return cands[0] if cands else None


def read_extra(name, markets):
    """extra_symbols.txt lines 'exchange:SPEC' -> forced-include ccxt symbols for this exchange."""
    path = os.path.join(BASE, "extra_symbols.txt")
    out = []
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            exn, spec = line.split(":", 1)
            if exn.strip() != name:
                continue
            sym = resolve_symbol(name, markets, spec.strip())
            if sym:
                out.append(sym)
            else:
                log(f"  extra: UNRESOLVED '{line}'")
    return out


def top_symbols(ex, name, n):
    markets = ex.load_markets()
    tickers = call(ex, ex.fetch_tickers)
    rows = []
    for sym, m in markets.items():
        if not eligible(name, m):
            continue
        t = tickers.get(sym)
        qv = (t or {}).get("quoteVolume") or 0
        rows.append((sym, qv))
    rows.sort(key=lambda x: -x[1])
    return [s for s, _ in rows[:n]]


def slim_market(m):
    return {
        "symbol": m.get("symbol"),
        "id": m.get("id"),
        "type": m.get("type"),
        "base": m.get("base"),
        "quote": m.get("quote"),
        "settle": m.get("settle"),
        "linear": m.get("linear"),
        "contractSize": m.get("contractSize"),
        "active": m.get("active"),
        "precision": {"price": g(m, "precision", "price"), "amount": g(m, "precision", "amount")},
        "limits": {
            "amount": {"min": g(m, "limits", "amount", "min"), "max": g(m, "limits", "amount", "max")},
            "cost": {"min": g(m, "limits", "cost", "min")},
        },
        "taker": m.get("taker"),
        "maker": m.get("maker"),
    }


# --------------------------------------------------------------------------- #
# fetch
# --------------------------------------------------------------------------- #
def fetch_ohlcv(ex, symbol, tf, since, until):
    out, cur, step = [], since, TF_MS[tf]
    while cur < until:
        batch = call(ex, ex.fetch_ohlcv, symbol, tf, cur, OHLCV_LIMIT)
        if not batch:
            break
        out.extend(batch)
        last = batch[-1][0]
        if len(batch) < OHLCV_LIMIT:
            break
        cur = last + step
    return out


def fetch_funding(ex, symbol, floor_ms):
    """Backward pagination via `until` (Bybit keeps funding history in a bounded window;
    a far-past `since` returns empty, so we walk back from now down to floor_ms)."""
    out, end = [], now_ms()
    while True:
        batch = call(ex, ex.fetch_funding_rate_history, symbol, None, FUNDING_LIMIT, {"until": end})
        if not batch:
            break
        batch = sorted(batch, key=lambda r: int(r["timestamp"]))
        out = [[int(r["timestamp"]), float(r["fundingRate"])]
               for r in batch if int(r["timestamp"]) >= floor_ms] + out
        first = int(batch[0]["timestamp"])
        if first <= floor_ms or len(batch) < FUNDING_LIMIT:
            break
        end = first - 1
    return out


# --------------------------------------------------------------------------- #
# parquet io + resume
# --------------------------------------------------------------------------- #
def resume_since(path, step, default_since):
    """Continue from last stored ts (+step) for incremental/resume; else default."""
    if os.path.exists(path):
        try:
            df = pd.read_parquet(path)
            if not df.empty:
                return int(df["ts"].iloc[-1]) + step
        except Exception:
            pass
    return default_since


def write_ohlcv(path, rows):
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df = df.astype({"ts": "int64", "open": "float64", "high": "float64",
                    "low": "float64", "close": "float64", "volume": "float64"})
    if os.path.exists(path):
        df = pd.concat([pd.read_parquet(path), df], ignore_index=True)
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    return df


def write_funding(path, rows):
    df = pd.DataFrame(rows, columns=["ts", "rate"])
    if not df.empty:
        df = df.astype({"ts": "int64", "rate": "float64"})
    if os.path.exists(path):
        df = pd.concat([pd.read_parquet(path), df], ignore_index=True)
    if df.empty:
        return df
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    return df


# --------------------------------------------------------------------------- #
# manifest
# --------------------------------------------------------------------------- #
def load_manifest():
    if os.path.exists(MANIFEST):
        with open(MANIFEST) as f:
            return json.load(f)
    return {"files": {}, "selected_symbols": {}}


def save_manifest(man):
    man["updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(MANIFEST, "w") as f:
        json.dump(man, f, indent=2)


def manifest_entry(df, kind):
    if df is None or df.empty:
        return {"kind": kind, "rows": 0, "first_ts": None, "last_ts": None}
    return {
        "kind": kind,
        "rows": int(len(df)),
        "first_ts": int(df["ts"].iloc[0]),
        "last_ts": int(df["ts"].iloc[-1]),
        "first_utc": datetime.fromtimestamp(df["ts"].iloc[0] / 1000, timezone.utc).isoformat(),
        "last_utc": datetime.fromtimestamp(df["ts"].iloc[-1] / 1000, timezone.utc).isoformat(),
    }


STALE_MS = 3 * 86_400_000  # symbol dropped from top-50 and not updated >3 days -> stale


def parse_rel(rel):
    """data/bybit/BTC-USDT-PERP_1h.parquet -> (bybit, BTC-USDT-PERP); funding/... -> (ex, nsym)."""
    parts = rel.split(os.sep)
    if len(parts) < 3:
        return None, None
    exch, fname = parts[1], parts[-1]
    stem = fname[:-len(".parquet")] if fname.endswith(".parquet") else fname
    nsym = stem.rsplit("_", 1)[0] if parts[0] == "data" else stem
    return exch, nsym


def mark_stale(man):
    """Flag files whose symbol left the current top-50 and hasn't updated in >3 days.
    Files are kept (not deleted); only the manifest carries stale=true."""
    now = now_ms()
    for rel, entry in man["files"].items():
        exch, nsym = parse_rel(rel)
        selected = man.get("selected_symbols", {}).get(exch, [])
        last = entry.get("last_ts")
        if nsym in selected:
            entry.pop("stale", None)
        elif last and (now - last) > STALE_MS:
            entry["stale"] = True
        else:
            entry.pop("stale", None)


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def run(exchanges, timeframes, days, top, smoke=False, only_meta=False):
    man = load_manifest()
    man.setdefault("selected_symbols", {})
    man.setdefault("files", {})
    man["run_started_at"] = datetime.now(timezone.utc).isoformat()
    until = now_ms()
    default_since = (until - days * 86_400_000) if days else FULL_SINCE

    for name in exchanges:
        ex = make_exchange(name)
        log(f"=== {name} : load_markets ===")
        markets = ex.load_markets()

        # ----- select symbols -----
        if smoke:
            picks = top_symbols(ex, name, 300)
            btc = next((s for s in picks if markets[s]["base"] == "BTC"), None)
            eth = next((s for s in picks if markets[s]["base"] == "ETH"), None)
            alt = next((s for s in picks if markets[s]["base"] not in ("BTC", "ETH")), None)
            syms = [s for s in (btc, eth, alt) if s]
        else:
            syms = top_symbols(ex, name, top)
        # forced extra symbols (extra_symbols.txt), deduped, appended
        for s in read_extra(name, markets):
            if s not in syms:
                syms.append(s)
        nsyms = [norm_symbol(name, markets[s]) for s in syms]
        man["selected_symbols"][name] = nsyms
        log(f"  selected ({len(syms)}): {nsyms}")

        # ----- meta: full (gitignored) + slim (tracked) -----
        os.makedirs(META, exist_ok=True)
        with open(os.path.join(META, f"{name}_markets.full.json"), "w") as f:
            json.dump(markets, f, default=str)
        slim = {norm_symbol(name, markets[s]): slim_market(markets[s]) for s in syms}
        slim_path = os.path.join(META, f"{name}_markets.json")
        with open(slim_path, "w") as f:
            json.dump(slim, f, indent=2, default=str)
        log(f"  meta slim -> {slim_path} ({os.path.getsize(slim_path)} bytes)")

        if only_meta:
            save_manifest(man)
            continue

        # ----- fetch -----
        for sym in syms:
            m = markets[sym]
            nsym = norm_symbol(name, m)
            for tf in timeframes:
                path = os.path.join(DATA, name, f"{nsym}_{tf}.parquet")
                rel = os.path.relpath(path, BASE)
                since = resume_since(path, TF_MS[tf], default_since)
                rows = fetch_ohlcv(ex, sym, tf, since, until) if since < until else []
                df = write_ohlcv(path, rows) if rows else (
                    pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame())
                man["files"][rel] = manifest_entry(df, "ohlcv")
                log(f"  ohlcv {nsym} {tf}: +{len(rows)} -> {len(df)} rows")

            if m.get("swap"):
                fpath = os.path.join(FUND, name, f"{nsym}.parquet")
                frel = os.path.relpath(fpath, BASE)
                fsince = resume_since(fpath, 1, default_since)
                try:
                    frows = fetch_funding(ex, sym, fsince)
                    fdf = write_funding(fpath, frows) if frows else (
                        pd.read_parquet(fpath) if os.path.exists(fpath) else pd.DataFrame())
                    man["files"][frel] = manifest_entry(fdf, "funding")
                    log(f"  funding {nsym}: +{len(frows)} -> {len(fdf)} rows")
                except Exception as e:
                    log(f"  funding {nsym}: SKIP ({type(e).__name__}: {e})")

            save_manifest(man)  # checkpoint per symbol

    mark_stale(man)
    man["run_finished_at"] = datetime.now(timezone.utc).isoformat()
    save_manifest(man)
    log("done.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exchanges", default="bybit,binance")
    ap.add_argument("--timeframes", default="1h,4h,1d")
    ap.add_argument("--days", type=int, default=0, help="0 = full history")
    ap.add_argument("--top", type=int, default=50)
    ap.add_argument("--smoke", action="store_true", help="BTC, ETH + 1 alt, 1h, 30d")
    ap.add_argument("--list", dest="list_only", action="store_true",
                    help="select symbols + write meta only, no OHLCV fetch")
    a = ap.parse_args()

    if a.smoke:
        run(a.exchanges.split(","), ["1h"], 30, a.top, smoke=True)
    elif a.list_only:
        run(a.exchanges.split(","), a.timeframes.split(","), a.days, a.top, only_meta=True)
    else:
        run(a.exchanges.split(","), a.timeframes.split(","), a.days, a.top)


if __name__ == "__main__":
    main()
