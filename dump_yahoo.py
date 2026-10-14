#!/usr/bin/env python3
"""
TradFi 1d dumper for the backtest lab (inverse-pairs / macro overlays). READ-ONLY.

Sources:
  - Yahoo Finance via yfinance  -> data/yahoo/<TICKER>_1d.parquet
  - FRED public CSV (no key)     -> data/fred/<SERIES>.parquet
    endpoint: https://fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES>

Contract:
  yahoo cols: [ts_ms:int64 (UTC ms), open, high, low, close, volume : float64]
  fred  cols: [ts_ms:int64 (UTC ms), value : float64]
  - ts unique, ascending
  - NaN kept as NaN (gaps/holidays/missing are marked, NOT forward-filled)
  - daily bars stamped at 00:00:00 UTC of the observation date

Range: 2004-01-01 -> now (yfinance may return earlier history; we keep whatever it gives >= floor).
Timeframe: 1d only.
"""
import os
import sys
import json
import time
import io
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests
import yfinance as yf

BASE = os.path.dirname(os.path.abspath(__file__))
YDIR = os.path.join(BASE, "data", "yahoo")
FDIR = os.path.join(BASE, "data", "fred")
PROOF = os.path.join(BASE, "yahoo_proof.json")

START = "2004-01-01"
FLOOR_MS = int(datetime(2004, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

# requested Yahoo tickers (^VVIX is best-effort / optional)
YAHOO = [
    "^GSPC", "^VIX", "^VIX3M", "TLT", "IEF", "GLD", "SLV", "TIP",
    "DX-Y.NYB", "^TNX", "CL=F", "USO", "JETS", "UUP", "EEM", "EMLC",
    "JPY=X", "USDJPY=X", "SQQQ", "PSQ", "SH", "VXX", "^MOVE", "^VVIX",
]
# ^VVIX best-effort; ^VIX3M has NO Yahoo history (dead symbol, ^VXV also dead)
# -> substituted by FRED VXVCLS (CBOE 3-Month Volatility, same underlying).
OPTIONAL = {"^VVIX", "^VIX3M"}

# rows below this = degenerate/broken source response (real series have 1000s)
MIN_ROWS = 100

# FRED series (public CSV, no API key required)
# VXVCLS = 3-month CBOE vol, substitute for the unavailable Yahoo ^VIX3M.
# NOTE: BAMLH0A0HYM2 (ICE BofA HY OAS) is license-limited to a ~3y rolling window.
FRED = [
    "DFII10", "T10YIE", "DGS10", "BAMLH0A0HYM2",
    "WALCL", "WTREGEN", "RRPONTSYD", "VXVCLS",
]

MAX_RETRY = 4


def log(*a):
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}]", *a, flush=True)


def utc_iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# yahoo
# --------------------------------------------------------------------------- #
def fetch_yahoo(ticker):
    """Return DataFrame [ts_ms, open, high, low, close, volume] or None on hard failure."""
    last = None
    for attempt in range(MAX_RETRY):
        try:
            # period="max" not start=: yfinance's start= is broken for the
            # Treasury-yield index family (^TNX/^FVX/^IRX/^TYX -> only ~19 rows).
            # max returns full history; FLOOR_MS trims to >= 2004 below.
            df = yf.download(
                ticker, period="max", interval="1d",
                auto_adjust=False, actions=False, progress=False, threads=False,
            )
            if df is None or df.empty:
                raise ValueError("empty frame")
            # flatten possible MultiIndex columns (('Close','^GSPC') -> 'Close')
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.rename(columns=str.lower)
            # index -> UTC midnight ms
            # pandas 3.0 keeps yfinance's second-resolution dtype; force ns before view
            idx = pd.DatetimeIndex(pd.to_datetime(df.index, utc=True)).as_unit("ns")
            ts_ms = (idx.view("int64") // 1_000_000).astype("int64")
            out = pd.DataFrame({
                "ts_ms": ts_ms,
                "open": pd.to_numeric(df.get("open"), errors="coerce"),
                "high": pd.to_numeric(df.get("high"), errors="coerce"),
                "low": pd.to_numeric(df.get("low"), errors="coerce"),
                "close": pd.to_numeric(df.get("close"), errors="coerce"),
                "volume": pd.to_numeric(df.get("volume"), errors="coerce"),
            })
            out = out[out["ts_ms"] >= FLOOR_MS]
            out = out.drop_duplicates("ts_ms").sort_values("ts_ms").reset_index(drop=True)
            out = out.astype({"ts_ms": "int64", "open": "float64", "high": "float64",
                              "low": "float64", "close": "float64", "volume": "float64"})
            return out
        except Exception as e:
            last = e
            log(f"    {ticker} attempt {attempt+1} err: {type(e).__name__}: {e}")
            time.sleep(2.0 * (attempt + 1))
    log(f"  {ticker}: FAILED after {MAX_RETRY} tries ({last})")
    return None


# --------------------------------------------------------------------------- #
# fred
# --------------------------------------------------------------------------- #
def fetch_fred(series):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
    last = None
    for attempt in range(MAX_RETRY):
        try:
            # urllib hangs on FRED's TLS read on this host; requests is reliable
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=45)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            # first col is the date (observation_date / DATE), second is the value
            date_col, val_col = df.columns[0], df.columns[1]
            idx = pd.DatetimeIndex(pd.to_datetime(df[date_col], utc=True)).as_unit("ns")
            ts_ms = (idx.view("int64") // 1_000_000).astype("int64")
            # FRED marks missing values as '.'
            val = pd.to_numeric(df[val_col].replace(".", np.nan), errors="coerce")
            out = pd.DataFrame({"ts_ms": np.asarray(ts_ms), "value": val.values})
            out = out[out["ts_ms"] >= FLOOR_MS]
            out = out.drop_duplicates("ts_ms").sort_values("ts_ms").reset_index(drop=True)
            out = out.astype({"ts_ms": "int64", "value": "float64"})
            return out
        except Exception as e:
            last = e
            log(f"    {series} attempt {attempt+1} err: {type(e).__name__}: {e}")
            time.sleep(2.0 * (attempt + 1))
    log(f"  {series}: FAILED after {MAX_RETRY} tries ({last})")
    return None


# --------------------------------------------------------------------------- #
# proof
# --------------------------------------------------------------------------- #
def proof_entry(df, value_col="close"):
    head = df.head(3).copy()
    return {
        "rows": int(len(df)),
        "first_utc": utc_iso(int(df["ts_ms"].iloc[0])),
        "last_utc": utc_iso(int(df["ts_ms"].iloc[-1])),
        "last_close": (None if pd.isna(df[value_col].iloc[-1]) else float(df[value_col].iloc[-1])),
        "nan_rows": {c: int(df[c].isna().sum()) for c in df.columns if c != "ts_ms"},
        "head3": [
            {c: (None if (isinstance(v, float) and pd.isna(v)) else
                 (int(v) if c == "ts_ms" else float(v)))
             for c, v in row.items()}
            for row in head.to_dict("records")
        ],
    }


def main():
    os.makedirs(YDIR, exist_ok=True)
    os.makedirs(FDIR, exist_ok=True)
    proof = {"generated_at": datetime.now(timezone.utc).isoformat(),
             "range_start": START, "yahoo": {}, "fred": {}, "failed": []}

    log(f"=== YAHOO ({len(YAHOO)} tickers) ===")
    for t in YAHOO:
        df = fetch_yahoo(t)
        if df is None or df.empty:
            if t in OPTIONAL:
                log(f"  {t}: skipped (optional, unavailable)")
            else:
                log(f"  {t}: FAILED (empty/unreachable)")
                proof["failed"].append(t)
            continue
        if len(df) < MIN_ROWS:
            if t in OPTIONAL:
                log(f"  {t}: skipped (only {len(df)} rows, source has no history)")
            else:
                log(f"  {t}: FAILED (degenerate {len(df)} rows)")
                proof["failed"].append(t)
            continue
        path = os.path.join(YDIR, f"{t}_1d.parquet")
        df.to_parquet(path, index=False)
        proof["yahoo"][t] = proof_entry(df, "close")
        e = proof["yahoo"][t]
        log(f"  {t}: {e['rows']} rows {e['first_utc'][:10]}..{e['last_utc'][:10]} "
            f"last_close={e['last_close']}")
        time.sleep(1.5)  # throttle to avoid Yahoo rate-limit (silent empty frames)

    log(f"=== FRED ({len(FRED)} series) ===")
    for s in FRED:
        df = fetch_fred(s)
        if df is None or df.empty:
            proof["failed"].append(s)
            continue
        path = os.path.join(FDIR, f"{s}.parquet")
        df.to_parquet(path, index=False)
        proof["fred"][s] = proof_entry(df, "value")
        e = proof["fred"][s]
        log(f"  {s}: {e['rows']} rows {e['first_utc'][:10]}..{e['last_utc'][:10]} "
            f"last={e['last_close']}")

    with open(PROOF, "w") as f:
        json.dump(proof, f, indent=2)
    log(f"proof -> {PROOF}")
    if proof["failed"]:
        log(f"FAILED (non-optional): {proof['failed']}")
    log("done.")


if __name__ == "__main__":
    main()
