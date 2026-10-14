#!/usr/bin/env python3
"""
Funding-rate dumper for carry backtests (Bybit linear perps). READ-ONLY to exchange.

Symbols: the whole backtest universe = every Bybit linear perp that already has an OHLCV
         parquet in data/bybit/ (derived from *_1h.parquet stems). 59 perps as of dump.
Range  : from listing (walks back until the funding endpoint runs out; floor 2020-01-01)
         .. now. Funding interval per symbol (8h / 4h / 1h) is taken from the market's
         info.fundingInterval (minutes) and recorded in meta/bybit_funding_meta.json.
Output : data/bybit/funding/<symbol>_funding.parquet
         cols: ts_ms:int64 (UTC), symbol:str, funding_rate:float (fraction/interval),
               mark_price:float
Contract: no duplicate ts, sorted by ts, no NaN.

Funding history endpoint carries only (ts, fundingRate) -> mark_price is joined from
Bybit v5 mark-price-kline (4h). Funding ts are multiples of the funding interval; the
nearest 4h mark boundary at/earlier than each funding ts gives the mark price, with a
Binance-close backfill for the pre-mark-kline gap.

ANNUALIZATION NOTE: proof reports BOTH `ann_x3x365` (naive mean*3*365, assumes 8h) and
`ann_by_interval` (mean * (1440/interval_min) * 365, the correct factor). For 4h perps
the naive x3x365 UNDER-counts by 2x; use ann_by_interval.
"""
import os
import re
import json
import time
import glob
from datetime import datetime, timezone

import pandas as pd

import bisect

from dump_ohlcv import make_exchange, call, norm_symbol, now_ms, log

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE, "data", "bybit", "funding")
META_DIR = os.path.join(BASE, "meta")

FLOOR_MS = 1_577_836_800_000          # 2020-01-01T00:00:00Z (linear perps began 2021; safe floor)
FUNDING_LIMIT = 200
MARK_LIMIT = 1000
MARK_STEP_MS = 4 * 3_600_000          # 4h mark klines


def build_universe(markets):
    """Universe = every Bybit linear perp that already has an OHLCV parquet in data/bybit/.
    Returns [(ccxt_symbol, nsym, funding_interval_min), ...] sorted by nsym."""
    rev = {}
    for s, m in markets.items():
        if m.get("swap") and m.get("linear") and m.get("quote") == "USDT":
            rev[norm_symbol("bybit", m)] = s
    nsyms = sorted({
        re.sub(r"_1h$", "", os.path.basename(p)[:-len(".parquet")])
        for p in glob.glob(os.path.join(BASE, "data", "bybit", "*_1h.parquet"))
    })
    out = []
    for nsym in nsyms:
        sym = rev.get(nsym)
        if not sym:
            log(f"  universe: UNRESOLVED {nsym} (no live linear market), skip")
            continue
        iv = (markets[sym].get("info") or {}).get("fundingInterval")
        iv = int(iv) if iv is not None else None
        out.append((sym, nsym, iv))
    return out


def fetch_funding(ex, symbol, floor_ms):
    """Backward pagination via `until` down to floor_ms. -> [[ts_ms, rate], ...] sorted asc."""
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


def fetch_mark_4h(ex, market_id, floor_ms):
    """Bybit v5 mark-price-kline (interval=240). Backward pagination via `end`.
    -> dict ts_ms -> open price (mark price at that 4h boundary).
    Stops only when the endpoint runs out (oldest stops decreasing) or floor reached,
    so a short mid-history page never aborts pagination early."""
    marks, end, prev_oldest = {}, now_ms(), None
    while True:
        r = call(ex, ex.publicGetV5MarketMarkPriceKline, {
            "category": "linear", "symbol": market_id,
            "interval": "240", "end": str(end), "limit": str(MARK_LIMIT),
        })
        lst = (r.get("result") or {}).get("list") or []
        if not lst:
            break
        for row in lst:  # [start, open, high, low, close], newest first
            marks[int(row[0])] = float(row[1])
        oldest = min(int(row[0]) for row in lst)
        if oldest <= floor_ms or oldest == prev_oldest:
            break
        prev_oldest = oldest
        end = oldest - 1
    return marks


def fetch_binance_close_4h(base, since, until):
    """Real market price to backfill the pre-mark-kline gap (Bybit keeps funding history
    deeper than mark-price-kline for some symbols). Binance spot <base>/USDT 4h close.
    -> dict ts_ms -> close. Empty {} if the pair is unavailable."""
    try:
        bn = make_exchange("binance")
        bn.load_markets()
        sym = f"{base}/USDT"
        if sym not in bn.markets:
            return {}
        out, cur, step = {}, since, MARK_STEP_MS
        while cur < until:
            batch = call(bn, bn.fetch_ohlcv, sym, "4h", cur, 1000)
            if not batch:
                break
            for row in batch:
                out[int(row[0])] = float(row[4])
            last = batch[-1][0]
            if len(batch) < 1000:
                break
            cur = last + step
        return out
    except Exception as e:
        log(f"    binance backfill {base}: {type(e).__name__}: {e}")
        return {}


def join_mark(frows, price_map):
    """Attach a price to each funding row from a merged price map (Bybit mark, authoritative,
    overlaid on Binance-close gap backfill). Exact 4h-boundary match; else nearest earlier,
    else nearest later. Guarantees no NaN as long as price_map is non-empty."""
    ts_sorted = sorted(price_map.keys())
    out = []
    for ts, rate in frows:
        if ts in price_map:
            mp = price_map[ts]
        else:
            j = bisect.bisect_right(ts_sorted, ts) - 1
            if j >= 0:
                mp = price_map[ts_sorted[j]]
            else:
                mp = price_map[ts_sorted[0]]
        out.append([ts, rate, mp])
    return out


def write_parquet(path, nsym, rows):
    df = pd.DataFrame(rows, columns=["ts_ms", "funding_rate", "mark_price"])
    df.insert(1, "symbol", nsym)
    df = df.astype({"ts_ms": "int64", "symbol": "string",
                    "funding_rate": "float64", "mark_price": "float64"})
    df = df.drop_duplicates("ts_ms").sort_values("ts_ms").reset_index(drop=True)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    return df


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def main():
    ex = make_exchange("bybit")
    log("load_markets + build universe from data/bybit/*_1h.parquet")
    markets = ex.load_markets()
    universe = build_universe(markets)
    log(f"universe: {len(universe)} perps")

    proofs = {}
    meta = {}
    for sym, nsym, iv_min in universe:
        m = markets[sym]
        mid = m["id"]
        log(f"=== {nsym} ({mid}) interval={iv_min}m ===")
        frows = fetch_funding(ex, sym, FLOOR_MS)
        if not frows:
            log(f"  no funding rows, skip")
            continue
        marks = fetch_mark_4h(ex, mid, frows[0][0])
        # backfill the pre-mark-kline gap (funding deeper than mark history) with Binance close
        earliest_mark = min(marks) if marks else None
        price_map = {}
        if earliest_mark is None or frows[0][0] < earliest_mark:
            gap_until = earliest_mark if earliest_mark is not None else now_ms()
            bn = fetch_binance_close_4h(m["base"], frows[0][0], gap_until)
            if bn:
                price_map.update(bn)
                log(f"  binance backfill {m['base']}: {len(bn)} pts "
                    f"< {iso(gap_until)[:10] if earliest_mark else 'now'}")
        price_map.update(marks)  # Bybit mark is authoritative where present
        if not price_map:
            log(f"  WARN no price source for {nsym}; mark_price would be NaN, skip")
            continue
        rows = join_mark(frows, price_map)
        path = os.path.join(OUT_DIR, f"{nsym}_funding.parquet")
        df = write_parquet(path, nsym, rows)

        assert df["ts_ms"].is_monotonic_increasing, "ts not sorted"
        assert not df["ts_ms"].duplicated().any(), "dup ts"
        assert not df.isnull().values.any(), "NaN present"

        mean = float(df["funding_rate"].mean())
        ann_naive = mean * 3 * 365
        per_day = (1440 / iv_min) if iv_min else 3
        ann_iv = mean * per_day * 365
        # realized carry (interval-agnostic): total funding / elapsed years. Robust to symbols
        # whose funding interval CHANGED over history (e.g. 1h at listing -> 4h later).
        diffs_min = df["ts_ms"].diff().dropna() / 60_000.0
        med_iv = float(diffs_min.median()) if len(diffs_min) else (iv_min or 0)
        span_days = (int(df["ts_ms"].iloc[-1]) - int(df["ts_ms"].iloc[0])) / 86_400_000
        span_years = span_days / 365.25 if span_days > 0 else None
        ann_realized = float(df["funding_rate"].sum() / span_years) if span_years else None
        meta[nsym] = {
            "funding_interval_min": iv_min,
            "funding_interval_h": round(iv_min / 60, 3) if iv_min else None,
            "periods_per_day": per_day,
            "median_interval_min": round(med_iv, 1),
            "variable_interval": bool(iv_min and abs(med_iv - iv_min) > 1),
        }
        proofs[nsym] = {
            "rows": int(len(df)),
            "first_utc": iso(int(df["ts_ms"].iloc[0])),
            "last_utc": iso(int(df["ts_ms"].iloc[-1])),
            "funding_interval_min": iv_min,
            "median_interval_min": round(med_iv, 1),
            "mean_rate": round(mean, 10),
            "ann_x3x365": round(ann_naive, 6),
            "ann_by_interval": round(ann_iv, 6),
            "ann_realized": round(ann_realized, 6) if ann_realized is not None else None,
            "head5": df.head(5).to_dict("records"),
        }
        log(f"  {nsym}: {len(df)} rows  {iso(int(df['ts_ms'].iloc[0]))} .. "
            f"{iso(int(df['ts_ms'].iloc[-1]))}  x3x365={ann_naive:.4%}  byIv={ann_iv:.4%}  "
            + (f"realized={ann_realized:.4%}" if ann_realized is not None else ""))

    os.makedirs(META_DIR, exist_ok=True)
    with open(os.path.join(META_DIR, "bybit_funding_meta.json"), "w") as f:
        json.dump(meta, f, indent=2, default=str)
    with open(os.path.join(BASE, "funding_carry_proof.json"), "w") as f:
        json.dump(proofs, f, indent=2, default=str)
    log(f"done. {len(proofs)} symbols. proof -> funding_carry_proof.json, "
        f"meta -> meta/bybit_funding_meta.json")


if __name__ == "__main__":
    main()
