#!/usr/bin/env python3
"""
Funding-rate dumper for carry backtests (Bybit linear perps). READ-ONLY to exchange.

Symbols: BTC-USDT-PERP, ETH-USDT-PERP + top-10 Bybit linear by 24h quoteVolume (deduped).
Range  : 2021-01-01 .. now, 8h funding rate.
Output : data/bybit/funding/<symbol>_funding.parquet
         cols: ts_ms:int64 (UTC), symbol:str, funding_rate:float (fraction/interval),
               mark_price:float
Contract: no duplicate ts, sorted by ts, no NaN.

Funding history endpoint carries only (ts, fundingRate) -> mark_price is joined from
Bybit v5 mark-price-kline (4h). Funding ts are multiples of 4h, so the 4h candle whose
OPEN == funding ts gives the mark price at settlement; missing points forward-fill.
"""
import os
import json
import time
from datetime import datetime, timezone

import pandas as pd

import bisect

from dump_ohlcv import make_exchange, call, top_symbols, norm_symbol, now_ms, log

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE, "data", "bybit", "funding")

FLOOR_MS = 1_609_459_200_000          # 2021-01-01T00:00:00Z
FUNDING_LIMIT = 200
MARK_LIMIT = 1000
MARK_STEP_MS = 4 * 3_600_000          # 4h mark klines
CORE = ["BTC/USDT:USDT", "ETH/USDT:USDT"]


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
    log("load_markets + top-10 by volume")
    markets = ex.load_markets()
    top = top_symbols(ex, "bybit", 10)
    syms = list(CORE) + [s for s in top if s not in CORE]

    proofs = {}
    for sym in syms:
        m = markets[sym]
        nsym = norm_symbol("bybit", m)
        mid = m["id"]
        log(f"=== {nsym} ({mid}) ===")
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
        rows = join_mark(frows, price_map)
        path = os.path.join(OUT_DIR, f"{nsym}_funding.parquet")
        df = write_parquet(path, nsym, rows)

        assert df["ts_ms"].is_monotonic_increasing, "ts not sorted"
        assert not df["ts_ms"].duplicated().any(), "dup ts"
        assert not df.isnull().values.any(), "NaN present"

        ann = float(df["funding_rate"].mean() * 3 * 365)
        proofs[nsym] = {
            "rows": int(len(df)),
            "first_utc": iso(int(df["ts_ms"].iloc[0])),
            "last_utc": iso(int(df["ts_ms"].iloc[-1])),
            "annualized_mean": round(ann, 6),
            "head5": df.head(5).to_dict("records"),
        }
        log(f"  {nsym}: {len(df)} rows  {iso(int(df['ts_ms'].iloc[0]))} .. "
            f"{iso(int(df['ts_ms'].iloc[-1]))}  ann={ann:.4%}")

    with open(os.path.join(BASE, "funding_carry_proof.json"), "w") as f:
        json.dump(proofs, f, indent=2, default=str)
    log("done. proof -> funding_carry_proof.json")


if __name__ == "__main__":
    main()
