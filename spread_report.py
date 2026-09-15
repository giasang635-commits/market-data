#!/usr/bin/env python3
"""
Weekly report over the Bybit-vs-Hyperliquid funding-spread log. READ-ONLY. No trading.

Reads data/spread/bybit_hl_spread.csv, spread = bybit_rate_8h - hl_rate_8h_equiv (8h-equiv
fraction). Window defaults to last 7 days (--days N, 0 = all).

Per symbol computes:
  n_hours              rows in window
  mean_signed_spread   mean(bybit - hl)
  mean_abs_spread      mean(|bybit - hl|)
  pct_hours_gt_1bp     % of hours with |spread| > 0.0001 (0.01%/8h-equiv)
  mean_sign_run_h      mean run length (hours) of one spread sign (rows are hourly)

Outputs:
  - prints a numeric table + BTC/ETH "strategy alive" verdict
  - writes data/spread/report_<window>.csv  (per-symbol numbers, no prose)

ALIVE criterion (per task): BTC and ETH each need
  |mean_signed_spread| >= 0.0001  AND  mean_sign_run_h >= 48
Anything less = buried, by the numbers.
"""
import os
import csv
import sys
import argparse
from datetime import datetime, timezone

import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE, "data", "spread", "bybit_hl_spread.csv")
OUT_DIR = os.path.join(BASE, "data", "spread")

THRESH = 0.0001   # 0.01% per 8h-equiv
RUN_MIN_H = 48.0


def mean_sign_run(signs):
    """Mean length (in rows == hours) of consecutive same-sign runs. Zeros break runs
    and are excluded from run lengths."""
    runs, cur, cur_sign = [], 0, None
    for s in signs:
        if s == 0:
            if cur:
                runs.append(cur)
            cur, cur_sign = 0, None
            continue
        if s == cur_sign:
            cur += 1
        else:
            if cur:
                runs.append(cur)
            cur, cur_sign = 1, s
    if cur:
        runs.append(cur)
    return (sum(runs) / len(runs)) if runs else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7, help="lookback window in days (0 = all)")
    a = ap.parse_args()

    if not os.path.exists(CSV_PATH):
        print(f"no data: {CSV_PATH}")
        sys.exit(1)
    df = pd.read_csv(CSV_PATH)
    if df.empty:
        print("empty CSV")
        sys.exit(1)

    df["bybit_rate_8h"] = df["bybit_rate_8h"].astype(float)
    df["hl_rate_8h_equiv"] = df["hl_rate_8h_equiv"].astype(float)
    df["spread"] = df["bybit_rate_8h"] - df["hl_rate_8h_equiv"]

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if a.days > 0:
        cutoff = now_ms - a.days * 86_400_000
        df = df[df["ts_ms"] >= cutoff]
    if df.empty:
        print("no rows in window")
        sys.exit(1)

    win_first = datetime.fromtimestamp(int(df["ts_ms"].min()) / 1000, timezone.utc).isoformat()
    win_last = datetime.fromtimestamp(int(df["ts_ms"].max()) / 1000, timezone.utc).isoformat()

    recs = []
    for sym, g in df.sort_values("ts_ms").groupby("symbol"):
        sp = g["spread"].to_numpy()
        signs = [(1 if x > 0 else (-1 if x < 0 else 0)) for x in sp]
        recs.append({
            "symbol": sym,
            "n_hours": int(len(sp)),
            "mean_signed_spread": round(float(sp.mean()), 10),
            "mean_abs_spread": round(float(abs(sp).mean()), 10),
            "pct_hours_gt_1bp": round(100.0 * (abs(sp) > THRESH).mean(), 2),
            "mean_sign_run_h": round(mean_sign_run(signs), 2),
        })
    rep = pd.DataFrame(recs)
    rep["abs_mean"] = rep["mean_signed_spread"].abs()
    rep = rep.sort_values("abs_mean", ascending=False).drop(columns="abs_mean").reset_index(drop=True)

    win_tag = f"{a.days}d" if a.days > 0 else "all"
    out_csv = os.path.join(OUT_DIR, f"report_{win_tag}.csv")
    rep.to_csv(out_csv, index=False)

    pd.set_option("display.width", 160)
    pd.set_option("display.max_columns", 20)
    print(f"window: {win_first} .. {win_last}  ({win_tag}, rows={len(df)})")
    print(rep.to_string(index=False))
    print("\nTOP-5 by |mean signed spread|:")
    print(rep.head(5)[["symbol", "mean_signed_spread", "mean_abs_spread",
                       "pct_hours_gt_1bp", "mean_sign_run_h"]].to_string(index=False))

    print("\nALIVE check (|mean signed|>=1e-4 AND mean_sign_run_h>=48):")
    verdict_all_alive = True
    for sym in ("BTC", "ETH"):
        r = rep[rep["symbol"] == sym]
        if r.empty:
            print(f"  {sym}: NO DATA")
            verdict_all_alive = False
            continue
        r = r.iloc[0]
        alive = abs(r["mean_signed_spread"]) >= THRESH and r["mean_sign_run_h"] >= RUN_MIN_H
        verdict_all_alive &= alive
        print(f"  {sym}: signed={r['mean_signed_spread']:.6f} run={r['mean_sign_run_h']:.1f}h "
              f"-> {'ALIVE' if alive else 'BURIED'}")
    print(f"\nVERDICT: strategy {'ALIVE' if verdict_all_alive else 'BURIED'}")
    print(f"report -> {out_csv}")


if __name__ == "__main__":
    main()
