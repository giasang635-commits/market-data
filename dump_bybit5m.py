#!/usr/bin/env python3
"""
One-off (NOT in daily cron): 5m Bybit linear klines over event windows.

Window sources:
  1) kit signals: each "PUMP DETECTED: SYM" line in data/kit/kit_raw.jsonl
     (date field = UTC) -> [date-6h, date+72h].
  2) 1h repo data from 2026-06-23..now: each hour where
     close/close(24h ago)-1 >= 10%  AND  sum(volume*close) over trailing 24h >= 3e6
     -> [hour-6h, hour+72h].
Overlapping windows of the same symbol are merged.

Output: data/bybit5m/<SYM>-USDT-PERP_5m.parquet  (cols: ts,open,high,low,close,volume)
Kline pulled via Bybit v5 public endpoint through the same proxy chain as the dumper.
"""
import os, re, glob, json, time
from datetime import datetime, timezone

import requests
import pandas as pd

import dump_ohlcv as D

STEP_5M = 300_000
TF5 = "5"                         # bybit v5 interval for 5m
W_PRE = 6 * 3_600_000            # 6h before
W_POST = 72 * 3_600_000         # 72h after
RET_MIN = 0.10
TURNOVER_MIN = 3_000_000
SRC2_FROM = int(datetime(2026, 6, 23, tzinfo=timezone.utc).timestamp() * 1000)
OUTDIR = os.path.join(D.DATA, "bybit5m")
PX_LIST = D.PROXIES


def log(*a):
    D.log(*a)


def base_from_bybit_sym(s):
    # "DEXEUSDT" -> "DEXE", "1000PEPEUSDT" -> "1000PEPE"
    return s[:-4] if s.endswith("USDT") else s


# --------------------------------------------------------------------------- #
# window collection
# --------------------------------------------------------------------------- #
def windows_from_kit(path):
    """return dict base -> list[(start,end)] ; and raw count"""
    out = {}
    n = 0
    pat = re.compile(r"PUMP DETECTED:\s*([A-Z0-9]+)")
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            txt = rec.get("text", "")
            m = pat.search(txt)
            if not m:
                continue
            sym = m.group(1)
            base = base_from_bybit_sym(sym)
            dt = datetime.strptime(rec["date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            t = int(dt.timestamp() * 1000)
            out.setdefault(base, []).append((t - W_PRE, t + W_POST))
            n += 1
    return out, n


def windows_from_1h():
    """scan repo 1h parquets; return dict base -> list[(start,end)] ; and raw count"""
    out = {}
    n = 0
    for p in sorted(glob.glob(os.path.join(D.DATA, "bybit", "*_1h.parquet"))):
        base = os.path.basename(p)[:-len("-USDT-PERP_1h.parquet")]
        df = pd.read_parquet(p)
        if df.empty or len(df) < 25:
            continue
        df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
        idx = pd.to_datetime(df["ts"], unit="ms", utc=True)
        s = df.set_index(idx)
        close = s["close"]
        tv = (s["volume"] * s["close"])
        # close 24h ago, aligned by timestamp
        prev = close.reindex(close.index - pd.Timedelta("24h"))
        prev.index = close.index
        ret = close / prev.values - 1.0
        # trailing 24h turnover (inclusive current bar)
        turn = tv.rolling("24h").sum()
        cond = (ret >= RET_MIN) & (turn >= TURNOVER_MIN) & (df["ts"].values >= SRC2_FROM)
        hit_ts = df["ts"].values[cond.values]
        for t in hit_ts:
            out.setdefault(base, []).append((int(t) - W_PRE, int(t) + W_POST))
            n += 1
    return out, n


def merge(intervals):
    if not intervals:
        return []
    iv = sorted(intervals)
    merged = [list(iv[0])]
    for s, e in iv[1:]:
        if s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return [(s, e) for s, e in merged]


# --------------------------------------------------------------------------- #
# raw 5m kline fetch (handles listed + delisted via v5 public)
# --------------------------------------------------------------------------- #
def raw_kline5(symbol, start, end):
    cur = start
    rows = []
    pxi = 0
    while cur < end:
        r = None
        for attempt in range(D.MAX_RETRY):
            try:
                proxy = PX_LIST[pxi % len(PX_LIST)]
                r = requests.get(
                    "https://api.bybit.com/v5/market/kline",
                    params={"category": "linear", "symbol": symbol,
                            "interval": TF5, "start": cur, "end": end, "limit": 1000},
                    proxies={"https": proxy, "http": proxy}, timeout=25).json()
                break
            except Exception:
                pxi += 1
                time.sleep(1.2 * (attempt + 1))
        if r is None:
            break
        L = r.get("result", {}).get("list", [])
        if not L:
            break
        asc = sorted(L, key=lambda x: int(x[0]))
        for k in asc:
            t = int(k[0])
            if start <= t <= end:
                rows.append([t, float(k[1]), float(k[2]),
                             float(k[3]), float(k[4]), float(k[5])])
        newest = int(asc[-1][0])
        if len(L) < 1000:
            break
        nxt = newest + STEP_5M
        if nxt <= cur:
            break
        cur = nxt
    return rows


def main():
    os.makedirs(OUTDIR, exist_ok=True)
    w1, n1 = windows_from_kit(os.path.join(D.DATA, "kit", "kit_raw.jsonl"))
    w2, n2 = windows_from_1h()
    log(f"raw windows: src1(kit)={n1}  src2(1h)={n2}")

    # combine per base and merge overlaps
    bases = set(w1) | set(w2)
    per_base = {}
    total_merged = 0
    for b in bases:
        merged = merge(w1.get(b, []) + w2.get(b, []))
        per_base[b] = merged
        total_merged += len(merged)
    log(f"symbols={len(bases)}  merged windows total={total_merged}")

    files = 0
    rows_total = 0
    no_data = []
    for i, b in enumerate(sorted(bases), 1):
        sym = f"{b}USDT"
        nsym = f"{b}-USDT-PERP"
        path = os.path.join(OUTDIR, f"{nsym}_5m.parquet")
        got = 0
        for (s, e) in per_base[b]:
            rows = raw_kline5(sym, s, e)
            if rows:
                D.write_ohlcv(path, rows)
                got += len(rows)
        if got:
            df = pd.read_parquet(path)
            files += 1
            rows_total += len(df)
        else:
            no_data.append(b)
        if i % 20 == 0:
            log(f"  progress {i}/{len(bases)} files={files} rows={rows_total}")

    summary = {
        "src1_windows": n1, "src2_windows": n2,
        "symbols": len(bases), "merged_windows": total_merged,
        "files": files, "rows_total": rows_total,
        "no_data": sorted(no_data),
    }
    json.dump(summary, open("/tmp/bybit5m_summary.json", "w"), indent=2)
    log("SUMMARY: " + json.dumps(summary, ensure_ascii=False))
    log("done.")


if __name__ == "__main__":
    main()
