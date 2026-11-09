#!/usr/bin/env python3
"""
One-off expansion: add ALL Bybit linear USDT perps (status Trading) for 1h/4h
since 2026-01-01, plus delisted-after-2026-02-01 perps that Bybit kline still serves.

Reuses dump_ohlcv.py helpers (format/manifest identical). READ-ONLY to exchange.
Daily cron (dump_ohlcv.py --top 50) is untouched.
"""
import os, json, time, glob, argparse
from datetime import datetime, timezone

import requests
import pandas as pd

import dump_ohlcv as D

SINCE_2026 = 1_767_225_600_000  # 2026-01-01T00:00:00Z
CUT_DELIST = 1_769_904_000_000  # 2026-02-01T00:00:00Z
TFS = ["1h", "4h"]
INTERVAL = {"1h": "60", "4h": "240"}
PROXY = D.PROXIES[0]
PX = {"https": PROXY, "http": PROXY}


def log(*a):
    D.log(*a)


def disk_syms():
    out = set()
    for p in glob.glob(os.path.join(D.DATA, "bybit", "*_1h.parquet")):
        out.add(os.path.basename(p)[:-len("_1h.parquet")])
    return out


# ---- raw kline for symbols ccxt no longer lists (delisted) ----
def raw_kline(symbol, tf, since):
    step = D.TF_MS[tf]
    cur = since
    rows = []
    while True:
        for attempt in range(D.MAX_RETRY):
            try:
                r = requests.get(
                    "https://api.bybit.com/v5/market/kline",
                    params={"category": "linear", "symbol": symbol,
                            "interval": INTERVAL[tf], "start": cur, "limit": 1000},
                    proxies=PX, timeout=25).json()
                break
            except Exception:
                time.sleep(1.5 * (attempt + 1))
        else:
            break
        L = r.get("result", {}).get("list", [])
        if not L:
            break
        asc = sorted(L, key=lambda x: int(x[0]))
        for k in asc:
            rows.append([int(k[0]), float(k[1]), float(k[2]),
                         float(k[3]), float(k[4]), float(k[5])])
        newest = int(L[0][0])
        if len(L) < 1000:
            break
        nxt = newest + step
        if nxt <= cur:
            break
        cur = nxt
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", choices=["trading", "delisted", "both"], default="both")
    a = ap.parse_args()

    man = D.load_manifest()
    man.setdefault("files", {})
    man.setdefault("selected_symbols", {})

    ex = D.make_exchange("bybit")
    log("load_markets (bybit linear)")
    markets = ex.load_markets()
    eligible = [s for s in markets if D.eligible("bybit", markets[s])]
    norm = {s: D.norm_symbol("bybit", markets[s]) for s in eligible}
    have = disk_syms()
    log(f"eligible Trading={len(eligible)}  on-disk(before)={len(have)}")

    added_trading = []
    if a.part in ("trading", "both"):
        # dump ALL eligible: new -> since 2026-01-01, existing -> cheap incremental resume
        for i, sym in enumerate(eligible, 1):
            nsym = norm[sym]
            is_new = nsym not in have
            for tf in TFS:
                path = os.path.join(D.DATA, "bybit", f"{nsym}_{tf}.parquet")
                rel = os.path.relpath(path, D.BASE)
                since = D.resume_since(path, D.TF_MS[tf], SINCE_2026)
                until = D.now_ms()
                rows = D.fetch_ohlcv(ex, sym, tf, since, until) if since < until else []
                df = D.write_ohlcv(path, rows) if rows else (
                    pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame())
                man["files"][rel] = D.manifest_entry(df, "ohlcv")
            if is_new:
                added_trading.append(nsym)
            if i % 25 == 0:
                D.save_manifest(man)
                log(f"  trading progress {i}/{len(eligible)} (new so far {len(added_trading)})")
        D.save_manifest(man)
        log(f"trading done: added {len(added_trading)} new symbols")

    added_delisted, no_data = [], []
    if a.part in ("delisted", "both"):
        dl = json.load(open("/tmp/delisted_test.json")) if os.path.exists("/tmp/delisted_test.json") else None
        if dl is None:
            log("WARN: /tmp/delisted_test.json missing; skip delisted part")
        else:
            cands = sorted(dl["has"].keys())  # bases that still return candles
            for i, base in enumerate(cands, 1):
                nsym = f"{base}-USDT-PERP"
                raw = f"{base}USDT"
                got_any = False
                for tf in TFS:
                    path = os.path.join(D.DATA, "bybit", f"{nsym}_{tf}.parquet")
                    rel = os.path.relpath(path, D.BASE)
                    since = D.resume_since(path, D.TF_MS[tf], SINCE_2026)
                    rows = raw_kline(raw, tf, since)
                    df = D.write_ohlcv(path, rows) if rows else (
                        pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame())
                    if not df.empty:
                        got_any = True
                    man["files"][rel] = D.manifest_entry(df, "ohlcv")
                if got_any:
                    added_delisted.append(nsym)
                else:
                    no_data.append(base)
                if i % 20 == 0:
                    D.save_manifest(man)
                    log(f"  delisted progress {i}/{len(cands)}")
            man["delisted_no_data"] = sorted(dl["empty"])
            D.save_manifest(man)
            log(f"delisted done: added {len(added_delisted)}; no_data_from_api {len(dl['empty'])}")

    D.mark_stale(man)
    man["expand_finished_at"] = datetime.now(timezone.utc).isoformat()
    D.save_manifest(man)
    json.dump({"added_trading": added_trading, "added_delisted": added_delisted},
              open("/tmp/expand_result.json", "w"))
    log(f"ALL DONE. new_trading={len(added_trading)} new_delisted={len(added_delisted)}")


if __name__ == "__main__":
    main()
