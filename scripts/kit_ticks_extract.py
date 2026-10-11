#!/usr/bin/env python3
"""Extract Bybit tick windows around Kit signals (p.1) and fill-entries (p.2).
Archive: https://public.bybit.com/trading/<SYM>/<SYM><YYYY-MM-DD>.csv.gz
Direct download; on HTTP 403 retry via proxy 'second' (31.70.81.103:3128).
Window per event: [t-20min, t+10min]; crosses midnight -> both day files.
Out: data/kit_ticks/<SYM>_<YYYYMMDDHHMMSS>.csv.gz (all archive columns).

Memory-safe: streams gz line-by-line (never loads whole file), groups events by
symbol so each day archive is read once, deletes a symbol's cached archives after
processing to keep disk low (earlyoom kills python on OOM).
"""
import os, re, csv, gzip, json, sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import requests

ROOT   = "/root/projects/backtest-data"
RAW    = f"{ROOT}/data/kit/kit_raw.jsonl"
FILLS  = "/root/kit_fills.csv"
OUTDIR = f"{ROOT}/data/kit_ticks"
CACHE  = f"/tmp/kit_arch_{os.getpid()}"   # per-process cache: no cross-run delete race
PROXY  = "http://31.70.81.103:3128"   # 'second' (IONOS Berlin)
BASE   = "https://public.bybit.com/trading"
os.makedirs(OUTDIR, exist_ok=True)
os.makedirs(CACHE, exist_ok=True)

PRE  = timedelta(minutes=20)
POST = timedelta(minutes=10)

def parse_dt(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

PUMP_RE = re.compile(r'PUMP DETECTED:\s*([A-Z0-9]+)')
def load_signals():
    out = []
    with open(RAW) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            m = PUMP_RE.search(o.get("text", ""))
            if not m:
                continue
            sym = m.group(1).upper()
            if "USDT" not in sym:
                sym += "USDT"
            out.append((sym, parse_dt(o["date"])))
    return out

def load_entries():
    rows = []
    with open(FILLS) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    rows.sort(key=lambda r: int(r["execTime"]))
    net = defaultdict(float)
    entries = []
    for r in rows:
        sym, side, qty = r["symbol"], r["side"], float(r["execQty"])
        if side == "Sell" and abs(round(net[sym], 8)) < 1e-8:
            t = datetime.fromtimestamp(int(r["execTime"]) / 1000, tz=timezone.utc)
            entries.append((sym, t))
        net[sym] = round(net[sym] + (-qty if side == "Sell" else qty), 8)
    return entries

_missing_days = set()   # keys "SYM_YYYY-MM-DD" confirmed 404
def download_day(sym, dstr):
    key = f"{sym}_{dstr}"
    if key in _missing_days:
        return None
    cpath = f"{CACHE}/{key}.csv.gz"
    if os.path.exists(cpath):
        return cpath
    url = f"{BASE}/{sym}/{sym}{dstr}.csv.gz"
    r = None
    try:
        r = requests.get(url, timeout=120)
    except Exception:
        r = None
    if r is not None and r.status_code == 403:
        try:
            r = requests.get(url, timeout=120, proxies={"http": PROXY, "https": PROXY})
        except Exception:
            r = None
    if r is None:
        # last attempt via proxy
        try:
            r = requests.get(url, timeout=120, proxies={"http": PROXY, "https": PROXY})
        except Exception:
            _missing_days.add(key); return None
    if r.status_code == 404:
        _missing_days.add(key); return None
    if r.status_code != 200:
        _missing_days.add(key); return None
    with open(cpath, "wb") as fo:
        fo.write(r.content)
    return cpath

def main():
    sigs = load_signals()
    ents = load_entries()
    print(f"[counts] p.1 signals windows = {len(sigs)}")
    print(f"[counts] p.2 entry   windows = {len(ents)}")

    # events grouped by symbol
    events = [("sig", s, t) for s, t in sigs] + [("ent", s, t) for s, t in ents]
    by_sym = defaultdict(list)
    for src, sym, t in events:
        by_sym[sym].append(t)

    files_written = 0
    empty_windows = 0
    windows_missing = 0        # window whose archive day(s) all missing
    day_downloads = 0
    day_notfound = 0

    for sym in sorted(by_sym):
        times = by_sym[sym]
        # build window specs for this symbol
        wins = []  # (event_t, s_ts, e_ts, [body lines])
        needed_days = set()
        for t in times:
            s = t - PRE; e = t + POST
            wins.append([t, s.timestamp(), e.timestamp(), []])
            needed_days.add(s.date()); needed_days.add(e.date())
        # download + stream each needed day once
        header = None
        avail_days = []
        for d in sorted(needed_days):
            dstr = d.strftime("%Y-%m-%d")
            had = os.path.exists(f"{CACHE}/{sym}_{dstr}.csv.gz")
            cpath = download_day(sym, dstr)
            if cpath is None:
                day_notfound += 1
                continue
            if not had:
                day_downloads += 1
            avail_days.append((d, cpath))
        # stream days in chronological order so bodies stay time-sorted
        for d, cpath in sorted(avail_days):
            with gzip.open(cpath, "rt") as fi:
                first = fi.readline()
                if header is None and first:
                    header = first.rstrip("\n")
                for ln in fi:
                    c = ln.find(",")
                    if c <= 0:
                        continue
                    try:
                        ts = float(ln[:c])
                    except ValueError:
                        continue
                    for w in wins:
                        if w[1] <= ts <= w[2]:
                            w[3].append(ln.rstrip("\n"))
        # write window files
        for t, s_ts, e_ts, body in wins:
            # determine if any needed day existed for this window
            wdays = {datetime.fromtimestamp(s_ts, tz=timezone.utc).date(),
                     datetime.fromtimestamp(e_ts, tz=timezone.utc).date()}
            any_avail = any(dd in {ad for ad, _ in avail_days} for dd in wdays)
            if not any_avail:
                windows_missing += 1
                continue
            fname = f"{OUTDIR}/{sym}_{t.strftime('%Y%m%d%H%M%S')}.csv.gz"
            if os.path.exists(fname):
                files_written += 1
                continue
            with gzip.open(fname, "wt") as fo:
                fo.write((header or "") + "\n")
                for ln in body:
                    fo.write(ln + "\n")
            files_written += 1
            if not body:
                empty_windows += 1
        # free this symbol's cache to keep disk low
        for d in sorted(needed_days):
            p = f"{CACHE}/{sym}_{d.strftime('%Y-%m-%d')}.csv.gz"
            if os.path.exists(p):
                try: os.remove(p)
                except OSError: pass

    print(f"[result] windows_total={len(events)} files_written={files_written} "
          f"empty_windows={empty_windows} windows_missing_archive={windows_missing}")
    print(f"[result] day_archives_downloaded={day_downloads} day_archives_notfound={day_notfound}")

if __name__ == "__main__":
    main()
