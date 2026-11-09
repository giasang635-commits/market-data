#!/usr/bin/env python3
"""ISOLATED single-run variant. Private cache + private output dir so a
concurrent run of kit_ticks_extract.py cannot interfere. Streams gz line by
line (memory-safe), groups events by symbol, re-downloads if a cache file
disappears, writes outputs to a private dir for atomic swap afterwards."""
import os, re, csv, gzip, json, sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import requests

ROOT   = "/root/projects/backtest-data"
RAW    = f"{ROOT}/data/kit/kit_raw.jsonl"
FILLS  = "/root/kit_fills.csv"
OUTDIR = "/root/ktxout"               # private (outside /tmp glob cleanups); swapped later
CACHE  = f"/root/ktxcache_{os.getpid()}"  # private cache, per-pid, not in /tmp
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

_missing_days = set()
def download_day(sym, dstr):
    key = f"{sym}_{dstr}"
    if key in _missing_days:
        return None
    cpath = f"{CACHE}/{key}.csv.gz"
    if os.path.exists(cpath) and os.path.getsize(cpath) > 0:
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
        try:
            r = requests.get(url, timeout=120, proxies={"http": PROXY, "https": PROXY})
        except Exception:
            _missing_days.add(key); return None
    if r.status_code != 200:
        _missing_days.add(key)
        return None
    os.makedirs(CACHE, exist_ok=True)   # recreate if an external glob cleanup removed it
    with open(cpath, "wb") as fo:
        fo.write(r.content)
    return cpath

def main():
    sigs = load_signals()
    ents = load_entries()
    print(f"[counts] p.1 signals windows = {len(sigs)}", flush=True)
    print(f"[counts] p.2 entry   windows = {len(ents)}", flush=True)

    events = [("sig", s, t) for s, t in sigs] + [("ent", s, t) for s, t in ents]
    by_sym = defaultdict(list)
    for src, sym, t in events:
        by_sym[sym].append(t)

    files_written = empty_windows = windows_missing = 0
    day_downloads = day_notfound = 0
    done = 0

    for sym in sorted(by_sym):
        times = by_sym[sym]
        # resume: skip symbol if every expected output already exists
        expected = [f"{OUTDIR}/{sym}_{t.strftime('%Y%m%d%H%M%S')}.csv.gz" for t in times]
        if expected and all(os.path.exists(p) for p in expected):
            files_written += len(expected)
            done += 1
            continue
        wins = []
        needed_days = set()
        for t in times:
            s = t - PRE; e = t + POST
            wins.append([t, s.timestamp(), e.timestamp(), []])
            needed_days.add(s.date()); needed_days.add(e.date())
        avail = set()
        paths = {}
        for d in sorted(needed_days):
            dstr = d.strftime("%Y-%m-%d")
            existed = os.path.exists(f"{CACHE}/{sym}_{dstr}.csv.gz")
            cpath = download_day(sym, dstr)
            if cpath is None:
                day_notfound += 1
                continue
            if not existed:
                day_downloads += 1
            avail.add(d); paths[d] = cpath
        for d in sorted(avail):
            cpath = paths[d]
            if not os.path.exists(cpath):
                cpath = download_day(sym, d.strftime("%Y-%m-%d"))
                if cpath is None:
                    continue
            header = None
            try:
                with gzip.open(cpath, "rt") as fi:
                    header_line = fi.readline()
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
            except FileNotFoundError:
                continue
            # keep header globally
            if header_line:
                globals()["_HDR"] = header_line.rstrip("\n")
        hdr = globals().get("_HDR", "")
        for t, s_ts, e_ts, body in wins:
            wdays = {datetime.fromtimestamp(s_ts, tz=timezone.utc).date(),
                     datetime.fromtimestamp(e_ts, tz=timezone.utc).date()}
            if not any(dd in avail for dd in wdays):
                windows_missing += 1
                continue
            fname = f"{OUTDIR}/{sym}_{t.strftime('%Y%m%d%H%M%S')}.csv.gz"
            if os.path.exists(fname):
                files_written += 1
                continue
            with gzip.open(fname, "wt") as fo:
                fo.write(hdr + "\n")
                for ln in body:
                    fo.write(ln + "\n")
            files_written += 1
            if not body:
                empty_windows += 1
        for d in sorted(needed_days):
            p = f"{CACHE}/{sym}_{d.strftime('%Y-%m-%d')}.csv.gz"
            if os.path.exists(p):
                try: os.remove(p)
                except OSError: pass
        done += 1
        if done % 20 == 0:
            print(f"[progress] symbols {done}/{len(by_sym)} files={files_written}", flush=True)

    print(f"[result] windows_total={len(events)} files_written={files_written} "
          f"empty_windows={empty_windows} windows_missing_archive={windows_missing}", flush=True)
    print(f"[result] day_archives_downloaded={day_downloads} day_archives_notfound={day_notfound}", flush=True)

if __name__ == "__main__":
    main()
