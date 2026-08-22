#!/usr/bin/env bash
# Daily incremental update + snapshot push. Meant for cron.
# - resume-since makes the dump cheap (only new bars + funding back to last stored ts)
# - flock prevents overlap if a previous run is still going
# - pinned core (extra_symbols.txt) is always refreshed even on top-50 drift
# - stale files (dropped from top-50, >3d old) are flagged in manifest, not deleted
set -euo pipefail
cd /root/projects/backtest-data

exec 9>.cron.lock
flock -n 9 || { echo "[$(date -u +%FT%TZ)] already running, skip" >> cron.log; exit 0; }

{
  echo "===== cron run $(date -u +%FT%TZ) ====="
  python3 dump_ohlcv.py --top 50
  ./push_snapshot.sh
  echo "===== cron done $(date -u +%FT%TZ) ====="
} >> cron.log 2>&1
