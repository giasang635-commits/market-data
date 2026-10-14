#!/usr/bin/env bash
# push_snapshot.sh v2 (s.41, 14.10.2026)
# REMOTE main = one parentless commit (force-push). LOCAL master = the same single commit.
# v1 kept full local history (+~0.7 GB/week of .git); v2 keeps nothing older.
# Clone side: git clone --depth 1 https://github.com/giasang635-commits/market-data
# Usage: ./push_snapshot.sh [remote_branch]   (default: main)
set -euo pipefail
cd "$(dirname "$0")"

REMOTE_BRANCH="${1:-main}"
LOCAL_BRANCH=master
TS="$(date -u +%FT%TZ)"

[ "$(git symbolic-ref --short -q HEAD)" = "$LOCAL_BRANCH" ] || { echo "push_snapshot: HEAD is not ${LOCAL_BRANCH}, stop"; exit 1; }

# 1) one parentless commit with the current data (no branch switching, working tree untouched)
git add -A
NEW="$(git commit-tree "$(git write-tree)" -m "data snapshot ${TS}")"

# 2) force-push it as the remote snapshot
git push -f origin "${NEW}:refs/heads/${REMOTE_BRANCH}"

# 3) local branch and tracking ref := the same commit; drop everything older
git update-ref "refs/heads/${LOCAL_BRANCH}" "${NEW}"
git update-ref "refs/remotes/origin/${REMOTE_BRANCH}" "${NEW}"
git reflog expire --expire=now --expire-unreachable=now --all
git -c pack.window=0 -c pack.threads=1 gc --prune=now --quiet

echo "pushed snapshot ${TS} -> origin/${REMOTE_BRANCH}; local ${LOCAL_BRANCH} = $(git rev-list --count HEAD) commit(s); .git $(du -sh .git | cut -f1)"
