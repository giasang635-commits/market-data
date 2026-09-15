#!/usr/bin/env bash
# Snapshot push: the REMOTE repo is a SNAPSHOT (single commit), not an archive.
# Local working branch keeps full history; remote gets one orphan commit (force).
# Clone side: git clone --depth 1 https://github.com/giasang635-commits/market-data
#
# AUTH (do ONE of these on the server; never paste the token into chat):
#   git remote set-url origin https://<TOKEN>@github.com/giasang635-commits/market-data
#   # or:
#   printf 'https://<TOKEN>@github.com\n' > ~/.git-credentials && git config --global credential.helper store
#
# Usage:  ./push_snapshot.sh [remote_branch]     # default remote branch: main
set -euo pipefail
cd "$(dirname "$0")"

REMOTE_BRANCH="${1:-main}"
SRC_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
TS="$(date -u +%FT%TZ)"

# 1) persist any new/updated data on the local working branch (keeps history)
git add -A
git commit -q -m "update ${TS}" || true

# 2) build a fresh single-commit orphan and force-push it as the remote snapshot
git branch -D _snapshot 2>/dev/null || true
git checkout -q --orphan _snapshot
git add -A
git commit -q -m "data snapshot ${TS}"
git push -f origin "_snapshot:${REMOTE_BRANCH}"

# 3) return to the working branch, drop the temp orphan
git checkout -qf "${SRC_BRANCH}"
git branch -D _snapshot

echo "pushed snapshot ${TS} -> origin/${REMOTE_BRANCH} (single commit, --depth 1 friendly)"
