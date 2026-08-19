#!/usr/bin/env bash
# Snapshot push: repo is a SNAPSHOT, not an archive.
# Pushes current tree as a single orphan commit with force -> remote history stays flat (1 commit).
# Clone side uses: git clone --depth 1 <remote>
#
# Usage:
#   git remote add origin <REMOTE_URL>   # once, token in URL or via credential helper (NOT in chat)
#   ./push_snapshot.sh [branch]          # default branch: main
set -euo pipefail
cd "$(dirname "$0")"

BR="${1:-main}"
TS="$(date -u +%FT%TZ)"

# fresh orphan branch = single commit, no history growth
git checkout --orphan _snapshot_tmp
git add -A
git commit -q -m "data snapshot ${TS}"
git branch -M _snapshot_tmp "${BR}"
git push -f origin "${BR}"

echo "pushed snapshot ${TS} -> origin/${BR} (single commit, --depth 1 friendly)"
