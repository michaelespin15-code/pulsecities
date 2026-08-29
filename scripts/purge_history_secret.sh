#!/usr/bin/env bash
#
# Rewrite every commit so no Postgres password survives in git history.
#
# Context. The live database password sat in 551 blobs reachable from
# origin/main, in two files: scripts/run_bbl_audit.py and
# scripts/audit/prod_diagnostic.sh. Neither still carries it in the current
# tree; the exposure is entirely historical. The credential itself was rotated
# on 2026-08-29, so this is hygiene rather than an open risk.
#
# The replacement is a regex over any postgres DSN, so the secret is never
# typed, never passed as an argument, and never lands in a shell history.
#
# This script stops before the force-push. That step is irreversible and
# rewrites every commit SHA, so it is yours to run deliberately.
#
#   bash scripts/purge_history_secret.sh
#
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

BACKUP="/root/pulsecities-prepurge-$(date +%Y%m%d-%H%M%S).bundle"

# --- refuse to run in a state where the rewrite would lose work -------------
if [ -n "$(git status --porcelain)" ]; then
    echo "ABORT: working tree is dirty. Commit or stash first." >&2; exit 1
fi
if [ -n "$(git log --oneline origin/main..HEAD 2>/dev/null)" ]; then
    echo "ABORT: unpushed commits. filter-repo rewrites every SHA, so push first." >&2; exit 1
fi
if ! command -v git-filter-repo >/dev/null; then
    echo "ABORT: git-filter-repo is not installed." >&2; exit 1
fi

# --- a full copy of history before touching it ------------------------------
git bundle create "$BACKUP" --all
echo "backup written: $BACKUP"

BEFORE=$(git rev-list --all | wc -l)

# --- the rewrite ------------------------------------------------------------
# `regex:` matches any postgres DSN carrying a password and keeps the user.
REPLACE=$(mktemp)
trap 'rm -f "$REPLACE"' EXIT
printf '%s\n' 'regex:postgresql://([A-Za-z0-9_]+):[^@\s"'"'"']+@==>postgresql://\1:REDACTED@' > "$REPLACE"

ORIGIN=$(git remote get-url origin)
git filter-repo --replace-text "$REPLACE" --force

# filter-repo drops the remote on purpose; put it back.
git remote add origin "$ORIGIN" 2>/dev/null || git remote set-url origin "$ORIGIN"
git fetch origin --quiet || true

AFTER=$(git rev-list --all | wc -l)
echo
echo "commits before: $BEFORE   after: $AFTER"
echo
echo "Verify no DSN password survives anywhere in history:"
echo "  git rev-list --all | xargs -I{} git grep -lE 'postgresql://[^:]+:[^@]+@' {} 2>/dev/null | grep -v REDACTED | head"
echo
echo "If that prints nothing, publish the rewrite with:"
echo "  git push --force origin main"
echo
echo "Then ask GitHub Support to garbage-collect unreferenced objects, or the"
echo "old commits stay reachable by direct SHA for a while. You have 0 forks,"
echo "which is what makes this actually effective."
echo
echo "To undo before pushing:  git fetch $BACKUP '*:*'"
