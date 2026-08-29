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
# Derive the exact strings to redact from the two files that carried them,
# rather than matching any DSN. A broad `regex:postgresql://user:...@` also
# rewrites .github/workflows/ci.yml, whose DSN has to keep matching the
# throwaway service container's `ci_only_not_a_secret` or every CI run fails
# to connect, and it rewrites the placeholders in .env.example and the
# docstring in scripts/lib/pgenv.sh, which say nothing. Measured on this repo:
# one real credential across both files, thirteen characters.
REPLACE=$(mktemp)
trap 'rm -f "$REPLACE"' EXIT
python3 - "$REPLACE" <<'EXTRACT'
import subprocess, sys, re
revs = subprocess.run(["git","rev-list","--all"],capture_output=True,text=True).stdout.split()
found = set()
for path in ("scripts/run_bbl_audit.py", "scripts/audit/prod_diagnostic.sh"):
    for rev in revs:
        blob = subprocess.run(["git","show",f"{rev}:{path}"],
                              capture_output=True,text=True).stdout
        for m in re.finditer(r"postgresql://[A-Za-z0-9_]+:([^@\s\"']+)@", blob):
            pw = m.group(1)
            if pw.lower() not in {"password","pass","secret","changeme"} and len(pw) >= 8:
                found.add(pw)
if not found:
    print("nothing to redact; history is already clean", file=sys.stderr)
    sys.exit(3)
open(sys.argv[1], "w").write("\n".join(f"{pw}==>REDACTED" for pw in sorted(found)) + "\n")
print(f"redacting {len(found)} distinct credential(s)", file=sys.stderr)
EXTRACT

ORIGIN=$(git remote get-url origin)
yes | git filter-repo --replace-text "$REPLACE" --force

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
