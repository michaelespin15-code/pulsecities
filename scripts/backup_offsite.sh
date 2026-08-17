#!/usr/bin/env bash
# Nightly off-box copy of the newest database dump to Cloudflare R2.
#
# WHY: the local dumps in /var/backups/pulsecities live on the same disk as the
# database; a dead disk loses the site AND every backup. This pushes the newest
# dump off-box for pennies.
#
# WHERE: the vs-archive bucket (violation-leads' R2, same owner, same box)
# under the pulsecities-backups/ prefix, using that project's bucket-scoped
# token from /root/violation-leads/.env. A dedicated bucket + token is the
# clean end-state; when created, set PULSECITIES_R2_BUCKET,
# PULSECITIES_R2_TOKEN, and PULSECITIES_R2_ACCOUNT_ID in this project's .env
# and nothing else changes.
#
# RETENTION is by key shape, no list/delete plumbing:
#   daily/<mon..sun>.sql.gz  — seven rolling slots, each overwritten weekly
#   monthly/<YYYY-MM>.sql.gz — pinned on the 1st, one per month, kept forever
#
# TRANSPORT (copied from vl-clean/scripts/archive_to_r2.sh, proven 2026-07-10):
# bucket-scoped Object R&W tokens authorize R2's S3 endpoint only, so uploads
# go via curl --aws-sigv4; access key id = the token's id (from /tokens/verify),
# secret = SHA-256 hex of the token value; every request needs an explicit
# x-amz-content-sha256 header.
#
# Failure is loud: exits 1 and emails ops through scheduler.alerts.notify_ops.
#
# Usage:
#   scripts/backup_offsite.sh            # push newest dump
#   scripts/backup_offsite.sh --dry-run  # print the plan, upload nothing

set -uo pipefail

APP_DIR="/root/pulsecities"
BACKUP_DIR="/var/backups/pulsecities"
LOG_DIR="/var/log/pulsecities"
CRED_ENV="/root/violation-leads/.env"

DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1

fail() {
    echo "FAIL: $*" >&2
    if [ "$DRY" -eq 0 ]; then
        cd "$APP_DIR"
        REASON="$*" "$APP_DIR/venv/bin/python" - <<'PY'
import os
from dotenv import load_dotenv
load_dotenv("/root/pulsecities/.env")
from scheduler.alerts import notify_ops
notify_ops(
    "Offsite backup failed",
    "backup_offsite.sh could not push the newest dump to R2:\n\n"
    + os.environ.get("REASON", "unknown")
    + "\n\n  tail -50 /var/log/pulsecities/backup_offsite.log",
)
PY
    fi
    exit 1
}

# --- credentials -------------------------------------------------------------
# Derivation lives in scripts/lib/r2_creds.sh so retire_raw_data.sh can reach
# the same bucket by the same route; it had its own attempt and got a 403.
# shellcheck source=scripts/lib/r2_creds.sh
. "$APP_DIR/scripts/lib/r2_creds.sh"
r2_load_credentials "$APP_DIR" "$CRED_ENV" || fail "could not derive R2 credentials"
BUCKET="$R2_BUCKET"
PREFIX="$R2_PREFIX"

# --- pick the newest completed dump ------------------------------------------
DUMP=$(ls -t "$BACKUP_DIR"/pulsecities_*.sql.gz 2>/dev/null | head -1)
[ -n "$DUMP" ] || fail "no dump found in $BACKUP_DIR"
gzip -t "$DUMP" || fail "newest dump fails gzip -t: $DUMP"
BYTES=$(stat -c%s "$DUMP")
[ "$BYTES" -gt 100000000 ] || fail "newest dump suspiciously small (${BYTES}B): $DUMP"

SLOT="daily/$(date -u +%a | tr 'A-Z' 'a-z').sql.gz"
KEYS=("$SLOT")
[ "$(date -u +%d)" = "01" ] && KEYS+=("monthly/$(date -u +%Y-%m).sql.gz")

echo "$(date -u '+%F %T') pushing $DUMP (${BYTES}B) -> ${KEYS[*]} (bucket=$BUCKET)"
[ "$DRY" -eq 1 ] && { echo "dry-run: stopping before upload"; exit 0; }

# --- derived S3 credentials (never printed) ----------------------------------
# r2_load_credentials already derived and exported these, along with the
# RCLONE_CONFIG_R2_* vars that define the R2: remote. Kept under the local
# names the rest of this script (and the state-archive encryption below) uses.
S3_KEYID="$R2_S3_KEYID"
S3_SECRET="$R2_S3_SECRET"
S3_EP="$R2_S3_ENDPOINT"
EMPTY_SHA=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
command -v rclone >/dev/null || fail "rclone not installed (apt-get install rclone)"

for key in "${KEYS[@]}"; do
    rclone copyto --s3-no-check-bucket --retries 3 --low-level-retries 10 \
        "$DUMP" "R2:$BUCKET/$PREFIX/$key" 2>>/tmp/rclone_offsite_err.log \
        || fail "rclone upload of $key failed: $(tail -2 /tmp/rclone_offsite_err.log)"

    # %header{} writeout needs curl >= 7.83 and this box has 7.81, so read the
    # content-length off the dumped response headers instead.
    remote_bytes=$(curl -s --head --max-time 60 \
        --aws-sigv4 "aws:amz:auto:s3" --user "$S3_KEYID:$S3_SECRET" \
        -H "x-amz-content-sha256: $EMPTY_SHA" \
        "$S3_EP/$BUCKET/$PREFIX/$key" \
        | awk 'tolower($1) == "content-length:" {print $2}' | tr -dc '0-9')
    [ "$remote_bytes" = "$BYTES" ] || fail "size mismatch on $key: local $BYTES vs remote $remote_bytes"
    echo "$(date -u '+%F %T') verified $PREFIX/$key ($remote_bytes bytes)"

    # Record the verified push so weekly_ops_health can age each slot without
    # re-deriving these credentials. The seven daily slots are overwritten on a
    # weekly rotation, so one failing weekday leaves a slot quietly rotting while
    # the other six stay fresh and the newest-backup check still reads green:
    # the 'sat' slot went nine days stale exactly that way. Reporting the newest
    # object cannot catch it. Ages per slot can.
    SLOTS_JSON="$LOG_DIR/backup_offsite_slots.json"
    python3 - "$SLOTS_JSON" "$key" "$remote_bytes" <<'PY' || echo "warn: could not record slot state"
import json, os, sys
from datetime import datetime, timezone

path, key, size = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    with open(path) as fh:
        state = json.load(fh)
    if not isinstance(state, dict):
        state = {}
except Exception:
    state = {}

state[key] = {"pushed_at": datetime.now(timezone.utc).isoformat(), "bytes": int(size)}
tmp = path + ".tmp"
os.makedirs(os.path.dirname(path), exist_ok=True)
with open(tmp, "w") as fh:
    json.dump(state, fh, indent=1, sort_keys=True)
os.replace(tmp, path)
PY
done

# --- secrets + curated state -------------------------------------------------
# The dump restores the database; it does not restore the box. Without .env
# there are no credentials to reach this bucket, and eviction_flips_editions
# is the approved published archive that no scraper can regenerate. Encrypted
# with the same R2 secret so the archive is useless on its own.
STATE_TAR="/tmp/pulsecities_state_$$.tar.gz.enc"
trap 'rm -f "$STATE_TAR"' EXIT

tar -czf - -C "$APP_DIR" \
        .env \
        scripts/eviction_flips_editions.json \
        scripts/eviction_flips_state.json \
        scripts/building_alerts_state.json \
    2>/dev/null \
    | openssl enc -aes-256-cbc -pbkdf2 -salt -pass pass:"$S3_SECRET" -out "$STATE_TAR" \
    || fail "state archive build failed"

state_bytes=$(stat -c %s "$STATE_TAR")
[ "$state_bytes" -gt 256 ] || fail "state archive suspiciously small ($state_bytes bytes)"

rclone copyto --s3-no-check-bucket --retries 3 --low-level-retries 10 \
    "$STATE_TAR" "R2:$BUCKET/$PREFIX/state-latest.tar.gz.enc" \
    2>>/tmp/rclone_offsite_err.log \
    || fail "rclone upload of state archive failed: $(tail -2 /tmp/rclone_offsite_err.log)"

remote_state=$(curl -s --head --max-time 60 \
    --aws-sigv4 "aws:amz:auto:s3" --user "$S3_KEYID:$S3_SECRET" \
    -H "x-amz-content-sha256: $EMPTY_SHA" \
    "$S3_EP/$BUCKET/$PREFIX/state-latest.tar.gz.enc" \
    | awk 'tolower($1) == "content-length:" {print $2}' | tr -dc '0-9')
[ "$remote_state" = "$state_bytes" ] || fail "size mismatch on state archive: local $state_bytes vs remote $remote_state"
echo "$(date -u '+%F %T') verified $PREFIX/state-latest.tar.gz.enc ($remote_state bytes)"

echo "$(date -u '+%F %T') offsite backup complete"
