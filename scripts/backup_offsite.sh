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

# --- credentials: project .env overrides, shared vl .env as the default -----
set -a; . "$APP_DIR/.env" 2>/dev/null || true; set +a
TOKEN="${PULSECITIES_R2_TOKEN:-}"
ACCOUNT="${PULSECITIES_R2_ACCOUNT_ID:-}"
BUCKET="${PULSECITIES_R2_BUCKET:-}"
PREFIX="pulsecities-backups"
if [ -z "$TOKEN" ] || [ -z "$ACCOUNT" ]; then
    set -a; . "$CRED_ENV" 2>/dev/null || true; set +a
    TOKEN="${R2_CLOUDFLARE_API_TOKEN:-}"
    ACCOUNT="${CLOUDFLARE_ACCOUNT_ID:-}"
    BUCKET="${BUCKET:-vs-archive}"
fi
[ -n "$TOKEN" ] || fail "no R2 token (PULSECITIES_R2_TOKEN or $CRED_ENV)"
[ -n "$ACCOUNT" ] || fail "no R2 account id"

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
S3_KEYID=$(curl -s --max-time 15 -H "Authorization: Bearer $TOKEN" \
    "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT/tokens/verify" \
    | python3 -c "import json,sys;print(json.load(sys.stdin)['result']['id'])" 2>/dev/null)
[ -n "$S3_KEYID" ] || fail "could not verify the R2 token / derive its id"
S3_SECRET=$(printf '%s' "$TOKEN" | sha256sum | cut -d' ' -f1)
S3_EP="https://$ACCOUNT.r2.cloudflarestorage.com"
EMPTY_SHA=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

# Upload via rclone: curl --aws-sigv4 cannot sign a streamed (-T) body on this
# box's curl 7.81 (it drops x-amz-content-sha256 from the canonical request,
# guaranteed 403) and --data-binary slurps the whole 1.6GB dump into memory.
# rclone streams and multiparts properly.
export RCLONE_CONFIG_R2_TYPE=s3
export RCLONE_CONFIG_R2_ACCESS_KEY_ID="$S3_KEYID"
export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY="$S3_SECRET"
export RCLONE_CONFIG_R2_ENDPOINT="$S3_EP"
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
done

echo "$(date -u '+%F %T') offsite backup complete"
