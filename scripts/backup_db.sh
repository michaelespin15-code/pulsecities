#!/usr/bin/env bash
# =============================================================================
# Nightly PostgreSQL backup for PulseCities.
#   - pg_dump the pulsecities database
#   - gzip to /var/backups/pulsecities/pulsecities_YYYY-MM-DD.sql.gz
#   - prune dumps older than RETENTION_DAYS
#
# Runs from cron at 03:30 UTC, after the 02:00 scraper pipeline has settled.
#
# Restore:
#   gunzip -c /var/backups/pulsecities/pulsecities_YYYY-MM-DD.sql.gz | psql "$PGDSN"
# =============================================================================

set -euo pipefail

APP_DIR="/root/pulsecities"
BACKUP_DIR="/var/backups/pulsecities"

# Two local dumps, which at ~1.7GB each is ~3.3GB on a 78GB disk. Deeper local
# history is redundant: backup_offsite.sh pushes weekday slots plus a monthly pin
# to R2, and Sunday's restore-test proves the newest dump actually restores.
#
# This was 7 while a crontab line pruned the same directory at -mtime +1 an hour
# later. The cron ran last so the cron won, and the repo described a week of
# dumps that never existed. That line was removed on 2026-08-17, so this is now
# the only place the policy is written. Raising it is a free decision again;
# each dump is ~1.6GB, so a week costs about 11GB of the local disk.
RETENTION_DAYS=1

mkdir -p "$BACKUP_DIR"

# Credentials come in through the environment, not argv. Passing the full URI
# to pg_dump put the password in `ps` for the length of the dump; see
# scripts/lib/pgenv.sh for what that cost on 2026-08-29.
. "$APP_DIR/scripts/lib/pgenv.sh"

OUT="$BACKUP_DIR/pulsecities_$(date +%F).sql.gz"

# Dump to a temp name and move into place only when the whole pipeline
# succeeded. Writing $OUT directly meant a mid-dump failure left a truncated
# file as the newest "backup" until Sunday's restore test noticed.
TMP="$OUT.tmp"
trap 'rm -f "$TMP"' EXIT
pg_dump "$PGDSN" | gzip > "$TMP"
gzip -t "$TMP"
mv "$TMP" "$OUT"
trap - EXIT

# Drop anything older than the retention window so the disk doesn't fill.
find "$BACKUP_DIR" -name 'pulsecities_*.sql.gz' -mtime "+$RETENTION_DAYS" -delete

echo "$(date '+%Y-%m-%d %H:%M:%S') backup written: $OUT ($(du -h "$OUT" | cut -f1))"
