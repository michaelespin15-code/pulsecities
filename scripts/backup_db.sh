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
#   gunzip -c /var/backups/pulsecities/pulsecities_YYYY-MM-DD.sql.gz | psql "$DATABASE_URL"
# =============================================================================

set -euo pipefail

APP_DIR="/root/pulsecities"
BACKUP_DIR="/var/backups/pulsecities"
RETENTION_DAYS=7

mkdir -p "$BACKUP_DIR"

# pg_dump takes a libpq connection URI as its dbname argument, so the existing
# DATABASE_URL works as-is — no need to export the rest of the env.
DATABASE_URL=$(grep -E '^DATABASE_URL=' "$APP_DIR/.env" | cut -d= -f2-)
if [ -z "${DATABASE_URL:-}" ]; then
    echo "ERROR: DATABASE_URL not found in $APP_DIR/.env" >&2
    exit 1
fi

OUT="$BACKUP_DIR/pulsecities_$(date +%F).sql.gz"

pg_dump "$DATABASE_URL" | gzip > "$OUT"

# Drop anything older than the retention window so the disk doesn't fill.
find "$BACKUP_DIR" -name 'pulsecities_*.sql.gz' -mtime "+$RETENTION_DAYS" -delete

echo "$(date '+%Y-%m-%d %H:%M:%S') backup written: $OUT ($(du -h "$OUT" | cut -f1))"
