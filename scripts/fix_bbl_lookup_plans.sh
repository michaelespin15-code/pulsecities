#!/usr/bin/env bash
# Composite (bbl, date) indexes for the two tables a property page reads.
#
# Why: complaints_raw carries btree(bbl) and btree(created_date) separately and
# no composite. Given "WHERE bbl = ? AND created_date >= ? ORDER BY created_date
# DESC LIMIT 50", the planner walks the date index backward expecting to hit 50
# matches quickly. On BBL 2048330028 it discarded 997,424 rows and read 213,157
# pages (~1.6GB) to return 50, in 85.8 seconds. Googlebot 504'd on that URL four
# times, and it is 56 of the 5xx on /property over two weeks.
#
# This is not one bad building. Any BBL whose records are sparse in date order
# gets the same plan, and /property is the site's highest-volume template.
#
# CONCURRENTLY: two passes, no write lock, safe against live traffic. Slow on a
# 9.3GB table. Measured headroom before writing this: maintenance_work_mem
# 256MB against an index of roughly 5.1M x 30 bytes, 1.7GB RAM available, 30GB
# free disk.
#
# A failed CONCURRENTLY build leaves an INVALID index behind rather than rolling
# back; the verify step below names any it finds so they can be dropped and
# retried.

set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Same pattern as backup_db.sh. .env is not valid shell (ALERT_SNOOZE carries a
# value with spaces and a colon), so sourcing it exits 127; only this one
# variable is needed anyway.
DATABASE_URL=$(grep -E '^DATABASE_URL=' "$APP_DIR/.env" | cut -d= -f2-)
if [ -z "${DATABASE_URL:-}" ]; then
    echo "ERROR: DATABASE_URL not found in $APP_DIR/.env" >&2
    exit 1
fi

run() {
    echo ">>> $1"
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block, so each
    # statement goes in its own psql invocation with autocommit.
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SET maintenance_work_mem = '256MB';" -c "$1"
}

time run "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_complaints_bbl_date
          ON complaints_raw (bbl, created_date DESC);"

time run "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_violations_bbl_date
          ON violations_raw (bbl, inspection_date DESC);"

echo ">>> invalid indexes (should be empty):"
psql "$DATABASE_URL" -c "
    SELECT c.relname FROM pg_class c
    JOIN pg_index i ON i.indexrelid = c.oid
    WHERE NOT i.indisvalid AND c.relname LIKE 'idx_%_bbl_date';"

echo ">>> plan check on the BBL that was timing out:"
psql "$DATABASE_URL" -c "
    EXPLAIN (ANALYZE, BUFFERS)
    SELECT * FROM complaints_raw
    WHERE bbl = '2048330028' AND created_date >= now() - interval '365 days'
    ORDER BY created_date DESC LIMIT 50;" | grep -E "Index|Execution|Buffers"
