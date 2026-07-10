#!/usr/bin/env bash
# =============================================================================
# Backup restore-test for PulseCities.
#
# "We have backups" only means something if they restore. This actually
# restores the latest nightly dump into a throwaway database, checks the core
# tables came back with sane row counts, then drops the scratch database.
# The result is written as JSON for the weekly ops-health email to report.
#
# Failure modes this catches that a bare `ls` of the backup dir does not:
#   - truncated / corrupt gzip (gzip -t)
#   - a pg_dump that aborted mid-write (restore hits ON_ERROR_STOP)
#   - a dump that restores structurally but lost its data (row-count floor)
#
# Runs weekly from cron. Uses the postgres superuser via peer auth for
# createdb/dropdb because pulsecities_user has no CREATEDB. Restore is gated
# on free disk so it degrades to a reported skip instead of filling the disk.
# =============================================================================

set -uo pipefail

APP_DIR="/root/pulsecities"
BACKUP_DIR="/var/backups/pulsecities"
SCRATCH_DB="pulsecities_restore_test"
RESULT="/var/log/pulsecities/backup_restore_test.json"
MIN_FREE_GB=22
CORE_TABLES=(complaints_raw permits_raw evictions_raw violations_raw ownership_raw operators)
ROW_FLOOR_RATIO=0.80   # restored count must be >= this fraction of live

DATABASE_URL=$(grep -E '^DATABASE_URL=' "$APP_DIR/.env" | cut -d= -f2-)

_now() { date -u '+%Y-%m-%dT%H:%M:%SZ'; }

write_result() {
    # write_result <status> <detail> [tables_json]
    local status="$1" detail="$2" tables="${3:-{\}}"
    cat > "$RESULT" <<EOF
{
  "checked_at": "$(_now)",
  "status": "$status",
  "detail": "$detail",
  "backup_file": "${LATEST:-none}",
  "tables": $tables
}
EOF
    echo "$(_now) restore-test: $status — $detail"
}

cleanup() {
    sudo -u postgres dropdb --if-exists "$SCRATCH_DB" >/dev/null 2>&1 || true
}
trap cleanup EXIT

LATEST=$(ls -t "$BACKUP_DIR"/pulsecities_*.sql.gz 2>/dev/null | head -1)
if [ -z "${LATEST:-}" ]; then
    write_result "fail" "no backup file found in $BACKUP_DIR"
    exit 1
fi

# 1. Integrity: a truncated or corrupt dump fails here before we touch the DB.
if ! gzip -t "$LATEST" 2>/dev/null; then
    write_result "fail" "gzip integrity check failed on $(basename "$LATEST")"
    exit 1
fi

# 2. Disk gate: a full restore needs headroom for the scratch copy.
FREE_GB=$(df --output=avail -BG / | tail -1 | tr -dc '0-9')
if [ "${FREE_GB:-0}" -lt "$MIN_FREE_GB" ]; then
    write_result "skipped" "insufficient disk: ${FREE_GB}GB free, need ${MIN_FREE_GB}GB (gzip integrity passed)"
    exit 0
fi

# 3. Real restore into a throwaway database.
cleanup
if ! sudo -u postgres createdb "$SCRATCH_DB" 2>/dev/null; then
    write_result "fail" "could not create scratch database $SCRATCH_DB"
    exit 1
fi

if ! zcat "$LATEST" | sudo -u postgres psql -q -v ON_ERROR_STOP=1 -d "$SCRATCH_DB" >/dev/null 2>/tmp/restore_test_err.log; then
    write_result "fail" "restore aborted: $(tail -1 /tmp/restore_test_err.log | tr '"' "'" | cut -c1-200)"
    exit 1
fi

# 4. Row-count sanity: restored core tables must be non-empty and within the
#    floor of the live counts (the backup is < 24h old, so they track closely).
tables_json="{"
first=1
all_ok=1
for tbl in "${CORE_TABLES[@]}"; do
    restored=$(sudo -u postgres psql -tAc "SELECT COUNT(*) FROM $tbl" -d "$SCRATCH_DB" 2>/dev/null | tr -dc '0-9')
    live=$(psql "$DATABASE_URL" -tAc "SELECT COUNT(*) FROM $tbl" 2>/dev/null | tr -dc '0-9')
    restored=${restored:-0}; live=${live:-0}
    ok="true"
    # non-empty, and >= floor * live (guard against live=0)
    if [ "$restored" -eq 0 ]; then
        ok="false"; all_ok=0
    elif [ "$live" -gt 0 ]; then
        floor=$(awk "BEGIN{printf \"%d\", $live * $ROW_FLOOR_RATIO}")
        if [ "$restored" -lt "$floor" ]; then ok="false"; all_ok=0; fi
    fi
    [ $first -eq 0 ] && tables_json+=","
    tables_json+="\n    \"$tbl\": {\"restored\": $restored, \"live\": $live, \"ok\": $ok}"
    first=0
done
tables_json+="\n  }"

if [ "$all_ok" -eq 1 ]; then
    write_result "pass" "restored $(basename "$LATEST") and verified ${#CORE_TABLES[@]} core tables" "$(echo -e "$tables_json")"
    exit 0
else
    write_result "fail" "restored but one or more core tables below row floor" "$(echo -e "$tables_json")"
    exit 1
fi
