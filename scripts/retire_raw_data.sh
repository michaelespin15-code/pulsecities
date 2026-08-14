#!/usr/bin/env bash
# Retire raw_data from complaints_raw and violations_raw.
#
# Why: raw_data is 87% of every complaints row and 79% of every violations row,
# stored inline in the heap because the tuples sit under the 2KB TOAST
# threshold (toast_tuple_target cannot help below it; measured 2026-08-14).
# Nothing reads either column, both datasets stay fetchable from Socrata by
# their stable keys, and every scan of the site's two biggest tables drags the
# dead weight through the buffer cache. Expected result: complaints heap
# 8.1GB -> ~1.1GB, violations 3.5GB -> ~1.1GB.
#
# Two phases so the risky part can be scheduled:
#
#   retire_raw_data.sh archive   safe any time; writes verified zstd archives
#   retire_raw_data.sh drop      the maintenance window; refuses to run
#                                without verified archives from `archive`
#
# ORDER MATTERS at the window: deploy the code commit that removes raw_data
# from the models and scrapers, reload gunicorn, THEN run `drop`. The ORM
# references the column until that commit lands, and a dropped column under a
# live model 500s every query that touches it.
#
# Rollback: the archives are ndjson keyed by unique_key / violation_id.
# scripts snippet in docs/ops/raw_data_retirement.md restores them into a side
# table if a future feature wants the payloads back.

set -euo pipefail

ARCHIVE_DIR=/var/backups/pulsecities/raw_data_archive
STAMP=$(date -u +%Y%m%d)
PSQL="sudo -u postgres psql -d pulsecities -v ON_ERROR_STOP=1"

archive_one() {
    local table=$1 key=$2
    local out="$ARCHIVE_DIR/${table}_raw_data_${STAMP}.ndjson.zst"
    local expected got
    expected=$($PSQL -tAc "SELECT count(*) FROM ${table} WHERE raw_data IS NOT NULL")
    echo "[$table] archiving ${expected} payloads -> ${out}"
    $PSQL -tAc "COPY (
        SELECT jsonb_build_object('key', ${key}, 'raw', raw_data)
        FROM ${table} WHERE raw_data IS NOT NULL ORDER BY id
    ) TO STDOUT" | zstd -q -3 -o "$out" -f
    got=$(zstd -dcq "$out" | wc -l)
    if [ "$got" != "$expected" ]; then
        echo "[$table] VERIFY FAILED: archive holds $got rows, table holds $expected" >&2
        exit 1
    fi
    sha256sum "$out" >> "$ARCHIVE_DIR/MANIFEST"
    echo "[$table] verified: $got rows, $(du -h "$out" | cut -f1)"
}

cmd_archive() {
    mkdir -p "$ARCHIVE_DIR"
    archive_one complaints_raw unique_key
    archive_one violations_raw violation_id
    if command -v rclone >/dev/null && rclone listremotes 2>/dev/null | grep -q .; then
        remote=$(rclone listremotes | head -1)
        echo "uploading archives to ${remote}"
        rclone copy "$ARCHIVE_DIR" "${remote}pulsecities-backups/raw_data_archive/" --include "*_${STAMP}.ndjson.zst"
    else
        echo "NOTE: rclone remote not found; archives are local-only until uploaded" >&2
    fi
    echo "archive phase complete. Manifest: $ARCHIVE_DIR/MANIFEST"
}

cmd_drop() {
    # Gate: refuse without verified archives for both tables.
    for t in complaints_raw violations_raw; do
        ls "$ARCHIVE_DIR"/${t}_raw_data_*.ndjson.zst >/dev/null 2>&1 || {
            echo "no archive found for $t; run '$0 archive' first" >&2; exit 1; }
    done
    # Gate: refuse while the code still references the column.
    if grep -rq "raw_data" /root/pulsecities/models/complaints.py /root/pulsecities/models/violations.py; then
        echo "models still declare raw_data; deploy the code commit and reload first" >&2
        exit 1
    fi

    echo "sizes before:"
    $PSQL -c "SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) FROM pg_class WHERE relname IN ('complaints_raw','violations_raw')"

    for t in complaints_raw violations_raw; do
        echo "[$t] dropping column and rewriting (table locked for the rewrite)..."
        $PSQL -c "ALTER TABLE $t DROP COLUMN raw_data;"
        time $PSQL -c "VACUUM FULL ANALYZE $t;"
    done

    echo "sizes after:"
    $PSQL -c "SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) FROM pg_class WHERE relname IN ('complaints_raw','violations_raw')"
    echo "done. Buffer cache will repopulate over the next day; hit ratio should climb."
}

case "${1:-}" in
    archive) cmd_archive ;;
    drop)    cmd_drop ;;
    *) echo "usage: $0 archive|drop" >&2; exit 1 ;;
esac
