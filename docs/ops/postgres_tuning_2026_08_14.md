# Postgres tuning, 2026-08-14

Applied because the buffer cache hit ratio sat at 59% with 115M disk reads:
`shared_buffers` was still the packaged 128MB default against a 16GB database.

## Applied (via ALTER SYSTEM, lands in postgresql.auto.conf)

| Setting                | Was     | Now    | Restart? | Why |
|------------------------|---------|--------|----------|-----|
| shared_buffers         | 128MB   | 512MB  | yes      | ~13% of the box's 3.9GB. The default caches almost nothing of a 16GB working set. |
| effective_cache_size   | 4GB     | 2GB    | no       | Was larger than the machine's total RAM, so the planner assumed a cache that cannot exist. |
| work_mem               | 4MB     | 16MB   | no       | Sorts on score_history spilled to disk. Only 4 connections are ever open, so the worst case is bounded. |
| maintenance_work_mem   | 64MB    | 256MB  | no       | VACUUM and index builds on the 9GB complaints table. |
| random_page_cost       | 4.0     | 1.1    | no       | Spinning-disk default on SSD-backed storage; it pushed the planner toward sequential scans. |

## Rollback

    sudo -u postgres psql -c "ALTER SYSTEM RESET shared_buffers;" \
      -c "ALTER SYSTEM RESET effective_cache_size;" -c "ALTER SYSTEM RESET work_mem;" \
      -c "ALTER SYSTEM RESET maintenance_work_mem;" -c "ALTER SYSTEM RESET random_page_cost;"
    systemctl restart postgresql && systemctl reload pulsecities

## Also dropped, same day

Two indexes on complaints_raw with zero scans across 27 days of accumulated
stats covering every nightly ingest. Roughly 499MB, and every insert was
maintaining them for nothing. Recreate statements are in
`dropped_indexes_2026_08_14.sql` if a query ever needs them.
