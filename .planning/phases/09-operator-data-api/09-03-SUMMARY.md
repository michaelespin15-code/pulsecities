---
phase: 09-operator-data-api
plan: 03
subsystem: api
tags: [search, operators, fastapi, grouped-search, rate-limiting]
dependency_graph:
  requires: [09-01]
  provides: [grouped-search-endpoint]
  affects: [10-operator-ui-map-layer]
tech_stack:
  added: []
  patterns: [grouped-response-shape, parameterized-sql-ilike, slowapi-rate-limit]
key_files:
  created: []
  modified:
    - api/routes/search.py
    - tests/test_operators_api.py
decisions:
  - "No doc_type filter on properties query — name ILIKE match sufficient for grouped search; doc_type filter belongs in /landlord portfolio view only"
  - "search_grouped() registered at GET '/' BEFORE /landlord to keep route ordering clean; FastAPI still routes /landlord correctly"
  - "Tests verify DB query logic directly rather than HTTP layer — no test server spun up for integration suite"
metrics:
  duration_minutes: 8
  completed_date: "2026-04-24"
  tasks_completed: 1
  files_changed: 2
---

# Phase 09 Plan 03: Grouped Search Endpoint Summary

**One-liner:** GET /api/search grouped endpoint returning operators + properties in a single response keyed by type, rate-limited 30/minute, with a 3-char minimum guard.

## What Was Built

Added `search_grouped()` to `api/routes/search.py`, registered at `GET /` on the search router (mounted at `/api/search`). The endpoint returns a grouped response containing:

- `results.operators` — up to 10 operators from the operators table, matched by `display_name ILIKE` or `operator_root ILIKE`, ordered by `total_properties DESC`
- `results.properties` — up to 20 ownership records from `ownership_raw JOIN parcels`, matched by `party_name_normalized ILIKE`, with the same mortgage servicer exclusions as `/landlord`, ordered by `doc_date DESC NULLS LAST`

Security mitigations from the threat model are all in place: parameterized bind variables prevent SQL injection, LIMIT 10/20 caps response size, 3-char minimum aborts before any DB query, and slowapi enforces 30/minute per-IP.

The existing `/landlord` endpoint was not touched.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 (RED) | TestGroupedSearch tests | 0655f22 | tests/test_operators_api.py |
| 1 (GREEN) | search_grouped endpoint | f828d71 | api/routes/search.py |

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

None.

## Threat Flags

No new threat surface introduced beyond what is in the plan's threat model. All four STRIDE threats (T-09-09 through T-09-12) are mitigated in the implementation.

## Self-Check: PASSED

- api/routes/search.py modified: FOUND
- tests/test_operators_api.py modified: FOUND
- commit 0655f22 (RED): FOUND
- commit f828d71 (GREEN): FOUND
- All 14 integration tests pass (3 TestGroupedSearch + 11 earlier)
- Routes: ['/search/', '/search/landlord'] — both registered
