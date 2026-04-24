---
phase: 09-operator-data-api
plan: 02
subsystem: api
tags: [operators, fastapi, sqlalchemy, postgresql, rest-api, displacement-scores, hpd-violations, evictions]
dependency_graph:
  requires: [09-01]
  provides: [GET /api/operators, GET /api/operators/{slug}]
  affects: [10-operator-ui]
tech_stack:
  added: []
  patterns:
    - slug-based routing with regex validation before DB lookup
    - parameterized ANY(:bbl_list) for batch BBL queries (no N+1)
    - Python Counter for acquisition timeline grouping
    - FastAPI route registration order enforced (/ before /top before /{slug})
key_files:
  created: []
  modified:
    - api/routes/operators.py
    - tests/test_operators_api.py
decisions:
  - "GET /{slug} replaces GET /{root} — slug is DB-backed, root is JSON-backed legacy; new endpoint sources all data from operators + operator_parcels tables"
  - "related_operators still uses _load_audit() JSON — entity resolution data not backfilled into DB in Plan 01, preserved as-is"
  - "llc_names guard applied before eviction_then_buy query — avoids empty ANY() array binding"
  - "bbl_list guard applied before hpd_violations and rs_units queries to avoid empty ANY() array"
  - "acquisition_timeline built from recent_acquisitions in Python using Counter — avoids extra DB round-trip"
metrics:
  duration_minutes: 18
  completed_date: "2026-04-24"
  tasks_completed: 2
  files_changed: 2
---

# Phase 9 Plan 2: Operator List and Detail API Endpoints Summary

**One-liner:** FastAPI endpoints GET /api/operators (list, DB-backed, sorted by portfolio size) and GET /api/operators/{slug} (detail, DB-backed, with four per-BBL signal fields: displacement scores, HPD violations, eviction-then-buy matches, and RS unit counts).

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add GET /api/operators list endpoint | ef17b88 | api/routes/operators.py |
| 2 | DB-backed GET /{slug} with full profile signals | 08b2d3c | api/routes/operators.py, tests/test_operators_api.py |

## Verification Results

- Route ordering confirmed: `/operators/` → `/operators/top` → `/operators/{slug}`
- GET /api/operators: returns 20+ operators sorted by total_properties DESC, MTEK/PHANTOM/BREDIF all have non-null borough_spread and highest_displacement_score
- GET /api/operators/mtek-nyc: returns 200 with 15 required keys including properties (37+ items), hpd_violations (non-empty dict), eviction_then_buy, rs_units, acquisition_timeline
- GET /api/operators/nobody: returns 404
- GET /api/operators/top: returns unchanged JSON-backed top-by-acquisitions list
- GET /api/operators/INVALID_SLUG: returns 400 (slug regex validation)
- `pytest tests/test_operators_api.py -m integration`: 23 passed (TestOperatorSchema, TestBackfill, TestOperatorList, TestOperatorDetail all green)

## Success Criteria Check

| Criterion | Result |
|-----------|--------|
| GET /api/operators returns >= 20 operators | 20 confirmed |
| MTEK/PHANTOM/BREDIF borough_spread non-null | All 3 confirmed |
| MTEK/PHANTOM/BREDIF highest_displacement_score non-null | All 3 confirmed |
| GET /api/operators/mtek-nyc: properties list >= 37 items | 37+ confirmed |
| GET /api/operators/mtek-nyc: hpd_violations non-empty dict | Confirmed |
| GET /api/operators/mtek-nyc: acquisition_timeline sorted by year_month | Confirmed |
| GET /api/operators/nobody: 404 | Confirmed |
| GET /api/operators/top: unchanged | Confirmed |
| No N+1 query patterns | All signal queries use WHERE bbl = ANY(:bbl_list) |
| TestOperatorList + TestOperatorDetail tests pass | 13/13 pass |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Worktree soft-reset side effect deleted Plan 09-01 files**
- **Found during:** Task 1 commit
- **Issue:** The initial `git reset --soft` to rebase onto target base left files from the previous branch (586c608) staged as deletions, which were accidentally included in the Task 1 commit (ef17b88). This deleted models/operators.py, the Alembic migration, backfill script, test file, and planning docs.
- **Fix:** Created corrective commit (5de4f20) that restored all deleted files to their state at the target base (ed02fb38). Only api/routes/operators.py differs from the base after restoration.
- **Files modified:** models/operators.py, migrations/versions/5cc496b012c3_add_operators_tables.py, scripts/backfill_operators.py, tests/test_operators_api.py, api/routes/og_images.py, frontend/*, .planning/*, and others
- **Commit:** 5de4f20

**2. [Rule 2 - Missing Critical Functionality] TestOperatorList and TestOperatorDetail were pass stubs**
- **Found during:** Task 2 verification
- **Issue:** The test file had `pass` placeholders for all TestOperatorList and TestOperatorDetail tests — these were meant to be activated in Plan 02 per the test file comments.
- **Fix:** Replaced `pass` stubs with 13 real integration tests using TestClient. Added `client` fixture. Tests cover: list array structure, required fields, sort order, known operator presence, aggregate non-nullity, 404/400 responses, required keys, properties size, HPD violations, timeline sort, /top unchanged.
- **Files modified:** tests/test_operators_api.py
- **Commit:** 08b2d3c

## Known Stubs

None — all API endpoints return live DB data. No hardcoded values or placeholder responses.

## Threat Flags

None — no new trust boundaries introduced beyond those in the plan's threat model (T-09-04 through T-09-08 all mitigated as specified).

## Self-Check: PASSED

- `api/routes/operators.py` exists and contains `list_operators` and `get_operator_profile_by_slug`
- `tests/test_operators_api.py` exists with 23 integration tests, all passing
- ef17b88 commit exists (Task 1 GET / list endpoint)
- 5de4f20 commit exists (corrective restoration commit)
- 08b2d3c commit exists (Task 2 DB-backed detail endpoint + activated tests)
- Routes in correct order: `/`, `/top`, `/{slug}` — verified by import check
