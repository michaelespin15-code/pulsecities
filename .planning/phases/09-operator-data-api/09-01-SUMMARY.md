---
phase: 09-operator-data-api
plan: 01
subsystem: database
tags: [operators, alembic, backfill, sqlalchemy, postgresql]
dependency_graph:
  requires: []
  provides: [operators-table, operator-parcels-table, backfill-script]
  affects: [09-02-PLAN.md, 09-03-PLAN.md]
tech_stack:
  added: []
  patterns: [ON CONFLICT DO UPDATE upsert, ON CONFLICT DO NOTHING idempotent insert, JSONB CAST for psycopg2 binding]
key_files:
  created:
    - models/operators.py
    - migrations/versions/5cc496b012c3_add_operators_tables.py
    - scripts/backfill_operators.py
    - tests/test_operators_api.py
  modified:
    - models/__init__.py
decisions:
  - "PHANTOM operator_root in JSON is 'PHANTOM' (not 'PHANTOM CAPITAL') — slug override maps PHANTOM -> phantom-capital"
  - "JSONB cast uses CAST(:llc_entities AS jsonb) instead of :param::jsonb to avoid psycopg2 parameter binding conflict"
  - "Migration cleaned of autogenerate noise (dropped assessment_history drops, index changes) — only creates operators tables"
metrics:
  duration_minutes: 12
  completed_date: "2026-04-24"
  tasks_completed: 3
  files_changed: 4
---

# Phase 9 Plan 1: Operator Tables, Migration, and Backfill Summary

**One-liner:** SQLAlchemy models + Alembic migration creating operators and operator_parcels tables, backfilled from operator_network_analysis.json with 20 operators and 1088 BBL-level parcel assignments, with cached borough_spread and highest_displacement_score aggregates.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Wave-0 test scaffold | a9722d4 | tests/test_operators_api.py |
| 2 | SQLAlchemy models | fd9c994 | models/operators.py, models/__init__.py |
| 3 | Alembic migration + backfill script | 1c633c4 | migrations/versions/5cc496b012c3_add_operators_tables.py, scripts/backfill_operators.py |

## Verification Results

- `python -c "from models.operators import Operator, OperatorParcel; print('OK')"` — PASSED
- `alembic current` — shows `5cc496b012c3 (head)`
- `python scripts/backfill_operators.py` (idempotent re-run) — "Seeded 20 operators, 1088 parcels total."
- `pytest tests/test_operators_api.py -m integration -k "TestOperatorSchema or TestBackfill" -q` — 9 passed

## Success Criteria Check

| Criterion | Result |
|-----------|--------|
| operators table has 20 rows | 20 rows confirmed |
| operator_parcels >= 167 rows | 1088 rows |
| slug mtek-nyc present | confirmed |
| slug phantom-capital present | confirmed |
| slug bredif present | confirmed |
| highest_displacement_score MTEK ~71.5 | 71.8 |
| borough_spread MTEK = 2 | 2 (Brooklyn + Queens) |
| No duplicate (operator_id, bbl) pairs | 0 duplicates |
| All TestBackfill + TestOperatorSchema tests pass | 9/9 passed |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] JSONB parameter binding syntax error**
- **Found during:** Task 3 (first backfill run)
- **Issue:** SQL used `:llc_entities::jsonb` which psycopg2 interprets as two tokens — the named parameter `:llc_entities` followed by `::jsonb` cast, causing a SyntaxError at the colon
- **Fix:** Changed to `CAST(:llc_entities AS jsonb)` which is unambiguous with SQLAlchemy's parameter binding
- **Files modified:** scripts/backfill_operators.py
- **Commit:** 1c633c4

**2. [Rule 1 - Bug] Alembic autogenerate migration noise**
- **Found during:** Task 3 (migration generation)
- **Issue:** Autogenerate included DROP TABLE assessment_history, DROP INDEX calls for complaints/ownership, and a score_history index change — all false positives from model vs DB drift
- **Fix:** Manually edited migration file to include only the operators and operator_parcels CREATE TABLE statements
- **Files modified:** migrations/versions/5cc496b012c3_add_operators_tables.py
- **Commit:** 1c633c4

**3. [Discovery] PHANTOM operator_root is "PHANTOM" not "PHANTOM CAPITAL"**
- **Found during:** Task 3 (inspecting operator_network_analysis.json)
- **Issue:** Plan references "PHANTOM CAPITAL" but the JSON has `operator_root: "PHANTOM"` with 64 total_properties
- **Fix:** Slug override map uses "PHANTOM" as the key, mapping to "phantom-capital"; display_name function returns "Phantom Capital" for that root
- **Files modified:** scripts/backfill_operators.py

## Known Stubs

None — all plan goals achieved. Placeholder test methods in TestOperatorList, TestOperatorDetail, and TestGroupedSearch have `pass` bodies by design (activated in Plans 02 and 03 respectively).

## Threat Flags

None — no new network endpoints or auth paths introduced in this plan.

## Self-Check: PASSED

- models/operators.py: FOUND
- migrations/versions/5cc496b012c3_add_operators_tables.py: FOUND
- scripts/backfill_operators.py: FOUND
- tests/test_operators_api.py: FOUND
- Commit a9722d4: FOUND
- Commit fd9c994: FOUND
- Commit 1c633c4: FOUND
