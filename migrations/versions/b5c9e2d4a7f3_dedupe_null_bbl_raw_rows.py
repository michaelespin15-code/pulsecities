"""dedupe_null_bbl_raw_rows

The unique keys on evictions_raw and permits_raw include bbl, and Postgres
treats NULLs as distinct in unique constraints, so NULL-bbl rows bypassed
ON CONFLICT entirely. The nightly scraper re-inserted every NULL-bbl eviction
in its lookback window each run: 21,960 rows for 3,558 distinct events, which
inflated per-ZIP eviction counts everywhere they are aggregated. Permits had
the same hole at much smaller scale (150 duplicate rows).

Deletes the duplicates (keeping the earliest row per identity, preserving the
original ingest timestamp) and adds COALESCE-based unique indexes that close
the NULL hole. The scrapers switch to bare on_conflict_do_nothing() so any
unique violation, old constraint or new index, is absorbed.

Revision ID: b5c9e2d4a7f3
Revises: a9c4d2e7b8f1
Create Date: 2026-07-10

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'b5c9e2d4a7f3'
down_revision: Union[str, None] = 'a9c4d2e7b8f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Duplicates can only exist among NULL-bbl rows; the existing constraints
    # already cover non-null keys. Keep the earliest row per identity.
    op.execute("""
        DELETE FROM evictions_raw e
        USING evictions_raw d
        WHERE e.bbl IS NULL AND d.bbl IS NULL
          AND e.executed_date = d.executed_date
          AND e.docket_number = d.docket_number
          AND e.id > d.id
    """)
    op.execute("""
        DELETE FROM permits_raw e
        USING permits_raw d
        WHERE e.bbl IS NULL AND d.bbl IS NULL
          AND e.filing_date = d.filing_date
          AND e.permit_type = d.permit_type
          AND e.work_type = d.work_type
          AND e.id > d.id
    """)
    op.execute("""
        CREATE UNIQUE INDEX uq_evictions_raw_identity
        ON evictions_raw (COALESCE(bbl, ''), executed_date, docket_number)
    """)
    op.execute("""
        CREATE UNIQUE INDEX uq_permits_raw_identity
        ON permits_raw (COALESCE(bbl, ''), filing_date, permit_type, work_type)
    """)


def downgrade() -> None:
    # Deleted duplicate rows are not restorable; only the indexes revert.
    op.execute("DROP INDEX IF EXISTS uq_evictions_raw_identity")
    op.execute("DROP INDEX IF EXISTS uq_permits_raw_identity")
