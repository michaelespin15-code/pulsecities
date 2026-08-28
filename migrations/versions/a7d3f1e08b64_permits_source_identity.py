"""give permits_raw a source and a per-source identity

permits_raw was written by one scraper reading one dataset, so its identity was
(bbl, filing_date, permit_type, work_type) and that was enough. It is about to
be written by two.

The reason is in docs/CHECKPOINT.md: scrapers/permits.py reads ipu4-2q9a, the
legacy DOB Permit Issuance dataset, and DOB NOW superseded it. We hold 6,501
permits for the last 365 days against roughly 170,000 upstream, and after the
scoring filter the permit signal is 414 records carrying 24.7% of the composite
score. The fix is a second scraper on DOB NOW, and DOB NOW rows carry a real
identity of their own: `job_filing_number`.

Two sources cannot share one identity rule. Two different DOB NOW jobs on the
same lot, permitted the same day, with the same job type collide under the old
key even though they are different jobs with different filing numbers, and
`on_conflict_do_nothing` would drop the second one without a word. That is the
silent-row-loss class this codebase has paid for before, so each source gets the
identity it actually has:

    source_id IS NULL      legacy BIS rows, keyed as before on
                           (COALESCE(bbl,''), filing_date, permit_type, work_type)
    source_id IS NOT NULL  DOB NOW rows, keyed on (source, source_id)

Both new indexes are built CONCURRENTLY, so this does not lock the table. The
old ones are dropped only after the replacements exist.

Guarded, the same way c4e17b2a9d38 is and for the same reason: this revision
sits after b8e30d5c1746, which drops raw_data from the two largest tables and is
still waiting for a maintenance window. Shipping the permit fix should not
require taking that window first, so the DDL was applied directly when the
scraper shipped and this skips whatever it finds already in place. On a fresh
database the chain runs in order and this builds everything normally.

Revision ID: a7d3f1e08b64
Revises: c4e17b2a9d38
Create Date: 2026-08-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a7d3f1e08b64"
down_revision: Union[str, None] = "c4e17b2a9d38"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "permits_raw"
BIS_IDENTITY = "uq_permits_raw_identity_bis"
NOW_IDENTITY = "uq_permits_raw_source_id"
OLD_INDEX = "uq_permits_raw_identity"
OLD_CONSTRAINT = "uq_permits_raw_bbl_date_type_work"


def _columns() -> set:
    return {c["name"] for c in sa.inspect(op.get_bind()).get_columns(TABLE)}


def _indexes() -> set:
    bind = op.get_bind()
    names = {i["name"] for i in sa.inspect(bind).get_indexes(TABLE)}
    names |= {r[0] for r in bind.execute(sa.text(
        "SELECT conname FROM pg_constraint WHERE conrelid = 'permits_raw'::regclass"))}
    return names


def upgrade() -> None:
    have = _columns()
    if "source" not in have:
        # PG 11+ stores the default in the catalog, so this does not rewrite the
        # 745k-row heap. Existing rows are BIS rows by definition: they are the
        # only ones that existed before DOB NOW was scraped.
        op.execute(f"ALTER TABLE {TABLE} ADD COLUMN source varchar(16) "
                   f"NOT NULL DEFAULT 'dob_bis'")
    if "source_id" not in have:
        op.execute(f"ALTER TABLE {TABLE} ADD COLUMN source_id varchar(40)")

    # CONCURRENTLY cannot run inside a transaction block, and alembic wraps the
    # migration in one. autocommit_block lifts that for these four statements.
    with op.get_context().autocommit_block():
        have = _indexes()
        if NOW_IDENTITY not in have:
            op.execute(f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {NOW_IDENTITY} "
                       f"ON {TABLE} (source, source_id) WHERE source_id IS NOT NULL")
        if BIS_IDENTITY not in have:
            op.execute(f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {BIS_IDENTITY} "
                       f"ON {TABLE} (COALESCE(bbl, ''), filing_date, permit_type, "
                       f"work_type) WHERE source_id IS NULL")
        # Only now that both replacements exist.
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {OLD_INDEX}")
        op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT IF EXISTS {OLD_CONSTRAINT}")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {OLD_INDEX} "
                   f"ON {TABLE} (COALESCE(bbl, ''), filing_date, permit_type, work_type)")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {BIS_IDENTITY}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {NOW_IDENTITY}")
    op.drop_column(TABLE, "source_id")
    op.drop_column(TABLE, "source")
