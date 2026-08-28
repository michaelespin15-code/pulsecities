"""cover the scoring query's permit aggregate

`_aggregate_permits` reads a 365-day window of alteration permits joined to
parcels, and it went from 414 matching rows to 32,786 when the DOB NOW scraper
landed (a7d3f1e08b64). The query itself was unchanged and its cost went from
negligible to 8.8 seconds.

The plan showed why: a bitmap heap scan over `idx_permits_raw_filing_date`
fetching 97,784 rows to keep 32,786, and permits_raw rows are 2.5KB because they
carry raw_data, so that is roughly 250MB pulled off disk (56,754 heap blocks
read, not hit) to read four narrow columns.

Covering index, so the scan never touches the heap. INCLUDE rather than a wider
key because bbl and zip_code are only ever projected here, never searched on.
Partial on the two NOT NULL predicates the query already applies. 58MB, and the
aggregate drops to 1.4s warm.

An index-only scan needs a current visibility map, so the bulk load is followed
by VACUUM ANALYZE. That is already done on the live database; a fresh one gets
it from the ordinary autovacuum.

Guarded and CONCURRENTLY, for the reason given in a7d3f1e08b64: b8e30d5c1746 is
still waiting on a maintenance window and this should not have to wait with it.

Revision ID: b2e5c93a17df
Revises: a7d3f1e08b64
Create Date: 2026-08-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "b2e5c93a17df"
down_revision: Union[str, None] = "a7d3f1e08b64"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

INDEX = "idx_permits_scoring"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        existing = {i["name"] for i in sa.inspect(op.get_bind()).get_indexes("permits_raw")}
        if INDEX not in existing:
            op.execute(f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX}
                ON permits_raw (permit_type, filing_date) INCLUDE (bbl, zip_code)
                WHERE bbl IS NOT NULL AND zip_code IS NOT NULL
            """)
        op.execute("ANALYZE permits_raw")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX}")
