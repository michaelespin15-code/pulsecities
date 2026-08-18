"""relax raw_data to nullable on complaints_raw and violations_raw

Step one of retiring the column (see docs/ops/raw_data_retirement.md and
scripts/retire_raw_data.sh). It exists to remove a gap in the documented
order, which was: deploy the code that stops writing raw_data, reload, then
drop the column. Both columns are NOT NULL with no server default and both
scrapers build plain dicts for a Core insert, so between that deploy and the
drop every insert omits a NOT NULL column. The nightly scrape runs at 02:00;
any window spanning it would have failed the run outright.

Dropping NOT NULL first decouples the two. It is a catalogue-only change --
no table rewrite, no scan of the 9GB heap -- so it is safe to apply on a live
system, and afterwards the code deploy can happen whenever, with new rows
simply carrying NULL until the column goes.

The drop itself is the next migration and wants a maintenance window, because
reclaiming the space needs VACUUM FULL.

Revision ID: a1f4c07b9e52
Revises: c7d1e93b4a26
Create Date: 2026-08-18

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = 'a1f4c07b9e52'
down_revision: Union[str, None] = 'c7d1e93b4a26'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLES = ("complaints_raw", "violations_raw")


def upgrade() -> None:
    for table in TABLES:
        op.alter_column(table, "raw_data", existing_type=postgresql.JSONB(),
                        nullable=True)


def downgrade() -> None:
    # Going back means every row needs a value again. Rows written after the
    # upgrade carry NULL, so backfill them to an empty object rather than
    # letting the NOT NULL re-add fail on a table nobody can repair by hand.
    for table in TABLES:
        op.execute(sa.text(
            f"UPDATE {table} SET raw_data = '{{}}'::jsonb WHERE raw_data IS NULL"
        ))
        op.alter_column(table, "raw_data", existing_type=postgresql.JSONB(),
                        nullable=False)
