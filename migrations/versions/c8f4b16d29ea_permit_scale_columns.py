"""job cost and dwelling-unit change on permits_raw

The permit signal counts alteration permits on buildings with 3+ residential
units and treats them all alike. Measured on the 80,308 DOB NOW alterations in
the last 365 days, the median job is $30,960 and the 10th percentile is $1,000,
so half the signal is repair work rather than the renovation pressure the signal
is meant to read.

DOB NOW carries the three fields that separate them and BIS carries none of
them, which is stated here because it is a real consequence rather than a
footnote: any predicate on these columns excludes every legacy row. That is
1.3% of the current signal and it is the biased remnant diagnosed in
docs/CHECKPOINT.md, so losing it is a gain, but it should be lost on purpose.

    job_cost         initial_cost, 100% populated and numeric on DOB NOW
    units_existing   existing_dwelling_units, 89% populated
    units_proposed   proposed_dwelling_units, same

The second pair is the more interesting one. 1,985 alteration jobs in the last
year propose fewer dwelling units than the building already has, which is
housing being removed rather than a proxy for it, and nothing on the site reads
it yet.

Backfilled from permits_raw.raw_data, which already holds the full Socrata row.

Guarded and idempotent, for the reason given in a7d3f1e08b64: b8e30d5c1746 is
still waiting on a maintenance window and this should not wait with it.

Revision ID: c8f4b16d29ea
Revises: b2e5c93a17df
Create Date: 2026-08-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c8f4b16d29ea"
down_revision: Union[str, None] = "b2e5c93a17df"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "permits_raw"
COLUMNS = {
    "job_cost": "numeric(14,2)",
    "units_existing": "integer",
    "units_proposed": "integer",
}

# Socrata sends these as text and a blank or "N/A" is common, so the backfill
# only reads values that are actually numeric rather than casting and failing
# the whole statement on one bad row.
BACKFILL = """
    UPDATE permits_raw SET
        job_cost = NULLIF(raw_data->>'initial_cost', '')::numeric,
        units_existing  = NULLIF(raw_data->>'existing_dwelling_units', '')::int,
        units_proposed  = NULLIF(raw_data->>'proposed_dwelling_units', '')::int
    WHERE source = 'dob_now'
      AND job_cost IS NULL
      AND (raw_data->>'initial_cost') ~ '^[0-9]+(\\.[0-9]+)?$'
      AND (raw_data->>'existing_dwelling_units') ~ '^[0-9]+$'
      AND (raw_data->>'proposed_dwelling_units') ~ '^[0-9]+$'
"""

# The 11% of rows with a cost but no usable unit pair still want their cost.
BACKFILL_COST_ONLY = """
    UPDATE permits_raw SET job_cost = NULLIF(raw_data->>'initial_cost', '')::numeric
    WHERE source = 'dob_now' AND job_cost IS NULL
      AND (raw_data->>'initial_cost') ~ '^[0-9]+(\\.[0-9]+)?$'
"""


def upgrade() -> None:
    have = {c["name"] for c in sa.inspect(op.get_bind()).get_columns(TABLE)}
    for name, ddl_type in COLUMNS.items():
        if name not in have:
            op.execute(f"ALTER TABLE {TABLE} ADD COLUMN {name} {ddl_type}")
    op.execute(BACKFILL)
    op.execute(BACKFILL_COST_ONLY)

    with op.get_context().autocommit_block():
        existing = {i["name"] for i in sa.inspect(op.get_bind()).get_indexes(TABLE)}
        if "idx_permits_unit_loss" not in existing:
            # Small: 1,985 rows a year qualify citywide.
            op.execute("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permits_unit_loss
                ON permits_raw (filing_date, bbl)
                WHERE units_proposed < units_existing
            """)
        op.execute(f"ANALYZE {TABLE}")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_permits_unit_loss")
    for name in COLUMNS:
        op.execute(f"ALTER TABLE {TABLE} DROP COLUMN IF EXISTS {name}")
