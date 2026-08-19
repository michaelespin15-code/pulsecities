"""drop raw_data from complaints_raw and violations_raw

Step two, and the one that wants a maintenance window. DROP COLUMN itself is
instant -- Postgres only marks the attribute dead -- but the space is not
returned until the heap is rewritten, and VACUUM FULL takes an ACCESS EXCLUSIVE
lock for the duration. On this hardware that is the multi-GB rewrite of the
site's two biggest tables, so run it when nobody is reading.

    ./venv/bin/python -m alembic upgrade head     # this migration
    scripts/retire_raw_data.sh drop               # the VACUUM FULL

Preconditions, both enforced by that script before it will do anything:
verified archives exist for both tables, and no model still declares the
column. As of a1f4c07b9e52 the column is nullable and nothing writes it, so
the system is already in a safe steady state and this can wait for a good
night rather than being rushed.

Expected: complaints_raw 9.3GB -> ~1.1GB, violations_raw 3.9GB -> ~1.1GB, and
the nightly dump falls with them.

There is no honest downgrade. The payloads live in the zstd ndjson archives
listed in that directory's MANIFEST; docs/ops/raw_data_retirement.md has the
restore snippet, which loads them into a side table keyed by unique_key /
violation_id. Recreating an empty column would satisfy alembic and quietly
lose 7.2 million payloads, so this refuses instead.

Revision ID: b8e30d5c1746
Revises: c7f2b4a91e83
Create Date: 2026-08-18

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'b8e30d5c1746'
down_revision: Union[str, None] = 'c7f2b4a91e83'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLES = ("complaints_raw", "violations_raw")


def upgrade() -> None:
    for table in TABLES:
        op.drop_column(table, "raw_data")


def downgrade() -> None:
    raise RuntimeError(
        "raw_data cannot be restored by a migration. Reload the payloads from "
        "/var/backups/pulsecities/raw_data_archive/ using the snippet in "
        "docs/ops/raw_data_retirement.md; recreating the column empty would "
        "look like a rollback and lose 7.2M records."
    )
