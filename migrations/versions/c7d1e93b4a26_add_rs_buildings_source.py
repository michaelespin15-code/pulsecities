"""add_rs_buildings_source

rs_buildings holds two different datasets that mean different things:

  - DHCR rent-stabilization registrations (years 2018-2023, loaded by
    scripts/backfill_rs_history.py), where rs_unit_count is genuinely the
    count of registered rent-stabilized units.
  - NYC Open Data kj4p-ruqc, "Buildings Subject to HPD Jurisdiction"
    (loaded nightly by scrapers/dhcr_rs.py under the current calendar
    year), where the ingested field is legalclassa, defined by HPD as
    "the number of apartments in a multiple dwelling". That is total
    apartments, not stabilized units.

Nothing distinguished them, so consumers that took the most recent year
per BBL surfaced apartment counts labelled as rent-stabilized units.
This column makes the provenance explicit so every consumer can filter.

Revision ID: c7d1e93b4a26
Revises: b9e4f2a7c1d8
Create Date: 2026-08-13

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c7d1e93b4a26'
down_revision: Union[str, None] = 'b9e4f2a7c1d8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "rs_buildings",
        sa.Column("source", sa.String(length=32), nullable=True),
    )
    # Backfill by loader: the HPD cohort is everything the nightly scraper
    # wrote (it stamps the current calendar year and carries HPD's own
    # columns in raw_data); the DHCR backfill covers 2018-2023.
    op.execute(
        "UPDATE rs_buildings SET source = 'hpd_jurisdiction' "
        "WHERE year >= 2024 OR raw_data ? 'legalclassa'"
    )
    op.execute("UPDATE rs_buildings SET source = 'dhcr' WHERE source IS NULL")
    op.create_index(
        "idx_rs_buildings_source_year",
        "rs_buildings",
        ["source", "year"],
    )


def downgrade() -> None:
    op.drop_index("idx_rs_buildings_source_year", table_name="rs_buildings")
    op.drop_column("rs_buildings", "source")
