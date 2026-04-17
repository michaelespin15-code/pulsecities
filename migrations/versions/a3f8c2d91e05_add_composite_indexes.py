"""add_composite_indexes

Composite indexes for the five most common query patterns:
  - permits_raw(zip_code, filing_date)       — pulse query per-ZIP date range
  - evictions_raw(zip_code, executed_date)   — pulse + scoring per-ZIP date range
  - complaints_raw(complaint_type, created_date) — scoring complaint-type + date range
  - ownership_raw(party_type, doc_date)      — scoring grantee LLC date range
  - ownership_raw(bbl, doc_date)             — pulse LLC join + date range

Revision ID: a3f8c2d91e05
Revises: 1068d4a0da32
Create Date: 2026-04-15

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'a3f8c2d91e05'
down_revision: Union[str, None] = '1068d4a0da32'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "idx_permits_raw_zip_filing",
        "permits_raw", ["zip_code", "filing_date"],
    )
    op.create_index(
        "idx_evictions_raw_zip_executed",
        "evictions_raw", ["zip_code", "executed_date"],
    )
    op.create_index(
        "idx_complaints_raw_type_date",
        "complaints_raw", ["complaint_type", "created_date"],
    )
    op.create_index(
        "idx_ownership_raw_party_type_date",
        "ownership_raw", ["party_type", "doc_date"],
    )
    op.create_index(
        "idx_ownership_raw_bbl_date",
        "ownership_raw", ["bbl", "doc_date"],
    )


def downgrade() -> None:
    op.drop_index("idx_permits_raw_zip_filing", table_name="permits_raw")
    op.drop_index("idx_evictions_raw_zip_executed", table_name="evictions_raw")
    op.drop_index("idx_complaints_raw_type_date", table_name="complaints_raw")
    op.drop_index("idx_ownership_raw_party_type_date", table_name="ownership_raw")
    op.drop_index("idx_ownership_raw_bbl_date", table_name="ownership_raw")
