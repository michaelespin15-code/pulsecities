"""add trigram indexes for search

Revision ID: 16d8c55f1fb6
Revises: d7e8f9a0b1c2
Create Date: 2026-04-30

GIN trigram indexes on parcels.address and ownership_raw.party_name_normalized
so that ILIKE substring queries use index scans instead of sequential scans.
Requires pg_trgm extension (enabled here if not already present).
"""
from typing import Sequence, Union

from alembic import op

revision: str = '16d8c55f1fb6'
down_revision: Union[str, None] = 'd7e8f9a0b1c2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_parcels_address_trgm
        ON parcels USING gin (address gin_trgm_ops)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_ownership_party_name_trgm
        ON ownership_raw USING gin (party_name_normalized gin_trgm_ops)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_parcels_address_trgm")
    op.execute("DROP INDEX IF EXISTS idx_ownership_party_name_trgm")
