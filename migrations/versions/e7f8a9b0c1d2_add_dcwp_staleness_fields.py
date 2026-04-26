"""add_dcwp_staleness_fields

Adds staleness tracking and change-detection columns to dcwp_licenses:
  - source_last_seen_at:     last time this license appeared in any API response
  - source_last_refreshed_at: last time a historical refresh deliberately rechecked this row
  - source_hash:             SHA-256 of normalized mutable source fields; changes when
                             status, expiry, name, address, or location change

These support the DCWP hybrid refresh strategy: daily incremental + periodic
chunked historical refresh to catch renewals/status changes on old licenses.

Revision ID: e7f8a9b0c1d2
Revises: 5cc496b012c3
Create Date: 2026-04-26 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'e7f8a9b0c1d2'
down_revision: Union[str, None] = '5cc496b012c3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('dcwp_licenses', sa.Column('source_last_seen_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('dcwp_licenses', sa.Column('source_last_refreshed_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('dcwp_licenses', sa.Column('source_hash', sa.String(length=64), nullable=True))
    op.create_index(
        'idx_dcwp_licenses_last_refreshed',
        'dcwp_licenses',
        ['source_last_refreshed_at'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('idx_dcwp_licenses_last_refreshed', table_name='dcwp_licenses')
    op.drop_column('dcwp_licenses', 'source_hash')
    op.drop_column('dcwp_licenses', 'source_last_refreshed_at')
    op.drop_column('dcwp_licenses', 'source_last_seen_at')
