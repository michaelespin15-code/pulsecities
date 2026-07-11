"""complaints_rollup_index

Performance and hygiene pass on indexes:

1. complaints_raw (created_date, zip_code) partial index:
   The 30-day complaint rollup in /api/stats and /api/neighborhoods/top-risk
   groups recent complaints by ZIP. The existing indexes either lead with
   complaint_type (unusable here) or cover created_date alone, forcing heap
   fetches across the 4.7M-row table — about 3 seconds cold. The composite
   makes it a single index-only range scan.

2. Drop two redundant indexes:
   - idx_api_keys_key_hash duplicates the index behind the api_keys.key_hash
     unique constraint.
   - idx_subscribers_email is a prefix of uq_subscribers_email_zip, which
     already serves email-only lookups.
   Both cost write amplification on every insert and buy nothing.

Revision ID: b9e4f2a7c1d8
Revises: a3d7e1f9c5b2
Create Date: 2026-07-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b9e4f2a7c1d8'
down_revision: Union[str, None] = 'a3d7e1f9c5b2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'idx_complaints_raw_date_zip',
        'complaints_raw',
        ['created_date', 'zip_code'],
        postgresql_where=sa.text('zip_code IS NOT NULL'),
    )
    op.drop_index('idx_api_keys_key_hash', table_name='api_keys')
    op.drop_index('idx_subscribers_email', table_name='subscribers')


def downgrade() -> None:
    op.create_index('idx_subscribers_email', 'subscribers', ['email'])
    op.create_index('idx_api_keys_key_hash', 'api_keys', ['key_hash'])
    op.drop_index('idx_complaints_raw_date_zip', table_name='complaints_raw')
