"""add_party_addresses_to_ownership_raw

Revision ID: b2c3d4e5f6a7
Revises: 3a4b5c6d7e8f
Create Date: 2026-04-18

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, None] = '3a4b5c6d7e8f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('ownership_raw', sa.Column('party_addr_1', sa.String(200), nullable=True))
    op.add_column('ownership_raw', sa.Column('party_addr_2', sa.String(200), nullable=True))
    op.add_column('ownership_raw', sa.Column('party_city', sa.String(100), nullable=True))
    op.add_column('ownership_raw', sa.Column('party_state', sa.String(2), nullable=True))
    op.add_column('ownership_raw', sa.Column('party_zip', sa.String(10), nullable=True))


def downgrade() -> None:
    op.drop_column('ownership_raw', 'party_zip')
    op.drop_column('ownership_raw', 'party_state')
    op.drop_column('ownership_raw', 'party_city')
    op.drop_column('ownership_raw', 'party_addr_2')
    op.drop_column('ownership_raw', 'party_addr_1')
