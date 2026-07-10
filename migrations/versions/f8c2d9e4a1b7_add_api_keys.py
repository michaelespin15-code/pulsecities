"""api_keys for partner access to the public API

Keys are stored as SHA-256 hashes; the plaintext exists only in the
mint script's output. The public tier stays keyless, so this table
carries partner and newsroom keys with usage visibility.

Revision ID: f8c2d9e4a1b7
Revises: e6b1c8d3f2a4
Create Date: 2026-07-10

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'f8c2d9e4a1b7'
down_revision: Union[str, None] = 'e6b1c8d3f2a4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'api_keys',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('key_hash', sa.String(length=64), nullable=False, unique=True),
        sa.Column('label', sa.String(length=120), nullable=False),
        sa.Column('owner_email', sa.String(length=254), nullable=False),
        sa.Column('tier', sa.String(length=20), nullable=False, server_default='partner'),
        sa.Column('active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('last_used_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )
    op.create_index('idx_api_keys_key_hash', 'api_keys', ['key_hash'])


def downgrade() -> None:
    op.drop_index('idx_api_keys_key_hash', table_name='api_keys')
    op.drop_table('api_keys')
