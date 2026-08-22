"""add condo_unit_addresses

Recovered addresses for the condo unit lots PLUTO does not carry: 17,086 deed
BBLs, a quarter of the deed record, joined to no address at all. Where a tax
block holds exactly one condo billing lot with an address, every unit lot on
that block inherits it (4,399 lots today); ambiguous blocks wait for DOF's
PAD mapping and are never guessed.

Derived and rebuilt nightly by scripts/refresh_condo_addresses.py, so the
downgrade is a plain drop.

Revision ID: d4b8a2c7e9f1
Revises: c7f2b4a91e83
Create Date: 2026-08-19

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'd4b8a2c7e9f1'
down_revision: Union[str, None] = 'c7f2b4a91e83'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'condo_unit_addresses',
        sa.Column('bbl', sa.String(length=10), nullable=False),
        sa.Column('billing_bbl', sa.String(length=10), nullable=False),
        sa.Column('address', sa.String(length=255), nullable=False),
        sa.Column('zip_code', sa.String(length=5), nullable=True),
        sa.Column('source', sa.String(length=20), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('bbl'),
    )
    op.create_index('idx_condo_unit_addresses_billing', 'condo_unit_addresses', ['billing_bbl'])


def downgrade() -> None:
    op.drop_index('idx_condo_unit_addresses_billing', table_name='condo_unit_addresses')
    op.drop_table('condo_unit_addresses')
