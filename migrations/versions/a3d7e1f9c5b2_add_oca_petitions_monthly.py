"""oca_petitions_monthly: ZIP-level housing-court petition volumes

Aggregates only. The OCA Data Collective source is deliberately
de-identified (ZIP, no street or BBL) and CC BY-NC-SA licensed, so this
table stays a display-layer early-warning signal and never feeds the
commercial API surface.

Revision ID: a3d7e1f9c5b2
Revises: f8c2d9e4a1b7
Create Date: 2026-07-10

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'a3d7e1f9c5b2'
down_revision: Union[str, None] = 'f8c2d9e4a1b7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'oca_petitions_monthly',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('zip_code', sa.String(length=5), nullable=False),
        sa.Column('month', sa.Date(), nullable=False),
        sa.Column('classification', sa.String(length=40), nullable=False),
        sa.Column('filings', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.UniqueConstraint('zip_code', 'month', 'classification', name='uq_oca_zip_month_class'),
    )
    op.create_index('idx_oca_zip_month', 'oca_petitions_monthly', ['zip_code', 'month'])


def downgrade() -> None:
    op.drop_index('idx_oca_zip_month', table_name='oca_petitions_monthly')
    op.drop_table('oca_petitions_monthly')
