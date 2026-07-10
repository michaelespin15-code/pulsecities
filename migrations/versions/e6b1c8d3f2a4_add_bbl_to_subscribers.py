"""subscribers.bbl for building-level watch alerts

A subscriber row now watches exactly one of: a ZIP, the city, an
operator cluster, or a single building. bbl references parcels.bbl
informally (no FK: parcel rows are refreshed by the DOF scrape and we
never want that to cascade into subscriber deletions).

Revision ID: e6b1c8d3f2a4
Revises: b5c9e2d4a7f3
Create Date: 2026-07-10

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'e6b1c8d3f2a4'
down_revision: Union[str, None] = 'b5c9e2d4a7f3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('subscribers', sa.Column('bbl', sa.String(length=10), nullable=True))
    op.create_index('idx_subscribers_bbl', 'subscribers', ['bbl'])
    # One watch per (email, building); partial so NULLs from the other
    # subscription types stay out of the constraint.
    op.create_index(
        'uq_subscribers_email_bbl',
        'subscribers',
        ['email', 'bbl'],
        unique=True,
        postgresql_where=sa.text('bbl IS NOT NULL'),
    )


def downgrade() -> None:
    op.drop_index('uq_subscribers_email_bbl', table_name='subscribers')
    op.drop_index('idx_subscribers_bbl', table_name='subscribers')
    op.drop_column('subscribers', 'bbl')
