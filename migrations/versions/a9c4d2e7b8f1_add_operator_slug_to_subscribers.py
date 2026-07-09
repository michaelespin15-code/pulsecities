"""subscribers.operator_slug for operator-follow alerts

A subscriber row now watches exactly one of: a ZIP, the city, or an
operator cluster. operator_slug references operators.slug informally
(no FK: operator rows are rebuilt by the clustering pipeline and we
never want that to cascade into subscriber deletions).

Revision ID: a9c4d2e7b8f1
Revises: d4e1f7a92c10
Create Date: 2026-07-09

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'a9c4d2e7b8f1'
down_revision: Union[str, None] = 'd4e1f7a92c10'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('subscribers', sa.Column('operator_slug', sa.String(length=120), nullable=True))
    op.create_index('idx_subscribers_operator_slug', 'subscribers', ['operator_slug'])
    # One follow per (email, operator); partial so NULLs from ZIP/citywide
    # rows stay out of the constraint.
    op.create_index(
        'uq_subscribers_email_operator',
        'subscribers',
        ['email', 'operator_slug'],
        unique=True,
        postgresql_where=sa.text('operator_slug IS NOT NULL'),
    )


def downgrade() -> None:
    op.drop_index('uq_subscribers_email_operator', table_name='subscribers')
    op.drop_index('idx_subscribers_operator_slug', table_name='subscribers')
    op.drop_column('subscribers', 'operator_slug')
