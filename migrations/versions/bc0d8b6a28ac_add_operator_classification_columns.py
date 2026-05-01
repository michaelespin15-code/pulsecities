"""add_operator_classification_columns

Four columns to operators table to store the output of the promotion gate:
  operator_class            TEXT NOT NULL DEFAULT 'review'
  classification_reasons    JSONB NOT NULL DEFAULT '[]'
  classification_confidence NUMERIC(4,3)
  classified_at             TIMESTAMP WITH TIME ZONE

All additions are nullable or have defaults, so this migration is safe to run
against the existing prod table without a table lock or downtime.

Revision ID: bc0d8b6a28ac
Revises: 16d8c55f1fb6
Create Date: 2026-05-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = 'bc0d8b6a28ac'
down_revision: Union[str, None] = '16d8c55f1fb6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'operators',
        sa.Column('operator_class', sa.Text(), nullable=False, server_default='review'),
    )
    op.add_column(
        'operators',
        sa.Column(
            'classification_reasons',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default='[]',
        ),
    )
    op.add_column(
        'operators',
        sa.Column('classification_confidence', sa.Numeric(4, 3), nullable=True),
    )
    op.add_column(
        'operators',
        sa.Column('classified_at', sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('operators', 'classified_at')
    op.drop_column('operators', 'classification_confidence')
    op.drop_column('operators', 'classification_reasons')
    op.drop_column('operators', 'operator_class')
