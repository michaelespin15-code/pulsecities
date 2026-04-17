"""add_rs_unit_loss_to_score_history

Adds rs_unit_loss nullable float column to score_history so the Phase 6-06
signal is captured in historical snapshots alongside the other five signals.
Previously the composite_score included rs_unit_loss weight but the column
was absent, making per-signal history unverifiable.

Revision ID: f4a7b2c91d3e
Revises: c3e8f1a042bd
Create Date: 2026-04-17

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'f4a7b2c91d3e'
down_revision: Union[str, None] = 'c3e8f1a042bd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'score_history',
        sa.Column('rs_unit_loss', sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('score_history', 'rs_unit_loss')
