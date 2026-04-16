"""assessment_history_stabilized_units

Add stabilized_units column to assessment_history and relax assessed_total
to nullable so RS-only rows (no DOF assessment for that year) can be stored.

Revision ID: c3e8f1a042bd
Revises: 8945f5483cc9
Create Date: 2026-04-16

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c3e8f1a042bd'
down_revision: Union[str, None] = '8945f5483cc9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop NOT NULL on assessed_total — rows loaded from nycdb rentstab won't
    # have a DOF assessment value for every historical year.
    op.alter_column(
        "assessment_history",
        "assessed_total",
        existing_type=sa.Numeric(),
        nullable=True,
    )

    op.add_column(
        "assessment_history",
        sa.Column("stabilized_units", sa.Integer(), nullable=True),
    )

    op.create_index(
        "idx_assessment_history_bbl",
        "assessment_history",
        ["bbl"],
    )


def downgrade() -> None:
    op.drop_index("idx_assessment_history_bbl", table_name="assessment_history")
    op.drop_column("assessment_history", "stabilized_units")
    op.alter_column(
        "assessment_history",
        "assessed_total",
        existing_type=sa.Numeric(),
        nullable=False,
    )
