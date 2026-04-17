"""add_assessment_history

Revision ID: 1068d4a0da32
Revises: 09f6388c27e7
Create Date: 2026-04-15 02:42:42.677442

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1068d4a0da32'
down_revision: Union[str, None] = '09f6388c27e7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "assessment_history",
        sa.Column("bbl", sa.String(), nullable=False),
        sa.Column("assessed_total", sa.Numeric(), nullable=False),
        sa.Column("tax_year", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("bbl", "tax_year", name="pk_assessment_history"),
    )
    op.create_index("idx_assessment_history_tax_year", "assessment_history", ["tax_year"])


def downgrade() -> None:
    op.drop_index("idx_assessment_history_tax_year", table_name="assessment_history")
    op.drop_table("assessment_history")
