"""add_citywide_subscribers

Revision ID: 6aa0c3e6ef29
Revises: c33db6e35e32
Create Date: 2026-04-26 20:01:26.332992

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6aa0c3e6ef29'
down_revision: Union[str, None] = 'c33db6e35e32'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # zip_code must become nullable to support citywide (no-ZIP) subscribers.
    # The existing uq_subscribers_email_zip unique constraint is kept — it handles
    # ZIP-based uniqueness. PostgreSQL treats NULL != NULL in unique constraints,
    # so (email, NULL) pairs are not constrained by it. A separate partial index
    # enforces one citywide subscription per email address.
    op.alter_column("subscribers", "zip_code", nullable=True)
    op.add_column(
        "subscribers",
        sa.Column("is_citywide", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_subscribers_citywide_email "
        "ON subscribers (email) WHERE is_citywide = true"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_subscribers_citywide_email")
    op.drop_column("subscribers", "is_citywide")
    op.alter_column("subscribers", "zip_code", nullable=False)
