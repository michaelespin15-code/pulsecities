"""scoring_changes: when the score moved because we changed, not because the city did

Revision ID: e2b71c4d9a35
Revises: d9c3a71e40b8
Create Date: 2026-08-28

The weekly digest told subscribers "87 ZIP codes moved by 3+ points this week".
Every one of those moves came from recomputing the permit signal after the DOB
NOW backfill, not from anything that happened in New York. A displacement tracker
reporting its own correction as the city's news is the same failure the block
digest already has a guard for, where a backfill of 485,000 historical permits
was about to be announced as new activity.

A row here says the scores moved for our reasons on that date, and the digest
reads it before it attributes movement to the record.
"""
import sqlalchemy as sa
from alembic import op

revision = "e2b71c4d9a35"
down_revision = "d9c3a71e40b8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scoring_changes",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("changed_on", sa.Date, nullable=False),
        sa.Column("summary", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
    )
    op.create_index("idx_scoring_changes_changed_on", "scoring_changes", ["changed_on"])


def downgrade() -> None:
    op.drop_index("idx_scoring_changes_changed_on", table_name="scoring_changes")
    op.drop_table("scoring_changes")
