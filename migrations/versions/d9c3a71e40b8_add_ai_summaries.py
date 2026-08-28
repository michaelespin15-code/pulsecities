"""ai_summaries: the panel read, cached where every worker can see it

Revision ID: d9c3a71e40b8
Revises: c8f4b16d29ea
Create Date: 2026-08-28

The read was cached in a per-process dict. Two consequences, both of which the
reader paid for: each gunicorn worker held its own copy, so the first visitor to
a ZIP on each worker waited out a full generation, and every reload threw all of
it away. On a day with eight deploys that is eight cold starts per ZIP.

Keyed on the score the summary was written against, so a re-scored ZIP
regenerates and an unchanged one never does.
"""
import sqlalchemy as sa
from alembic import op

revision = "d9c3a71e40b8"
down_revision = "c8f4b16d29ea"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ai_summaries",
        sa.Column("zip_code", sa.String(5), primary_key=True),
        sa.Column("score_key", sa.Integer, nullable=False),
        # The score itself, not only its rounded key, because freshness is a
        # judgement about whether the prose would differ and not about whether
        # the number moved. See api.routes.ai_summary.is_fresh.
        sa.Column("score", sa.Float, nullable=True),
        sa.Column("summary", sa.Text, nullable=False),
        sa.Column("model", sa.String(64), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
    )


def downgrade() -> None:
    op.drop_table("ai_summaries")
