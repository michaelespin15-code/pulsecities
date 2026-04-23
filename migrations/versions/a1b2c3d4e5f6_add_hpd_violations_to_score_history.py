"""add_hpd_violations_to_score_history

Adds hpd_violations nullable float column to score_history, replacing the
dormant assessment_spike slot in the 8% weight position.

assessment_spike column is retained in score_history for schema continuity —
it will remain NULL going forward. The hpd_violations column captures Class
B+C HPD violations (90-day inspection window, 3+ unit parcels) once the
violations scraper populates violations_raw.

Revision ID: a1b2c3d4e5f6
Revises: f4a7b2c91d3e
Create Date: 2026-04-22
"""

from alembic import op
import sqlalchemy as sa

revision = 'a1b2c3d4e5f6'
down_revision = 'f4a7b2c91d3e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'score_history',
        sa.Column('hpd_violations', sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('score_history', 'hpd_violations')
