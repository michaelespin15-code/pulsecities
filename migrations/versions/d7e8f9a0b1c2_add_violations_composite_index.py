"""add_violations_composite_index

Two performance fixes for /api/neighborhoods/top-risk and /api/stats cold-cache latency:

1. violations_raw composite index (violation_class, inspection_date, zip_code):
   Without this, PostgreSQL BitmapAnd-s two separate single-column indexes
   (1.47M rows from violation_class + 323K rows from inspection_date), producing
   many lossy bitmap pages that must be rechecked against the heap — 4+ seconds
   cold. With the composite the planner does a single index-only range scan.

2. rs_buildings (year, bbl) index:
   The rs_loss_counts self-join was doing a full seq scan of the 529K-row table
   for both sides of the join because year was only a JOIN condition, not a WHERE
   predicate. The SQL was fixed to move prior.year into WHERE so the planner can
   use this index and short-circuit when the prior year is absent (saving 800ms).

Revision ID: d7e8f9a0b1c2
Revises: 6aa0c3e6ef29
Create Date: 2026-04-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'd7e8f9a0b1c2'
down_revision: Union[str, None] = '6aa0c3e6ef29'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'idx_violations_raw_class_date_zip',
        'violations_raw',
        ['violation_class', 'inspection_date', 'zip_code'],
        postgresql_where=sa.text('zip_code IS NOT NULL'),
    )
    op.create_index(
        'idx_rs_buildings_year_bbl',
        'rs_buildings',
        ['year', 'bbl'],
    )


def downgrade() -> None:
    op.drop_index('idx_violations_raw_class_date_zip', table_name='violations_raw')
    op.drop_index('idx_rs_buildings_year_bbl', table_name='rs_buildings')
