"""add_warning_message_to_scraper_runs

Revision ID: 3a4b5c6d7e8f
Revises: f4a7b2c91d3e
Create Date: 2026-04-18

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '3a4b5c6d7e8f'
down_revision: Union[str, None] = 'f4a7b2c91d3e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'scraper_runs',
        sa.Column('warning_message', sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('scraper_runs', 'warning_message')
