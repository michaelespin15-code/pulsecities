"""add_mtek_alerts

Revision ID: 8945f5483cc9
Revises: a3f8c2d91e05
Create Date: 2026-04-16

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '8945f5483cc9'
down_revision: Union[str, None] = 'a3f8c2d91e05'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'mtek_alerts',
        sa.Column('bbl', sa.String(length=10), nullable=False),
        sa.Column('address', sa.String(length=200), nullable=True),
        sa.Column('entity', sa.String(length=100), nullable=True),
        sa.Column('alert_type', sa.String(length=20), nullable=False),
        sa.Column('violation_class', sa.String(length=1), nullable=True),
        sa.Column('event_date', sa.Date(), nullable=True),
        sa.Column('detail', sa.Text(), nullable=True),
        sa.Column('source_id', sa.String(length=50), nullable=False),
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('bbl', 'alert_type', 'source_id', name='uq_mtek_alerts_bbl_type_source'),
    )
    op.create_index('idx_mtek_alerts_bbl',        'mtek_alerts', ['bbl'])
    op.create_index('idx_mtek_alerts_event_date',  'mtek_alerts', ['event_date'])
    op.create_index('idx_mtek_alerts_alert_type',  'mtek_alerts', ['alert_type'])
    op.create_index('idx_mtek_alerts_created_at',  'mtek_alerts', ['created_at'])


def downgrade() -> None:
    op.drop_index('idx_mtek_alerts_created_at',  table_name='mtek_alerts')
    op.drop_index('idx_mtek_alerts_alert_type',  table_name='mtek_alerts')
    op.drop_index('idx_mtek_alerts_event_date',  table_name='mtek_alerts')
    op.drop_index('idx_mtek_alerts_bbl',         table_name='mtek_alerts')
    op.drop_table('mtek_alerts')
