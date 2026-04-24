"""add_operators_tables

Revision ID: 5cc496b012c3
Revises: 1538c4ec936b
Create Date: 2026-04-24 01:35:00.233483

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '5cc496b012c3'
down_revision: Union[str, None] = '1538c4ec936b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'operators',
        sa.Column('slug', sa.String(length=100), nullable=False),
        sa.Column('operator_root', sa.String(length=50), nullable=False),
        sa.Column('display_name', sa.String(length=200), nullable=False),
        sa.Column('llc_entities', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('total_properties', sa.Integer(), nullable=False),
        sa.Column('total_acquisitions', sa.Integer(), nullable=False),
        sa.Column('borough_spread', sa.Integer(), nullable=True),
        sa.Column('highest_displacement_score', sa.Float(), nullable=True),
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('operator_root', name='uq_operators_root'),
        sa.UniqueConstraint('slug', name='uq_operators_slug'),
    )
    op.create_index('idx_operators_slug', 'operators', ['slug'], unique=False)

    op.create_table(
        'operator_parcels',
        sa.Column('operator_id', sa.Integer(), nullable=False),
        sa.Column('bbl', sa.String(length=10), nullable=False),
        sa.Column('acquiring_entity', sa.String(length=200), nullable=True),
        sa.Column('acquisition_date', sa.Date(), nullable=True),
        sa.Column('acquisition_price', sa.Numeric(precision=15, scale=2), nullable=True),
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['operator_id'], ['operators.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('operator_id', 'bbl', name='uq_operator_parcels_op_bbl'),
    )
    op.create_index('idx_operator_parcels_bbl', 'operator_parcels', ['bbl'], unique=False)
    op.create_index('idx_operator_parcels_operator_id', 'operator_parcels', ['operator_id'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_operator_parcels_operator_id', table_name='operator_parcels')
    op.drop_index('idx_operator_parcels_bbl', table_name='operator_parcels')
    op.drop_table('operator_parcels')
    op.drop_index('idx_operators_slug', table_name='operators')
    op.drop_table('operators')
