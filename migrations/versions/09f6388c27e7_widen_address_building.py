"""widen_address_building

Revision ID: 09f6388c27e7
Revises: 16380c5c8a85
Create Date: 2026-04-15 02:30:57.756970

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '09f6388c27e7'
down_revision: Union[str, None] = '16380c5c8a85'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('dcwp_licenses', 'address_building',
                    type_=sa.String(length=200),
                    existing_type=sa.String(length=20),
                    existing_nullable=True)


def downgrade() -> None:
    op.alter_column('dcwp_licenses', 'address_building',
                    type_=sa.String(length=20),
                    existing_type=sa.String(length=200),
                    existing_nullable=True)
