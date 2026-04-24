"""merge_party_addresses_and_hpd_violations

Revision ID: 1538c4ec936b
Revises: a1b2c3d4e5f6, b2c3d4e5f6a7
Create Date: 2026-04-22 23:49:48.365078

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1538c4ec936b'
down_revision: Union[str, None] = ('a1b2c3d4e5f6', 'b2c3d4e5f6a7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
