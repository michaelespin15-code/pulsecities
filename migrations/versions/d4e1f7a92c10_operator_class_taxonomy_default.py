"""operator_class taxonomy: default unclassified

Flip operators.operator_class server default from 'review' to 'unclassified'
and reset existing rows so the new public-gate taxonomy
(operator / financial_institution / government / nonprofit_hdfc / unclassified)
is the column's meaning. scripts/classify_operators.py then writes the real
class per cluster.

Revision ID: d4e1f7a92c10
Revises: bc0d8b6a28ac
Create Date: 2026-06-11

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'd4e1f7a92c10'
down_revision: Union[str, None] = 'bc0d8b6a28ac'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('operators', 'operator_class', server_default='unclassified')
    # Reset rows still carrying the old default so the gate starts from a known
    # state; classify_operators.py assigns the real class immediately after.
    op.execute("UPDATE operators SET operator_class = 'unclassified' WHERE operator_class = 'review'")


def downgrade() -> None:
    op.alter_column('operators', 'operator_class', server_default='review')
    op.execute("UPDATE operators SET operator_class = 'review' WHERE operator_class = 'unclassified'")
