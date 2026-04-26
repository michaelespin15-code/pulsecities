"""backfill_confirmed_subscribers

Revision ID: c33db6e35e32
Revises: 5cc496b012c3
Create Date: 2026-04-26 19:32:18.687913

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c33db6e35e32'
down_revision: Union[str, None] = '5cc496b012c3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Backfill pre-double-opt-in subscribers. These 6 accounts signed up April
    # 19-20 and received two digests under the old code, which had no confirmed
    # filter. Set confirmed_at = created_at to preserve the original signup
    # timestamp as the opt-in anchor. Condition is tight: only rows that are
    # still unconfirmed with no confirmed_at are touched.
    op.execute("""
        UPDATE subscribers
        SET    confirmed    = true,
               confirmed_at = created_at
        WHERE  confirmed    = false
          AND  confirmed_at IS NULL
    """)


def downgrade() -> None:
    # Reverting is only safe if no new confirmed subscribers exist beyond the
    # original backfill. This is a best-effort rollback for development use.
    op.execute("""
        UPDATE subscribers
        SET    confirmed    = false,
               confirmed_at = NULL
        WHERE  confirmed    = true
          AND  confirmed_at = created_at
    """)

