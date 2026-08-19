"""add family_slug to subscribers

A follow on an entity family, the thing /network/{slug} is a page for. The
existing operator follow keys on operators.slug, and only three rows there are
classed 'operator', so it could never cover the 26 portfolios the site
actually publishes.

Families are computed from the deed record rather than stored, so there is no
table to point a foreign key at. The slug is validated against the clustering
at write time, and the digest resolves it the same way. A family that stops
clustering leaves rows that resolve to nothing and are skipped, which is the
same failure mode an unlisted operator already has.

The partial unique index mirrors the operator one (a9c4d2e7b8f1): one row per
email per family, and NULLs stay out of it so the other subscription kinds are
unaffected.

Revision ID: c7f2b4a91e83
Revises: a1f4c07b9e52
Create Date: 2026-08-19

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'c7f2b4a91e83'
down_revision: Union[str, None] = 'a1f4c07b9e52'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('subscribers', sa.Column('family_slug', sa.String(length=120), nullable=True))
    op.create_index('idx_subscribers_family_slug', 'subscribers', ['family_slug'])
    op.create_index(
        'uq_subscribers_email_family',
        'subscribers',
        ['email', 'family_slug'],
        unique=True,
        postgresql_where=sa.text('family_slug IS NOT NULL'),
    )


def downgrade() -> None:
    op.drop_index('uq_subscribers_email_family', table_name='subscribers')
    op.drop_index('idx_subscribers_family_slug', table_name='subscribers')
    op.drop_column('subscribers', 'family_slug')
