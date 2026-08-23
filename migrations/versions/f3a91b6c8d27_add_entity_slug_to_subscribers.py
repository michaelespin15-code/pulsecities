"""add entity_slug to subscribers

A follow on one exact buyer entity, the thing /llc/{slug} is a page for.

That template is the site's second-biggest organic landing surface: 192 search
landings in the fortnight to 2026-08-23 against /property's 543, across 651
pages in the sitemap. It had no capture of any kind. The family follow
(c7f2b4a91e83) looks like it covers this and does not: _FOLLOW_CARD renders
only inside entity_family_page, so it reaches the 26 published portfolios and
none of the individual LLCs, and most LLCs never cluster into a family at all.

There is no entity table to point a foreign key at. Entities exist only as
distinct party_name_normalized values in ownership_raw, and the page resolves
a slug by applying the same normalisation in SQL, so the slug is validated
against that at write time and the digest resolves it the same way. An entity
that leaves the deed record resolves to nothing and its followers are skipped
rather than errored, which is the failure mode the family follow already has.

The partial unique index mirrors the family one: one row per email per entity,
NULLs excluded so the other subscription kinds are untouched.

Revision ID: f3a91b6c8d27
Revises: d4b8a2c7e9f1
Create Date: 2026-08-23

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'f3a91b6c8d27'
down_revision: Union[str, None] = 'd4b8a2c7e9f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('subscribers', sa.Column('entity_slug', sa.String(length=200), nullable=True))
    op.create_index('idx_subscribers_entity_slug', 'subscribers', ['entity_slug'])
    op.create_index(
        'uq_subscribers_email_entity',
        'subscribers',
        ['email', 'entity_slug'],
        unique=True,
        postgresql_where=sa.text('entity_slug IS NOT NULL'),
    )


def downgrade() -> None:
    op.drop_index('uq_subscribers_email_entity', table_name='subscribers')
    op.drop_index('idx_subscribers_entity_slug', table_name='subscribers')
    op.drop_column('subscribers', 'entity_slug')
