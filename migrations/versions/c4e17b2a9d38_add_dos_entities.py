"""add dos_entities

New York Department of State registration for entities that already appear in
the deed record: DOS ID, filing date, jurisdiction, and the name and address
designated for service of process.

This exists because /llc ranks at position 5 to 9 for roughly 1,700 monthly
impressions of queries asking who controls a company, and answers none of them.
A deed names the grantee and stops, because New York does not require an LLC to
name a member on one. DOS carries the rest.

Guarded creation, deliberately. This revision sits after b8e30d5c1746, which
drops raw_data from the two largest tables and is waiting for a maintenance
window. Creating this table should not require taking that window first, so the
table was created directly when the feature shipped and this migration skips
creation when it finds it already there. On a fresh database the chain runs in
order and this builds it normally.

Revision ID: c4e17b2a9d38
Revises: b8e30d5c1746
Create Date: 2026-08-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c4e17b2a9d38"
down_revision: Union[str, None] = "b8e30d5c1746"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _exists() -> bool:
    return "dos_entities" in sa.inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if _exists():
        return
    op.create_table(
        "dos_entities",
        sa.Column("dos_id", sa.String(16), primary_key=True),
        sa.Column("entity_name", sa.String(255), nullable=False),
        sa.Column("entity_name_normalized", sa.String(255), nullable=False),
        sa.Column("entity_type", sa.String(80), nullable=True),
        sa.Column("jurisdiction", sa.String(60), nullable=True),
        sa.Column("county", sa.String(40), nullable=True),
        sa.Column("initial_filing_date", sa.Date(), nullable=True),
        sa.Column("agent_name", sa.String(255), nullable=True),
        sa.Column("agent_address", sa.String(255), nullable=True),
        sa.Column("agent_city", sa.String(80), nullable=True),
        sa.Column("agent_state", sa.String(2), nullable=True),
        sa.Column("agent_zip", sa.String(10), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("idx_dos_entities_name_norm", "dos_entities",
                    ["entity_name_normalized"])
    op.create_index("idx_dos_entities_agent", "dos_entities", ["agent_name"])


def downgrade() -> None:
    # Derived data, rebuilt from data.ny.gov by scripts/refresh_dos_entities.py.
    # Dropping it loses nothing that a re-run does not restore.
    op.drop_index("idx_dos_entities_agent", table_name="dos_entities")
    op.drop_index("idx_dos_entities_name_norm", table_name="dos_entities")
    op.drop_table("dos_entities")
