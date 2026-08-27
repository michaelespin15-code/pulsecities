"""
New York Department of State registration for entities that appear in the deed
record.

The gap this closes is the one the search data is loudest about. The 2026-08-27
Search Console export put /llc at position 5 to 9 on roughly 1,700 impressions
of deed-research queries that convert at zero, and reading them shows why: they
are not asking what the company bought, which is the question the page answers.
They ask who is behind it. "873 east 228th street llc registered agent bronx".
"324 east 86th street llc beneficial owner". "ownership and controlling party
1832 fulton street llc brooklyn ny". A deed cannot answer any of those, because
New York does not require an LLC to name a member on one.

DOS can, partly. It carries the DOS ID, the filing date, the jurisdiction the
company was formed in, and the address and name designated for service of
process. That last field is the useful one and it needs reading carefully:

  - Most numbered LLCs designate themselves, which says nothing.
  - Some designate a commercial registered-agent service, which says only that
    they paid for privacy.
  - The rest designate a real third party, and that is the answer people are
    searching for. All 82 FLGSP shells that took 4,823 rent-stabilized units in
    one day on 2026-03-31 designate SUMMIT MALLS MANAGEMENT LLC at 1350 6th
    Avenue, across six different spellings of the same name.

Selective by design. The full dataset is 4.2M rows and we care about the 23,444
LLC-form names that appear as deed buyers, of which 78% resolve. Storing the
matches only keeps this near 18,000 rows instead of adding a gigabyte to a
database already at 69% of the disk.

Source: data.ny.gov dataset n9v6-gdp6, "Active Corporations: Beginning 1800".
Filled by scripts/refresh_dos_entities.py. Never edited by hand.
"""

from datetime import date, datetime

from sqlalchemy import Date, DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, utcnow


class DosEntity(Base):
    # Natural key is the DOS ID, which is what a reader can look up in the
    # state's own system. No serial id.
    __tablename__ = "dos_entities"

    dos_id: Mapped[str] = mapped_column(String(16), primary_key=True)

    # As DOS spells it, and as we match it. The normalized form is the join key
    # to ownership_raw.party_name_normalized, so a lookup never depends on the
    # two sources agreeing about punctuation.
    entity_name: Mapped[str] = mapped_column(String(255), nullable=False)
    entity_name_normalized: Mapped[str] = mapped_column(String(255), nullable=False)

    entity_type: Mapped[str | None] = mapped_column(String(80), nullable=True)
    # "New York" for a domestic company, "Delaware" and the rest for a foreign
    # one registered to do business here. A Delaware LLC holding one Brooklyn
    # building is itself worth showing.
    jurisdiction: Mapped[str | None] = mapped_column(String(60), nullable=True)
    county: Mapped[str | None] = mapped_column(String(40), nullable=True)
    initial_filing_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    # Designated for service of process. agent_is_self and agent_is_commercial
    # are computed at write time so the page can decide what to say without
    # re-deriving the rule per render.
    agent_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    agent_address: Mapped[str | None] = mapped_column(String(255), nullable=True)
    agent_city: Mapped[str | None] = mapped_column(String(80), nullable=True)
    agent_state: Mapped[str | None] = mapped_column(String(2), nullable=True)
    agent_zip: Mapped[str | None] = mapped_column(String(10), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    __table_args__ = (
        # The join every page does: normalized name to deed party name.
        Index("idx_dos_entities_name_norm", "entity_name_normalized"),
        # "who else designates this agent", the second independent signal for
        # entity-family clustering beyond the deed filing address.
        Index("idx_dos_entities_agent", "agent_name"),
    )

    def __repr__(self) -> str:
        return f"<DosEntity {self.dos_id} {self.entity_name}>"
