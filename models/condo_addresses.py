"""
Recovered addresses for condo unit lots that PLUTO does not carry.

17,086 deed BBLs are unit lots (xxxx1001 and up) with no parcels row, so a
quarter of the deed record joins to no address. The unit lot shares its tax
block with the condo's billing lot (xxxx7501 and up), which PLUTO does carry.
Where a block holds exactly one billing lot with an address, every unit lot on
that block belongs to that condo and inherits its address. Where the block
holds several condos, nothing is guessed; those wait for DOF's PAD file, which
maps unit lots to billing lots authoritatively.

Derived data, rebuilt nightly by scripts/refresh_condo_addresses.py from
whatever ownership_raw and parcels contain. Never edited by hand.
"""

from datetime import datetime

from sqlalchemy import DateTime, Index, String

from models.base import Base, utcnow
from sqlalchemy.orm import Mapped, mapped_column


class CondoUnitAddress(Base):
    # Not TimestampMixin: the mixin brings a serial id primary key, and this
    # table's natural key is the unit-lot BBL. Timestamp columns are declared
    # here with the mixin's own defaults instead.
    __tablename__ = "condo_unit_addresses"

    # The unit lot, e.g. 1007381001. One row per recovered lot.
    bbl: Mapped[str] = mapped_column(String(10), primary_key=True)

    # The billing lot whose address this is.
    billing_bbl: Mapped[str] = mapped_column(String(10), nullable=False)

    address: Mapped[str] = mapped_column(String(255), nullable=False)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)

    # 'block_billing' for the single-condo-block inference; 'pad' when the
    # DOF mapping lands. PAD rows win on conflict.
    source: Mapped[str] = mapped_column(String(20), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    __table_args__ = (
        Index("idx_condo_unit_addresses_billing", "billing_bbl"),
    )

    def __repr__(self) -> str:
        return f"<CondoUnitAddress bbl={self.bbl} via={self.source}>"
