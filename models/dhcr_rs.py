"""
DHCR Rent Stabilization Buildings model.
Dataset: yn95-5t2d (DHCR RS Buildings — NYC Open Data)

Stores annual rent-stabilized unit counts per BBL.
Enables year-over-year RS unit loss computation per ZIP code —
the single most predictive displacement signal.

Field mapping (Socrata → model):
  bbl        → bbl (10-digit, normalized via normalize_bbl)
  year       → year (integer — registration year)
  unitsstab  → rs_unit_count (RS unit count for that year)
"""

from datetime import datetime

from sqlalchemy import Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class RsBuilding(TimestampMixin, Base):
    __tablename__ = "rs_buildings"

    bbl: Mapped[str | None] = mapped_column(String(10), nullable=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rs_unit_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint("bbl", "year", name="uq_rs_buildings_bbl_year"),
        Index("idx_rs_buildings_bbl", "bbl"),
        Index("idx_rs_buildings_year", "year"),
    )
