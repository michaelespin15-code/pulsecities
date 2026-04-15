"""
HPD housing violation raw records.
Source: NYC Open Data dataset wvxf-dwi5 — daily updates.
Append-only raw table.
Upsert key: violation_id (HPD assigns a unique violation number)

Class B and C violations are documented landlord harassment tactics.
Priority signal for displacement scoring — higher weight than 311 complaints.
"""

from datetime import date

from sqlalchemy import Date, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class ViolationRaw(TimestampMixin, Base):
    __tablename__ = "violations_raw"

    # HPD's own violation identifier — natural upsert key
    violation_id: Mapped[str] = mapped_column(String(20), nullable=False)

    # BBL normalized to canonical 10-digit form at ingest
    bbl: Mapped[str | None] = mapped_column(String(10), nullable=True)

    address: Mapped[str | None] = mapped_column(String(200), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)
    borough: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Violation class: A (non-hazardous), B (hazardous), C (immediately hazardous)
    # B and C are the displacement signal — weight these heavily in scoring
    violation_class: Mapped[str | None] = mapped_column(String(1), nullable=True)
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)

    inspection_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    nov_issued_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    current_status: Mapped[str | None] = mapped_column(String(50), nullable=True)

    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint("violation_id", name="uq_violations_raw_violation_id"),
        Index("idx_violations_raw_bbl", "bbl"),
        Index("idx_violations_raw_zip_code", "zip_code"),
        Index("idx_violations_raw_class", "violation_class"),
        Index("idx_violations_raw_inspection_date", "inspection_date"),
        Index("idx_violations_raw_created_at", "created_at"),  # for 90-day cleanup
    )

    def __repr__(self) -> str:
        return f"<ViolationRaw id={self.violation_id} class={self.violation_class} bbl={self.bbl}>"
