"""
DOB building permit raw records.
Source: NYC Open Data dataset ipu4-2q9a — daily updates.
Append-only raw table — records are never modified after insert.
Upsert key: (bbl, filing_date, permit_type, work_type)
"""

from datetime import date

from sqlalchemy import Date, Index, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class PermitRaw(TimestampMixin, Base):
    __tablename__ = "permits_raw"

    # BBL normalized to canonical 10-digit form at ingest
    bbl: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # BIN (Building Identification Number) — alternate property key from DOB
    bin: Mapped[str | None] = mapped_column(String(7), nullable=True)

    address: Mapped[str | None] = mapped_column(String(200), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)
    borough: Mapped[str | None] = mapped_column(String(20), nullable=True)

    permit_type: Mapped[str | None] = mapped_column(String(10), nullable=True)
    work_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    owner_name: Mapped[str | None] = mapped_column(String(200), nullable=True)

    filing_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    expiration_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    job_description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Full raw API response for this record — never lose source data
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint(
            "bbl", "filing_date", "permit_type", "work_type",
            name="uq_permits_raw_bbl_date_type_work",
        ),
        Index("idx_permits_raw_bbl", "bbl"),
        Index("idx_permits_raw_filing_date", "filing_date"),
        Index("idx_permits_raw_zip_code", "zip_code"),
        Index("idx_permits_raw_created_at", "created_at"),  # for 90-day cleanup
    )

    def __repr__(self) -> str:
        return f"<PermitRaw bbl={self.bbl} type={self.permit_type} date={self.filing_date}>"
