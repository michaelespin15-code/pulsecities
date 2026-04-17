"""
NYC 311 complaint raw records.
Source: NYC Open Data dataset erm2-nwe9 — daily updates.
Append-only raw table.
Upsert key: unique_key (311 assigns a globally unique key per complaint)
"""

from datetime import datetime
from typing import Any

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class ComplaintRaw(TimestampMixin, Base):
    __tablename__ = "complaints_raw"

    # 311's own unique identifier — natural upsert key
    unique_key: Mapped[str] = mapped_column(String(20), nullable=False)

    # BBL normalized to canonical 10-digit form at ingest
    bbl: Mapped[str | None] = mapped_column(String(10), nullable=True)

    complaint_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    descriptor: Mapped[str | None] = mapped_column(String(200), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)
    borough: Mapped[str | None] = mapped_column(String(20), nullable=True)
    address: Mapped[str | None] = mapped_column(String(200), nullable=True)
    agency: Mapped[str | None] = mapped_column(String(10), nullable=True)
    status: Mapped[str | None] = mapped_column(String(20), nullable=True)

    created_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    closed_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Point geometry from lat/lng in the source data
    location: Mapped[Any] = mapped_column(
        Geometry("POINT", srid=4326), nullable=True
    )

    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint("unique_key", name="uq_complaints_raw_unique_key"),
        Index("idx_complaints_raw_bbl", "bbl"),
        Index("idx_complaints_raw_zip_code", "zip_code"),
        Index("idx_complaints_raw_created_date", "created_date"),
        Index("idx_complaints_raw_created_at", "created_at"),  # for 90-day cleanup
        Index("idx_complaints_raw_location", "location", postgresql_using="gist"),
    )

    def __repr__(self) -> str:
        return f"<ComplaintRaw key={self.unique_key} type={self.complaint_type}>"
