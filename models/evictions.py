"""
Eviction filing raw records.
Source: NYC Open Data dataset 6z8x-wfk4 — weekly updates.
Append-only raw table.
Upsert key: (bbl, executed_date, docket_number)

IMPORTANT: OCA (Office of Court Administration) reports lag actual filing
date by 2-4 weeks. This is a known characteristic of this dataset, not a
data quality failure. Display data_freshness with this caveat in the UI.

Evictions are a lagging indicator — displacement is already happening by
the time it appears here. Weight accordingly in the score engine.
"""

from datetime import date

from sqlalchemy import Date, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class EvictionRaw(TimestampMixin, Base):
    __tablename__ = "evictions_raw"

    # BBL normalized to canonical 10-digit form at ingest
    bbl: Mapped[str | None] = mapped_column(String(10), nullable=True)

    docket_number: Mapped[str | None] = mapped_column(String(30), nullable=True)
    address: Mapped[str | None] = mapped_column(String(200), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)
    borough: Mapped[str | None] = mapped_column(String(20), nullable=True)
    eviction_type: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # executed_date = date marshal physically executed the eviction
    executed_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    # OCA reporting lag: this date lags executed_date by 2-4 weeks
    # Store it for freshness tracking and methodology transparency
    court_index_number: Mapped[str | None] = mapped_column(String(30), nullable=True)

    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint(
            "bbl", "executed_date", "docket_number",
            name="uq_evictions_raw_bbl_date_docket",
        ),
        Index("idx_evictions_raw_bbl", "bbl"),
        Index("idx_evictions_raw_zip_code", "zip_code"),
        Index("idx_evictions_raw_executed_date", "executed_date"),
        Index("idx_evictions_raw_created_at", "created_at"),  # for 90-day cleanup
    )

    def __repr__(self) -> str:
        return f"<EvictionRaw bbl={self.bbl} date={self.executed_date}>"
