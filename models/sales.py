"""
DOF rolling property sales raw records.
Source: NYC Open Data dataset usep-8jbt — quarterly updates.
Append-only raw table.
Upsert key: (bbl, sale_date, sale_price)

Sale price vs neighborhood median identifies speculative purchasing.
This is a Tier 2 enrichment signal — ingested in Phase 3.
"""

from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class SaleRaw(TimestampMixin, Base):
    __tablename__ = "sales_raw"

    # BBL normalized to canonical 10-digit form at ingest
    bbl: Mapped[str | None] = mapped_column(String(10), nullable=True)

    sale_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    sale_price: Mapped[Decimal | None] = mapped_column(
        Numeric(precision=15, scale=2), nullable=True
    )

    building_class: Mapped[str | None] = mapped_column(String(10), nullable=True)
    gross_sqft: Mapped[int | None] = mapped_column(Integer, nullable=True)
    address: Mapped[str | None] = mapped_column(String(200), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)
    borough: Mapped[int | None] = mapped_column(Integer, nullable=True)

    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint(
            "bbl", "sale_date", "sale_price",
            name="uq_sales_raw_bbl_date_price",
        ),
        Index("idx_sales_raw_bbl", "bbl"),
        Index("idx_sales_raw_sale_date", "sale_date"),
        Index("idx_sales_raw_zip_code", "zip_code"),
        Index("idx_sales_raw_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<SaleRaw bbl={self.bbl} date={self.sale_date} price={self.sale_price}>"
