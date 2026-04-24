"""
Operator registry and parcel membership tables.

operators — one row per identified operator cluster (e.g. MTEK, PHANTOM CAPITAL).
            Slug is the stable public identifier used in URLs and API routes.
operator_parcels — join table: one row per (operator, BBL) pair.
                   Backfilled from ownership_raw; refreshed nightly (future: FRESH-01).
"""

from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Float, ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class Operator(TimestampMixin, Base):
    __tablename__ = "operators"

    slug: Mapped[str] = mapped_column(String(100), nullable=False)
    operator_root: Mapped[str] = mapped_column(String(50), nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    llc_entities: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    total_properties: Mapped[int] = mapped_column(Integer, default=0)
    total_acquisitions: Mapped[int] = mapped_column(Integer, default=0)
    # Cached aggregates recomputed by backfill script
    borough_spread: Mapped[int | None] = mapped_column(Integer, nullable=True)
    highest_displacement_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("operator_root", name="uq_operators_root"),
        UniqueConstraint("slug", name="uq_operators_slug"),
        Index("idx_operators_slug", "slug"),
    )


class OperatorParcel(TimestampMixin, Base):
    __tablename__ = "operator_parcels"

    operator_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    bbl: Mapped[str] = mapped_column(String(10), nullable=False)
    acquiring_entity: Mapped[str | None] = mapped_column(String(200), nullable=True)
    acquisition_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    acquisition_price: Mapped[Decimal | None] = mapped_column(Numeric(15, 2), nullable=True)

    __table_args__ = (
        UniqueConstraint("operator_id", "bbl", name="uq_operator_parcels_op_bbl"),
        Index("idx_operator_parcels_operator_id", "operator_id"),
        Index("idx_operator_parcels_bbl", "bbl"),
    )
