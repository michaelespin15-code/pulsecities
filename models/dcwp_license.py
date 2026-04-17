"""
DCWP (Department of Consumer and Worker Protection) Business Licenses model.
Dataset: w7w3-xahh (DCWP Issued Licenses — NYC Open Data)

Stores licensed businesses across the 48 DCWP-regulated trade categories.
Primary use: contractor license correlation for renovation-flip signal detection.

Note: DCWP only covers 48 regulated categories (home improvement contractors,
tow trucks, tobacco dealers, etc.) — NOT restaurants, retail, or most businesses.
"""

from datetime import date, datetime

from sqlalchemy import Date, Float, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class DcwpLicense(TimestampMixin, Base):
    __tablename__ = "dcwp_licenses"

    license_nbr: Mapped[str | None] = mapped_column(String(30), nullable=True)
    business_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    dba_trade_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    business_category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    license_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    license_creation_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    lic_expir_dd: Mapped[date | None] = mapped_column(Date, nullable=True)
    address_building: Mapped[str | None] = mapped_column(String(20), nullable=True)
    address_street_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    address_zip: Mapped[str | None] = mapped_column(String(10), nullable=True)
    address_borough: Mapped[str | None] = mapped_column(String(50), nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    bbl: Mapped[str | None] = mapped_column(String(10), nullable=True)
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint("license_nbr", name="uq_dcwp_license_nbr"),
        Index("idx_dcwp_licenses_zip", "address_zip"),
        Index("idx_dcwp_licenses_category", "business_category"),
        Index("idx_dcwp_licenses_creation_date", "license_creation_date"),
    )
