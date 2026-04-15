"""
Parcel model — individual property at BBL level.
Source: MapPLUTO (primary) + DOF assessment data.
units_res is critical — used for per-unit normalization in the score engine.
"""

from typing import Any

from geoalchemy2 import Geometry
from sqlalchemy import Boolean, Float, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class Parcel(TimestampMixin, Base):
    __tablename__ = "parcels"

    # BBL — canonical 10-digit zero-padded, universal join key
    bbl: Mapped[str] = mapped_column(String(10), nullable=False)
    borough: Mapped[int | None] = mapped_column(Integer, nullable=True)
    block: Mapped[str | None] = mapped_column(String(5), nullable=True)
    lot: Mapped[str | None] = mapped_column(String(4), nullable=True)

    address: Mapped[str | None] = mapped_column(String(200), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)

    # Unit counts — required for per-unit score normalization (Phase 4)
    # A zero here is a data gap, not zero units; handled with borough median fallback
    units_res: Mapped[int | None] = mapped_column(Integer, nullable=True)
    units_total: Mapped[int | None] = mapped_column(Integer, nullable=True)

    year_built: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lot_area: Mapped[float | None] = mapped_column(Float, nullable=True)
    bldg_area: Mapped[float | None] = mapped_column(Float, nullable=True)
    zoning_dist: Mapped[str | None] = mapped_column(String(20), nullable=True)
    land_use: Mapped[str | None] = mapped_column(String(2), nullable=True)

    # Owner info from PLUTO
    owner_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    owner_type: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # HPD Speculation Watch List flag
    on_speculation_watch_list: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )

    # Assessed value from DOF
    assessed_total: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Parcel centroid point — used for spatial joins and map rendering
    geometry: Mapped[Any] = mapped_column(
        Geometry("POINT", srid=4326), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("bbl", name="uq_parcels_bbl"),
        Index("idx_parcels_bbl", "bbl"),
        Index("idx_parcels_zip_code", "zip_code"),
        Index("idx_parcels_geometry", "geometry", postgresql_using="gist"),
    )

    def __repr__(self) -> str:
        return f"<Parcel bbl={self.bbl} address={self.address}>"
