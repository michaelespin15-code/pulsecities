"""
Neighborhood model — zip code level aggregation unit.
Geometry column holds the zip code boundary polygon for choropleth rendering.
Boundaries loaded once from NYC ZCTA GeoJSON (separate data load step).
"""

from typing import Any

from geoalchemy2 import Geometry
from sqlalchemy import Float, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class Neighborhood(TimestampMixin, Base):
    __tablename__ = "neighborhoods"

    zip_code: Mapped[str] = mapped_column(String(5), nullable=False)
    name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    borough: Mapped[str | None] = mapped_column(String(20), nullable=True)
    borough_code: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Zip code boundary polygon — loaded from ZCTA GeoJSON
    # Nullable until geometry data is loaded
    geometry: Mapped[Any] = mapped_column(
        Geometry("MULTIPOLYGON", srid=4326), nullable=True
    )

    # Denormalized current displacement score for fast map queries
    # Recomputed nightly from displacement_scores table
    current_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("zip_code", name="uq_neighborhoods_zip_code"),
        Index("idx_neighborhoods_zip_code", "zip_code"),
        Index("idx_neighborhoods_geometry", "geometry", postgresql_using="gist"),
    )

    def __repr__(self) -> str:
        return f"<Neighborhood zip={self.zip_code} name={self.name}>"
