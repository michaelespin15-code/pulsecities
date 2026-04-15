"""
Displacement score models — pre-computed nightly, never on-request.

DisplacementScore: neighborhood (zip code) level composite score
PropertyScore: individual parcel (BBL) level score

Scores are recomputed from raw tables each nightly run.
cache_generated_at tracks when this row was last recalculated.
signal_breakdown stores raw per-signal values for API transparency.
"""

from datetime import datetime

from sqlalchemy import DateTime, Float, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class DisplacementScore(TimestampMixin, Base):
    """
    Composite 1-100 displacement risk score per zip code.
    Recomputed nightly after all scrapers complete.
    """

    __tablename__ = "displacement_scores"

    zip_code: Mapped[str] = mapped_column(String(5), nullable=False)

    # Composite score 0-100
    score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Full per-signal breakdown for API transparency
    # Shape: {"permits": 0.0, "evictions": 0.0, "llc_acquisitions": 0.0,
    #         "assessment_spike": 0.0, "complaint_rate": 0.0}
    signal_breakdown: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Individual signal values (denormalized from signal_breakdown for query performance)
    permit_intensity: Mapped[float | None] = mapped_column(Float, nullable=True)
    eviction_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    llc_acquisition_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    assessment_spike: Mapped[float | None] = mapped_column(Float, nullable=True)
    complaint_rate: Mapped[float | None] = mapped_column(Float, nullable=True)

    # When the score was last recomputed — shown in UI as data freshness
    cache_generated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Per-signal last_updated for data freshness display in the UI
    # Shape: {"permits": "2026-04-10T02:00:00Z", "evictions": "2026-04-08T02:00:00Z", ...}
    signal_last_updated: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint("zip_code", name="uq_displacement_scores_zip_code"),
        Index("idx_displacement_scores_zip_code", "zip_code"),
        Index("idx_displacement_scores_score", "score"),
    )

    def __repr__(self) -> str:
        return f"<DisplacementScore zip={self.zip_code} score={self.score}>"


class PropertyScore(TimestampMixin, Base):
    """
    Composite displacement score per individual BBL.
    Supports the block drill-down API endpoint (Phase 5).
    """

    __tablename__ = "property_scores"

    bbl: Mapped[str] = mapped_column(String(10), nullable=False)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)

    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    signal_breakdown: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    cache_generated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("bbl", name="uq_property_scores_bbl"),
        Index("idx_property_scores_bbl", "bbl"),
        Index("idx_property_scores_zip_code", "zip_code"),
        Index("idx_property_scores_score", "score"),
    )

    def __repr__(self) -> str:
        return f"<PropertyScore bbl={self.bbl} score={self.score}>"
