"""
ScoreHistory model — append-only nightly snapshot of displacement scores per zip code.

One row per (zip_code, scored_at) — the UNIQUE constraint prevents duplicates.
The nightly pipeline writes one row per ZIP after compute_scores() completes.
History is retained indefinitely; no soft-delete, no updates.
"""

from datetime import date

from sqlalchemy import Date, Float, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class ScoreHistory(TimestampMixin, Base):
    """
    Nightly displacement score snapshot per zip code.

    Append-only: ON CONFLICT DO NOTHING ensures re-running the pipeline
    on the same calendar day produces no duplicate rows.
    """

    __tablename__ = "score_history"

    zip_code: Mapped[str] = mapped_column(String(5), nullable=False)
    scored_at: Mapped[date] = mapped_column(Date, nullable=False)

    # Composite 0-100 score captured at snapshot time
    composite_score: Mapped[float] = mapped_column(Float, nullable=False)

    # Per-signal values — nullable because not all signals are always available
    permit_intensity: Mapped[float | None] = mapped_column(Float, nullable=True)
    eviction_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    llc_acquisition_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    assessment_spike: Mapped[float | None] = mapped_column(Float, nullable=True)  # retained, now NULL going forward
    complaint_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    rs_unit_loss: Mapped[float | None] = mapped_column(Float, nullable=True)
    hpd_violations: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "zip_code",
            "scored_at",
            name="uq_score_history_zip_date",
        ),
        Index(
            "idx_score_history_zip_scored_at",
            "zip_code",
            "scored_at",
            postgresql_ops={"scored_at": "DESC"},
        ),
    )

    def __repr__(self) -> str:
        return f"<ScoreHistory zip={self.zip_code} date={self.scored_at} score={self.composite_score}>"
