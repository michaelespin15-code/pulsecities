"""
Scraper infrastructure models.

ScraperRun: audit log for every scraper execution — foundation for monitoring.
ScraperQuarantine: dead letter table for records that fail validation.

Every scraper writes to ScraperRun on completion (success or failure).
Every invalid record goes to ScraperQuarantine instead of being silently dropped.
The GET /api/health endpoint reads ScraperRun to report last_run per scraper.
"""

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class ScraperRun(TimestampMixin, Base):
    """
    One row per scraper execution.
    Written at the END of the run so partial/failed runs are correctly recorded.
    Watermark only updated here after a successful DB commit.
    """

    __tablename__ = "scraper_runs"

    scraper_name: Mapped[str] = mapped_column(String(50), nullable=False)

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    records_processed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_failed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Expected minimum for anomaly detection — WARNING if actual < 50% of this
    expected_min_records: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # "success" or "failure"
    status: Mapped[str] = mapped_column(String(10), nullable=False)

    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # The watermark stored after this run — next run fetches records newer than this
    watermark_timestamp: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("idx_scraper_runs_scraper_name", "scraper_name"),
        Index("idx_scraper_runs_started_at", "started_at"),
        Index("idx_scraper_runs_status", "status"),
    )

    def __repr__(self) -> str:
        return f"<ScraperRun scraper={self.scraper_name} status={self.status} records={self.records_processed}>"


class ScraperQuarantine(Base):
    """
    Dead letter table for records that fail validation.
    Never silently drop a record — always quarantine with a reason code.
    Review weekly to catch upstream API schema changes early.
    No TimestampMixin — no updated_at since quarantine records are immutable.
    """

    __tablename__ = "scraper_quarantine"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scraper_name: Mapped[str] = mapped_column(String(50), nullable=False)

    # Full raw record exactly as received from the API
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Human-readable reason: "missing_bbl", "invalid_bbl_format", "missing_required_field:complaint_type"
    reason: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        Index("idx_quarantine_scraper_name", "scraper_name"),
        Index("idx_quarantine_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<ScraperQuarantine scraper={self.scraper_name} reason={self.reason}>"
