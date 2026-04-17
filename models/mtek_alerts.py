"""
MTEK portfolio event alerts — violations, permits, and evictions
detected after each property's acquisition date.

Populated nightly by scripts/mtek_monitor.py. Unique on
(bbl, alert_type, source_id) so re-runs are safe.
"""

from datetime import date

from sqlalchemy import Date, Index, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class MtekAlert(TimestampMixin, Base):
    __tablename__ = "mtek_alerts"

    # Property identifiers
    bbl: Mapped[str] = mapped_column(String(10), nullable=False)
    address: Mapped[str | None] = mapped_column(String(200), nullable=True)
    entity: Mapped[str | None] = mapped_column(String(100), nullable=True)  # MTEK LLC that owns it

    # Event classification: "hpd_violation" | "dob_permit" | "eviction"
    alert_type: Mapped[str] = mapped_column(String(20), nullable=False)

    # For HPD violations: "A", "B", or "C" — null for other alert types
    violation_class: Mapped[str | None] = mapped_column(String(1), nullable=True)

    # Date the event occurred at the source (inspection_date / filing_date / executed_date)
    event_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    # Short description: violation text, permit type, eviction type
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Natural key from the source table — prevents duplicate alerts per event.
    # violations_raw → violation_id
    # permits_raw    → "permit_{id}" (DB row id — stable across upserts)
    # evictions_raw  → docket_number or "eviction_{id}"
    source_id: Mapped[str] = mapped_column(String(50), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "bbl", "alert_type", "source_id",
            name="uq_mtek_alerts_bbl_type_source",
        ),
        Index("idx_mtek_alerts_bbl", "bbl"),
        Index("idx_mtek_alerts_event_date", "event_date"),
        Index("idx_mtek_alerts_alert_type", "alert_type"),
        Index("idx_mtek_alerts_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<MtekAlert bbl={self.bbl} type={self.alert_type} "
            f"class={self.violation_class} date={self.event_date}>"
        )
