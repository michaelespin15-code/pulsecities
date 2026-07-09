"""
Email subscriber model for weekly neighborhood digest.
Stores email + zip code only — nothing else per spec.
unsubscribe_token is a random UUID used in one-click unsubscribe links (CAN-SPAM).
"""

import secrets
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


def _generate_token() -> str:
    return secrets.token_urlsafe(32)


class Subscriber(TimestampMixin, Base):
    __tablename__ = "subscribers"

    email: Mapped[str] = mapped_column(String(254), nullable=False)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)
    is_citywide: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, server_default="false")

    # Operator follow: weekly alert when this cluster records new acquisitions.
    # A row watches exactly one of: a ZIP, the city, or an operator.
    operator_slug: Mapped[str | None] = mapped_column(String(120), nullable=True)

    # Single opt-in: set True at creation. The digest sends only to
    # confirmed rows, so an unconfirmed row never receives anything.
    confirmed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Random token for one-click unsubscribe — required by CAN-SPAM
    unsubscribe_token: Mapped[str] = mapped_column(
        String(64), default=_generate_token, nullable=False, unique=True
    )

    confirmed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("email", "zip_code", name="uq_subscribers_email_zip"),
        # Partial unique indexes for citywide (migration 6aa0c3e6ef29) and
        # operator follows (migration a9c4d2e7b8f1). Not declared here because
        # SQLAlchemy can't express partial indexes inline.
        Index("idx_subscribers_email", "email"),
        Index("idx_subscribers_zip_code", "zip_code"),
        Index("idx_subscribers_confirmed", "confirmed"),
        Index("idx_subscribers_operator_slug", "operator_slug"),
    )

    def __repr__(self) -> str:
        return f"<Subscriber email={self.email} zip={self.zip_code} citywide={self.is_citywide} confirmed={self.confirmed}>"
