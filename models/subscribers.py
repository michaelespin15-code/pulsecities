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

    # False until user clicks confirmation link
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
        # Partial unique index for citywide — enforced in migration 6aa0c3e6ef29.
        # Not declared here because SQLAlchemy can't express partial indexes inline.
        Index("idx_subscribers_email", "email"),
        Index("idx_subscribers_zip_code", "zip_code"),
        Index("idx_subscribers_confirmed", "confirmed"),
    )

    def __repr__(self) -> str:
        return f"<Subscriber email={self.email} zip={self.zip_code} citywide={self.is_citywide} confirmed={self.confirmed}>"
