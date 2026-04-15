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
    zip_code: Mapped[str] = mapped_column(String(5), nullable=False)

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
        Index("idx_subscribers_email", "email"),
        Index("idx_subscribers_zip_code", "zip_code"),
        Index("idx_subscribers_confirmed", "confirmed"),
    )

    def __repr__(self) -> str:
        return f"<Subscriber email={self.email} zip={self.zip_code} confirmed={self.confirmed}>"
