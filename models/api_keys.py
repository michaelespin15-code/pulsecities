"""
Partner API keys. The public read tier is keyless; a key identifies a
partner (newsroom data team, researcher) for usage visibility and, later,
higher rate tiers. Only the SHA-256 of the key is stored — the plaintext
is shown once by scripts/mint_api_key.py and never persisted.
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class ApiKey(TimestampMixin, Base):
    __tablename__ = "api_keys"

    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    label: Mapped[str] = mapped_column(String(120), nullable=False)
    owner_email: Mapped[str] = mapped_column(String(254), nullable=False)
    tier: Mapped[str] = mapped_column(String(20), nullable=False, default="partner", server_default="partner")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_api_keys_key_hash", "key_hash"),
    )

    def __repr__(self) -> str:
        return f"<ApiKey label={self.label} tier={self.tier} active={self.active}>"
