"""
ACRIS property ownership raw records — deed transfers and party data.
Sources:
  Master:  NYC Open Data bnx9-e6tj
  Parties: NYC Open Data 636b-3b5g
  Legals:  NYC Open Data 8h5j-fqxa (three-dataset join)

Append-only raw table.
Upsert key: (document_id, party_type)

CRITICAL: Use broader doc_type filter — filtering only 'DEED' misses ~30% of
LLC acquisitions. Include: DEED, DEEDP, DEED TRUST, BARGAIN AND SALE, ASST,
ASSIGNMENT OF LEASE, MEMO OF LEASE. (See config/nyc.py ACRIS_TRANSFER_DOC_TYPES)

Store both raw party_name AND party_name_normalized — raw is source of truth,
normalized enables LLC portfolio aggregation across name variants.
"""

from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Index, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class OwnershipRaw(TimestampMixin, Base):
    __tablename__ = "ownership_raw"

    # BBL normalized to canonical 10-digit form at ingest
    bbl: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # ACRIS document identifier — from the master dataset
    document_id: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Document type — see ACRIS_TRANSFER_DOC_TYPES in config/nyc.py
    doc_type: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Party type: GRANTOR (seller/transferor) or GRANTEE (buyer/transferee)
    party_type: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # Raw party name exactly as it appears in ACRIS
    party_name: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Normalized: uppercase, stripped punctuation, LLC variants unified
    # Enables portfolio aggregation: same LLC across 'LLC', 'L.L.C.', 'LIMITED LIABILITY CO'
    party_name_normalized: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Transfer date and amount
    doc_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    doc_amount: Mapped[Decimal | None] = mapped_column(
        Numeric(precision=15, scale=2), nullable=True
    )

    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint(
            "document_id", "party_type",
            name="uq_ownership_raw_document_party",
        ),
        Index("idx_ownership_raw_bbl", "bbl"),
        Index("idx_ownership_raw_doc_date", "doc_date"),
        Index("idx_ownership_raw_party_name_norm", "party_name_normalized"),
        Index("idx_ownership_raw_doc_type", "doc_type"),
        Index("idx_ownership_raw_created_at", "created_at"),  # for 90-day cleanup
        # Composite: scoring query filters WHERE party_type = '2' AND doc_date >= X (before LIKE scan)
        Index("idx_ownership_raw_party_type_date", "party_type", "doc_date"),
        # Composite: pulse LLC query resolves bbl join then filters doc_date >= X
        Index("idx_ownership_raw_bbl_date", "bbl", "doc_date"),
    )

    def __repr__(self) -> str:
        return f"<OwnershipRaw bbl={self.bbl} doc={self.document_id} party={self.party_name_normalized}>"
