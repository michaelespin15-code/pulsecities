"""
DOB building permit raw records.

Two sources write here, and they do not share an identity.

  dob_bis  ipu4-2q9a, the legacy DOB Permit Issuance dataset. No stable row id
           upstream, so it is keyed on (bbl, filing_date, permit_type,
           work_type) as it always was.
  dob_now  w9ak-ipjd, DOB NOW Build job filings, which superseded BIS. Carries
           `job_filing_number`, one row per job, so it is keyed on that.

The second source exists because the first stopped being the whole record. We
held 6,501 permits for the last 365 days against roughly 170,000 upstream, and
after the scoring filter the permit signal was 414 rows carrying 24.7% of the
composite score. See docs/CHECKPOINT.md, 2026-08-28.

Giving both sources one identity would have dropped rows silently: two DOB NOW
jobs on the same lot, permitted the same day, with the same job type are
different jobs with different filing numbers and collide under the old key.
Migration a7d3f1e08b64 splits the unique index on `source_id IS NULL`.

Append-only raw table — records are never modified after insert.
"""

from datetime import date

from sqlalchemy import Date, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class PermitRaw(TimestampMixin, Base):
    __tablename__ = "permits_raw"

    # BBL normalized to canonical 10-digit form at ingest
    bbl: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # BIN (Building Identification Number) — alternate property key from DOB
    bin: Mapped[str | None] = mapped_column(String(7), nullable=True)

    address: Mapped[str | None] = mapped_column(String(200), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(5), nullable=True)
    borough: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Which upstream dataset this row came from. Existing rows predate DOB NOW
    # scraping and are BIS rows by definition, which is what the column default
    # records.
    source: Mapped[str] = mapped_column(
        String(16), nullable=False, default="dob_bis", server_default="dob_bis"
    )

    # The row's identity in its own source. NULL for BIS, which has none;
    # `job_filing_number` for DOB NOW. The unique indexes key off whether this
    # is null, so it is the switch between the two identity rules.
    source_id: Mapped[str | None] = mapped_column(String(40), nullable=True)

    # Short job-type code. BIS supplies these directly (AL, NB, DM); the DOB NOW
    # scraper maps its long `job_type` onto the same vocabulary, so
    # `permit_type = 'AL'` in scoring means the same thing across both sources
    # and needed no change when the second source arrived.
    permit_type: Mapped[str | None] = mapped_column(String(10), nullable=True)
    work_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    owner_name: Mapped[str | None] = mapped_column(String(200), nullable=True)

    filing_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    expiration_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    job_description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Full raw API response for this record — never lose source data
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # The two unique indexes are partial and are created by migration
    # a7d3f1e08b64 rather than declared here: SQLAlchemy cannot express
    # `UNIQUE (COALESCE(bbl,''), ...) WHERE source_id IS NULL` as a constraint,
    # and both are built CONCURRENTLY against a live 745k-row table.
    #
    #   uq_permits_raw_source_id     (source, source_id) WHERE source_id IS NOT NULL
    #   uq_permits_raw_identity_bis  (COALESCE(bbl,''), filing_date, permit_type,
    #                                 work_type) WHERE source_id IS NULL
    __table_args__ = (
        Index("idx_permits_raw_bbl", "bbl"),
        Index("idx_permits_raw_filing_date", "filing_date"),
        Index("idx_permits_raw_zip_code", "zip_code"),
        Index("idx_permits_raw_created_at", "created_at"),  # for 90-day cleanup
        # Composite: pulse query filters WHERE zip_code = :zip AND filing_date >= X
        Index("idx_permits_raw_zip_filing", "zip_code", "filing_date"),
    )

    def __repr__(self) -> str:
        return (f"<PermitRaw bbl={self.bbl} type={self.permit_type} "
                f"date={self.filing_date} source={self.source}>")
