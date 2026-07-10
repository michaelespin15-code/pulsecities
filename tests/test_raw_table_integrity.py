"""
Raw-table integrity guards.

Regression guard for the NULL-bbl duplicate class: unique constraints that
include a nullable column don't constrain NULL rows (Postgres treats NULLs
as distinct), so a scraper's ON CONFLICT silently re-inserts them every run.
evictions_raw accumulated 18k duplicate rows this way (migration
b5c9e2d4a7f3), inflating every per-ZIP eviction aggregate including
displacement scores.

Driven off the live DB: any new duplication in these tables fails the suite
regardless of how it got in.
"""

import pytest
from sqlalchemy import text

from models.database import SessionLocal


@pytest.fixture(scope="module")
def db():
    session = SessionLocal()
    yield session
    session.close()


# (table, identity expression) — identity must be unique per real-world event,
# NULLs coalesced so duplicate NULL-key rows still collide.
IDENTITY_KEYS = [
    ("evictions_raw", "COALESCE(bbl, ''), executed_date, docket_number"),
    ("permits_raw", "COALESCE(bbl, ''), filing_date, permit_type, work_type"),
    ("violations_raw", "violation_id"),
    ("complaints_raw", "unique_key"),
    ("sales_raw", "COALESCE(bbl, ''), sale_date, sale_price"),
    ("ownership_raw", "document_id, party_type"),
]


@pytest.mark.parametrize("table,key", IDENTITY_KEYS, ids=[t for t, _ in IDENTITY_KEYS])
def test_no_duplicate_identity_rows(db, table, key):
    total, distinct = db.execute(text(
        f"SELECT COUNT(*), COUNT(DISTINCT ({key})) FROM {table}"
    )).fetchone()
    assert total == distinct, (
        f"{table}: {total - distinct} duplicate rows for identity ({key}). "
        "A scraper is bypassing its unique key — check for NULLs in key columns."
    )


def test_eviction_ingest_volume_sane(db):
    """A re-ingestion loop shows up as ingest volume far above the real
    eviction rate. Executed evictions run roughly 300-500/week citywide;
    the OCA feed lags 2-4 weeks, so a catch-up week can be a multiple of
    that, but 5,000+ ingested rows in 7 days means duplication is back."""
    ingested = db.execute(text(
        "SELECT COUNT(*) FROM evictions_raw WHERE created_at >= now() - interval '7 days'"
    )).scalar()
    assert ingested < 5000, (
        f"{ingested} eviction rows ingested in 7 days — re-ingestion loop suspected"
    )
