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


# (table, identity expression, scope) — identity must be unique per real-world
# event, NULLs coalesced so duplicate NULL-key rows still collide. `scope` is an
# optional WHERE clause for a table whose rows do not all share one identity.
IDENTITY_KEYS = [
    ("evictions_raw", "COALESCE(bbl, ''), executed_date, docket_number", None),
    # permits_raw is written by two scrapers with two identities, split on
    # `source_id IS NULL` by migration a7d3f1e08b64. Checking the BIS key
    # against the whole table reports 20,742 duplicates, and every one of them
    # is a real distinct DOB NOW job: two jobs on the same lot, permitted the
    # same day, with the same job type and trades. That number is the measured
    # answer to "does the second source need its own key", and it is why it got
    # one instead of being folded into the first.
    ("permits_raw", "COALESCE(bbl, ''), filing_date, permit_type, work_type",
     "source_id IS NULL"),
    ("permits_raw", "source, source_id", "source_id IS NOT NULL"),
    ("violations_raw", "violation_id", None),
    ("complaints_raw", "unique_key", None),
    ("sales_raw", "COALESCE(bbl, ''), sale_date, sale_price", None),
    ("ownership_raw", "document_id, party_type", None),
]


@pytest.mark.parametrize(
    "table,key,scope", IDENTITY_KEYS,
    ids=[t if s is None else f"{t}[{s}]" for t, _, s in IDENTITY_KEYS],
)
def test_no_duplicate_identity_rows(db, table, key, scope):
    where = f" WHERE {scope}" if scope else ""
    total, distinct = db.execute(text(
        f"SELECT COUNT(*), COUNT(DISTINCT ({key})) FROM {table}{where}"
    )).fetchone()
    assert total == distinct, (
        f"{table}{where}: {total - distinct} duplicate rows for identity ({key}). "
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
