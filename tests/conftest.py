"""
Pytest configuration and shared fixtures for PulseCities test suite.

Markers:
    integration — tests that require a live PostgreSQL database.
                  These are skipped in CI via: pytest -m "not integration"

Usage:
    # Run only unit tests (CI-safe):
    pytest -m "not integration"

    # Run only integration tests (requires local DB):
    pytest -m "integration"

    # Run everything:
    pytest
"""
import os

import pytest

# The test suite must never send real email or webhooks. send_ops_email and
# the digest read these at call time, and load_dotenv(override=False) won't
# replace values already present in the environment, so blanking them here
# (before any test module imports app code) guarantees every send path
# no-ops. Four fake "pipeline failed" alerts reached the real ops inbox
# before this guard existed, one per full-suite run.
os.environ["RESEND_API_KEY"] = ""
os.environ["ALERT_WEBHOOK_URL"] = ""
os.environ["ALERT_EMAIL"] = ""


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: marks tests as requiring a live PostgreSQL database",
    )
    config.addinivalue_line(
        "markers",
        "needs_data: test asserts on rendered records, so it needs a database "
        "holding the nightly feeds rather than only the schema",
    )


_SEEDED: bool | None = None


def _database_is_seeded() -> bool:
    """Does this database hold records, or only the schema?

    CI provisions Postgres and runs `alembic upgrade head`, which is worth doing
    on its own: it proves the migration chain still applies from nothing. What a
    runner cannot hold is 918,000 parcels, so a test asserting that a rendered
    page names a building has nothing to assert against and would go red for a
    reason that is not a regression. Those tests carry `needs_data` and skip
    here; every one of them still runs on the box, where the records are.

    Asked once per session and cached, including the failure: a database that
    cannot be reached is a database with no records in it as far as this goes.
    """
    global _SEEDED
    if _SEEDED is None:
        try:
            from sqlalchemy import text

            from models.database import SessionLocal
            db = SessionLocal()
            try:
                _SEEDED = bool(db.execute(text("SELECT 1 FROM parcels LIMIT 1")).first())
            finally:
                db.close()
        except Exception:
            _SEEDED = False
    return _SEEDED


def pytest_runtest_setup(item):
    if item.get_closest_marker("needs_data") and not _database_is_seeded():
        pytest.skip("database holds the schema and no records")
