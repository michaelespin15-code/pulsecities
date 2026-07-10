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
