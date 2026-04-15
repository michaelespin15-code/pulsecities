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
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: marks tests as requiring a live PostgreSQL database",
    )
