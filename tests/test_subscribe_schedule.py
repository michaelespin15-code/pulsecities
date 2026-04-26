"""
Tests for GET /api/schedule and schedule/UI consistency.

Guards against a recurrence of the Monday/Sunday mismatch: if the cron
or send day changes, the endpoint test breaks and forces a UI update.
"""

import pytest
from fastapi.testclient import TestClient

from api.main import app
from config.schedule import DIGEST_CRON, DIGEST_SEND_DAY

client = TestClient(app)

_REPO_CRON_TEMPLATE = "deploy/pulsecities.cron"
_LIVE_CRON_FILE     = "/etc/cron.d/pulsecities"


def _parse_digest_cron(path: str) -> str:
    """Return the 5-field cron expression for the weekly_digest line in a cron file."""
    with open(path) as f:
        lines = f.read().splitlines()
    digest_line = next(
        (l for l in lines if "weekly_digest" in l and not l.startswith("#")),
        None,
    )
    assert digest_line, f"No weekly_digest line found in {path}"
    fields = digest_line.split()
    return " ".join(fields[:5])


class TestScheduleEndpoint:
    def test_returns_200(self):
        resp = client.get("/api/schedule")
        assert resp.status_code == 200

    def test_returns_send_day(self):
        body = client.get("/api/schedule").json()
        assert "send_day" in body
        assert isinstance(body["send_day"], str)
        assert body["send_day"] != ""

    def test_send_day_matches_config(self):
        body = client.get("/api/schedule").json()
        assert body["send_day"] == DIGEST_SEND_DAY, (
            f"Endpoint returned '{body['send_day']}' but config says '{DIGEST_SEND_DAY}'"
        )

    def test_cron_matches_config(self):
        body = client.get("/api/schedule").json()
        assert body["cron"] == DIGEST_CRON


class TestScheduleConsistency:
    """Verify config constants stay in sync with the repo cron template and live system."""

    def test_repo_cron_template_matches_config(self):
        """
        deploy/pulsecities.cron is the repo-owned source of truth for the cron schedule.
        This test always runs (no skip) — CI will catch drift even without the live server.
        """
        cron_expr = _parse_digest_cron(_REPO_CRON_TEMPLATE)
        assert cron_expr == DIGEST_CRON, (
            f"deploy/pulsecities.cron has '{cron_expr}' but config/schedule.py has '{DIGEST_CRON}'. "
            "Update config/schedule.py to match, or update the cron template."
        )

    def test_live_cron_matches_config_when_present(self):
        """
        On the VPS, the deployed /etc/cron.d/pulsecities must also match.
        Skipped in CI and dev environments where the file is absent.
        """
        try:
            cron_expr = _parse_digest_cron(_LIVE_CRON_FILE)
        except FileNotFoundError:
            pytest.skip(f"{_LIVE_CRON_FILE} not present in this environment")

        assert cron_expr == DIGEST_CRON, (
            f"{_LIVE_CRON_FILE} has '{cron_expr}' but config/schedule.py has '{DIGEST_CRON}'. "
            f"Re-deploy: cp {_REPO_CRON_TEMPLATE} {_LIVE_CRON_FILE}"
        )

    def test_frontend_does_not_hardcode_wrong_day(self):
        """
        Ensure app.html no longer contains the old hardcoded Monday string.
        The dynamic success message is built from _digestSendDay (API-sourced).
        """
        with open("frontend/app.html") as f:
            content = f.read()

        assert "every Monday" not in content, (
            "frontend/app.html still contains 'every Monday'. "
            "The success message must be built from _digestSendDay (API-sourced), not hardcoded."
        )
        assert "cada lunes" not in content, (
            "frontend/app.html still contains 'cada lunes'. "
            "Update the Spanish success message to use the API-sourced day name."
        )
