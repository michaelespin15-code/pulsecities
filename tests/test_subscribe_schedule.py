"""
Tests for GET /api/schedule and schedule/UI consistency.

Guards against a recurrence of the Monday/Sunday mismatch: if the cron
or send day changes, the endpoint test breaks and forces a UI update.
"""

import re
import subprocess

import pytest
from fastapi.testclient import TestClient

from api.main import app
from config.schedule import DIGEST_CRON, DIGEST_SEND_DAY

client = TestClient(app)


class TestScheduleEndpoint:
    def test_returns_200(self):
        resp = client.get("/api/schedule")
        assert resp.status_code == 200

    def test_returns_send_day(self):
        body = resp = client.get("/api/schedule").json()
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
    """Verify that config constants stay in sync with the deployed cron entry."""

    def test_cron_day_matches_config(self):
        """
        Parse /etc/cron.d/pulsecities and confirm the weekday field matches
        DIGEST_CRON. Fails loudly if someone edits the cron file without
        updating config/schedule.py.
        """
        try:
            with open("/etc/cron.d/pulsecities") as f:
                content = f.read()
        except FileNotFoundError:
            pytest.skip("/etc/cron.d/pulsecities not present in this environment")

        # Extract the digest cron line (the one that runs weekly_digest.py)
        digest_line = next(
            (l for l in content.splitlines() if "weekly_digest" in l and not l.startswith("#")),
            None,
        )
        assert digest_line, "No weekly_digest line found in /etc/cron.d/pulsecities"

        # Cron fields: minute hour dom month dow ...
        fields = digest_line.split()
        cron_expr = " ".join(fields[:5])
        assert cron_expr == DIGEST_CRON, (
            f"Cron file has '{cron_expr}' but config/schedule.py has '{DIGEST_CRON}'. "
            "Update config/schedule.py to match."
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
