"""
Tests for GET /api/schedule and schedule/UI consistency.

Guards against a recurrence of the Monday/Sunday mismatch: if the cron
or send day changes, the endpoint test breaks and forces a UI update.
"""

import re

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


def _digest_line(path: str) -> str:
    with open(path) as f:
        lines = f.read().splitlines()
    line = next((l for l in lines if "weekly_digest" in l and not l.startswith("#")), None)
    assert line, f"No weekly_digest line found in {path}"
    return line


def _assert_cron_implements_config(path: str) -> None:
    """Check the crontab fires at the hour config/schedule.py asks for.

    DIGEST_CRON is written in America/New_York; cron runs in UTC. The crontab
    line bridges that by listing both possible UTC hours and guarding on the
    eastern clock, so the two are never equal as strings and comparing them
    directly could only ever fail. That is what it had been doing, quietly, for
    long enough that the suite carried a permanent red mark. Check the property
    that actually matters instead: the guarded eastern hour and the day of week
    are the ones configured, and the UTC hours are that hour's two DST offsets.
    """
    want_minute, want_hour, _, _, want_dow = DIGEST_CRON.split()
    line = _digest_line(path)
    minute, hours = line.split()[0], line.split()[1]
    dow = line.split()[4]

    assert minute == want_minute, f"{path}: minute {minute} != configured {want_minute}"
    assert dow == want_dow, f"{path}: day-of-week {dow} != configured {want_dow}"

    guard = re.search(r'date \+.?.?%H.?.?\)?"?\s*=\s*"(\d{2})"', line)
    assert guard, (
        f"{path}: the digest line has no eastern-clock guard. Without it the "
        "job fires twice on one of the two listed UTC hours."
    )
    assert int(guard.group(1)) == int(want_hour), (
        f"{path}: guarded on {guard.group(1)}:00 ET but config/schedule.py says "
        f"{want_hour}:00 ET"
    )

    # EDT is UTC-4 and EST is UTC-5, so one eastern hour maps to two UTC hours
    # and both must be listed or the job skips half the year.
    expected_utc = sorted({(int(want_hour) + 4) % 24, (int(want_hour) + 5) % 24})
    assert sorted(int(h) for h in hours.split(",")) == expected_utc, (
        f"{path}: UTC hours {hours} do not cover both DST offsets of "
        f"{want_hour}:00 ET (expected {','.join(str(h) for h in expected_utc)})"
    )


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
        _assert_cron_implements_config(_REPO_CRON_TEMPLATE)

    def test_live_cron_matches_config_when_present(self):
        """
        On the VPS, the deployed /etc/cron.d/pulsecities must also match.
        Skipped in CI and dev environments where the file is absent.
        """
        try:
            _assert_cron_implements_config(_LIVE_CRON_FILE)
        except FileNotFoundError:
            pytest.skip(f"{_LIVE_CRON_FILE} not present in this environment")

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
