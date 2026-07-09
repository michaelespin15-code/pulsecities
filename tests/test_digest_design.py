"""
Digest visual design — guards for the case-file email layout.

The redesign's promises, pinned so a future edit can't quietly regress them:

  1. The reader's ZIP gets a personal 90-day pulse trace image, served from
     /og/spark/{zip}.png. The email must reference it and the endpoint must
     actually produce a PNG, or every client shows a broken-image box.
  2. Address-level events render as "The Record" ledger; the section only
     exists when there are events to show.
  3. Everything the compliance tests in test_weekly_digest.py pin (tokens,
     disclaimers, links, no em dashes) still holds; those tests stay the
     source of truth and are not duplicated here.
"""

import pytest

from scripts.weekly_digest import render_zip_digest

_EMPTY_EVENTS = {"llc_rows": [], "eviction_rows": [], "permit_rows": [], "hpd_rows": []}


def _summary(**overrides) -> dict:
    base = {
        "zip": "10026",
        "name": "Harlem",
        "score_now": 72.0,
        "score_prev": 67.0,
        "delta": 5.0,
        "tier_now": "high",
        "tier_prev": "high",
        "tier_increased": False,
        "elevated": [("eviction_rate", 81.0)],
        "hpd_count": 12,
        "eviction_count": 4,
        "permit_count": 2,
        "llc_count": 3,
        "complaint_count": 9,
        "hpd_avg": 6.5,
        "eviction_avg": 1.2,
        "permit_avg": 2.1,
        "complaint_avg": 8.8,
    }
    base.update(overrides)
    return base


def _subscription() -> dict:
    return {"email": "reader@example.com", "unsubscribe_token": "tok123"}


class TestZipDigestDesign:
    def test_email_embeds_the_readers_pulse_trace(self):
        rendered = render_zip_digest(_subscription(), _summary(), ["x"], _EMPTY_EVENTS)
        assert "https://pulsecities.com/og/spark/10026.png" in rendered["html"]

    def test_record_section_present_when_events_exist(self):
        events = dict(_EMPTY_EVENTS)
        events["eviction_rows"] = [("123 EXAMPLE ST", __import__("datetime").date(2026, 7, 6))]
        rendered = render_zip_digest(_subscription(), _summary(), ["x"], events)
        assert "The Record" in rendered["html"]

    def test_record_section_absent_when_no_events(self):
        rendered = render_zip_digest(_subscription(), _summary(), ["x"], _EMPTY_EVENTS)
        assert "The Record" not in rendered["html"]


@pytest.mark.integration
class TestSparklineEndpoint:
    def _get_client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_returns_png_for_a_scored_zip(self):
        client = self._get_client()
        resp = client.get("/og/spark/11216.png")
        assert resp.status_code == 200, resp.text[:200]
        assert resp.headers["content-type"] == "image/png"
        assert resp.content[:4] == b"\x89PNG"

    def test_unknown_zip_still_returns_an_image(self):
        # A broken-image box in the email is never acceptable; an unscored ZIP
        # gets a flat placeholder trace rather than a 404.
        client = self._get_client()
        resp = client.get("/og/spark/99999.png")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/png"
