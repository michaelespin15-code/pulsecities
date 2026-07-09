"""
Freshness-state tests for /api/status.

Core invariant (the bug this guards against): the public ok/delayed badge must
reflect DATA AGE, not run recency. A source whose scraper succeeds nightly but
whose data_through is stale must read 'delayed'; a source whose data_through is
fresh must read 'ok' — regardless of when the scraper last ran.

The state logic lives in the pure helper api.routes.status._source_state, so the
per-source cases below run without a database. Two integration tests at the end
exercise the live endpoint end-to-end.
"""

from datetime import datetime, timedelta, timezone

import pytest

from api.routes.status import (
    _FRESHNESS,
    _SNAPSHOT_SOURCES,
    SOURCES,
    _source_state,
)

# Fixed reference instant — keeps cases deterministic (no wall-clock dependency).
NOW = datetime(2026, 6, 11, 12, 0, 0, tzinfo=timezone.utc)

# Sources whose freshness is judged by their watermark (everything except the
# annual snapshot sources, which have no incremental date).
WATERMARK_SOURCES = [key for key, _ in SOURCES if key not in _SNAPSHOT_SOURCES]


# ── Per-source: data age drives state ──────────────────────────────────────────

@pytest.mark.parametrize("key", WATERMARK_SOURCES)
def test_fresh_data_reads_ok(key):
    """A watermark well inside the threshold reads ok."""
    fresh_watermark = NOW - timedelta(hours=6)
    last_run = NOW - timedelta(hours=6)
    assert _source_state(key, fresh_watermark, last_run, NOW) == "ok"


@pytest.mark.parametrize("key", WATERMARK_SOURCES)
def test_stale_data_with_recent_run_reads_delayed(key):
    """The bug: data older than the threshold reads delayed even though the
    scraper succeeded an hour ago."""
    recent_run = NOW - timedelta(hours=1)
    stale_watermark = NOW - _FRESHNESS[key] - timedelta(days=1)
    assert _source_state(key, stale_watermark, recent_run, NOW) == "delayed"


@pytest.mark.parametrize("key", WATERMARK_SOURCES)
def test_run_recency_never_overrides_stale_data(key):
    """A brand-new successful run must not flip stale data back to ok."""
    just_ran = NOW
    stale_watermark = NOW - _FRESHNESS[key] - timedelta(days=2)
    assert _source_state(key, stale_watermark, just_ran, NOW) == "delayed"


@pytest.mark.parametrize("key", WATERMARK_SOURCES)
def test_missing_watermark_reads_delayed(key):
    """No data_through on a watermark-based source cannot read ok."""
    assert _source_state(key, None, NOW, NOW) == "delayed"


# ── Annual snapshot source (dhcr_rs): no watermark, judged by last refresh ──────

def test_snapshot_source_ok_on_recent_refresh():
    """dhcr_rs has no watermark; a recent successful refresh reads ok."""
    assert _source_state("dhcr_rs", None, NOW - timedelta(days=30), NOW) == "ok"


def test_snapshot_source_not_falsely_delayed_when_months_old():
    """An annual dataset must not read delayed just for being months old."""
    assert _source_state("dhcr_rs", None, NOW - timedelta(days=120), NOW) == "ok"


def test_snapshot_source_delayed_when_refresh_far_too_old():
    """Past the annual threshold (no refresh in over a year) it does read delayed."""
    too_old = NOW - _FRESHNESS["dhcr_rs"] - timedelta(days=1)
    assert _source_state("dhcr_rs", None, too_old, NOW) == "delayed"


# ── Regression cases mirroring the reported live state ──────────────────────────

def test_dcwp_apr16_with_nightly_success_reads_delayed():
    """The exact report: Business licenses data through Apr 16, scraper ran today."""
    apr16 = datetime(2026, 4, 16, tzinfo=timezone.utc)
    ran_today = datetime(2026, 6, 11, 2, 3, tzinfo=timezone.utc)
    assert _source_state("dcwp_licenses", apr16, ran_today, NOW) == "delayed"


def test_acris_apr30_reads_delayed():
    apr30 = datetime(2026, 4, 30, tzinfo=timezone.utc)
    last_success = datetime(2026, 5, 12, tzinfo=timezone.utc)
    assert _source_state("acris_ownership", apr30, last_success, NOW) == "delayed"


@pytest.mark.parametrize("key,age_days", [
    ("evictions", 3),
    ("hpd_violations", 2),
    ("311_complaints", 2),
    ("dob_permits", 5),
])
def test_healthy_daily_sources_stay_ok(key, age_days):
    """Current healthy daily watermarks (including permits' normal ~5-day trail)
    must not flap to delayed."""
    watermark = NOW - timedelta(days=age_days)
    assert _source_state(key, watermark, NOW, NOW) == "ok", f"{key} at {age_days}d"


# ── Live endpoint ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def status_payload():
    from fastapi.testclient import TestClient
    from api.main import app
    resp = TestClient(app).get("/api/status")
    assert resp.status_code == 200
    return resp.json()


@pytest.mark.integration
def test_status_endpoint_shape(status_payload):
    assert "sources" in status_payload and status_payload["sources"]
    for s in status_payload["sources"]:
        assert s["state"] in ("ok", "delayed")
        assert {"key", "name", "data_through", "last_success", "state"} <= set(s)


@pytest.mark.integration
def test_status_endpoint_dcwp_delayed_with_pause_note(status_payload):
    by = {s["key"]: s for s in status_payload["sources"]}
    dcwp = by["dcwp_licenses"]
    assert dcwp["state"] == "delayed", "DCWP (Apr-16 data) must read delayed"
    assert dcwp.get("note") == "Source feed paused upstream at NYC Open Data."


@pytest.mark.integration
def test_status_endpoint_dhcr_not_falsely_delayed(status_payload):
    by = {s["key"]: s for s in status_payload["sources"]}
    assert by["dhcr_rs"]["state"] == "ok", "Annual rent-stabilization source must not read delayed"


@pytest.mark.integration
class TestAcrisWatermarkHonesty:
    """
    The ACRIS data_through on /status must never claim a date past the
    newest doc_date actually persisted. The feed watermark ran 2 days
    ahead of the table in the 2026-06-24 audit.
    """

    def test_acris_data_through_matches_table(self):
        from api.main import app
        from fastapi.testclient import TestClient
        from models.database import SessionLocal
        from sqlalchemy import text as _text

        db = SessionLocal()
        try:
            max_doc = db.execute(_text("SELECT MAX(doc_date) FROM ownership_raw")).scalar()
        finally:
            db.close()
        if max_doc is None:
            pytest.skip("no ACRIS rows in the database")

        with TestClient(app, raise_server_exceptions=False) as client:
            data = client.get("/api/status").json()
        acris = next(s for s in data["sources"] if s["key"] == "acris_ownership")
        assert acris["data_through"] == max_doc.isoformat(), (
            f"status claims {acris['data_through']}, table holds {max_doc}"
        )
