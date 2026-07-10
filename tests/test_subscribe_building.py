"""
POST /api/subscribe with bbl — building-level watch alerts.

A watch must point at a real parcel; unknown BBLs 404 rather than
accepting a watch that can never fire. Rows are born confirmed because
the alert scan filters on confirmed=true and no confirm-link flow
exists. The scan itself windows on created_at (ingest time), so ACRIS
backfills still alert.
"""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from models.database import SessionLocal

TEST_EMAIL = "watch-test@example.com"


@pytest.fixture(scope="module")
def client():
    # Blank the key so no real confirmation email leaves the box
    import api.routes.subscribe as sub_mod
    saved = sub_mod.resend.api_key
    sub_mod.resend.api_key = ""
    from api.main import app
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
    sub_mod.resend.api_key = saved


@pytest.fixture(scope="module")
def real_bbl():
    db = SessionLocal()
    try:
        row = db.execute(text("SELECT bbl FROM parcels WHERE address IS NOT NULL LIMIT 1")).fetchone()
    finally:
        db.close()
    if not row:
        pytest.skip("no parcels in the database")
    return row.bbl


@pytest.fixture(scope="module")
def deed_bbl():
    """A BBL that has at least one grantee-side deed row, for scan tests."""
    db = SessionLocal()
    try:
        row = db.execute(text("""
            SELECT bbl, created_at FROM ownership_raw
            WHERE doc_type IN ('DEED', 'DEEDP') AND party_type = '2' AND bbl IS NOT NULL
            ORDER BY created_at DESC LIMIT 1
        """)).fetchone()
    finally:
        db.close()
    if not row:
        pytest.skip("no deed rows in the database")
    return row


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM subscribers WHERE email = :e"), {"e": TEST_EMAIL})
        db.commit()
    finally:
        db.close()


@pytest.mark.integration
class TestBuildingWatch:

    def test_watch_creates_confirmed_row(self, client, real_bbl):
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "bbl": real_bbl})
        assert resp.status_code == 201, resp.text
        db = SessionLocal()
        try:
            row = db.execute(text(
                "SELECT confirmed, bbl, zip_code, is_citywide, operator_slug "
                "FROM subscribers WHERE email = :e"
            ), {"e": TEST_EMAIL}).fetchone()
        finally:
            db.close()
        assert row is not None
        assert row.confirmed is True, "row born unconfirmed would never receive an alert"
        assert row.bbl == real_bbl
        assert row.zip_code is None and row.is_citywide is False and row.operator_slug is None

    def test_duplicate_watch_409(self, client, real_bbl):
        assert client.post("/api/subscribe", json={"email": TEST_EMAIL, "bbl": real_bbl}).status_code == 201
        assert client.post("/api/subscribe", json={"email": TEST_EMAIL, "bbl": real_bbl}).status_code == 409

    def test_unknown_bbl_404(self, client):
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "bbl": "1999999999"})
        assert resp.status_code == 404

    def test_malformed_bbl_422(self, client):
        for bad in ("12345", "6123456789", "1abcdefghi", ""):
            resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "bbl": bad})
            assert resp.status_code == 422, f"bbl {bad!r} accepted"

    def test_bbl_plus_zip_rejected(self, client, real_bbl):
        resp = client.post("/api/subscribe", json={
            "email": TEST_EMAIL, "bbl": real_bbl, "zip_code": "11216",
        })
        assert resp.status_code == 422


@pytest.mark.integration
class TestAlertScan:
    """scan() windows on created_at and only reports watched buildings."""

    def _watch(self, db, bbl):
        db.execute(text("""
            INSERT INTO subscribers (email, bbl, confirmed, unsubscribe_token, created_at, updated_at)
            VALUES (:e, :b, true, :t, now(), now())
        """), {"e": TEST_EMAIL, "b": bbl, "t": "test-token-" + bbl})
        db.commit()

    def test_scan_picks_up_watched_deed(self, deed_bbl):
        from scripts.building_alerts import scan
        db = SessionLocal()
        try:
            self._watch(db, deed_bbl.bbl)
            since = deed_bbl.created_at - timedelta(seconds=1)
            alerts = scan(db, since)
        finally:
            db.close()
        mine = [a for a in alerts if a["email"] == TEST_EMAIL]
        assert len(mine) == 1
        assert any(e["kind"] == "deed" for e in mine[0]["events"])
        assert mine[0]["token"] == "test-token-" + deed_bbl.bbl

    def test_scan_quiet_window_sends_nothing(self, deed_bbl):
        from scripts.building_alerts import scan
        db = SessionLocal()
        try:
            self._watch(db, deed_bbl.bbl)
            alerts = scan(db, datetime.now(timezone.utc) + timedelta(days=1))
        finally:
            db.close()
        assert [a for a in alerts if a["email"] == TEST_EMAIL] == []


class TestAlertEmail:

    def _alert(self, **over):
        alert = {
            "email": TEST_EMAIL,
            "bbl": "2050840054",
            "address": "4575 Furman Avenue",
            "token": "tok-123",
            "events": [{"kind": "deed", "line": "Deed transfer to X LLC for $500,000, dated Jul 8, 2025."}],
        }
        alert.update(over)
        return alert

    def test_email_carries_unsubscribe_and_property_link(self):
        from scripts.building_alerts import build_email
        subject, html, text_body = build_email(self._alert())
        assert "unsubscribe?token=tok-123" in html and "unsubscribe?token=tok-123" in text_body
        assert "/property/2050840054" in html and "/property/2050840054" in text_body
        assert subject == "New at 4575 Furman Avenue: 1 new record"

    def test_email_escapes_record_strings(self):
        from scripts.building_alerts import build_email
        evil = self._alert(
            address='<script>alert(1)</script>',
            events=[{"kind": "deed", "line": 'Deed transfer to <img src=x onerror=alert(1)> LLC.'}],
        )
        _, html, _ = build_email(evil)
        assert "<script>" not in html and "<img" not in html

    def test_no_em_dash_in_email(self):
        from scripts.building_alerts import build_email
        subject, html, text_body = build_email(self._alert())
        for part in (subject, html, text_body):
            assert "—" not in part
