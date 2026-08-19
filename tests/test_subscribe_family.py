"""
POST /api/subscribe with family_slug — portfolio-follow alerts.

Families are computed, not stored, so the slug is validated against the live
clustering rather than a table. New rows must be born confirmed, since the
digest filters on confirmed=true and no confirm-link flow exists.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from models.database import SessionLocal

TEST_EMAIL = "family-follow-test@example.com"


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
def family_slug():
    from api.entity_families import families_cached
    from api.routes.frontend import _is_buyer_entity
    db = SessionLocal()
    try:
        fams = families_cached(db, _is_buyer_entity)
    finally:
        db.close()
    if not fams:
        pytest.skip("no entity families in the database")
    return next(iter(fams))


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
class TestFamilyFollow:

    def test_follow_creates_confirmed_row(self, client, family_slug):
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "family_slug": family_slug})
        assert resp.status_code == 201, resp.text
        db = SessionLocal()
        try:
            row = db.execute(text(
                "SELECT confirmed, family_slug, operator_slug, zip_code, is_citywide "
                "FROM subscribers WHERE email = :e"
            ), {"e": TEST_EMAIL}).fetchone()
        finally:
            db.close()
        assert row is not None
        assert row.confirmed is True, "row born unconfirmed would never receive a digest"
        assert row.family_slug == family_slug
        assert row.operator_slug is None
        assert row.zip_code is None and row.is_citywide is False

    def test_duplicate_follow_409(self, client, family_slug):
        assert client.post("/api/subscribe", json={"email": TEST_EMAIL, "family_slug": family_slug}).status_code == 201
        assert client.post("/api/subscribe", json={"email": TEST_EMAIL, "family_slug": family_slug}).status_code == 409

    def test_unknown_family_404(self, client):
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "family_slug": "no-such-family"})
        assert resp.status_code == 404

    def test_family_plus_zip_rejected(self, client, family_slug):
        resp = client.post("/api/subscribe", json={
            "email": TEST_EMAIL, "family_slug": family_slug, "zip_code": "11216",
        })
        assert resp.status_code == 422

    def test_family_plus_operator_rejected(self, client, family_slug):
        resp = client.post("/api/subscribe", json={
            "email": TEST_EMAIL, "family_slug": family_slug, "operator_slug": "mtek-nyc",
        })
        assert resp.status_code == 422

    def test_invalid_slug_rejected(self, client):
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "family_slug": "NOT A SLUG"})
        assert resp.status_code == 422

    def test_follow_card_on_hub_page(self, client, family_slug):
        resp = client.get(f"/network/{family_slug}")
        # A hub whose entities overlap a curated operator 301s there instead;
        # any non-redirected hub must carry the follow card.
        if resp.status_code == 200 and "follow" not in resp.text:
            pytest.fail("hub page renders without the follow card")
