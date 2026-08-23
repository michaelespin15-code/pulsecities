"""
POST /api/subscribe with operator_slug — operator-follow alerts.

Follows the classification gate: only clusters classed 'operator' are
followable. New rows must be born confirmed, since the digest filters
on confirmed=true and no confirm-link flow exists.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from models.database import SessionLocal

TEST_EMAIL = "follow-test@example.com"


@pytest.fixture(scope="module")
def client():
    # Blank the key so no real confirmation email leaves the box.
    #
    # Also disable the route limiter, the way test_operator_class_gate does.
    # /api/subscribe allows 10/minute per address and every subscribe module
    # posts from the same TestClient address, so once there were enough of
    # these files the shared window ran out mid-run and the last module to
    # execute collected the 429s. The per-email daily cap inside the endpoint
    # is a database check, not slowapi, so it stays live and still guarded.
    import api.routes.subscribe as sub_mod
    saved = sub_mod.resend.api_key
    sub_mod.resend.api_key = ""
    limiter_was = sub_mod.limiter.enabled
    sub_mod.limiter.enabled = False
    from api.main import app
    try:
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c
    finally:
        sub_mod.resend.api_key = saved
        sub_mod.limiter.enabled = limiter_was


@pytest.fixture(scope="module")
def operator_slug():
    db = SessionLocal()
    try:
        row = db.execute(text(
            "SELECT slug FROM operators WHERE operator_class = 'operator' AND slug IS NOT NULL LIMIT 1"
        )).fetchone()
    finally:
        db.close()
    if not row:
        pytest.skip("no classified operator in the database")
    return row.slug


@pytest.fixture(scope="module")
def gated_slug():
    db = SessionLocal()
    try:
        row = db.execute(text(
            "SELECT slug FROM operators WHERE operator_class IS DISTINCT FROM 'operator' "
            "AND slug IS NOT NULL LIMIT 1"
        )).fetchone()
    finally:
        db.close()
    return row.slug if row else None


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
class TestOperatorFollow:

    def test_follow_creates_confirmed_row(self, client, operator_slug):
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "operator_slug": operator_slug})
        assert resp.status_code == 201, resp.text
        db = SessionLocal()
        try:
            row = db.execute(text(
                "SELECT confirmed, operator_slug, zip_code, is_citywide FROM subscribers WHERE email = :e"
            ), {"e": TEST_EMAIL}).fetchone()
        finally:
            db.close()
        assert row is not None
        assert row.confirmed is True, "row born unconfirmed would never receive a digest"
        assert row.operator_slug == operator_slug
        assert row.zip_code is None and row.is_citywide is False

    def test_duplicate_follow_409(self, client, operator_slug):
        assert client.post("/api/subscribe", json={"email": TEST_EMAIL, "operator_slug": operator_slug}).status_code == 201
        assert client.post("/api/subscribe", json={"email": TEST_EMAIL, "operator_slug": operator_slug}).status_code == 409

    def test_unknown_operator_404(self, client):
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "operator_slug": "no-such-operator"})
        assert resp.status_code == 404

    def test_gated_operator_404(self, client, gated_slug):
        if not gated_slug:
            pytest.skip("no gated cluster in the database")
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "operator_slug": gated_slug})
        assert resp.status_code == 404, "gate leak: non-operator cluster is followable"

    def test_operator_plus_zip_rejected(self, client, operator_slug):
        resp = client.post("/api/subscribe", json={
            "email": TEST_EMAIL, "operator_slug": operator_slug, "zip_code": "11216",
        })
        assert resp.status_code == 422

    def test_invalid_slug_rejected(self, client):
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "operator_slug": "NOT A SLUG"})
        assert resp.status_code == 422

    def test_zip_subscription_born_confirmed(self, client):
        resp = client.post("/api/subscribe", json={"email": TEST_EMAIL, "zip_code": "11216"})
        assert resp.status_code == 201
        db = SessionLocal()
        try:
            confirmed = db.execute(text(
                "SELECT confirmed FROM subscribers WHERE email = :e"
            ), {"e": TEST_EMAIL}).scalar()
        finally:
            db.close()
        assert confirmed is True
