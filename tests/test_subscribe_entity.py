"""
POST /api/subscribe with entity_slug — follow one buyer entity.

/llc is the site's second organic landing surface and had no capture at all:
the portfolio card renders only inside entity_family_page, so it covered the
published families and none of the individual companies.

Entities are not a table. They are distinct party_name_normalized values in
ownership_raw, so the slug is validated against the deed record at write time
and the digest resolves it the same way. An entity that leaves the record
resolves to nothing and its followers are skipped, not errored.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from models.database import SessionLocal

TEST_EMAIL = "entity-test@example.com"


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
def entity():
    """A slug the /llc page would render, resolved the same way it resolves."""
    db = SessionLocal()
    try:
        row = db.execute(text("""
            SELECT btrim(regexp_replace(lower(party_name_normalized),
                                        '[^a-z0-9]+', '-', 'g'), '-') AS slug,
                   party_name_normalized AS name
            FROM ownership_raw
            WHERE doc_type = 'DEED' AND party_type = '2'
              AND party_name_normalized IS NOT NULL
            LIMIT 1
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
class TestEntityFollow:

    def test_follow_creates_confirmed_row(self, client, entity):
        resp = client.post("/api/subscribe",
                           json={"email": TEST_EMAIL, "entity_slug": entity.slug})
        assert resp.status_code == 201, resp.text
        db = SessionLocal()
        try:
            row = db.execute(text(
                "SELECT confirmed, entity_slug, zip_code, is_citywide, bbl, family_slug "
                "FROM subscribers WHERE email = :e"
            ), {"e": TEST_EMAIL}).fetchone()
        finally:
            db.close()
        assert row is not None
        assert row.confirmed is True, "row born unconfirmed would never receive an alert"
        assert row.entity_slug == entity.slug
        assert row.zip_code is None and row.is_citywide is False
        assert row.bbl is None and row.family_slug is None

    def test_duplicate_follow_409(self, client, entity):
        assert client.post("/api/subscribe",
                           json={"email": TEST_EMAIL, "entity_slug": entity.slug}
                           ).status_code == 201
        assert client.post("/api/subscribe",
                           json={"email": TEST_EMAIL, "entity_slug": entity.slug}
                           ).status_code == 409

    def test_unknown_entity_404(self, client):
        resp = client.post("/api/subscribe",
                           json={"email": TEST_EMAIL, "entity_slug": "no-such-company-llc"})
        assert resp.status_code == 404

    def test_malformed_slug_422(self, client):
        for bad in ("Not A Slug", "has_underscore", "", "x" * 201, "slash/es", "dot.dot"):
            resp = client.post("/api/subscribe",
                               json={"email": TEST_EMAIL, "entity_slug": bad})
            assert resp.status_code == 422, f"slug {bad!r} accepted"

    def test_slug_is_lowercased_not_rejected(self, client, entity):
        """Same normalisation the operator and family validators apply: case is
        folded before the pattern check, so a slug typed in caps resolves to
        the same entity rather than 422ing."""
        resp = client.post("/api/subscribe",
                           json={"email": TEST_EMAIL, "entity_slug": entity.slug.upper()})
        assert resp.status_code == 201, resp.text
        db = SessionLocal()
        try:
            row = db.execute(text("SELECT entity_slug FROM subscribers WHERE email = :e"),
                             {"e": TEST_EMAIL}).fetchone()
        finally:
            db.close()
        assert row.entity_slug == entity.slug, "stored slug must be the lowercase form"

    def test_entity_cannot_combine_with_other_targets(self, client, entity):
        for extra in ({"zip_code": "11216"}, {"is_citywide": True},
                      {"family_slug": "flgsp"}):
            resp = client.post("/api/subscribe", json={
                "email": TEST_EMAIL, "entity_slug": entity.slug, **extra})
            assert resp.status_code == 422, f"{extra} accepted alongside entity_slug"


@pytest.mark.integration
class TestEntityDigest:
    """The weekly arm: load, build, render."""

    def test_load_returns_only_entity_rows(self, client, entity):
        from scripts.weekly_digest import load_entity_follows
        client.post("/api/subscribe", json={"email": TEST_EMAIL, "entity_slug": entity.slug})
        db = SessionLocal()
        try:
            rows = load_entity_follows(db)
        finally:
            db.close()
        mine = [r for r in rows if r["email"] == TEST_EMAIL]
        assert len(mine) == 1
        assert mine[0]["entity_slug"] == entity.slug
        assert mine[0]["unsubscribe_token"]

    def test_build_updates_skips_unknown_slug(self):
        from scripts.weekly_digest import build_entity_updates
        db = SessionLocal()
        try:
            assert build_entity_updates(db, set()) == {}
            assert build_entity_updates(db, {"no-such-company-llc"}) == {}
        finally:
            db.close()

    def test_render_is_wellformed_and_escapes(self):
        from scripts.weekly_digest import render_entity_digest
        sub = {"email": TEST_EMAIL, "entity_slug": "x-llc", "unsubscribe_token": "tok123"}
        update = {
            "slug": "x-llc",
            "label": 'ACME <script>"& LLC',
            "deeds": [{"address": "1 Main St", "zip": "11216", "date": "2026-08-20",
                       "price": 1250000.0, "side": "Bought"}],
        }
        out = render_entity_digest(sub, update)
        assert out["subject"].startswith('ACME <script>"& LLC')
        assert "<script>" not in out["html"], "label must be escaped into the HTML"
        assert "&lt;script&gt;" in out["html"]
        assert "tok123" in out["html"] and "tok123" in out["text"]
        assert "/llc/x-llc" in out["html"] and "/llc/x-llc" in out["text"]
        assert "$1,250,000" in out["html"]
        assert "Bought" in out["html"]
        assert "—" not in out["html"], "em dash in digest copy"
        assert out["html"].count("{") == 0, "unfilled format placeholder left in the shell"

    def test_render_reports_both_sides(self):
        from scripts.weekly_digest import render_entity_digest
        sub = {"email": TEST_EMAIL, "entity_slug": "x-llc", "unsubscribe_token": "t"}
        update = {"slug": "x-llc", "label": "X LLC", "deeds": [
            {"address": "1 Main St", "zip": "", "date": "2026-08-20",
             "price": None, "side": "Sold"}]}
        out = render_entity_digest(sub, update)
        assert "Sold" in out["html"], "a company selling is the half worth reporting"
