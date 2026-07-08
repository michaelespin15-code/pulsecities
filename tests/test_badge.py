"""
/badge/{zip}.svg — embeddable score badge for press and community sites.

The badge must be a self-contained SVG (no external fonts, images, or
scripts) so it renders inside an <img> tag, where browsers block all
subresource fetches.
"""

import pytest
from fastapi.testclient import TestClient

from models.database import SessionLocal
from sqlalchemy import text


@pytest.fixture(scope="module")
def client():
    from api.main import app
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture(scope="module")
def scored_zip():
    db = SessionLocal()
    try:
        row = db.execute(text(
            "SELECT n.zip_code FROM neighborhoods n "
            "JOIN displacement_scores ds ON ds.zip_code = n.zip_code "
            "WHERE ds.score IS NOT NULL LIMIT 1"
        )).fetchone()
    finally:
        db.close()
    if not row:
        pytest.skip("no scored neighborhood in the database")
    return row.zip_code


@pytest.mark.integration
class TestBadge:

    def test_returns_svg(self, client, scored_zip):
        resp = client.get(f"/badge/{scored_zip}.svg")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("image/svg+xml")
        assert resp.text.startswith("<svg")

    def test_contains_zip_and_score(self, client, scored_zip):
        body = client.get(f"/badge/{scored_zip}.svg").text
        assert scored_zip in body
        assert "/100" in body

    def test_self_contained(self, client, scored_zip):
        body = client.get(f"/badge/{scored_zip}.svg").text
        assert "http" not in body.replace("http://www.w3.org", "")
        assert "<script" not in body

    def test_unknown_zip_404(self, client):
        assert client.get("/badge/00000.svg").status_code == 404

    def test_invalid_zip_400(self, client):
        assert client.get("/badge/abcde.svg").status_code == 400

    def test_cacheable(self, client, scored_zip):
        resp = client.get(f"/badge/{scored_zip}.svg")
        assert "max-age" in resp.headers.get("cache-control", "")
