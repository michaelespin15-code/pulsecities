"""
/og/operator/{slug}.png — branded share cards for operator profiles.

Gated clusters and unknown slugs get the generic site image, never a
branded card that could read as an endorsement of the cluster data.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from models.database import SessionLocal

_DEFAULT = (Path(__file__).parent.parent / "frontend" / "og-image.png").read_bytes()


@pytest.fixture(scope="module")
def client():
    from api.main import app
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


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


@pytest.mark.integration
class TestOperatorOgImage:

    def test_renders_branded_png(self, client, operator_slug):
        resp = client.get(f"/og/operator/{operator_slug}.png")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/png"
        assert resp.content[:8] == b"\x89PNG\r\n\x1a\n"
        assert resp.content != _DEFAULT, "real operator should get a branded card"

    def test_unknown_slug_gets_default(self, client):
        resp = client.get("/og/operator/no-such-operator.png")
        assert resp.status_code == 200
        assert resp.content == _DEFAULT

    def test_gated_cluster_gets_default(self, client):
        db = SessionLocal()
        try:
            row = db.execute(text(
                "SELECT slug FROM operators WHERE operator_class IS DISTINCT FROM 'operator' "
                "AND slug IS NOT NULL LIMIT 1"
            )).fetchone()
        finally:
            db.close()
        if not row:
            pytest.skip("no gated cluster in the database")
        resp = client.get(f"/og/operator/{row.slug}.png")
        assert resp.content == _DEFAULT, "gate leak: branded card for a non-operator cluster"

    def test_operator_page_references_branded_card(self, client, operator_slug):
        resp = client.get(f"/operator/{operator_slug}")
        assert f"/og/operator/{operator_slug}.png" in resp.text
