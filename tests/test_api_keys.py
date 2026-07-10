"""
Partner API keys and the /developers docs page.

The public tier is keyless: requests without X-API-Key are untouched.
A request that does carry a key must resolve to an active row or fail
with 401; silently ignoring a bad key would look like public access to
the caller and like no partner traffic to us. The docs tripwire keeps
/developers honest: every endpoint it documents must exist in the app.
"""

import hashlib
import re
import secrets
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from models.database import SessionLocal

TEST_LABEL = "pytest-key"


@pytest.fixture(scope="module")
def client():
    from api.main import app
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture()
def minted_key():
    key = "pc_test_" + secrets.token_urlsafe(16)
    key_hash = hashlib.sha256(key.encode()).hexdigest()
    db = SessionLocal()
    try:
        db.execute(text("""
            INSERT INTO api_keys (key_hash, label, owner_email, tier, active, created_at, updated_at)
            VALUES (:h, :l, 'pytest@example.com', 'partner', true, now(), now())
        """), {"h": key_hash, "l": TEST_LABEL})
        db.commit()
        yield key
        db.execute(text("DELETE FROM api_keys WHERE label = :l"), {"l": TEST_LABEL})
        db.commit()
    finally:
        db.close()


def _clear_key_cache():
    from api import main
    main._API_KEY_CACHE.clear()


@pytest.mark.integration
class TestApiKeyMiddleware:

    def test_public_tier_needs_no_key(self, client):
        assert client.get("/api/stats").status_code == 200

    def test_invalid_key_401(self, client):
        _clear_key_cache()
        resp = client.get("/api/stats", headers={"X-API-Key": "pc_live_not_a_real_key"})
        assert resp.status_code == 401

    def test_valid_key_passes_and_records_usage(self, client, minted_key):
        _clear_key_cache()
        resp = client.get("/api/stats", headers={"X-API-Key": minted_key})
        assert resp.status_code == 200
        db = SessionLocal()
        try:
            last_used = db.execute(text(
                "SELECT last_used_at FROM api_keys WHERE label = :l"
            ), {"l": TEST_LABEL}).scalar()
        finally:
            db.close()
        assert last_used is not None, "keyed request left no usage trace"

    def test_revoked_key_401(self, client, minted_key):
        db = SessionLocal()
        try:
            db.execute(text("UPDATE api_keys SET active = false WHERE label = :l"), {"l": TEST_LABEL})
            db.commit()
        finally:
            db.close()
        _clear_key_cache()
        resp = client.get("/api/stats", headers={"X-API-Key": minted_key})
        assert resp.status_code == 401


@pytest.mark.integration
class TestDeveloperDocs:
    """Every endpoint the docs page names must exist in the app."""

    def test_documented_endpoints_exist(self, client):
        html = (Path(__file__).parent.parent / "frontend" / "developers.html").read_text()
        documented = set(re.findall(r'<span class="method">GET</span>(/api/[^\s<?]+)', html))
        assert documented, "no endpoints found in developers.html; selector drifted?"
        for path in documented:
            probe = (path.replace("{zip}", "11216"))
            resp = client.get(probe)
            assert resp.status_code == 200, f"documented endpoint {path} returned {resp.status_code}"
