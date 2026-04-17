"""
Integration tests for GET /api/stats — plan 999.1-01.

Covers:
  1. Returns 200 with correct top-level keys
  2. llc_transfers_30d and evictions_30d are non-negative integers
  3. top_risk object has required shape (zip_code, name, borough, score, last_updated)
  4. X-RateLimit-Limit header is present (confirms slowapi is active)
"""

import pytest
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)


@pytest.mark.integration
class TestStatsAPI:
    """GET /api/stats — homepage hero chip data."""

    def test_returns_200(self):
        resp = client.get("/api/stats")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"

    def test_returns_correct_top_level_keys(self):
        resp = client.get("/api/stats")
        body = resp.json()
        assert "llc_transfers_30d" in body, f"Missing llc_transfers_30d: {list(body.keys())}"
        assert "evictions_30d" in body, f"Missing evictions_30d: {list(body.keys())}"
        assert "top_risk" in body, f"Missing top_risk: {list(body.keys())}"

    def test_counts_are_non_negative_integers(self):
        resp = client.get("/api/stats")
        body = resp.json()
        assert isinstance(body["llc_transfers_30d"], int), "llc_transfers_30d must be int"
        assert isinstance(body["evictions_30d"], int), "evictions_30d must be int"
        assert body["llc_transfers_30d"] >= 0
        assert body["evictions_30d"] >= 0

    def test_top_risk_shape_when_present(self):
        resp = client.get("/api/stats")
        body = resp.json()
        if body["top_risk"] is None:
            pytest.skip("No displacement scores in test DB")
        tr = body["top_risk"]
        for key in ("zip_code", "name", "borough", "score", "last_updated"):
            assert key in tr, f"top_risk missing key '{key}': {list(tr.keys())}"

    def test_rate_limit_header_present(self):
        resp = client.get("/api/stats")
        assert "x-ratelimit-limit" in resp.headers, (
            f"X-RateLimit-Limit missing from headers: {dict(resp.headers)}"
        )
