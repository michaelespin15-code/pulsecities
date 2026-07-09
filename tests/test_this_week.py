"""
/this-week — standing weekly review page.
"""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    from api.main import app
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.mark.integration
class TestThisWeekPage:

    def test_returns_200_html(self, client):
        resp = client.get("/this-week")
        assert resp.status_code == 200
        assert "This week in NYC displacement" in resp.text

    def test_has_all_sections(self, client):
        body = client.get("/this-week").text
        for marker in ("Score movers", "New on the record", "Newest flips"):
            assert marker in body, f"section '{marker}' missing"

    def test_canonical_and_meta(self, client):
        body = client.get("/this-week").text
        assert 'rel="canonical" href="https://pulsecities.com/this-week"' in body
        assert 'property="og:title"' in body

    def test_no_em_dash(self, client):
        assert "—" not in client.get("/this-week").text

    def test_counts_are_formatted(self, client):
        body = client.get("/this-week").text
        assert "eviction filings" in body
        assert "311 housing complaints" in body
