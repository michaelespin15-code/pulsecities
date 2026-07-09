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


@pytest.mark.integration
class TestWeeklyArchive:

    def test_archive_index_200(self, client):
        resp = client.get("/this-week/archive")
        assert resp.status_code == 200
        assert "Weekly review archive" in resp.text

    def test_this_week_links_to_archive(self, client):
        assert "/this-week/archive" in client.get("/this-week").text

    def test_a_real_week_edition_renders(self, client):
        # Pull a live edition slug from the archive rather than hardcoding a date.
        import re
        body = client.get("/this-week/archive").text
        m = re.search(r"/week/(\d{4}-W\d{2})", body)
        if not m:
            pytest.skip("no completed weeks in this environment yet")
        slug = m.group(1)
        resp = client.get(f"/week/{slug}")
        assert resp.status_code == 200
        assert "Score movers" in resp.text
        assert "New on the record" in resp.text
        assert f'rel="canonical" href="https://pulsecities.com/week/{slug}"' in resp.text
        assert "BreadcrumbList" in resp.text

    def test_out_of_range_week_404(self, client):
        assert client.get("/week/2020-W01").status_code == 404

    def test_malformed_week_404(self, client):
        assert client.get("/week/not-a-week").status_code == 404

    def test_no_em_dash_in_week_pages(self, client):
        assert "—" not in client.get("/this-week/archive").text
