"""
Route integrity tests for PulseCities frontend pages.

Verifies that each static HTML file exists, contains the right content
markers, and does not contain markers from the wrong page.
These tests do not require a live database.

For live HTTP route testing, use: scripts/smoke_routes.sh
"""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

FRONTEND = Path(__file__).parent.parent / "frontend"


class TestStaticFileContent:
    """Each page file must exist and contain page-specific markers."""

    def test_methodology_html_exists(self):
        assert (FRONTEND / "methodology.html").exists()

    def test_methodology_is_standalone_page(self):
        content = (FRONTEND / "methodology.html").read_text()
        assert "PulseCities methodology" in content

    def test_methodology_is_not_app(self):
        content = (FRONTEND / "methodology.html").read_text()
        # app.html embeds the methodology modal; methodology.html must not
        assert "methodology-modal" not in content
        # app.html loads the MapLibre script bundle; methodology.html only
        # mentions it as a tech chip in copy, never loads it
        assert "maplibre-gl@" not in content.lower()

    def test_about_html_exists(self):
        assert (FRONTEND / "about.html").exists()

    def test_about_has_correct_marker(self):
        content = (FRONTEND / "about.html").read_text()
        assert "About PulseCities" in content

    def test_about_is_not_index(self):
        content = (FRONTEND / "about.html").read_text()
        assert "search-input" not in content

    def test_app_html_has_maplibre(self):
        content = (FRONTEND / "app.html").read_text()
        assert "maplibre" in content.lower()

    def test_app_html_no_dead_methodology_handler(self):
        content = (FRONTEND / "app.html").read_text()
        # Removed: auto-open modal when pathname === '/methodology'
        assert "pathname === '/methodology'" not in content

    def test_operator_html_exists(self):
        assert (FRONTEND / "operator.html").exists()

    def test_index_html_exists(self):
        assert (FRONTEND / "index.html").exists()


class TestFastAPIRoutes:
    """
    FastAPI serves the right file for each static route.
    These tests run against the TestClient (no nginx, no live DB needed
    for static routes).
    """

    @pytest.fixture(scope="class")
    def client(self):
        from api.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

    def test_map_returns_app_html(self, client):
        resp = client.get("/map")
        assert resp.status_code == 200
        assert "maplibre" in resp.text.lower()

    def test_methodology_returns_methodology_html(self, client):
        resp = client.get("/methodology")
        assert resp.status_code == 200
        assert "PulseCities methodology" in resp.text

    def test_methodology_not_app_html(self, client):
        resp = client.get("/methodology")
        assert "methodology-modal" not in resp.text

    def test_about_returns_about_html(self, client):
        resp = client.get("/about")
        assert resp.status_code == 200
        assert "About PulseCities" in resp.text

    def test_about_not_index_html(self, client):
        resp = client.get("/about")
        assert "search-input" not in resp.text

    def test_operator_returns_operator_shell(self, client):
        resp = client.get("/operator/mtek-nyc")
        assert resp.status_code == 200
        # Must serve operator.html shell, not app.html
        assert "maplibre" not in resp.text.lower()
