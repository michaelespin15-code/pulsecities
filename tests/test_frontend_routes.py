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

    def test_operator_head_returns_200(self, client):
        for slug in ("mtek-nyc", "phantom-capital", "bredif"):
            resp = client.head(f"/operator/{slug}")
            assert resp.status_code == 200, f"HEAD /operator/{slug} returned {resp.status_code}"

    def test_operator_head_has_no_body(self, client):
        resp = client.head("/operator/mtek-nyc")
        assert resp.content == b""


@pytest.mark.integration
class TestNeighborhoodOGInjection:
    """
    Verify /neighborhood/{zip} pages are full SSR civic intelligence cards:
    unique OG/Twitter meta, visible body content, FAQ, copy-link, map CTA.
    Requires a live database.
    """

    @pytest.fixture(scope="class")
    def client(self):
        from api.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

    # --- meta / OG ---

    def test_neighborhood_returns_200(self, client):
        assert client.get("/neighborhood/11221").status_code == 200

    def test_neighborhood_is_not_app_shell(self, client):
        resp = client.get("/neighborhood/11221")
        assert "maplibre" not in resp.text.lower(), "page returned app.html shell"

    def test_og_title_is_specific(self, client):
        resp = client.get("/neighborhood/11216")
        line = next((l for l in resp.text.splitlines() if 'property="og:title"' in l), "")
        assert "11216" in line or "Bedford" in line, f"og:title not specific: {line}"

    def test_og_description_is_specific(self, client):
        resp = client.get("/neighborhood/11216")
        line = next((l for l in resp.text.splitlines() if 'property="og:description"' in l), "")
        assert "178 neighborhoods" not in line, f"og:description still generic: {line}"

    def test_og_url_is_correct(self, client):
        resp = client.get("/neighborhood/11216")
        assert 'content="https://pulsecities.com/neighborhood/11216"' in resp.text

    def test_og_image_present(self, client):
        resp = client.get("/neighborhood/11221")
        assert 'og:image' in resp.text
        assert "/og/11221.png" in resp.text

    def test_twitter_card_present(self, client):
        resp = client.get("/neighborhood/11221")
        assert 'twitter:card' in resp.text
        assert 'summary_large_image' in resp.text

    def test_canonical_is_correct(self, client):
        resp = client.get("/neighborhood/11221")
        assert 'href="https://pulsecities.com/neighborhood/11221"' in resp.text

    def test_title_tag_is_specific(self, client):
        resp = client.get("/neighborhood/11216")
        assert "<title>Explore | PulseCities</title>" not in resp.text
        assert "Bedford" in resp.text or "11216" in resp.text

    # --- visible body content ---

    def test_h1_contains_displacement_signals(self, client):
        resp = client.get("/neighborhood/11221")
        assert "Displacement Signals" in resp.text

    def test_page_contains_neighborhood_name(self, client):
        resp = client.get("/neighborhood/11221")
        assert "Bushwick" in resp.text

    def test_page_contains_zip_code(self, client):
        resp = client.get("/neighborhood/11221")
        assert "11221" in resp.text

    def test_page_contains_score(self, client):
        resp = client.get("/neighborhood/11221")
        # Score block or "not yet available" fallback
        assert "DISPLACEMENT PRESSURE" in resp.text or "Score data not yet available" in resp.text

    def test_page_contains_signal_label(self, client):
        resp = client.get("/neighborhood/11221")
        assert "LLC property acquisitions" in resp.text

    def test_page_contains_methodology_link(self, client):
        resp = client.get("/neighborhood/11221")
        assert "methodology" in resp.text.lower()

    def test_page_contains_copy_link(self, client):
        resp = client.get("/neighborhood/11221")
        assert "Copy link" in resp.text

    def test_page_contains_map_cta(self, client):
        resp = client.get("/neighborhood/11221")
        assert "/map?q=11221" in resp.text

    # --- FAQ + JSON-LD ---

    def test_faq_question_present(self, client):
        resp = client.get("/neighborhood/11221")
        assert "What does this displacement score mean" in resp.text

    def test_faqpage_jsonld_present(self, client):
        resp = client.get("/neighborhood/11221")
        assert "FAQPage" in resp.text

    def test_dataset_jsonld_present(self, client):
        resp = client.get("/neighborhood/11221")
        assert '"Dataset"' in resp.text

    # --- other pilot ZIPs ---

    def test_bedford_stuyvesant_page(self, client):
        resp = client.get("/neighborhood/11216")
        assert resp.status_code == 200
        assert "Bedford" in resp.text
        assert "/map?q=11216" in resp.text

    def test_two_bridges_page(self, client):
        resp = client.get("/neighborhood/10038")
        assert resp.status_code == 200
        assert "Two Bridges" in resp.text
        assert "/map?q=10038" in resp.text

    def test_norwood_page(self, client):
        resp = client.get("/neighborhood/10467")
        assert resp.status_code == 200
        assert "Norwood" in resp.text
        assert "/map?q=10467" in resp.text

    def test_chelsea_page(self, client):
        resp = client.get("/neighborhood/10001")
        assert resp.status_code == 200
        assert "Chelsea" in resp.text
        assert "/map?q=10001" in resp.text
