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


class TestTierBands:
    """
    _tier_info must match the canonical bands used by the map legend,
    the weekly digest, and _build_summary: Low 0-33, Moderate 34-66,
    High 67-84, Critical 85+. A drifted copy here means the SSR meta
    description names a different tier than the map colors show.
    """

    def test_band_boundaries(self):
        from api.routes.frontend import _tier_info
        assert _tier_info(0)[0] == "Low"
        assert _tier_info(33)[0] == "Low"
        assert _tier_info(34)[0] == "Moderate"
        assert _tier_info(66)[0] == "Moderate"
        assert _tier_info(67)[0] == "High"
        assert _tier_info(84)[0] == "High"
        assert _tier_info(85)[0] == "Critical"
        assert _tier_info(100)[0] == "Critical"


@pytest.mark.integration
class TestPropertyPage:
    """
    /property/{bbl} SSR pages. Requires a live database. A scored parcel
    must render, not 500: this route broke silently once when a helper
    was renamed and only this call site kept the old name.
    """

    @pytest.fixture(scope="class")
    def client(self):
        from api.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

    @pytest.fixture(scope="class")
    def scored_bbl(self):
        from models.database import SessionLocal
        from sqlalchemy import text
        db = SessionLocal()
        try:
            row = db.execute(text(
                "SELECT p.bbl FROM parcels p "
                "JOIN displacement_scores ds ON p.zip_code = ds.zip_code "
                "WHERE ds.score IS NOT NULL LIMIT 1"
            )).fetchone()
        finally:
            db.close()
        if not row:
            pytest.skip("no scored parcel in the database")
        return row.bbl

    def test_scored_property_returns_200(self, client, scored_bbl):
        resp = client.get(f"/property/{scored_bbl}")
        assert resp.status_code == 200, f"/property/{scored_bbl} returned {resp.status_code}"

    def test_scored_property_has_og_description(self, client, scored_bbl):
        resp = client.get(f"/property/{scored_bbl}")
        line = next((l for l in resp.text.splitlines() if 'property="og:description"' in l), "")
        assert "displacement" in line.lower(), f"og:description missing or generic: {line}"

    def test_unknown_bbl_returns_404(self, client):
        """A 200 app shell here is a soft 404; crawlers must see a real one."""
        resp = client.get("/property/9999999999")
        assert resp.status_code == 404
        assert 'name="robots" content="noindex"' in resp.text

    def test_non_numeric_bbl_returns_404(self, client):
        resp = client.get("/property/not-a-bbl")
        assert resp.status_code == 404


class TestCanonicalTierBands:
    """
    Tripwire for tier-band drift in the client. Canonical bands are
    Low 0-33, Moderate 34-66, High 67-84, Critical 85+ everywhere:
    map fill, legend, panel label, summaries, digest. This has drifted
    three separate times; if a threshold changes, change it in every
    surface and update this test in the same commit.
    """

    def _app(self):
        return (Path(__file__).parent.parent / "frontend" / "app.html").read_text()

    def test_choropleth_uses_canonical_steps(self):
        app = self._app()
        assert "'step', ['coalesce', ['feature-state', 'score'], ['get', 'score']]" in app, \
            "choropleth no longer uses discrete steps over the replay-aware score"
        assert "interpolate', ['linear'], ['get', 'score']" not in app, \
            "choropleth reverted to a continuous ramp; legend bands no longer match the map"

    def test_no_legacy_score_thresholds(self):
        app = self._app()
        for legacy in ("score >= 70", "score >= 76", "score >= 55", "score >= 56", "s >= 76", "s >= 56"):
            assert legacy not in app, f"legacy tier threshold '{legacy}' is back in app.html"

    def test_og_images_use_canonical_thresholds(self):
        og = (Path(__file__).parent.parent / "api" / "routes" / "og_images.py").read_text()
        for legacy in ("score >= 70", "score >= 55", "score >= 35"):
            assert legacy not in og, f"legacy tier threshold '{legacy}' in og_images.py"
        assert "score >= 85" in og

    def test_canonical_thresholds_present(self):
        app = self._app()
        for canon in ("score >= 85", "score >= 67", "score >= 34"):
            assert canon in app, f"canonical threshold '{canon}' missing from app.html"

    def test_landing_page_uses_canonical_thresholds(self):
        idx = (Path(__file__).parent.parent / "frontend" / "index.html").read_text()
        for legacy in ("score >= 70", "score >= 40) return", "score >= 15) return"):
            assert legacy not in idx, f"legacy tier threshold '{legacy}' in index.html"

    # Canonical tier palette, decided 2026-07-10 from real map renders:
    # low #3E6B54, moderate #C08B2D, high #F97316, critical #EF4444.
    # Fills and chips carry the palette; low-as-text stays slate for
    # contrast. If a hex changes, change every surface in the same commit.
    _STALE_TIER_HEXES = ("#16a34a", "#eab308", "#22c55e", "#4ade80")

    def test_map_fill_and_legend_use_canonical_palette(self):
        app = self._app()
        for canon in ("#3E6B54", "#C08B2D"):
            assert canon in app, f"canonical tier color '{canon}' missing from app.html"
        for stale in self._STALE_TIER_HEXES:
            assert stale not in app, f"stale tier color '{stale}' is back in app.html"

    def test_landing_legend_uses_canonical_palette(self):
        idx = (Path(__file__).parent.parent / "frontend" / "index.html").read_text()
        for canon in ("#3E6B54", "#C08B2D", "#F97316", "#EF4444"):
            assert canon in idx, f"canonical tier color '{canon}' missing from index.html legend"
        for stale in self._STALE_TIER_HEXES:
            assert stale not in idx, f"stale tier color '{stale}' in index.html"

    def test_ssr_tier_colors_use_canonical_palette(self):
        for name in ("frontend.py", "briefs.py"):
            src = (Path(__file__).parent.parent / "api" / "routes" / name).read_text()
            assert "#C08B2D" in src, f"canonical moderate color missing from {name}"
            for stale in self._STALE_TIER_HEXES:
                assert stale not in src, f"stale tier color '{stale}' in {name}"

    def test_digest_tier_colors_use_canonical_palette(self):
        src = (Path(__file__).parent.parent / "scripts" / "weekly_digest.py").read_text()
        for canon in ("#3E6B54", "#C08B2D"):
            assert canon in src, f"canonical tier color '{canon}' missing from weekly_digest.py"
        for stale in self._STALE_TIER_HEXES:
            assert stale not in src, f"stale tier color '{stale}' in weekly_digest.py"


@pytest.mark.integration
class TestSearchResolvesDeedBbl:
    """
    Address search must land on the BBL that carries the deed record,
    not an adjacent lot. The 2026-06-24 audit found '1130 Greene Ave'
    resolving to a neighboring BBL with zero records, a dead end for a
    journalist verifying an acquisition.
    """

    def test_operator_acquisition_address_resolves_to_its_bbl(self):
        from api.main import app
        from models.database import SessionLocal
        from sqlalchemy import text as _text
        db = SessionLocal()
        try:
            row = db.execute(_text(
                "SELECT op.bbl, p.address FROM operator_parcels op "
                "JOIN operators o ON o.id = op.operator_id AND o.operator_class = 'operator' "
                "JOIN parcels p ON p.bbl = op.bbl "
                "WHERE p.address IS NOT NULL ORDER BY op.acquisition_date DESC LIMIT 1"
            )).fetchone()
        finally:
            db.close()
        if not row:
            pytest.skip("no operator acquisition with an address")
        with TestClient(app, raise_server_exceptions=False) as client:
            resp = client.get("/api/search/", params={"q": row.address})
            assert resp.status_code == 200
            props = resp.json()["groups"]["properties"]
            bbls = [p["bbl"] for p in props]
            assert row.bbl in bbls, (
                f"search for '{row.address}' returned {bbls}, expected deed BBL {row.bbl}"
            )
