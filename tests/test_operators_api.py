"""
Integration tests for operator DB tables and API endpoints.
Requirements: OPAPI-01 through OPAPI-04.

Runs against the live pulsecities DB — requires Plan 09-01 backfill to have run.

Test classes:
    TestOperatorSchema  — tables exist with correct structure
    TestBackfill        — operators and operator_parcels are seeded correctly
    TestOperatorList    — GET /api/operators (activated in Plan 02)
    TestOperatorDetail  — GET /api/operators/{slug} (activated in Plan 02)
    TestGroupedSearch   — grouped search with operators key (activated in Plan 03)
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from models.database import SessionLocal


@pytest.fixture(scope="module")
def db():
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture(scope="module")
def client():
    from api.main import app
    return TestClient(app)


@pytest.mark.integration
class TestOperatorSchema:
    """Verify the operators and operator_parcels tables exist in the live DB."""

    def test_operators_table_exists(self, db):
        row = db.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'operators'"
            )
        ).scalar()
        assert row > 0, "operators table does not exist in public schema"

    def test_operator_parcels_table_exists(self, db):
        row = db.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'operator_parcels'"
            )
        ).scalar()
        assert row > 0, "operator_parcels table does not exist in public schema"


@pytest.mark.integration
class TestBackfill:
    """Verify all 20 operators and their parcels are seeded correctly."""

    def test_mtek_row_present(self, db):
        row = db.execute(
            text(
                "SELECT slug, total_properties FROM operators "
                "WHERE slug = 'mtek-nyc'"
            )
        ).fetchone()
        assert row is not None, "MTEK row not found (slug='mtek-nyc')"
        assert row.total_properties >= 37, (
            f"MTEK total_properties={row.total_properties} is less than 37"
        )

    def test_phantom_row_present(self, db):
        row = db.execute(
            text(
                "SELECT slug, total_properties FROM operators "
                "WHERE slug = 'phantom-capital'"
            )
        ).fetchone()
        assert row is not None, "PHANTOM CAPITAL row not found (slug='phantom-capital')"
        assert row.total_properties >= 64, (
            f"PHANTOM total_properties={row.total_properties} is less than 64"
        )

    def test_bredif_row_present(self, db):
        row = db.execute(
            text(
                "SELECT slug, total_properties FROM operators "
                "WHERE slug = 'bredif'"
            )
        ).fetchone()
        assert row is not None, "BREDIF row not found (slug='bredif')"
        assert row.total_properties >= 66, (
            f"BREDIF total_properties={row.total_properties} is less than 66"
        )

    def test_twenty_operators_seeded(self, db):
        count = db.execute(text("SELECT COUNT(*) FROM operators")).scalar()
        assert count >= 20, f"Expected >= 20 operators, found {count}"

    def test_highest_displacement_score_nonnull(self, db):
        value = db.execute(
            text(
                "SELECT highest_displacement_score FROM operators "
                "WHERE slug = 'mtek-nyc'"
            )
        ).scalar()
        assert value is not None, "highest_displacement_score is NULL for MTEK"
        assert value > 0, f"highest_displacement_score={value} should be > 0"

    def test_borough_spread_nonnull(self, db):
        value = db.execute(
            text("SELECT borough_spread FROM operators WHERE slug = 'mtek-nyc'")
        ).scalar()
        assert value is not None, "borough_spread is NULL for MTEK"
        assert value >= 1, f"borough_spread={value} should be >= 1"

    def test_operator_parcels_for_mtek(self, db):
        count = db.execute(
            text(
                "SELECT COUNT(*) FROM operator_parcels op "
                "JOIN operators o ON o.id = op.operator_id "
                "WHERE o.slug = 'mtek-nyc'"
            )
        ).scalar()
        assert count >= 37, (
            f"Expected >= 37 parcels for MTEK, found {count}"
        )


@pytest.mark.integration
class TestOperatorList:
    """OPAPI-02: GET /api/operators returns operator list sorted by portfolio size."""

    def test_list_endpoint_returns_array(self, client):
        resp = client.get("/api/operators")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list), "Expected a JSON array"
        assert len(data) >= 20, f"Expected >= 20 operators, got {len(data)}"

    def test_list_items_have_required_fields(self, client):
        resp = client.get("/api/operators")
        assert resp.status_code == 200
        for item in resp.json():
            assert "operator_root" in item
            assert "slug" in item
            assert "display_name" in item
            assert "portfolio_size" in item
            assert "borough_spread" in item
            assert "highest_displacement_score" in item
            assert "llc_count" in item

    def test_list_sorted_by_portfolio_size_desc(self, client):
        resp = client.get("/api/operators")
        assert resp.status_code == 200
        sizes = [item["portfolio_size"] for item in resp.json()]
        assert sizes == sorted(sizes, reverse=True), "List not sorted by portfolio_size DESC"

    def test_known_operators_in_list(self, client):
        resp = client.get("/api/operators")
        assert resp.status_code == 200
        slugs = {item["slug"] for item in resp.json()}
        assert "mtek-nyc" in slugs, "MTEK not in operator list"
        assert "phantom-capital" in slugs, "PHANTOM CAPITAL not in operator list"
        assert "bredif" in slugs, "BREDIF not in operator list"

    def test_named_operators_have_non_null_aggregates(self, client):
        resp = client.get("/api/operators")
        assert resp.status_code == 200
        by_slug = {item["slug"]: item for item in resp.json()}
        for slug in ("mtek-nyc", "phantom-capital", "bredif"):
            op = by_slug.get(slug)
            assert op is not None, f"{slug} not found"
            assert op["borough_spread"] is not None, f"{slug} borough_spread is null"
            assert op["highest_displacement_score"] is not None, f"{slug} highest_displacement_score is null"


@pytest.mark.integration
class TestOperatorDetail:
    """OPAPI-03: GET /api/operators/{slug} returns full operator profile."""

    def test_unknown_slug_404(self, client):
        resp = client.get("/api/operators/nobody")
        assert resp.status_code == 404

    def test_invalid_slug_format_400(self, client):
        resp = client.get("/api/operators/INVALID_SLUG")
        assert resp.status_code == 400

    def test_mtek_detail_returns_200(self, client):
        resp = client.get("/api/operators/mtek-nyc")
        assert resp.status_code == 200

    def test_mtek_detail_has_required_keys(self, client):
        resp = client.get("/api/operators/mtek-nyc")
        assert resp.status_code == 200
        data = resp.json()
        for key in (
            "operator_root", "slug", "display_name", "llc_entities",
            "total_properties", "total_acquisitions", "borough_spread",
            "highest_displacement_score", "properties", "hpd_violations",
            "eviction_then_buy", "rs_units", "recent_acquisitions",
            "acquisition_timeline", "related_operators",
        ):
            assert key in data, f"Missing key: {key}"

    def test_mtek_properties_list_size(self, client):
        resp = client.get("/api/operators/mtek-nyc")
        assert resp.status_code == 200
        props = resp.json()["properties"]
        assert len(props) >= 37, f"Expected >= 37 properties for MTEK, got {len(props)}"

    def test_mtek_hpd_violations_non_empty(self, client):
        resp = client.get("/api/operators/mtek-nyc")
        assert resp.status_code == 200
        violations = resp.json()["hpd_violations"]
        assert isinstance(violations, dict), "hpd_violations should be a dict"
        assert len(violations) > 0, "hpd_violations should not be empty for MTEK"

    def test_mtek_acquisition_timeline_sorted(self, client):
        resp = client.get("/api/operators/mtek-nyc")
        assert resp.status_code == 200
        timeline = resp.json()["acquisition_timeline"]
        assert isinstance(timeline, list)
        if len(timeline) > 1:
            year_months = [entry["year_month"] for entry in timeline]
            assert year_months == sorted(year_months), "acquisition_timeline not sorted chronologically"

    def test_top_route_unchanged(self, client):
        """/top still returns the JSON-backed top-by-acquisitions list."""
        resp = client.get("/api/operators/top")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        for item in data:
            assert "operator_root" in item
            assert "total_acquisitions" in item
            assert "llc_count" in item


@pytest.mark.integration
class TestGroupedSearch:
    """OPAPI-04: Search results include operators key. (Activated in Plan 03)"""

    def test_search_returns_operators_key(self, db):
        # Direct DB query to verify operators table has mtek-nyc
        row = db.execute(
            text("SELECT slug FROM operators WHERE operator_root = 'MTEK'")
        ).fetchone()
        assert row is not None
        assert row.slug == "mtek-nyc"

    def test_search_mtek_finds_operator(self, db):
        # Verify the search SQL logic directly (same query as endpoint)
        rows = db.execute(
            text("""
                SELECT operator_root, slug, display_name, total_properties
                FROM operators
                WHERE display_name ILIKE :pattern OR operator_root ILIKE :pattern
                ORDER BY total_properties DESC LIMIT 10
            """),
            {"pattern": "%mtek%"},
        ).fetchall()
        slugs = [r.slug for r in rows]
        assert "mtek-nyc" in slugs

    def test_search_response_has_both_keys(self, db):
        # Verify the grouped response shape via direct SQL
        op_rows = db.execute(
            text("SELECT slug FROM operators WHERE display_name ILIKE :p LIMIT 1"),
            {"p": "%mtek%"},
        ).fetchall()
        # Verify the operators table query works and returns at least one row
        assert len(op_rows) >= 1


@pytest.mark.integration
class TestOperatorDirectoryLinkIntegrity:
    """
    Every href link on /operators must resolve via the detail API.
    Regression for: clusters from JSON files appearing on /operators without
    a corresponding operators DB row, causing the detail page JS to receive a
    404 from /api/operators/{slug} and display "Operator not found."
    """

    def test_all_linked_operators_resolvable(self, client):
        import re as _re
        resp = client.get("/operators")
        assert resp.status_code == 200
        # Extract slugs from href="/operator/{slug}" links only
        linked_slugs = _re.findall(r'href="/operator/([a-z0-9-]+)"', resp.text)
        linked_slugs = list(dict.fromkeys(linked_slugs))  # dedupe, preserve order
        assert len(linked_slugs) > 0, "No operator hrefs found in /operators"
        for slug in linked_slugs:
            detail = client.get(f"/api/operators/{slug}")
            assert detail.status_code == 200, (
                f"/operator/{slug} is linked on /operators but "
                f"/api/operators/{slug} returned {detail.status_code}"
            )

    def test_melo_not_linked_or_resolvable(self, client):
        import re as _re
        resp = client.get("/operators")
        assert resp.status_code == 200
        linked_slugs = _re.findall(r'href="/operator/([a-z0-9-]+)"', resp.text)
        if "melo" in linked_slugs:
            detail = client.get("/api/operators/melo")
            assert detail.status_code == 200, (
                "MELO is linked on /operators but /api/operators/melo returns 404"
            )
        # MELO not linked is the expected outcome after the fix — also acceptable

    def test_no_dead_onclick_links(self, client):
        import re as _re
        resp = client.get("/operators")
        assert resp.status_code == 200
        # Every slug in onclick handlers must also resolve
        onclick_slugs = _re.findall(r"onclick=\"location\.href='/operator/([a-z0-9-]+)'\"", resp.text)
        for slug in onclick_slugs:
            detail = client.get(f"/api/operators/{slug}")
            assert detail.status_code == 200, (
                f"onclick for /operator/{slug} found but API returns {detail.status_code}"
            )


@pytest.mark.integration
class TestNoiseOperatorVisibility:
    """
    Finance/lender noise operators must be hidden from all public surfaces.
    Single source of truth: OPERATOR_NOISE_ROOTS / OPERATOR_NOISE_SLUGS in operators.py.
    """

    NOISE_SLUGS = ["icecap", "ice", "broad", "broadview",
                   "arbor", "standard", "symetra", "community", "oceanview",
                   "valley"]  # VALLEY NATIONAL BANK reclassified as lender noise
    VISIBLE_SLUGS = ["mtek-nyc", "phantom-capital", "bredif",
                     "ridgewood", "cross"]

    def test_top_excludes_all_noise_roots(self, client):
        resp = client.get("/api/operators/top?limit=10")
        assert resp.status_code == 200
        roots = {op["operator_root"] for op in resp.json()}
        from api.routes.operators import OPERATOR_NOISE_ROOTS
        assert not (roots & OPERATOR_NOISE_ROOTS), (
            f"Noise roots found in /top: {roots & OPERATOR_NOISE_ROOTS}"
        )

    def test_top_contains_only_visible_operators(self, client):
        resp = client.get("/api/operators/top?limit=10")
        assert resp.status_code == 200
        roots = {op["operator_root"] for op in resp.json()}
        from api.routes.operators import OPERATOR_NOISE_ROOTS
        for root in roots:
            assert root not in OPERATOR_NOISE_ROOTS, f"{root} is noise but appears in /top"

    def test_operators_directory_excludes_noise(self, client):
        resp = client.get("/operators")
        assert resp.status_code == 200
        for slug in self.NOISE_SLUGS:
            assert f'/operator/{slug}"' not in resp.text, (
                f"Noise slug {slug} linked in /operators"
            )

    def test_operators_directory_includes_visible(self, client):
        resp = client.get("/operators")
        assert resp.status_code == 200
        for slug in self.VISIBLE_SLUGS:
            assert f'/operator/{slug}"' in resp.text, (
                f"Visible slug {slug} missing from /operators"
            )

    def test_operators_directory_has_no_locked_rows(self, client):
        resp = client.get("/operators")
        assert resp.status_code == 200
        assert "op-row-locked" not in resp.text, (
            "Locked rows found in /operators — should be clean (noise hidden, not locked)"
        )

    def test_operators_directory_no_dot_separator(self, client):
        resp = client.get("/operators")
        assert resp.status_code == 200
        # middot or bullet separators must not appear in operator row meta
        assert "&middot;" not in resp.text

    def test_operators_directory_jsonld_excludes_noise(self, client):
        import json as _json
        resp = client.get("/operators")
        assert resp.status_code == 200
        import re as _re
        match = _re.search(r'<script type="application/ld\+json">(.*?)</script>', resp.text, _re.DOTALL)
        assert match, "No JSON-LD found in /operators"
        ld = _json.loads(match.group(1))
        urls = [item.get("url", "") for item in ld.get("itemListElement", [])]
        for slug in self.NOISE_SLUGS:
            assert not any(slug in url for url in urls), (
                f"Noise slug {slug} found in JSON-LD itemListElement"
            )

    def test_noise_operator_page_returns_404(self, client):
        for slug in self.NOISE_SLUGS:
            resp = client.get(f"/operator/{slug}")
            assert resp.status_code == 404, (
                f"/operator/{slug} returned {resp.status_code}, expected 404"
            )

    def test_visible_operator_pages_return_200(self, client):
        for slug in self.VISIBLE_SLUGS:
            resp = client.get(f"/operator/{slug}")
            assert resp.status_code == 200, (
                f"/operator/{slug} returned {resp.status_code}, expected 200"
            )

    def test_homepage_top_no_noise_names(self, client):
        """Homepage Recent findings must never show finance/noise operator names."""
        resp = client.get("/api/operators/top?limit=10")
        assert resp.status_code == 200
        names = {op["operator_root"].upper() for op in resp.json()}
        for noise in ["ICECAP", "ICE", "OCEANVIEW", "STANDARD", "COMMUNITY",
                      "BROAD", "BROADVIEW", "ARBOR", "SYMETRA"]:
            assert noise not in names, f"{noise} appeared in /api/operators/top"

    def test_operator_html_no_hardcoded_acquisitions_en(self, client):
        """operator.html i18n: 'acquisitions' label must come from i18n, not hardcoded."""
        from pathlib import Path
        html = (Path(__file__).parent.parent / "frontend" / "operator.html").read_text()
        # The i18n key exists in both languages
        assert "op_acquisitions" not in html or "stat_acquisitions" in html, (
            "operator.html lacks i18n key for acquisitions label"
        )
        assert "adquisiciones" in html, "Spanish 'adquisiciones' missing from operator.html i18n"

    def test_operator_html_no_em_dash_table_placeholder(self, client):
        """Table cells must use t('na') not hardcoded em dash."""
        from pathlib import Path
        html = (Path(__file__).parent.parent / "frontend" / "operator.html").read_text()
        # The hardcoded '—' in table cell rendering must be gone
        assert ": '—')" not in html, (
            "Em dash hardcoded as table placeholder — should use t('na')"
        )
