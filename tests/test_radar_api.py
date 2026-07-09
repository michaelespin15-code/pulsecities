"""
Speculation Radar — regression guards for GET /api/radar and the SSR /radar page.

The radar promises one specific, defensible pattern: a single LLC buyer took the
deed on three or more distinct buildings in the same ZIP inside a 90-day window.
That is cluster detection ahead of entity resolution — it names the pattern, not
the person. Failure modes pinned here:

  1. Threshold drift — a query change that lets 1- or 2-building buyers in turns
     the radar into a generic acquisitions list and kills the "concentrated
     buying" claim the page makes.
  2. Window drift — deeds older than the stated window inflating a cluster.
  3. Lender noise — a servicer or GSE taking title on scattered lots reads as a
     speculation cluster unless the same noise filter Flip Watch uses is applied.

The unit checks run in CI without a database. The integration checks run against
the live DB and skip gracefully when no clusters exist in the current window.
"""

import pytest

from api.routes.flips import _NOISE_TERMS
from api.routes.radar import (
    _NOISE_SQL,
    MIN_BUILDINGS,
    RADAR_WINDOW_DAYS,
    FEED_LIMIT,
)


def _get_client():
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


# ---------------------------------------------------------------------------
# Unit — no database required
# ---------------------------------------------------------------------------

class TestRadarConstants:
    def test_cluster_threshold_matches_published_claim(self):
        # The page states "3 or more buildings". Anything lower turns the radar
        # into an ordinary acquisitions feed.
        assert MIN_BUILDINGS == 3

    def test_window_matches_published_claim(self):
        # The page states "within 90 days".
        assert RADAR_WINDOW_DAYS == 90
        assert FEED_LIMIT > 0

    def test_noise_filter_reuses_flip_watch_terms(self):
        # One source of truth: the radar must exclude the same lender/servicer
        # entities Flip Watch does. If a term is added there, it applies here.
        for term in _NOISE_TERMS:
            assert f"'%{term}%'" in _NOISE_SQL, f"{term} missing from radar noise SQL"
        assert _NOISE_SQL.count("NOT ILIKE") == len(_NOISE_TERMS)


# ---------------------------------------------------------------------------
# Integration — requires a live PostgreSQL database
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestRadarAPI:
    def test_endpoint_returns_200_with_documented_shape(self):
        client = _get_client()
        resp = client.get("/api/radar")
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text}"
        body = resp.json()
        for key in ("window_days", "min_buildings", "count", "clusters"):
            assert key in body, f"missing key {key}"
        assert body["window_days"] == RADAR_WINDOW_DAYS
        assert body["min_buildings"] == MIN_BUILDINGS
        assert isinstance(body["clusters"], list)
        assert body["count"] == len(body["clusters"])
        assert body["count"] <= FEED_LIMIT

    def test_each_cluster_has_required_keys(self):
        client = _get_client()
        clusters = client.get("/api/radar").json()["clusters"]
        if not clusters:
            pytest.skip("No clusters detected in the current window")
        required = {
            "buyer", "zip_code", "neighborhood", "building_count",
            "first_deed", "last_deed", "span_days", "total_amount", "properties",
        }
        for c in clusters:
            assert required.issubset(c.keys()), f"missing keys: {required - c.keys()}"

    def test_every_cluster_meets_the_threshold(self):
        # Core guard: 3+ distinct buildings, and the count matches the property
        # list actually shown, so the headline number is never inflated.
        client = _get_client()
        clusters = client.get("/api/radar").json()["clusters"]
        if not clusters:
            pytest.skip("No clusters detected in the current window")
        for c in clusters:
            assert c["building_count"] >= MIN_BUILDINGS, c["buyer"]
            distinct_bbls = {p["bbl"] for p in c["properties"]}
            assert len(distinct_bbls) == c["building_count"], (
                f"{c['buyer']}: building_count {c['building_count']} != "
                f"{len(distinct_bbls)} distinct BBLs in properties"
            )

    def test_every_cluster_fits_the_window(self):
        # Core guard: the whole buying run happened inside the stated window.
        client = _get_client()
        clusters = client.get("/api/radar").json()["clusters"]
        if not clusters:
            pytest.skip("No clusters detected in the current window")
        for c in clusters:
            assert c["first_deed"] <= c["last_deed"], c["buyer"]
            assert 0 <= c["span_days"] <= RADAR_WINDOW_DAYS, (
                f"{c['buyer']}: span {c['span_days']}d outside [0, {RADAR_WINDOW_DAYS}]"
            )

    def test_buyers_are_llcs_with_no_lender_noise(self):
        client = _get_client()
        clusters = client.get("/api/radar").json()["clusters"]
        if not clusters:
            pytest.skip("No clusters detected in the current window")
        for c in clusters:
            name = (c["buyer"] or "").upper()
            assert "LLC" in name, f"non-LLC buyer in radar: {c['buyer']}"
            leaked = [t for t in _NOISE_TERMS if t in name]
            assert not leaked, f"lender noise leaked: {c['buyer']} matched {leaked}"

    def test_zip_codes_are_five_digits(self):
        client = _get_client()
        clusters = client.get("/api/radar").json()["clusters"]
        if not clusters:
            pytest.skip("No clusters detected in the current window")
        for c in clusters:
            assert len(str(c["zip_code"])) == 5 and str(c["zip_code"]).isdigit()


@pytest.mark.integration
class TestRadarPage:
    def test_ssr_page_renders_with_core_markers(self):
        client = _get_client()
        resp = client.get("/radar")
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text[:200]}"
        html = resp.text
        assert "Speculation Radar" in html
        assert '<link rel="canonical" href="https://pulsecities.com/radar">' in html
        # The defensibility disclaimer must be present so the page never reads
        # as an accusation about any buyer.
        assert "not wrongdoing" in html

    def test_cluster_properties_link_to_property_pages(self):
        client = _get_client()
        html = client.get("/radar").text
        if "radar-row" in html:
            assert "/property/" in html
