"""
Integration tests for neighborhood API endpoints.
Requirements: API-01 (GeoJSON FeatureCollection), API-02 (score + signal_breakdown + last_updated).
Runs against real database — requires 02-01 (ZCTA load) and 02-02 (scoring) to have run.
"""

import pytest
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)


@pytest.mark.integration
class TestNeighborhoodsGeoJSON:
    """API-01: GET /api/neighborhoods returns GeoJSON FeatureCollection."""

    def test_returns_200(self):
        resp = client.get("/api/neighborhoods")
        assert resp.status_code == 200

    def test_is_feature_collection(self):
        resp = client.get("/api/neighborhoods")
        body = resp.json()
        assert body["type"] == "FeatureCollection"
        assert "features" in body
        assert isinstance(body["features"], list)

    def test_has_features(self):
        resp = client.get("/api/neighborhoods")
        body = resp.json()
        assert len(body["features"]) > 0, "neighborhoods table should have ZCTA data"

    def test_feature_structure(self):
        resp = client.get("/api/neighborhoods")
        feature = resp.json()["features"][0]
        assert feature["type"] == "Feature"
        assert "geometry" in feature
        assert feature["geometry"]["type"] in ("Polygon", "MultiPolygon")
        assert "coordinates" in feature["geometry"]
        assert "properties" in feature

    def test_feature_properties_include_score(self):
        resp = client.get("/api/neighborhoods")
        feature = resp.json()["features"][0]
        props = feature["properties"]
        assert "score" in props
        assert "zip_code" in props
        assert "last_updated" in props


@pytest.mark.integration
class TestNeighborhoodScore:
    """API-02: GET /api/neighborhoods/{zip_code}/score returns score + signal_breakdown."""

    def test_valid_zip_returns_200_or_404(self):
        # Use a zip code known to exist from ZCTA load
        resp = client.get("/api/neighborhoods")
        zip_code = resp.json()["features"][0]["properties"]["zip_code"]
        resp2 = client.get(f"/api/neighborhoods/{zip_code}/score")
        assert resp2.status_code in (200, 404)  # 404 if no score yet

    def test_score_response_shape(self):
        # Find a scored zip code
        resp = client.get("/api/neighborhoods")
        scored = [f for f in resp.json()["features"] if f["properties"]["score"] is not None]
        if not scored:
            pytest.skip("No scored neighborhoods yet")
        zip_code = scored[0]["properties"]["zip_code"]
        resp2 = client.get(f"/api/neighborhoods/{zip_code}/score")
        body = resp2.json()
        assert "score" in body
        assert "signal_breakdown" in body
        assert "last_updated" in body

    def test_nonexistent_zip_returns_404(self):
        resp = client.get("/api/neighborhoods/99999/score")
        assert resp.status_code == 404

    def test_invalid_zip_returns_400(self):
        resp = client.get("/api/neighborhoods/abc/score")
        assert resp.status_code == 400

    def test_signal_breakdown_five_keys(self):
        """
        signal_breakdown must contain all five named signal keys.
        Verifies SCOR-02 at the API layer.
        """
        resp = client.get("/api/neighborhoods")
        scored = [f for f in resp.json()["features"] if f["properties"]["score"] is not None]
        if not scored:
            pytest.skip("No scored neighborhoods yet — run compute_scores() first")
        zip_code = scored[0]["properties"]["zip_code"]
        resp2 = client.get(f"/api/neighborhoods/{zip_code}/score")
        assert resp2.status_code == 200
        body = resp2.json()
        assert "signal_breakdown" in body
        breakdown = body["signal_breakdown"]
        for key in ("permits", "evictions", "llc_acquisitions", "assessment_spike", "complaint_rate"):
            assert key in breakdown, f"'{key}' missing from signal_breakdown — got: {list(breakdown.keys())}"
            assert isinstance(breakdown[key], (int, float)), f"signal_breakdown[{key!r}] is not numeric"

    def test_signal_last_updated_in_response(self):
        """
        Response must include signal_last_updated dict with per-signal timestamps.
        Verifies D-18: last_updated per signal exposed in API response.
        """
        resp = client.get("/api/neighborhoods")
        scored = [f for f in resp.json()["features"] if f["properties"]["score"] is not None]
        if not scored:
            pytest.skip("No scored neighborhoods yet — run compute_scores() first")
        zip_code = scored[0]["properties"]["zip_code"]
        resp2 = client.get(f"/api/neighborhoods/{zip_code}/score")
        assert resp2.status_code == 200
        body = resp2.json()
        assert "signal_last_updated" in body, "signal_last_updated missing from response"
        sig_updated = body["signal_last_updated"]
        assert isinstance(sig_updated, dict), f"signal_last_updated must be dict, got {type(sig_updated)}"

    def test_bulk_endpoint_has_no_signal_breakdown(self):
        """
        GET /api/neighborhoods (FeatureCollection) must NOT include signal_breakdown
        in feature properties — only composite score (payload size concern per D-19).
        """
        resp = client.get("/api/neighborhoods")
        assert resp.status_code == 200
        features = resp.json()["features"]
        if not features:
            pytest.skip("No neighborhood features loaded yet")
        # Check first feature's properties — signal_breakdown must not be present
        props = features[0]["properties"]
        assert "signal_breakdown" not in props, (
            "signal_breakdown must NOT appear in bulk /api/neighborhoods response"
        )


@pytest.mark.integration
class TestTopRiskNeighborhoods:
    """GET /api/neighborhoods/top-risk — landing page at-risk list."""

    def test_returns_200(self):
        resp = client.get("/api/neighborhoods/top-risk")
        assert resp.status_code == 200

    def test_response_has_neighborhoods_list(self):
        resp = client.get("/api/neighborhoods/top-risk")
        body = resp.json()
        assert "neighborhoods" in body, f"Missing 'neighborhoods' key: {list(body.keys())}"
        assert isinstance(body["neighborhoods"], list)

    def test_item_shape(self):
        resp = client.get("/api/neighborhoods/top-risk")
        body = resp.json()
        if not body["neighborhoods"]:
            pytest.skip("No scored neighborhoods — run compute_scores() first")
        item = body["neighborhoods"][0]
        for key in ("rank", "zip_code", "score", "dominant_signal"):
            assert key in item, f"'{key}' missing from top-risk item — got: {list(item.keys())}"
        assert item["rank"] == 1, "First item must have rank=1"

    def test_ordered_by_score_descending(self):
        resp = client.get("/api/neighborhoods/top-risk?limit=10")
        body = resp.json()
        neighborhoods = body["neighborhoods"]
        if len(neighborhoods) < 2:
            pytest.skip("Need at least 2 scored neighborhoods to verify ordering")
        scores = [n["score"] for n in neighborhoods]
        assert scores == sorted(scores, reverse=True), (
            "Neighborhoods must be sorted by score descending"
        )

    def test_default_limit_is_10(self):
        resp = client.get("/api/neighborhoods/top-risk")
        body = resp.json()
        assert len(body["neighborhoods"]) <= 10, (
            f"Default limit should be at most 10, got {len(body['neighborhoods'])}"
        )

    def test_limit_param_respected(self):
        resp = client.get("/api/neighborhoods/top-risk?limit=3")
        body = resp.json()
        assert len(body["neighborhoods"]) <= 3, (
            f"limit=3 should return at most 3 items, got {len(body['neighborhoods'])}"
        )

    def test_limit_capped_at_25(self):
        """Requesting limit=100 must return at most 25 regardless."""
        resp = client.get("/api/neighborhoods/top-risk?limit=100")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["neighborhoods"]) <= 25, (
            f"limit cap at 25 not enforced — got {len(body['neighborhoods'])} items"
        )

    def test_zip_code_is_five_digit_string(self):
        resp = client.get("/api/neighborhoods/top-risk")
        body = resp.json()
        if not body["neighborhoods"]:
            pytest.skip("No scored neighborhoods yet")
        for item in body["neighborhoods"]:
            assert len(item["zip_code"]) == 5 and item["zip_code"].isdigit(), (
                f"zip_code must be a 5-digit string, got {item['zip_code']!r}"
            )

    def test_score_is_numeric(self):
        resp = client.get("/api/neighborhoods/top-risk")
        body = resp.json()
        if not body["neighborhoods"]:
            pytest.skip("No scored neighborhoods yet")
        for item in body["neighborhoods"]:
            assert isinstance(item["score"], (int, float)), (
                f"score must be numeric, got {type(item['score'])}"
            )


@pytest.mark.integration
class TestTopRiskNewFields:
    """Plan 999.1-01: GET /api/neighborhoods/top-risk must include raw_count + percentile_tier."""

    def test_top_risk_has_raw_count(self):
        resp = client.get("/api/neighborhoods/top-risk?limit=3")
        assert resp.status_code == 200
        items = resp.json().get("neighborhoods", [])
        if not items:
            pytest.skip("No neighborhoods in test DB")
        for item in items:
            assert "raw_count" in item, f"Missing raw_count in item: {list(item.keys())}"
            assert isinstance(item["raw_count"], int), "raw_count must be int"

    def test_top_risk_has_raw_count_label(self):
        resp = client.get("/api/neighborhoods/top-risk?limit=3")
        items = resp.json().get("neighborhoods", [])
        if not items:
            pytest.skip("No neighborhoods in test DB")
        for item in items:
            assert "raw_count_label" in item, f"Missing raw_count_label: {list(item.keys())}"
            assert isinstance(item["raw_count_label"], str)

    def test_top_risk_has_percentile_tier(self):
        resp = client.get("/api/neighborhoods/top-risk?limit=3")
        items = resp.json().get("neighborhoods", [])
        if not items:
            pytest.skip("No neighborhoods in test DB")
        for item in items:
            assert "percentile_tier" in item, f"Missing percentile_tier: {list(item.keys())}"
            assert item["percentile_tier"].startswith("top "), (
                f"percentile_tier should start with 'top ': {item['percentile_tier']}"
            )
            assert "%" in item["percentile_tier"], (
                f"percentile_tier should contain '%': {item['percentile_tier']}"
            )
