"""
Route tests for GET /api/search/ grouped search endpoint.

Verifies bucket separation (neighborhoods / properties / operators),
response shape stability, edge cases, and that no fake data leaks in.

All tests require a live database — mark with @pytest.mark.integration.
Run with: pytest tests/test_search_api.py -m integration -v
"""

import pytest
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


@pytest.mark.integration
class TestSearchGroupedShape:
    """Response always has the stable groups shape."""

    def test_returns_200_and_groups_key(self):
        resp = client.get("/api/search/?q=11216")
        assert resp.status_code == 200
        body = resp.json()
        assert "query" in body
        assert "groups" in body
        assert set(body["groups"].keys()) == {"neighborhoods", "properties", "operators"}

    def test_each_result_has_type_and_href(self):
        resp = client.get("/api/search/?q=Bedford")
        assert resp.status_code == 200
        groups = resp.json()["groups"]
        for bucket in groups.values():
            for item in bucket:
                assert "type" in item, f"missing type: {item}"
                assert "href" in item, f"missing href: {item}"


@pytest.mark.integration
class TestSearchNeighborhoods:
    """Neighborhood bucket — ZIP and name matching."""

    def test_zip_returns_neighborhood(self):
        resp = client.get("/api/search/?q=11216")
        assert resp.status_code == 200
        nbhds = resp.json()["groups"]["neighborhoods"]
        assert len(nbhds) >= 1
        assert nbhds[0]["zip"] == "11216"
        assert nbhds[0]["type"] == "neighborhood"
        assert "/neighborhood/11216" in nbhds[0]["href"]

    def test_zip_result_includes_score(self):
        resp = client.get("/api/search/?q=11216")
        nbhds = resp.json()["groups"]["neighborhoods"]
        assert nbhds[0]["score"] is not None

    def test_name_search_bedford(self):
        resp = client.get("/api/search/?q=Bedford")
        assert resp.status_code == 200
        nbhds = resp.json()["groups"]["neighborhoods"]
        assert any("Bedford" in n["name"] for n in nbhds)

    def test_zip_search_skips_properties_bucket(self):
        resp = client.get("/api/search/?q=11216")
        props = resp.json()["groups"]["properties"]
        assert props == [], "ZIP query should not populate the properties bucket"


@pytest.mark.integration
class TestSearchOperators:
    """Operator bucket — real DB entries only."""

    def test_mtek_returns_operator(self):
        resp = client.get("/api/search/?q=MTEK")
        assert resp.status_code == 200
        ops = resp.json()["groups"]["operators"]
        assert len(ops) >= 1
        slugs = [o["slug"] for o in ops]
        assert "mtek-nyc" in slugs

    def test_mtek_has_portfolio_size(self):
        resp = client.get("/api/search/?q=MTEK")
        ops = resp.json()["groups"]["operators"]
        mtek = next(o for o in ops if o["slug"] == "mtek-nyc")
        assert mtek["portfolio_size"] and mtek["portfolio_size"] > 0

    def test_phantom_returns_operator(self):
        resp = client.get("/api/search/?q=phantom")
        ops = resp.json()["groups"]["operators"]
        slugs = [o["slug"] for o in ops]
        assert "phantom-capital" in slugs

    def test_bredif_returns_operator(self):
        resp = client.get("/api/search/?q=bredif")
        ops = resp.json()["groups"]["operators"]
        slugs = [o["slug"] for o in ops]
        assert "bredif" in slugs

    def test_operator_href_points_to_operator_route(self):
        resp = client.get("/api/search/?q=MTEK")
        ops = resp.json()["groups"]["operators"]
        mtek = next(o for o in ops if o["slug"] == "mtek-nyc")
        assert mtek["href"] == "/operator/mtek-nyc"

    def test_operator_type_field(self):
        resp = client.get("/api/search/?q=MTEK")
        ops = resp.json()["groups"]["operators"]
        assert all(o["type"] == "operator" for o in ops)

    def test_mtek_not_in_properties_bucket(self):
        resp = client.get("/api/search/?q=MTEK")
        props = resp.json()["groups"]["properties"]
        for p in props:
            assert "MTEK" not in (p.get("address") or "").upper(), \
                "MTEK is an operator — should not appear as a property address"


@pytest.mark.integration
class TestSearchProperties:
    """Properties bucket — parcel address search only."""

    def test_address_search_sterling(self):
        resp = client.get("/api/search/?q=Sterling")
        assert resp.status_code == 200
        props = resp.json()["groups"]["properties"]
        assert len(props) >= 1
        assert all("Sterling" in p["address"] for p in props)

    def test_property_has_bbl_and_href(self):
        resp = client.get("/api/search/?q=Sterling")
        props = resp.json()["groups"]["properties"]
        assert props[0]["bbl"]
        assert props[0]["href"].startswith("/property/")

    def test_property_type_field(self):
        resp = client.get("/api/search/?q=Sterling")
        props = resp.json()["groups"]["properties"]
        assert all(p["type"] == "property" for p in props)


@pytest.mark.integration
class TestSearchEdgeCases:
    """Empty queries, short queries, unknown names, punctuation."""

    def test_empty_query_returns_400(self):
        resp = client.get("/api/search/?q=")
        assert resp.status_code == 400

    def test_short_query_returns_400(self):
        resp = client.get("/api/search/?q=ab")
        assert resp.status_code == 400
        assert "too short" in resp.json()["detail"].lower()

    def test_blackstone_returns_empty_not_500(self):
        resp = client.get("/api/search/?q=Blackstone")
        assert resp.status_code == 200
        groups = resp.json()["groups"]
        assert groups["operators"] == []

    def test_punctuation_does_not_crash(self):
        resp = client.get("/api/search/?q=O'Brien")
        assert resp.status_code in (200, 400)

    def test_special_chars_do_not_crash(self):
        resp = client.get("/api/search/?q=!!!")
        assert resp.status_code in (200, 400)

    def test_unknown_query_returns_empty_groups(self):
        resp = client.get("/api/search/?q=xyzzynotreal")
        assert resp.status_code == 200
        groups = resp.json()["groups"]
        assert groups["neighborhoods"] == []
        assert groups["properties"] == []
        assert groups["operators"] == []
