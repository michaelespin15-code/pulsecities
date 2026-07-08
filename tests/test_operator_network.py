"""
/api/operators/{slug}/network — ego graph of operator -> LLC -> ZIP.

DB-driven like the classification gate tests: real operators must return a
well-formed graph, gated clusters must 404 so no signal data leaks through
this surface either.
"""

import pytest
from fastapi.testclient import TestClient

from models.database import SessionLocal
from sqlalchemy import text


@pytest.fixture(scope="module")
def client():
    from api.main import app
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture(scope="module")
def operator_slug():
    db = SessionLocal()
    try:
        row = db.execute(text(
            "SELECT o.slug FROM operators o "
            "JOIN operator_parcels op ON op.operator_id = o.id "
            "WHERE o.operator_class = 'operator' "
            "GROUP BY o.slug ORDER BY COUNT(*) DESC LIMIT 1"
        )).fetchone()
    finally:
        db.close()
    if not row:
        pytest.skip("no classified operator with parcels in the database")
    return row.slug


@pytest.fixture(scope="module")
def gated_slugs():
    db = SessionLocal()
    try:
        rows = db.execute(text(
            "SELECT slug FROM operators "
            "WHERE operator_class IS DISTINCT FROM 'operator' AND slug IS NOT NULL"
        )).fetchall()
    finally:
        db.close()
    return [r.slug for r in rows]


@pytest.mark.integration
class TestNetworkGraph:

    def test_returns_200_with_graph(self, client, operator_slug):
        resp = client.get(f"/api/operators/{operator_slug}/network")
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data and "edges" in data

    def test_exactly_one_operator_node(self, client, operator_slug):
        data = client.get(f"/api/operators/{operator_slug}/network").json()
        op_nodes = [n for n in data["nodes"] if n["type"] == "operator"]
        assert len(op_nodes) == 1

    def test_has_llc_and_zip_nodes(self, client, operator_slug):
        data = client.get(f"/api/operators/{operator_slug}/network").json()
        types = {n["type"] for n in data["nodes"]}
        assert "llc" in types
        assert "zip" in types

    def test_edges_reference_existing_nodes(self, client, operator_slug):
        data = client.get(f"/api/operators/{operator_slug}/network").json()
        ids = {n["id"] for n in data["nodes"]}
        for e in data["edges"]:
            assert e["source"] in ids, f"dangling edge source {e['source']}"
            assert e["target"] in ids, f"dangling edge target {e['target']}"

    def test_zip_nodes_carry_score_and_color(self, client, operator_slug):
        data = client.get(f"/api/operators/{operator_slug}/network").json()
        zips = [n for n in data["nodes"] if n["type"] == "zip"]
        assert zips
        for z in zips:
            assert "color" in z
            assert "score" in z  # may be None for unscored ZIPs

    def test_llc_nodes_carry_parcel_counts(self, client, operator_slug):
        data = client.get(f"/api/operators/{operator_slug}/network").json()
        llcs = [n for n in data["nodes"] if n["type"] == "llc"]
        assert llcs
        assert all(n.get("parcels", 0) > 0 for n in llcs)


@pytest.mark.integration
class TestNetworkGate:

    def test_gated_clusters_404(self, client, gated_slugs):
        for slug in gated_slugs:
            resp = client.get(f"/api/operators/{slug}/network")
            assert resp.status_code == 404, f"gate leak: /network served {slug}"

    def test_unknown_slug_404(self, client):
        assert client.get("/api/operators/no-such-operator/network").status_code == 404

    def test_invalid_slug_400(self, client):
        assert client.get("/api/operators/NOT%20A%20SLUG/network").status_code == 400
