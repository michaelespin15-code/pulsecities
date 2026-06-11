"""
Institutional classification gate — public-surface integration test.

Source of truth: operators.operator_class. Only rows with operator_class == 'operator'
may appear on a public surface that presents a cluster *as a trackable operator*.
Every other class (financial_institution, government, nonprofit_hdfc, unclassified)
must be invisible across all of:

    GET /api/operators
    GET /api/operators/top
    GET /api/stats
    GET /api/search/?q=
    GET /api/search/landlord?q=
    GET /sitemap.xml
    GET /operator/{slug}   (OG/SSR meta route)

Regression guard for the RIDGEWOOD-class leak: a financial_institution that was
screened out of /api/operators and /operators (DB-gated on operator_class) but
still surfaced on /api/operators/top and grouped search, which gated on the
hardcoded OPERATOR_NOISE_ROOTS list — an incomplete subset of the suppressed set.

The gate is driven entirely off the live DB, so a newly misclassified cluster is
covered the moment it lands in the operators table. No hardcoded slug list to drift.

Note on /api/search/landlord: it is a raw deed-record search (party name -> deeds),
not an operator surface. A suppressed bank's deeds may legitimately appear there;
what must NOT appear is an operator identity (a slug / /operator/ profile link).
The test enforces the latter without breaking the raw-record investigative feature.
"""

import json
from pathlib import Path

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
    # This test fans out one request per suppressed cluster across search and
    # landlord search. Left rate-limited, those calls would exhaust the shared
    # per-IP window and bleed 429s into other test modules. Disable the route
    # limiters for the duration of this module, then restore them.
    from api.main import app
    import api.routes.operators as operators_mod
    import api.routes.search as search_mod
    import api.routes.stats as stats_mod

    limiters = [operators_mod.limiter, search_mod.limiter, stats_mod.limiter]
    previous = [lim.enabled for lim in limiters]
    for lim in limiters:
        lim.enabled = False
    try:
        yield TestClient(app)
    finally:
        for lim, prev in zip(limiters, previous):
            lim.enabled = prev


@pytest.fixture(scope="module")
def suppressed(db):
    """Every operator cluster that is NOT class 'operator' — the set that must be gated."""
    rows = db.execute(
        text(
            "SELECT operator_root, slug, display_name, operator_class FROM operators "
            "WHERE operator_class IS DISTINCT FROM 'operator' "
            "ORDER BY operator_root"
        )
    ).fetchall()
    return rows


@pytest.fixture(scope="module")
def confirmed(db):
    """Class 'operator' rows — the only clusters allowed on public operator surfaces."""
    return db.execute(
        text(
            "SELECT operator_root, slug FROM operators "
            "WHERE operator_class = 'operator' ORDER BY operator_root"
        )
    ).fetchall()


@pytest.mark.integration
class TestInstitutionalGate:
    """Suppressed clusters must not surface as operators on any public endpoint."""

    def test_gate_has_subjects(self, suppressed):
        """Sanity: there is something to gate, so the assertions below aren't vacuous."""
        assert suppressed, (
            "No non-operator clusters found in the operators table; gate test would "
            "pass vacuously. Seed at least one institutional cluster."
        )

    def test_confirmed_operators_still_visible(self, client, confirmed):
        """Positive control: the gate hides institutions without nuking real operators."""
        assert confirmed, "Expected at least one class 'operator' row"
        listed = {r["slug"] for r in client.get("/api/operators").json()}
        for row in confirmed:
            assert row.slug in listed, (
                f"Confirmed operator {row.slug} missing from /api/operators — gate is over-filtering"
            )

    def test_api_operators_excludes_suppressed(self, client, suppressed):
        data = client.get("/api/operators").json()
        roots = {r["operator_root"] for r in data}
        slugs = {r["slug"] for r in data}
        leaked = [
            s.operator_root for s in suppressed
            if s.operator_root in roots or s.slug in slugs
        ]
        assert not leaked, f"/api/operators leaked suppressed clusters: {leaked}"

    def test_api_operators_top_excludes_suppressed(self, client, suppressed):
        # limit is capped at 10 server-side; ask for the max to widen the net.
        data = client.get("/api/operators/top?limit=10").json()
        roots = {r["operator_root"] for r in data}
        leaked = [s.operator_root for s in suppressed if s.operator_root in roots]
        assert not leaked, f"/api/operators/top leaked suppressed clusters: {leaked}"

    def test_api_search_operators_group_excludes_suppressed(self, client, suppressed):
        leaked = []
        for s in suppressed:
            resp = client.get("/api/search/", params={"q": s.operator_root})
            if resp.status_code != 200:
                continue
            ops = resp.json().get("groups", {}).get("operators", [])
            for entry in ops:
                if entry.get("slug") == s.slug or entry.get("href") == f"/operator/{s.slug}":
                    leaked.append(s.operator_root)
                    break
        assert not leaked, f"/api/search operators group leaked suppressed clusters: {leaked}"

    def test_api_search_landlord_emits_no_operator_identity(self, client, suppressed):
        """Landlord search may return a bank's raw deeds, but never an operator link."""
        leaked = []
        for s in suppressed:
            resp = client.get("/api/search/landlord", params={"q": s.operator_root})
            if resp.status_code != 200:
                continue
            body = resp.text
            if "/operator/" in body:
                leaked.append((s.operator_root, "profile link in body"))
                continue
            for row in resp.json().get("results", []):
                if {"slug", "operator_root", "href"} & set(row.keys()):
                    leaked.append((s.operator_root, "operator identity field in result"))
                    break
        assert not leaked, f"/api/search/landlord exposed operator identity: {leaked}"

    def test_api_stats_emits_no_operator_identity(self, client):
        """Stats is a ZIP/signal surface — it must not carry any operator profile link."""
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        body = resp.text
        assert "/operator/" not in body, "/api/stats leaked an operator profile link"
        # Defensive: no operator-identity field names in the payload.
        payload = json.dumps(resp.json())
        assert '"slug"' not in payload and '"operator_root"' not in payload, (
            "/api/stats payload carries operator-identity fields"
        )

    def test_sitemap_excludes_suppressed(self, suppressed):
        # sitemap.xml is a static file served by nginx (no FastAPI route), so it
        # is checked on disk rather than through the test client.
        sitemap = Path(__file__).parent.parent / "frontend" / "sitemap.xml"
        body = sitemap.read_text()
        leaked = [s.operator_root for s in suppressed if f"/operator/{s.slug}" in body]
        assert not leaked, f"sitemap.xml lists suppressed operator profiles: {leaked}"

    def test_og_operator_page_is_not_a_full_profile(self, client, suppressed):
        """Direct hit on /operator/{slug} must 404 (noise) or render the minimal
        'not an operator' page — never a full profile with portfolio meta."""
        leaked = []
        for s in suppressed:
            resp = client.get(f"/operator/{s.slug}")
            if resp.status_code == 404:
                continue
            if resp.status_code == 200 and "Not an operator profile" in resp.text:
                continue
            leaked.append((s.operator_root, resp.status_code))
        assert not leaked, (
            f"/operator/{{slug}} rendered a full profile for suppressed clusters: {leaked}"
        )
