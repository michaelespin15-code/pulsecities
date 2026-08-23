"""
Guard: the property SSR page carries a working watch-this-building CTA.

/property takes roughly 88% of organic landings (measured 2026-08-23), which
makes this the site's main conversion point. Before the card existed the page
offered a reader no way to come back, and the subscriber table went seven weeks
without a signup while search traffic rose sevenfold. The alert side has run
since July; only the form was missing, so what these tests protect is the
wiring: the card posts to /api/subscribe with the page's own BBL, and the
condo unit lots resolve too.
"""

import warnings

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.main import app
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)


@pytest.fixture(scope="module")
def scored_bbl():
    db = SessionLocal()
    try:
        row = db.execute(text("""
            SELECT p.bbl FROM parcels p
            JOIN ownership_raw o ON o.bbl = p.bbl
            WHERE p.address IS NOT NULL LIMIT 1
        """)).fetchone()
    finally:
        db.close()
    if not row:
        pytest.skip("no parcel with a deed in the database")
    return row.bbl


@pytest.fixture(scope="module")
def condo_bbl():
    db = SessionLocal()
    try:
        row = db.execute(text(
            "SELECT bbl FROM condo_unit_addresses WHERE address IS NOT NULL LIMIT 1"
        )).fetchone()
    finally:
        db.close()
    if not row:
        pytest.skip("no condo unit lots in the database")
    return row.bbl


@pytest.mark.integration
def test_watch_card_present_and_wired(scored_bbl):
    body = client.get(f"/property/{scored_bbl}").text
    assert 'class="watch-card"' in body
    assert 'id="pw-btn"' in body and 'id="pw-email"' in body
    assert "/api/subscribe" in body
    assert f'"{scored_bbl}"' in body, "the page BBL must be baked into the subscribe payload"
    assert "Building Watch Submit" in body, "conversion event missing"


@pytest.mark.integration
def test_watch_card_on_condo_unit_lot(condo_bbl):
    """Unit lots have no parcels row. They still render a page, so they must
    still render a card, and the endpoint must resolve them (see
    test_subscribe_building)."""
    body = client.get(f"/property/{condo_bbl}").text
    assert 'class="watch-card"' in body
    assert f'"{condo_bbl}"' in body


@pytest.mark.integration
def test_watch_card_avoids_stale_green(scored_bbl):
    # Same palette rule the neighborhood card is held to: no retired brights.
    body = client.get(f"/property/{scored_bbl}").text
    for stale in ("#4ade80", "#22c55e", "#16a34a"):
        assert stale not in body, f"stale green {stale} rendered on the page"


@pytest.mark.integration
def test_watch_card_copy_rules(scored_bbl):
    """House copy rules: no em dash as a connector, no trailing period on the
    subtitle line."""
    import re
    body = client.get(f"/property/{scored_bbl}").text
    card = re.search(r'<section class="watch-card">.*?</section>', body, re.S)
    assert card, "watch card not rendered"
    card = card.group(0)
    assert "—" not in card, "em dash in watch-card copy"
    sub = re.search(r'<p class="section-sub">(.*?)</p>', card, re.S)
    assert sub and not sub.group(1).strip().endswith("."), \
        "subtitle lines take no trailing period"
