"""
Guards for the /displacement findings showcase.

The invariant that matters most: named eviction-to-flip arcs on this public page
come only from APPROVED editions. The raw weekly scan stays behind a human
review gate, and this page must not bypass it.
"""

import json

from fastapi.testclient import TestClient

from api.main import app
from api.routes.frontend import _approved_flip_arcs, _FRONTEND

client = TestClient(app)


def test_displacement_renders_all_sections():
    r = client.get("/displacement")
    assert r.status_code == 200
    body = r.text
    for needle in (
        "State of NYC Displacement",
        "Evicted, then flipped",
        "Highest pressure",
        "largest landlords",
        "Buying clusters",
        'rel="canonical" href="https://pulsecities.com/displacement"',
        "application/ld+json",
    ):
        assert needle in body, f"missing: {needle}"


def test_only_approved_arcs_are_published():
    """_approved_flip_arcs must return exactly the arcs from approved editions."""
    path = _FRONTEND.parent / "scripts" / "eviction_flips_editions.json"
    editions = json.loads(path.read_text()).get("editions", [])

    approved_keys = {
        a.get("key") or a.get("bbl")
        for ed in editions if ed.get("approved")
        for a in ed.get("arcs", [])
    }
    pending_keys = {
        a.get("key") or a.get("bbl")
        for ed in editions if not ed.get("approved")
        for a in ed.get("arcs", [])
    } - approved_keys  # a key approved in one edition is fine

    returned = {a.get("key") or a.get("bbl") for a in _approved_flip_arcs()}

    assert returned <= approved_keys, "published an arc that was never approved"
    assert not (returned & pending_keys), "a pending-only arc leaked onto the page"


def test_pending_arc_address_not_on_page():
    """No address that exists only in an unapproved edition may appear on the page."""
    path = _FRONTEND.parent / "scripts" / "eviction_flips_editions.json"
    editions = json.loads(path.read_text()).get("editions", [])

    approved_addrs = {
        (a.get("address") or "").upper()
        for ed in editions if ed.get("approved")
        for a in ed.get("arcs", [])
    }
    pending_only = {
        (a.get("address") or "").upper()
        for ed in editions if not ed.get("approved")
        for a in ed.get("arcs", [])
    } - approved_addrs

    body = client.get("/displacement").text.upper()
    for addr in pending_only:
        if addr:
            assert addr not in body, f"pending-only address leaked: {addr}"
