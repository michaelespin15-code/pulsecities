"""
SSR top-nav consistency guard.

Every SSR page used to hand-roll its own <nav>; the link sets disagreed and
none but the homepage surfaced /displacement or /this-week. They now share
_ssr_nav(). This test fails the suite if a page's top nav drops a hub link or
if the shared helper stops emitting the canonical set, so the next divergence
fails here instead of shipping.
"""

import re
import warnings

import pytest
from sqlalchemy import text
from fastapi.testclient import TestClient

from api.main import app
from api.routes import frontend
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)

# Every SSR top nav must link the full hub set.
HUB = {"/map", "/displacement", "/neighborhoods", "/operators",
       "/flips", "/radar", "/this-week", "/methodology"}


def _top_nav_paths(html: str) -> set:
    """Internal hrefs inside the first <nav> of a page."""
    m = re.search(r"<nav\b.*?</nav>", html, re.S)
    if not m:
        return set()
    return {h for h in re.findall(r'href="([^"]+)"', m.group(0))
            if h.startswith("/")}


# --- unit: the helper itself (no DB) --------------------------------------

def test_helper_emits_full_hub_set():
    paths = _top_nav_paths(frontend._ssr_nav())
    missing = HUB - paths
    assert not missing, f"_ssr_nav missing hub links {sorted(missing)}"


def test_helper_marks_active_link():
    html = frontend._ssr_nav("/flips")
    assert 'href="/flips" aria-current="page"' in html
    # non-active links keep the dim colour + hover handlers
    assert 'href="/radar"' in html and "onmouseover" in html


def test_helper_spanish_labels():
    html = frontend._ssr_nav("/neighborhoods", lang="es")
    assert ">Desplazamiento<" in html
    assert ">Vecindarios<" in html
    assert ">Esta semana<" in html
    # paths stay canonical (English) regardless of label language
    assert HUB - _top_nav_paths(html) == set()


def test_helper_appends_toggle_and_track():
    html = frontend._ssr_nav("/operators", toggle_html=frontend._LANG_TOGGLE_BTN)
    assert 'id="lang-toggle"' in html
    tracked = frontend._ssr_nav("/displacement", track=True)
    assert "plausible('Showcase Nav'" in tracked
    assert "{props:{to:'operators'}}" in tracked
    # the active page's own link is not a tracked outbound click
    assert "to:'displacement'" not in tracked


# --- integration: every rendered SSR page ---------------------------------

STATIC_HUB_ROUTES = [
    "/operators", "/neighborhoods", "/borough/brooklyn", "/flips",
    "/flips/editions", "/radar", "/this-week", "/displacement",
    "/neighborhood/11216",
]


@pytest.mark.integration
@pytest.mark.parametrize("route", STATIC_HUB_ROUTES)
def test_ssr_route_top_nav(route):
    resp = client.get(route)
    assert resp.status_code == 200, f"{route} returned {resp.status_code}"
    paths = _top_nav_paths(resp.text)
    missing = HUB - paths
    assert not missing, f"{route} top nav missing {sorted(missing)} (has {sorted(paths)})"


@pytest.mark.integration
def test_property_page_top_nav():
    db = SessionLocal()
    try:
        r = db.execute(text(
            "SELECT o.bbl FROM ownership_raw o JOIN parcels p ON p.bbl = o.bbl "
            "WHERE p.address IS NOT NULL LIMIT 1"
        )).first()
    finally:
        db.close()
    if not r:
        pytest.skip("no property with records in current data")
    resp = client.get(f"/property/{r.bbl}")
    assert resp.status_code == 200
    missing = HUB - _top_nav_paths(resp.text)
    assert not missing, f"/property top nav missing {sorted(missing)}"


@pytest.mark.integration
def test_week_edition_top_nav():
    archive = client.get("/this-week/archive").text
    slug = re.search(r'href="(/week/[^"]+)"', archive)
    if not slug:
        pytest.skip("no completed week editions in current data")
    resp = client.get(slug.group(1))
    assert resp.status_code == 200
    missing = HUB - _top_nav_paths(resp.text)
    assert not missing, f"{slug.group(1)} top nav missing {sorted(missing)}"


@pytest.mark.integration
def test_es_pages_keep_hub_and_toggle():
    # Bilingual pages must keep the full hub set and the EN/ES toggle at ?lang=es.
    for route in ("/neighborhoods?lang=es", "/borough/brooklyn?lang=es",
                  "/neighborhood/11216?lang=es"):
        resp = client.get(route)
        assert resp.status_code == 200, f"{route} -> {resp.status_code}"
        nav = re.search(r"<nav\b.*?</nav>", resp.text, re.S).group(0)
        assert 'id="lang-toggle"' in nav, f"{route} lost the language toggle"
        missing = HUB - _top_nav_paths(resp.text)
        assert not missing, f"{route} top nav missing {sorted(missing)}"
