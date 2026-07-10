"""
Footer consistency guard.

The footer markup is duplicated across static pages and several SSR
templates, and it drifted into six different variants before the 2026-07-10
audit unified them. Rather than trusting the copies to stay aligned, this
renders every HTML-serving surface and asserts the canonical link set is
present, so the next divergence fails the suite instead of shipping.
"""

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

FRONTEND = Path(__file__).parent.parent / "frontend"

# Every page footer must contain at least these paths.
CANON = {"/", "/methodology", "/about", "/press", "/status"}

# Static pages served straight off disk by nginx.
STATIC_PAGES = ["index.html", "about.html", "methodology.html", "press.html",
                "status.html", "operator.html"]

# SSR routes rendered by FastAPI. nginx maps /brooklyn -> /borough/brooklyn,
# so the app-level path is used here. (/map is the app shell, no footer.)
SSR_ROUTES = ["/neighborhoods", "/borough/brooklyn", "/this-week",
              "/neighborhood/11216", "/brief/operator/mtek-nyc"]


def _footer_paths(html: str) -> set:
    m = re.search(r"<footer.*?</footer>", html, re.S)
    if not m:
        return set()
    return {h for h in re.findall(r'href="([^"]+)"', m.group(0))
            if h.startswith("/")}


@pytest.mark.parametrize("page", STATIC_PAGES)
def test_static_page_footer(page):
    html = (FRONTEND / page).read_text()
    paths = _footer_paths(html)
    missing = CANON - paths
    assert not missing, f"{page} footer missing {sorted(missing)} (has {sorted(paths)})"


@pytest.mark.integration
@pytest.mark.parametrize("route", SSR_ROUTES)
def test_ssr_route_footer(route):
    from api.main import app
    client = TestClient(app)
    resp = client.get(route)
    assert resp.status_code == 200, f"{route} returned {resp.status_code}"
    paths = _footer_paths(resp.text)
    missing = CANON - paths
    assert not missing, f"{route} footer missing {sorted(missing)} (has {sorted(paths)})"
