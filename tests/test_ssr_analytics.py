"""
Guard: every server-rendered page loads Plausible exactly once.

The SSR pages build their own <head> and were historically untracked. This test
fails if a page ships without analytics, or double-injects the script.
"""

import warnings

from fastapi.testclient import TestClient

from api.main import app

warnings.filterwarnings("ignore")
client = TestClient(app)

# One representative URL per SSR page builder. Parameterized routes use a ZIP /
# operator / week slug known to exist in the production dataset this box serves.
SSR_PAGES = [
    "/displacement",
    "/flips",
    "/flips/editions",
    "/radar",
    "/operators",
    "/neighborhoods",
    "/this-week",
    "/this-week/archive",
    "/week/2026-W27",
    "/borough/brooklyn",
    "/neighborhood/11216",
    "/property/1016880011",
    "/operator/mtek-nyc",
]


def test_every_ssr_page_loads_plausible_once():
    misses = []
    for path in SSR_PAGES:
        r = client.get(path)
        if r.status_code != 200:
            continue  # data-dependent pages (skip if the fixture row is gone)
        n = r.text.count("plausible.io/js")
        if n != 1:
            misses.append(f"{path}: {n} plausible tags")
    assert not misses, "SSR pages with wrong analytics coverage: " + "; ".join(misses)
