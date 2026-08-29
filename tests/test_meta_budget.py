"""
Every template's title and description, not just /property's.

tests/test_title_budget.py fixed /property after a measurement found 100% of
97,790 titles over the budget. It reads the /property template and nothing
else, so /evictions, /flips, /radar and /neighborhood drifted past 60
characters unnoticed, and /neighborhood shipped a 209-character description on
177 pages. Bing's URL inspector flagged one of them on 2026-08-29; the other
182 were the same defect.

Why it matters more here than on most sites: the 2026-08-27 search read has
roughly 1,700 impressions sitting at positions 5 to 9 converting at zero. At
those positions the title and description are the entire product, because they
are all a searcher sees before deciding. A description truncated at 160 loses
its last quarter on the highest-intent pages the site has.

This renders each template and measures what a reader would see. Reading the
source instead would pass on a template whose interpolations happen to be long,
which is exactly how the property tail hid: the literal was short and the
rendered result never was.
"""
import html
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

TITLE_MAX = 60    # Google renders ~580px, about 60 characters, and drops the rest
DESC_MIN = 110    # shorter wastes the space the snippet is given
DESC_MAX = 160

# One representative URL per server-rendered template.
SSR = [
    "/this-week", "/this-week/archive", "/week/2026-W34", "/displacement",
    "/evictions", "/llc", "/neighborhoods", "/neighborhood/10457",
    "/borough/bronx", "/flips", "/radar", "/operators", "/who-owns-my-building",
]
# Served by nginx from disk, so the app cannot render them.
STATIC = ["index.html", "press.html", "developers.html", "privacy.html",
          "status.html", "methodology.html", "about.html"]

_TITLE = re.compile(r"<title>(.*?)</title>", re.S)
_DESC = re.compile(r'<meta name="description" content="(.*?)"', re.S)


def _lengths(markup: str) -> tuple[int, int]:
    t = _TITLE.search(markup)
    d = _DESC.search(markup)
    return (len(html.unescape(t.group(1)).strip()) if t else 0,
            len(html.unescape(d.group(1)).strip()) if d else 0)


@pytest.fixture(scope="module")
def measured():
    from fastapi.testclient import TestClient
    from api.main import app

    client = TestClient(app)
    out = {}
    for path in SSR:
        r = client.get(path)
        assert r.status_code == 200, f"{path} returned {r.status_code}"
        out[path] = _lengths(r.text)
    for name in STATIC:
        out["/" + name] = _lengths((REPO / "frontend" / name).read_text())
    return out


@pytest.mark.integration
@pytest.mark.needs_data
class TestEveryTemplateFitsTheSerp:
    def test_no_title_is_truncated(self, measured):
        over = {p: t for p, (t, _) in measured.items() if t > TITLE_MAX}
        assert not over, (
            f"titles over {TITLE_MAX} characters, so the tail is dropped:\n  "
            + "\n  ".join(f"{p}  {n}" for p, n in sorted(over.items())))

    def test_every_page_has_a_description(self, measured):
        missing = [p for p, (_, d) in measured.items() if d == 0]
        assert not missing, f"no meta description, so the engine writes one: {missing}"

    def test_no_description_is_truncated_or_wasteful(self, measured):
        bad = {p: d for p, (_, d) in measured.items()
               if d and not (DESC_MIN <= d <= DESC_MAX)}
        assert not bad, (
            f"descriptions outside {DESC_MIN}-{DESC_MAX}; over the top is "
            f"truncated, under the floor wastes the snippet:\n  "
            + "\n  ".join(f"{p}  {n}" for p, n in sorted(bad.items())))

    def test_the_measurement_covers_the_high_volume_templates(self):
        """A guard that checks only cheap pages passes forever. These three are
        177, 97,790 and 5 pages; /property has its own file."""
        for path in ("/neighborhood/10457", "/borough/bronx", "/evictions"):
            assert path in SSR, f"{path} dropped out of the sweep"
