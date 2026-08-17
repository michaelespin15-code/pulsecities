"""
Typography baseline guard.

Every page on this site carries its own inline <style> block. That was a
deliberate perf call (one request, nothing render-blocking), but it means a
base typographic rule has to be written 26 times, and the nav CSS already
proved what happens next: the copies drift, and nobody notices until a page
looks wrong next to its neighbours.

Two rules are held here.

`text-wrap: balance` on headings. Without it the browser breaks greedily and
leaves widows: the /press h1 wrapped to a 334px line over a 41px orphan, and
the borough h1 to 324px over 33px. Balance is not free in both directions --
the /displacement h1 trades a slightly evener pair for a break that keeps
"NYC displacement" whole -- but across every heading that wraps at 390, 768
and 1200 it fixes five and costs one.

`font-variant-numeric: tabular-nums` on the two number columns that are not
set in JetBrains Mono. This one is narrower than it looks. Mono is already
fixed-pitch, so the property is a no-op there, and DM Sans as Google serves
it ships no `tnum` table at all, so the property is dead CSS on anything set
in it. Bricolage Grotesque is the one face on the site that is both
proportional and carries the feature, and /displacement is the one page that
sets numbers in it: a "1" is drawn at half the width of a "0", which spread a
column of six gains across 21px of ragged edge until the property went on.
"""

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).parent.parent
FRONTEND = ROOT / "frontend"

# Whitespace-tolerant: frontend.py writes the rule minified, the static pages
# pretty-print it, and the f-string templates double the braces.
BALANCE = re.compile(r"h1\s*,\s*h2\s*,\s*h3\s*\{\{?\s*text-wrap\s*:\s*balance\s*;?\s*\}\}?")

# Every static page nginx serves off disk. 404.html and ops.html are included
# on purpose -- an internal page is still a page.
STATIC_PAGES = ["index.html", "about.html", "methodology.html", "press.html",
                "status.html", "operator.html", "developers.html", "app.html",
                "ops.html", "404.html"]

# One route per distinct <style> block in api/routes/frontend.py, plus the two
# shared CSS constants (_WEEK_CSS via /week/*, _LLC_PAGE_CSS via /llc*).
SSR_ROUTES = ["/neighborhood/11216", "/property/3013370001", "/operators",
              "/neighborhoods", "/borough/brooklyn", "/flips", "/flips/editions",
              "/radar", "/this-week", "/this-week/archive", "/displacement",
              "/evictions", "/who-owns-my-building", "/llc",
              "/is-my-building-rent-stabilized"]


@pytest.mark.parametrize("page", STATIC_PAGES)
def test_static_page_balances_headings(page):
    html = (FRONTEND / page).read_text()
    assert BALANCE.search(html), f"{page} is missing the h1,h2,h3 text-wrap:balance baseline"


@pytest.mark.integration
@pytest.mark.parametrize("route", SSR_ROUTES)
def test_ssr_route_balances_headings(route):
    from api.main import app
    client = TestClient(app)
    resp = client.get(route)
    assert resp.status_code == 200, f"{route} returned {resp.status_code}"
    assert BALANCE.search(resp.text), f"{route} is missing the h1,h2,h3 text-wrap:balance baseline"


@pytest.mark.integration
def test_operator_not_found_page_balances_headings():
    from api.main import app
    resp = TestClient(app).get("/operator/zzz-not-a-real-operator")
    assert resp.status_code == 404
    assert BALANCE.search(resp.text), "operator not-found page is missing the baseline"


@pytest.mark.integration
@pytest.mark.parametrize("cls", ["arc-gain", "row-val"])
def test_displacement_number_columns_are_tabular(cls):
    from api.main import app
    resp = TestClient(app).get("/displacement")
    rule = re.search(r"\." + cls + r"\{([^}]*)\}", resp.text)
    assert rule, f".{cls} rule not found on /displacement"
    assert "font-variant-numeric:tabular-nums" in rule.group(1).replace(" ", ""), (
        f".{cls} is a right-aligned number column in Bricolage Grotesque and "
        f"needs tabular figures: {rule.group(1)}"
    )
    assert "Bricolage Grotesque" in rule.group(1), (
        f".{cls} left Bricolage Grotesque; if it is mono now the tabular-nums "
        f"declaration is dead weight and this guard should go with it"
    )


def test_no_alarm_coloured_or_faded_focus_rings():
    """A focus ring says "you are here", not "this row is a problem".

    app.html painted its eviction rows with `focus:ring-red-400/50`: the alarm
    colour of the panel, at half opacity, on a plain `:focus` so a mouse click
    lit it up too. Sky is the site's focus colour (operator.html has outlined
    its network nodes in #6fb1d8 all along), and the alpha is the same
    de-emphasis reflex that test_text_contrast.py exists to stop.
    """
    offenders = []
    for page in FRONTEND.glob("*.html"):
        for m in re.finditer(r"(?:focus|focus-visible):ring-([a-z]+-\d+)(/\d+)?", page.read_text()):
            colour, alpha = m.group(1), m.group(2)
            if colour.startswith("red-") or colour.startswith("orange-"):
                offenders.append(f"{page.name}: ring-{colour} is an alarm colour")
            if alpha:
                offenders.append(f"{page.name}: ring-{colour}{alpha} is alpha-thinned")
    assert not offenders, "\n".join(offenders)
