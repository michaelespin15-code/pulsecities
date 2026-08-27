"""
Guards the source-freshness line on the deed-backed pages.

/flips and /radar are built entirely on ACRIS. Their windows are anchored to
CURRENT_DATE while the deeds in them stop wherever the city last published, and
on 2026-08-18 that gap was eighteen days: /radar told readers "8 buying runs
detected across NYC in the past 90 days" over a window whose last eighteen days
contained no deeds at all, with nothing on the page saying so. Someone checking
us against a deed recorded the week before would have found us silently wrong,
on a site whose whole pitch is that the numbers are checkable.

This is the same rule as the homepage LLC chip and the /evictions through-line:
never let a page imply coverage the query does not have. The failure it catches
is a page losing the disclosure while keeping the window claim, which is
invisible in normal operation because the two only diverge when the upstream
feed stalls -- exactly when nobody is looking at that page's copy.
"""

import re

import pytest
from fastapi.testclient import TestClient

# Pages whose numbers come from ACRIS, with the id of their disclosure element.
DEED_BACKED = [("/flips", "fw-through"), ("/radar", "sr-through")]

WINDOW_CLAIM = re.compile(r"in the past \d+ (days|months)", re.I)
# The page writes the date through _long_date: full month, day, year. This
# pattern asked for an abbreviated month and no year, so it matched nothing the
# renderer has ever produced and every assertion built on it was red from the
# commit that added it.
THROUGH_LINE = re.compile(r"Deeds recorded through [A-Z][a-z]+ \d{1,2}, \d{4}, "
                          r"the most recent day the city has published\.?")


def rendered_through(html: str, element_id: str) -> str:
    """Text inside the disclosure element as served, ignoring the i18n table.

    Matching the sentence anywhere in the document is not enough: the same
    string also sits in the JS i18n object, so a page that lost the visible
    element entirely still looked compliant. Found by deleting the element and
    watching this file stay green.
    """
    m = re.search(rf'id="{element_id}"[^>]*>([^<]*)<', html)
    return m.group(1).strip() if m else ""


@pytest.fixture(scope="module")
def client():
    from api.main import app
    return TestClient(app)


@pytest.mark.integration
@pytest.mark.parametrize("route,element_id", DEED_BACKED)
def test_page_discloses_where_the_deed_record_ends(client, route, element_id):
    text = rendered_through(client.get(route).text, element_id)
    assert THROUGH_LINE.fullmatch(text), (
        f"{route} does not render a through-line in #{element_id} (got {text!r}). "
        f"It reports a window anchored to today over data that stops whenever "
        f"the city last published; without this the gap is invisible."
    )


@pytest.mark.integration
@pytest.mark.parametrize("route,element_id", DEED_BACKED)
def test_window_claim_is_accompanied_by_the_through_line(client, route, element_id):
    """A window claim without a through-line is the exact regression."""
    html = client.get(route).text
    if WINDOW_CLAIM.search(html):
        assert THROUGH_LINE.fullmatch(rendered_through(html, element_id)), (
            f"{route} claims a window ending today with no statement of where "
            f"the data actually ends"
        )


@pytest.mark.integration
@pytest.mark.parametrize("route,element_id", DEED_BACKED)
def test_through_line_is_translated(client, route, element_id):
    """The Spanish reader gets the caveat too, not just the English one.

    Both pages swap copy client-side from a JS i18n table, so a disclosure
    added only to the served HTML would vanish the moment someone switched
    language -- which is worse than never having added it.
    """
    html = client.get(route).text
    assert re.search(r"Escrituras registradas hasta el \d{1,2} de \w+", html), (
        f"{route} has no Spanish through-line in its i18n table; switching to "
        f"Spanish would drop the caveat"
    )


@pytest.mark.integration
def test_through_line_reports_the_real_date(client):
    """The date shown must be the one the freshness module reports, not today."""
    from datetime import date

    from api.freshness import ACRIS_THROUGH_SQL
    from models.database import SessionLocal

    db = SessionLocal()
    try:
        actual = db.execute(__import__("sqlalchemy").text(ACRIS_THROUGH_SQL)).scalar()
    finally:
        db.close()
    if actual is None:
        pytest.skip("no ACRIS data to compare against")

    # Format the date with the renderer's own helper rather than a second copy
    # of the month table. The copy here spelled months in three letters while
    # the page spelled them in full, so this never compared what it claimed to.
    from api.routes.frontend import _en_date

    expected = f"Deeds recorded through {_en_date(actual)},"
    html = client.get("/radar").text
    assert expected in html, (
        f"/radar should name {expected!r} (from api/freshness.py) but does not; "
        f"a hand-rolled date here would drift from /api/status and /evictions"
    )
    assert isinstance(actual, date)
