"""
Every block of facts on /property names its source and the date that source was
last current.

The blocks already named their source. None of them said when it was current, so
a figure lifted off the page travelled as a bare claim with no way to date it.
That stopped being a cosmetic problem: over the fifteen days to 2026-08-27 the AI
crawlers fetched 82,631 pages here against Googlebot's 68,881, and a machine
reading a page cannot click through to check a number later. "42 violations"
repeated without a date outlives the record it came from. "42 open HPD
violations, current through August 25, 2026" does not.

The dates come from api.freshness, the same query /api/status publishes, so the
page and the status endpoint cannot drift apart and say different things about
the same feed.
"""

import re

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api import freshness
from api.main import app
from api.routes import frontend
from models.database import SessionLocal

CITE = re.compile(r"Source: ([^,<]+), current through ([A-Z][a-z]+ \d{1,2}, \d{4})\.")


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


@pytest.fixture(scope="module")
def busy_bbl():
    """A parcel carrying a deed, an eviction and violations, so every block
    renders. A quiet lot would pass these tests by having nothing to cite."""
    db = SessionLocal()
    try:
        row = db.execute(text("""
            SELECT p.bbl FROM parcels p
            WHERE p.address IS NOT NULL
              AND EXISTS (SELECT 1 FROM ownership_raw o
                          WHERE o.bbl = p.bbl AND o.doc_type = 'DEED')
              AND EXISTS (SELECT 1 FROM violations_raw v WHERE v.bbl = p.bbl)
            LIMIT 1
        """)).first()
    finally:
        db.close()
    if not row:
        pytest.skip("no parcel with both a deed and a violation")
    return row.bbl


class TestEveryFeedCanBeCited:
    def test_every_freshness_feed_has_a_display_name(self):
        """A feed added to api.freshness and not named here renders no citation
        at all, which fails silently and looks exactly like working code."""
        missing = [s for s, *_ in freshness.FRESHNESS_SOURCES
                   if s not in frontend._FEED_SOURCE]
        assert not missing, f"feeds with no source name to print: {missing}"

    def test_no_source_name_without_a_feed(self):
        """The other direction: a name kept after its feed was removed."""
        slugs = {s for s, *_ in freshness.FRESHNESS_SOURCES}
        orphans = [k for k in frontend._FEED_SOURCE if k not in slugs]
        assert not orphans, f"source names with no feed behind them: {orphans}"

    def test_cite_is_empty_rather_than_wrong_when_a_date_is_missing(self):
        """A feed the query could not date must print nothing. A citation with
        a blank or guessed date is worse than no citation."""
        assert frontend._cite({}, "acris") == ""
        assert frontend._cite({"acris": None}, "acris") == ""


class TestPropertyPageCitesItsSources:
    def test_property_page_carries_citations(self, client, busy_bbl):
        html = client.get(f"/property/{busy_bbl}").text
        found = CITE.findall(html)
        assert found, "/property renders no source-and-date citation at all"

    def test_the_cited_date_is_the_one_the_freshness_module_reports(
        self, client, busy_bbl
    ):
        """The number on the page and the number on /api/status come from one
        query or they will eventually disagree in public."""
        db = SessionLocal()
        try:
            through = db.execute(
                text(freshness.db_through_sql("acris"))
            ).scalar()
        finally:
            db.close()
        if through is None:
            pytest.skip("no ACRIS data to compare against")
        expected = frontend._en_date(
            through.date() if hasattr(through, "date") else through
        )
        html = client.get(f"/property/{busy_bbl}").text
        assert f"Source: NYC ACRIS, current through {expected}." in html, (
            f"/property does not cite the freshness module's ACRIS date "
            f"({expected}); a hand-rolled date here drifts from /api/status"
        )

    def test_citations_never_claim_a_future_date(self, client, busy_bbl):
        """The whole class of bug this sits next to. A feed cannot be current
        through a date that has not happened."""
        from datetime import date, datetime

        html = client.get(f"/property/{busy_bbl}").text
        for _src, shown in CITE.findall(html):
            when = datetime.strptime(shown, "%B %d, %Y").date()
            assert when <= date.today(), (
                f"page claims data current through {shown}, which is in the future"
            )
