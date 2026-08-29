"""
One absolute number, because everything else on the site is a rank.

The composite score is percentile-normalised per signal against that night's
spread across 177 ZIPs, so the top 5% fill the top of the scale by construction.
Measured over 314 days of score_history the mean composite moved 1.1 points and
its standard deviation did not move at all, while actual executed evictions ran
between 1,246 and 1,842 a month, a 48% swing. The index absorbed all of it.

That is what percentile normalisation is for and it is not a defect. The defect
was that nothing anywhere on the site could answer "is this getting worse?",
because every number was a position rather than a level. Michael asked exactly
that question on 2026-08-28 and the honest answer was that the map cannot say.

This is the level: executed residential evictions per 1,000 apartments, citywide,
over complete months, with the trailing twelve compared against the twelve before
them.

**The rule this guards is the partial month.** Counting up to CURRENT_DATE puts a
half-finished month at the end of the series and against a full one a year
earlier, which renders as a collapse in evictions every single time the page is
built before the 28th. It is the same false-freshness shape as the ACRIS window
that read 1 deed instead of 558, and it would be worse here because a fabricated
40% drop in evictions is a claim a journalist might repeat.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


class TestTheWindowIsWholeMonths:
    def test_the_query_excludes_the_current_month(self):
        src = (REPO / "api" / "routes" / "frontend.py").read_text()
        m = re.search(r"def _citywide_eviction_trend.*?(?=\ndef |\n@router)", src, re.S)
        assert m, "could not find _citywide_eviction_trend; the grep has rotted"
        q = m.group(0)
        assert "date_trunc('month', CURRENT_DATE)" in q or 'date_trunc("month", CURRENT_DATE)' in q, (
            "the window must be anchored on the first of the current month so the "
            "series ends with the last COMPLETE month"
        )
        assert re.search(r"<\s+date_trunc\('month', CURRENT_DATE\)", q), (
            "the upper bound must EXCLUDE the current month. Including a partial "
            "month renders a fabricated collapse in evictions on any day before "
            "month end, and compares it against a full month a year earlier."
        )

    def test_both_halves_of_the_comparison_are_twelve_months(self):
        src = (REPO / "api" / "routes" / "frontend.py").read_text()
        q = re.search(r"def _citywide_eviction_trend.*?(?=\ndef |\n@router)", src, re.S).group(0)
        assert q.count("INTERVAL '12 months'") >= 2 and "INTERVAL '24 months'" in q, (
            "year over year has to compare twelve complete months against the "
            "twelve before them, not against whatever remains"
        )

    def test_residential_only(self):
        src = (REPO / "api" / "routes" / "frontend.py").read_text()
        q = re.search(r"def _citywide_eviction_trend.*?(?=\ndef |\n@router)", src, re.S).group(0)
        assert "Residential" in q, (
            "commercial evictions are in the same table and are not displacement"
        )


@pytest.mark.needs_data
class TestItRendersAnAnswer:
    """
    Rendered, not grepped. The first version of these asserted on the source and
    passed on the helper's own docstring, which is the same way the /property
    apartment-number test passed on an unused import.
    """

    def _page(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app).get("/displacement").text

    def test_the_page_answers_the_question(self):
        page = self._page()
        assert "getting worse" in page.lower(), (
            "the section should be headed with the question a reader actually has"
        )

    def test_it_prints_both_years_and_a_direction(self):
        """A single count is not an answer; the comparison is the answer."""
        page = re.sub(r"<[^>]+>", " ", self._page())
        assert re.search(r"\b1[0-9],[0-9]{3}\b", page), (
            "the trailing-twelve eviction count is missing from the page"
        )
        assert re.search(r"(up|down)\s+[0-9.]+%", page, re.I), (
            "the page has to say which direction, not only the two numbers"
        )

    def test_the_rank_versus_level_distinction_is_stated(self):
        """
        Scoped to the block. "rank" appears elsewhere on /displacement, so a
        whole-page search would pass with the sentence deleted.
        """
        page = re.sub(r"<[^>]+>", " ", self._page()).lower()
        i = page.find("is it getting worse")
        assert i >= 0, "the block is missing"
        page = page[i:i + 900]
        assert "rank" in page, (
            "the block has to say that the scores rank and this measures, or it "
            "reads as one more number and the confusion it exists to fix survives"
        )

    def test_no_partial_month_in_the_series(self):
        """
        The rendered series must not end on the current month. This is the one
        that would catch the regression the source grep only describes.
        """
        from datetime import date
        page = self._page()
        this_month = date.today().strftime("%b %Y")
        assert this_month not in page, (
            f"{this_month} is still running and its partial count would render "
            f"as a collapse against a full month a year earlier"
        )
