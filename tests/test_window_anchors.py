"""
A window over a lagging feed must end where the feed ends.

ACRIS publishes on a lag and freezes for weeks. On 2026-08-28 its newest deed
was 28 days old, so every window running to CURRENT_DATE spent its tail on days
that could not contain a deed. The shortfall is not proportional to the lag, it
is proportional to the lag over the window:

    window     to CURRENT_DATE      to the last published deed
    30 days                  1 deed                    558 deeds
    90 days       4 radar clusters             11 radar clusters
    365 days            639 flips                     700 flips

The 30-day case is the one that mattered. It made `signal_counts.llc_acquisitions`
zero for every ZIP in /api/stats, which meant the dominant-signal label on the
site's central ranking could never say an LLC was buying, however many were.
After the fix Wakefield reads `dominant_signal: llc_acquisitions`.

A disclosure line was shipped for this class on 2026-08-18 and was only half the
fix: the page said which days it covered while the number underneath stayed
wrong. api/freshness.window_sql owns the rule now and this greps for the next
hand-rolled copy.

Only ACRIS gets this treatment. Evictions, violations and 311 were all within
two days of the calendar when this was written, and anchoring a current feed on
itself buys nothing while costing a query.
"""

import re
from pathlib import Path

import pytest

from api.freshness import window_sql

REPO = Path(__file__).resolve().parent.parent
SEARCH_DIRS = ("api", "scripts", "scoring", "scheduler")
ALLOWED = {"api/freshness.py", "tests/test_window_anchors.py"}

# A deed-date window measured from the calendar rather than from the record.
_BARE = re.compile(r"doc_date\s*>=?\s*CURRENT_DATE\s*-")


class TestNoBareDeedWindows:
    def test_nothing_windows_deeds_against_the_calendar(self):
        offenders = []
        for d in SEARCH_DIRS:
            for path in sorted((REPO / d).rglob("*.py")):
                rel = path.relative_to(REPO).as_posix()
                if rel in ALLOWED or "__pycache__" in rel:
                    continue
                for i, line in enumerate(path.read_text().splitlines()):
                    if _BARE.search(line):
                        offenders.append(f"{rel}:{i + 1}")
        assert not offenders, (
            "these window deeds from CURRENT_DATE, so the tail of the window "
            "holds no deeds while ACRIS lags. Use api.freshness.window_sql:\n  "
            + "\n  ".join(offenders)
        )

    def test_the_grep_would_still_catch_the_old_code(self):
        """A guard that matches nothing passes forever."""
        assert _BARE.search("AND o.doc_date >= CURRENT_DATE - INTERVAL '30 days'")
        assert _BARE.search("AND doc_date > CURRENT_DATE - make_interval(days => :w)")

    def test_it_does_not_fire_on_the_anchored_form(self):
        assert not _BARE.search(window_sql("o.doc_date", 30))


class TestWindowSql:
    def test_both_ends_are_bound_to_the_anchor(self):
        """Only closing the lower end would leave the window open into the
        future, which for a filer-typed doc_date is not hypothetical."""
        sql = window_sql("o.doc_date", 30)
        assert sql.count(":anchor") == 2
        assert "<=" in sql and ">" in sql

    def test_the_lower_bound_is_exclusive(self):
        """So two adjacent windows do not both claim the boundary day."""
        assert "doc_date > (" in window_sql("o.doc_date", 30)

    def test_the_column_is_left_bare_for_the_index(self):
        assert window_sql("o.doc_date", 30).startswith("o.doc_date")

    def test_the_param_can_be_renamed_for_a_query_with_two_feeds(self):
        assert ":deed_anchor" in window_sql("o.doc_date", 30, param="deed_anchor")

    @pytest.mark.parametrize("days", [30, 90, 180, 365])
    def test_the_window_length_reaches_the_sql(self, days):
        assert f"'{days} days'" in window_sql("o.doc_date", days)


class TestFeedAnchor:
    def test_it_falls_back_to_today_rather_than_failing(self):
        """A windowing helper must not be able to take a page down."""
        from datetime import date

        from api.freshness import feed_anchor

        class _Boom:
            def execute(self, *_a, **_k):
                raise RuntimeError("no database")

        assert feed_anchor(_Boom()) == date.today()

    def test_an_empty_feed_falls_back_too(self):
        from datetime import date

        from api.freshness import feed_anchor

        class _Empty:
            def execute(self, *_a, **_k):
                return self

            def scalar(self):
                return None

        assert feed_anchor(_Empty()) == date.today()
