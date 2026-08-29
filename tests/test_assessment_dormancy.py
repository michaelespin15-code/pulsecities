"""
The assessment signal is dormant for a real reason, and the log gave a false one.

Every night since it shipped, scoring has logged:

    Assessment spike signal is dormant: assessment_history has <2 tax years.

`assessment_history` has **seven** distinct tax years. The message is false, and
it cost two separate investigations before anyone read the table.

What is actually in there is two unrelated populations sharing one table:

    tax_year 2018-2023   197,730 rows from scripts/backfill_rs_history.py
                         stabilized_units set, assessed_total NULL for all of them
    tax_year 2026        917,978 rows of real DOF assessments

`_aggregate_assessment_spike` counts DISTINCT tax_year across the whole table, gets 7,
clears its own >= 2 guard, then takes the two most recent years as current and
prior. Prior resolves to 2023, which is a rent-stabilization year carrying no
assessed_total, so `WHERE p.assessed_total > 0` discards all 32,565 joined rows.
Measured: of the 32,565 BBLs present in both 2026 and 2023, **exactly zero**
satisfy the comparison, which is not a data pattern, it is a schema collision.

The conclusion "one real assessment year, so the signal stays dormant" was right.
Everything it was inferred from was wrong.

Two things are fixed here. The year selection only considers years that carry
assessments, so a rent-stabilization backfill can never be chosen as the prior
year; and the message says what is true. Today that changes nothing, because
2026 is the only assessment year either way. It matters the moment
backfill_rs_history writes a year at or above the newest assessment year, which
would silently pick an all-NULL prior and return zero while reporting a reason
that has nothing to do with it.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SRC = (REPO / "scoring" / "compute.py").read_text()


# Python adjacent-string concatenation splits one SQL statement across source
# lines, so a naive grep sees "FROM assessment_history" and then a quote. Same
# shape that hid the /property meta description from the eviction-label guard.
_CONCAT = re.compile(r"[\"']\s*\n\s*[frbu]*[\"']", re.I)


def _fn() -> str:
    m = re.search(r"def _aggregate_assessment_spike.*?(?=\ndef )", SRC, re.S)
    assert m, "could not find _aggregate_assessment_spike; the grep has rotted"
    return _CONCAT.sub(" ", m.group(0))


class TestYearSelectionIgnoresNonAssessmentYears:
    def test_the_year_count_requires_an_assessment(self):
        fn = _fn()
        m = re.search(r"COUNT\(DISTINCT tax_year\)\s+FROM assessment_history([^\"']*)", fn)
        assert m, "the distinct-year count is gone or reshaped"
        assert "assessed_total IS NOT NULL" in m.group(1), (
            "counting every tax_year includes the 197,730 rent-stabilization rows "
            "written by backfill_rs_history.py, which carry no assessed_total. The "
            "count then clears its own >= 2 guard on years that cannot be compared."
        )

    def test_the_two_years_chosen_must_carry_assessments(self):
        fn = _fn()
        m = re.search(r"SELECT DISTINCT tax_year FROM assessment_history([^)]*)\)", fn)
        assert m, "the two_years CTE is gone or reshaped"
        assert "assessed_total IS NOT NULL" in m.group(1), (
            "current and prior are picked as the two most recent tax_years in the "
            "table. Without this filter prior resolves to 2023, a rent-stabilization "
            "year, and every joined row is discarded by `p.assessed_total > 0`."
        )


class TestTheMessageIsTrue:
    def test_it_does_not_claim_a_year_count_it_did_not_measure(self):
        assert "assessment_history has <2 tax years" not in SRC, (
            "the table has seven tax years. The message has to describe the real "
            "condition, which is that fewer than two of them carry assessments."
        )

    def test_it_names_the_real_condition(self):
        m = re.search(r"Assessment spike signal is dormant[^\"']*", SRC)
        assert m, "the dormancy log line is gone"
        assert "assess" in m.group(0).lower(), m.group(0)
