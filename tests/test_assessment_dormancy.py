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

import pytest
from sqlalchemy import text

from models.database import SessionLocal

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


@pytest.fixture()
def db():
    session = SessionLocal()
    yield session
    session.rollback()
    session.close()


@pytest.mark.integration
@pytest.mark.needs_data
class TestOnlyAComparablePairCounts:
    """What the signal must ignore, asserted by running it.

    The two checks here used to grep the function's source for
    `assessed_total IS NOT NULL` beside two particular CTE names. Both went red
    the first time the query was reshaped, while the rule they protect was
    still enforced -- more strictly, since the ranking now filters the NULLs at
    the source and no all-NULL year can be ranked at all. A guard that fails on
    a rewrite it approves of is not measuring the rule.

    So this sets up each shape the signal must reject, in one transaction that
    is rolled back, and asks which ZIPs come out. The years are far in the
    future on purpose: the aggregate looks at the newest year in the table, so
    2098/2099 rows put every real row out of contention and leave the fixtures
    alone in the result.
    """

    # One case per lot, each in its own ZIP so the result is unambiguous.
    RISE = ("3038840333", "11207")      # 2098 -> 2099, up. The only one that counts.
    NULL_PRIOR = ("5012720320", "10303")  # prior year carries no assessment
    GAP = ("4024660017", "11377")       # 2090 -> 2099, eight years apart
    FALL = ("4097710009", "11432")      # 2098 -> 2099, down

    def _seed(self, db):
        rows = [
            (self.RISE[0], 2098, 100_000.0), (self.RISE[0], 2099, 200_000.0),
            (self.NULL_PRIOR[0], 2098, None), (self.NULL_PRIOR[0], 2099, 200_000.0),
            (self.GAP[0], 2090, 100_000.0), (self.GAP[0], 2099, 200_000.0),
            (self.FALL[0], 2098, 200_000.0), (self.FALL[0], 2099, 100_000.0),
        ]
        db.execute(
            text("""INSERT INTO assessment_history (bbl, assessed_total, tax_year, created_at)
                    VALUES (:bbl, :total, :year, NOW())
                    ON CONFLICT (bbl, tax_year) DO UPDATE
                       SET assessed_total = EXCLUDED.assessed_total"""),
            [{"bbl": b, "year": y, "total": t} for b, y, t in rows],
        )

    def test_the_signal_takes_the_rise_and_nothing_else(self, db):
        from scoring.compute import _aggregate_assessment_spike

        self._seed(db)
        zips = dict(_aggregate_assessment_spike(db))

        assert self.RISE[1] in zips, (
            "a lot assessed higher in the newest year than in the year before it "
            "is the whole signal, and it did not come through"
        )
        assert zips[self.RISE[1]] > 0

        assert self.NULL_PRIOR[1] not in zips, (
            "the prior year carried no assessed_total. That is the shape of the "
            "197,730 rent-stabilization rows backfill_rs_history.py writes, and "
            "choosing one as the prior year is what made the dormancy message lie"
        )
        assert self.GAP[1] not in zips, (
            "2090 and 2099 are not a year over year comparison. scrapers/dof.py "
            "files a frozen archive that stops at 2018/19, so without this the "
            "signal would read an eight-year drift on the 14,143 condo unit lots "
            "that appear in both feeds as a spike"
        )
        assert self.FALL[1] not in zips, "an assessment that fell is not a spike"

    def test_it_is_dormant_on_the_real_table(self, db):
        """No synthetic rows: today nothing has two consecutive years, and the
        signal has to say so by returning nothing rather than by guessing."""
        from scoring.compute import _aggregate_assessment_spike

        assert _aggregate_assessment_spike(db) == []


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
