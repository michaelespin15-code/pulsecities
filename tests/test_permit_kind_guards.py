"""
One answer to "is this a permit to renovate an existing building".

permits_raw is written by two scrapers and they do not share a vocabulary.
Legacy BIS puts a DOB job code in `raw_data->>'job_type'` (A1, A2, A3, NB, DM);
DOB NOW spells the job type out and the scraper maps it into `permit_type`.
Neither column can express the other source's rows.

Four queries hand-rolled `raw_data->>'job_type' IN ('A1', 'A2')`. That matches
BIS and nothing else, so once DOB NOW carried 96% of permits they were reading
4,474 renovation permits in a year against 81,486. **Flip Watch is one of the
four. It is live at /flips, feeds the homepage docket and generates the weekly
editions, and it found 15 flips in the last 365 days where it should have found
639.**

Same shape as the freshness rule with seventeen bypasses and the five email
senders with five rules, so the same fix: api/permit_kinds.py owns it and this
file greps for the next copy. A rule with more readers than enforcers drifts.
"""

import re
from pathlib import Path

import pytest

from api import permit_kinds as pk

REPO = Path(__file__).resolve().parent.parent
# tests/ is in scope deliberately. test_neighborhood_flips.py carried its own
# copy of the predicate, kept the old one when production was fixed, and then
# failed claiming a ZIP had no flips while the page it checked had found some.
SEARCH_DIRS = ("api", "scripts", "scoring", "scheduler", "tests")
ALLOWED = {"api/permit_kinds.py", "tests/test_permit_kind_guards.py"}

# The hand-rolled shape: a job_type test written inline instead of imported.
_HAND_ROLLED = re.compile(r"job_type'\s*\)?\s*IN\s*\(", re.I)


class TestOneRuleOnly:
    def test_nothing_hand_rolls_the_job_type_predicate(self):
        offenders = []
        for d in SEARCH_DIRS:
            for path in sorted((REPO / d).rglob("*.py")):
                rel = path.relative_to(REPO).as_posix()
                if rel in ALLOWED or "__pycache__" in rel:
                    continue
                for i, line in enumerate(path.read_text().splitlines()):
                    if _HAND_ROLLED.search(line):
                        offenders.append(f"{rel}:{i + 1}")
        assert not offenders, (
            "these test job_type inline, which matches legacy BIS rows and "
            "nothing else. Use api.permit_kinds.renovation_sql():\n  "
            + "\n  ".join(offenders)
        )

    def test_the_grep_would_still_catch_the_old_code(self):
        """A guard that matches nothing passes forever."""
        assert _HAND_ROLLED.search("WHERE raw_data->>'job_type' IN ('A1', 'A2')")
        assert _HAND_ROLLED.search("AND pr.raw_data->>'job_type' IN ('A1','A2','NB')")


class TestPredicateCoversBothSources:
    @pytest.mark.parametrize("sql_fn", [pk.renovation_sql, pk.new_building_sql,
                                        pk.renovation_or_new_sql])
    def test_every_predicate_names_both_sources(self, sql_fn):
        """A single-column predicate silently drops one feed. That is the bug."""
        sql = sql_fn()
        assert "dob_now" in sql and "dob_bis" in sql
        assert "permit_type" in sql and "job_type" in sql

    def test_alias_is_applied_to_every_column(self):
        sql = pk.renovation_sql("pr")
        assert "pr.source" in sql and "pr.permit_type" in sql and "pr.raw_data" in sql
        assert " source" not in sql.replace("pr.source", "")

    def test_no_alias_leaves_columns_bare(self):
        assert "." not in pk.renovation_sql().replace("raw_data->>", "")

    def test_renovation_excludes_new_building(self):
        assert "'NB'" not in pk.renovation_sql()

    def test_a3_is_excluded(self):
        """A3 is the minor-work class, a sign or a fence. All four call sites
        this replaced excluded it and the replacement must not quietly add it."""
        assert "A3" not in pk.renovation_or_new_sql()

    def test_combined_is_the_union_without_repeats(self):
        sql = pk.renovation_or_new_sql()
        assert sql.count("'NB'") == 2  # once per source branch, not more


class TestLabels:
    @pytest.mark.parametrize("code,expected", [
        ("A1", "Major renovation"), ("A2", "Renovation"), ("NB", "New building"),
        ("AL", "Renovation"), ("DM", "Demolition"),
    ])
    def test_both_vocabularies_have_a_label(self, code, expected):
        assert pk.label(code) == expected

    def test_a_dob_now_row_does_not_claim_major(self):
        """BIS splits major from minor alteration and DOB NOW does not, so an
        AL row cannot honestly say "major"."""
        assert "Major" not in pk.label("AL")

    def test_an_unknown_code_degrades_to_a_word(self):
        assert pk.label("ZZ") == "Permit" and pk.label(None) == "Permit"

    def test_the_pulse_route_uses_this_map(self):
        from api.routes.pulse import PERMIT_TYPE_LABELS
        assert PERMIT_TYPE_LABELS["AL"] == pk.label("AL")


class TestAgreesWithTheScraper:
    def test_the_codes_match_what_the_scraper_writes(self):
        """permit_kinds keeps these as literals so the API does not import a
        scraper. That is only safe while the two agree."""
        from scrapers.dob_now_permits import JOB_TYPE_CODE
        written = set(JOB_TYPE_CODE.values())
        assert set(pk.NOW_RENOVATION_CODES) <= written
        assert set(pk.NOW_NEW_BUILDING_CODES) <= written
        assert written <= set(pk.KIND_LABELS), (
            "the scraper writes a code no label covers; it would render raw")
