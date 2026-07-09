"""
DOF scraper batch de-dupe: repeated BBLs in one upsert batch caused
CardinalityViolation and failed the nightly pipeline three retries deep
on 2026-07-09.
"""

from scrapers.dof import _dedupe_by_bbl


class TestDedupeByBbl:

    def test_last_occurrence_wins(self):
        batch = [
            {"bbl": "1000010001", "assessed_total": 100},
            {"bbl": "1000010002", "assessed_total": 200},
            {"bbl": "1000010001", "assessed_total": 150},
        ]
        out = _dedupe_by_bbl(batch)
        assert len(out) == 2
        assert {r["bbl"]: r["assessed_total"] for r in out}["1000010001"] == 150

    def test_no_duplicates_passthrough(self):
        batch = [{"bbl": "1"}, {"bbl": "2"}]
        assert _dedupe_by_bbl(batch) == batch

    def test_empty_batch(self):
        assert _dedupe_by_bbl([]) == []
