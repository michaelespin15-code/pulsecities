"""
The history chart and the map must not disagree about the same day.

`compute_scores()` writes the same numbers to two tables in one pass, and until
2026-08-28 the two writes had opposite conflict policies:

    displacement_scores   ON CONFLICT ... DO UPDATE    last run of the day wins
    score_history         ON CONFLICT ... DO NOTHING   first run of the day wins

That is harmless on a day that scores once and wrong on every day that scores
twice, which is precisely what a data fix looks like. On 2026-08-28 the nightly
pipeline scored at 02:07 and a recompute followed the permit backfill at 02:21.
The map took the second run and the history chart kept the first: 156 of 177
ZIPs disagreed, by a mean of 0.92 points and a maximum of 8.0, and eight of them
landed in a different band in the chart than on the map. 10452 and 10457 read
Critical in history and High on the map on the same afternoon.

The pipeline's own snapshot guard could not see it. It compares a row count
against the number of ZIPs scored, and 177 stale rows count the same as 177
fresh ones.

Two rules, both grepped here because both were bypassed in production:

  1. The score_history insert refreshes on conflict, so the last scoring run of
     a day is the one the chart shows, matching the map by construction.
  2. score_history has exactly one writer. `snapshot_scores()` used to be a
     second, copying displacement_scores into history after the fact. It could
     not write a correct row -- displacement_scores carries no hpd_violations or
     rs_unit_loss column, so two of six signals were always NULL -- and it was
     reachable from production code while being called only by its own tests.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SEARCH_DIRS = ("scrapers", "scripts", "scoring", "api", "scheduler")

_INSERT_HISTORY = re.compile(
    r"INSERT\s+INTO\s+score_history\b(.*?)(?=INSERT\s+INTO|\"\"\"|$)",
    re.S | re.I,
)


def _history_inserts():
    """Every raw-SQL INSERT INTO score_history in application code."""
    for d in SEARCH_DIRS:
        for path in sorted((REPO / d).rglob("*.py")):
            if "__pycache__" in path.as_posix():
                continue
            src = path.read_text()
            if "INSERT INTO score_history" not in src:
                continue
            for m in _INSERT_HISTORY.finditer(src):
                yield path.relative_to(REPO).as_posix(), m.group(0)


class TestHistoryRefreshesOnConflict:
    def test_every_history_insert_updates_on_conflict(self):
        """A second scoring run on the same date must refresh the row, not drop it."""
        sites = list(_history_inserts())
        assert sites, "grep found no INSERT INTO score_history -- the pattern has rotted"

        offenders = [
            path for path, sql in sites
            if re.search(r"ON\s+CONFLICT.*?DO\s+NOTHING", sql, re.S | re.I)
        ]
        assert not offenders, (
            "score_history insert uses DO NOTHING, so the first scoring run of a "
            "day wins and every later run is silently discarded. The map upserts, "
            f"so the two tables diverge for that date. Offending file(s): {offenders}"
        )

    def test_history_insert_sets_updated_at_on_conflict(self):
        """The row that refreshed must say it refreshed."""
        for path, sql in _history_inserts():
            if not re.search(r"DO\s+UPDATE", sql, re.I):
                continue
            assert re.search(r"updated_at\s*=", sql, re.I), (
                f"{path}: DO UPDATE on score_history must assign updated_at, or the "
                "column freezes at first insert while the score keeps changing"
            )


class TestSingleWriter:
    def test_score_history_has_one_writer(self):
        """
        Two writers with different semantics is the shape that caused this.
        compute_scores() Step 7 is the writer; nothing else may insert.
        """
        writers = {path for path, _ in _history_inserts()}
        assert writers == {"scoring/compute.py"}, (
            "score_history must have exactly one writer (scoring/compute.py Step 7). "
            f"Found: {sorted(writers)}"
        )

    def test_snapshot_scores_is_gone(self):
        """
        It copied displacement_scores into history, which cannot carry
        hpd_violations or rs_unit_loss, so two of six signals were always NULL.
        It survived only as a re-export for its own tests.
        """
        for d in SEARCH_DIRS:
            for path in sorted((REPO / d).rglob("*.py")):
                if "__pycache__" in path.as_posix():
                    continue
                assert "def snapshot_scores" not in path.read_text(), (
                    f"{path.relative_to(REPO)} still defines snapshot_scores(); "
                    "score_history is written by compute_scores() Step 7 alone"
                )


class TestHealthReportCanSeeIt:
    """
    The two instruments that should have caught 2026-08-28 and did not.

    `scraper_health_label()` has a DEGRADED branch keyed on expected_min_records.
    The only production caller passed None for that argument and the query behind
    it never selected the column, so the branch could not fire. On the night the
    DOB NOW feed returned 0 records against a floor of 120 -- the feed whose own
    config comment says "watch dob_now_permits: this one going quiet is the plan"
    -- the one report a human reads printed OK.

    The live-vs-history drift check is the other. It compares the two tables'
    *averages* at a 30% tolerance. On 08-28 the averages were 31.1 and 31.7, a
    1.9% difference, while 156 of 177 individual ZIPs disagreed and 8 sat in
    different bands. Averaging is the wrong instrument for per-row divergence
    because the errors cancel; the per-ZIP count in scheduler/pipeline.py is the
    right one. This test pins the label function, not the threshold.
    """

    def test_degraded_branch_is_reachable(self):
        from scripts.pipeline_health import scraper_health_label
        assert scraper_health_label("success", 0, 120) == "DEGRADED"
        assert scraper_health_label("success", 59, 120) == "DEGRADED"
        assert scraper_health_label("success", 200, 120) == "OK"

    def test_caller_passes_the_real_minimum(self):
        """A hardcoded None here is what made the branch dead."""
        src = (REPO / "scripts" / "pipeline_health.py").read_text()
        assert "scraper_health_label(status, recs, None)" not in src, (
            "pipeline_health passes None for expected_min, so scraper_health_label "
            "can never return DEGRADED and a feed that goes quiet prints OK"
        )
        assert "expected_min_records" in src, (
            "pipeline_health must select expected_min_records to classify a run"
        )
