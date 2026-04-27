"""
Tests for the batch sanity guard in scoring/compute.py.

All unit tests — no live DB required.  The refactored _batch_sanity_check
accepts a pre-fetched ``prior`` dict instead of a live DB session, eliminating
the bug where the guard would read the session's own uncommitted writes and
compare the new batch against itself.

Integration tests (marked @pytest.mark.integration) verify end-to-end
behaviour: guard blocks before displacement_scores or score_history are written.
"""

import pytest
from unittest.mock import MagicMock

from scoring.compute import (
    ScoringGuardError,
    _batch_sanity_check,
    _count_active_signals,
    _fetch_prior_baseline,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_prior(
    max_score: float = 72.0,
    avg_score: float = 35.0,
    count: int = 183,
    active_signals: int = 5,
) -> dict:
    """Build a prior baseline dict as returned by _fetch_prior_baseline()."""
    return {
        "max":            max_score,
        "avg":            avg_score,
        "count":          count,
        "active_signals": active_signals,
    }


def _signal_norms(n_zips: int = 183, active_signals: list[str] | None = None) -> dict:
    all_signals = [
        "permits", "evictions", "llc_acquisitions",
        "hpd_violations", "complaint_rate", "rs_unit_loss",
    ]
    if active_signals is None:
        active_signals = [
            "permits", "evictions", "llc_acquisitions",
            "hpd_violations", "complaint_rate",
        ]
    norms = {}
    for sig in all_signals:
        val = 45.0 if sig in active_signals else 0.0
        norms[sig] = {str(i): val for i in range(n_zips)}
    return norms


def _healthy_scores(n: int = 183, max_score: float = 72.0) -> dict:
    import random
    random.seed(42)
    return {
        str(10000 + i): max(1.0, min(100.0, random.uniform(5.0, max_score)))
        for i in range(n)
    }


# ---------------------------------------------------------------------------
# _count_active_signals
# ---------------------------------------------------------------------------

class TestCountActiveSignals:
    def test_all_active(self):
        norms = {sig: {"z1": 50.0, "z2": 40.0} for sig in ["permits", "evictions", "llc"]}
        assert _count_active_signals(norms) == 3

    def test_none_active(self):
        norms = {sig: {"z1": 0.0, "z2": 0.0} for sig in ["permits", "evictions"]}
        assert _count_active_signals(norms) == 0

    def test_mixed(self):
        norms = {
            "permits":   {"z1": 50.0, "z2": 40.0},
            "evictions": {"z1": 0.0,  "z2": 0.0},
            "llc":       {"z1": 20.0, "z2": 10.0},
        }
        assert _count_active_signals(norms) == 2

    def test_empty(self):
        assert _count_active_signals({}) == 0

    def test_mean_at_boundary(self):
        norms = {"permits": {"z1": 1.0, "z2": 1.0}}
        assert _count_active_signals(norms) == 0

        norms = {"permits": {"z1": 1.1, "z2": 1.1}}
        assert _count_active_signals(norms) == 1


# ---------------------------------------------------------------------------
# _fetch_prior_baseline — unit tests via mock DB
# ---------------------------------------------------------------------------

class TestFetchPriorBaseline:
    def _db_with_rows(self, scores: list[float], breakdown: dict | None = None) -> MagicMock:
        if breakdown is None:
            breakdown = {"permits": 45.0, "evictions": 38.0, "llc_acquisitions": 52.0,
                         "hpd_violations": 30.0, "complaint_rate": 40.0, "rs_unit_loss": 0.0}
        rows = [(s, breakdown) for s in scores]
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = rows
        return db

    def test_empty_table_returns_none(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []
        assert _fetch_prior_baseline(db) is None

    def test_returns_correct_max_avg_count(self):
        scores = [10.0, 20.0, 30.0, 40.0, 50.0]
        db = self._db_with_rows(scores)
        prior = _fetch_prior_baseline(db)
        assert prior is not None
        assert prior["max"] == 50.0
        assert prior["avg"] == pytest.approx(30.0)
        assert prior["count"] == 5

    def test_counts_active_signals(self):
        # breakdown has 5 signals with mean > 1.0 and rs_unit_loss = 0
        breakdown = {"permits": 45.0, "evictions": 38.0, "llc_acquisitions": 52.0,
                     "hpd_violations": 30.0, "complaint_rate": 40.0, "rs_unit_loss": 0.0}
        db = self._db_with_rows([50.0, 60.0], breakdown)
        prior = _fetch_prior_baseline(db)
        assert prior["active_signals"] == 5

    def test_zero_signal_not_counted_active(self):
        # All signals are zero → active_signals = 0
        breakdown = {"permits": 0.0, "evictions": 0.0}
        db = self._db_with_rows([30.0, 40.0], breakdown)
        prior = _fetch_prior_baseline(db)
        assert prior["active_signals"] == 0

    def test_returns_dict_with_required_keys(self):
        db = self._db_with_rows([35.0])
        prior = _fetch_prior_baseline(db)
        assert prior is not None
        for key in ("max", "avg", "count", "active_signals"):
            assert key in prior


# ---------------------------------------------------------------------------
# _batch_sanity_check — guard passes
# ---------------------------------------------------------------------------

class TestGuardPasses:
    def test_healthy_batch_passes(self):
        prior  = _make_prior(max_score=72.0, avg_score=35.0)
        scores = _healthy_scores(183, max_score=72.0)
        norms  = _signal_norms(183)
        _batch_sanity_check(prior, scores, norms, force=False)  # must not raise

    def test_no_prior_data_always_passes(self):
        """First-ever run with empty displacement_scores must not be blocked."""
        scores = _healthy_scores(183)
        norms  = _signal_norms(183)
        _batch_sanity_check(None, scores, norms, force=False)

    def test_force_bypasses_all_checks(self):
        """--force skips guard even with catastrophically bad scores."""
        prior  = _make_prior(max_score=72.0, avg_score=35.0)
        scores = {str(i): 1.0 for i in range(50)}  # only 50 ZIPs, all at floor
        norms  = _signal_norms(50, active_signals=[])
        _batch_sanity_check(prior, scores, norms, force=True)  # must not raise

    def test_prior_dict_not_db_session(self):
        """Guard must accept a dict, not a DB session — passing a dict proves
        the refactor is complete and the guard no longer reads from the DB."""
        prior  = _make_prior()
        scores = _healthy_scores(183)
        norms  = _signal_norms(183)
        # If _batch_sanity_check still accepted a Session it would fail here
        # because _make_prior() returns a plain dict.
        _batch_sanity_check(prior, scores, norms, force=False)


# ---------------------------------------------------------------------------
# _batch_sanity_check — guard blocks
# ---------------------------------------------------------------------------

class TestGuardBlocks:
    def test_max_score_collapse_blocks(self):
        """New max < 50% of previous max triggers guard."""
        prior  = _make_prior(max_score=72.0, avg_score=35.0)
        scores = {str(i): 30.0 for i in range(183)}  # new max=30 < 72*0.5=36
        norms  = _signal_norms(183)
        with pytest.raises(ScoringGuardError, match="max score collapsed"):
            _batch_sanity_check(prior, scores, norms, force=False)

    def test_avg_score_collapse_blocks(self):
        """New average < 50% of previous average triggers guard."""
        prior      = _make_prior(avg_score=35.0, max_score=35.0)
        new_scores = {str(0): 35.0, **{str(i): 4.0 for i in range(1, 183)}}
        norms      = _signal_norms(183)
        with pytest.raises(ScoringGuardError, match="avg score collapsed"):
            _batch_sanity_check(prior, new_scores, norms, force=False)

    def test_too_few_zips_blocks(self):
        """ZIP count < 170 triggers guard."""
        prior  = _make_prior()
        scores = _healthy_scores(n=100)
        norms  = _signal_norms(100)
        with pytest.raises(ScoringGuardError, match="ZIP count too low"):
            _batch_sanity_check(prior, scores, norms, force=False)

    def test_floor_majority_blocks(self):
        """> 50% of ZIPs at score <= 5 triggers guard."""
        prior  = _make_prior(avg_score=30.0, max_score=50.0)
        scores = {str(i): (2.0 if i < 100 else 50.0) for i in range(183)}
        norms  = _signal_norms(183)
        with pytest.raises(ScoringGuardError, match="score <= 5"):
            _batch_sanity_check(prior, scores, norms, force=False)

    def test_signal_collapse_blocks(self):
        """Previous >= 4 active signals, new <= 2 active signals triggers guard."""
        prior  = _make_prior(active_signals=5)
        scores = _healthy_scores(183, max_score=72.0)
        norms  = _signal_norms(183, active_signals=["hpd_violations"])  # only 1 active
        with pytest.raises(ScoringGuardError, match="active signal count collapsed"):
            _batch_sanity_check(prior, scores, norms, force=False)

    def test_blocked_run_does_not_commit(self):
        """_batch_sanity_check never calls db.commit — commit is the caller's job."""
        # The refactored guard no longer accepts a db argument, so there is no
        # DB object to inadvertently commit.  This test verifies the function
        # raises without touching any external state.
        prior  = _make_prior()
        scores = {str(i): 1.0 for i in range(50)}  # triggers ZIP count < 170
        norms  = _signal_norms(50)
        with pytest.raises(ScoringGuardError):
            _batch_sanity_check(prior, scores, norms, force=False)

    def test_force_bypasses_zip_count(self):
        prior  = _make_prior()
        scores = _healthy_scores(n=50)
        norms  = _signal_norms(50)
        _batch_sanity_check(prior, scores, norms, force=True)  # must not raise

    def test_force_bypasses_signal_collapse(self):
        prior  = _make_prior(active_signals=5)
        scores = _healthy_scores(183, max_score=72.0)
        norms  = _signal_norms(183, active_signals=[])  # 0 active
        _batch_sanity_check(prior, scores, norms, force=True)

    def test_guard_uses_prior_not_live_db(self):
        """
        Core regression test for the original bug.

        Before the fix: guard called db.execute() to read displacement_scores
        *after* new values were written in the same transaction.  PostgreSQL
        returned the session's own uncommitted rows, so prev_avg == new_avg
        and the collapse check (new_avg < prev_avg * 0.50) always passed.

        After the fix: guard accepts a pre-fetched `prior` dict.  There is no
        DB read inside _batch_sanity_check at all.  We simulate the bug scenario
        by constructing:
          - prior baseline: avg=35 (healthy, from before the run)
          - new batch: avg ≈ 6.79 (collapsed, what the bad run computed)
        The guard must catch this collapse even though we pass the prior
        directly rather than reading from DB.
        """
        prior = _make_prior(max_score=72.4, avg_score=35.0, active_signals=5)

        # Simulate today's collapsed batch: ~184 ZIPs, most at ~4, one at 72.4
        collapsed = {"99901": 72.4}
        collapsed.update({str(10000 + i): 4.0 for i in range(183)})
        # avg ≈ (72.4 + 183*4) / 184 ≈ 4.39; well below 35.0 * 0.50 = 17.5

        norms = _signal_norms(183, active_signals=["hpd_violations"])  # 1 active (signal collapse)

        with pytest.raises(ScoringGuardError):
            _batch_sanity_check(prior, collapsed, norms, force=False)


# ---------------------------------------------------------------------------
# Signal collapse edge cases
# ---------------------------------------------------------------------------

class TestSignalCollapseEdgeCases:
    def test_prev_3_active_no_collapse_trigger(self):
        """Signal collapse only triggers when previous had >= 4 active."""
        prior  = _make_prior(active_signals=3)
        scores = _healthy_scores(183, max_score=72.0)
        norms  = _signal_norms(183, active_signals=["permits"])  # 1 active now
        _batch_sanity_check(prior, scores, norms, force=False)  # must not raise

    def test_exactly_2_active_signals_passes_when_prev_had_3(self):
        prior  = _make_prior(active_signals=3)
        scores = _healthy_scores(183)
        norms  = _signal_norms(183, active_signals=["permits", "evictions"])
        _batch_sanity_check(prior, scores, norms, force=False)  # must not raise


# ---------------------------------------------------------------------------
# Orphan ZIP cleanup (integration)
# ---------------------------------------------------------------------------

class TestOrphanCleanup:
    """
    Verify compute_scores() deletes displacement_scores rows for ZIPs with no
    matching ZCTA geometry in the neighborhoods table (Step 9).
    """

    @pytest.mark.integration
    def test_orphan_zips_removed_after_scoring(self):
        from models.database import SessionLocal
        from sqlalchemy import text
        from scoring.compute import compute_scores

        db = SessionLocal()
        try:
            db.execute(text(
                "DELETE FROM displacement_scores WHERE zip_code IN ('99998', '99999')"
            ))
            db.commit()

            db.execute(text("""
                INSERT INTO displacement_scores
                    (zip_code, score, signal_breakdown, signal_last_updated,
                     cache_generated_at, created_at, updated_at)
                VALUES ('99998', 50.0, '{}', '{}', NOW(), NOW(), NOW())
                ON CONFLICT ON CONSTRAINT uq_displacement_scores_zip_code DO NOTHING
            """))
            db.commit()

            before = db.execute(text(
                "SELECT COUNT(*) FROM displacement_scores WHERE zip_code = '99998'"
            )).scalar()
            assert before == 1

            compute_scores(db, force=True)

            after = db.execute(text(
                "SELECT COUNT(*) FROM displacement_scores WHERE zip_code = '99998'"
            )).scalar()
            assert after == 0, "Step 9 must remove orphan ZIP 99998"

            leftover = db.execute(text("""
                SELECT COUNT(*) FROM displacement_scores ds
                LEFT JOIN neighborhoods n ON ds.zip_code = n.zip_code
                WHERE n.zip_code IS NULL
            """)).scalar()
            assert leftover == 0, f"{leftover} orphan ZIP(s) still present after cleanup"
        finally:
            db.execute(text(
                "DELETE FROM displacement_scores WHERE zip_code IN ('99998', '99999')"
            ))
            db.commit()
            db.close()


# ---------------------------------------------------------------------------
# Guard fires before writes (integration)
# ---------------------------------------------------------------------------

class TestGuardBlocksBeforeWrites:
    """
    Verify that when the guard fires, no rows are written to displacement_scores
    or score_history.  This is the end-to-end proof that the fix works.
    """

    @pytest.mark.integration
    def test_collapsed_batch_blocked_before_displacement_scores_written(self):
        """
        Inject a synthetic prior baseline that makes any real scoring run look
        collapsed, trigger compute_scores(), and confirm displacement_scores
        was not modified.
        """
        from models.database import SessionLocal
        from sqlalchemy import text
        from scoring.compute import compute_scores

        db = SessionLocal()
        try:
            before_max = db.execute(
                text("SELECT MAX(score) FROM displacement_scores")
            ).scalar() or 0.0
            before_updated = db.execute(
                text("SELECT MAX(updated_at) FROM displacement_scores")
            ).scalar()

            # Run with an absurdly high force-threshold via a patched prior.
            # We patch _fetch_prior_baseline to return a baseline whose avg is
            # 1000x above any realistic score, guaranteeing the guard fires.
            import unittest.mock as mock
            fake_prior = {
                "max": 9999.0, "avg": 9999.0,
                "count": 183, "active_signals": 5,
            }
            with mock.patch(
                "scoring.compute._fetch_prior_baseline", return_value=fake_prior
            ):
                with pytest.raises(ScoringGuardError):
                    compute_scores(db, force=False)

            # displacement_scores must be unchanged
            after_updated = db.execute(
                text("SELECT MAX(updated_at) FROM displacement_scores")
            ).scalar()
            assert after_updated == before_updated, (
                "displacement_scores.updated_at changed after a guard-blocked run"
            )
        finally:
            db.close()

    @pytest.mark.integration
    def test_collapsed_batch_does_not_write_score_history(self):
        """
        When the guard blocks, score_history must not receive a new snapshot.
        Uses the same fake-prior patching approach as the displacement_scores test.
        """
        from datetime import date
        from models.database import SessionLocal
        from sqlalchemy import text
        from scoring.compute import compute_scores
        import unittest.mock as mock

        db = SessionLocal()
        try:
            today = date.today()
            # Delete any existing history for today so we can detect a fresh write.
            db.execute(
                text("DELETE FROM score_history WHERE scored_at = :d"), {"d": today}
            )
            db.commit()

            fake_prior = {
                "max": 9999.0, "avg": 9999.0,
                "count": 183, "active_signals": 5,
            }
            with mock.patch(
                "scoring.compute._fetch_prior_baseline", return_value=fake_prior
            ):
                with pytest.raises(ScoringGuardError):
                    compute_scores(db, force=False)

            count_today = db.execute(
                text("SELECT COUNT(*) FROM score_history WHERE scored_at = :d"),
                {"d": today},
            ).scalar()
            assert count_today == 0, (
                f"score_history gained {count_today} rows despite guard blocking the run"
            )
        finally:
            db.close()
