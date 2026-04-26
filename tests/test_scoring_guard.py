"""
Tests for the batch sanity guard in scoring/compute.py.

All unit tests — no live DB required. DB reads inside _batch_sanity_check
are mocked to return controlled baseline data.
"""

import pytest
from unittest.mock import MagicMock, patch

from scoring.compute import ScoringGuardError, _batch_sanity_check, _count_active_signals


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _db_with_prev(prev_scores: list[float], signal_means: dict | None = None) -> MagicMock:
    """
    Return a mock DB whose displacement_scores query returns the given prev scores.
    signal_means: per-signal mean value written into each row's signal_breakdown.
    """
    if signal_means is None:
        # Default: all 5 active signals well above 1.0
        signal_means = {
            "permits": 45.0, "evictions": 38.0, "llc_acquisitions": 52.0,
            "hpd_violations": 30.0, "complaint_rate": 40.0, "rs_unit_loss": 0.0,
        }

    rows = [
        MagicMock(
            __getitem__=lambda self, i: (score if i == 0 else signal_means)[i],
        )
        for score in prev_scores
    ]
    # Use tuples so r[0] and r[1] work directly
    rows = [(score, signal_means) for score in prev_scores]

    db = MagicMock()
    db.execute.return_value.fetchall.return_value = rows
    return db


def _signal_norms(n_zips: int = 183, active_signals: list[str] | None = None) -> dict:
    """
    Build a signal_norms dict where active_signals have mean > 1.0 and others are 0.
    Default: 5 active signals.
    """
    all_signals = ["permits", "evictions", "llc_acquisitions", "hpd_violations", "complaint_rate", "rs_unit_loss"]
    if active_signals is None:
        active_signals = ["permits", "evictions", "llc_acquisitions", "hpd_violations", "complaint_rate"]

    norms = {}
    for sig in all_signals:
        val = 45.0 if sig in active_signals else 0.0
        norms[sig] = {str(i): val for i in range(n_zips)}
    return norms


def _healthy_scores(n: int = 183, max_score: float = 72.0) -> dict:
    """Return a healthy computed_scores dict."""
    import random
    random.seed(42)
    scores = {}
    for i in range(n):
        scores[str(10000 + i)] = max(1.0, min(100.0, random.uniform(5.0, max_score)))
    return scores


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
        # mean exactly 1.0 is NOT active (must be > 1.0)
        norms = {"permits": {"z1": 1.0, "z2": 1.0}}
        assert _count_active_signals(norms) == 0

        norms = {"permits": {"z1": 1.1, "z2": 1.1}}
        assert _count_active_signals(norms) == 1


# ---------------------------------------------------------------------------
# _batch_sanity_check — guard passes
# ---------------------------------------------------------------------------

class TestGuardPasses:
    def test_healthy_batch_passes(self):
        scores = _healthy_scores(183, max_score=72.0)
        norms  = _signal_norms(183, active_signals=["permits", "evictions", "llc_acquisitions", "hpd_violations", "complaint_rate"])
        db = _db_with_prev([72.0, 60.0, 55.0, 50.0, 45.0] * 36 + [72.0, 60.0, 55.0])

        # Should not raise
        _batch_sanity_check(db, scores, norms, force=False)

    def test_no_prior_data_always_passes(self):
        """First-ever run with empty displacement_scores must not be blocked."""
        scores = _healthy_scores(183)
        norms  = _signal_norms(183)
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []

        _batch_sanity_check(db, scores, norms, force=False)

    def test_force_bypasses_all_checks(self):
        """--force skips guard even with catastrophically bad scores."""
        scores = {str(i): 1.0 for i in range(50)}  # only 50 ZIPs, all at floor
        norms  = _signal_norms(50, active_signals=[])
        db = _db_with_prev([72.0] * 183)

        # Should not raise despite failing every threshold
        _batch_sanity_check(db, scores, norms, force=True)


# ---------------------------------------------------------------------------
# _batch_sanity_check — guard blocks
# ---------------------------------------------------------------------------

class TestGuardBlocks:
    def _prev_db(self, prev_max: float = 72.0, n: int = 183, active: int = 5) -> MagicMock:
        active_sigs = list(_signal_norms().keys())[:active]
        signal_means = {s: (45.0 if s in active_sigs else 0.0) for s in _signal_norms().keys()}
        scores = [prev_max] + [prev_max * 0.8] * (n - 1)
        return _db_with_prev(scores, signal_means)

    def test_max_score_collapse_blocks(self):
        """New max < 50% of previous max triggers guard."""
        prev_max = 72.0
        scores = {str(i): 30.0 for i in range(183)}  # new max = 30 < 72*0.5 = 36
        norms  = _signal_norms(183)
        db = self._prev_db(prev_max=prev_max)

        with pytest.raises(ScoringGuardError, match="max score collapsed"):
            _batch_sanity_check(db, scores, norms, force=False)

    def test_avg_score_collapse_blocks(self):
        """New average < 50% of previous average triggers guard, max left intact."""
        # prev avg = 35.0, threshold = 17.5
        # new: one high score keeps max healthy (35.0 >= 17.5), rest very low → avg ~4.2 < 17.5
        prev_scores = [35.0] * 183
        new_scores  = {str(0): 35.0, **{str(i): 4.0 for i in range(1, 183)}}
        norms = _signal_norms(183)
        db = _db_with_prev(prev_scores)

        with pytest.raises(ScoringGuardError, match="avg score collapsed"):
            _batch_sanity_check(db, new_scores, norms, force=False)

    def test_too_few_zips_blocks(self):
        """ZIP count < 170 triggers guard."""
        scores = _healthy_scores(n=100)  # only 100 ZIPs
        norms  = _signal_norms(100)
        db = self._prev_db()

        with pytest.raises(ScoringGuardError, match="ZIP count too low"):
            _batch_sanity_check(db, scores, norms, force=False)

    def test_floor_majority_blocks(self):
        """> 50% of ZIPs at score <= 5 triggers guard, max/avg kept healthy."""
        # prev avg = 30.0 → threshold = 15.0
        # new: 100 ZIPs at 2.0 (floor, 54.6%), 83 ZIPs at 50.0
        # new max = 50.0 >= 15.0 ✓, new avg = (200+4150)/183 ≈ 23.8 >= 15.0 ✓
        # floor count = 100 > 183*0.5 = 91.5 → triggers floor check
        scores = {str(i): (2.0 if i < 100 else 50.0) for i in range(183)}
        norms  = _signal_norms(183)
        db = _db_with_prev([30.0] * 183)

        with pytest.raises(ScoringGuardError, match="score <= 5"):
            _batch_sanity_check(db, scores, norms, force=False)

    def test_signal_collapse_blocks(self):
        """Previous >= 4 active signals, new <= 2 active signals triggers guard."""
        scores = _healthy_scores(183, max_score=72.0)
        # Only 1 active signal in new batch
        norms  = _signal_norms(183, active_signals=["hpd_violations"])
        # Previous had 5 active signals
        db = self._prev_db(active=5)

        with pytest.raises(ScoringGuardError, match="active signal count collapsed"):
            _batch_sanity_check(db, scores, norms, force=False)

    def test_blocked_run_does_not_commit(self):
        """On ScoringGuardError the caller must rollback — guard itself does not commit."""
        scores = {str(i): 1.0 for i in range(50)}  # only 50 ZIPs
        norms  = _signal_norms(50)
        db = self._prev_db()

        with pytest.raises(ScoringGuardError):
            _batch_sanity_check(db, scores, norms, force=False)

        # Guard never calls commit
        db.execute.return_value.fetchall.return_value  # already consumed
        # commit was not called by _batch_sanity_check itself
        db.commit.assert_not_called()

    def test_force_bypasses_zip_count(self):
        scores = _healthy_scores(n=50)
        norms  = _signal_norms(50)
        db = self._prev_db()

        # Should not raise despite < 170 ZIPs
        _batch_sanity_check(db, scores, norms, force=True)

    def test_force_bypasses_signal_collapse(self):
        scores = _healthy_scores(183, max_score=72.0)
        norms  = _signal_norms(183, active_signals=[])  # 0 active signals
        db = self._prev_db(active=5)

        _batch_sanity_check(db, scores, norms, force=True)


# ---------------------------------------------------------------------------
# Signal collapse edge cases
# ---------------------------------------------------------------------------

class TestSignalCollapseEdgeCases:
    def test_prev_3_active_no_collapse_trigger(self):
        """Signal collapse only triggers when previous had >= 4 active."""
        scores = _healthy_scores(183, max_score=72.0)
        norms  = _signal_norms(183, active_signals=["permits"])  # 1 active now
        # Previous only had 3 active — guard should NOT trigger on signal collapse
        prev_means = {"permits": 45.0, "evictions": 40.0, "llc_acquisitions": 35.0,
                      "hpd_violations": 0.0, "complaint_rate": 0.0, "rs_unit_loss": 0.0}
        db = _db_with_prev([72.0] * 183, prev_means)

        # Only signal collapse checked here — other checks pass with healthy scores/counts
        _batch_sanity_check(db, scores, norms, force=False)

    def test_exactly_2_active_signals_passes_when_prev_had_3(self):
        scores = _healthy_scores(183)
        norms  = _signal_norms(183, active_signals=["permits", "evictions"])  # 2 active
        prev_means = {"permits": 45.0, "evictions": 40.0, "llc_acquisitions": 35.0,
                      "hpd_violations": 0.0, "complaint_rate": 0.0, "rs_unit_loss": 0.0}
        db = _db_with_prev([72.0] * 183, prev_means)

        _batch_sanity_check(db, scores, norms, force=False)
