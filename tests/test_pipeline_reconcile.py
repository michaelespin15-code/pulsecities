"""
Guards on the self-healing step in the nightly pipeline.

Two things have to hold or this becomes the next alert nobody reads.

A rewind that lands is a fix in progress, not an incident, so the first night a
feed drifts must be silent. A rewind that cannot move, because the watermark is
already behind the drifted range, means last night's heal did not work, and that
is the night to say something.

Reconciliation must also never be able to fail the pipeline. It is a repair step
bolted onto a run whose real job is already done.
"""

from datetime import date
from unittest.mock import patch

import scheduler.pipeline as pipeline


def _result(feed_name, drifted_day):
    """A reconcile_feed() result shaped like the real one."""
    class _Feed:
        scraper_name = feed_name
        dataset_id = "xxxx-yyyy"
    rows = []
    if drifted_day:
        rows.append({"day": drifted_day, "upstream": 10_000, "ours": 9_000,
                     "gap": 1_000, "pct": 0.1, "drifted": True})
    else:
        rows.append({"day": date(2026, 8, 10), "upstream": 10_000, "ours": 10_000,
                     "gap": 0, "pct": 0.0, "drifted": False})
    return {"feed": _Feed(), "rows": rows}


class TestHealingIsQuietOnTheFirstNight:
    def test_a_landed_rewind_sends_no_alert(self):
        """Drift found, watermark moved back. Tomorrow's run repairs it."""
        with patch.object(pipeline, "reconcile_feed",
                          return_value=_result("311_complaints", date(2026, 8, 5))), \
             patch.object(pipeline, "rewind_watermark", return_value="moved") as rw, \
             patch.object(pipeline, "send_alert") as alert:
            pipeline._reconcile_and_heal(object())

        assert rw.called, "drift must trigger a rewind"
        assert not alert.called, "a landed rewind is a fix in progress, not an incident"

    def test_a_rewind_that_cannot_move_does_alert(self):
        """Already rewound, still short. The heal did not work."""
        with patch.object(pipeline, "reconcile_feed",
                          return_value=_result("311_complaints", date(2026, 8, 5))), \
             patch.object(pipeline, "rewind_watermark", return_value=None), \
             patch.object(pipeline, "send_alert") as alert:
            pipeline._reconcile_and_heal(object())

        assert alert.called
        # The stub reports drift for every configured feed, so each one alerts.
        assert alert.call_count == len(pipeline.FEEDS)
        assert all("Ingestion drift persists" in c[0][0] for c in alert.call_args_list)
        assert all("rewind cannot widen" in c[0][1] for c in alert.call_args_list)

    def test_no_drift_does_nothing_at_all(self):
        with patch.object(pipeline, "reconcile_feed", return_value=_result("311_complaints", None)), \
             patch.object(pipeline, "rewind_watermark") as rw, \
             patch.object(pipeline, "send_alert") as alert:
            pipeline._reconcile_and_heal(object())

        assert not rw.called
        assert not alert.called


class TestReconcileCannotBreakTheRun:
    def test_a_crash_is_swallowed(self):
        """The scrape and the scores are already in. A repair step must never
        take the run down with it."""
        with patch.object(pipeline, "reconcile_feed", side_effect=RuntimeError("socrata down")), \
             patch.object(pipeline, "send_alert"):
            pipeline._safe_reconcile(object())  # must not raise

    def test_the_unguarded_form_really_does_raise(self):
        """Proves the guard above is doing work rather than the call being safe
        on its own."""
        import pytest
        with patch.object(pipeline, "reconcile_feed", side_effect=RuntimeError("socrata down")):
            with pytest.raises(RuntimeError):
                pipeline._reconcile_and_heal(object())

    def test_a_crash_in_reconciliation_does_not_fail_the_pipeline(self):
        with patch.object(pipeline, "_reconcile_and_heal", side_effect=RuntimeError("boom")):
            assert pipeline._safe_reconcile(object()) is None


class TestWiring:
    def test_reconciliation_runs_after_the_scrapers(self):
        """Rewinding before the scrapers run would be undone by the same run."""
        import inspect
        src = inspect.getsource(pipeline.run_nightly_pipeline)
        assert src.index("for scraper_name, ScraperClass in scrapers:") < src.index("_safe_reconcile(")

    def test_reconciliation_runs_before_scoring(self):
        """Scores should reflect whatever the repair recovered."""
        import inspect
        src = inspect.getsource(pipeline.run_nightly_pipeline)
        assert src.index("_safe_reconcile(") < src.index("_run_scoring()")
