"""
Guards on the reconciler.

This is the only check that can find an ingestion bug nobody has thought of yet,
so its own failure modes matter more than most. The two that would make it lie:
reporting a failed probe as "the source has nothing" (which reads as a surplus,
not a gap), and judging a day the source has not finished publishing.

No network, no database writes.
"""

from datetime import date, datetime, timedelta, timezone

import scripts.reconcile_upstream as rec


def _feed(**kw):
    base = dict(scraper_name="311_complaints", dataset_id="erm2-nwe9",
                upstream_field="created_date", table="complaints_raw",
                column="created_date", settle_days=3)
    base.update(kw)
    return rec.Feed(**base)


class TestUpstreamQuery:
    def test_range_form_is_half_open(self):
        """A closed upper bound drops everything after 23:59:59 on the day."""
        where = _feed().upstream_where(date(2026, 8, 14))
        assert "created_date>='2026-08-14T00:00:00'" in where
        assert "created_date<'2026-08-15T00:00:00'" in where

    def test_text_dates_match_exactly(self):
        """permits stores filing_date as MM/DD/YYYY text, so a range would sort
        lexicographically. Equality on the formatted day is correct there."""
        where = _feed(upstream_is_text=True, upstream_field="filing_date").upstream_where(
            date(2026, 8, 14)
        )
        assert where == "filing_date='08/14/2026'"


class TestProbeFailures:
    def test_a_failed_probe_is_none_not_zero(self, monkeypatch):
        """Zero would read as 'the source has no rows', making our copy look
        like a surplus and hiding a real gap."""
        def _boom(url, params=None, timeout=None):
            raise RuntimeError("socrata down")
        monkeypatch.setattr(rec.requests, "get", _boom)
        assert rec.upstream_count(_feed(), date(2026, 8, 14)) is None

    def test_a_failed_probe_does_not_count_as_drift(self, monkeypatch):
        def _boom(url, params=None, timeout=None):
            raise RuntimeError("socrata down")
        monkeypatch.setattr(rec.requests, "get", _boom)

        class _DB:
            def execute(self, *a, **k):
                raise AssertionError("must not query locally when the probe failed")

        res = rec.reconcile_feed(_DB(), _feed(), window_days=2)
        assert all(r["drifted"] is False for r in res["rows"])
        assert rec.earliest_drift(res) is None


class TestDriftClassification:
    def _reconciled(self, monkeypatch, upstream, ours, window=1):
        class _Resp:
            @staticmethod
            def raise_for_status(): return None
            @staticmethod
            def json(): return [{"n": str(upstream)}]

        monkeypatch.setattr(rec.requests, "get",
                            lambda url, params=None, timeout=None: _Resp())

        class _DB:
            def execute(self, *a, **k):
                class _R:
                    @staticmethod
                    def scalar(): return ours
                return _R()

        return rec.reconcile_feed(_DB(), _feed(), window_days=window)

    def test_a_shortfall_past_the_threshold_is_drift(self, monkeypatch):
        res = self._reconciled(monkeypatch, upstream=10_000, ours=9_800)
        assert res["rows"][0]["drifted"] is True
        assert res["rows"][0]["gap"] == 200

    def test_an_exact_match_is_not_drift(self, monkeypatch):
        res = self._reconciled(monkeypatch, upstream=10_000, ours=10_000)
        assert res["rows"][0]["drifted"] is False

    def test_noise_below_the_threshold_is_not_drift(self, monkeypatch):
        res = self._reconciled(monkeypatch, upstream=10_000, ours=9_990)  # 0.1%
        assert res["rows"][0]["drifted"] is False

    def test_holding_more_than_the_source_is_not_drift(self, monkeypatch):
        """The source retracting rows is not an ingestion defect, and must not
        trigger a rewind that would re-read the range forever."""
        res = self._reconciled(monkeypatch, upstream=9_000, ours=10_000)
        assert res["rows"][0]["drifted"] is False

    def test_the_threshold_is_tight_enough_for_the_311_case(self):
        """311 sat at 1.2% short while genuinely losing rows every day."""
        assert rec.DRIFT_ALERT_PCT < 0.012

    def test_unsettled_days_are_never_judged(self, monkeypatch):
        """Feeds publish on a lag, so the newest days are short by design."""
        res = self._reconciled(monkeypatch, upstream=10_000, ours=9_000, window=5)
        newest = max(r["day"] for r in res["rows"])
        assert newest <= date.today() - timedelta(days=_feed().settle_days)


class TestRewind:
    class _FakeDB:
        def __init__(self, current):
            self.current = current
            self.written = None
            self.committed = False

        def execute(self, stmt, params=None):
            sql = str(stmt)
            if sql.strip().upper().startswith("UPDATE"):
                self.written = params["wm"]
                class _R: rowcount = 1
                return _R()
            outer = self
            class _R:
                @staticmethod
                def fetchone():
                    if outer.current is None:
                        return None
                    class _Row:
                        id = 1
                        watermark_timestamp = outer.current
                    return _Row()
            return _R()

        def commit(self):
            self.committed = True

    def test_rewind_moves_the_watermark_back(self):
        db = self._FakeDB(datetime(2026, 8, 15, tzinfo=timezone.utc))
        out = rec.rewind_watermark(db, "311_complaints", date(2026, 8, 4))
        assert out == datetime(2026, 8, 4, tzinfo=timezone.utc)
        assert db.written == out
        assert db.committed

    def test_rewind_never_moves_the_watermark_forward(self):
        """Healing must only ever widen what the next run reads."""
        db = self._FakeDB(datetime(2026, 8, 1, tzinfo=timezone.utc))
        assert rec.rewind_watermark(db, "311_complaints", date(2026, 8, 10)) is None
        assert db.written is None

    def test_rewind_is_capped(self):
        """A bad reconciliation must not order a re-scan of the whole dataset."""
        db = self._FakeDB(datetime.now(timezone.utc))
        out = rec.rewind_watermark(db, "311_complaints", date(2000, 1, 1))
        floor = datetime.now(timezone.utc) - timedelta(days=rec.MAX_REWIND_DAYS + 1)
        assert out is not None and out > floor

    def test_a_scraper_with_no_successful_run_is_left_alone(self):
        db = self._FakeDB(None)
        assert rec.rewind_watermark(db, "311_complaints", date(2026, 8, 4)) is None


class TestFeedCoverage:
    def test_every_daily_feed_is_reconciled(self):
        names = {f.scraper_name for f in rec.FEEDS}
        assert {"311_complaints", "hpd_violations", "evictions", "dob_permits"} <= names

    def test_every_feed_declares_a_settle_window(self):
        for f in rec.FEEDS:
            assert f.settle_days >= 1, f"{f.scraper_name} would judge unpublished days"


class TestHealingDoesNotCryWolf:
    """The first heal run emailed a drift warning about the 2,224 rows it had
    just recovered. An alert channel survives on being right."""

    def test_alerting_is_driven_by_the_post_heal_measurement(self):
        import inspect
        src = inspect.getsource(rec.main)
        heal_at = src.index("_run_scrapers(")
        alert_at = src.index("send_alert(")
        remeasure_at = src.index("reconcile_feed(db, r[\"feed\"]")
        assert heal_at < remeasure_at < alert_at, (
            "main() must re-measure between healing and alerting"
        )

    def test_the_remeasure_reassigns_what_alerting_reads(self):
        import inspect
        src = inspect.getsource(rec.main)
        tail = src[src.index("_run_scrapers("):src.index("send_alert(")]
        assert "drifted_feeds = [r for r in results if earliest_drift(r)]" in tail
