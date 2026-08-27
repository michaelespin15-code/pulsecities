"""
Guards on the one thing every freshness surface has to agree about.

Three separate bugs shipped here because the rule for "how stale is too stale"
was written down four times: the public page said ok, pipeline_health said
CRITICAL, and daily_health_check emailed a stale alert, all for the same feed on
the same night. Two more came from a probe that read a date column the wrong way
and a query that trusted a filer-typed date from the future.

These tests fail if any of that drifts back. None of them write to the database.
"""

import api.freshness as freshness
import scripts.daily_health_check as dhc
import scripts.pipeline_health as ph
from api.routes.status import _FRESHNESS as PUBLIC_FRESHNESS


class TestSingleSourceOfTruth:
    def test_acris_threshold_is_defined_once(self):
        """Every surface that ages ACRIS reads the same number.

        Deeds publish on a ~2-week lag, so a 7-day threshold fires on a healthy
        feed. A flag that is usually on says nothing when the feed really stops.
        """
        canonical = freshness.staleness_days("acris_ownership")
        assert canonical == 21

        assert ph.ACRIS_FROZEN_CRITICAL_DAYS == canonical
        assert PUBLIC_FRESHNESS["acris_ownership"].days == canonical

    def test_health_check_carries_no_thresholds_of_its_own(self):
        """The nightly job reads thresholds, it does not declare them."""
        for check in dhc.FRESHNESS_CHECKS:
            assert len(check) == 3, (
                "FRESHNESS_CHECKS grew a local threshold or table column again. "
                "Those belong in api/freshness.py."
            )

    def test_every_checked_scraper_resolves_a_threshold(self):
        for scraper_name, _dataset, _date_col in dhc.FRESHNESS_CHECKS:
            assert freshness.staleness_days(scraper_name) > 0

    def test_staleness_lookup_accepts_both_naming_schemes(self):
        """The API keys feeds by slug, the schedulers key them by scraper name."""
        assert freshness.staleness_days("acris") == freshness.staleness_days("acris_ownership")
        assert freshness.staleness_days("permits") == freshness.staleness_days("dob_permits")

    def test_unknown_feed_is_an_error_not_a_silent_default(self):
        """A typo'd feed name must not quietly inherit a permissive default."""
        try:
            freshness.staleness_days("no_such_feed")
        except KeyError:
            return
        raise AssertionError("staleness_days() swallowed an unknown feed name")


class TestFutureDatesNeverCountAsFresh:
    def test_through_sql_bounds_on_today(self):
        sql = freshness.through_sql("ownership_raw", "doc_date")
        assert "CURRENT_DATE" in sql

    def test_the_bound_keeps_the_column_sargable(self):
        """A `column::date` cast is also correct and forces a seq scan on the
        5M-row tables. Keep the column bare on the left of the comparison."""
        sql = freshness.through_sql("complaints_raw", "created_date")
        assert "created_date::date" not in sql
        assert "CAST(created_date" not in sql

    def test_the_bound_does_not_drop_records_filed_today(self):
        """CURRENT_DATE widens to midnight, so `col <= CURRENT_DATE` discards
        every timestamp row filed so far today and understates freshness."""
        sql = freshness.through_sql("complaints_raw", "created_date")
        assert "<= CURRENT_DATE" not in sql
        assert "CURRENT_DATE + INTERVAL '1 day'" in sql

    def test_every_db_freshness_query_carries_the_guard(self):
        """Two ACRIS rows dated in the future drove db_stale_days to -10, which
        no threshold can ever exceed. The check was green by construction."""
        for scraper_name, _dataset, _date_col in dhc.FRESHNESS_CHECKS:
            sql = freshness.db_through_sql(scraper_name)
            assert "CURRENT_DATE" in sql, f"{scraper_name} would trust a future date"

    def test_a_mistyped_date_stays_excluded_once_the_calendar_reaches_it(self):
        """The future bound expires. The same two ACRIS rows were typed sixteen
        days ahead, and the morning the calendar caught up they became the
        newest legal deed date in the table: pipeline_health went CRITICAL to
        HEALTHY and /api/status advertised a feed frozen since July as current.
        A record cannot have happened after we wrote it down, and that bound
        does not expire."""
        for scraper_name, _dataset, _date_col in dhc.FRESHNESS_CHECKS:
            sql = freshness.db_through_sql(scraper_name)
            assert "<= created_at" in sql, (
                f"{scraper_name} trusts a typed date once it stops being in the future"
            )


class TestUpstreamProbes:
    """The DOB permits probe reported a healthy feed as 1,974 days stale for ten
    nights. It ordered by a text column, which sorts lexicographically, to work
    around MAX() sorting lexicographically on a text column."""

    def _captured_params(self, monkeypatch, dataset_id, date_col):
        seen = {}

        class _Resp:
            @staticmethod
            def raise_for_status():
                return None

            @staticmethod
            def json():
                return [{"max_dt": "2026-08-16T00:00:00.000"}]

        def _fake_get(url, params=None, timeout=None):
            seen["url"] = url
            seen["params"] = params or {}
            return _Resp()

        monkeypatch.setattr(dhc.requests, "get", _fake_get)
        result = dhc.fetch_upstream_max(dataset_id, date_col)
        return seen, result

    def test_permits_probe_uses_a_real_aggregate(self, monkeypatch):
        seen, result = self._captured_params(monkeypatch, "ipu4-2q9a", "dobrundate")

        assert "MAX(dobrundate)" in seen["params"].get("$select", "")
        assert "$order" not in seen["params"], (
            "Ordering by a text date column reintroduces the lexicographic bug."
        )
        assert result is not None

    def test_no_dataset_gets_a_bespoke_probe(self):
        """The special case rested on a false premise: dobrundate is a
        calendar_date, not text, so the standard path always worked."""
        assert not hasattr(dhc, "DOBRUNDATE_DATASET")

    def test_probe_failure_returns_none_rather_than_a_wrong_date(self, monkeypatch):
        def _boom(url, params=None, timeout=None):
            raise RuntimeError("socrata is down")

        monkeypatch.setattr(dhc.requests, "get", _boom)
        assert dhc.fetch_upstream_max("erm2-nwe9", "created_date") is None


class TestClassification:
    def test_a_feed_inside_its_threshold_is_ok(self):
        assert dhc.classify_status(3, 3, 10) == "ok"

    def test_either_side_past_threshold_is_stale(self):
        assert dhc.classify_status(40, 2, 21) == "stale"
        assert dhc.classify_status(2, 40, 21) == "stale"

    def test_unknown_reads_as_warn_not_ok(self):
        assert dhc.classify_status(None, 3, 10) == "warn"

    def test_acris_at_seventeen_days_is_not_an_alert(self):
        """The exact case that emailed a false alert every night."""
        acris = freshness.staleness_days("acris_ownership")
        assert dhc.classify_status(17, 17, acris) == "ok"
