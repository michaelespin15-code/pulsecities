"""
Unit tests for scripts/pipeline_health.py.

All tests are pure-unit — no DB required.
"""

from datetime import date, datetime, timezone

import pytest

from scripts.pipeline_health import (
    ACRIS_FROZEN_CRITICAL_DAYS,
    HISTORY_DAY_OVER_DAY_PCT,
    LIVE_HISTORY_DRIFT_PCT,
    SCORE_MAX_FLOOR,
    acris_frozen_days,
    build_exit_flags,
    history_day_over_day_ratio,
    score_drift_pct,
    scraper_health_label,
)


# ---------------------------------------------------------------------------
# scraper_health_label
# ---------------------------------------------------------------------------

class TestScraperHealthLabel:
    def test_success_with_records_is_ok(self):
        assert scraper_health_label("success", 300, 200) == "OK"

    def test_success_zero_records_no_min_is_ok(self):
        assert scraper_health_label("success", 0, None) == "OK"

    def test_failure_is_failed(self):
        assert scraper_health_label("failure", 0, 200) == "FAILED"
        assert scraper_health_label("failure", 500, None) == "FAILED"

    def test_warning_zero_records_is_frozen(self):
        assert scraper_health_label("warning", 0, 200) == "FROZEN"

    def test_warning_with_records_is_degraded_if_below_min(self):
        # 10 < 50% of 200 → DEGRADED
        assert scraper_health_label("warning", 10, 200) == "DEGRADED"

    def test_success_below_half_min_is_degraded(self):
        assert scraper_health_label("success", 50, 200) == "DEGRADED"

    def test_success_exactly_half_min_is_ok(self):
        # records == expected * 0.50 exactly → OK (threshold is strict <, not <=)
        assert scraper_health_label("success", 100, 200) == "OK"

    def test_success_one_below_half_min_is_degraded(self):
        assert scraper_health_label("success", 99, 200) == "DEGRADED"

    def test_running_status(self):
        assert scraper_health_label("running", 0, 200) == "RUNNING"


# ---------------------------------------------------------------------------
# acris_frozen_days
# ---------------------------------------------------------------------------

class TestAcrisFrozenDays:
    def test_none_max_date_returns_none(self):
        assert acris_frozen_days(None) is None

    def test_today_returns_zero(self):
        assert acris_frozen_days(date.today()) == 0

    def test_known_staleness(self):
        from datetime import timedelta
        stale_date = date.today() - timedelta(days=27)
        result = acris_frozen_days(stale_date)
        assert result == 27

    def test_one_day_stale(self):
        from datetime import timedelta
        assert acris_frozen_days(date.today() - timedelta(days=1)) == 1


# ---------------------------------------------------------------------------
# score_drift_pct
# ---------------------------------------------------------------------------

class TestScoreDriftPct:
    def test_identical_values_zero_drift(self):
        assert score_drift_pct(31.18, 31.18) == pytest.approx(0.0)

    def test_small_drift(self):
        # |31.5 - 30.0| / 30.0 = 0.05
        assert score_drift_pct(31.5, 30.0) == pytest.approx(0.05)

    def test_large_drift(self):
        # |6.79 - 30.87| / 30.87 ≈ 0.78
        result = score_drift_pct(6.79, 30.87)
        assert result == pytest.approx((30.87 - 6.79) / 30.87)

    def test_zero_history_returns_none(self):
        assert score_drift_pct(30.0, 0.0) is None

    def test_drift_is_absolute(self):
        # drift is symmetric — lower live or higher live same result
        assert score_drift_pct(40.0, 30.0) == pytest.approx(score_drift_pct(20.0, 30.0))


# ---------------------------------------------------------------------------
# history_day_over_day_ratio
# ---------------------------------------------------------------------------

class TestHistoryDayOverDayRatio:
    def test_equal_days_ratio_one(self):
        assert history_day_over_day_ratio(30.0, 30.0) == pytest.approx(1.0)

    def test_half_returns_0_5(self):
        assert history_day_over_day_ratio(15.0, 30.0) == pytest.approx(0.5)

    def test_collapsed(self):
        assert history_day_over_day_ratio(6.79, 30.87) == pytest.approx(6.79 / 30.87)

    def test_none_today_returns_none(self):
        assert history_day_over_day_ratio(None, 30.0) is None

    def test_none_yesterday_returns_none(self):
        assert history_day_over_day_ratio(30.0, None) is None

    def test_zero_yesterday_returns_none(self):
        assert history_day_over_day_ratio(30.0, 0.0) is None


# ---------------------------------------------------------------------------
# build_exit_flags
# ---------------------------------------------------------------------------

def _healthy_scrapers():
    return [
        {"name": s, "status": "success"} for s in
        ["acris_ownership", "dob_permits", "311_complaints", "evictions", "hpd_violations"]
    ]


class TestBuildExitFlags:
    def test_all_healthy_no_flags(self):
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=5,
            scores_max=72.0,
            drift=0.01,
            dod_ratio=0.99,
        )
        assert flags == []

    def test_failure_on_key_scraper_triggers_flag(self):
        scrapers = _healthy_scrapers()
        scrapers[0] = {"name": "acris_ownership", "status": "failure"}
        flags = build_exit_flags(
            scraper_rows=scrapers,
            acris_days=5,
            scores_max=72.0,
            drift=0.01,
            dod_ratio=0.99,
        )
        assert any("failure" in f for f in flags)

    def test_acris_frozen_above_threshold_triggers_flag(self):
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=ACRIS_FROZEN_CRITICAL_DAYS + 1,
            scores_max=72.0,
            drift=0.01,
            dod_ratio=0.99,
        )
        assert any("frozen" in f.lower() for f in flags)

    def test_acris_frozen_at_threshold_no_flag(self):
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=ACRIS_FROZEN_CRITICAL_DAYS,
            scores_max=72.0,
            drift=0.01,
            dod_ratio=0.99,
        )
        assert not any("frozen" in f.lower() for f in flags)

    def test_score_max_below_floor_triggers_flag(self):
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=5,
            scores_max=SCORE_MAX_FLOOR - 0.1,
            drift=0.01,
            dod_ratio=0.99,
        )
        assert any("max=" in f or "floor" in f for f in flags)

    def test_score_max_above_floor_no_flag(self):
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=5,
            scores_max=SCORE_MAX_FLOOR + 0.1,
            drift=0.01,
            dod_ratio=0.99,
        )
        assert not any("max=" in f or "floor" in f for f in flags)

    def test_large_drift_triggers_flag(self):
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=5,
            scores_max=72.0,
            drift=LIVE_HISTORY_DRIFT_PCT + 0.01,
            dod_ratio=0.99,
        )
        assert any("drift" in f.lower() for f in flags)

    def test_drift_at_threshold_no_flag(self):
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=5,
            scores_max=72.0,
            drift=LIVE_HISTORY_DRIFT_PCT,
            dod_ratio=0.99,
        )
        assert not any("drift" in f.lower() for f in flags)

    def test_dod_collapse_triggers_flag(self):
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=5,
            scores_max=72.0,
            drift=0.01,
            dod_ratio=HISTORY_DAY_OVER_DAY_PCT - 0.01,
        )
        assert any("day" in f.lower() or "ratio" in f.lower() for f in flags)

    def test_dod_at_threshold_no_flag(self):
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=5,
            scores_max=72.0,
            drift=0.01,
            dod_ratio=HISTORY_DAY_OVER_DAY_PCT,
        )
        assert not any("day" in f.lower() or "ratio" in f.lower() for f in flags)

    def test_none_drift_and_dod_no_flag(self):
        """None values (no history rows) must not trigger numeric thresholds."""
        flags = build_exit_flags(
            scraper_rows=_healthy_scrapers(),
            acris_days=None,
            scores_max=None,
            drift=None,
            dod_ratio=None,
        )
        assert flags == []

    def test_non_key_scraper_failure_does_not_flag(self):
        """Auxiliary scrapers (mappluto, dof_assessments) must not cause exit."""
        scrapers = _healthy_scrapers()
        scrapers.append({"name": "mappluto", "status": "failure"})
        flags = build_exit_flags(
            scraper_rows=scrapers,
            acris_days=5,
            scores_max=72.0,
            drift=0.01,
            dod_ratio=0.99,
        )
        assert flags == []

    def test_dob_degraded_count_does_not_flag(self):
        """DOB with 2 records (post-bulk-recovery) must not trigger failure flag."""
        scrapers = _healthy_scrapers()
        # DOB with 2 records is 'success' status — the rolling-avg warning is
        # a scraper warning_message, not a pipeline exit condition.
        flags = build_exit_flags(
            scraper_rows=scrapers,
            acris_days=5,
            scores_max=72.0,
            drift=0.01,
            dod_ratio=0.99,
        )
        assert flags == []

    def test_multiple_flags_all_reported(self):
        scrapers = [{"name": "acris_ownership", "status": "failure"}]
        scrapers += [{"name": s, "status": "success"} for s in
                     ["dob_permits", "311_complaints", "evictions", "hpd_violations"]]
        flags = build_exit_flags(
            scraper_rows=scrapers,
            acris_days=ACRIS_FROZEN_CRITICAL_DAYS + 5,
            scores_max=SCORE_MAX_FLOOR - 5,
            drift=LIVE_HISTORY_DRIFT_PCT + 0.1,
            dod_ratio=HISTORY_DAY_OVER_DAY_PCT - 0.1,
        )
        assert len(flags) >= 4
