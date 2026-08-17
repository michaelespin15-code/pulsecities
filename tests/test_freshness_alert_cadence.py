"""
Guards on how often a stale feed is allowed to email, and about what.

A stale feed is two different findings wearing the same subject line. When the
publisher stops, the finding is true on night one and identical on night thirty,
and no amount of paging changes it. When the publisher is current and we are
behind, that is our bug. Sending both nightly under one subject is how a channel
stops being read, which is the failure that hid the 2026-08-15 outage.

These tests pin the split and the cadence. None of them touch the database or
the network.
"""

from datetime import date

import pytest

import scripts.daily_health_check as dhc


class TestStaleCause:
    def test_frozen_publisher_is_not_our_bug(self):
        """Upstream past threshold, and we match it. Nothing here to fix."""
        assert dhc.stale_cause(30, 30, 21) == "upstream"

    def test_falling_behind_a_live_source_is_our_bug(self):
        assert dhc.stale_cause(2, 40, 21) == "ingest"

    def test_upstream_wins_when_both_are_stale(self):
        """Our lag is a consequence of theirs, not a second independent finding."""
        assert dhc.stale_cause(40, 45, 21) == "upstream"

    def test_healthy_feed_has_no_cause(self):
        assert dhc.stale_cause(3, 3, 10) is None

    def test_unknown_probe_is_not_reported_as_a_gap(self):
        """A failed probe reads as None, and None is not evidence of staleness."""
        assert dhc.stale_cause(None, 3, 10) is None
        assert dhc.stale_cause(3, None, 10) is None


class TestAlertCadence:
    def test_first_sighting_always_alerts(self):
        fires, entry = dhc._should_alert("acris_ownership", "upstream", date(2026, 8, 21), {})
        assert fires
        assert entry["first_seen"] == "2026-08-21"

    def test_frozen_publisher_is_not_re_sent_the_next_night(self):
        state = {}
        day_one = date(2026, 8, 21)
        fires, state["acris_ownership"] = dhc._should_alert(
            "acris_ownership", "upstream", day_one, state)
        assert fires

        fires, _ = dhc._should_alert(
            "acris_ownership", "upstream", date(2026, 8, 22), state)
        assert not fires, "a frozen upstream re-paged the night after it was reported"

    def test_frozen_publisher_is_re_reported_on_the_cadence(self):
        state = {"acris_ownership": {
            "cause": "upstream",
            "first_seen": "2026-08-21",
            "last_alerted": "2026-08-21",
        }}
        due = date(2026, 8, 21 + dhc.UPSTREAM_REALERT_DAYS)
        fires, entry = dhc._should_alert("acris_ownership", "upstream", due, state)
        assert fires
        assert entry["first_seen"] == "2026-08-21", "age reset on the re-report"

    def test_our_own_gap_stays_loud_every_run(self):
        state = {"dob_permits": {
            "cause": "ingest",
            "first_seen": "2026-08-21",
            "last_alerted": "2026-08-21",
        }}
        fires, _ = dhc._should_alert("dob_permits", "ingest", date(2026, 8, 22), state)
        assert fires, "an ingest gap went quiet; that is the alert worth waking for"

    def test_cause_change_restarts_the_clock(self):
        """A frozen source that thaws while we stay behind is a new finding."""
        state = {"acris_ownership": {
            "cause": "upstream",
            "first_seen": "2026-08-01",
            "last_alerted": "2026-08-21",
        }}
        fires, entry = dhc._should_alert(
            "acris_ownership", "ingest", date(2026, 8, 22), state)
        assert fires
        assert entry["first_seen"] == "2026-08-22"

    def test_unparseable_timestamp_alerts_rather_than_stays_silent(self):
        state = {"acris_ownership": {
            "cause": "upstream",
            "first_seen": "2026-08-21",
            "last_alerted": "not-a-date",
        }}
        fires, _ = dhc._should_alert(
            "acris_ownership", "upstream", date(2026, 8, 22), state)
        assert fires


class TestAlertText:
    def _build(self, cause, **over):
        args = dict(
            scraper_name="acris_ownership",
            cause=cause,
            upstream_date=date(2026, 7, 31),
            db_date=date(2026, 7, 31),
            up_days=30,
            db_days=30,
            threshold=21,
            entry={"first_seen": "2026-08-21"},
            today=date(2026, 8, 28),
        )
        args.update(over)
        return dhc._stale_alert(**args)

    def test_subjects_name_which_failure_it_is(self):
        upstream_subject, _ = self._build("upstream")
        ingest_subject, _ = self._build("ingest", up_days=2, db_days=40)
        assert upstream_subject != ingest_subject
        assert "frozen" in upstream_subject.lower()
        assert "behind" in ingest_subject.lower()

    def test_frozen_alert_says_there_is_nothing_to_fix_here(self):
        _, body = self._build("upstream")
        assert "nothing to fix" in body

    def test_frozen_alert_carries_its_age(self):
        _, body = self._build("upstream")
        assert "7d" in body, "a repeat with no age reads as news instead of a status"

    def test_ingest_alert_puts_the_gap_on_us(self):
        _, body = self._build("ingest", up_days=2, db_days=40)
        assert "this gap is ours" in body


class TestStateFile:
    def test_unreadable_state_does_not_stop_the_checks(self, tmp_path, monkeypatch):
        """Losing the file re-sends one alert. Raising would skip every check."""
        corrupt = tmp_path / "freshness_alert_state.json"
        corrupt.write_text("{not json")
        monkeypatch.setattr(dhc, "STATE_PATH", corrupt)
        assert dhc._read_alert_state() == {}

    def test_missing_state_is_empty_not_an_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(dhc, "STATE_PATH", tmp_path / "absent.json")
        assert dhc._read_alert_state() == {}

    def test_write_then_read_round_trips(self, tmp_path, monkeypatch):
        path = tmp_path / "freshness_alert_state.json"
        monkeypatch.setattr(dhc, "STATE_PATH", path)
        dhc._write_alert_state({"acris_ownership": {"cause": "upstream"}})
        assert dhc._read_alert_state()["acris_ownership"]["cause"] == "upstream"

    def test_write_failure_is_swallowed(self, tmp_path, monkeypatch):
        """Bookkeeping must never take down the job that produces the alerts."""
        monkeypatch.setattr(dhc, "STATE_PATH", tmp_path / "nope" / "state.json")
        dhc._write_alert_state({"a": {"cause": "upstream"}})  # must not raise


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
