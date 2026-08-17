"""
Guards on alert suppression.

A snooze is the one piece of config whose whole job is to stop you hearing
something. Two ways it goes wrong: it matches more than it was written for, and
it outlives the condition it was written for. The DCWP entry did both. It was
set to quiet a frozen feed and, being a bare substring matched against subject
and body, it also swallowed "Scraper failed: dcwp_licenses" and the quarantine
trip that exists to catch an upstream schema change.
"""

import os
from datetime import date, timedelta
from unittest.mock import patch

import scheduler.alerts as alerts


def _snooze(value):
    return patch.dict(os.environ, {"ALERT_SNOOZE": value})


class TestSnoozeScope:
    def test_a_snooze_silences_what_it_names(self):
        with _snooze("Scraper anomaly: dcwp_licenses"):
            assert alerts._snoozed("Scraper anomaly: dcwp_licenses", "0 new rows")

    def test_a_snooze_does_not_silence_a_hard_failure(self):
        """All retries exhausted is never a known-benign condition."""
        with _snooze("Scraper anomaly: dcwp_licenses"):
            assert not alerts._snoozed(
                "Scraper failed: dcwp_licenses", "All retries exhausted. Error: 500"
            )

    def test_a_snooze_does_not_silence_a_quarantine_spike(self):
        """A quarantine spike is how an upstream schema change announces itself."""
        with _snooze("Scraper anomaly: dcwp_licenses"):
            assert not alerts._snoozed(
                "High quarantine rate: dcwp_licenses",
                "412/800 records quarantined (51.5%) — possible upstream schema change.",
            )

    def test_an_unrelated_scraper_is_never_caught(self):
        with _snooze("Scraper anomaly: dcwp_licenses"):
            assert not alerts._snoozed("Scraper failed: acris_ownership", "boom")

    def test_no_snooze_configured_silences_nothing(self):
        with _snooze(""):
            assert not alerts._snoozed("Scraper anomaly: dcwp_licenses", "0 new rows")


class TestSnoozeExpiry:
    """A snooze with no set-date cannot be reviewed, so it lives forever."""

    def test_a_dated_snooze_still_matches_on_the_pattern_alone(self):
        with _snooze("Scraper anomaly: dcwp_licenses@2026-04-24"):
            assert alerts._snoozed("Scraper anomaly: dcwp_licenses", "0 new rows")

    def test_active_snoozes_reports_pattern_and_date(self):
        with _snooze("Scraper anomaly: dcwp_licenses@2026-04-24"):
            entries = alerts.active_snoozes()
        assert entries == [("Scraper anomaly: dcwp_licenses", date(2026, 4, 24))]

    def test_an_undated_snooze_reports_no_date_rather_than_crashing(self):
        with _snooze("some legacy pattern"):
            entries = alerts.active_snoozes()
        assert entries == [("some legacy pattern", None)]

    def test_an_unparseable_date_is_treated_as_undated(self):
        with _snooze("thing@not-a-date"):
            entries = alerts.active_snoozes()
        assert entries == [("thing@not-a-date", None)]

    def test_multiple_snoozes_split_on_commas(self):
        with _snooze("first: a@2026-01-01, second: b"):
            entries = alerts.active_snoozes()
        assert len(entries) == 2
        assert entries[0][1] == date(2026, 1, 1)
        assert entries[1][1] is None

    def test_stale_snoozes_surface_for_review(self):
        old = (date.today() - timedelta(days=45)).isoformat()
        recent = (date.today() - timedelta(days=3)).isoformat()
        with _snooze(f"old one@{old}, new one@{recent}"):
            stale = alerts.stale_snoozes(max_age_days=30)
        assert [p for p, _, _ in stale] == ["old one"]

    def test_an_undated_snooze_counts_as_stale(self):
        """No date means nobody can say whether it is still needed."""
        with _snooze("mystery pattern"):
            stale = alerts.stale_snoozes(max_age_days=30)
        assert [p for p, _, _ in stale] == ["mystery pattern"]

    def test_nothing_is_stale_when_nothing_is_snoozed(self):
        with _snooze(""):
            assert alerts.stale_snoozes(max_age_days=30) == []
