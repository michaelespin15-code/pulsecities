"""
Rows we imported are not rows that happened.

The daily building alert and the monthly block digest window on ingest time
rather than event date, deliberately: ACRIS publishes deeds a median of 47 days
after they are signed and HPD releases violations 234 days late at the 90th
percentile, so a watcher keyed on event dates would never hear about the deed on
their own building.

That design cannot tell a source publishing late from us importing history. On
2026-08-28 the DOB NOW backfill loaded 485,443 permits going back to 2021 and
the 03:25 alert run emailed a real subscriber "New at 1062 Elton Street: 5 new
records", listing five permits filed in 2023 and 2024. The dates on them were
correct. The word "new" was not.

An age ceiling was measured as the fix and rejected: any cutoff tight enough to
catch a 2023 permit also drops the deeds and violations that are genuinely news.
BaseScraper already marks a historical walk with status 'backfill', so the run's
own window names exactly the rows to skip.
"""

from datetime import datetime, timedelta, timezone

import pytest

from scripts.lib import backfill_windows as bw


class _Row:
    def __init__(self, started_at, ended_at):
        self.started_at, self.ended_at = started_at, ended_at


class _Db:
    """Returns canned scraper_runs rows, so this needs no database."""
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def execute(self, _sql, params=None):
        self.params = params
        return self

    def fetchall(self):
        return self.rows


NOW = datetime(2026, 8, 28, 2, 0, tzinfo=timezone.utc)


class TestRanges:
    def test_a_table_nobody_backfilled_yields_nothing(self):
        assert bw.backfill_ranges(_Db([]), "permits_raw") == []

    def test_an_unknown_table_does_not_query_at_all(self):
        db = _Db([_Row(NOW, NOW)])
        assert bw.backfill_ranges(db, "not_a_table") == []
        assert db.params is None

    def test_the_window_is_padded_on_both_sides(self):
        """created_at is stamped by the database inside the run; the pad covers
        skew between that clock and the one writing started_at."""
        start, end = bw.backfill_ranges(_Db([_Row(NOW, NOW + timedelta(minutes=20))]),
                                        "permits_raw")[0]
        assert start == NOW - bw.PAD
        assert end == NOW + timedelta(minutes=20) + bw.PAD

    def test_it_asks_for_every_scraper_that_fills_the_table(self):
        """permits_raw has two, and a backfill of either puts history in it."""
        db = _Db([])
        bw.backfill_ranges(db, "permits_raw")
        assert set(db.params["names"]) == {"dob_permits", "dob_now_permits"}

    def test_every_raw_table_an_alert_reads_is_covered(self):
        """A table missing here silently opts out of the protection."""
        for table in ("permits_raw", "ownership_raw", "violations_raw",
                      "complaints_raw", "evictions_raw"):
            assert table in bw.TABLE_SCRAPERS


class TestExclusion:
    def test_no_backfill_means_no_sql_and_no_params(self):
        """Callers concatenate unconditionally, so the quiet case must be empty
        rather than something like '1=1'."""
        assert bw.exclusion(_Db([]), "permits_raw") == ("", {})

    def test_the_clause_excludes_rather_than_includes(self):
        sql, _ = bw.exclusion(_Db([_Row(NOW, NOW)]), "permits_raw")
        assert "AND NOT (" in sql

    def test_the_alias_is_applied(self):
        sql, _ = bw.exclusion(_Db([_Row(NOW, NOW)]), "permits_raw", "pr")
        assert "pr.created_at" in sql

    def test_timestamps_are_bound_not_interpolated(self):
        sql, params = bw.exclusion(_Db([_Row(NOW, NOW)]), "permits_raw")
        assert "2026" not in sql
        assert set(params) == {"bf0_s", "bf0_e"}

    def test_several_backfills_each_get_their_own_clause(self):
        rows = [_Row(NOW, NOW), _Row(NOW - timedelta(days=30), NOW - timedelta(days=30))]
        sql, params = bw.exclusion(_Db(rows), "permits_raw")
        assert sql.count("AND NOT (") == 2
        assert len(params) == 4

    def test_the_prefix_keeps_two_callers_from_colliding(self):
        """One query can carry exclusions for more than one table."""
        _, a = bw.exclusion(_Db([_Row(NOW, NOW)]), "permits_raw", param_prefix="p")
        _, b = bw.exclusion(_Db([_Row(NOW, NOW)]), "violations_raw", param_prefix="v")
        assert not set(a) & set(b)


class TestBothSendersUseIt:
    @pytest.mark.parametrize("module", ["scripts.building_alerts", "scripts.block_digest"])
    def test_the_ingest_windowed_senders_import_it(self, module):
        """These two are the reason it exists. A third sender windowing on
        created_at needs it too."""
        import importlib
        assert hasattr(importlib.import_module(module), "exclusion")

    @pytest.mark.parametrize("module", ["scripts.building_alerts", "scripts.block_digest"])
    def test_every_kind_maps_to_a_table(self, module):
        import importlib
        m = importlib.import_module(module)
        assert set(m._EVENT_SQL) == set(m._TABLE_OF_KIND), (
            "an event kind with no table cannot be protected")
        for table in m._TABLE_OF_KIND.values():
            assert table in bw.TABLE_SCRAPERS
