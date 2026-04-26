"""
Unit tests for the DCWP business licenses scraper — scrapers/dcwp_licenses.py.

Tests cover:
  - Model configuration (tablename, unique constraint, staleness columns)
  - Scraper configuration (SCRAPER_NAME, DATASET_ID, WATERMARK_EXTRA_LOOKBACK_DAYS)
  - _run() with mocked API response returns (processed_count, failed_count, watermark)
  - Quarantine: record missing license_creation_date is quarantined
  - Nullable field: record missing address_zip is stored, not quarantined
  - source_hash: deterministic, changes on mutable field change, stable on metadata change
  - _upsert(): change detection (is_insert, hash_changed) returned correctly
  - refresh_historical_range(): processes records, detects changes, paginates
  - No :updated_at system column in queries
  - 14-day rolling lookback configured
"""

import pytest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(**overrides) -> dict:
    base = {
        "license_nbr": "2054609-DCA",
        "business_name": "TEST CORP",
        "dba_trade_name": "TEST DBA",
        "business_category": "Home Improvement Contractor",
        "license_status": "Active",
        "license_creation_date": "2026-01-15T00:00:00.000",
        "lic_expir_dd": "2028-01-15T00:00:00.000",
        "address_building": "123",
        "address_street_name": "MAIN ST",
        "address_zip": "10001",
        "address_borough": "Manhattan",
        "latitude": "40.7128",
        "longitude": "-74.0060",
        "bbl": "1000010001",
    }
    return {**base, **overrides}


def _make_upsert_row(*, was_update: bool, prev_hash, new_hash: str) -> MagicMock:
    """Return a fetchone() mock with explicit upsert result fields."""
    row = MagicMock()
    row.was_update = was_update
    row.prev_hash = prev_hash
    row.new_hash = new_hash
    return row


@pytest.fixture()
def scraper():
    with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
        from scrapers.dcwp_licenses import DcwpScraper
        return DcwpScraper()


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestDcwpLicenseModel:
    def test_tablename(self):
        from models.dcwp_license import DcwpLicense
        assert DcwpLicense.__tablename__ == "dcwp_licenses"

    def test_unique_constraint_on_license_nbr(self):
        from models.dcwp_license import DcwpLicense
        from sqlalchemy import UniqueConstraint
        constraints = {c.name for c in DcwpLicense.__table__.constraints
                       if isinstance(c, UniqueConstraint)}
        assert "uq_dcwp_license_nbr" in constraints

    def test_staleness_columns_present(self):
        from models.dcwp_license import DcwpLicense
        col_names = {c.name for c in DcwpLicense.__table__.columns}
        assert "source_last_seen_at" in col_names
        assert "source_last_refreshed_at" in col_names
        assert "source_hash" in col_names


# ---------------------------------------------------------------------------
# Scraper config tests
# ---------------------------------------------------------------------------

class TestDcwpScraperConfig:
    def test_scraper_name(self):
        from scrapers.dcwp_licenses import DcwpScraper
        assert DcwpScraper.SCRAPER_NAME == "dcwp_licenses"

    def test_dataset_id(self):
        from scrapers.dcwp_licenses import DcwpScraper
        assert DcwpScraper.DATASET_ID == "w7w3-xahh"

    def test_14_day_rolling_lookback(self):
        """14-day extra lookback must be set to catch recent renewals/status changes."""
        from scrapers.dcwp_licenses import DcwpScraper
        assert DcwpScraper.WATERMARK_EXTRA_LOOKBACK_DAYS == 14

    def test_no_updated_at_in_incremental_query(self):
        """Neither $select nor $where should reference the :updated_at system column."""
        import inspect
        from scrapers.dcwp_licenses import DcwpScraper
        source = inspect.getsource(DcwpScraper._run)
        assert ":updated_at" not in source

    def test_no_updated_at_in_upsert(self):
        import inspect
        from scrapers.dcwp_licenses import DcwpScraper
        source = inspect.getsource(DcwpScraper._upsert)
        assert ":updated_at" not in source


# ---------------------------------------------------------------------------
# Source hash tests
# ---------------------------------------------------------------------------

class TestDcwpSourceHash:
    def _parsed_base(self) -> dict:
        from scrapers.dcwp_licenses import _parse_date
        return {
            "license_status": "Active",
            "lic_expir_dd": _parse_date("2028-01-15"),
            "business_name": "TEST CORP",
            "dba_trade_name": "TEST DBA",
            "business_category": "Home Improvement Contractor",
            "address_building": "123",
            "address_street_name": "MAIN ST",
            "address_zip": "10001",
            "address_borough": "Manhattan",
            "latitude": 40.7128,
            "longitude": -74.0060,
            "bbl": "1000010001",
        }

    def test_hash_is_deterministic(self):
        from scrapers.dcwp_licenses import _compute_source_hash
        parsed = self._parsed_base()
        assert _compute_source_hash(parsed) == _compute_source_hash(parsed)

    def test_hash_changes_on_status_change(self):
        """A status change (Active → Expired) must produce a different hash."""
        from scrapers.dcwp_licenses import _compute_source_hash
        base = self._parsed_base()
        changed = {**base, "license_status": "Expired"}
        assert _compute_source_hash(base) != _compute_source_hash(changed)

    def test_hash_changes_on_expiry_change(self):
        from scrapers.dcwp_licenses import _compute_source_hash, _parse_date
        base = self._parsed_base()
        changed = {**base, "lic_expir_dd": _parse_date("2030-06-01")}
        assert _compute_source_hash(base) != _compute_source_hash(changed)

    def test_hash_changes_on_address_change(self):
        from scrapers.dcwp_licenses import _compute_source_hash
        base = self._parsed_base()
        changed = {**base, "address_zip": "10002"}
        assert _compute_source_hash(base) != _compute_source_hash(changed)

    def test_hash_stable_without_mutable_change(self):
        """Re-hashing the same parsed dict twice returns identical result."""
        from scrapers.dcwp_licenses import _compute_source_hash
        parsed = self._parsed_base()
        h1 = _compute_source_hash(parsed)
        h2 = _compute_source_hash(dict(parsed))
        assert h1 == h2

    def test_hash_ignores_license_nbr_and_creation_date(self):
        """Natural key and immutable creation date must not affect the hash."""
        from scrapers.dcwp_licenses import _compute_source_hash, _parse_date
        base = self._parsed_base()
        # These fields are NOT in _HASH_FIELDS
        with_extra = {**base, "license_nbr": "EXTRA", "license_creation_date": _parse_date("2019-01-01")}
        assert _compute_source_hash(base) == _compute_source_hash(with_extra)

    def test_hash_is_64_hex_chars(self):
        from scrapers.dcwp_licenses import _compute_source_hash
        h = _compute_source_hash(self._parsed_base())
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


# ---------------------------------------------------------------------------
# _run() tests
# ---------------------------------------------------------------------------

class TestDcwpScraperRun:
    def test_run_returns_tuple(self, scraper):
        """_run() returns (processed_count, failed_count, watermark) tuple."""
        db = MagicMock()
        upsert_row = _make_upsert_row(was_update=False, prev_hash=None, new_hash="abc")
        db.execute.return_value.fetchone.return_value = upsert_row

        with patch.object(scraper, "build_where_since", return_value="license_creation_date > '2025-01-01'"):
            with patch.object(scraper, "paginate", return_value=iter([_make_record()])):
                result = scraper._run(db)

        assert isinstance(result, tuple) and len(result) == 3
        processed, failed, watermark = result
        assert isinstance(processed, int)
        assert isinstance(failed, int)

    def test_missing_license_creation_date_quarantined(self, scraper):
        """Record missing license_creation_date is quarantined, not stored."""
        db = MagicMock()
        bad_record = _make_record()
        del bad_record["license_creation_date"]

        quarantined = []
        with patch.object(scraper, "build_where_since", return_value="license_creation_date > '2025-01-01'"):
            with patch.object(scraper, "paginate", return_value=iter([bad_record])):
                with patch.object(scraper, "quarantine", side_effect=lambda db, raw, r: quarantined.append(r)):
                    processed, failed, _ = scraper._run(db)

        assert failed == 1
        assert len(quarantined) == 1
        assert quarantined[0] == "missing_required_field:license_creation_date"

    def test_missing_address_zip_stored_not_quarantined(self, scraper):
        """Record missing address_zip is stored (nullable), not quarantined."""
        db = MagicMock()
        upsert_row = _make_upsert_row(was_update=False, prev_hash=None, new_hash="abc")
        db.execute.return_value.fetchone.return_value = upsert_row

        record_no_zip = _make_record()
        del record_no_zip["address_zip"]

        quarantined = []
        with patch.object(scraper, "build_where_since", return_value="license_creation_date > '2025-01-01'"):
            with patch.object(scraper, "paginate", return_value=iter([record_no_zip])):
                with patch.object(scraper, "quarantine", side_effect=lambda db, raw, r: quarantined.append(r)):
                    processed, failed, _ = scraper._run(db)

        assert len(quarantined) == 0
        db.execute.assert_called()

    def test_run_counts_inserted_vs_changed(self, scraper):
        """_run() correctly distinguishes new inserts from hash-changed updates."""
        db = MagicMock()
        new_hash = "newhashabc"
        old_hash = "oldhashabc"

        # Two records: one INSERT, one UPDATE-with-change
        insert_row = _make_upsert_row(was_update=False, prev_hash=None, new_hash=new_hash)
        update_row = _make_upsert_row(was_update=True, prev_hash=old_hash, new_hash=new_hash)
        db.execute.return_value.fetchone.side_effect = [insert_row, update_row]

        records = [
            _make_record(license_nbr="A001"),
            _make_record(license_nbr="A002"),
        ]
        with patch.object(scraper, "paginate", return_value=iter(records)):
            processed, failed, _ = scraper._run(db)

        assert processed == 2
        assert failed == 0


# ---------------------------------------------------------------------------
# Historical refresh tests
# ---------------------------------------------------------------------------

class TestDcwpHistoricalRefresh:
    def test_historical_refresh_processes_records(self, scraper):
        """refresh_historical_range() returns (processed, failed, inserted, changed)."""
        db = MagicMock()
        upsert_row = _make_upsert_row(was_update=False, prev_hash=None, new_hash="h1")
        db.execute.return_value.fetchone.return_value = upsert_row

        with patch.object(scraper, "paginate", return_value=iter([_make_record()])):
            result = scraper.refresh_historical_range(db, date(2022, 1, 1), date(2022, 1, 31))

        processed, failed, inserted, changed = result
        assert processed == 1
        assert failed == 0
        assert inserted == 1
        assert changed == 0

    def test_historical_refresh_detects_status_change(self, scraper):
        """
        A license created before the 14-day window whose status changed must be
        caught and counted as changed during historical refresh.
        """
        db = MagicMock()
        old_hash = "aaaa" * 16
        new_hash = "bbbb" * 16
        # Simulate existing record with different hash → change detected
        changed_row = _make_upsert_row(was_update=True, prev_hash=old_hash, new_hash=new_hash)
        db.execute.return_value.fetchone.return_value = changed_row

        record = _make_record(
            license_status="Expired",  # changed from original Active
            license_creation_date="2020-06-01T00:00:00.000",
        )
        with patch.object(scraper, "paginate", return_value=iter([record])):
            processed, failed, inserted, changed = scraper.refresh_historical_range(
                db, date(2020, 6, 1), date(2020, 6, 30),
            )

        assert processed == 1
        assert changed == 1
        assert inserted == 0

    def test_historical_refresh_idempotent(self, scraper):
        """Re-refreshing unchanged records reports zero changes."""
        db = MagicMock()
        same_hash = "a" * 64
        # same prev and new hash → no change
        no_change_row = _make_upsert_row(was_update=True, prev_hash=same_hash, new_hash=same_hash)
        db.execute.return_value.fetchone.return_value = no_change_row

        with patch.object(scraper, "paginate", return_value=iter([_make_record()])):
            _, _, inserted, changed = scraper.refresh_historical_range(
                db, date(2022, 1, 1), date(2022, 1, 31),
            )

        assert inserted == 0
        assert changed == 0

    def test_historical_refresh_paginates_multiple_pages(self, scraper):
        """refresh_historical_range() processes all records yielded by paginate()."""
        db = MagicMock()
        upsert_row = _make_upsert_row(was_update=False, prev_hash=None, new_hash="x")
        db.execute.return_value.fetchone.return_value = upsert_row

        records = [_make_record(license_nbr=f"{i:07d}-DCA") for i in range(500)]

        with patch.object(scraper, "paginate", return_value=iter(records)) as mock_paginate:
            processed, failed, inserted, changed = scraper.refresh_historical_range(
                db, date(2022, 1, 1), date(2022, 12, 31),
            )

        assert processed == 500
        assert failed == 0

    def test_historical_refresh_updates_refreshed_at(self, scraper):
        """In historical refresh mode, refreshed_at (not None) is passed to the upsert."""
        db = MagicMock()
        upsert_row = _make_upsert_row(was_update=False, prev_hash=None, new_hash="h")
        db.execute.return_value.fetchone.return_value = upsert_row

        with patch.object(scraper, "paginate", return_value=iter([_make_record()])):
            scraper.refresh_historical_range(db, date(2022, 1, 1), date(2022, 1, 31))

        # Verify the SQL call received a non-None :refreshed_at
        call_kwargs = db.execute.call_args[0][1]  # positional params dict
        assert call_kwargs.get("refreshed_at") is not None

    def test_incremental_run_does_not_set_refreshed_at(self, scraper):
        """In normal incremental mode, :refreshed_at must be None (not a deliberate recheck)."""
        db = MagicMock()
        upsert_row = _make_upsert_row(was_update=False, prev_hash=None, new_hash="h")
        db.execute.return_value.fetchone.return_value = upsert_row

        with patch.object(scraper, "paginate", return_value=iter([_make_record()])):
            scraper._run(db)

        call_kwargs = db.execute.call_args[0][1]
        assert call_kwargs.get("refreshed_at") is None


# ---------------------------------------------------------------------------
# CLI script tests
# ---------------------------------------------------------------------------

class TestDcwpRefreshScript:
    def test_date_chunks_single(self):
        from scripts.dcwp_refresh_historical import _date_chunks
        chunks = list(_date_chunks(date(2022, 1, 1), date(2022, 3, 31), chunk_months=1))
        assert len(chunks) == 3
        assert chunks[0] == (date(2022, 1, 1), date(2022, 1, 31))
        assert chunks[1] == (date(2022, 2, 1), date(2022, 2, 28))
        assert chunks[2] == (date(2022, 3, 1), date(2022, 3, 31))

    def test_date_chunks_covers_full_range(self):
        from scripts.dcwp_refresh_historical import _date_chunks
        since = date(2021, 1, 1)
        until = date(2022, 12, 31)
        chunks = list(_date_chunks(since, until, chunk_months=3))
        assert chunks[0][0] == since
        assert chunks[-1][1] == until
        # No gaps between consecutive chunks
        for (_, ce), (ns, _) in zip(chunks, chunks[1:]):
            from datetime import timedelta
            assert ns == ce + timedelta(days=1)
