"""
Unit tests for the DCWP business licenses scraper — scrapers/dcwp_licenses.py.

Tests cover:
  - Model configuration (tablename, unique constraint)
  - Scraper configuration (SCRAPER_NAME, DATASET_ID)
  - _run() with mocked API response returns (processed_count, failed_count, watermark)
  - Quarantine: record missing license_creation_date is quarantined
  - Nullable field: record missing address_zip is stored, not quarantined
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date, datetime, timezone


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestDcwpLicenseModel:
    def test_tablename(self):
        from models.dcwp_license import DcwpLicense
        assert DcwpLicense.__tablename__ == "dcwp_licenses"

    def test_unique_constraint_on_license_nbr(self):
        """DcwpLicense must have a UNIQUE constraint on license_nbr named uq_dcwp_license_nbr."""
        from models.dcwp_license import DcwpLicense
        from sqlalchemy import UniqueConstraint
        constraints = {c.name for c in DcwpLicense.__table__.constraints
                       if isinstance(c, UniqueConstraint)}
        assert "uq_dcwp_license_nbr" in constraints, (
            f"Expected 'uq_dcwp_license_nbr' in {constraints}"
        )


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


# ---------------------------------------------------------------------------
# Scraper _run() tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def scraper():
    with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
        from scrapers.dcwp_licenses import DcwpScraper
        return DcwpScraper()


class TestDcwpScraperRun:
    def test_run_returns_tuple(self, scraper):
        """_run() returns (processed_count, failed_count, watermark) tuple."""
        db = MagicMock()
        db.execute.return_value.rowcount = 1

        mock_record = {
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

        with patch.object(scraper, "build_where_since", return_value="license_creation_date > '2025-01-01'"):
            with patch.object(scraper, "paginate", return_value=iter([mock_record])):
                result = scraper._run(db)

        assert isinstance(result, tuple), "Expected tuple return from _run()"
        assert len(result) == 3, "Expected (processed, failed, watermark) tuple"
        processed, failed, watermark = result
        assert isinstance(processed, int)
        assert isinstance(failed, int)

    def test_missing_license_creation_date_quarantined(self, scraper):
        """Record missing license_creation_date is quarantined, not stored."""
        db = MagicMock()

        bad_record = {
            "license_nbr": "BAD-001",
            "business_name": "BAD CORP",
            "business_category": "Tow Truck Company",
            "license_status": "Active",
            # license_creation_date intentionally absent
            "address_zip": "10001",
        }

        quarantined = []
        def mock_quarantine(db, raw, reason):
            quarantined.append((raw, reason))

        with patch.object(scraper, "build_where_since", return_value="license_creation_date > '2025-01-01'"):
            with patch.object(scraper, "paginate", return_value=iter([bad_record])):
                with patch.object(scraper, "quarantine", side_effect=mock_quarantine):
                    processed, failed, watermark = scraper._run(db)

        assert failed == 1, f"Expected 1 failed record, got {failed}"
        assert len(quarantined) == 1, "Expected 1 quarantined record"
        _, reason = quarantined[0]
        assert reason == "missing_required_field:license_creation_date", (
            f"Expected quarantine reason 'missing_required_field:license_creation_date', got '{reason}'"
        )

    def test_missing_address_zip_stored_not_quarantined(self, scraper):
        """Record missing address_zip is stored (nullable), not quarantined."""
        db = MagicMock()
        db.execute.return_value.rowcount = 1

        record_no_zip = {
            "license_nbr": "2054609-DCA",
            "business_name": "TEST CORP",
            "business_category": "Home Improvement Contractor",
            "license_status": "Active",
            "license_creation_date": "2026-01-15T00:00:00.000",
            # address_zip intentionally absent
        }

        quarantined = []
        def mock_quarantine(db, raw, reason):
            quarantined.append((raw, reason))

        with patch.object(scraper, "build_where_since", return_value="license_creation_date > '2025-01-01'"):
            with patch.object(scraper, "paginate", return_value=iter([record_no_zip])):
                with patch.object(scraper, "quarantine", side_effect=mock_quarantine):
                    processed, failed, watermark = scraper._run(db)

        assert len(quarantined) == 0, (
            f"Expected 0 quarantined records (zip is nullable), got {len(quarantined)}"
        )
        # Record should have been processed (upserted)
        db.execute.assert_called()
