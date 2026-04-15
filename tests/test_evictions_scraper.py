"""
Unit tests for the NYC Marshal Eviction Records scraper — scrapers/evictions.py.

Tests cover:
  - EvictionsScraper class config (constants)
  - _parse_date()   ISO 8601 date parsing variants
  - _clean_zip()    ZIP+4 stripping, invalid format rejection
  - _parse()        field mapping, address concatenation, quarantine paths,
                    borough field variants, null-BBL acceptance, eviction_type
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date

from scrapers.evictions import EvictionsScraper, _parse_date, _clean_zip


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def scraper():
    with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
        return EvictionsScraper()


# ---------------------------------------------------------------------------
# EvictionsScraper class configuration
# ---------------------------------------------------------------------------

class TestEvictionScraperConfig:
    def test_initial_lookback_days(self):
        assert EvictionsScraper.INITIAL_LOOKBACK_DAYS == 730

    def test_scraper_name(self):
        assert EvictionsScraper.SCRAPER_NAME == "evictions"

    def test_dataset_id(self):
        assert EvictionsScraper.DATASET_ID == "6z8x-wfk4"


# ---------------------------------------------------------------------------
# _parse_date
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_iso_datetime_format(self):
        result = _parse_date("2026-03-15T00:00:00.000")
        assert result == date(2026, 3, 15)

    def test_date_only_format(self):
        result = _parse_date("2026-03-15")
        assert result == date(2026, 3, 15)

    def test_none_returns_none(self):
        assert _parse_date(None) is None

    def test_empty_returns_none(self):
        assert _parse_date("") is None

    def test_invalid_returns_none(self):
        assert _parse_date("not-a-date") is None


# ---------------------------------------------------------------------------
# _clean_zip (evictions variant)
# ---------------------------------------------------------------------------

class TestCleanZipEvictions:
    def test_valid_zip(self):
        assert _clean_zip("11211") == "11211"

    def test_strips_plus4(self):
        assert _clean_zip("11211-3456") == "11211"

    def test_none_returns_none(self):
        assert _clean_zip(None) is None

    def test_non_numeric_returns_none(self):
        assert _clean_zip("BROOKLYN") is None


# ---------------------------------------------------------------------------
# _parse: valid record — field mapping
# ---------------------------------------------------------------------------

VALID_RAW = {
    "executed_date": "2026-02-10T00:00:00.000",
    "court_index_number": "LT-001234/2026",
    "docket_number": "D-56789",
    "eviction_address": "456 FLATBUSH AVE",
    "apartment_no": "3B",
    "eviction_borough": "Brooklyn",
    "zip_code": "11217",
    "bbl": "3055670010",
    "residential_commercial_ind": "R",
}


class TestParseValid:
    def test_executed_date_parsed(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["executed_date"] == date(2026, 2, 10)

    def test_address_with_apartment(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["address"] == "456 FLATBUSH AVE Apt 3B"

    def test_address_without_apartment(self, scraper):
        db = MagicMock()
        raw = {k: v for k, v in VALID_RAW.items() if k != "apartment_no"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["address"] == "456 FLATBUSH AVE"

    def test_bbl_normalized(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["bbl"] == "3055670010"

    def test_null_bbl_accepted(self, scraper):
        """Null BBL is NOT quarantine — records without BBL still contribute to zip-level scoring."""
        db = MagicMock()
        raw = {k: v for k, v in VALID_RAW.items() if k != "bbl"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["bbl"] is None

    def test_zip_mapped(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["zip_code"] == "11217"

    def test_borough_variant_field(self, scraper):
        """Scraper must accept 'borough' as an alias for 'eviction_borough'."""
        db = MagicMock()
        raw = {k: v for k, v in VALID_RAW.items() if k != "eviction_borough"}
        raw["borough"] = "Brooklyn"
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["borough"] is not None

    def test_eviction_type_mapped(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["eviction_type"] == "R"

    def test_raw_data_stored(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["raw_data"] == VALID_RAW

    def test_eviction_zip_code_variant(self, scraper):
        """Scraper must accept 'eviction_zip_code' as alias for 'zip_code'."""
        db = MagicMock()
        raw = {k: v for k, v in VALID_RAW.items() if k != "zip_code"}
        raw["eviction_zip_code"] = "11217"
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["zip_code"] == "11217"

    def test_eviction_type_fallback_field(self, scraper):
        """Scraper must fall back to 'eviction_type' if 'residential_commercial_ind' absent."""
        db = MagicMock()
        raw = {k: v for k, v in VALID_RAW.items() if k != "residential_commercial_ind"}
        raw["eviction_type"] = "C"
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["eviction_type"] == "C"


# ---------------------------------------------------------------------------
# _parse: quarantine paths
# ---------------------------------------------------------------------------

class TestParseQuarantine:
    def test_missing_executed_date_quarantined(self, scraper):
        """Records without executed_date must be quarantined — watermark field is required."""
        db = MagicMock()
        raw = {k: v for k, v in VALID_RAW.items() if k != "executed_date"}
        result = scraper._parse(db, raw)
        assert result is None
        db.add.assert_called_once()

    def test_no_identifiers_quarantined(self, scraper):
        """Records with executed_date but no bbl, docket_number, or court_index_number must be quarantined."""
        db = MagicMock()
        raw = {
            "executed_date": "2026-02-10T00:00:00.000",
            "eviction_address": "456 FLATBUSH AVE",
            "eviction_borough": "Brooklyn",
            "zip_code": "11217",
            # No bbl, docket_number, or court_index_number
        }
        result = scraper._parse(db, raw)
        assert result is None
        db.add.assert_called_once()
