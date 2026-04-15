"""
Unit tests for the 311 complaints scraper — scrapers/complaints.py.

Tests cover:
  - _parse()     field mapping, BBL normalization, geometry, quarantine path
  - _parse_dt()  ISO 8601 datetime parsing variants
  - _clean_zip() ZIP+4 stripping, invalid format rejection
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from scrapers.complaints import ComplaintsScraper, _parse_dt, _clean_zip


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def scraper():
    with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
        return ComplaintsScraper()


# ---------------------------------------------------------------------------
# _parse_dt
# ---------------------------------------------------------------------------

class TestParseDt:
    def test_full_iso(self):
        dt = _parse_dt("2026-03-15T14:30:00.000")
        assert dt == datetime(2026, 3, 15, 14, 30, 0, tzinfo=timezone.utc)

    def test_iso_no_ms(self):
        dt = _parse_dt("2026-03-15T14:30:00")
        assert dt == datetime(2026, 3, 15, 14, 30, 0, tzinfo=timezone.utc)

    def test_date_only(self):
        dt = _parse_dt("2026-03-15")
        assert dt is not None
        assert dt.year == 2026 and dt.month == 3 and dt.day == 15

    def test_none_returns_none(self):
        assert _parse_dt(None) is None

    def test_empty_returns_none(self):
        assert _parse_dt("") is None

    def test_invalid_returns_none(self):
        assert _parse_dt("not-a-date") is None


# ---------------------------------------------------------------------------
# _clean_zip
# ---------------------------------------------------------------------------

class TestCleanZip:
    def test_valid_5_digit(self):
        assert _clean_zip("10001") == "10001"

    def test_strips_plus4(self):
        assert _clean_zip("10001-1234") == "10001"

    def test_none_returns_none(self):
        assert _clean_zip(None) is None

    def test_empty_returns_none(self):
        assert _clean_zip("") is None

    def test_non_numeric_returns_none(self):
        assert _clean_zip("BRONX") is None

    def test_too_short_returns_none(self):
        assert _clean_zip("1001") is None

    def test_whitespace_stripped(self):
        assert _clean_zip("  10001  ") == "10001"


# ---------------------------------------------------------------------------
# _parse: valid record
# ---------------------------------------------------------------------------

class TestParseValid:
    VALID_RAW = {
        "unique_key": "12345678",
        "complaint_type": "HEAT/HOT WATER",
        "descriptor": "APARTMENT ONLY",
        "incident_zip": "10030",
        "incident_address": "123 MAIN ST",
        "borough": "MANHATTAN",
        "agency": "HPD",
        "status": "Closed",
        "created_date": "2026-03-01T10:00:00.000",
        "closed_date": "2026-03-05T12:00:00.000",
        "latitude": "40.7580",
        "longitude": "-73.9855",
        "bbl": "1002340045",
    }

    def test_unique_key_mapped(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, self.VALID_RAW)
        assert row is not None
        assert row["unique_key"] == "12345678"

    def test_complaint_type_mapped(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, self.VALID_RAW)
        assert row["complaint_type"] == "HEAT/HOT WATER"

    def test_zip_code_mapped(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, self.VALID_RAW)
        assert row["zip_code"] == "10030"

    def test_created_date_parsed(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, self.VALID_RAW)
        assert row["created_date"] == datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)

    def test_location_geometry_created(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, self.VALID_RAW)
        assert row["location"] is not None  # GeoAlchemy2 WKBElement

    def test_raw_data_stored(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, self.VALID_RAW)
        assert row["raw_data"] == self.VALID_RAW

    def test_bbl_normalized(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, self.VALID_RAW)
        assert row["bbl"] == "1002340045"

    def test_missing_lat_lng_no_location(self, scraper):
        db = MagicMock()
        raw = {**self.VALID_RAW, "latitude": None, "longitude": None}
        row = scraper._parse(db, raw)
        assert row["location"] is None

    def test_null_bbl_acceptable(self, scraper):
        """Null-BBL records are acceptable — they participate in zip-code scoring."""
        db = MagicMock()
        raw = {k: v for k, v in self.VALID_RAW.items() if k != "bbl"}
        raw["bbl"] = None
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["bbl"] is None
        db.add.assert_not_called()  # must NOT be quarantined

    def test_absent_bbl_key_acceptable(self, scraper):
        """Record with no bbl key at all is acceptable — bbl remains None."""
        db = MagicMock()
        raw = {k: v for k, v in self.VALID_RAW.items() if k != "bbl"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["bbl"] is None
        db.add.assert_not_called()  # must NOT be quarantined

    def test_invalid_lat_no_exception(self, scraper):
        """Bad float string for latitude must NOT raise — location set to None."""
        db = MagicMock()
        raw = {**self.VALID_RAW, "latitude": "not_a_float", "longitude": "-73.9855"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["location"] is None


# ---------------------------------------------------------------------------
# _parse: quarantine paths
# ---------------------------------------------------------------------------

class TestParseQuarantine:
    def test_missing_unique_key_quarantined(self, scraper):
        db = MagicMock()
        result = scraper._parse(db, {"unique_key": "", "created_date": "2026-01-01T00:00:00.000"})
        assert result is None
        db.add.assert_called_once()  # quarantine row added

    def test_null_unique_key_quarantined(self, scraper):
        db = MagicMock()
        result = scraper._parse(db, {"created_date": "2026-01-01T00:00:00.000"})
        assert result is None
        db.add.assert_called_once()
