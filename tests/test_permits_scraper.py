"""
Tests for the DOB permits scraper — BBL construction, date parsing, zip cleaning.

TDD Wave 0 stubs: some tests will be RED until Task 3 fixes permits.py.

Specifically:
  - _build_bbl tests: RED until _build_bbl is added to scrapers/permits.py (Task 3)
  - test_parse_date_mm_dd_yyyy: RED until _parse_date is updated to handle MM/DD/YYYY (Task 3)
  - _parse tests: use a minimal mock for quarantine since _parse is a method on PermitsScraper
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from scrapers.permits import PermitsScraper, _parse_date, _clean_zip


# ---------------------------------------------------------------------------
# Helpers — try to import _build_bbl (does not exist yet in Task 1 RED phase)
# ---------------------------------------------------------------------------

try:
    from scrapers.permits import _build_bbl
    _BUILD_BBL_AVAILABLE = True
except ImportError:
    _BUILD_BBL_AVAILABLE = False
    _build_bbl = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# _build_bbl tests — RED until Task 3 adds the function
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _BUILD_BBL_AVAILABLE, reason="_build_bbl not yet implemented (Task 3)")
class TestBuildBbl:
    def test_build_bbl_manhattan(self):
        """Borough name MANHATTAN maps to code 1 -> BBL 1005070025"""
        result = _build_bbl({"borough": "MANHATTAN", "block": "00507", "lot": "00025"})
        assert result == "1005070025"

    def test_build_bbl_brooklyn(self):
        """Borough name BROOKLYN maps to code 3 -> BBL 3056780042"""
        result = _build_bbl({"borough": "BROOKLYN", "block": "05678", "lot": "0042"})
        assert result == "3056780042"

    def test_build_bbl_missing_borough(self):
        """Missing borough field should return None"""
        result = _build_bbl({"block": "00507", "lot": "00025"})
        assert result is None

    def test_build_bbl_missing_block(self):
        """Missing block field should return None"""
        result = _build_bbl({"borough": "MANHATTAN", "lot": "00025"})
        assert result is None

    def test_build_bbl_invalid_borough(self):
        """Unrecognized borough name should return None (not in allowlist)"""
        result = _build_bbl({"borough": "INVALID", "block": "00507", "lot": "00025"})
        assert result is None


# ---------------------------------------------------------------------------
# _parse() tests — method on PermitsScraper, need to mock quarantine
# ---------------------------------------------------------------------------

def _make_scraper() -> PermitsScraper:
    """Instantiate PermitsScraper with quarantine patched out."""
    scraper = PermitsScraper()
    scraper.quarantine = MagicMock()
    return scraper


class TestParse:
    def test_parse_valid_record(self):
        """Full record with all fields should produce a correctly populated dict."""
        scraper = _make_scraper()
        raw = {
            "borough": "MANHATTAN",
            "block": "00507",
            "lot": "00025",
            "bin__": "1234567",
            "house__": "100",
            "street_name": "BROADWAY",
            "zip_code": "10012",
            "permit_type": "NB",
            "work_type": "NB",
            "filing_date": "04/09/2026",
            "expiration_date": "04/09/2027",
            "owner_s_first_name": "JOHN",
            "owner_s_last_name": "DOE",
            "job_description1": "NEW BUILDING",
        }
        result = scraper._parse(scraper, raw) if False else scraper._parse(MagicMock(), raw)
        # _parse should return a dict (not None) for a valid record
        # When _build_bbl exists, bbl should be "1005070025"
        assert result is not None
        assert result["permit_type"] == "NB"
        assert result["zip_code"] == "10012"

    def test_parse_missing_borough_and_bin(self):
        """Record with no borough (no BBL) and no BIN should go to quarantine and return None."""
        scraper = _make_scraper()
        db_mock = MagicMock()
        raw = {
            "block": "00507",
            "lot": "00025",
            "zip_code": "10012",
            "permit_type": "NB",
            "filing_date": "04/09/2026",
        }
        result = scraper._parse(db_mock, raw)
        assert result is None
        scraper.quarantine.assert_called_once()


# ---------------------------------------------------------------------------
# _parse_date tests
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_parse_date_mm_dd_yyyy(self):
        """MM/DD/YYYY format (confirmed in research as the actual API format)
        NOTE: This test is RED until Task 3 updates _parse_date to handle this format."""
        result = _parse_date("04/09/2026")
        assert result == date(2026, 4, 9)

    def test_parse_date_iso(self):
        """ISO format YYYY-MM-DD should continue to work."""
        result = _parse_date("2024-01-15")
        assert result == date(2024, 1, 15)

    def test_parse_date_none(self):
        """None input should return None."""
        assert _parse_date(None) is None

    def test_parse_date_empty_string(self):
        """Empty string should return None."""
        assert _parse_date("") is None


# ---------------------------------------------------------------------------
# _clean_zip tests
# ---------------------------------------------------------------------------

class TestCleanZip:
    def test_clean_zip_valid(self):
        """Standard 5-digit zip should pass through unchanged."""
        assert _clean_zip("10001") == "10001"

    def test_clean_zip_with_plus4(self):
        """ZIP+4 format should be stripped to 5 digits."""
        assert _clean_zip("10001-1234") == "10001"

    def test_clean_zip_none(self):
        """None input should return None."""
        assert _clean_zip(None) is None

    def test_clean_zip_invalid(self):
        """Non-numeric or wrong-length zip should return None."""
        assert _clean_zip("ABCDE") is None
        assert _clean_zip("1234") is None
