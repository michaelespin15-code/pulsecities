"""
Unit tests for the DOF property assessment scraper — scrapers/dof.py.

Tests cover:
  - Scraper configuration constants
  - _parse() BBL mapping from bble field
  - _parse() BBL fallback from boro/block/lot parts
  - _parse() assessed_total logic (avtot primary, fullval fallback)
  - _parse() quarantine path for invalid/missing BBL
  - _parse() field mapping for address, zip_code, raw_data
"""

import pytest
from unittest.mock import MagicMock, patch

from scrapers.dof import DOFScraper


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def scraper():
    with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
        return DOFScraper()


VALID_RAW = {
    "bble": "1017280024",
    "boro": "1",
    "block": "1728",
    "lot": "24",
    "owner": "ROSETTA M WILLIAMS",
    "avtot": "138751",
    "fullval": "885000",
    "staddr": "23 WEST 130 STREET",
    "zip": "10037",
    "year": "2018/19",
}


# ---------------------------------------------------------------------------
# TestDOFScraperConfig
# ---------------------------------------------------------------------------

class TestDOFScraperConfig:
    def test_scraper_name(self):
        assert DOFScraper.SCRAPER_NAME == "dof_assessments"

    def test_dataset_id(self):
        assert DOFScraper.DATASET_ID == "w7rz-68fs"

    def test_initial_lookback_days(self):
        assert DOFScraper.INITIAL_LOOKBACK_DAYS == 0


# ---------------------------------------------------------------------------
# TestParseValid
# ---------------------------------------------------------------------------

class TestParseValid:
    def test_bbl_from_bble_field(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["bbl"] == "1017280024"

    def test_assessed_total_from_avtot(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["assessed_total"] == 138751.0

    def test_borough_int(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["borough"] == 1

    def test_address_mapped(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["address"] == "23 WEST 130 STREET"

    def test_zip_code_mapped(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["zip_code"] == "10037"

    def test_raw_data_stored(self, scraper):
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["raw_data"] == VALID_RAW


# ---------------------------------------------------------------------------
# TestParseBBLFallback
# ---------------------------------------------------------------------------

class TestParseBBLFallback:
    def test_bbl_constructed_from_parts(self, scraper):
        """When bble is absent, BBL should be constructed from boro/block/lot."""
        db = MagicMock()
        raw = {k: v for k, v in VALID_RAW.items() if k != "bble"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["bbl"] == "1017280024"

    def test_missing_bbl_quarantined(self, scraper):
        """When both bble and boro/block/lot are absent, record must be quarantined."""
        db = MagicMock()
        raw = {
            "owner": "SOME OWNER",
            "avtot": "100000",
            "fullval": "500000",
            "staddr": "123 MAIN ST",
            "zip": "10001",
            "year": "2018/19",
        }
        row = scraper._parse(db, raw)
        assert row is None
        db.add.assert_called_once()

    def test_null_bble_with_no_parts_quarantined(self, scraper):
        """bble=None and no boro/block/lot must be quarantined."""
        db = MagicMock()
        raw = {"bble": None, "avtot": "100000"}
        row = scraper._parse(db, raw)
        assert row is None
        db.add.assert_called_once()

    def test_bbl_from_parts_zero_pads_block_and_lot(self, scraper):
        """Block and lot must be zero-padded when constructing BBL from parts."""
        db = MagicMock()
        raw = {
            "boro": "2",
            "block": "100",
            "lot": "5",
            "avtot": "50000",
            "year": "2018/19",
        }
        row = scraper._parse(db, raw)
        assert row is not None
        # borough=2, block=00100, lot=0005 → 2001000005
        assert row["bbl"] == "2001000005"


# ---------------------------------------------------------------------------
# TestParseAssessedValue
# ---------------------------------------------------------------------------

class TestParseAssessedValue:
    def test_zero_avtot_falls_back_to_fullval(self, scraper):
        """When avtot is zero, assessed_total should be populated from fullval."""
        db = MagicMock()
        raw = {**VALID_RAW, "avtot": "0", "fullval": "885000"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["assessed_total"] == 885000.0

    def test_both_null_assessed_total_is_none(self, scraper):
        """When both avtot and fullval are None, assessed_total should be None, record valid."""
        db = MagicMock()
        raw = {**VALID_RAW, "avtot": None, "fullval": None}
        row = scraper._parse(db, raw)
        assert row is not None  # Record is still valid — just no assessment value
        assert row["assessed_total"] is None

    def test_absent_avtot_uses_fullval(self, scraper):
        """When avtot key is absent, fallback to fullval."""
        db = MagicMock()
        raw = {k: v for k, v in VALID_RAW.items() if k != "avtot"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["assessed_total"] == 885000.0

    def test_string_avtot_parsed_as_float(self, scraper):
        """String numeric values for avtot must be parsed to float."""
        db = MagicMock()
        raw = {**VALID_RAW, "avtot": "250000"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["assessed_total"] == 250000.0

    def test_invalid_avtot_falls_back_to_fullval(self, scraper):
        """Non-numeric avtot string should fall back to fullval."""
        db = MagicMock()
        raw = {**VALID_RAW, "avtot": "N/A", "fullval": "500000"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["assessed_total"] == 500000.0


# ---------------------------------------------------------------------------
# TestParseFieldMapping
# ---------------------------------------------------------------------------

class TestParseFieldMapping:
    def test_block_zero_padded_to_5(self, scraper):
        """Block number must be zero-padded to 5 characters."""
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["block"] == "01728"

    def test_lot_zero_padded_to_4(self, scraper):
        """Lot number must be zero-padded to 4 characters."""
        db = MagicMock()
        row = scraper._parse(db, VALID_RAW)
        assert row is not None
        assert row["lot"] == "0024"

    def test_missing_staddr_yields_none_address(self, scraper):
        """Missing staddr should produce address=None, not empty string."""
        db = MagicMock()
        raw = {k: v for k, v in VALID_RAW.items() if k != "staddr"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["address"] is None

    def test_zip_plus4_stripped(self, scraper):
        """ZIP+4 format in zip field should be stripped to 5 digits."""
        db = MagicMock()
        raw = {**VALID_RAW, "zip": "10037-1234"}
        row = scraper._parse(db, raw)
        assert row is not None
        assert row["zip_code"] == "10037"

    def test_returns_none_not_exception_on_bad_borough(self, scraper):
        """Non-numeric boro field should not raise — should quarantine via fallback failure."""
        db = MagicMock()
        raw = {**VALID_RAW, "bble": None, "boro": "X", "block": "abc", "lot": "xyz"}
        row = scraper._parse(db, raw)
        assert row is None
        db.add.assert_called_once()
