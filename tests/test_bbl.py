"""
Tests for normalize_bbl() — the universal property identifier utility.
This function is called by every scraper before any record is persisted.
Correctness here prevents months of silent cross-source join failures.
"""

import pytest
from models.bbl import normalize_bbl, bbl_to_parts


class TestNormalizeBbl:
    def test_canonical_10digit_passthrough(self):
        assert normalize_bbl("1000010001") == "1000010001"

    def test_hyphenated_full_padding(self):
        assert normalize_bbl("1-00001-0001") == "1000010001"

    def test_hyphenated_short_form(self):
        # Short form with no zero-padding
        assert normalize_bbl("1-1-1") == "1000010001"

    def test_brooklyn_hyphenated(self):
        assert normalize_bbl("3-05678-0042") == "3056780042"

    def test_queens_hyphenated(self):
        assert normalize_bbl("4-12345-0100") == "4123450100"

    def test_all_boroughs(self):
        for borough in range(1, 6):
            result = normalize_bbl(f"{borough}-00100-0001")
            assert result is not None
            assert result[0] == str(borough)
            assert len(result) == 10

    def test_integer_input(self):
        # Some APIs return BBL as integer
        assert normalize_bbl(1000010001) == "1000010001"

    def test_none_returns_none(self):
        assert normalize_bbl(None) is None

    def test_empty_string_returns_none(self):
        assert normalize_bbl("") is None

    def test_whitespace_stripped(self):
        assert normalize_bbl("  1000010001  ") == "1000010001"

    def test_invalid_borough_code_returns_none(self):
        assert normalize_bbl("6000010001") is None  # borough 6 doesn't exist
        assert normalize_bbl("0000010001") is None  # borough 0 doesn't exist

    def test_invalid_format_returns_none(self):
        assert normalize_bbl("not-a-bbl") is None
        assert normalize_bbl("12345") is None  # too short
        assert normalize_bbl("123456789012") is None  # too long

    def test_output_always_10_digits(self):
        result = normalize_bbl("2-00050-0007")
        assert result is not None
        assert len(result) == 10
        assert result.isdigit()


class TestBblToParts:
    def test_splits_correctly(self):
        borough, block, lot = bbl_to_parts("1000010001")
        assert borough == 1
        assert block == 1
        assert lot == 1

    def test_brooklyn_splits(self):
        borough, block, lot = bbl_to_parts("3056780042")
        assert borough == 3
        assert block == 5678
        assert lot == 42

    def test_invalid_returns_none(self):
        assert bbl_to_parts("not-valid") is None
        assert bbl_to_parts(None) is None
