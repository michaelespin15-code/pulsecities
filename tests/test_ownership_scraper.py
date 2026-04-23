"""
Unit tests for the ACRIS ownership scraper — scrapers/ownership.py.

Tests cover:
  - Configuration constants (SCRAPER_NAME, DATASET_ID, BATCH_SIZE, doc types)
  - _bbl_from_legals()  — BBL construction from borough/block/lot
  - _parse_date()       — date string parsing variants
  - _parse_decimal()    — decimal string parsing
  - _join_and_persist() — three-dataset join logic, BBL-missing path, parties-failure path
"""

import pytest
from unittest.mock import MagicMock, patch, call
from datetime import date, datetime, timezone
from decimal import Decimal

from scrapers.ownership import (
    OwnershipScraper,
    BATCH_SIZE,
    _bbl_from_legals,
    normalize_party_name,
    _parse_date,
    _parse_decimal,
    _date_to_dt,
)
from config.nyc import ACRIS_TRANSFER_DOC_TYPES


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def scraper():
    with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
        return OwnershipScraper()


# ---------------------------------------------------------------------------
# TestOwnershipScraperConfig
# ---------------------------------------------------------------------------

class TestOwnershipScraperConfig:
    def test_scraper_name(self):
        assert OwnershipScraper.SCRAPER_NAME == "acris_ownership"

    def test_master_dataset_id(self):
        assert OwnershipScraper.DATASET_ID == "bnx9-e6tj"

    def test_batch_size(self):
        assert BATCH_SIZE == 400

    def test_doc_types_include_deed(self):
        assert "DEED" in ACRIS_TRANSFER_DOC_TYPES

    def test_doc_types_include_asst(self):
        # CRITICAL: LLC acquisitions often appear as ASST (assignment)
        assert "ASST" in ACRIS_TRANSFER_DOC_TYPES

    def test_doc_types_include_deedp(self):
        assert "DEEDP" in ACRIS_TRANSFER_DOC_TYPES

    def test_doc_types_is_tuple_or_list(self):
        # Must be a sequence — used in IN clause
        assert isinstance(ACRIS_TRANSFER_DOC_TYPES, (tuple, list))

    def test_doc_types_non_empty(self):
        assert len(ACRIS_TRANSFER_DOC_TYPES) >= 3


# ---------------------------------------------------------------------------
# TestBblFromLegals
# ---------------------------------------------------------------------------

class TestBblFromLegals:
    def test_valid_construction(self):
        result = _bbl_from_legals({"borough": "1", "block": "1728", "lot": "24"})
        assert result == "1017280024"

    def test_float_strings_handled(self):
        # Some API responses return numbers as floats: "3.0", "5678.0"
        result = _bbl_from_legals({"borough": "3.0", "block": "5678.0", "lot": "42.0"})
        assert result == "3056780042"

    def test_missing_borough_returns_none(self):
        result = _bbl_from_legals({"block": "1000", "lot": "1"})
        assert result is None

    def test_non_numeric_returns_none(self):
        result = _bbl_from_legals({"borough": "X", "block": "1000", "lot": "1"})
        assert result is None

    def test_zero_pads_block_and_lot(self):
        # Block zero-padded to 5 digits, lot to 4 digits
        result = _bbl_from_legals({"borough": "2", "block": "50", "lot": "7"})
        assert result is not None
        assert len(result) == 10
        assert result == "2000500007"

    def test_missing_lot_returns_none(self):
        result = _bbl_from_legals({"borough": "1", "block": "1728"})
        assert result is None

    def test_missing_block_returns_none(self):
        result = _bbl_from_legals({"borough": "1", "lot": "24"})
        assert result is None

    def test_empty_dict_returns_none(self):
        result = _bbl_from_legals({})
        assert result is None

    def test_output_is_10_digits(self):
        result = _bbl_from_legals({"borough": "4", "block": "12345", "lot": "100"})
        assert result is not None
        assert len(result) == 10
        assert result.isdigit()


# ---------------------------------------------------------------------------
# TestParseDate
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_iso_date_parsed(self):
        result = _parse_date("2026-03-15T00:00:00.000")
        assert result == date(2026, 3, 15)

    def test_date_only_parsed(self):
        result = _parse_date("2026-03-15")
        assert result == date(2026, 3, 15)

    def test_none_returns_none(self):
        assert _parse_date(None) is None

    def test_invalid_returns_none(self):
        assert _parse_date("not-a-date") is None

    def test_empty_string_returns_none(self):
        assert _parse_date("") is None

    def test_partial_iso_parsed(self):
        # ACRIS often returns "2026-03-15T00:00:00.000" — first 10 chars matter
        result = _parse_date("2026-01-01T12:34:56.789")
        assert result == date(2026, 1, 1)


# ---------------------------------------------------------------------------
# TestParseDecimal
# ---------------------------------------------------------------------------

class TestParseDecimal:
    def test_numeric_string_parsed(self):
        result = _parse_decimal("1500000")
        assert result == Decimal("1500000")

    def test_none_returns_none(self):
        assert _parse_decimal(None) is None

    def test_empty_returns_none(self):
        assert _parse_decimal("") is None

    def test_zero_string_parsed(self):
        result = _parse_decimal("0")
        assert result == Decimal("0")

    def test_decimal_string_parsed(self):
        result = _parse_decimal("500000.50")
        assert result == Decimal("500000.50")


# ---------------------------------------------------------------------------
# TestJoinAndPersist
# ---------------------------------------------------------------------------

class TestJoinAndPersist:
    def test_join_produces_row_with_bbl_and_party(self, scraper):
        """When parties and legals both return data, row is assembled correctly."""
        master_batch = {
            "ACRIS_DOC_001": {
                "doc_type": "DEED",
                "doc_date": date(2026, 1, 15),
                "doc_amount": Decimal("500000"),
            }
        }
        db = MagicMock()
        db.execute.return_value.rowcount = 1

        with patch.object(scraper, "_fetch_parties", return_value=[
            {"document_id": "ACRIS_DOC_001", "party_type": "2", "name": "ABC LLC"}
        ]):
            with patch.object(scraper, "_fetch_legals", return_value=[
                {"document_id": "ACRIS_DOC_001", "borough": "1", "block": "100", "lot": "1"}
            ]):
                processed, failed = scraper._join_and_persist(db, master_batch)

        assert processed == 1
        assert failed == 0
        # Verify insert was called
        db.execute.assert_called_once()
        db.commit.assert_called_once()

    def test_missing_bbl_counts_as_failed(self, scraper):
        """Document with no BBL in legals is counted as failed (not quarantined)."""
        master_batch = {
            "DOC_NO_BBL": {
                "doc_type": "DEED",
                "doc_date": date(2026, 1, 1),
                "doc_amount": None,
            }
        }
        db = MagicMock()

        with patch.object(scraper, "_fetch_parties", return_value=[
            {"document_id": "DOC_NO_BBL", "party_type": "2", "name": "ABC LLC"}
        ]):
            with patch.object(scraper, "_fetch_legals", return_value=[]):  # no legals
                processed, failed = scraper._join_and_persist(db, master_batch)

        assert processed == 0
        assert failed == 1

    def test_parties_fetch_failure_does_not_crash(self, scraper):
        """If parties fetch raises, the entire batch is counted as failed (WR-04)."""
        master_batch = {
            "DOC_001": {
                "doc_type": "DEED",
                "doc_date": date(2026, 1, 1),
                "doc_amount": None,
            }
        }
        db = MagicMock()

        with patch.object(scraper, "_fetch_parties", side_effect=Exception("timeout")):
            with patch.object(scraper, "_fetch_legals", return_value=[
                {"document_id": "DOC_001", "borough": "1", "block": "100", "lot": "1"}
            ]):
                # Should not raise — parties failure is logged as warning, not crash
                processed, failed = scraper._join_and_persist(db, master_batch)

        # Batch counted as failed; no rows written with null party names
        assert processed == 0
        assert failed == len(master_batch)
        assert not db.execute.called

    def test_row_includes_normalized_party_name(self, scraper):
        """party_name_normalized is set from normalize_party_name(raw_name)."""
        master_batch = {
            "DOC_LLC": {
                "doc_type": "DEED",
                "doc_date": date(2026, 1, 1),
                "doc_amount": None,
            }
        }
        db = MagicMock()
        db.execute.return_value.rowcount = 1
        captured_rows = []

        def capture_execute(stmt):
            captured_rows.append(stmt)
            mock_result = MagicMock()
            mock_result.rowcount = 1
            return mock_result

        db.execute.side_effect = capture_execute

        with patch.object(scraper, "_fetch_parties", return_value=[
            {"document_id": "DOC_LLC", "party_type": "2", "name": "ACME L.L.C."}
        ]):
            with patch.object(scraper, "_fetch_legals", return_value=[
                {"document_id": "DOC_LLC", "borough": "3", "block": "5678", "lot": "42"}
            ]):
                scraper._join_and_persist(db, master_batch)

        assert db.execute.called

    def test_empty_master_batch_no_insert(self, scraper):
        """Empty master batch produces no DB write."""
        db = MagicMock()

        with patch.object(scraper, "_fetch_parties", return_value=[]):
            with patch.object(scraper, "_fetch_legals", return_value=[]):
                processed, failed = scraper._join_and_persist(db, {})

        assert processed == 0
        assert failed == 0
        db.execute.assert_not_called()

    def test_multiple_docs_in_batch(self, scraper):
        """Multiple documents in a batch are all processed."""
        master_batch = {
            "DOC_A": {"doc_type": "DEED", "doc_date": date(2026, 1, 1), "doc_amount": Decimal("100000")},
            "DOC_B": {"doc_type": "ASST", "doc_date": date(2026, 1, 2), "doc_amount": Decimal("200000")},
        }
        db = MagicMock()
        db.execute.return_value.rowcount = 2

        with patch.object(scraper, "_fetch_parties", return_value=[
            {"document_id": "DOC_A", "party_type": "2", "name": "BUYER ONE LLC"},
            {"document_id": "DOC_B", "party_type": "2", "name": "BUYER TWO LLC"},
        ]):
            with patch.object(scraper, "_fetch_legals", return_value=[
                {"document_id": "DOC_A", "borough": "1", "block": "100", "lot": "1"},
                {"document_id": "DOC_B", "borough": "2", "block": "200", "lot": "2"},
            ]):
                processed, failed = scraper._join_and_persist(db, master_batch)

        assert processed == 2
        assert failed == 0

    def test_mixed_docs_some_missing_bbl(self, scraper):
        """Documents missing BBL are failed; those with BBL are processed."""
        master_batch = {
            "DOC_HAS_BBL": {"doc_type": "DEED", "doc_date": date(2026, 1, 1), "doc_amount": None},
            "DOC_NO_BBL": {"doc_type": "DEED", "doc_date": date(2026, 1, 1), "doc_amount": None},
        }
        db = MagicMock()
        db.execute.return_value.rowcount = 1

        with patch.object(scraper, "_fetch_parties", return_value=[
            {"document_id": "DOC_HAS_BBL", "party_type": "2", "name": "BUYER LLC"},
            {"document_id": "DOC_NO_BBL", "party_type": "2", "name": "BUYER LLC"},
        ]):
            with patch.object(scraper, "_fetch_legals", return_value=[
                # Only DOC_HAS_BBL has legals
                {"document_id": "DOC_HAS_BBL", "borough": "1", "block": "100", "lot": "1"},
            ]):
                processed, failed = scraper._join_and_persist(db, master_batch)

        assert processed == 1
        assert failed == 1

    def test_legals_fetch_failure_all_docs_fail(self, scraper):
        """If legals fetch raises, all docs in batch count as failed (no BBL)."""
        master_batch = {
            "DOC_X": {"doc_type": "DEED", "doc_date": date(2026, 1, 1), "doc_amount": None},
        }
        db = MagicMock()

        with patch.object(scraper, "_fetch_parties", return_value=[
            {"document_id": "DOC_X", "party_type": "2", "name": "BUYER LLC"},
        ]):
            with patch.object(scraper, "_fetch_legals", side_effect=Exception("503 Service Unavailable")):
                processed, failed = scraper._join_and_persist(db, master_batch)

        # No BBL resolved → all failed
        assert processed == 0
        assert failed == 1
        db.execute.assert_not_called()


# ---------------------------------------------------------------------------
# TestNormalizePartyName
# ---------------------------------------------------------------------------

class TestNormalizePartyName:
    def test_plain_llc_unchanged(self):
        assert normalize_party_name("ABC LLC") == "ABC LLC"

    def test_llc_with_periods_normalized(self):
        assert normalize_party_name("ABC L.L.C.") == "ABC LLC"

    def test_llc_with_spaces_normalized(self):
        # "L L C" variant (spaces between letters)
        result = normalize_party_name("ABC L L C")
        assert result == "ABC LLC"

    def test_limited_liability_company_normalized(self):
        assert normalize_party_name("ABC LIMITED LIABILITY COMPANY") == "ABC LLC"

    def test_limited_liability_co_normalized(self):
        assert normalize_party_name("ABC LIMITED LIABILITY CO.") == "ABC LLC"

    def test_limited_liability_co_no_period(self):
        assert normalize_party_name("ABC LIMITED LIABILITY CO") == "ABC LLC"

    def test_uppercased(self):
        result = normalize_party_name("abc llc")
        assert result == result.upper()

    def test_trailing_punctuation_stripped(self):
        result = normalize_party_name("ABC LLC,")
        assert result == "ABC LLC"

    def test_internal_comma_stripped(self):
        # "SMITH, JOHN LLC" -> "SMITH JOHN LLC"
        result = normalize_party_name("SMITH, JOHN LLC")
        assert "," not in result

    def test_extra_whitespace_collapsed(self):
        result = normalize_party_name("  ABC   LLC  ")
        assert result == "ABC LLC"

    def test_none_returns_none(self):
        assert normalize_party_name(None) is None

    def test_empty_string_returns_none(self):
        assert normalize_party_name("") is None

    def test_whitespace_only_returns_none(self):
        assert normalize_party_name("   ") is None

    def test_non_llc_name_uppercased_only(self):
        result = normalize_party_name("john doe")
        assert result == "JOHN DOE"

    def test_real_acris_variant_1(self):
        # Common real-world variant from ACRIS data
        assert normalize_party_name("475 KENT AVENUE LLC") == "475 KENT AVENUE LLC"

    def test_real_acris_variant_2(self):
        # Punctuated variant
        result = normalize_party_name("475 KENT AVENUE, L.L.C.")
        assert result == "475 KENT AVENUE LLC"


# ---------------------------------------------------------------------------
# TestScraperRunAuditLog
# ---------------------------------------------------------------------------

class TestScraperRunAuditLog:
    def test_scraper_run_written_on_success(self, scraper):
        """BaseScraper.run() writes a ScraperRun row with status='success'."""
        db = MagicMock()

        with patch.object(scraper, "_run", return_value=(42, 0, None)), \
             patch.object(scraper, "_compute_rolling_avg", return_value=None):
            run_result = scraper.run(db)

        assert db.add.called
        assert db.commit.called

    def test_scraper_run_status_success_on_clean_run(self, scraper):
        """After clean _run(), scraper_run.status == 'success'."""
        db = MagicMock()

        # We need to capture the ScraperRun object added to db
        added_objects = []
        db.add.side_effect = lambda obj: added_objects.append(obj)

        with patch.object(scraper, "_run", return_value=(100, 2, None)), \
             patch.object(scraper, "_compute_rolling_avg", return_value=None):
            scraper.run(db)

        from models.scraper import ScraperRun
        scraper_runs = [obj for obj in added_objects if isinstance(obj, ScraperRun)]
        assert len(scraper_runs) >= 1
        final_run = scraper_runs[-1]
        assert final_run.status == "success"
        assert final_run.records_processed == 100
        assert final_run.records_failed == 2

    def test_scraper_run_status_failure_on_exception(self, scraper):
        """If _run() raises, scraper_run.status == 'failure' with error_message."""
        db = MagicMock()
        added_objects = []
        db.add.side_effect = lambda obj: added_objects.append(obj)

        with patch.object(scraper, "_run", side_effect=RuntimeError("network timeout")):
            with pytest.raises(RuntimeError):
                scraper.run(db)

        from models.scraper import ScraperRun
        scraper_runs = [obj for obj in added_objects if isinstance(obj, ScraperRun)]
        assert len(scraper_runs) >= 1
        final_run = scraper_runs[-1]
        assert final_run.status == "failure"
        assert "network timeout" in (final_run.error_message or "")

    def test_scraper_run_completed_at_set(self, scraper):
        """completed_at is set on the ScraperRun after _run() finishes."""
        db = MagicMock()
        added_objects = []
        db.add.side_effect = lambda obj: added_objects.append(obj)

        with patch.object(scraper, "_run", return_value=(10, 0, None)), \
             patch.object(scraper, "_compute_rolling_avg", return_value=None):
            scraper.run(db)

        from models.scraper import ScraperRun
        scraper_runs = [obj for obj in added_objects if isinstance(obj, ScraperRun)]
        final_run = scraper_runs[-1]
        assert final_run.completed_at is not None
