"""
Unit tests for the HPD housing violations scraper — scrapers/violations.py.

Tests cover:
  - Scraper configuration (SCRAPER_NAME, DATASET_ID)
  - _parse(): valid record, missing violationid, missing inspectiondate
  - _upsert_batch(): batch deduplication on violation_id
  - Regression: DB error in _upsert_batch() must write status='failure', not leave 'running'
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date, datetime, timezone


@pytest.fixture()
def violations_scraper():
    with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
        from scrapers.violations import ViolationsScraper
        return ViolationsScraper()


# ---------------------------------------------------------------------------
# Scraper config
# ---------------------------------------------------------------------------

class TestViolationsScraperConfig:
    def test_scraper_name(self):
        from scrapers.violations import ViolationsScraper
        assert ViolationsScraper.SCRAPER_NAME == "hpd_violations"

    def test_dataset_id(self):
        from scrapers.violations import ViolationsScraper
        assert ViolationsScraper.DATASET_ID == "wvxf-dwi5"


# ---------------------------------------------------------------------------
# _parse()
# ---------------------------------------------------------------------------

class TestViolationsScraperParse:
    def _raw(self, **overrides):
        base = {
            "violationid": "12345",
            "bbl": "1000470001",
            "housenumber": "100",
            "streetname": "MAIN ST",
            "zip": "10001",
            "boro": "MANHATTAN",
            "class": "B",
            "novdescription": "Peeling paint in unit",
            "inspectiondate": "2026-01-15T00:00:00.000",
            "novissueddate": "2026-01-16T00:00:00.000",
            "currentstatus": "Open",
        }
        base.update(overrides)
        return base

    def test_valid_record_parsed(self, violations_scraper):
        db = MagicMock()
        result = violations_scraper._parse(db, self._raw())
        assert result is not None
        assert result["violation_id"] == "12345"
        assert result["violation_class"] == "B"
        assert result["inspection_date"] == date(2026, 1, 15)

    def test_missing_violationid_quarantined(self, violations_scraper):
        db = MagicMock()
        quarantined = []
        with patch.object(violations_scraper, "quarantine",
                          side_effect=lambda db, r, reason: quarantined.append(reason)):
            result = violations_scraper._parse(db, self._raw(violationid=None))
        assert result is None
        assert any("violationid" in r for r in quarantined)

    def test_missing_inspectiondate_quarantined(self, violations_scraper):
        db = MagicMock()
        quarantined = []
        with patch.object(violations_scraper, "quarantine",
                          side_effect=lambda db, r, reason: quarantined.append(reason)):
            result = violations_scraper._parse(db, self._raw(inspectiondate=None))
        assert result is None
        assert any("inspectiondate" in r for r in quarantined)

    def test_invalid_class_quarantined(self, violations_scraper):
        db = MagicMock()
        quarantined = []
        with patch.object(violations_scraper, "quarantine",
                          side_effect=lambda db, r, reason: quarantined.append(reason)):
            result = violations_scraper._parse(db, self._raw(**{"class": "X"}))
        assert result is None
        assert any("invalid_violation_class" in r for r in quarantined)


# ---------------------------------------------------------------------------
# _upsert_batch() deduplication
# ---------------------------------------------------------------------------

class TestViolationsUpsertBatch:
    def test_deduplicates_within_batch(self, violations_scraper):
        """Duplicate violation_id in a batch keeps only the last occurrence."""
        db = MagicMock()
        db.execute.return_value.rowcount = 1

        row_a = {
            "violation_id": "99", "bbl": None, "address": None, "zip_code": None,
            "borough": None, "violation_class": "B", "description": None,
            "inspection_date": date(2026, 1, 1), "nov_issued_date": None,
            "current_status": "Open", "raw_data": {},
        }
        row_b = {**row_a, "current_status": "Closed"}  # same id, later status

        violations_scraper._upsert_batch(db, [row_a, row_b])

        # db.execute called once with a single-row VALUES clause (deduped)
        call_args = db.execute.call_args
        stmt = call_args[0][0]
        # The compiled statement should reference only one row for violation_id "99"
        assert db.execute.call_count == 1


# ---------------------------------------------------------------------------
# Regression: DB error in _upsert_batch() must write failure status
# ---------------------------------------------------------------------------

class TestViolationsFailurePath:
    def test_db_error_in_upsert_writes_failure_status(self, violations_scraper):
        """
        Regression: if _upsert_batch() raises a DataError, run() must write
        status='failure'.  The old code called db.get() before db.rollback(),
        which raised InFailedSqlTransaction on the aborted connection and left
        the ScraperRun row stuck at status='running'.
        """
        from sqlalchemy.exc import DataError
        from models.scraper import ScraperRun

        fresh_run = ScraperRun(scraper_name="hpd_violations", status="running")

        db = MagicMock()
        db.get.return_value = fresh_run

        raw_record = {
            "violationid": "55555",
            "bbl": "1000470001",
            "housenumber": "100",
            "streetname": "MAIN ST",
            "zip": "10001",
            "boro": "MANHATTAN",
            "class": "C",
            "novdescription": "No heat",
            "inspectiondate": "2026-02-01T00:00:00.000",
            "novissueddate": None,
            "currentstatus": "Open",
        }

        # Simulate DataError on execute (the _upsert_batch INSERT)
        db.execute.side_effect = DataError("stmt", {}, Exception("column too long"))

        with patch.object(violations_scraper, "build_where_since",
                          return_value="inspectiondate > '2026-01-01T00:00:00.000'"), \
             patch.object(violations_scraper, "paginate",
                          return_value=iter([raw_record])), \
             patch("scrapers.base.SCRAPER_EXPECTED_MIN_RECORDS", {}):
            with pytest.raises(DataError):
                violations_scraper.run(db)

        assert fresh_run.status == "failure", (
            "HPD run() must write status='failure' when _upsert_batch() raises DataError"
        )
        assert fresh_run.error_message is not None
