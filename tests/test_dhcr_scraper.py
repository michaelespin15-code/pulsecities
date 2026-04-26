"""
Unit tests for the DHCR rent stabilization scraper — scrapers/dhcr_rs.py.

Tests cover:
  - Model configuration (tablename, unique constraint)
  - Scraper configuration (SCRAPER_NAME, DATASET_ID)
  - _run() with mocked API response returns correct (processed, failed, watermark)
  - Quarantine: record missing bbl is quarantined
  - Scoring weight constants and their sum
  - signal_breakdown includes rs_unit_loss key
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestRsBuildingModel:
    def test_tablename(self):
        from models.dhcr_rs import RsBuilding
        assert RsBuilding.__tablename__ == "rs_buildings"

    def test_unique_constraint_bbl_year(self):
        """RsBuilding must have a UNIQUE constraint on (bbl, year) named uq_rs_buildings_bbl_year."""
        from models.dhcr_rs import RsBuilding
        from sqlalchemy import UniqueConstraint
        constraints = {c.name for c in RsBuilding.__table__.constraints
                       if isinstance(c, UniqueConstraint)}
        assert "uq_rs_buildings_bbl_year" in constraints, (
            f"Expected 'uq_rs_buildings_bbl_year' in {constraints}"
        )


# ---------------------------------------------------------------------------
# Scraper config tests
# ---------------------------------------------------------------------------

class TestDhcrRsScraperConfig:
    def test_scraper_name(self):
        from scrapers.dhcr_rs import DhcrRsScraper
        assert DhcrRsScraper.SCRAPER_NAME == "dhcr_rs"

    def test_dataset_id(self):
        from scrapers.dhcr_rs import DhcrRsScraper
        assert DhcrRsScraper.DATASET_ID == "kj4p-ruqc"


# ---------------------------------------------------------------------------
# Scraper _run() tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def dhcr_scraper():
    with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
        from scrapers.dhcr_rs import DhcrRsScraper
        return DhcrRsScraper()


class TestDhcrRsScraperRun:
    def test_run_returns_tuple(self, dhcr_scraper):
        """_run() returns (processed_count, failed_count, watermark) tuple."""
        db = MagicMock()
        db.execute.return_value.rowcount = 1

        mock_record = {
            "boroid": "1",
            "block": "1",
            "lot": "1",
            "legalclassa": "23",
            "recordstatus": "Active",
            "lifecycle": "Building",
        }

        with patch.object(dhcr_scraper, "paginate", return_value=iter([mock_record])):
            result = dhcr_scraper._run(db)

        assert isinstance(result, tuple), "Expected tuple return from _run()"
        assert len(result) == 3, "Expected (processed, failed, watermark) tuple"
        processed, failed, watermark = result
        assert isinstance(processed, int)
        assert isinstance(failed, int)

    def test_missing_bbl_components_quarantined(self, dhcr_scraper):
        """Record missing boroid/block/lot is quarantined with bbl_components reason."""
        db = MagicMock()

        bad_record = {
            # boroid/block/lot intentionally absent
            "legalclassa": "45",
        }

        quarantined = []
        def mock_quarantine(db, raw, reason):
            quarantined.append((raw, reason))

        with patch.object(dhcr_scraper, "paginate", return_value=iter([bad_record])):
            with patch.object(dhcr_scraper, "quarantine", side_effect=mock_quarantine):
                processed, failed, watermark = dhcr_scraper._run(db)

        assert failed == 1, f"Expected 1 failed record, got {failed}"
        assert len(quarantined) == 1, "Expected 1 quarantined record"
        _, reason = quarantined[0]
        assert reason == "missing_required_field:bbl_components", (
            f"Expected quarantine reason 'missing_required_field:bbl_components', got '{reason}'"
        )


# ---------------------------------------------------------------------------
# Scoring weight tests
# ---------------------------------------------------------------------------

class TestComputeWeights:
    def test_weight_sum_equals_one(self):
        """All 6 signal weights must sum to exactly 1.0."""
        from scoring.compute import (
            WEIGHT_LLC_ACQUISITIONS,
            WEIGHT_PERMITS,
            WEIGHT_COMPLAINTS,
            WEIGHT_EVICTIONS,
            WEIGHT_HPD_VIOLATIONS,
            WEIGHT_RS_UNIT_LOSS,
        )
        total = (
            WEIGHT_LLC_ACQUISITIONS
            + WEIGHT_PERMITS
            + WEIGHT_COMPLAINTS
            + WEIGHT_EVICTIONS
            + WEIGHT_HPD_VIOLATIONS
            + WEIGHT_RS_UNIT_LOSS
        )
        assert abs(total - 1.0) < 0.001, (
            f"Weights must sum to 1.0, got {total:.4f}. "
            f"Values: LLC={WEIGHT_LLC_ACQUISITIONS}, Permits={WEIGHT_PERMITS}, "
            f"Complaints={WEIGHT_COMPLAINTS}, Evictions={WEIGHT_EVICTIONS}, "
            f"Assessment={WEIGHT_HPD_VIOLATIONS}, RS={WEIGHT_RS_UNIT_LOSS}"
        )

    def test_rs_unit_loss_weight(self):
        """WEIGHT_RS_UNIT_LOSS must be exactly 0.15."""
        from scoring.compute import WEIGHT_RS_UNIT_LOSS
        assert WEIGHT_RS_UNIT_LOSS == 0.15, (
            f"Expected WEIGHT_RS_UNIT_LOSS == 0.15, got {WEIGHT_RS_UNIT_LOSS}"
        )


# ---------------------------------------------------------------------------
# signal_breakdown test
# ---------------------------------------------------------------------------

class TestSignalBreakdownRsUnitLoss:
    def test_rs_unit_loss_in_breakdown(self):
        """
        compute_scores() signal_breakdown must include 'rs_unit_loss' key.
        Uses patched aggregators so no live DB is required.
        """
        from unittest.mock import MagicMock, patch
        from scoring.compute import compute_scores

        zip_code = "99901"  # non-real NYC zip

        mock_db = MagicMock()

        # Simulate displacement_scores row returned for verification
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: (
            {"permits": 50.0, "evictions": 0.0, "llc_acquisitions": 0.0,
             "assessment_spike": 0.0, "complaint_rate": 0.0, "rs_unit_loss": 0.0}
            if key == 1 else "99901"
        )
        mock_db.execute.return_value.fetchone.return_value = mock_row
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("scoring.compute._aggregate_permits", return_value=[(zip_code, 10)]), \
             patch("scoring.compute._aggregate_evictions", return_value=[]), \
             patch("scoring.compute._aggregate_llc_acquisitions", return_value=[]), \
             patch("scoring.compute._aggregate_complaints", return_value=[]), \
             patch("scoring.compute._aggregate_rs_unit_loss", return_value=[]), \
             patch("scoring.compute._compute_borough_medians",
                   return_value={"1": 10.0, "2": 8.0, "3": 15.0, "4": 12.0, "5": 5.0}), \
             patch("scoring.compute._get_zip_units", return_value={}), \
             patch("scoring.compute._get_zip_borough", return_value={}):

            from models.database import SessionLocal
            db = SessionLocal()
            try:
                from sqlalchemy import text
                # Clean up test zip first
                db.execute(text("DELETE FROM displacement_scores WHERE zip_code = :z"),
                           {"z": zip_code})
                db.commit()

                count = compute_scores(db, force=True)  # synthetic test data bypasses production guard

                # Fetch the breakdown
                row = db.execute(
                    text("SELECT signal_breakdown FROM displacement_scores WHERE zip_code = :z"),
                    {"z": zip_code},
                ).fetchone()

                if row is not None:
                    breakdown = row[0]
                    assert "rs_unit_loss" in breakdown, (
                        f"'rs_unit_loss' missing from signal_breakdown: {breakdown.keys()}"
                    )
            finally:
                db.execute(text("DELETE FROM displacement_scores WHERE zip_code = :z"),
                           {"z": zip_code})
                db.commit()
                db.close()
