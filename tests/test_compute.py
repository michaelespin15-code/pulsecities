"""
Integration tests for displacement score computation — scoring/compute.py.

These are integration tests that use a real database connection (SessionLocal).
Each test inserts synthetic permit data into permits_raw, calls compute_scores,
asserts on the result, then deletes the test data to leave no side effects.

Test zip codes use 99xxx range (non-NYC) to avoid colliding with real permit data.

Normalization contract (five-signal composite):
  - Single zip code: composite score is clamped to [1, 100]; signal components are 50.0
  - per-unit normalization uses borough median as denominator when units_res null/0
  - signal_breakdown stores normalized [0–100] floats, NOT raw event counts
  - Two calls produce the same scores and row count (idempotent)
"""

import pytest
from sqlalchemy import text
from unittest.mock import patch

from models.database import SessionLocal



# ---------------------------------------------------------------------------
# Unique marker for test data so cleanup is safe
# ---------------------------------------------------------------------------
TEST_ZIP_PREFIX = "99"  # non-real NYC zips — won't collide with production data


def _insert_test_permits(db, rows):
    """
    Insert minimal permit rows into permits_raw for testing.

    rows: list of (zip_code, filing_date_str) tuples.
    filing_date_str: ISO date string, e.g. "2026-01-15".
    """
    for zip_code, filing_date in rows:
        db.execute(
            text(
                """
                INSERT INTO permits_raw
                    (bbl, zip_code, filing_date, permit_type, work_type,
                     raw_data, created_at, updated_at)
                VALUES
                    (:bbl, :zip_code, :filing_date, 'NB', 'OT', '{}', NOW(), NOW())
                ON CONFLICT DO NOTHING
                """
            ),
            {
                "bbl": f"99{zip_code}001",
                "zip_code": zip_code,
                "filing_date": filing_date,
            },
        )
    db.commit()


def _cleanup_test_data(db, zip_codes):
    """Remove test data from permits_raw, displacement_scores, score_history, and neighborhoods."""
    for zip_code in zip_codes:
        db.execute(
            text("DELETE FROM permits_raw WHERE zip_code = :zip_code"),
            {"zip_code": zip_code},
        )
        db.execute(
            text("DELETE FROM displacement_scores WHERE zip_code = :zip_code"),
            {"zip_code": zip_code},
        )
        db.execute(
            text("DELETE FROM score_history WHERE zip_code = :zip_code"),
            {"zip_code": zip_code},
        )
        db.execute(
            text("DELETE FROM neighborhoods WHERE zip_code = :zip_code"),
            {"zip_code": zip_code},
        )
    db.commit()


def _insert_test_neighborhoods(db, zip_codes):
    """
    Register test zip codes in neighborhoods so compute_scores step 9 (orphan
    cleanup) does not delete freshly-scored rows before the test can assert on them.
    geometry is nullable; name/borough are optional.
    """
    for zip_code in zip_codes:
        db.execute(
            text(
                """
                INSERT INTO neighborhoods (zip_code, created_at, updated_at)
                VALUES (:zip_code, NOW(), NOW())
                ON CONFLICT DO NOTHING
                """
            ),
            {"zip_code": zip_code},
        )
    db.commit()


# ---------------------------------------------------------------------------
# Fixture: import compute_scores lazily so missing module fails at collection
# ---------------------------------------------------------------------------
@pytest.fixture()
def compute_scores():
    """Import compute_scores from scoring.compute (skips gracefully if not yet implemented)."""
    scoring_compute = pytest.importorskip(
        "scoring.compute",
        reason="scoring/compute.py not yet implemented — plan 02-02",
    )
    return scoring_compute.compute_scores


# ---------------------------------------------------------------------------
# Helper: generate a unique BBL for each (zip, index) pair
# ---------------------------------------------------------------------------
def _bbl(zip_code: str, idx: int) -> str:
    # Keep within 10-char limit; zip is 5 chars, idx up to 5 digits
    return f"{zip_code}{str(idx).zfill(5)}"


def _rows_for_zip(zip_code: str, count: int, base_date: str = "2026-01-15"):
    """Return `count` (zip_code, filing_date) pairs for the given zip."""
    return [(zip_code, base_date)] * count


# Shared patch list for all five aggregators + borough helpers — used to
# isolate tests from live DB data while testing normalization math.
_EMPTY_FIVE_SIGNALS = {
    "scoring.compute._aggregate_evictions": [],
    "scoring.compute._aggregate_llc_acquisitions": [],
    "scoring.compute._aggregate_complaints": [],
    "scoring.compute._compute_borough_medians": {
        "1": 10.0, "2": 8.0, "3": 15.0, "4": 12.0, "5": 5.0
    },
    "scoring.compute._get_zip_units": {},
    "scoring.compute._get_zip_borough": {},
}


@pytest.mark.integration
class TestScoreNormalization:
    def test_score_single_zip(self, compute_scores):
        """
        When only 1 zip code has permit data and all other signals are empty,
        the permit signal normalizes to 50.0 (min==max guard), and the
        composite score is within [1, 100].

        Verifies: _normalize guard fires when min == max, composite is clamped.
        """
        zip_code = "99001"
        db = SessionLocal()
        try:
            _cleanup_test_data(db, [zip_code])

            # Patch all six signals: only permits has data (single zip → 0.0-guard → 50.0 normalized).
            # assessment_spike and rs_unit_loss are patched dormant so the dormancy
            # check fires correctly and their weights are redistributed to permits.
            _insert_test_neighborhoods(db, [zip_code])
            with patch("scoring.compute._aggregate_permits", return_value=[(zip_code, 25)]), \
                 patch("scoring.compute._aggregate_evictions", return_value=[]), \
                 patch("scoring.compute._aggregate_llc_acquisitions", return_value=[]), \
                 patch("scoring.compute._aggregate_complaints", return_value=[]), \
                 patch("scoring.compute._aggregate_violations", return_value=[]), \
                 patch("scoring.compute._aggregate_assessment_spike", return_value=[]), \
                 patch("scoring.compute._aggregate_rs_unit_loss", return_value=[]), \
                 patch("scoring.compute._compute_borough_medians", return_value={"1": 10.0, "2": 8.0, "3": 15.0, "4": 12.0, "5": 5.0}), \
                 patch("scoring.compute._get_zip_units", return_value={}), \
                 patch("scoring.compute._get_zip_borough", return_value={}):
                count = compute_scores(db, force=True)

            assert count == 1, f"Expected 1 zip scored, got {count}"

            row = db.execute(
                text("SELECT score, signal_breakdown FROM displacement_scores WHERE zip_code = :z"),
                {"z": zip_code},
            ).fetchone()
            assert row is not None, "No displacement_scores row written"
            score = row[0]
            breakdown = row[1]
            # Single zip, only permits active → permits normalized to 50.0 (zero-guard),
            # all other signals 0.0 (dormant). Only permits weight is active (redistributed
            # to 1.0), so composite = 1.0 * 50.0 = 50.0.
            assert 1.0 <= score <= 100.0, f"Score {score} is outside [1, 100]"
            assert breakdown["permits"] == 50.0, f"Expected permits breakdown 50.0, got {breakdown['permits']}"
            assert breakdown["hpd_violations"] == 0.0, (
                f"Dormant hpd_violations should be 0.0 when no violations data, got {breakdown['hpd_violations']}"
            )
            assert breakdown["rs_unit_loss"] == 0.0, (
                f"Dormant rs_unit_loss should be 0.0, got {breakdown['rs_unit_loss']}"
            )
        finally:
            _cleanup_test_data(db, [zip_code])
            db.close()

    def test_score_normalization(self, compute_scores):
        """
        Three zip codes with raw permit counts 10, 30, 50 (and no other signals)
        must produce permits breakdown values 0.0, 50.0, 100.0 via linear min-max.
        Composite scores are the weighted permit contribution — all in [1, 100].

        Uses full patch of all five aggregators to isolate normalization math.
        """
        zips = [("99011", 10), ("99012", 30), ("99013", 50)]
        zip_codes = [z[0] for z in zips]
        db = SessionLocal()
        try:
            _cleanup_test_data(db, zip_codes)

            _insert_test_neighborhoods(db, zip_codes)
            with patch("scoring.compute._aggregate_permits", return_value=zips), \
                 patch("scoring.compute._aggregate_evictions", return_value=[]), \
                 patch("scoring.compute._aggregate_llc_acquisitions", return_value=[]), \
                 patch("scoring.compute._aggregate_complaints", return_value=[]), \
                 patch("scoring.compute._aggregate_violations", return_value=[]), \
                 patch("scoring.compute._aggregate_assessment_spike", return_value=[]), \
                 patch("scoring.compute._aggregate_rs_unit_loss", return_value=[]), \
                 patch("scoring.compute._compute_borough_medians", return_value={"1": 10.0, "2": 8.0, "3": 15.0, "4": 12.0, "5": 5.0}), \
                 patch("scoring.compute._get_zip_units", return_value={}), \
                 patch("scoring.compute._get_zip_borough", return_value={}):
                count = compute_scores(db, force=True)

            assert count == 3, f"Expected 3 zips scored, got {count}"

            rows = db.execute(
                text(
                    "SELECT zip_code, score, signal_breakdown FROM displacement_scores "
                    "WHERE zip_code = ANY(:zips) ORDER BY score"
                ),
                {"zips": zip_codes},
            ).fetchall()

            breakdown_by_zip = {r[0]: r[2] for r in rows}
            # Permit signal normalized to 0.0, 50.0, 100.0 via min-max
            assert breakdown_by_zip["99011"]["permits"] == 0.0, (
                f"Got permits breakdown {breakdown_by_zip['99011']['permits']}"
            )
            assert breakdown_by_zip["99012"]["permits"] == 50.0, (
                f"Got permits breakdown {breakdown_by_zip['99012']['permits']}"
            )
            assert breakdown_by_zip["99013"]["permits"] == 100.0, (
                f"Got permits breakdown {breakdown_by_zip['99013']['permits']}"
            )
        finally:
            _cleanup_test_data(db, zip_codes)
            db.close()

    def test_score_empty_data(self, compute_scores):
        """
        When no permit data exists in the relevant window,
        compute_scores must return 0 (no rows written).
        """
        db = SessionLocal()
        # We rely on there being no permits with zip_codes in our test prefix
        # that are within the past 365 days AND have no cleanup needed.
        # To be safe, ensure no 99xxx rows exist first.
        _cleanup_test_data(db, ["99001", "99011", "99012", "99013", "99021", "99031"])
        try:
            # For empty data test, patch all five aggregators to return empty
            from unittest.mock import MagicMock

            mock_db = MagicMock()
            mock_db.execute.return_value.fetchall.return_value = []
            mock_db.execute.return_value.scalar.return_value = 0

            with patch("scoring.compute._aggregate_permits", return_value=[]), \
                 patch("scoring.compute._aggregate_evictions", return_value=[]), \
                 patch("scoring.compute._aggregate_llc_acquisitions", return_value=[]), \
                 patch("scoring.compute._aggregate_complaints", return_value=[]), \
                 patch("scoring.compute._aggregate_rs_unit_loss", return_value=[]):
                result = compute_scores(mock_db)

            assert result == 0, f"Expected 0 for empty data, got {result}"
        finally:
            db.close()

    def test_signal_breakdown_has_permits(self, compute_scores):
        """
        The signal_breakdown JSON for every scored zip must contain a 'permits' key
        whose value is a normalized float in [0.0, 100.0] — not the raw count.
        All five keys must be present.
        """
        zip_code = "99021"
        db = SessionLocal()
        try:
            _cleanup_test_data(db, [zip_code])
            for i in range(12):
                db.execute(
                    text(
                        """
                        INSERT INTO permits_raw
                            (bbl, zip_code, filing_date, permit_type, work_type,
                             raw_data, created_at, updated_at)
                        VALUES
                            (:bbl, :zip_code, '2026-01-15', 'NB', 'OT', '{}', NOW(), NOW())
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {"bbl": _bbl(zip_code, i), "zip_code": zip_code},
                )
            db.commit()

            # Patch other signals to isolate; permits normalized to 50.0 (single zip min==max guard).
            # _aggregate_permits is patched directly because its JOIN on parcels requires
            # residential parcel rows that aren't part of this test fixture.
            _insert_test_neighborhoods(db, [zip_code])
            with patch("scoring.compute._aggregate_permits", return_value=[(zip_code, 12)]), \
                 patch("scoring.compute._aggregate_evictions", return_value=[]), \
                 patch("scoring.compute._aggregate_llc_acquisitions", return_value=[]), \
                 patch("scoring.compute._aggregate_complaints", return_value=[]), \
                 patch("scoring.compute._aggregate_violations", return_value=[]), \
                 patch("scoring.compute._aggregate_rs_unit_loss", return_value=[]), \
                 patch("scoring.compute._compute_borough_medians", return_value={"1": 10.0, "2": 8.0, "3": 15.0, "4": 12.0, "5": 5.0}), \
                 patch("scoring.compute._get_zip_units", return_value={}), \
                 patch("scoring.compute._get_zip_borough", return_value={}):
                compute_scores(db, force=True)

            row = db.execute(
                text(
                    "SELECT signal_breakdown FROM displacement_scores WHERE zip_code = :z"
                ),
                {"z": zip_code},
            ).fetchone()
            assert row is not None, "No displacement_scores row written"
            breakdown = row[0]
            # All six keys must be present
            for key in ("permits", "evictions", "llc_acquisitions", "hpd_violations", "complaint_rate", "rs_unit_loss"):
                assert key in breakdown, f"'{key}' key missing from signal_breakdown: {breakdown}"
                val = breakdown[key]
                assert isinstance(val, (int, float)), f"{key} is not numeric: {val}"
                assert 0.0 <= float(val) <= 100.0, f"{key}={val} is outside [0, 100]"
        finally:
            _cleanup_test_data(db, [zip_code])
            db.close()

    def test_idempotent(self, compute_scores):
        """
        Calling compute_scores twice produces the same score and the same number
        of rows (upsert, not double-insert).
        """
        zip_code = "99031"
        db = SessionLocal()
        try:
            _cleanup_test_data(db, [zip_code])
            for i in range(5):
                db.execute(
                    text(
                        """
                        INSERT INTO permits_raw
                            (bbl, zip_code, filing_date, permit_type, work_type,
                             raw_data, created_at, updated_at)
                        VALUES
                            (:bbl, :zip_code, '2026-01-15', 'NB', 'OT', '{}', NOW(), NOW())
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {"bbl": _bbl(zip_code, i), "zip_code": zip_code},
                )
            db.commit()

            # Patch all signal aggregators for determinism. _aggregate_permits is patched
            # because its JOIN on parcels requires residential parcel rows absent here.
            patches = dict(
                _aggregate_permits=[(zip_code, 5)],
                _aggregate_evictions=[],
                _aggregate_llc_acquisitions=[],
                _aggregate_complaints=[],
                _aggregate_violations=[],
                _aggregate_rs_unit_loss=[],
                _compute_borough_medians={"1": 10.0, "2": 8.0, "3": 15.0, "4": 12.0, "5": 5.0},
                _get_zip_units={},
                _get_zip_borough={},
            )
            _insert_test_neighborhoods(db, [zip_code])
            with patch("scoring.compute._aggregate_permits", return_value=patches["_aggregate_permits"]), \
                 patch("scoring.compute._aggregate_evictions", return_value=patches["_aggregate_evictions"]), \
                 patch("scoring.compute._aggregate_llc_acquisitions", return_value=patches["_aggregate_llc_acquisitions"]), \
                 patch("scoring.compute._aggregate_complaints", return_value=patches["_aggregate_complaints"]), \
                 patch("scoring.compute._aggregate_violations", return_value=patches["_aggregate_violations"]), \
                 patch("scoring.compute._aggregate_rs_unit_loss", return_value=patches["_aggregate_rs_unit_loss"]), \
                 patch("scoring.compute._compute_borough_medians", return_value=patches["_compute_borough_medians"]), \
                 patch("scoring.compute._get_zip_units", return_value=patches["_get_zip_units"]), \
                 patch("scoring.compute._get_zip_borough", return_value=patches["_get_zip_borough"]):
                count1 = compute_scores(db, force=True)
                row1 = db.execute(
                    text(
                        "SELECT score, signal_breakdown FROM displacement_scores WHERE zip_code = :z"
                    ),
                    {"z": zip_code},
                ).fetchone()

                count2 = compute_scores(db, force=True)
                row2 = db.execute(
                    text(
                        "SELECT score, signal_breakdown FROM displacement_scores WHERE zip_code = :z"
                    ),
                    {"z": zip_code},
                ).fetchone()

            assert count1 == count2, f"Row count changed: {count1} vs {count2}"
            assert row1[0] == row2[0], f"Score changed: {row1[0]} vs {row2[0]}"
            assert row1[1] == row2[1], f"signal_breakdown changed: {row1[1]} vs {row2[1]}"

            # Verify only 1 row exists (upsert, not duplicate insert)
            total = db.execute(
                text(
                    "SELECT COUNT(*) FROM displacement_scores WHERE zip_code = :z"
                ),
                {"z": zip_code},
            ).fetchone()[0]
            assert total == 1, f"Expected 1 row, got {total} (double-insert bug)"
        finally:
            _cleanup_test_data(db, [zip_code])
            db.close()

    def test_five_signals_normalized(self, compute_scores):
        """
        All five signal values in signal_breakdown must be normalized floats in [0.0, 100.0],
        not raw event counts. Verifies SCOR-02: per-signal values are stored normalized.
        """
        zip_code = "99041"
        db = SessionLocal()
        try:
            _cleanup_test_data(db, [zip_code])
            # Patch all six aggregations to return known counts for one zip
            _insert_test_neighborhoods(db, [zip_code])
            with patch("scoring.compute._aggregate_permits", return_value=[(zip_code, 10)]), \
                 patch("scoring.compute._aggregate_evictions", return_value=[(zip_code, 5)]), \
                 patch("scoring.compute._aggregate_llc_acquisitions", return_value=[(zip_code, 3)]), \
                 patch("scoring.compute._aggregate_complaints", return_value=[(zip_code, 7)]), \
                 patch("scoring.compute._aggregate_violations", return_value=[]), \
                 patch("scoring.compute._aggregate_rs_unit_loss", return_value=[]), \
                 patch("scoring.compute._compute_borough_medians", return_value={"1": 20.0, "2": 15.0, "3": 18.0, "4": 12.0, "5": 8.0}), \
                 patch("scoring.compute._get_zip_units", return_value={}), \
                 patch("scoring.compute._get_zip_borough", return_value={}):
                count = compute_scores(db, force=True)
            assert count >= 1
            row = db.execute(
                text("SELECT signal_breakdown FROM displacement_scores WHERE zip_code = :z"),
                {"z": zip_code},
            ).fetchone()
            assert row is not None, "No displacement_scores row written"
            breakdown = row[0]
            for key in ("permits", "evictions", "llc_acquisitions", "hpd_violations", "complaint_rate", "rs_unit_loss"):
                assert key in breakdown, f"'{key}' missing from signal_breakdown"
                val = breakdown[key]
                assert isinstance(val, (int, float)), f"{key} is not numeric: {val}"
                assert 0.0 <= float(val) <= 100.0, f"{key}={val} is outside [0, 100]"
        finally:
            _cleanup_test_data(db, [zip_code])
            db.close()

    def test_zero_unit_fallback(self, compute_scores):
        """
        When a zip code's parcels all have units_res = null or 0,
        compute_scores must use the borough median as denominator — no ZeroDivisionError.
        Verifies SCOR-03: per-unit normalization with documented fallback.
        """
        zip_code = "99051"
        db = SessionLocal()
        try:
            _cleanup_test_data(db, [zip_code])
            # Provide a parcel with units_res = None to trigger the fallback path.
            # Borough medians dict has an entry for borough 3 (Brooklyn) = 20 units.
            _insert_test_neighborhoods(db, [zip_code])
            with patch("scoring.compute._aggregate_permits", return_value=[(zip_code, 8)]), \
                 patch("scoring.compute._aggregate_evictions", return_value=[]), \
                 patch("scoring.compute._aggregate_llc_acquisitions", return_value=[]), \
                 patch("scoring.compute._aggregate_complaints", return_value=[]), \
                 patch("scoring.compute._aggregate_rs_unit_loss", return_value=[]), \
                 patch("scoring.compute._compute_borough_medians", return_value={"1": 10.0, "2": 8.0, "3": 20.0, "4": 12.0, "5": 5.0}), \
                 patch("scoring.compute._get_zip_borough", return_value={zip_code: 3}), \
                 patch("scoring.compute._get_zip_units", return_value={zip_code: None}):
                try:
                    count = compute_scores(db, force=True)
                except ZeroDivisionError:
                    pytest.fail("ZeroDivisionError raised — borough median fallback not implemented")
            # A score row must exist and score must not be None
            row = db.execute(
                text("SELECT score FROM displacement_scores WHERE zip_code = :z"),
                {"z": zip_code},
            ).fetchone()
            assert row is not None, "No displacement_scores row written for zero-unit zip"
            assert row[0] is not None, "Score is None for zero-unit zip — expected fallback value"
        finally:
            _cleanup_test_data(db, [zip_code])
            db.close()

    def test_displacement_types_filter(self):
        """
        DISPLACEMENT_COMPLAINT_TYPES constant must exist in config/nyc.py with
        exactly 7 entries including the double-R spelling 'HARRASSMENT'.
        Verifies D-12: constant is importable and correct.
        """
        from config.nyc import DISPLACEMENT_COMPLAINT_TYPES
        assert len(DISPLACEMENT_COMPLAINT_TYPES) == 7, (
            f"Expected 7 displacement complaint types, got {len(DISPLACEMENT_COMPLAINT_TYPES)}"
        )
        assert "HARRASSMENT" in DISPLACEMENT_COMPLAINT_TYPES, (
            "HARRASSMENT (double-R) missing — must match NYC Open Data spelling"
        )
        assert "HEAT/HOT WATER" in DISPLACEMENT_COMPLAINT_TYPES
        assert "MOLD" in DISPLACEMENT_COMPLAINT_TYPES


def _insert_test_parcels(db, rows):
    """
    Insert minimal parcel rows into parcels for testing.
    rows: list of (zip_code, borough, units_res) tuples.

    Note: on_speculation_watch_list is NOT NULL with no server default — must supply False.
    """
    for idx, (zip_code, borough, units_res) in enumerate(rows):
        db.execute(
            text(
                """
                INSERT INTO parcels
                    (bbl, zip_code, borough, units_res,
                     on_speculation_watch_list, created_at, updated_at)
                VALUES
                    (:bbl, :zip_code, :borough, :units_res,
                     FALSE, NOW(), NOW())
                ON CONFLICT ON CONSTRAINT uq_parcels_bbl DO UPDATE SET
                    zip_code = EXCLUDED.zip_code,
                    borough = EXCLUDED.borough,
                    units_res = EXCLUDED.units_res
                """
            ),
            {
                "bbl": f"{zip_code}{str(idx).zfill(5)}",
                "zip_code": zip_code,
                "borough": borough,
                "units_res": units_res,
            },
        )
    db.commit()


def _cleanup_test_parcels(db, zip_codes):
    """Remove test parcel rows."""
    for zip_code in zip_codes:
        db.execute(
            text("DELETE FROM parcels WHERE zip_code = :zip_code AND bbl LIKE :prefix"),
            {"zip_code": zip_code, "prefix": f"{zip_code}%"},
        )
    db.commit()


@pytest.mark.integration
class TestScoreValidation:
    def test_distribution(self, compute_scores):
        """
        After per-unit normalization, residential zip codes with high event-per-unit
        rates should score strictly higher than commercial zip codes with low event-
        per-unit rates. Verifies SCOR-03 and D-21 (ANHD-derived weighting rationale).

        Seed design:
          - Residential Brooklyn/Bronx: 30 permits, 5 residential units -> 6.0 permits/unit
          - Commercial Manhattan: 100 permits, 200 residential units -> 0.5 permits/unit
          - Only permit signal is active (others patched empty) to isolate per-unit logic.

        Expected: min(residential scores) > max(commercial scores) after normalization.
        """
        from unittest.mock import patch

        # Test zip codes using 99xxx prefix to avoid production data collision
        residential_zips = [
            ("99207", 3, 5),   # East New York proxy — Brooklyn, 5 units
            ("99221", 3, 5),   # Bushwick proxy — Brooklyn, 5 units
            ("99454", 2, 5),   # Mott Haven proxy — Bronx, 5 units
        ]
        commercial_zips = [
            ("99036", 1, 200), # Midtown proxy — Manhattan, 200 units
            ("99007", 1, 200), # Lower Manhattan proxy — Manhattan, 200 units
        ]
        all_zip_rows = residential_zips + commercial_zips
        all_zip_codes = [z[0] for z in all_zip_rows]

        db = SessionLocal()
        try:
            # Clean slate
            _cleanup_test_data(db, all_zip_codes)
            _cleanup_test_parcels(db, all_zip_codes)

            # Seed parcels so _get_zip_units and _get_zip_borough can find them
            _insert_test_parcels(db, all_zip_rows)

            # Permit counts: residential gets 30 each, commercial gets 100 each
            # Per-unit: residential = 30/5 = 6.0, commercial = 100/200 = 0.5
            # Patch _aggregate_permits so only these 5 test zips drive normalization
            # (prevents contamination from real permits_raw data in the DB)
            permit_patch = (
                [(z, 30) for z, _, _ in residential_zips]
                + [(z, 100) for z, _, _ in commercial_zips]
            )

            _insert_test_neighborhoods(db, all_zip_codes)
            # Patch all signals to known values — permits only, isolated from real DB
            with patch("scoring.compute._aggregate_permits", return_value=permit_patch), \
                 patch("scoring.compute._aggregate_evictions", return_value=[]), \
                 patch("scoring.compute._aggregate_llc_acquisitions", return_value=[]), \
                 patch("scoring.compute._aggregate_complaints", return_value=[]), \
                 patch("scoring.compute._aggregate_violations", return_value=[]), \
                 patch("scoring.compute._aggregate_assessment_spike", return_value=[]), \
                 patch("scoring.compute._aggregate_rs_unit_loss", return_value=[]):
                count = compute_scores(db, force=True)

            assert count >= 5, f"Expected at least 5 zip codes scored, got {count}"

            # Fetch scores for all test zip codes
            rows = db.execute(
                text(
                    "SELECT zip_code, score FROM displacement_scores "
                    "WHERE zip_code = ANY(:zips)"
                ),
                {"zips": all_zip_codes},
            ).fetchall()
            scores_by_zip = {r[0]: r[1] for r in rows}

            for z in all_zip_codes:
                assert z in scores_by_zip, f"No score written for test zip {z}"

            residential_scores = [scores_by_zip[z] for z, _, _ in residential_zips]
            commercial_scores  = [scores_by_zip[z] for z, _, _ in commercial_zips]

            assert min(residential_scores) > max(commercial_scores), (
                f"Per-unit normalization failed: residential min={min(residential_scores):.1f} "
                f"should be > commercial max={max(commercial_scores):.1f}. "
                f"Residential scores: {residential_scores}, "
                f"Commercial scores: {commercial_scores}"
            )
        finally:
            _cleanup_test_data(db, all_zip_codes)
            _cleanup_test_parcels(db, all_zip_codes)
            db.close()


class TestScoreSanityCheck:
    """Tests for _assert_score_valid() pre-commit sanity guard in compute.py."""

    VALID_BREAKDOWN = {
        "permits": 45.0,
        "evictions": 30.0,
        "llc_acquisitions": 60.0,
        "hpd_violations": 0.0,
        "complaint_rate": 25.0,
        "rs_unit_loss": 15.0,
    }

    def test_valid_score_passes(self):
        """A score of 85.0 with all 6 valid signal keys raises no exception."""
        from scoring.compute import _assert_score_valid
        # Must not raise
        _assert_score_valid("10001", 85.0, self.VALID_BREAKDOWN)

    def test_score_above_100_raises(self):
        """Score above 100.0 raises ValueError with 'Score out of range'."""
        from scoring.compute import _assert_score_valid
        with pytest.raises(ValueError, match="Score out of range"):
            _assert_score_valid("10001", 150.0, self.VALID_BREAKDOWN)

    def test_score_below_1_raises(self):
        """Score below 1.0 raises ValueError with 'Score out of range'."""
        from scoring.compute import _assert_score_valid
        with pytest.raises(ValueError, match="Score out of range"):
            _assert_score_valid("10001", 0.5, self.VALID_BREAKDOWN)

    def test_missing_signal_key_raises(self):
        """Breakdown missing 'rs_unit_loss' raises ValueError containing 'Missing signal keys'."""
        from scoring.compute import _assert_score_valid
        incomplete = {k: v for k, v in self.VALID_BREAKDOWN.items() if k != "rs_unit_loss"}
        with pytest.raises(ValueError, match="Missing signal keys"):
            _assert_score_valid("10001", 50.0, incomplete)

    def test_signal_value_out_of_range_raises(self):
        """Breakdown with a signal value > 100.0 raises ValueError."""
        from scoring.compute import _assert_score_valid
        bad = {**self.VALID_BREAKDOWN, "permits": 150.0}
        with pytest.raises(ValueError):
            _assert_score_valid("10001", 50.0, bad)

    def test_boundary_score_1_passes(self):
        """Minimum valid score (1.0) passes validation."""
        from scoring.compute import _assert_score_valid
        _assert_score_valid("10001", 1.0, self.VALID_BREAKDOWN)

    def test_boundary_score_100_passes(self):
        """Maximum valid score (100.0) passes validation."""
        from scoring.compute import _assert_score_valid
        _assert_score_valid("10001", 100.0, self.VALID_BREAKDOWN)


class TestAssessmentSpike:
    """
    Unit tests for _aggregate_assessment_spike().

    Uses a real DB connection but restricts all fixture data to bbl values
    prefixed "99" (non-NYC) so they never collide with production parcels.
    Each test cleans up after itself.
    """

    # BBLs and zip used across tests — 99xxx range, no production overlap
    ZIP = "99901"
    BBLS = ["9990100001", "9990100002"]

    def _cleanup(self, db):
        db.execute(
            text("DELETE FROM assessment_history WHERE bbl LIKE '9990%'")
        )
        db.execute(
            text("DELETE FROM parcels WHERE bbl LIKE '9990%'")
        )
        db.commit()

    def _insert_parcel(self, db, bbl, zip_code, units_res):
        db.execute(
            text("""
                INSERT INTO parcels (bbl, zip_code, units_res, on_speculation_watch_list,
                                     created_at, updated_at)
                VALUES (:bbl, :zip_code, :units_res, FALSE, NOW(), NOW())
                ON CONFLICT ON CONSTRAINT uq_parcels_bbl DO UPDATE SET
                    zip_code  = EXCLUDED.zip_code,
                    units_res = EXCLUDED.units_res
            """),
            {"bbl": bbl, "zip_code": zip_code, "units_res": units_res},
        )

    def _insert_history(self, db, bbl, assessed_total, tax_year):
        db.execute(
            text("""
                INSERT INTO assessment_history (bbl, assessed_total, tax_year)
                VALUES (:bbl, :assessed_total, :tax_year)
                ON CONFLICT DO NOTHING
            """),
            {"bbl": bbl, "assessed_total": assessed_total, "tax_year": tax_year},
        )

    def test_dormant_when_table_empty(self):
        """Returns [] when assessment_history has no rows at all."""
        from scoring.compute import _aggregate_assessment_spike
        db = SessionLocal()
        try:
            self._cleanup(db)
            result = _aggregate_assessment_spike(db)
            assert result == [], f"Expected [] for empty table, got {result}"
        finally:
            self._cleanup(db)
            db.close()

    def test_dormant_when_single_year(self):
        """Returns [] when assessment_history has only one distinct tax_year."""
        from scoring.compute import _aggregate_assessment_spike
        db = SessionLocal()
        try:
            self._cleanup(db)
            self._insert_parcel(db, self.BBLS[0], self.ZIP, 10)
            self._insert_history(db, self.BBLS[0], 100_000, 2026)
            db.commit()

            result = _aggregate_assessment_spike(db)
            assert result == [], (
                f"Expected [] with only 1 tax year, got {result}"
            )
        finally:
            self._cleanup(db)
            db.close()

    def test_correct_value_two_years(self):
        """
        With two known BBLs across two tax years, the returned weighted spike
        matches the manually computed units_res-weighted average.

        BBL 1: prior=100_000, current=120_000, spike=0.20, units_res=10
        BBL 2: prior=200_000, current=220_000, spike=0.10, units_res=5
        Expected weighted spike for ZIP = (0.20*10 + 0.10*5) / (10+5) = 2.5/15 ≈ 0.1667
        """
        from scoring.compute import _aggregate_assessment_spike
        db = SessionLocal()
        try:
            self._cleanup(db)
            self._insert_parcel(db, self.BBLS[0], self.ZIP, 10)
            self._insert_parcel(db, self.BBLS[1], self.ZIP, 5)
            self._insert_history(db, self.BBLS[0], 100_000, 2025)
            self._insert_history(db, self.BBLS[0], 120_000, 2026)
            self._insert_history(db, self.BBLS[1], 200_000, 2025)
            self._insert_history(db, self.BBLS[1], 220_000, 2026)
            db.commit()

            result = _aggregate_assessment_spike(db)
            assert len(result) == 1, f"Expected 1 ZIP result, got {result}"
            zip_code, spike = result[0]
            assert zip_code == self.ZIP
            expected = (0.20 * 10 + 0.10 * 5) / (10 + 5)  # 2.5/15 ≈ 0.1667
            assert abs(spike - expected) < 0.001, (
                f"Expected spike ≈ {expected:.4f}, got {spike:.4f}"
            )
        finally:
            self._cleanup(db)
            db.close()
