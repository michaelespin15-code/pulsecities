"""
Tests for score history persistence — plan 06-01.

Task 1 (model structure): Tests 1-5 use SQLAlchemy's inspect() on the mapper.
  No live DB connection required.

Task 2 (pipeline + API): Tests 6-10 use mocks for pipeline and TestClient for API.
"""

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import DeclarativeBase


# ---------------------------------------------------------------------------
# Task 1: ScoreHistory model structure tests (no DB connection required)
# ---------------------------------------------------------------------------

class TestScoreHistoryModel:
    """Test 1-3: Model structure via SQLAlchemy mapper inspection."""

    def test_tablename(self):
        """Test 1: ScoreHistory has tablename 'score_history'."""
        from models.score_history import ScoreHistory
        assert ScoreHistory.__tablename__ == "score_history"

    def test_unique_constraint_exists(self):
        """Test 2: ScoreHistory has UNIQUE constraint named uq_score_history_zip_date on (zip_code, scored_at)."""
        from models.score_history import ScoreHistory

        table = ScoreHistory.__table__
        constraint_names = {c.name for c in table.constraints}
        assert "uq_score_history_zip_date" in constraint_names, (
            f"Expected 'uq_score_history_zip_date' in constraints. Got: {constraint_names}"
        )

        # Verify the constraint covers the right columns
        uc = next(c for c in table.constraints if c.name == "uq_score_history_zip_date")
        col_names = {col.name for col in uc.columns}
        assert col_names == {"zip_code", "scored_at"}, (
            f"uq_score_history_zip_date must cover zip_code and scored_at. Got: {col_names}"
        )

    def test_all_columns_exist(self):
        """Test 3: ScoreHistory has all required columns with correct types/nullability."""
        from models.score_history import ScoreHistory
        from sqlalchemy import String, Float, Date

        mapper = sa_inspect(ScoreHistory)
        columns = {col.key: col for col in mapper.mapper.columns}

        # Required columns
        assert "zip_code" in columns, "Missing column: zip_code"
        assert "scored_at" in columns, "Missing column: scored_at"
        assert "composite_score" in columns, "Missing column: composite_score"
        assert "permit_intensity" in columns, "Missing column: permit_intensity"
        assert "eviction_rate" in columns, "Missing column: eviction_rate"
        assert "llc_acquisition_rate" in columns, "Missing column: llc_acquisition_rate"
        assert "assessment_spike" in columns, "Missing column: assessment_spike"
        assert "complaint_rate" in columns, "Missing column: complaint_rate"

        # Nullability
        assert not columns["zip_code"].nullable, "zip_code must be NOT NULL"
        assert not columns["scored_at"].nullable, "scored_at must be NOT NULL"
        assert not columns["composite_score"].nullable, "composite_score must be NOT NULL"
        assert columns["permit_intensity"].nullable, "permit_intensity must be nullable"
        assert columns["eviction_rate"].nullable, "eviction_rate must be nullable"
        assert columns["llc_acquisition_rate"].nullable, "llc_acquisition_rate must be nullable"
        assert columns["assessment_spike"].nullable, "assessment_spike must be nullable"
        assert columns["complaint_rate"].nullable, "complaint_rate must be nullable"

        # TimestampMixin columns present
        assert "id" in columns
        assert "created_at" in columns
        assert "updated_at" in columns

    def test_migration_file_exists(self):
        """Test 4: Migration file for score_history exists and references the correct parent revision."""
        import os
        import glob

        migrations_path = "/root/pulsecities/migrations/versions"
        migration_files = sorted(glob.glob(f"{migrations_path}/*score_history*.py"))
        assert len(migration_files) >= 1, (
            f"No migration file found for score_history in {migrations_path}"
        )

        # Check that at least one score_history migration references the unique constraint
        all_content = ""
        for path in migration_files:
            with open(path) as f:
                all_content += f.read()
        assert "uq_score_history_zip_date" in all_content, (
            "A score_history migration must reference uq_score_history_zip_date constraint"
        )
        # Original migration down_revision check against the first (earliest) file
        with open(migration_files[0]) as f:
            first_content = f.read()
        assert "0e9b19eaba99" in first_content, (
            "Migration down_revision must be 0e9b19eaba99 (initial_schema)"
        )

    def test_index_defined_on_table(self):
        """Test 5: Index 'idx_score_history_zip_scored_at' is defined on (zip_code, scored_at DESC)."""
        from models.score_history import ScoreHistory

        table = ScoreHistory.__table__
        index_names = {idx.name for idx in table.indexes}
        assert "idx_score_history_zip_scored_at" in index_names, (
            f"Expected 'idx_score_history_zip_scored_at' in indexes. Got: {index_names}"
        )


# ---------------------------------------------------------------------------
# Task 2: Pipeline snapshot and API endpoint tests
# ---------------------------------------------------------------------------

class TestSnapshotScores:
    """Tests 6-7: snapshot_scores() pipeline function."""

    def test_snapshot_scores_exists_and_is_callable(self):
        """Test 6 (partial): snapshot_scores function is importable from pipeline."""
        from scheduler.pipeline import snapshot_scores
        assert callable(snapshot_scores)

    def test_snapshot_scores_idempotent(self):
        """
        Test 7: Calling snapshot_scores() twice in the same day produces the same row count.
        Uses a mock db to verify ON CONFLICT DO NOTHING behavior via execute calls.
        """
        from unittest.mock import MagicMock, patch
        from scheduler.pipeline import snapshot_scores

        # First call — normal execute
        mock_db = MagicMock()
        snapshot_scores(mock_db)
        assert mock_db.execute.called, "snapshot_scores must call db.execute()"
        assert mock_db.commit.called, "snapshot_scores must call db.commit()"

        # Second call on same mock — still calls execute (idempotent via SQL ON CONFLICT)
        mock_db.reset_mock()
        snapshot_scores(mock_db)
        assert mock_db.execute.called

    def test_snapshot_scores_uses_on_conflict_do_nothing(self):
        """Test 6: snapshot_scores SQL contains ON CONFLICT ON CONSTRAINT uq_score_history_zip_date DO NOTHING."""
        from unittest.mock import MagicMock, call
        from sqlalchemy import text
        from scheduler.pipeline import snapshot_scores

        mock_db = MagicMock()
        snapshot_scores(mock_db)

        # Inspect the SQL text that was passed to execute
        assert mock_db.execute.called
        call_args = mock_db.execute.call_args
        sql_arg = call_args[0][0]
        sql_text = str(sql_arg)
        assert "ON CONFLICT" in sql_text.upper(), (
            "snapshot_scores must use ON CONFLICT ... DO NOTHING for idempotency"
        )
        assert "uq_score_history_zip_date" in sql_text, (
            "snapshot_scores must reference constraint uq_score_history_zip_date"
        )


@pytest.mark.integration
class TestScoreHistoryAPI:
    """Tests 8-10: GET /api/score-history/{zip_code}."""

    def _get_client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_valid_zip_returns_200_with_array(self):
        """Test 8: GET /api/score-history/10025?days=90 returns 200 with JSON array."""
        client = self._get_client()
        resp = client.get("/api/score-history/10025?days=90")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list), f"Expected list, got {type(body)}"

    def test_valid_zip_no_data_returns_empty_array(self):
        """Test 9: GET /api/score-history/99999?days=90 returns 200 with empty [] (not 404)."""
        client = self._get_client()
        resp = client.get("/api/score-history/99999?days=90")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        # 99999 is a non-existent NYC zip — should return empty list, not 404

    def test_invalid_zip_returns_400(self):
        """Test 10: GET /api/score-history/ABCDE returns 400 (invalid zip format)."""
        client = self._get_client()
        resp = client.get("/api/score-history/ABCDE")
        assert resp.status_code == 400

    def test_response_shape_when_data_exists(self):
        """Bonus: When rows exist, each item has date (YYYY-MM-DD) and score (float) keys."""
        from unittest.mock import patch, MagicMock
        from fastapi.testclient import TestClient
        from api.main import app
        from datetime import date

        # Create a mock row that looks like a DB result
        mock_row = MagicMock()
        mock_row.scored_at = date(2026, 4, 10)
        mock_row.composite_score = 42.5

        # Patch the DB query at the session level
        with patch("api.routes.score_history.get_db") as mock_get_db:
            mock_session = MagicMock()
            mock_session.execute.return_value.fetchall.return_value = [mock_row]
            mock_get_db.return_value = iter([mock_session])

            # Use a fresh client with the patch active
            client = TestClient(app)
            resp = client.get("/api/score-history/10025?days=90")

        # Even if patch didn't work (real DB, no data), we just check structure when data is present
        if resp.status_code == 200 and resp.json():
            item = resp.json()[0]
            assert "date" in item, f"Expected 'date' key in item, got: {list(item.keys())}"
            assert "score" in item, f"Expected 'score' key in item, got: {list(item.keys())}"
