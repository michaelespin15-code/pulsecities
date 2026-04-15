"""
Tests for check constraints added to displacement-critical columns.

RED state: constraints do not exist yet in migrations.
Plan 07-02 will add the Alembic migration and turn these GREEN.

These tests use SQLAlchemy introspection on the live database to verify
constraints are active. They require a live DB connection.
"""
import pytest
from sqlalchemy import create_engine, inspect, text
import os


@pytest.mark.integration
class TestCheckConstraintsExist:
    """Verify check constraints exist in the live DB after migration runs."""

    def _get_engine(self):
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            pytest.skip("DATABASE_URL not set — skipping constraint DB test")
        return create_engine(db_url)

    def test_ownership_party_type_constraint_exists(self):
        """ownership_raw.party_type must have CHECK (party_type IS NULL OR party_type IN ('1', '2'))"""
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.table_constraints "
                "WHERE table_name = 'ownership_raw' AND constraint_type = 'CHECK' "
                "AND constraint_name = 'ck_ownership_party_type'"
            ))
            count = result.scalar()
        assert count == 1, "ck_ownership_party_type constraint missing from ownership_raw"

    def test_eviction_type_constraint_exists(self):
        """evictions_raw.eviction_type must have CHECK constraint for 'R'/'C' values."""
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.table_constraints "
                "WHERE table_name = 'evictions_raw' AND constraint_type = 'CHECK' "
                "AND constraint_name = 'ck_evictions_eviction_type'"
            ))
            count = result.scalar()
        assert count == 1, "ck_evictions_eviction_type constraint missing from evictions_raw"

    def test_parcels_borough_constraint_exists(self):
        """parcels.borough must have CHECK (borough IS NULL OR borough BETWEEN 1 AND 5)."""
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.table_constraints "
                "WHERE table_name = 'parcels' AND constraint_type = 'CHECK' "
                "AND constraint_name = 'ck_parcels_borough'"
            ))
            count = result.scalar()
        assert count == 1, "ck_parcels_borough constraint missing from parcels"

    def test_displacement_scores_score_range_constraint_exists(self):
        """displacement_scores.score must have CHECK (score IS NULL OR score BETWEEN 1 AND 100)."""
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.table_constraints "
                "WHERE table_name = 'displacement_scores' AND constraint_type = 'CHECK' "
                "AND constraint_name = 'ck_displacement_scores_score_range'"
            ))
            count = result.scalar()
        assert count == 1, "ck_displacement_scores_score_range constraint missing"

    def test_score_history_score_range_constraint_exists(self):
        """score_history.composite_score must have CHECK constraint for [1, 100] range."""
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.table_constraints "
                "WHERE table_name = 'score_history' AND constraint_type = 'CHECK' "
                "AND constraint_name = 'ck_score_history_composite_score_range'"
            ))
            count = result.scalar()
        assert count == 1, "ck_score_history_composite_score_range constraint missing"

    def test_parcels_units_res_non_negative_constraint_exists(self):
        """parcels.units_res must have CHECK (units_res IS NULL OR units_res >= 0)."""
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.table_constraints "
                "WHERE table_name = 'parcels' AND constraint_type = 'CHECK' "
                "AND constraint_name = 'ck_parcels_units_res_non_negative'"
            ))
            count = result.scalar()
        assert count == 1, "ck_parcels_units_res_non_negative constraint missing"
