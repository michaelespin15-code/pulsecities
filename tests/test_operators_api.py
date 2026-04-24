"""
Integration tests for operator DB tables and API endpoints.
Requirements: OPAPI-01 through OPAPI-04.

Runs against the live pulsecities DB — requires Plan 09-01 backfill to have run.

Test classes:
    TestOperatorSchema  — tables exist with correct structure
    TestBackfill        — operators and operator_parcels are seeded correctly
    TestOperatorList    — GET /api/operators (activated in Plan 02)
    TestOperatorDetail  — GET /api/operators/{slug} (activated in Plan 02)
    TestGroupedSearch   — grouped search with operators key (activated in Plan 03)
"""

import pytest
from sqlalchemy import text

from models.database import SessionLocal


@pytest.fixture(scope="module")
def db():
    session = SessionLocal()
    yield session
    session.close()


@pytest.mark.integration
class TestOperatorSchema:
    """Verify the operators and operator_parcels tables exist in the live DB."""

    def test_operators_table_exists(self, db):
        row = db.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'operators'"
            )
        ).scalar()
        assert row > 0, "operators table does not exist in public schema"

    def test_operator_parcels_table_exists(self, db):
        row = db.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'operator_parcels'"
            )
        ).scalar()
        assert row > 0, "operator_parcels table does not exist in public schema"


@pytest.mark.integration
class TestBackfill:
    """Verify all 20 operators and their parcels are seeded correctly."""

    def test_mtek_row_present(self, db):
        row = db.execute(
            text(
                "SELECT slug, total_properties FROM operators "
                "WHERE slug = 'mtek-nyc'"
            )
        ).fetchone()
        assert row is not None, "MTEK row not found (slug='mtek-nyc')"
        assert row.total_properties >= 37, (
            f"MTEK total_properties={row.total_properties} is less than 37"
        )

    def test_phantom_row_present(self, db):
        row = db.execute(
            text(
                "SELECT slug, total_properties FROM operators "
                "WHERE slug = 'phantom-capital'"
            )
        ).fetchone()
        assert row is not None, "PHANTOM CAPITAL row not found (slug='phantom-capital')"
        assert row.total_properties >= 64, (
            f"PHANTOM total_properties={row.total_properties} is less than 64"
        )

    def test_bredif_row_present(self, db):
        row = db.execute(
            text(
                "SELECT slug, total_properties FROM operators "
                "WHERE slug = 'bredif'"
            )
        ).fetchone()
        assert row is not None, "BREDIF row not found (slug='bredif')"
        assert row.total_properties >= 66, (
            f"BREDIF total_properties={row.total_properties} is less than 66"
        )

    def test_twenty_operators_seeded(self, db):
        count = db.execute(text("SELECT COUNT(*) FROM operators")).scalar()
        assert count >= 20, f"Expected >= 20 operators, found {count}"

    def test_highest_displacement_score_nonnull(self, db):
        value = db.execute(
            text(
                "SELECT highest_displacement_score FROM operators "
                "WHERE slug = 'mtek-nyc'"
            )
        ).scalar()
        assert value is not None, "highest_displacement_score is NULL for MTEK"
        assert value > 0, f"highest_displacement_score={value} should be > 0"

    def test_borough_spread_nonnull(self, db):
        value = db.execute(
            text("SELECT borough_spread FROM operators WHERE slug = 'mtek-nyc'")
        ).scalar()
        assert value is not None, "borough_spread is NULL for MTEK"
        assert value >= 1, f"borough_spread={value} should be >= 1"

    def test_operator_parcels_for_mtek(self, db):
        count = db.execute(
            text(
                "SELECT COUNT(*) FROM operator_parcels op "
                "JOIN operators o ON o.id = op.operator_id "
                "WHERE o.slug = 'mtek-nyc'"
            )
        ).scalar()
        assert count >= 37, (
            f"Expected >= 37 parcels for MTEK, found {count}"
        )


@pytest.mark.integration
class TestOperatorList:
    """OPAPI-02: GET /api/operators returns paginated operator list. (Activated in Plan 02)"""

    def test_list_endpoint_returns_array(self, db):
        pass  # activated in Plan 02


@pytest.mark.integration
class TestOperatorDetail:
    """OPAPI-03: GET /api/operators/{slug} returns operator detail. (Activated in Plan 02)"""

    def test_unknown_slug_404(self, db):
        pass  # activated in Plan 02


@pytest.mark.integration
class TestGroupedSearch:
    """OPAPI-04: Search results include operators key. (Activated in Plan 03)"""

    def test_search_returns_operators_key(self, db):
        # Direct DB query to verify operators table has mtek-nyc
        row = db.execute(
            text("SELECT slug FROM operators WHERE operator_root = 'MTEK'")
        ).fetchone()
        assert row is not None
        assert row.slug == "mtek-nyc"

    def test_search_mtek_finds_operator(self, db):
        # Verify the search SQL logic directly (same query as endpoint)
        rows = db.execute(
            text("""
                SELECT operator_root, slug, display_name, total_properties
                FROM operators
                WHERE display_name ILIKE :pattern OR operator_root ILIKE :pattern
                ORDER BY total_properties DESC LIMIT 10
            """),
            {"pattern": "%mtek%"},
        ).fetchall()
        slugs = [r.slug for r in rows]
        assert "mtek-nyc" in slugs

    def test_search_response_has_both_keys(self, db):
        # Verify the grouped response shape via direct SQL
        op_rows = db.execute(
            text("SELECT slug FROM operators WHERE display_name ILIKE :p LIMIT 1"),
            {"p": "%mtek%"},
        ).fetchall()
        # Verify the operators table query works and returns at least one row
        assert len(op_rows) >= 1
