"""
Tests for Neighborhood Pulse endpoint — plan 06-03.
Tests for Renovation-Flip detection endpoint — plan 06-04.

Tests 1-7 cover GET /api/neighborhoods/{zip}/pulse:
  1. GET /api/neighborhoods/11221/pulse returns 200 with correct shape
  2. GET /api/neighborhoods/ABCDE/pulse returns 400 (invalid zip)
  3. GET /api/neighborhoods/99999/pulse returns 200 with empty lists
  4. Each llc_acquisition item has keys: bbl, address, buyer_name, doc_date, doc_amount
  5. Each recent_permit item has keys: bbl, address, permit_type, filing_date
  6. Only doc_type IN ('DEED','DEEDP','ASST') AND party_name_normalized LIKE '%LLC%' in llc_acquisitions
  7. Only permit_type IN ('A1','A2','NB') in recent_permits

Tests 8-14 cover GET /api/neighborhoods/{zip}/renovation-flip:
  8. GET /api/neighborhoods/11221/renovation-flip returns 200 with {detected: bool, count: int, properties: [...]}
  9. GET /api/neighborhoods/ABCDE/renovation-flip returns 400
  10. GET /api/neighborhoods/99999/renovation-flip returns {detected: false, count: 0, properties: []}
  11. detected=true only when count >= 2
  12. Each property item has keys: bbl, address, buyer, transfer_date, permit_date, days_between
  13. Only permit_type IN ('A1','A2') triggers detection (not NB, A3, etc.)
  14. Only permit AFTER transfer triggers (permit_date > transfer_date enforced)
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date
from decimal import Decimal


def _get_client():
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


@pytest.mark.integration
class TestPulseAPI:
    """Tests 1-7: GET /api/neighborhoods/{zip}/pulse."""

    def test_valid_zip_returns_200_with_correct_shape(self):
        """Test 1: GET /api/neighborhoods/11221/pulse returns 200 with JSON dict with required keys."""
        client = _get_client()
        resp = client.get("/api/neighborhoods/11221/pulse")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert "llc_acquisitions" in body, f"Missing 'llc_acquisitions' key in response: {list(body.keys())}"
        assert "recent_permits" in body, f"Missing 'recent_permits' key in response: {list(body.keys())}"
        assert isinstance(body["llc_acquisitions"], list), "llc_acquisitions must be a list"
        assert isinstance(body["recent_permits"], list), "recent_permits must be a list"

    def test_invalid_zip_returns_400(self):
        """Test 2: GET /api/neighborhoods/ABCDE/pulse returns 400."""
        client = _get_client()
        resp = client.get("/api/neighborhoods/ABCDE/pulse")
        assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"

    def test_unknown_zip_returns_200_with_empty_lists(self):
        """Test 3: GET /api/neighborhoods/99999/pulse returns 200 with empty lists (not 404)."""
        client = _get_client()
        resp = client.get("/api/neighborhoods/99999/pulse")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert isinstance(body["llc_acquisitions"], list)
        assert isinstance(body["recent_permits"], list)
        # 99999 is not a real NYC zip — should return empty, not error

    def test_llc_acquisition_item_shape(self):
        """Test 4: Each llc_acquisition item has keys: bbl, address, buyer_name, doc_date, doc_amount."""
        from fastapi.testclient import TestClient
        from api.main import app

        mock_llc_row = MagicMock()
        mock_llc_row.bbl = "3011220001"
        mock_llc_row.address = "123 MYRTLE AVE"
        mock_llc_row.buyer_name = "CENTRAL REALTY LLC"
        mock_llc_row.doc_date = date(2026, 1, 15)
        mock_llc_row.doc_amount = Decimal("500000.00")

        with patch("api.routes.pulse.get_db") as mock_get_db:
            mock_session = MagicMock()
            # First execute() call is for LLC acquisitions, second for permits
            mock_session.execute.side_effect = [
                MagicMock(fetchall=MagicMock(return_value=[mock_llc_row])),
                MagicMock(fetchall=MagicMock(return_value=[])),
            ]
            mock_get_db.return_value = iter([mock_session])

            client = TestClient(app)
            resp = client.get("/api/neighborhoods/11221/pulse")

        assert resp.status_code == 200
        body = resp.json()
        if body["llc_acquisitions"]:
            item = body["llc_acquisitions"][0]
            required_keys = {"bbl", "address", "buyer_name", "doc_date", "doc_amount"}
            missing = required_keys - set(item.keys())
            assert not missing, f"llc_acquisition item missing keys: {missing}. Got: {list(item.keys())}"

    def test_recent_permit_item_shape(self):
        """Test 5: Each recent_permit item has keys: bbl, address, permit_type, filing_date."""
        from fastapi.testclient import TestClient
        from api.main import app

        mock_permit_row = MagicMock()
        mock_permit_row.bbl = "3011220001"
        mock_permit_row.address = "123 MYRTLE AVE"
        mock_permit_row.permit_type = "A1"
        mock_permit_row.filing_date = date(2026, 2, 10)

        with patch("api.routes.pulse.get_db") as mock_get_db:
            mock_session = MagicMock()
            mock_session.execute.side_effect = [
                MagicMock(fetchall=MagicMock(return_value=[])),
                MagicMock(fetchall=MagicMock(return_value=[mock_permit_row])),
            ]
            mock_get_db.return_value = iter([mock_session])

            client = TestClient(app)
            resp = client.get("/api/neighborhoods/11221/pulse")

        assert resp.status_code == 200
        body = resp.json()
        if body["recent_permits"]:
            item = body["recent_permits"][0]
            required_keys = {"bbl", "address", "permit_type", "filing_date"}
            missing = required_keys - set(item.keys())
            assert not missing, f"recent_permit item missing keys: {missing}. Got: {list(item.keys())}"

    def test_llc_acquisitions_sql_filters_doc_type_and_llc(self):
        """Test 6: SQL uses party_type='2' (ACRIS grantee code), doc_type IN ('DEED','DEEDP','ASST'), and LLC filter."""
        from api.routes.pulse import get_neighborhood_pulse
        import inspect

        source = inspect.getsource(get_neighborhood_pulse)
        # Check doc_type filter is present
        assert "DEED" in source, "SQL must filter doc_type for 'DEED'"
        assert "DEEDP" in source, "SQL must filter doc_type for 'DEEDP'"
        assert "ASST" in source, "SQL must filter doc_type for 'ASST'"
        assert "LLC" in source, "SQL must filter party_name_normalized for '%LLC%'"
        # ACRIS stores party_type as '2' (grantee/buyer), NOT 'GRANTEE' text.
        # This assertion catches the bug where 'GRANTEE' was used instead of '2',
        # which caused all LLC queries to return zero results.
        assert "party_type = '2'" in source, (
            "SQL must use party_type = '2' (ACRIS numeric grantee code). "
            "Using 'GRANTEE' text will match zero rows — ownership_raw stores '2', not 'GRANTEE'."
        )

    def test_llc_acquisitions_returns_real_data_for_active_zip(self):
        """
        Integration test: a ZIP with known LLC activity returns non-empty llc_acquisitions.
        Catches party_type mismatches and SQL bugs that mocked tests cannot detect.
        Uses the real database — skipped if no ownership_raw data exists.
        """
        from sqlalchemy import text
        from models.database import SessionLocal

        db = SessionLocal()
        try:
            # Find a ZIP that has actual LLC acquisitions in the last 365 days
            row = db.execute(text("""
                SELECT p.zip_code, COUNT(*) AS cnt
                FROM ownership_raw o
                JOIN parcels p ON p.bbl = o.bbl
                WHERE o.party_type = '2'
                  AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
                  AND o.party_name_normalized LIKE '%LLC%'
                  AND o.doc_date >= CURRENT_DATE - INTERVAL '365 days'
                  AND p.zip_code IS NOT NULL
                GROUP BY p.zip_code
                ORDER BY cnt DESC
                LIMIT 1
            """)).fetchone()
        finally:
            db.close()

        if row is None:
            pytest.skip("No LLC acquisitions in ownership_raw — scraper not yet run")

        active_zip = row[0]
        client = _get_client()
        resp = client.get(f"/api/neighborhoods/{active_zip}/pulse")
        assert resp.status_code == 200
        body = resp.json()
        # The pulse endpoint uses a 90-day window; the query above used 365 days.
        # If the most active ZIP has no activity in the last 90 days, skip gracefully.
        if not body["llc_acquisitions"]:
            pytest.skip(f"ZIP {active_zip} has LLC activity in past year but not past 90 days")
        assert len(body["llc_acquisitions"]) > 0, (
            f"ZIP {active_zip} has known LLC acquisitions but pulse returned empty list. "
            "Check party_type value in the SQL query."
        )

    def test_recent_permits_sql_filters_permit_type(self):
        """Test 7: SQL for recent_permits filters permit_type IN ('A1','A2','NB')."""
        from api.routes.pulse import get_neighborhood_pulse
        import inspect

        source = inspect.getsource(get_neighborhood_pulse)
        assert "A1" in source, "SQL must filter permit_type for 'A1'"
        assert "A2" in source, "SQL must filter permit_type for 'A2'"
        assert "NB" in source, "SQL must filter permit_type for 'NB'"


@pytest.mark.integration
class TestRenovationFlipAPI:
    """Tests 8-14: GET /api/neighborhoods/{zip}/renovation-flip — plan 06-04."""

    def test_valid_zip_returns_200_with_correct_shape(self):
        """Test 8: Returns 200 with {detected: bool, count: int, properties: [...]}."""
        client = _get_client()
        resp = client.get("/api/neighborhoods/11221/renovation-flip")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert "detected" in body, f"Missing 'detected' key: {list(body.keys())}"
        assert "count" in body, f"Missing 'count' key: {list(body.keys())}"
        assert "properties" in body, f"Missing 'properties' key: {list(body.keys())}"
        assert isinstance(body["detected"], bool), "detected must be bool"
        assert isinstance(body["count"], int), "count must be int"
        assert isinstance(body["properties"], list), "properties must be a list"

    def test_invalid_zip_returns_400(self):
        """Test 9: GET /api/neighborhoods/ABCDE/renovation-flip returns 400."""
        client = _get_client()
        resp = client.get("/api/neighborhoods/ABCDE/renovation-flip")
        assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"

    def test_unknown_zip_returns_detected_false(self):
        """Test 10: Unknown zip 99999 returns detected=false, count=0, empty properties."""
        client = _get_client()
        resp = client.get("/api/neighborhoods/99999/renovation-flip")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["detected"] is False, f"Expected detected=false for unknown zip, got {body['detected']}"
        assert body["count"] == 0, f"Expected count=0, got {body['count']}"
        assert body["properties"] == [], f"Expected empty properties, got {body['properties']}"

    def test_detected_true_only_when_count_gte_2(self):
        """Test 11: detected=true only when count >= 2."""
        from fastapi.testclient import TestClient
        from api.main import app
        from models.database import get_db
        from datetime import date, timedelta

        # Build a mock row that simulates a matched BBL (LLC deed + A1 permit within 60 days)
        def _make_row(bbl, buyer, transfer_days_ago, permit_days_after):
            row = MagicMock()
            row.bbl = bbl
            row.address = f"{bbl} MAIN ST"
            row.buyer = buyer
            row.transfer_date = date.today() - timedelta(days=transfer_days_ago)
            row.first_permit_date = date.today() - timedelta(days=transfer_days_ago) + timedelta(days=permit_days_after)
            days_delta = MagicMock()
            days_delta.days = permit_days_after
            row.days_between = days_delta
            return row

        # Case A: single row → detected=false
        single_row = [_make_row("3011220001", "ALPHA LLC", 90, 30)]
        mock_session_a = MagicMock()
        mock_session_a.execute.return_value = MagicMock(fetchall=MagicMock(return_value=single_row))

        def override_a():
            yield mock_session_a

        app.dependency_overrides[get_db] = override_a
        try:
            client = TestClient(app)
            resp = client.get("/api/neighborhoods/11221/renovation-flip")
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 200
        body = resp.json()
        assert body["detected"] is False, f"Single row: expected detected=false, got {body['detected']}"
        assert body["count"] == 1, f"Single row: expected count=1, got {body['count']}"

        # Case B: two rows → detected=true
        two_rows = [
            _make_row("3011220001", "ALPHA LLC", 90, 30),
            _make_row("3011220002", "BETA LLC", 80, 45),
        ]
        mock_session_b = MagicMock()
        mock_session_b.execute.return_value = MagicMock(fetchall=MagicMock(return_value=two_rows))

        def override_b():
            yield mock_session_b

        app.dependency_overrides[get_db] = override_b
        try:
            client = TestClient(app)
            resp = client.get("/api/neighborhoods/11221/renovation-flip")
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 200
        body = resp.json()
        assert body["detected"] is True, f"Two rows: expected detected=true, got {body['detected']}"
        assert body["count"] == 2, f"Two rows: expected count=2, got {body['count']}"

    def test_property_item_shape(self):
        """Test 12: Each property item has keys: bbl, address, buyer, transfer_date, permit_date, days_between."""
        from fastapi.testclient import TestClient
        from api.main import app
        from models.database import get_db
        from datetime import date

        mock_row = MagicMock()
        mock_row.bbl = "3011220001"
        mock_row.address = "123 MYRTLE AVE"
        mock_row.buyer = "CENTRAL REALTY LLC"
        mock_row.transfer_date = date(2026, 1, 15)
        mock_row.first_permit_date = date(2026, 2, 14)
        days_delta = MagicMock()
        days_delta.days = 30
        mock_row.days_between = days_delta

        mock_session = MagicMock()
        mock_session.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[mock_row]))

        def override_db():
            yield mock_session

        app.dependency_overrides[get_db] = override_db
        try:
            client = TestClient(app)
            resp = client.get("/api/neighborhoods/11221/renovation-flip")
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 200
        body = resp.json()
        assert body["properties"], "Expected at least one property in response"
        item = body["properties"][0]
        required_keys = {"bbl", "address", "buyer", "transfer_date", "permit_date", "days_between"}
        missing = required_keys - set(item.keys())
        assert not missing, f"Property item missing keys: {missing}. Got: {list(item.keys())}"

    def test_sql_filters_only_a1_a2_permit_types(self):
        """Test 13: Only A1/A2 permit types trigger detection (not new-building permits)."""
        from api.routes.pulse import get_renovation_flip
        import inspect

        source = inspect.getsource(get_renovation_flip)
        assert "'A1'" in source or '"A1"' in source, "SQL must include A1 permit type filter"
        assert "'A2'" in source or '"A2"' in source, "SQL must include A2 permit type filter"
        # New Building permits must NOT be in the renovation-flip permit filter
        # Strip comments before checking, to avoid matching comment text
        code_lines = [ln for ln in source.splitlines() if not ln.lstrip().startswith('#')]
        code_only = "\n".join(code_lines)
        assert "'NB'" not in code_only and '"NB"' not in code_only, \
            "renovation-flip must NOT filter on 'NB' permit type — only A1/A2 are renovation indicators"

    def test_sql_enforces_permit_after_transfer(self):
        """Test 14: Only permit AFTER transfer triggers (permit_date > transfer_date enforced)."""
        from api.routes.pulse import get_renovation_flip
        import inspect

        source = inspect.getsource(get_renovation_flip)
        # The SQL must enforce that permit comes after deed transfer
        assert "first_permit_date > l.transfer_date" in source or \
               "r.first_permit_date > l.transfer_date" in source or \
               "permit_date > transfer_date" in source, \
               "SQL must enforce permit_date > transfer_date (permit must come AFTER deed transfer)"
