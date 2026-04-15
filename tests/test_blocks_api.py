"""
Integration tests for GET /api/blocks/{bbl} (API-03).
Wave 0: stubs that will be filled in after blocks.py is implemented.
"""
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    from api.main import app
    return TestClient(app)


@pytest.mark.integration
class TestBlocksApi:
    def test_invalid_bbl_returns_400(self, client):
        """BBL format validation — non-numeric string must return 400."""
        resp = client.get("/api/blocks/NOTABBL")
        assert resp.status_code == 400
        assert "Invalid BBL" in resp.json()["detail"]

    def test_unknown_bbl_returns_empty_list(self, client):
        """Valid BBL format but no data in DB — 200 with empty events list."""
        # Use a valid-format BBL (borough 1, block/lot that won't exist in test DB)
        # Borough codes are 1-5; 1000019999 = borough 1, block 00001, lot 9999
        resp = client.get("/api/blocks/1000019999")
        assert resp.status_code == 200
        data = resp.json()
        assert "bbl" in data
        assert "events" in data
        assert isinstance(data["events"], list)

    def test_valid_bbl_returns_event_list(self, client):
        """BBL with real data returns 200 + non-empty events list."""
        pytest.skip("Requires seeded DB data — skip in CI")

    def test_events_sorted_by_date_descending(self, client):
        """Events must be sorted newest-first."""
        pytest.skip("Requires seeded DB data — skip in CI")

    def test_rate_limit_decorator_present(self):
        """Rate limit decorator must be present on the endpoint."""
        import os
        import subprocess
        # Resolve blocks.py path relative to this test file's directory
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        blocks_path = os.path.join(repo_root, "api", "routes", "blocks.py")
        result = subprocess.run(
            ["grep", "-n", "limiter.limit", blocks_path],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"limiter.limit not found in {blocks_path}"
        assert "60/minute" in result.stdout
