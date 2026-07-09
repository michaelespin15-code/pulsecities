"""
/api/score-history/frames — sampled citywide snapshots for the map replay.
"""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    from api.main import app
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.mark.integration
class TestScoreFrames:

    def test_shape_and_alignment(self, client):
        data = client.get("/api/score-history/frames?days=180&step=7").json()
        assert "dates" in data and "scores" in data
        n = len(data["dates"])
        assert n > 0, "no frames despite 187d of backfilled history"
        assert data["dates"] == sorted(data["dates"])
        for zip_code, series in data["scores"].items():
            assert len(series) == n, f"{zip_code} series misaligned with dates"

    def test_last_frame_is_newest_snapshot(self, client):
        frames = client.get("/api/score-history/frames?days=180&step=7").json()
        from models.database import SessionLocal
        from sqlalchemy import text
        db = SessionLocal()
        try:
            newest = db.execute(text("SELECT MAX(scored_at) FROM score_history")).scalar()
        finally:
            db.close()
        assert frames["dates"][-1] == newest.isoformat()

    def test_days_and_step_clamped(self, client):
        resp = client.get("/api/score-history/frames?days=100000&step=0")
        assert resp.status_code == 200
        assert len(resp.json()["dates"]) <= 366

    def test_cacheable(self, client):
        resp = client.get("/api/score-history/frames")
        assert "max-age" in resp.headers.get("cache-control", "")

    def test_zip_route_still_works(self, client):
        resp = client.get("/api/score-history/11216?days=30")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
