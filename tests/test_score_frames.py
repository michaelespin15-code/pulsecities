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


@pytest.mark.integration
class TestHistoryUniverse:
    """
    score_history must only hold ZIPs from the neighborhoods universe.
    The April 2026 cleanup dropped junk ZIPs (99901 Alaska, 12345
    Schenectady, Nassau County) from live scoring, but their backfilled
    history lingered and leaked into the frames payload.
    """

    def test_no_orphan_zips_in_history(self):
        from models.database import SessionLocal
        from sqlalchemy import text
        db = SessionLocal()
        try:
            orphans = db.execute(text(
                "SELECT DISTINCT sh.zip_code FROM score_history sh "
                "WHERE NOT EXISTS (SELECT 1 FROM neighborhoods n WHERE n.zip_code = sh.zip_code)"
            )).fetchall()
        finally:
            db.close()
        assert not orphans, f"orphan ZIPs back in score_history: {[o[0] for o in orphans]}"

    def test_frames_only_serve_known_zips(self, client):
        frames = client.get("/api/score-history/frames?days=180&step=30").json()
        from models.database import SessionLocal
        from sqlalchemy import text
        db = SessionLocal()
        try:
            known = {r[0] for r in db.execute(text("SELECT zip_code FROM neighborhoods")).fetchall()}
        finally:
            db.close()
        unknown = set(frames["scores"]) - known
        assert not unknown, f"frames include ZIPs outside the universe: {sorted(unknown)}"
