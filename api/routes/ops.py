"""
Private ops dashboard.

GET /ops?t={OPS_TOKEN}           — serves dashboard HTML
GET /api/ops/summary?t={token}   — returns JSON for the dashboard

Both return 404 on missing/wrong token so the endpoint is invisible to scanners.
"""

import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["ops"])

_TOKEN = os.getenv("OPS_TOKEN", "")
_FRONTEND = Path(__file__).parent.parent.parent / "frontend"
_ERROR_LOG = Path("/var/log/pulsecities/gunicorn-error.log")
_SCRAPER_LOG = Path("/var/log/pulsecities/scraper.log")
_DIGEST_LOG = Path("/var/log/pulsecities/digest.log")


def _auth(t: str = Query(default="")) -> None:
    if not _TOKEN or t != _TOKEN:
        raise HTTPException(status_code=404)


@router.get("/ops", include_in_schema=False)
def ops_page(t: str = Query(default="")):
    if not _TOKEN or t != _TOKEN:
        raise HTTPException(status_code=404)
    return FileResponse(str(_FRONTEND / "ops.html"), media_type="text/html")


@router.get("/api/ops/summary", include_in_schema=False)
def ops_summary(t: str = Query(default=""), db: Session = Depends(get_db)):
    if not _TOKEN or t != _TOKEN:
        raise HTTPException(status_code=404)

    # --- Subscribers ---
    sub_totals = db.execute(text("""
        SELECT
            COUNT(*)                                                                    AS total,
            COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE)                          AS today,
            COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '7 days')      AS week,
            COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '30 days')     AS month
        FROM subscribers
    """)).fetchone()

    top_zips = db.execute(text("""
        SELECT s.zip_code, n.name, COUNT(*) AS cnt
        FROM subscribers s
        LEFT JOIN neighborhoods n ON s.zip_code = n.zip_code
        GROUP BY s.zip_code, n.name
        ORDER BY cnt DESC
        LIMIT 8
    """)).fetchall()

    recent_subs = db.execute(text("""
        SELECT s.email, s.zip_code, n.name, s.created_at
        FROM subscribers s
        LEFT JOIN neighborhoods n ON s.zip_code = n.zip_code
        ORDER BY s.created_at DESC
        LIMIT 12
    """)).fetchall()

    # --- Data freshness ---
    freshness = db.execute(text("""
        SELECT
            (SELECT MAX(doc_date)           FROM ownership_raw)       AS acris,
            (SELECT MAX(executed_date)      FROM evictions_raw)       AS evictions,
            (SELECT MAX(filing_date)        FROM permits_raw)         AS permits,
            (SELECT MAX(created_date)       FROM complaints_raw)      AS complaints,
            (SELECT MAX(cache_generated_at) FROM displacement_scores) AS scores_computed,
            (SELECT MAX(scored_at)          FROM score_history)       AS score_history_latest
    """)).fetchone()

    # --- Score distribution ---
    dist = db.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE score >= 85)               AS critical,
            COUNT(*) FILTER (WHERE score >= 67 AND score < 85) AS high,
            COUNT(*) FILTER (WHERE score >= 34 AND score < 67) AS moderate,
            COUNT(*) FILTER (WHERE score < 34)                AS low,
            COUNT(*) FILTER (WHERE score IS NOT NULL)         AS scored,
            COUNT(*)                                          AS total
        FROM displacement_scores
    """)).fetchone()

    # --- Top risk right now ---
    top_risk = db.execute(text("""
        SELECT ds.zip_code, n.name, ds.score
        FROM displacement_scores ds
        LEFT JOIN neighborhoods n ON ds.zip_code = n.zip_code
        WHERE ds.score IS NOT NULL
          AND (
            CAST(ds.zip_code AS INTEGER) BETWEEN 10001 AND 10282 OR
            CAST(ds.zip_code AS INTEGER) BETWEEN 10301 AND 10314 OR
            CAST(ds.zip_code AS INTEGER) BETWEEN 10451 AND 10475 OR
            CAST(ds.zip_code AS INTEGER) BETWEEN 11201 AND 11239 OR
            CAST(ds.zip_code AS INTEGER) BETWEEN 11001 AND 11109 OR
            CAST(ds.zip_code AS INTEGER) BETWEEN 11354 AND 11697
          )
        ORDER BY ds.score DESC
        LIMIT 5
    """)).fetchall()

    # --- Recent log tail ---
    def _tail(path: Path, n: int = 10) -> list[str]:
        if not path.exists():
            return []
        try:
            lines = path.read_text(errors="replace").splitlines()
            return [l for l in lines if l.strip()][-n:]
        except Exception:
            return []

    # --- System ---
    disk = shutil.disk_usage("/")
    disk_pct = round(disk.used / disk.total * 100, 1)
    disk_free_gb = round(disk.free / 1024 ** 3, 1)

    def _mask(email: str) -> str:
        if not email or "@" not in email:
            return email
        local, domain = email.rsplit("@", 1)
        return local[0] + "***@" + domain

    def _fmt(val) -> str | None:
        if val is None:
            return None
        return val.isoformat() if hasattr(val, "isoformat") else str(val)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "subscribers": {
            "total": int(sub_totals.total or 0),
            "today": int(sub_totals.today or 0),
            "week":  int(sub_totals.week or 0),
            "month": int(sub_totals.month or 0),
            "top_zips": [
                {"zip_code": r.zip_code, "name": r.name or r.zip_code, "count": int(r.cnt)}
                for r in top_zips
            ],
            "recent": [
                {
                    "email":      _mask(r.email),
                    "zip_code":   r.zip_code,
                    "name":       r.name or r.zip_code,
                    "created_at": _fmt(r.created_at),
                }
                for r in recent_subs
            ],
        },
        "freshness": {
            "acris":                _fmt(freshness.acris),
            "evictions":            _fmt(freshness.evictions),
            "permits":              _fmt(freshness.permits),
            "complaints":           _fmt(freshness.complaints),
            "scores_computed":      _fmt(freshness.scores_computed),
            "score_history_latest": _fmt(freshness.score_history_latest),
        },
        "score_dist": {
            "critical": int(dist.critical or 0),
            "high":     int(dist.high or 0),
            "moderate": int(dist.moderate or 0),
            "low":      int(dist.low or 0),
            "scored":   int(dist.scored or 0),
            "total":    int(dist.total or 0),
        },
        "top_risk": [
            {"zip_code": r.zip_code, "name": r.name or r.zip_code, "score": round(float(r.score), 1)}
            for r in top_risk
        ],
        "logs": {
            "errors":  _tail(_ERROR_LOG, 8),
            "scraper": _tail(_SCRAPER_LOG, 6),
            "digest":  _tail(_DIGEST_LOG, 4),
        },
        "system": {
            "disk_used_pct": disk_pct,
            "disk_free_gb":  disk_free_gb,
        },
    }
