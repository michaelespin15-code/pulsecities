"""
GET /api/health

Returns system health: database connectivity + last scraper run per source.
Used by:
- Uptime monitoring (simple 200 = alive check)
- Internal dashboard to see when data was last updated
- Frontend "data freshness" indicator
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["system"])

SCRAPER_NAMES = [
    "311_complaints",
    "dob_permits",
    "evictions",
    "acris_ownership",
    "mappluto",
]


@router.api_route("/health", methods=["GET", "HEAD"])
def health(db: Session = Depends(get_db)):
    """
    Returns 200 with scraper status if DB is reachable.
    Returns 503 if DB connection fails.
    HEAD is accepted because third-party uptime monitors default to it;
    a 405 there reads as an outage.
    """
    try:
        db.execute(text("SELECT 1"))
    except Exception:
        logger.exception("health check: database unreachable")
        raise HTTPException(status_code=503, detail="database unavailable")

    try:
        # One DISTINCT ON instead of a query per scraper. This route is the
        # uptime-monitor target and carries no rate limit, so its cost is
        # whatever anyone cares to send at it; five round trips per hit made
        # that a cheap amplifier.
        rows = db.execute(
            text("""
                SELECT DISTINCT ON (scraper_name)
                       scraper_name, status, started_at, records_processed,
                       watermark_timestamp
                FROM scraper_runs
                WHERE scraper_name = ANY(:names)
                ORDER BY scraper_name, started_at DESC
            """),
            {"names": SCRAPER_NAMES},
        ).fetchall()
        latest = {r.scraper_name: r for r in rows}

        scrapers = {}
        for name in SCRAPER_NAMES:
            last_run = latest.get(name)
            scrapers[name] = (
                {
                    "status": last_run.status,
                    "last_run": last_run.started_at.isoformat() if last_run.started_at else None,
                    "records_processed": last_run.records_processed,
                    "watermark": (
                        last_run.watermark_timestamp.isoformat()
                        if last_run.watermark_timestamp
                        else None
                    ),
                }
                if last_run
                else {"status": "never_run"}
            )

        return {
            "status": "ok",
            "db": "connected",
            "scrapers": scrapers,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
    except Exception:
        logger.exception("health check: scraper status query failed")
        raise HTTPException(status_code=503, detail="database unavailable")
