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

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from models.database import get_db
from models.scraper import ScraperRun

logger = logging.getLogger(__name__)
router = APIRouter(tags=["system"])

SCRAPER_NAMES = [
    "311_complaints",
    "dob_permits",
    "evictions",
    "acris_ownership",
    "mappluto",
]


@router.get("/health")
def health(db: Session = Depends(get_db)):
    """
    Returns 200 with scraper status if DB is reachable.
    Returns 503 if DB connection fails.
    """
    try:
        scrapers = {}
        for name in SCRAPER_NAMES:
            last_run = (
                db.query(ScraperRun)
                .filter(ScraperRun.scraper_name == name)
                .order_by(ScraperRun.started_at.desc())
                .first()
            )
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
    except Exception as exc:
        logger.exception("Health check failed: %s", exc)
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")
