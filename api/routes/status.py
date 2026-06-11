"""
GET /api/status

Public data-freshness status, one entry per source. Reports the last
successful run, the records it processed, the data-through date (the
scraper's watermark), and an ok/delayed state (ok = last success within
48 hours). Powers /status and the homepage freshness chip.

Rate limited to 60/minute by client IP.
"""

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["status"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

# Public-facing sources in display order. Internal scrapers (mappluto,
# dof_assessments, mtek_monitor) are deliberately left off the public page.
SOURCES = [
    ("acris_ownership", "Property transfers"),
    ("evictions",       "Evictions"),
    ("hpd_violations",  "HPD violations"),
    ("311_complaints",  "311 complaints"),
    ("dob_permits",     "Permits"),
    ("dcwp_licenses",   "Business licenses"),
    ("dhcr_rs",         "Rent stabilization"),
]

# A source is "ok" when its last successful run is this recent.
_OK_WINDOW = timedelta(hours=48)

# Sources with a known upstream pause carry an explanatory note when delayed.
_DELAY_NOTES = {
    "acris_ownership": "Source feed paused upstream at NYC Open Data.",
}


@router.get("/status")
@limiter.limit("60/minute")
def get_status(request: Request, response: Response, db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT DISTINCT ON (scraper_name)
            scraper_name, started_at, records_processed, watermark_timestamp
        FROM scraper_runs
        WHERE status = 'success'
        ORDER BY scraper_name, started_at DESC
    """)).fetchall()
    by_name = {r.scraper_name: r for r in rows}

    now = datetime.now(timezone.utc)
    sources = []
    most_recent = None  # newest watermark across sources, for the hero chip

    for key, name in SOURCES:
        run = by_name.get(key)
        last_success = run.started_at if run else None
        watermark = run.watermark_timestamp if run else None

        ok = last_success is not None and (now - last_success) <= _OK_WINDOW
        state = "ok" if ok else "delayed"

        if watermark and (most_recent is None or watermark > most_recent):
            most_recent = watermark

        entry = {
            "key": key,
            "name": name,
            "data_through": watermark.date().isoformat() if watermark else None,
            "records_processed": run.records_processed if run else None,
            "last_success": last_success.isoformat() if last_success else None,
            "state": state,
        }
        if state == "delayed" and key in _DELAY_NOTES:
            entry["note"] = _DELAY_NOTES[key]
        sources.append(entry)

    return {
        "generated_at": now.isoformat(),
        "data_through": most_recent.date().isoformat() if most_recent else None,
        "sources": sources,
    }
