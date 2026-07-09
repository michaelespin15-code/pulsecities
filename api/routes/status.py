"""
GET /api/status

Public data-freshness status, one entry per source. Reports the last
successful run, the records it processed, the data-through date (the
scraper's watermark), and an ok/delayed state.

State reflects DATA AGE, not run recency. A source is "delayed" when its
data_through (watermark) has fallen further behind than that feed's natural
cadence allows. This matters because a scraper can succeed every night while
its upstream is frozen: the run is recent but the data is stale. Keying state
off the last successful run hid exactly that case (DCWP read "ok" on Apr-16
data because the nightly job kept succeeding). last_success is still reported
for the ops view, but it no longer drives the public badge.

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

# Per-source freshness threshold: a source is "delayed" once its data_through
# is older than this. Thresholds track each feed's natural cadence — not run
# recency — so a nightly job that keeps succeeding on a frozen upstream still
# reads delayed once the watermark stops advancing.
#
# The daily feeds get 7 days rather than a tight 2-3: their watermarks normally
# trail "today" by a few days (DOB permits routinely sits 4-5 days back) and a
# weekend adds two more, so anything under a week would flap on a healthy feed.
# A week-old daily source is genuinely stuck; weeks-old (DCWP, ACRIS) is the
# real failure this guards against.
_FRESHNESS = {
    "acris_ownership": timedelta(days=21),   # deeds publish with a ~2-week natural lag
    "evictions":       timedelta(days=7),
    "hpd_violations":  timedelta(days=7),
    "311_complaints":  timedelta(days=7),
    "dob_permits":     timedelta(days=7),
    "dcwp_licenses":   timedelta(days=7),
    "dhcr_rs":         timedelta(days=400),   # annual snapshot — see _SNAPSHOT_SOURCES
}
_DEFAULT_FRESHNESS = timedelta(days=7)

# Annual / snapshot sources expose no incremental data-through date (their
# watermark is null by design — there is no per-row date to advance). Freshness
# for these is measured from the last successful refresh instead: the snapshot
# is current as long as we keep pulling it. Without this they would read delayed
# forever for lack of a watermark.
_SNAPSHOT_SOURCES = {"dhcr_rs"}

# Sources with a known upstream pause carry an explanatory note when delayed.
_DELAY_NOTES = {
    "acris_ownership": "Source feed paused upstream at NYC Open Data.",
    "dcwp_licenses":   "Source feed paused upstream at NYC Open Data.",
}


def _source_state(
    key: str,
    watermark: datetime | None,
    last_success: datetime | None,
    now: datetime,
) -> str:
    """Public freshness state ('ok' | 'delayed'), driven by data age.

    For normal sources the reference is the watermark (data_through). For annual
    snapshot sources, which have no watermark, the reference is the last
    successful refresh. A missing reference reads delayed.
    """
    threshold = _FRESHNESS.get(key, _DEFAULT_FRESHNESS)
    reference = last_success if key in _SNAPSHOT_SOURCES else watermark
    if reference is None:
        return "delayed"
    return "ok" if (now - reference) <= threshold else "delayed"


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

        # ACRIS watermarks come from the feed's recorded_datetime, which can
        # run days ahead of the doc dates that actually persisted. Freshness
        # must describe what the site serves, so anchor to the table.
        if key == "acris_ownership":
            max_doc = db.execute(text("SELECT MAX(doc_date) FROM ownership_raw")).scalar()
            if max_doc is not None:
                watermark = datetime.combine(max_doc, datetime.min.time(), tzinfo=timezone.utc)

        state = _source_state(key, watermark, last_success, now)

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
