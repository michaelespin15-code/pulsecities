"""
Flip Watch — citywide renovation-flip feed.

Surfaces the renovation-flip pattern across all of NYC in one place: an LLC takes
a building by deed, then files an A1/A2 renovation permit on the same lot within
60 days. That fast turn is the signal the per-ZIP pulse feed already detects one
neighborhood at a time; here it is rolled up citywide and sorted by recency.

GET /api/flips  — JSON feed (also consumed by the SSR /flips page in frontend.py)

The 60-day buy-to-permit window matches the published methodology and the per-ZIP
/neighborhoods/{zip}/renovation-flip endpoint. The 365-day lookback is just how far
back the feed reaches — flips are rare enough that a year keeps the list substantial
without diluting the signal.
"""

import json
import logging
import time
from pathlib import Path

from fastapi import APIRouter, Depends, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.freshness import ACRIS_THROUGH_SQL
from api.permit_kinds import renovation_sql
from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/flips", tags=["flips"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

# How far back the feed reaches, and the buy-to-permit window that defines a flip.
LOOKBACK_DAYS = 365
FLIP_WINDOW_DAYS = 60
FEED_LIMIT = 60

# Lenders, servicers, and GSEs take title by deed too, but a bank filing a
# renovation permit is not an investor flip. Exclude the obvious debt entities so
# the feed stays about operators, not loan servicing.
_NOISE_TERMS = (
    "MORTGAGE", "LOAN", "LENDER", "FUNDING", "SERVICING",
    "FEDERAL SAVINGS", "CREDIT UNION", "BANK",
)

def _deed_anchor(db):
    """The last day the city published a deed, or today if that is unknowable.

    The deed side of the window ends here rather than at CURRENT_DATE. ACRIS
    publishes on a lag and freezes for weeks; anchoring on today spends the tail
    of the lookback on days that cannot contain a deed, which reads as a quiet
    market rather than an unpublished one.

    Only the deed side. The permit side stays on CURRENT_DATE, because the flip
    is a deed followed by a permit and permits are current: moving that end back
    would drop the most recent flips, which are the ones worth reading.
    """
    from datetime import date
    return db.execute(text(ACRIS_THROUGH_SQL)).scalar() or date.today()


_NOISE_SQL = "\n".join(
    f"      AND o.party_name_normalized NOT ILIKE '%{term}%'" for term in _NOISE_TERMS
)

_FLIP_SQL = text(f"""
    WITH llc_transfers AS (
        SELECT p.zip_code, o.bbl, o.doc_date AS transfer_date,
               o.party_name_normalized AS buyer, o.doc_amount, p.address
        FROM ownership_raw o
        JOIN parcels p ON p.bbl = o.bbl
        WHERE o.party_name_normalized LIKE '%LLC%'
          AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
          AND o.party_type = '2'
          AND o.doc_date >  (:deed_anchor)::date - make_interval(days => :lookback)
          AND o.doc_date <= (:deed_anchor)::date
          AND p.zip_code IS NOT NULL
{_NOISE_SQL}
    ),
    -- Scoped to the lots that could possibly qualify. The join below is an
    -- inner join on bbl, so every other building's permits were being read and
    -- then discarded: a full year of permits_raw, and the BIS half of the
    -- renovation rule is a JSONB extraction that no index can serve. That cost
    -- 30 seconds a render, and it only started costing it when the DOB NOW
    -- backfill made this query correct. Restricting the scan to the LLC
    -- purchases changes no row of the result.
    reno_permits AS (
        SELECT pr.bbl, MIN(pr.filing_date) AS first_permit_date
        FROM permits_raw pr
        WHERE {renovation_sql('pr')}
          AND pr.filing_date >= CURRENT_DATE - make_interval(days => :lookback)
          AND pr.bbl IN (SELECT bbl FROM llc_transfers)
        GROUP BY pr.bbl
    ),
    combined AS (
        SELECT DISTINCT ON (l.bbl)
               l.zip_code, l.bbl, l.address, l.buyer, l.doc_amount,
               l.transfer_date, r.first_permit_date,
               (r.first_permit_date - l.transfer_date) AS days_between
        FROM llc_transfers l
        JOIN reno_permits r ON r.bbl = l.bbl
        WHERE r.first_permit_date > l.transfer_date
          AND (r.first_permit_date - l.transfer_date) <= :flip_window
        ORDER BY l.bbl, l.transfer_date DESC
    )
    SELECT c.bbl, c.zip_code, c.address, c.buyer, c.doc_amount,
           c.transfer_date, c.first_permit_date, c.days_between,
           n.name AS neighborhood
    FROM combined c
    LEFT JOIN neighborhoods n ON n.zip_code = c.zip_code
    ORDER BY c.transfer_date DESC, c.first_permit_date DESC
    LIMIT :limit
""")

# Result cache. The underlying records only change on the nightly refresh, so a
# single query per TTL serves every visitor and keeps the citywide CTE off the hot path.
_CACHE_TTL = 3600
_cache: tuple[list[dict], float] | None = None


def query_flips(db: Session, limit: int = FEED_LIMIT) -> list[dict]:
    """Citywide renovation-flips, newest first. Cached for _CACHE_TTL seconds.

    Each row is one building: an LLC deed acquisition followed by an A1/A2 permit
    within FLIP_WINDOW_DAYS, scoped to the last LOOKBACK_DAYS.
    """
    global _cache
    if _cache and time.monotonic() < _cache[1]:
        return _cache[0][:limit]

    rows = db.execute(
        _FLIP_SQL,
        {"lookback": LOOKBACK_DAYS, "flip_window": FLIP_WINDOW_DAYS,
         "limit": FEED_LIMIT, "deed_anchor": _deed_anchor(db)},
    ).fetchall()

    def _days(v):
        # Postgres date-minus-date comes back as a plain int via psycopg, but guard
        # for a timedelta in case the driver hands one back.
        if v is None:
            return None
        return v.days if hasattr(v, "days") else int(v)

    flips = [
        {
            "bbl": row.bbl,
            "address": row.address or f"BBL {row.bbl}",
            "zip_code": row.zip_code,
            "neighborhood": row.neighborhood,
            "buyer": row.buyer,
            "doc_amount": float(row.doc_amount) if row.doc_amount else None,
            "transfer_date": row.transfer_date.isoformat() if row.transfer_date else None,
            "permit_date": row.first_permit_date.isoformat() if row.first_permit_date else None,
            "days_between": _days(row.days_between),
        }
        for row in rows
    ]
    _cache = (flips, time.monotonic() + _CACHE_TTL)
    return flips[:limit]


@router.get("")
@limiter.limit("60/minute")
def get_flips(request: Request, response: Response, db: Session = Depends(get_db)):
    """Citywide renovation-flip feed as JSON."""
    response.headers["Cache-Control"] = "public, max-age=3600"
    flips = query_flips(db)
    return {
        "window_days": LOOKBACK_DAYS,
        "flip_window_days": FLIP_WINDOW_DAYS,
        "count": len(flips),
        "flips": flips,
    }


# Weekly eviction-flip editions, written by scripts/weekly_eviction_flips.py.
# The scan appends editions with approved: false; flipping the flag after
# review is the human gate between scan output and publication. The homepage
# docket features the strongest arc of the newest approved edition and keeps
# its built-in example when nothing is approved yet.
_EDITIONS_PATH = Path(__file__).resolve().parents[2] / "scripts" / "eviction_flips_editions.json"

_BOROUGHS = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island"}


# The public shape of an arc. An explicit whitelist so internal fields in the
# editions file (dedupe keys, future reviewer notes) never leak to the API.
_ARC_FIELDS = ("bbl", "address", "zip_code", "eviction_date", "eviction_count",
               "buy_doc", "buy_date", "buy_amt", "buyer",
               "sell_doc", "sell_date", "sell_amt", "gain_pct")


@router.get("/editions/latest")
@limiter.limit("60/minute")
def latest_edition(request: Request, response: Response, db: Session = Depends(get_db)):
    """Featured arc of the newest approved eviction-flip edition."""
    response.headers["Cache-Control"] = "public, max-age=3600"
    # The editions file is script-written but human-edited for approval; a
    # stray edit must degrade to the no-edition response, never a 500.
    try:
        editions = json.loads(_EDITIONS_PATH.read_text()).get("editions", [])
        approved = [e for e in editions if e.get("approved") and e.get("arcs")]
        if not approved:
            return {"week": None, "arc": None}
        edition = approved[-1]
        arc = max(edition["arcs"], key=lambda a: float(a.get("gain_pct") or 0))
        payload = {k: arc.get(k) for k in _ARC_FIELDS}
        week = edition.get("week")
    except Exception:
        logger.exception("Malformed editions file at %s", _EDITIONS_PATH)
        return {"week": None, "arc": None}

    neighborhood = None
    if payload.get("zip_code"):
        neighborhood = db.execute(
            text("SELECT name FROM neighborhoods WHERE zip_code = :zip"),
            {"zip": payload["zip_code"]},
        ).scalar()
    payload["neighborhood"] = neighborhood
    payload["borough"] = _BOROUGHS.get(str(payload.get("bbl", ""))[:1])
    return {"week": week, "arc": payload}
