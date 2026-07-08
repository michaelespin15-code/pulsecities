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

import logging
import time

from fastapi import APIRouter, Depends, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

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
          AND o.doc_date >= CURRENT_DATE - make_interval(days => :lookback)
          AND p.zip_code IS NOT NULL
{_NOISE_SQL}
    ),
    reno_permits AS (
        SELECT bbl, MIN(filing_date) AS first_permit_date
        FROM permits_raw
        WHERE raw_data->>'job_type' IN ('A1', 'A2')
          AND filing_date >= CURRENT_DATE - make_interval(days => :lookback)
        GROUP BY bbl
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
        {"lookback": LOOKBACK_DAYS, "flip_window": FLIP_WINDOW_DAYS, "limit": FEED_LIMIT},
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
