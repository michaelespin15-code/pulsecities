"""
Speculation Radar — concentrated LLC buying, detected before entity resolution.

One LLC taking the deed on three or more distinct buildings in the same ZIP inside
90 days is the acquisition run that usually precedes repositioning: the buyer is
assembling a position, not picking up a single property. Entity resolution would
eventually group these under an operator profile; the radar surfaces the raw
pattern the moment the deeds land, months earlier.

GET /api/radar  — JSON feed (also consumed by the SSR /radar page in frontend.py)

Doc types are restricted to DEED and DEEDP because the page claims "took the deed";
assignments would widen recall but break that claim. The lender noise filter is
shared with Flip Watch so a servicer taking title on scattered lots never reads
as a speculation cluster.
"""

import logging
import time

from fastapi import APIRouter, Depends, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db
from api.routes.flips import _NOISE_TERMS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/radar", tags=["radar"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

# The published pattern: MIN_BUILDINGS distinct buildings in one ZIP within
# RADAR_WINDOW_DAYS. Both numbers appear verbatim on the /radar page.
RADAR_WINDOW_DAYS = 90
MIN_BUILDINGS = 3
FEED_LIMIT = 40

_NOISE_SQL = "\n".join(
    f"      AND o.party_name_normalized NOT ILIKE '%{term}%'" for term in _NOISE_TERMS
)

_RADAR_SQL = text(f"""
    WITH llc_deeds AS (
        SELECT DISTINCT ON (o.bbl, o.party_name_normalized)
               p.zip_code, o.bbl, o.doc_date, o.doc_amount, p.address,
               o.party_name_normalized AS buyer
        FROM ownership_raw o
        JOIN parcels p ON p.bbl = o.bbl
        WHERE o.party_name_normalized LIKE '%LLC%'
          AND o.doc_type IN ('DEED', 'DEEDP')
          AND o.party_type = '2'
          AND o.doc_date >= CURRENT_DATE - make_interval(days => :window)
          AND p.zip_code IS NOT NULL
{_NOISE_SQL}
        ORDER BY o.bbl, o.party_name_normalized, o.doc_date DESC
    ),
    clusters AS (
        SELECT buyer, zip_code,
               COUNT(*) AS building_count,
               MIN(doc_date) AS first_deed,
               MAX(doc_date) AS last_deed,
               (MAX(doc_date) - MIN(doc_date)) AS span_days,
               SUM(doc_amount) AS total_amount
        FROM llc_deeds
        GROUP BY buyer, zip_code
        HAVING COUNT(*) >= :min_buildings
    )
    SELECT c.buyer, c.zip_code, c.building_count, c.first_deed, c.last_deed,
           c.span_days, c.total_amount, n.name AS neighborhood,
           (SELECT json_agg(json_build_object(
                       'bbl', d.bbl, 'address', d.address,
                       'deed_date', d.doc_date, 'amount', d.doc_amount)
                    ORDER BY d.doc_date DESC)
            FROM llc_deeds d
            WHERE d.buyer = c.buyer AND d.zip_code = c.zip_code) AS properties
    FROM clusters c
    LEFT JOIN neighborhoods n ON n.zip_code = c.zip_code
    ORDER BY c.last_deed DESC, c.building_count DESC
    LIMIT :limit
""")

# Result cache. Deeds only change on the nightly refresh, so one query per TTL
# serves every visitor.
_CACHE_TTL = 3600
_cache: tuple[list[dict], float] | None = None


def query_radar(db: Session, limit: int = FEED_LIMIT) -> list[dict]:
    """Speculation clusters, most recent buying run first. Cached for _CACHE_TTL.

    Each row is one (buyer, ZIP) pair: an LLC that took the deed on
    MIN_BUILDINGS or more distinct buildings in that ZIP within RADAR_WINDOW_DAYS.
    """
    global _cache
    if _cache and time.monotonic() < _cache[1]:
        return _cache[0][:limit]

    rows = db.execute(
        _RADAR_SQL,
        {"window": RADAR_WINDOW_DAYS, "min_buildings": MIN_BUILDINGS, "limit": FEED_LIMIT},
    ).fetchall()

    def _days(v):
        # Postgres date-minus-date comes back as a plain int via psycopg, but guard
        # for a timedelta in case the driver hands one back.
        if v is None:
            return None
        return v.days if hasattr(v, "days") else int(v)

    clusters = [
        {
            "buyer": row.buyer,
            "zip_code": row.zip_code,
            "neighborhood": row.neighborhood,
            "building_count": int(row.building_count),
            "first_deed": row.first_deed.isoformat() if row.first_deed else None,
            "last_deed": row.last_deed.isoformat() if row.last_deed else None,
            "span_days": _days(row.span_days),
            "total_amount": float(row.total_amount) if row.total_amount else None,
            "properties": [
                {
                    "bbl": p["bbl"],
                    "address": p["address"] or f"BBL {p['bbl']}",
                    "deed_date": p["deed_date"],
                    "amount": float(p["amount"]) if p["amount"] else None,
                }
                for p in (row.properties or [])
            ],
        }
        for row in rows
    ]
    _cache = (clusters, time.monotonic() + _CACHE_TTL)
    return clusters[:limit]


@router.get("")
@limiter.limit("60/minute")
def get_radar(request: Request, response: Response, db: Session = Depends(get_db)):
    """Speculation clusters as JSON."""
    response.headers["Cache-Control"] = "public, max-age=3600"
    clusters = query_radar(db)
    return {
        "window_days": RADAR_WINDOW_DAYS,
        "min_buildings": MIN_BUILDINGS,
        "count": len(clusters),
        "clusters": clusters,
    }
