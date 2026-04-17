"""
Citywide statistics endpoint for the homepage hero stat chips.

GET /api/stats
  Returns citywide 30-day LLC transfer count, eviction filing count, and the current
  top-risk ZIP with borough and score. Designed for a single fetch — no client-side
  waterfall needed for the hero overlay.

Rate limited to 60/minute by client IP (consistent with other public read endpoints).
"""

from fastapi import APIRouter, Depends, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

router = APIRouter(prefix="/stats", tags=["stats"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

# Inline CASE expression mapping ZIP ranges to borough names.
# Matches _BOROUGH_CASE in neighborhoods.py exactly — keep in sync.
_BOROUGH_CASE = """
    CASE
        WHEN CAST(ds.zip_code AS INTEGER) BETWEEN 10001 AND 10282 THEN 'Manhattan'
        WHEN CAST(ds.zip_code AS INTEGER) BETWEEN 10301 AND 10314 THEN 'Staten Island'
        WHEN CAST(ds.zip_code AS INTEGER) BETWEEN 10451 AND 10475 THEN 'Bronx'
        WHEN CAST(ds.zip_code AS INTEGER) BETWEEN 11201 AND 11239 THEN 'Brooklyn'
        WHEN CAST(ds.zip_code AS INTEGER) BETWEEN 11001 AND 11109 THEN 'Queens'
        WHEN CAST(ds.zip_code AS INTEGER) BETWEEN 11354 AND 11697 THEN 'Queens'
        ELSE NULL
    END
"""

# All valid NYC ZIP ranges — excludes sentinel/junk values.
_VALID_NYC_ZIP_CLAUSE = (
    "(CAST(ds.zip_code AS INTEGER) BETWEEN 10001 AND 10282)"
    " OR (CAST(ds.zip_code AS INTEGER) BETWEEN 10301 AND 10314)"
    " OR (CAST(ds.zip_code AS INTEGER) BETWEEN 10451 AND 10475)"
    " OR (CAST(ds.zip_code AS INTEGER) BETWEEN 11201 AND 11239)"
    " OR (CAST(ds.zip_code AS INTEGER) BETWEEN 11001 AND 11109)"
    " OR (CAST(ds.zip_code AS INTEGER) BETWEEN 11354 AND 11697)"
)


@router.get("")
@limiter.limit("60/minute")
def get_citywide_stats(request: Request, response: Response, db: Session = Depends(get_db)):
    """
    Citywide 30-day LLC transfers, eviction filings, and the current top-risk ZIP.
    Consumed by the homepage hero overlay stat chips.
    """
    llc_count = db.execute(text("""
        SELECT COUNT(DISTINCT bbl) FROM ownership_raw
        WHERE party_type = '2'
          AND doc_type IN ('DEED', 'DEEDP', 'ASST')
          AND party_name_normalized LIKE '%LLC%'
          AND doc_date >= CURRENT_DATE - INTERVAL '30 days'
          AND party_name_normalized NOT ILIKE '%MORTGAGE%'
          AND party_name_normalized NOT ILIKE '%LOAN SERVICING%'
          AND party_name_normalized NOT ILIKE '%LOAN SERVICE%'
          AND party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
          AND party_name_normalized NOT ILIKE '%CREDIT UNION%'
    """)).scalar() or 0

    eviction_count = db.execute(text("""
        SELECT COUNT(*) FROM evictions_raw
        WHERE executed_date >= CURRENT_DATE - INTERVAL '30 days'
    """)).scalar() or 0

    top_row = db.execute(text(f"""
        SELECT
            ds.zip_code,
            n.name,
            {_BOROUGH_CASE} AS borough,
            ds.score,
            ds.cache_generated_at
        FROM displacement_scores ds
        LEFT JOIN neighborhoods n ON ds.zip_code = n.zip_code
        WHERE ds.score IS NOT NULL
          AND ({_VALID_NYC_ZIP_CLAUSE})
        ORDER BY ds.score DESC
        LIMIT 1
    """)).fetchone()

    top_risk = None
    if top_row:
        top_risk = {
            "zip_code": top_row.zip_code,
            "name": top_row.name,
            "borough": top_row.borough,
            "score": round(float(top_row.score), 1),
            "last_updated": (
                top_row.cache_generated_at.isoformat()
                if top_row.cache_generated_at else None
            ),
        }

    return {
        "llc_transfers_30d": int(llc_count),
        "evictions_30d": int(eviction_count),
        "top_risk": top_risk,
    }
