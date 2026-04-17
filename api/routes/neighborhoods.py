"""
Neighborhood (zip code) level API endpoints.

GET /api/neighborhoods              — GeoJSON FeatureCollection (API-01)
GET /api/neighborhoods/{zip_code}/score — score + signal breakdown (API-02)
"""

import json
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db
from models.scores import DisplacementScore

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/neighborhoods", tags=["neighborhoods"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)


@router.get("")
@limiter.limit("60/minute")
def list_neighborhoods_geojson(request: Request, response: Response, db: Session = Depends(get_db)):
    """
    Returns all neighborhoods as a GeoJSON FeatureCollection.
    Geometry is simplified via ST_SimplifyPreserveTopology for performance.
    Score is LEFT JOINed — null for neighborhoods with no displacement data.
    """
    rows = db.execute(
        text(
            """
            SELECT
                n.id,
                n.zip_code,
                n.name,
                n.borough,
                ds.score,
                ds.cache_generated_at,
                ST_AsGeoJSON(ST_SimplifyPreserveTopology(n.geometry, 0.0001)) as geom_json
            FROM neighborhoods n
            LEFT JOIN displacement_scores ds ON n.zip_code = ds.zip_code
            WHERE n.geometry IS NOT NULL
              AND n.zip_code != '99999'
            """
        )
    ).fetchall()

    features = []
    for row in rows:
        geom = json.loads(row.geom_json) if row.geom_json else None
        features.append(
            {
                "type": "Feature",
                "id": row.id,
                "geometry": geom,
                "properties": {
                    "zip_code": row.zip_code,
                    "name": row.name,
                    "borough": row.borough,
                    "score": round(row.score, 1) if row.score is not None else None,
                    "last_updated": (
                        row.cache_generated_at.isoformat()
                        if row.cache_generated_at
                        else None
                    ),
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}


@router.get("/{zip_code}/score")
@limiter.limit("60/minute")
def get_neighborhood_score(
    request: Request, response: Response, zip_code: str, db: Session = Depends(get_db)
):
    """
    Returns displacement score + signal breakdown for a single zip code.
    """
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        raise HTTPException(status_code=400, detail="zip_code must be a 5-digit string")

    score = (
        db.query(DisplacementScore)
        .filter(DisplacementScore.zip_code == zip_code)
        .first()
    )

    if not score:
        raise HTTPException(
            status_code=404,
            detail=f"No score data for zip code {zip_code}.",
        )

    breakdown = score.signal_breakdown or {}
    return {
        "zip_code": zip_code,
        "score": round(score.score, 1) if score.score is not None else None,
        # Five-signal breakdown — all values normalized to [0–100].
        # Keys: permits, evictions, llc_acquisitions, assessment_spike, complaint_rate.
        # assessment_spike is 0.0 (dormant Phase 4 — no YoY DOF baseline yet).
        "signal_breakdown": breakdown,
        # Human-readable summary — generated from score tier + top signals.
        "summary_text": _build_summary(score.score, breakdown),
        # Per-signal data freshness — ISO-8601 UTC timestamps.
        # Used by the UI freshness display panel (Phase 5).
        "signal_last_updated": score.signal_last_updated or {},
        "last_updated": (
            score.cache_generated_at.isoformat() if score.cache_generated_at else None
        ),
    }


_VALID_BOROUGHS = {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"}

# ZIP code ranges used to derive borough when the borough column is unpopulated.
# Each value is a raw SQL expression safe to embed — it contains no user input.
_BOROUGH_ZIP_CLAUSE = {
    "Manhattan":    "CAST(ds.zip_code AS INTEGER) BETWEEN 10001 AND 10282",
    "Staten Island":"CAST(ds.zip_code AS INTEGER) BETWEEN 10301 AND 10314",
    "Bronx":        "CAST(ds.zip_code AS INTEGER) BETWEEN 10451 AND 10475",
    "Brooklyn":     "CAST(ds.zip_code AS INTEGER) BETWEEN 11201 AND 11239",
    "Queens":       "(CAST(ds.zip_code AS INTEGER) BETWEEN 11001 AND 11109 OR CAST(ds.zip_code AS INTEGER) BETWEEN 11354 AND 11697)",
}

# Inline CASE expression that maps each ZIP to its borough name.
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

# Filters out junk/sentinel ZIP codes that fall outside all NYC borough ranges.
_VALID_NYC_ZIP_CLAUSE = " OR ".join(f"({v})" for v in _BOROUGH_ZIP_CLAUSE.values())


@router.get("/top-risk")
@limiter.limit("60/minute")
def get_top_risk_neighborhoods(
    request: Request,
    response: Response,
    limit: int = 10,
    borough: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Returns the top N zip codes by current displacement score.
    Consumed by the landing page "Most at-risk neighborhoods right now" section.
    limit is capped at 25 regardless of what the caller requests.
    Optional borough filter: one of Manhattan, Brooklyn, Queens, Bronx, Staten Island.
    Borough is derived from ZIP code ranges (the neighborhoods.borough column is unpopulated).
    """
    if borough is not None and borough not in _VALID_BOROUGHS:
        raise HTTPException(
            status_code=400,
            detail=f"borough must be one of: {', '.join(sorted(_VALID_BOROUGHS))}",
        )

    capped = min(max(1, limit), 25)
    params: dict = {"limit": capped}
    # Use the specific borough range when filtered; otherwise restrict to all valid NYC ZIPs
    # so junk/sentinel values (00000, 12345, etc.) never appear in results.
    zip_filter = (
        f"AND ({_BOROUGH_ZIP_CLAUSE[borough]})"
        if borough
        else f"AND ({_VALID_NYC_ZIP_CLAUSE})"
    )

    rows = db.execute(
        text(
            f"""
            SELECT
                ds.zip_code,
                n.name,
                {_BOROUGH_CASE} AS borough,
                ds.score,
                ds.signal_breakdown
            FROM displacement_scores ds
            LEFT JOIN neighborhoods n ON ds.zip_code = n.zip_code
            WHERE ds.score IS NOT NULL
              {zip_filter}
            ORDER BY ds.score DESC
            LIMIT :limit
            """
        ),
        params,
    ).fetchall()

    result = []
    for i, row in enumerate(rows):
        dominant, _ = _dominant_signal(row.signal_breakdown or {})
        result.append(
            {
                "rank": i + 1,
                "zip_code": row.zip_code,
                "name": row.name,
                "borough": row.borough,
                "score": round(row.score, 1),
                "dominant_signal": dominant,
            }
        )

    return {"neighborhoods": result}


def _dominant_signal(breakdown: dict) -> tuple:
    """Returns (key, value) for the highest-scoring signal in the breakdown."""
    valid = {k: v for k, v in breakdown.items() if isinstance(v, (int, float))}
    if not valid:
        return None, None
    key = max(valid, key=valid.__getitem__)
    return key, valid[key]


# ---------------------------------------------------------------------------
# Narrative summary
# ---------------------------------------------------------------------------

_SIGNAL_LABELS = {
    "permits": "renovation permit filings",
    "evictions": "eviction filings",
    "llc_acquisitions": "LLC property acquisitions",
    "complaint_rate": "tenant complaints",
    "rs_unit_loss": "rent-stabilized unit loss",
    "assessment_spike": "tax assessment increases",
}


def _build_summary(score: float | None, breakdown: dict[str, Any]) -> str:
    """
    Generate a 1–2 sentence plain-English summary from score + signal breakdown.

    Tier thresholds (mirrors frontend color bands):
      Critical  76–100  — severe, multiple signals elevated
      High      56–75   — clear pressure, dominant signal named
      Moderate  34–55   — emerging pressure, primary signal named
      Low        1–33   — limited evidence of displacement pressure

    Top signals are the breakdown keys with values above 30 (non-trivial),
    sorted descending. Up to 2 are named in the sentence.
    """
    if score is None:
        return "Displacement risk data is not yet available for this neighborhood."

    s = round(score, 1)

    # Tier label and opening clause
    if s >= 76:
        tier = "Critical"
        opening = f"This neighborhood shows critical displacement pressure (score {s})."
    elif s >= 56:
        tier = "High"
        opening = f"This neighborhood shows high displacement pressure (score {s})."
    elif s >= 34:
        tier = "Moderate"
        opening = f"This neighborhood shows moderate displacement pressure (score {s})."
    else:
        tier = "Low"
        opening = f"This neighborhood shows low displacement pressure relative to the rest of NYC (score {s})."

    # Find top signals above threshold
    active = sorted(
        [(k, v) for k, v in breakdown.items() if isinstance(v, (int, float)) and v > 30],
        key=lambda x: x[1],
        reverse=True,
    )
    top = [_SIGNAL_LABELS.get(k, k) for k, _ in active[:2]]

    if tier == "Low":
        detail = "No individual signal stands out above citywide averages."
    elif not top:
        detail = "Signals are present but no single factor dominates."
    elif len(top) == 1:
        detail = f"The primary driver is {top[0]}."
    else:
        detail = f"The dominant drivers are {top[0]} and {top[1]}."

    return f"{opening} {detail}"
