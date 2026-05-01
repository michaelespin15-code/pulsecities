"""
Neighborhood (zip code) level API endpoints.

GET /api/neighborhoods              — GeoJSON FeatureCollection
GET /api/neighborhoods/{zip_code}/score — score + signal breakdown
"""

import hashlib
import json
import logging
import os
import time as _time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.nyc import DISPLACEMENT_COMPLAINT_TYPES
from models.database import get_db
from models.neighborhoods import Neighborhood
from models.scores import DisplacementScore

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/neighborhoods", tags=["neighborhoods"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

_PERF = os.getenv("PERF_LOGGING") == "1"

# GeoJSON full FeatureCollection. Geometry only changes when MapPLUTO is refreshed
# (monthly), scores only change nightly. 23h TTL keeps this warm all day.
_GEOJSON_CACHE: dict = {}
_GEOJSON_TTL = 82800  # 23 hours


@router.get("")
@limiter.limit("60/minute")
def list_neighborhoods_geojson(request: Request, response: Response, db: Session = Depends(get_db)):
    """
    Returns all neighborhoods as a GeoJSON FeatureCollection.
    Geometry is simplified via ST_SimplifyPreserveTopology for performance.
    Score is LEFT JOINed — null for neighborhoods with no displacement data.
    """
    cached = _GEOJSON_CACHE.get("data")
    if cached and _time.monotonic() < cached[2]:
        body_bytes, etag, _cached_at = cached[0], cached[1], cached[3]
        age_ms = round((_time.monotonic() - _cached_at) * 1000)
        logger.info("[cache] neighborhoods hit age_ms=%d", age_ms)
    else:
        logger.info("[cache] neighborhoods miss")
        t0 = _time.monotonic()
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

        body_bytes = json.dumps(
            {"type": "FeatureCollection", "features": features},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        etag = f'"{hashlib.md5(body_bytes).hexdigest()}"'
        _cached_at = _time.monotonic()
        _GEOJSON_CACHE["data"] = (body_bytes, etag, _cached_at + _GEOJSON_TTL, _cached_at)

        elapsed = _time.monotonic() - t0
        logger.info("[cache] neighborhoods miss resolved %.2fs rows=%d bytes=%d",
                    elapsed, len(rows), len(body_bytes))

    headers = {"Cache-Control": "public, max-age=82800", "ETag": etag}

    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)

    return Response(content=body_bytes, media_type="application/json", headers=headers)


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

    response.headers["Cache-Control"] = "public, max-age=3600"
    score = (
        db.query(DisplacementScore)
        .filter(DisplacementScore.zip_code == zip_code)
        .first()
    )

    hood = db.query(Neighborhood).filter(Neighborhood.zip_code == zip_code).first()

    if not score:
        # displacement_scores row missing (e.g. mid-scraper state, test teardown).
        # Fall back to neighborhoods.current_score so the sidebar never 404s for
        # a ZIP that is part of the scoring universe.
        if not hood or hood.current_score is None:
            raise HTTPException(status_code=404, detail=f"No score data for zip code {zip_code}.")
        fallback_score = round(hood.current_score, 1)
        return {
            "zip_code": zip_code,
            "name": hood.name,
            "borough": _borough_from_zip(zip_code),
            "score": fallback_score,
            "signal_breakdown": {},
            "signal_raw_counts": {},
            "summary_text": None,
            "signal_last_updated": {},
            "last_updated": None,
        }

    breakdown   = score.signal_breakdown or {}
    raw_counts  = _fetch_raw_counts(db, zip_code)
    return {
        "zip_code": zip_code,
        "name": hood.name if hood else None,
        "borough": _borough_from_zip(zip_code),
        "score": round(score.score, 1) if score.score is not None else None,
        "signal_breakdown": breakdown,
        "signal_raw_counts": raw_counts,
        "summary_text": _build_summary(score.score, breakdown, raw_counts),
        "signal_last_updated": score.signal_last_updated or {},
        "last_updated": (
            score.cache_generated_at.isoformat() if score.cache_generated_at else None
        ),
    }


def _borough_from_zip(zip_code: str) -> str | None:
    try:
        z = int(zip_code)
    except ValueError:
        return None
    if 10001 <= z <= 10282:
        return "Manhattan"
    if 10301 <= z <= 10314:
        return "Staten Island"
    if 10451 <= z <= 10475:
        return "Bronx"
    if 11201 <= z <= 11239:
        return "Brooklyn"
    if (11001 <= z <= 11109) or (11354 <= z <= 11697):
        return "Queens"
    return None


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

# TTL caches — data changes once per day at 2am UTC.
# 23h keeps each worker warm all day without risking stale reads across scoring runs.
_TOP_RISK_CACHE: dict = {}
_TOP_RISK_TTL = 82800  # 23 hours

_TOP_MOVERS_CACHE: dict = {}
_TOP_MOVERS_TTL = 3600


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

    response.headers["Cache-Control"] = "public, max-age=82800"
    capped = min(max(1, limit), 25)
    cache_key = (capped, borough)
    cached = _TOP_RISK_CACHE.get(cache_key)
    if cached and _time.monotonic() < cached[1]:
        return {"neighborhoods": cached[0]}

    # Fetch extra rows so we can filter out any with raw_count == 0 and still fill the list.
    fetch_limit = min(capped * 3, 75)
    params: dict = {"limit": fetch_limit, "complaint_types": list(DISPLACEMENT_COMPLAINT_TYPES)}
    # Use the specific borough range when filtered; otherwise restrict to all valid NYC ZIPs
    # so junk/sentinel values (00000, 12345, etc.) never appear in results.
    zip_filter = (
        f"AND ({_BOROUGH_ZIP_CLAUSE[borough]})"
        if borough
        else f"AND ({_VALID_NYC_ZIP_CLAUSE})"
    )
    # When a borough filter is active the PERCENT_RANK() window covers only those ZIPs.
    # For the un-filtered case the window covers all valid NYC ZIPs (D-15 denominator).
    # outer_where is placed after "SELECT * FROM ranked" where ds alias is out of scope —
    # use bare zip_code (the column name ranked exposes) instead of ds.zip_code.
    outer_where = (
        f"WHERE ({_BOROUGH_ZIP_CLAUSE[borough].replace('ds.zip_code', 'zip_code')})"
        if borough else ""
    )

    _tr_t0 = _time.monotonic()
    rows = db.execute(
        text(
            f"""
            WITH
            -- 365-day raw counts per ZIP from source tables — matches scoring window.
            -- ownership_raw has no zip_code column — join via parcels to resolve ZIP.
            llc_counts AS (
                SELECT p.zip_code, COUNT(*) AS cnt
                FROM ownership_raw o
                JOIN parcels p ON o.bbl = p.bbl
                WHERE o.party_type = '2'
                  AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
                  AND o.party_name_normalized LIKE '%LLC%'
                  AND o.doc_date >= CURRENT_DATE - INTERVAL '365 days'
                  AND p.zip_code IS NOT NULL
                  AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
                  AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
                  AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
                  AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
                  AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
                  AND NOT EXISTS (
                    SELECT 1 FROM ownership_raw seller
                    WHERE seller.document_id = o.document_id
                      AND seller.party_type = '1'
                      AND seller.party_name_normalized LIKE '%LLC%'
                  )
                GROUP BY p.zip_code
            ),
            eviction_counts AS (
                SELECT zip_code, COUNT(*) AS cnt
                FROM evictions_raw
                WHERE executed_date >= CURRENT_DATE - INTERVAL '365 days'
                  AND zip_code IS NOT NULL
                GROUP BY zip_code
            ),
            permit_counts AS (
                SELECT zip_code, COUNT(*) AS cnt
                FROM permits_raw
                WHERE filing_date >= CURRENT_DATE - INTERVAL '365 days'
                  AND zip_code IS NOT NULL
                GROUP BY zip_code
            ),
            complaint_counts AS (
                -- Filter to displacement-relevant types only — matches scoring/compute.py.
                -- Unfiltered 365-day scan over all complaint types took ~12s (full table).
                -- The composite index on (complaint_type, created_date) makes this fast.
                SELECT zip_code, COUNT(*) AS cnt
                FROM complaints_raw
                WHERE created_date >= CURRENT_DATE - INTERVAL '365 days'
                  AND zip_code IS NOT NULL
                  AND complaint_type = ANY(:complaint_types)
                GROUP BY zip_code
            ),
            rs_loss_counts AS (
                -- rs_unit_loss: count RS buildings with unit loss in current vs prior year.
                -- rs_buildings has no zip_code — join via parcels.
                -- This is a point-in-time loss count, not a 30-day window.
                -- Both year predicates are in WHERE (not just the JOIN) so the planner can
                -- use idx_rs_buildings_year for the prior scan and short-circuit when the
                -- prior year is absent (as it is during the first year of data collection).
                SELECT par.zip_code, COUNT(*) AS cnt
                FROM rs_buildings cur
                JOIN rs_buildings prior ON cur.bbl = prior.bbl
                    AND prior.rs_unit_count > cur.rs_unit_count
                JOIN parcels par ON par.bbl = cur.bbl
                WHERE cur.year = EXTRACT(YEAR FROM CURRENT_DATE)::int
                  AND prior.year = EXTRACT(YEAR FROM CURRENT_DATE)::int - 1
                  AND par.zip_code IS NOT NULL
                GROUP BY par.zip_code
            ),
            hpd_violation_counts AS (
                SELECT zip_code, COUNT(*) AS cnt
                FROM violations_raw
                WHERE violation_class IN ('B', 'C')
                  AND inspection_date >= CURRENT_DATE - INTERVAL '90 days'
                  AND zip_code IS NOT NULL
                GROUP BY zip_code
            ),
            -- Join all raw counts to each valid NYC ZIP, then compute PERCENT_RANK()
            -- over actual counts. Window covers all valid ZIPs (D-15 denominator).
            -- assessment_spike: fallback to signal_breakdown JSONB — assessment_history
            -- lacks zip_code and requires a BBL-to-ZIP join plus YoY logic; the signal
            -- is dormant (0.0) until the second annual DOF scraper run completes.
            ranked AS (
                SELECT
                    ds.zip_code,
                    n.name,
                    {_BOROUGH_CASE} AS borough,
                    ds.score,
                    ds.signal_breakdown,
                    COALESCE(lc.cnt, 0)  AS raw_llc,
                    COALESCE(ec.cnt, 0)  AS raw_evictions,
                    COALESCE(pc.cnt, 0)  AS raw_permits,
                    COALESCE(cc.cnt, 0)  AS raw_complaint_rate,
                    COALESCE(rl.cnt, 0)  AS raw_rs_unit_loss,
                    COALESCE(hv.cnt, 0)  AS raw_hpd,
                    -- assessment_spike: use normalized signal value from signal_breakdown
                    -- as a proxy count (dormant signal — always 0 until 2027 at earliest).
                    COALESCE((ds.signal_breakdown->>'assessment_spike')::float, 0)
                                         AS raw_assessment_spike,
                    PERCENT_RANK() OVER (ORDER BY COALESCE(lc.cnt,  0)) AS pct_llc_acquisitions,
                    PERCENT_RANK() OVER (ORDER BY COALESCE(ec.cnt,  0)) AS pct_evictions,
                    PERCENT_RANK() OVER (ORDER BY COALESCE(pc.cnt,  0)) AS pct_permits,
                    PERCENT_RANK() OVER (ORDER BY COALESCE(cc.cnt,  0)) AS pct_complaint_rate,
                    PERCENT_RANK() OVER (ORDER BY COALESCE(rl.cnt,  0)) AS pct_rs_unit_loss,
                    PERCENT_RANK() OVER (ORDER BY COALESCE(hv.cnt,  0)) AS pct_hpd_violations,
                    PERCENT_RANK() OVER (
                        ORDER BY COALESCE((ds.signal_breakdown->>'assessment_spike')::float, 0)
                    ) AS pct_assessment_spike
                FROM displacement_scores ds
                LEFT JOIN neighborhoods n       ON ds.zip_code = n.zip_code
                LEFT JOIN llc_counts lc         ON lc.zip_code = ds.zip_code
                LEFT JOIN eviction_counts ec    ON ec.zip_code = ds.zip_code
                LEFT JOIN permit_counts pc      ON pc.zip_code = ds.zip_code
                LEFT JOIN complaint_counts cc   ON cc.zip_code = ds.zip_code
                LEFT JOIN rs_loss_counts rl     ON rl.zip_code = ds.zip_code
                LEFT JOIN hpd_violation_counts hv ON hv.zip_code = ds.zip_code
                WHERE ds.score IS NOT NULL
                  AND ({_VALID_NYC_ZIP_CLAUSE})
                  AND n.name IS NOT NULL
            )
            SELECT * FROM ranked
            {outer_where}
            ORDER BY score DESC
            LIMIT :limit
            """
        ),
        params,
    ).fetchall()

    # Raw count column map: dominant signal key -> actual 30-day count column from CTE.
    # These are real event counts from source tables (Option B per RESEARCH.md).
    RAW_COUNT_COLS = {
        "llc_acquisitions": "raw_llc",
        "evictions":        "raw_evictions",
        "permits":          "raw_permits",
        "complaint_rate":   "raw_complaint_rate",
        "rs_unit_loss":     "raw_rs_unit_loss",
        "hpd_violations":   "raw_hpd",
        "assessment_spike": "raw_assessment_spike",
    }
    PCT_RANK_COLS = {
        "llc_acquisitions": "pct_llc_acquisitions",
        "evictions":        "pct_evictions",
        "permits":          "pct_permits",
        "complaint_rate":   "pct_complaint_rate",
        "rs_unit_loss":     "pct_rs_unit_loss",
        "hpd_violations":   "pct_hpd_violations",
        "assessment_spike": "pct_assessment_spike",
    }

    candidates = []
    for row in rows:
        dominant, _ = _dominant_signal(row.signal_breakdown or {})

        raw_col = RAW_COUNT_COLS.get(dominant) if dominant else None
        raw_count = int(getattr(row, raw_col, 0) or 0) if raw_col else 0

        if not raw_count:
            continue

        pct_col = PCT_RANK_COLS.get(dominant) if dominant else None
        pct_rank = float(getattr(row, pct_col, 0.0) or 0.0) if pct_col else 0.0

        raw_count_label = _SIGNAL_LABELS.get(dominant, dominant or "") if dominant else ""

        candidates.append(
            {
                "zip_code": row.zip_code,
                "name": row.name,
                "borough": row.borough,
                "score": round(float(row.score), 1),
                "dominant_signal": dominant,
                "raw_count": raw_count,
                "raw_count_label": raw_count_label,
                "percentile_tier": _percentile_tier(pct_rank),
            }
        )

    result = [dict(entry, rank=i + 1) for i, entry in enumerate(candidates[:capped])]

    _tr_elapsed = _time.monotonic() - _tr_t0
    if _PERF or _tr_elapsed > 2.0:
        logger.info(
            "[perf] /api/neighborhoods/top-risk cold query %.2fs rows=%d",
            _tr_elapsed, len(rows),
        )

    _TOP_RISK_CACHE[cache_key] = (result, _time.monotonic() + _TOP_RISK_TTL)
    return {"neighborhoods": result}


@router.get("/top-movers")
@limiter.limit("60/minute")
def get_top_movers(
    request: Request,
    response: Response,
    limit: int = 8,
    db: Session = Depends(get_db),
):
    """
    Returns neighborhoods with the largest week-over-week displacement score change.
    Pulls from score_history snapshots; limit capped at 20.
    """
    response.headers["Cache-Control"] = "public, max-age=3600"
    capped = min(max(1, limit), 20)
    cached = _TOP_MOVERS_CACHE.get(capped)
    if cached and _time.monotonic() < cached[1]:
        return {"neighborhoods": cached[0]}

    valid_zip = _VALID_NYC_ZIP_CLAUSE.replace("ds.zip_code", "l.zip_code")

    rows = db.execute(text(f"""
        WITH latest AS (
            SELECT DISTINCT ON (zip_code)
                zip_code, composite_score,
                llc_acquisition_rate, eviction_rate,
                permit_intensity, complaint_rate
            FROM score_history
            ORDER BY zip_code, scored_at DESC
        ),
        week_ago AS (
            SELECT DISTINCT ON (zip_code)
                zip_code, composite_score AS prev_score
            FROM score_history
            WHERE scored_at <= CURRENT_DATE - INTERVAL '13 days'
            ORDER BY zip_code, scored_at DESC
        ),
        llc_counts AS (
            SELECT p.zip_code, COUNT(*) AS cnt
            FROM ownership_raw o
            JOIN parcels p ON o.bbl = p.bbl
            WHERE o.party_type = '2'
              AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
              AND o.party_name_normalized LIKE '%LLC%'
              AND o.doc_date >= CURRENT_DATE - INTERVAL '30 days'
              AND p.zip_code IS NOT NULL
              AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
              AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
              AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
            GROUP BY p.zip_code
        ),
        eviction_counts AS (
            SELECT zip_code, COUNT(*) AS cnt
            FROM evictions_raw
            WHERE executed_date >= CURRENT_DATE - INTERVAL '30 days'
              AND zip_code IS NOT NULL
            GROUP BY zip_code
        ),
        permit_counts AS (
            SELECT zip_code, COUNT(*) AS cnt
            FROM permits_raw
            WHERE filing_date >= CURRENT_DATE - INTERVAL '30 days'
              AND zip_code IS NOT NULL
            GROUP BY zip_code
        ),
        complaint_counts AS (
            SELECT zip_code, COUNT(*) AS cnt
            FROM complaints_raw
            WHERE created_date >= CURRENT_DATE - INTERVAL '30 days'
              AND zip_code IS NOT NULL
            GROUP BY zip_code
        )
        SELECT
            l.zip_code,
            n.name,
            ROUND((l.composite_score - w.prev_score)::numeric, 1) AS delta,
            ROUND(l.composite_score::numeric, 1)                   AS score_now,
            ROUND(w.prev_score::numeric, 1)                        AS score_prev,
            l.llc_acquisition_rate,
            l.eviction_rate,
            l.permit_intensity,
            l.complaint_rate,
            COALESCE(lc.cnt, 0) AS raw_llc,
            COALESCE(ec.cnt, 0) AS raw_evictions,
            COALESCE(pc.cnt, 0) AS raw_permits,
            COALESCE(cc.cnt, 0) AS raw_complaints
        FROM latest l
        JOIN week_ago w ON l.zip_code = w.zip_code
        LEFT JOIN neighborhoods n      ON l.zip_code = n.zip_code
        LEFT JOIN llc_counts lc        ON lc.zip_code = l.zip_code
        LEFT JOIN eviction_counts ec   ON ec.zip_code = l.zip_code
        LEFT JOIN permit_counts pc     ON pc.zip_code = l.zip_code
        LEFT JOIN complaint_counts cc  ON cc.zip_code = l.zip_code
        WHERE l.composite_score - w.prev_score >= 1.0
          AND ({valid_zip})
        ORDER BY delta DESC
        LIMIT :limit
    """), {"limit": min(capped * 3, 60)}).fetchall()

    _RAW_COLS = {
        "llc_acquisitions": "raw_llc",
        "evictions":        "raw_evictions",
        "permits":          "raw_permits",
        "complaint_rate":   "raw_complaints",
    }

    candidates = []
    for row in rows:
        signals = {
            "llc_acquisitions": float(row.llc_acquisition_rate or 0),
            "evictions":        float(row.eviction_rate or 0),
            "permits":          float(row.permit_intensity or 0),
            "complaint_rate":   float(row.complaint_rate or 0),
        }
        dominant = max(signals, key=signals.__getitem__) if any(v > 0 for v in signals.values()) else None
        raw_col   = _RAW_COLS.get(dominant) if dominant else None
        raw_count = int(getattr(row, raw_col, 0) or 0) if raw_col else 0
        if not raw_count:
            continue
        candidates.append({
            "zip_code":              row.zip_code,
            "name":                  row.name,
            "borough":               _borough_from_zip(row.zip_code),
            "score_now":             float(row.score_now),
            "score_prev":            float(row.score_prev),
            "delta":                 float(row.delta),
            "dominant_signal":       dominant,
            "dominant_signal_label": _SIGNAL_LABELS.get(dominant, "") if dominant else "",
            "raw_count":             raw_count,
        })

    result = candidates[:capped]

    _TOP_MOVERS_CACHE[capped] = (result, _time.monotonic() + _TOP_MOVERS_TTL)
    return {"neighborhoods": result}


def _dominant_signal(breakdown: dict) -> tuple:
    """Returns (key, value) for the highest-scoring signal in the breakdown."""
    valid = {k: v for k, v in breakdown.items() if isinstance(v, (int, float))}
    if not valid:
        return None, None
    key = max(valid, key=valid.__getitem__)
    return key, valid[key]


def _percentile_tier(percent_rank: float) -> str:
    """
    Converts PERCENT_RANK() output (0.0-1.0) to a display tier label.
    percent_rank = fraction of ZIPs with a LOWER raw event count for the signal.
    So percent_rank=0.97 means 97% of ZIPs have fewer events -> top 3%.
    """
    pct = (1.0 - percent_rank) * 100.0
    if pct <= 1:   return "top 1%"
    if pct <= 3:   return "top 3%"
    if pct <= 5:   return "top 5%"
    if pct <= 10:  return "top 10%"
    if pct <= 20:  return "top 20%"
    return f"top {int(pct)}%"


# ---------------------------------------------------------------------------
# Narrative summary
# ---------------------------------------------------------------------------

_SIGNAL_LABELS = {
    "permits": "renovation permit filings",
    "evictions": "eviction filings",
    "llc_acquisitions": "LLC property acquisitions",
    "complaint_rate": "tenant complaints",
    "rs_unit_loss": "rent-stabilized unit loss",
    "hpd_violations": "HPD housing violations",
    "assessment_spike": "tax assessment increases",
}


def _fetch_raw_counts(db: Session, zip_code: str) -> dict[str, int]:
    """Raw event counts for the past 365 days — same filters as the scoring engine."""
    rows = db.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM ownership_raw o
             JOIN parcels p ON o.bbl = p.bbl
             WHERE p.zip_code = :zip AND o.party_type = '2'
               AND o.doc_type IN ('DEED','DEEDP','ASST')
               AND o.doc_date >= CURRENT_DATE - INTERVAL '365 days'
               AND p.units_res > 0
               AND o.party_name_normalized LIKE '%LLC%'
               AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
               AND o.party_name_normalized NOT ILIKE '%LENDING%'
               AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
               AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
               AND o.party_name_normalized NOT ILIKE '%LOAN FUNDER%'
               AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
               AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
               AND o.party_name_normalized NOT ILIKE '% FINANCIAL %'
               AND o.party_name_normalized NOT ILIKE '% FINANCIAL LLC'
               AND o.party_name_normalized NOT ILIKE '%REVERSE LLC'
               AND o.party_name_normalized NOT ILIKE '%GUIDANCE RESIDENTIAL%'
               AND NOT EXISTS (
                 SELECT 1 FROM ownership_raw seller
                 WHERE seller.document_id = o.document_id
                   AND seller.party_type = '1'
                   AND seller.party_name_normalized LIKE '%LLC%'
               )
            ) AS llc_acquisitions,
            (SELECT COUNT(*) FROM evictions_raw
             WHERE zip_code = :zip
               AND executed_date >= CURRENT_DATE - INTERVAL '365 days'
               AND eviction_type ILIKE 'R%'
            ) AS evictions,
            (SELECT COUNT(*) FROM permits_raw pr
             JOIN parcels p ON pr.bbl = p.bbl
             WHERE p.zip_code = :zip AND pr.permit_type = 'AL'
               AND pr.filing_date >= CURRENT_DATE - INTERVAL '365 days'
               AND p.units_res >= 3
            ) AS permits,
            (SELECT COUNT(*) FROM complaints_raw
             WHERE zip_code = :zip
               AND created_date >= CURRENT_DATE - INTERVAL '365 days'
               AND complaint_type = ANY(:ctypes)
            ) AS complaint_rate
    """), {"zip": zip_code, "ctypes": list(DISPLACEMENT_COMPLAINT_TYPES)}).fetchone()

    return {
        "llc_acquisitions": int(rows[0] or 0),
        "evictions":        int(rows[1] or 0),
        "permits":          int(rows[2] or 0),
        "complaint_rate":   int(rows[3] or 0),
    }


def _build_summary(score: float | None, breakdown: dict[str, Any], raw_counts: dict[str, int] | None = None) -> str:
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

    counts = raw_counts or {}

    # Build count phrases for the top active signals
    _count_labels = {
        "llc_acquisitions": lambda n: f"{n} LLC acquisition{'s' if n != 1 else ''}",
        "evictions":        lambda n: f"{n} residential eviction{'s' if n != 1 else ''}",
        "permits":          lambda n: f"{n} alteration permit{'s' if n != 1 else ''}",
        "complaint_rate":   lambda n: f"{n} housing complaint{'s' if n != 1 else ''}",
    }

    count_parts = []
    for key, _ in active[:3]:
        n = counts.get(key, 0)
        if n > 0 and key in _count_labels:
            count_parts.append(_count_labels[key](n))

    if tier == "Low":
        detail = "No individual signal stands out above citywide averages."
    elif not active:
        detail = "Signals are present but no single factor dominates."
    elif count_parts:
        detail = "In the past year: " + ", ".join(count_parts) + "."
    elif len(active) == 1:
        detail = f"The primary driver is {_SIGNAL_LABELS.get(active[0][0], active[0][0])}."
    else:
        top = [_SIGNAL_LABELS.get(k, k) for k, _ in active[:2]]
        detail = f"The dominant drivers are {top[0]} and {top[1]}."

    return f"{opening} {detail}"


@router.get("/names")
@limiter.limit("60/minute")
def get_neighborhood_names(request: Request, response: Response, db: Session = Depends(get_db)):
    """Returns {zip_code: name} for all neighborhoods. Lightweight lookup for the UI."""
    response.headers["Cache-Control"] = "public, max-age=3600"
    rows = db.execute(
        text("SELECT zip_code, name FROM neighborhoods WHERE zip_code != '99999' AND name IS NOT NULL")
    ).fetchall()
    return {r.zip_code: r.name for r in rows}


import time as _time
from pathlib import Path as _Path
import json as _json

_BUYERS_CACHE: dict[str, tuple[list, float]] = {}
_BUYERS_TTL = 3600
_SCRIPTS_DIR = _Path(__file__).parent.parent.parent / "scripts"


@router.get("/{zip_code}/active-buyers")
@limiter.limit("60/minute")
def get_active_buyers(request: Request, response: Response, zip_code: str, db: Session = Depends(get_db)):
    """
    Top operator clusters actively acquiring in this ZIP over the past 365 days.
    Matches raw LLC grantee names against known operator clusters.
    """
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        raise HTTPException(status_code=400, detail="Invalid ZIP code")

    cached = _BUYERS_CACHE.get(zip_code)
    if cached and _time.monotonic() < cached[1]:
        return cached[0]

    # Top LLC grantees in this ZIP (same filters as scoring engine)
    rows = db.execute(text("""
        SELECT o.party_name_normalized, COUNT(*) AS cnt
        FROM ownership_raw o
        JOIN parcels p ON o.bbl = p.bbl
        WHERE p.zip_code = :zip
          AND o.party_type = '2'
          AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
          AND o.doc_date >= CURRENT_DATE - INTERVAL '365 days'
          AND p.units_res > 0
          AND o.party_name_normalized LIKE '%LLC%'
          AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
          AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
          AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
          AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
          AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
          AND o.party_name_normalized NOT ILIKE '% FINANCIAL %'
          AND o.party_name_normalized NOT ILIKE '% FINANCIAL LLC'
          AND NOT EXISTS (
              SELECT 1 FROM ownership_raw seller
              WHERE seller.document_id = o.document_id
                AND seller.party_type = '1'
                AND seller.party_name_normalized LIKE '%LLC%'
          )
        GROUP BY o.party_name_normalized
        ORDER BY cnt DESC
        LIMIT 50
    """), {"zip": zip_code}).fetchall()

    if not rows:
        _BUYERS_CACHE[zip_code] = ([], _time.monotonic() + _BUYERS_TTL)
        return []

    # Load operator clusters and build name → root lookup
    name_to_root: dict[str, str] = {}
    analysis_path = _SCRIPTS_DIR / "operator_network_analysis.json"
    if analysis_path.exists():
        try:
            data = _json.loads(analysis_path.read_text())
            for op in data.get("operators", []):
                root = op["operator_root"]
                for entity in op.get("llc_entities", []):
                    name_to_root[entity.upper()] = root
        except Exception:
            pass
    for inv_path in sorted(_SCRIPTS_DIR.glob("*_investigation.json")):
        try:
            inv = _json.loads(inv_path.read_text())
            root = str(inv.get("subject", inv_path.stem.replace("_investigation", "").upper())).upper().strip()
            if " " not in root:
                for entity in inv.get("llc_entities", []):
                    name_to_root[entity.upper()] = root
        except Exception:
            pass

    # Tally acquisitions per known cluster in this ZIP
    cluster_counts: dict[str, int] = {}
    for row in rows:
        name = (row.party_name_normalized or "").upper()
        root = name_to_root.get(name)
        if root:
            cluster_counts[root] = cluster_counts.get(root, 0) + int(row.cnt)

    top_roots = sorted(cluster_counts.items(), key=lambda x: -x[1])[:3]

    # Resolve operator_root → slug from the operators table
    root_to_slug: dict[str, str] = {}
    if top_roots:
        slug_rows = db.execute(
            text("SELECT operator_root, slug FROM operators WHERE operator_root = ANY(:roots)"),
            {"roots": [r for r, _ in top_roots]},
        ).fetchall()
        root_to_slug = {r.operator_root: r.slug for r in slug_rows}

    result = [
        {
            "operator_root": root,
            "slug": root_to_slug.get(root),
            "acquisitions_in_zip": cnt,
        }
        for root, cnt in top_roots
    ]

    _BUYERS_CACHE[zip_code] = (result, _time.monotonic() + _BUYERS_TTL)
    return result
