"""
Search endpoints.

GET /api/search/?q={query}
  Grouped search: neighborhoods by ZIP/name, buildings by address, operators by name.
  Returns three groups in a stable shape; each result carries a type and href.
  Minimum 3 characters. Rate-limited to 30/minute.

GET /api/search/landlord?q={query}
  Full-portfolio view of all deed records matching a landlord or LLC name substring.
  Designed for investigative journalism use: type an LLC name, see every property.

Response shape:
  {
    "query": "MTEK",
    "summary": {
      "total_properties": 12,
      "unique_zip_codes": 3,
      "total_llc_names": 2,
      "estimated_total_value": 4200000.0
    },
    "results": [
      {
        "bbl": "3012340001",
        "address": "796 Sterling Place",
        "zip_code": "11216",
        "buyer_name": "MTEK HOLDINGS LLC",
        "doc_date": "2023-08-15",
        "doc_amount": 850000.0,
        "latitude": 40.6712,
        "longitude": -73.9542
      }
    ]
  }

Security:
  - q must be >= 3 characters; returns 400 "Query too short" otherwise
  - Rate limited to 30/minute by client IP
  - LIMIT 50 on result rows caps response size
  - Mortgage servicers excluded using the same patterns as compute.py
  - All user input passed as parameterized SQL bind variables — no interpolation
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.nyc import ACRIS_TRANSFER_DOC_TYPES
from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/search", tags=["search"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)


@router.get("/")
@limiter.limit("30/minute")
def search_grouped(
    request: Request,
    response: Response,
    q: str = "",
    db: Session = Depends(get_db),
):
    """
    Grouped search across three buckets:
      - neighborhoods: exact ZIP match or neighborhood name ILIKE
      - properties:    parcel address ILIKE (not ownership/LLC names)
      - operators:     operator display_name or root ILIKE

    Minimum 3 characters. Each result carries a `type` and `href` the
    frontend can use directly. Returns HTTP 400 for queries shorter than 3
    characters; returns empty groups (HTTP 200) when nothing matches.
    """
    q = q.strip()
    if len(q) < 3:
        raise HTTPException(status_code=400, detail="Query too short")

    pattern = f"%{q}%"
    is_zip = q.isdigit() and len(q) == 5

    # ── Neighborhoods ──────────────────────────────────────────────────────────
    if is_zip:
        nbhd_rows = db.execute(
            text("""
                SELECT n.zip_code, n.name, n.borough, ds.score
                FROM neighborhoods n
                LEFT JOIN displacement_scores ds ON ds.zip_code = n.zip_code
                WHERE n.zip_code = :zip
                LIMIT 5
            """),
            {"zip": q},
        ).fetchall()
    else:
        nbhd_rows = db.execute(
            text("""
                SELECT n.zip_code, n.name, n.borough, ds.score
                FROM neighborhoods n
                LEFT JOIN displacement_scores ds ON ds.zip_code = n.zip_code
                WHERE n.name ILIKE :pattern
                ORDER BY ds.score DESC NULLS LAST
                LIMIT 5
            """),
            {"pattern": pattern},
        ).fetchall()

    neighborhoods = [
        {
            "type": "neighborhood",
            "zip": r.zip_code,
            "name": r.name,
            "borough": r.borough or None,
            "score": round(float(r.score), 1) if r.score is not None else None,
            "href": f"/neighborhood/{r.zip_code}",
        }
        for r in nbhd_rows
    ]

    # ── Properties (parcel address search) ────────────────────────────────────
    # Skipped for exact ZIP queries — the neighborhood result is the right match.
    # Operator/LLC name searches go to /api/search/landlord, not here.
    if is_zip:
        properties = []
    else:
        prop_rows = db.execute(
            text("""
                SELECT bbl, address, zip_code
                FROM parcels
                WHERE address ILIKE :pattern
                  AND address IS NOT NULL
                ORDER BY zip_code, address
                LIMIT 10
            """),
            {"pattern": pattern},
        ).fetchall()
        properties = [
            {
                "type": "property",
                "bbl": r.bbl,
                "address": r.address.title() if r.address else f"BBL {r.bbl}",
                "zip": r.zip_code,
                "href": f"/property/{r.bbl}",
            }
            for r in prop_rows
        ]

    # ── Operators ───────────────────────────────────────────────────────────────
    op_rows = db.execute(
        text("""
            SELECT operator_root, slug, display_name, total_properties
            FROM operators
            WHERE display_name ILIKE :pattern
               OR operator_root ILIKE :pattern
            ORDER BY total_properties DESC NULLS LAST
            LIMIT 10
        """),
        {"pattern": pattern},
    ).fetchall()

    operators = [
        {
            "type": "operator",
            "name": r.display_name,
            "slug": r.slug,
            "portfolio_size": r.total_properties,
            "href": f"/operator/{r.slug}",
        }
        for r in op_rows
    ]

    return {
        "query": q,
        "groups": {
            "neighborhoods": neighborhoods,
            "properties": properties,
            "operators": operators,
        },
    }


@router.get("/landlord")
@limiter.limit("30/minute")
def search_landlord(
    request: Request,
    response: Response,
    q: str = "",
    db: Session = Depends(get_db),
):
    """
    Returns all deed records matching a landlord or LLC name substring.

    Results capped at 50, ordered by doc_date DESC so the most recent
    acquisitions appear first. Summary aggregates cover all matching records,
    not just the top 50. lat/lng is pulled from the parcel point geometry;
    falls back to the neighborhood polygon centroid when the parcel has no
    geometry loaded yet.

    Mortgage servicers (Nationstar, Rocket, etc.) are excluded using the same
    ILIKE patterns as compute.py to avoid inflating portfolio counts with
    foreclosure acquisitions.
    """
    q = q.strip()
    if len(q) < 3:
        raise HTTPException(status_code=400, detail="Query too short")

    pattern = f"%{q}%"

    # --- Summary --- aggregates over ALL matching records, not capped at 50
    summary_row = db.execute(
        text("""
            SELECT
                COUNT(*) AS total_properties,
                COUNT(DISTINCT p.zip_code) AS unique_zip_codes,
                COUNT(DISTINCT o.party_name_normalized) AS total_llc_names,
                SUM(o.doc_amount) FILTER (WHERE o.doc_amount IS NOT NULL)
                    AS estimated_total_value
            FROM ownership_raw o
            JOIN parcels p ON p.bbl = o.bbl
            WHERE o.party_type = '2'
              AND o.doc_type = ANY(:doc_types)
              AND o.party_name_normalized ILIKE :pattern
              AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
              AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
              AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
        """),
        {"pattern": pattern, "doc_types": list(ACRIS_TRANSFER_DOC_TYPES)},
    ).fetchone()

    # --- Results --- LIMIT 50, most recent deed first
    # lat/lng: parcel point geometry preferred; neighborhood polygon centroid as fallback.
    result_rows = db.execute(
        text("""
            SELECT
                o.bbl,
                p.address,
                p.zip_code,
                o.party_name_normalized AS buyer_name,
                o.doc_date,
                o.doc_amount,
                COALESCE(
                    ST_Y(p.geometry::geometry),
                    ST_Y(ST_Centroid(n.geometry::geometry))
                ) AS latitude,
                COALESCE(
                    ST_X(p.geometry::geometry),
                    ST_X(ST_Centroid(n.geometry::geometry))
                ) AS longitude
            FROM ownership_raw o
            JOIN parcels p ON p.bbl = o.bbl
            LEFT JOIN neighborhoods n ON n.zip_code = p.zip_code
            WHERE o.party_type = '2'
              AND o.doc_type = ANY(:doc_types)
              AND o.party_name_normalized ILIKE :pattern
              AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
              AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
              AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
            ORDER BY o.doc_date DESC NULLS LAST
            LIMIT 50
        """),
        {"pattern": pattern, "doc_types": list(ACRIS_TRANSFER_DOC_TYPES)},
    ).fetchall()

    results = [
        {
            "bbl": row.bbl,
            "address": row.address or f"BBL {row.bbl}",
            "zip_code": row.zip_code,
            "buyer_name": row.buyer_name,
            "doc_date": row.doc_date.isoformat() if row.doc_date else None,
            "doc_amount": float(row.doc_amount) if row.doc_amount else None,
            "latitude": float(row.latitude) if row.latitude is not None else None,
            "longitude": float(row.longitude) if row.longitude is not None else None,
        }
        for row in result_rows
    ]

    return {
        "query": q,
        "summary": {
            "total_properties": int(summary_row.total_properties or 0),
            "unique_zip_codes": int(summary_row.unique_zip_codes or 0),
            "total_llc_names": int(summary_row.total_llc_names or 0),
            "estimated_total_value": (
                float(summary_row.estimated_total_value)
                if summary_row.estimated_total_value
                else None
            ),
        },
        "results": results,
    }
