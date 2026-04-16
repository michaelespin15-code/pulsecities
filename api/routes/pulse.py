"""
Neighborhood Pulse API endpoint.

GET /api/neighborhoods/{zip_code}/pulse
  Returns LLC acquisitions, renovation permits, and recent evictions from the
  last 90 days for the specified ZIP code, with specific street addresses and dates.

Response shape:
  {
    "zip_code": "11221",
    "llc_acquisitions": [
      {"bbl": "...", "address": "...", "buyer_name": "...", "doc_date": "YYYY-MM-DD", "doc_amount": float|null},
      ...
    ],
    "recent_permits": [
      {"bbl": "...", "address": "...", "permit_type": "A1", "permit_type_label": "Major Renovation", "filing_date": "YYYY-MM-DD"},
      ...
    ],
    "recent_evictions": [
      {"address": "...", "eviction_type": "...", "executed_date": "YYYY-MM-DD"},
      ...
    ]
  }

Security:
  - zip_code validated as 5-digit numeric before any DB query (T-06-03-01)
  - All API strings serialized to JSON (not injected into DOM via innerHTML) (T-06-03-02, T-06-03-03)
  - Rate limited to 60/minute (T-06-03-04)
  - LIMIT 25 on all SQL queries caps response size (T-06-03-04)
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/neighborhoods", tags=["pulse"])
limiter = Limiter(key_func=get_remote_address)

PERMIT_TYPE_LABELS = {
    "A1": "Major Renovation",
    "A2": "Alteration",
    "NB": "New Building",
}


@router.get("/{zip_code}/pulse")
@limiter.limit("60/minute")
def get_neighborhood_pulse(
    request: Request,
    zip_code: str,
    db: Session = Depends(get_db),
):
    """
    Returns LLC acquisitions and recent permits (last 90 days) for a ZIP code.

    Response is best-effort: empty lists are returned for unknown ZIPs (not 404).
    400 is returned for invalid zip format.

    LLC acquisitions: doc_type IN ('DEED','DEEDP','ASST'), party_type='GRANTEE',
    party_name_normalized LIKE '%LLC%', doc_date >= CURRENT_DATE - 90 days.
    Joined to parcels to resolve street address from BBL.

    Recent permits: permit_type IN ('A1','A2','NB'), zip_code = :zip,
    filing_date >= CURRENT_DATE - 90 days.
    """
    # --- Input validation (T-06-03-01) ---
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        raise HTTPException(status_code=400, detail="zip_code must be 5 digits")

    # --- LLC Acquisitions query ---
    # Joins ownership_raw to parcels on BBL to get street address.
    # party_type = '2' is the ACRIS numeric code for GRANTEE (buyer).
    # doc_type IN ('DEED','DEEDP','ASST') covers direct deeds and assignments.
    # DISTINCT ON (o.bbl) keeps the most recent acquisition per building — a BBL
    # that had two transfers in 90 days (rare but possible) only surfaces once.
    llc_rows = db.execute(
        text("""
            SELECT * FROM (
                SELECT DISTINCT ON (o.bbl)
                    o.bbl,
                    p.address,
                    o.party_name_normalized AS buyer_name,
                    o.doc_date,
                    o.doc_amount
                FROM ownership_raw o
                LEFT JOIN parcels p ON p.bbl = o.bbl
                WHERE p.zip_code = :zip
                  AND o.party_type = '2'
                  AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
                  AND o.party_name_normalized LIKE '%LLC%'
                  AND o.doc_date >= CURRENT_DATE - INTERVAL '90 days'
                  AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
                  AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
                  AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
                  AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
                  AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
                ORDER BY o.bbl, o.doc_date DESC
            ) deduped
            ORDER BY doc_date DESC
            LIMIT 25
        """),
        {"zip": zip_code},
    ).fetchall()

    # --- Recent Permits query ---
    # permits_raw has zip_code column directly — no join needed.
    # permit_type IN ('A1','A2','NB') covers major renovation, alteration, new building.
    # DISTINCT ON (bbl) keeps only the most recent permit per building — a building that
    # filed three alterations in 90 days (common for staged renos) only surfaces once,
    # preventing the same address from repeating in the activity feed.
    # The A1/A2/NB job classification is in raw_data->>'job_type'.
    permit_rows = db.execute(
        text("""
            SELECT * FROM (
                SELECT DISTINCT ON (bbl)
                    bbl,
                    address,
                    raw_data->>'job_type' AS permit_type,
                    filing_date
                FROM permits_raw
                WHERE zip_code = :zip
                  AND raw_data->>'job_type' IN ('A1', 'A2', 'NB')
                  AND filing_date >= CURRENT_DATE - INTERVAL '90 days'
                ORDER BY bbl, filing_date DESC
            ) deduped
            ORDER BY filing_date DESC
            LIMIT 25
        """),
        {"zip": zip_code},
    ).fetchall()

    # --- Recent Evictions query ---
    # evictions_raw has zip_code and executed_date columns directly.
    # DISTINCT ON (address, executed_date) deduplicates same-address same-day rows.
    eviction_rows = db.execute(
        text("""
            SELECT * FROM (
                SELECT DISTINCT ON (address, executed_date)
                    address,
                    eviction_type,
                    executed_date
                FROM evictions_raw
                WHERE zip_code = :zip
                  AND executed_date >= CURRENT_DATE - INTERVAL '90 days'
                ORDER BY address, executed_date DESC
            ) deduped
            ORDER BY executed_date DESC
            LIMIT 25
        """),
        {"zip": zip_code},
    ).fetchall()

    return {
        "zip_code": zip_code,
        "llc_acquisitions": [
            {
                "bbl": row.bbl,
                "address": row.address or f"BBL {row.bbl}",
                "buyer_name": row.buyer_name,
                "doc_date": row.doc_date.isoformat() if row.doc_date else None,
                "doc_amount": float(row.doc_amount) if row.doc_amount else None,
            }
            for row in llc_rows
        ],
        "recent_permits": [
            {
                "bbl": row.bbl,
                "address": row.address or f"BBL {row.bbl}",
                "permit_type": row.permit_type,
                "permit_type_label": PERMIT_TYPE_LABELS.get(row.permit_type, row.permit_type),
                "filing_date": row.filing_date.isoformat() if row.filing_date else None,
            }
            for row in permit_rows
        ],
        "recent_evictions": [
            {
                "address": row.address or "Unknown address",
                "eviction_type": row.eviction_type,
                "executed_date": row.executed_date.isoformat() if row.executed_date else None,
            }
            for row in eviction_rows
        ],
    }


@router.get("/{zip_code}/renovation-flip")
@limiter.limit("60/minute")
def get_renovation_flip(
    request: Request,
    zip_code: str,
    db: Session = Depends(get_db),
):
    """
    Detects the renovation-flip pattern: LLC deed transfer followed by an A1/A2
    renovation permit on the same BBL within 60 days, scoped to the last 180 days.

    Returns detected=true only when 2+ BBLs in the ZIP match the pattern.
    All matched BBLs are listed in properties regardless of count.

    Security:
      - zip_code validated as 5-digit numeric (T-06-04-01)
      - Rate limited to 60/minute (T-06-04-03)
      - Query scoped to single ZIP and 180-day window — no full-table scans (T-06-04-03)
    """
    # --- Input validation (T-06-04-01) ---
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        raise HTTPException(status_code=400, detail="zip_code must be 5 digits")

    # --- Renovation-flip CTE detection query ---
    # Detects: LLC deed transfer (ownership_raw) + A1/A2 permit (permits_raw)
    # on same BBL within 60 days, permit AFTER transfer, last 180 days.
    # Joins ownership_raw -> parcels to resolve ZIP (ownership_raw has no zip_code column).
    rows = db.execute(
        text("""
            WITH llc_transfers AS (
                SELECT p.zip_code, o.bbl, o.doc_date AS transfer_date,
                       o.party_name_normalized AS buyer, o.doc_amount,
                       p.address
                FROM ownership_raw o
                JOIN parcels p ON p.bbl = o.bbl
                WHERE o.party_name_normalized LIKE '%LLC%'
                  AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
                  AND o.party_type = '2'
                  AND o.doc_date >= CURRENT_DATE - INTERVAL '180 days'
                  AND p.zip_code = :zip_code
            ),
            reno_permits AS (
                SELECT bbl, MIN(filing_date) AS first_permit_date
                FROM permits_raw
                WHERE raw_data->>'job_type' IN ('A1', 'A2')
                  AND filing_date >= CURRENT_DATE - INTERVAL '180 days'
                  AND zip_code = :zip_code
                GROUP BY bbl
            ),
            combined AS (
                SELECT l.zip_code, l.bbl, l.address, l.buyer, l.doc_amount,
                       l.transfer_date, r.first_permit_date,
                       (r.first_permit_date - l.transfer_date) AS days_between
                FROM llc_transfers l
                JOIN reno_permits r ON r.bbl = l.bbl
                WHERE r.first_permit_date > l.transfer_date
                  AND (r.first_permit_date - l.transfer_date) <= 60
            )
            SELECT * FROM combined ORDER BY transfer_date DESC
        """),
        {"zip_code": zip_code},
    ).fetchall()

    properties = [
        {
            "bbl": row.bbl,
            "address": row.address or f"BBL {row.bbl}",
            "buyer": row.buyer,
            "transfer_date": row.transfer_date.isoformat() if row.transfer_date else None,
            "permit_date": row.first_permit_date.isoformat() if row.first_permit_date else None,
            "days_between": int(row.days_between.days) if row.days_between else None,
        }
        for row in rows
    ]
    count = len(properties)
    return {
        "zip_code": zip_code,
        "detected": count >= 2,
        "count": count,
        "properties": properties,
    }
