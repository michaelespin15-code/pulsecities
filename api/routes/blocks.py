"""
Block (BBL) level civic events endpoint.

GET /api/blocks/{bbl}  — all civic events for a specific BBL (API-03)

Returns unified event list from all four raw tables: permits, evictions,
ACRIS ownership transfers, 311 complaints — sorted by date descending.
Capped at 50 total events. Rate-limited at 60/minute (API-04).
"""

import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from models.bbl import normalize_bbl
from models.complaints import ComplaintRaw
from models.database import get_db
from models.evictions import EvictionRaw
from models.ownership import OwnershipRaw
from models.permits import PermitRaw
from models.scores import PropertyScore

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/blocks", tags=["blocks"])
limiter = Limiter(key_func=get_remote_address)

MAX_EVENTS = 50


@router.get("/{bbl}")
@limiter.limit("60/minute")
def get_block_events(request: Request, bbl: str, db: Session = Depends(get_db)):
    """
    Returns up to 50 civic events for a BBL, sorted newest-first.
    Queries permits, evictions, ACRIS transfers, and 311 complaints.
    BBL is normalized before query to prevent format-mismatch join failures.
    """
    canonical = normalize_bbl(bbl)
    if not canonical:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid BBL format: '{bbl}'. Expected 10-digit string (e.g. 1000010001) or hyphenated (e.g. 1-00001-0001).",
        )

    events = []

    # Permits — filing_date is Date type
    permits = (
        db.query(PermitRaw)
        .filter(PermitRaw.bbl == canonical)
        .order_by(PermitRaw.filing_date.desc())
        .limit(MAX_EVENTS)
        .all()
    )
    for p in permits:
        events.append({
            "type": "permit",
            "date": p.filing_date.isoformat() if p.filing_date else None,
            "description": f"{p.permit_type or 'Permit'} — {p.work_type or 'Unknown work type'}",
            "detail": p.job_description,
            "raw_date": p.filing_date,  # for sorting — removed before response
        })

    # Evictions — executed_date is Date type
    evictions = (
        db.query(EvictionRaw)
        .filter(EvictionRaw.bbl == canonical)
        .order_by(EvictionRaw.executed_date.desc())
        .limit(MAX_EVENTS)
        .all()
    )
    for e in evictions:
        events.append({
            "type": "eviction",
            "date": e.executed_date.isoformat() if e.executed_date else None,
            "description": f"Eviction filed — {e.eviction_type or 'type unknown'}",
            "detail": e.docket_number,
            "raw_date": e.executed_date,
        })

    # ACRIS ownership transfers — doc_date is Date type; only GRANTEE rows (buyer)
    # party_type "2" is the raw Socrata code for GRANTEE stored by the scraper
    ownership = (
        db.query(OwnershipRaw)
        .filter(OwnershipRaw.bbl == canonical, OwnershipRaw.party_type == "2")
        .order_by(OwnershipRaw.doc_date.desc())
        .limit(MAX_EVENTS)
        .all()
    )
    for o in ownership:
        buyer = o.party_name_normalized or o.party_name or "Unknown buyer"
        amount = f"${o.doc_amount:,.0f}" if o.doc_amount else "amount not recorded"
        events.append({
            "type": "ownership_transfer",
            "date": o.doc_date.isoformat() if o.doc_date else None,
            "description": f"Transfer to {buyer} ({o.doc_type or 'deed'}) — {amount}",
            "detail": None,
            "raw_date": o.doc_date,
        })

    # 311 complaints — created_date is DateTime(timezone=True), call .date() for sorting
    complaints = (
        db.query(ComplaintRaw)
        .filter(ComplaintRaw.bbl == canonical)
        .order_by(ComplaintRaw.created_date.desc())
        .limit(MAX_EVENTS)
        .all()
    )
    for c in complaints:
        event_date = c.created_date.date() if c.created_date else None
        events.append({
            "type": "complaint",
            "date": event_date.isoformat() if event_date else None,
            "description": f"311: {c.complaint_type or 'complaint'} — {c.descriptor or ''}".strip(" —"),
            "detail": c.status,
            "raw_date": event_date,  # normalized to date for consistent sorting
        })

    # Sort all events newest-first; None dates sort last
    events.sort(
        key=lambda ev: ev["raw_date"] if ev["raw_date"] is not None else date.min,
        reverse=True,
    )

    # Remove internal sort key, cap at MAX_EVENTS
    for ev in events:
        del ev["raw_date"]
    events = events[:MAX_EVENTS]

    # Optional: attach PropertyScore if computed
    score_row = db.query(PropertyScore).filter(PropertyScore.bbl == canonical).first()

    return {
        "bbl": canonical,
        "events": events,
        "score": round(score_row.score, 1) if score_row and score_row.score is not None else None,
        "signal_breakdown": score_row.signal_breakdown if score_row else {},
    }
