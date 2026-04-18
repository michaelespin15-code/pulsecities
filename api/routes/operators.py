"""
Operator profile endpoint.

GET /api/operators/{root}
  Full profile for an operator cluster identified by its root token (e.g. "MTEK").
  Returns LLC entities, acquisition stats, recent properties, and related operator
  affiliations detected by the entity resolution audit.

  Related operators are never merged into this cluster's counts — they are returned
  as separate data for the reader to evaluate. Every operator stays as the public
  records show them.

  Relationship data is sourced from scripts/entity_resolution_audit.json, which must
  be generated before this endpoint returns affiliation data. The JSON is cached in
  memory for 5 minutes to avoid per-request disk reads.

Rate-limited to 30/minute per IP.
"""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/operators", tags=["operators"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

_AUDIT_PATH = Path(__file__).parent.parent.parent / "scripts" / "entity_resolution_audit.json"
_ANALYSIS_WINDOW_DAYS = 548

_audit_cache: dict | None = None
_audit_loaded_at: float = 0.0
_AUDIT_TTL = 300  # 5 minutes


def _load_audit() -> dict:
    global _audit_cache, _audit_loaded_at
    now = time.monotonic()
    if _audit_cache is not None and now - _audit_loaded_at < _AUDIT_TTL:
        return _audit_cache
    empty: dict = {"clusters": {}, "by_operator": {}}
    if not _AUDIT_PATH.exists():
        _audit_cache = empty
        _audit_loaded_at = now
        return empty
    try:
        data = json.loads(_AUDIT_PATH.read_text())
        _audit_cache = data
        _audit_loaded_at = now
        return data
    except Exception as exc:
        logger.warning("Could not read entity_resolution_audit.json: %s", exc)
        _audit_cache = empty
        _audit_loaded_at = now
        return empty


@router.get("/{root}")
@limiter.limit("30/minute")
def get_operator_profile(
    request: Request,
    response: Response,
    root: str,
    db: Session = Depends(get_db),
):
    root = root.upper().strip()
    if len(root) < 2:
        raise HTTPException(status_code=400, detail="Operator root too short")

    audit = _load_audit()
    clusters = audit.get("clusters", {})
    by_operator = audit.get("by_operator", {})

    cluster = clusters.get(root)
    if cluster is None:
        raise HTTPException(status_code=404, detail="Operator not found")

    llc_names = cluster["llc_entities"]

    # --- Recent acquisitions ---
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_ANALYSIS_WINDOW_DAYS)).date()
    acq_rows = db.execute(
        text("""
            SELECT
                o.bbl,
                p.address,
                p.zip_code,
                o.party_name_normalized AS buyer_name,
                o.doc_date,
                o.doc_amount,
                o.party_addr_1,
                o.party_city,
                o.party_state,
                o.party_zip
            FROM ownership_raw o
            JOIN parcels p ON p.bbl = o.bbl
            WHERE o.party_type = '2'
              AND o.party_name_normalized = ANY(:names)
              AND o.doc_date >= :cutoff
            ORDER BY o.doc_date DESC NULLS LAST
            LIMIT 50
        """),
        {"names": llc_names, "cutoff": cutoff},
    ).fetchall()

    acquisitions = [
        {
            "bbl": r.bbl,
            "address": r.address or f"BBL {r.bbl}",
            "zip_code": r.zip_code,
            "buyer_name": r.buyer_name,
            "doc_date": r.doc_date.isoformat() if r.doc_date else None,
            "doc_amount": float(r.doc_amount) if r.doc_amount else None,
            "party_addr_1": r.party_addr_1,
            "party_city": r.party_city,
            "party_state": r.party_state,
            "party_zip": r.party_zip,
        }
        for r in acq_rows
    ]

    # --- Related operators with shared property addresses ---
    related = []
    for rel in by_operator.get(root, []):
        other_root = rel["related_root"]

        # Resolve shared BBL addresses from parcels
        shared_bbls = []
        for sig in rel.get("signals", []):
            if sig.get("signal_type") == "shared_bbl":
                shared_bbls = sig.get("shared_bbls", [])
                break

        shared_properties = []
        if shared_bbls:
            parcel_rows = db.execute(
                text("SELECT bbl, address, zip_code FROM parcels WHERE bbl = ANY(:bbls)"),
                {"bbls": shared_bbls},
            ).fetchall()
            shared_properties = [
                {"bbl": r.bbl, "address": r.address or f"BBL {r.bbl}", "zip_code": r.zip_code}
                for r in parcel_rows
            ]

        related.append({
            "operator_root":        other_root,
            "relationship_type":    rel["relationship_type"],
            "combined_confidence":  rel["combined_confidence"],
            "signals":              rel["signals"],
            "shared_properties":    shared_properties,
            "llc_entities":         clusters.get(other_root, {}).get("llc_entities", []),
            "total_properties":     clusters.get(other_root, {}).get("total_properties", 0),
        })

    return {
        "operator_root":      root,
        "llc_entities":       llc_names,
        "total_properties":   cluster["total_properties"],
        "total_acquisitions": cluster["total_acquisitions"],
        "zip_codes":          cluster.get("zip_codes", []),
        "recent_acquisitions": acquisitions,
        "related_operators":  related,
    }
