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
import re
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

_SCRIPTS_DIR = Path(__file__).parent.parent.parent / "scripts"
_AUDIT_PATH = _SCRIPTS_DIR / "entity_resolution_audit.json"
_ANALYSIS_PATH = _SCRIPTS_DIR / "operator_network_analysis.json"
_ANALYSIS_WINDOW_DAYS = 548

_audit_cache: dict | None = None
_audit_loaded_at: float = 0.0
_AUDIT_TTL = 300  # 5 minutes


def _load_audit() -> dict:
    """
    Build clusters and by_operator indexes from the two source files:
      - operator_network_analysis.json  → clusters dict (LLC entities, stats)
      - entity_resolution_audit.json    → by_operator dict (affiliation pairs)
    """
    global _audit_cache, _audit_loaded_at
    now = time.monotonic()
    if _audit_cache is not None and now - _audit_loaded_at < _AUDIT_TTL:
        return _audit_cache

    clusters: dict = {}
    if _ANALYSIS_PATH.exists():
        try:
            analysis = json.loads(_ANALYSIS_PATH.read_text())
            for op in analysis.get("operators", []):
                root = op["operator_root"]
                clusters[root] = {
                    "llc_entities":       op.get("llc_entities", []),
                    "total_properties":   op.get("total_properties", 0),
                    "total_acquisitions": op.get("total_acquisitions", 0),
                    "zip_codes":          op.get("zip_codes_targeted", []),
                }
        except Exception as exc:
            logger.warning("Could not read operator_network_analysis.json: %s", exc)

    # Supplement with individual investigation JSONs for operators not in the analysis file
    for inv_path in sorted(_SCRIPTS_DIR.glob("*_investigation.json")):
        try:
            inv = json.loads(inv_path.read_text())
            root = str(inv.get("subject", inv_path.stem.replace("_investigation", "").upper())).upper()
            if root not in clusters:
                geo = inv.get("geographic_concentration", {})
                zips = list(geo.keys()) if isinstance(geo, dict) else []
                clusters[root] = {
                    "llc_entities":       inv.get("llc_entities", []),
                    "total_properties":   inv.get("total_properties", 0),
                    "total_acquisitions": inv.get("total_acquisitions", 0),
                    "zip_codes":          zips,
                }
        except Exception as exc:
            logger.warning("Could not read %s: %s", inv_path.name, exc)

    by_operator: dict = {}
    if _AUDIT_PATH.exists():
        try:
            audit = json.loads(_AUDIT_PATH.read_text())
            for finding in audit.get("findings", []):
                a = finding["cluster_a"]
                b = finding["cluster_b"]
                action = finding.get("recommended_action", "flag_for_review")
                rel_type = "affiliated" if action == "merge_candidate" else "related"
                conf = finding.get("combined_confidence", 0.0)

                # Enrich shared_bbl signals with parsed BBL list
                enriched: list = []
                for sig in finding.get("signals", []):
                    s = dict(sig)
                    if s.get("signal_type") == "shared_bbl":
                        s["shared_bbls"] = re.findall(r"'(\d+)'", s.get("detail", ""))
                    enriched.append(s)

                for root, related_root in [(a, b), (b, a)]:
                    by_operator.setdefault(root, []).append({
                        "related_root":        related_root,
                        "relationship_type":   rel_type,
                        "combined_confidence": conf,
                        "signals":             enriched,
                    })
        except Exception as exc:
            logger.warning("Could not read entity_resolution_audit.json: %s", exc)

    result: dict = {"clusters": clusters, "by_operator": by_operator}
    _audit_cache = result
    _audit_loaded_at = now
    return result


@router.get("/top")
@limiter.limit("60/minute")
def get_top_operators(request: Request, response: Response, limit: int = 3):
    """Top operators by total acquisition count. Used by the landing page."""
    clusters = _load_audit()["clusters"]
    top = sorted(
        [{"operator_root": r, **c} for r, c in clusters.items()],
        key=lambda x: x.get("total_acquisitions", 0),
        reverse=True,
    )[:max(1, min(limit, 10))]
    return [
        {
            "operator_root": op["operator_root"],
            "total_acquisitions": op.get("total_acquisitions", 0),
            "llc_count": len(op.get("llc_entities") or []),
        }
        for op in top
    ]


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
