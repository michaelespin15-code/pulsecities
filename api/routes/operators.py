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

# Finance/lender operator roots hidden from all public surfaces.
# These clusters transact exclusively via ASST (mortgage note assignments),
# not DEED transfers — they are not property operators in the displacement
# sense. Appearing in "Recent findings" or the directory misleads journalists.
# Imported by api/routes/frontend.py to enforce the same list everywhere.
OPERATOR_NOISE_ROOTS: frozenset[str] = frozenset({
    "ICECAP", "ICE", "BROAD", "BROADVIEW",
    "ARBOR", "STANDARD", "SYMETRA", "COMMUNITY", "OCEANVIEW",
})

# Slug equivalents (lowercase), used to block direct /operator/{slug} URLs.
OPERATOR_NOISE_SLUGS: frozenset[str] = frozenset({
    "icecap", "ice", "broad", "broadview",
    "arbor", "standard", "symetra", "community", "oceanview",
})

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


@router.get("/")
@limiter.limit("60/minute")
def list_operators(request: Request, response: Response, db: Session = Depends(get_db)):
    """All operator clusters ordered by portfolio size. Served from cached DB columns."""
    rows = db.execute(
        text("""
            SELECT
                operator_root,
                slug,
                display_name,
                total_properties,
                total_acquisitions,
                borough_spread,
                highest_displacement_score,
                jsonb_array_length(llc_entities) AS llc_count
            FROM operators
            ORDER BY total_properties DESC
        """)
    ).fetchall()
    return [
        {
            "operator_root": r.operator_root,
            "slug": r.slug,
            "display_name": r.display_name,
            "portfolio_size": r.total_properties,
            "total_acquisitions": r.total_acquisitions,
            "borough_spread": r.borough_spread,
            "highest_displacement_score": (
                float(r.highest_displacement_score)
                if r.highest_displacement_score is not None
                else None
            ),
            "llc_count": r.llc_count,
        }
        for r in rows
    ]


@router.get("/top")
@limiter.limit("60/minute")
def get_top_operators(
    request: Request,
    response: Response,
    limit: int = 3,
    db: Session = Depends(get_db),
):
    """Top operators by total acquisition count, filtered to confirmed operators.

    Excludes finance/lender noise clusters (OPERATOR_NOISE_ROOTS) and any
    cluster without a valid DB row — so every returned entry resolves to a
    working /operator/{slug} profile page.
    """
    clusters = _load_audit()["clusters"]

    # Only surface operators that have a DB entry (valid profile page).
    db_roots: set[str] = {
        r.operator_root
        for r in db.execute(text("SELECT operator_root FROM operators")).fetchall()
    }

    top = sorted(
        [
            {"operator_root": r, **c}
            for r, c in clusters.items()
            if r not in OPERATOR_NOISE_ROOTS
            and r in db_roots
            and len(c.get("llc_entities") or []) > 0
        ],
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


@router.get("/{slug}")
@limiter.limit("30/minute")
def get_operator_profile_by_slug(
    request: Request,
    response: Response,
    slug: str,
    db: Session = Depends(get_db),
):
    """Full operator profile by slug. Returns LLC entities, portfolio, and four per-BBL signals."""
    if not re.match(r"^[a-z0-9-]+$", slug):
        raise HTTPException(status_code=400, detail="Invalid slug format")

    # --- Operator row lookup ---
    op_row = db.execute(
        text("SELECT * FROM operators WHERE slug = :slug"),
        {"slug": slug},
    ).fetchone()
    if op_row is None:
        raise HTTPException(status_code=404, detail="Operator not found")

    operator_id = op_row.id
    operator_root = op_row.operator_root
    llc_names = op_row.llc_entities or []

    # --- BBL list for this operator ---
    bbl_rows = db.execute(
        text("SELECT bbl FROM operator_parcels WHERE operator_id = :operator_id"),
        {"operator_id": operator_id},
    ).fetchall()
    bbl_list = [r.bbl for r in bbl_rows]

    # --- Signal 1: Properties with per-BBL displacement score ---
    prop_rows = db.execute(
        text("""
            SELECT op.bbl, p.zip_code, ds.score AS displacement_score
            FROM operator_parcels op
            JOIN parcels p ON p.bbl = op.bbl
            LEFT JOIN displacement_scores ds ON ds.zip_code = p.zip_code
            WHERE op.operator_id = :operator_id
            ORDER BY ds.score DESC NULLS LAST
        """),
        {"operator_id": operator_id},
    ).fetchall()
    properties = [
        {
            "bbl": r.bbl,
            "zip_code": r.zip_code,
            "displacement_score": float(r.displacement_score) if r.displacement_score is not None else None,
        }
        for r in prop_rows
    ]

    # --- Signal 2: HPD violations keyed by BBL → violation_class → count ---
    hpd_violations: dict = {}
    if bbl_list:
        viol_rows = db.execute(
            text("""
                SELECT bbl, violation_class, COUNT(*) AS count
                FROM violations_raw
                WHERE bbl = ANY(:bbl_list) AND violation_class IS NOT NULL
                GROUP BY bbl, violation_class
                ORDER BY bbl, violation_class
            """),
            {"bbl_list": bbl_list},
        ).fetchall()
        for r in viol_rows:
            hpd_violations.setdefault(r.bbl, {})[r.violation_class] = r.count

    # --- Signal 3: Eviction-then-buy matches ---
    eviction_then_buy = []
    if bbl_list and llc_names:
        etb_rows = db.execute(
            text("""
                SELECT DISTINCT ON (e.bbl, e.executed_date)
                    e.bbl, e.executed_date AS eviction_date, e.eviction_type,
                    o.doc_date AS acquisition_date, o.party_name_normalized AS acquiring_entity
                FROM evictions_raw e
                JOIN ownership_raw o
                    ON o.bbl = e.bbl
                    AND o.party_type = '2'
                    AND o.party_name_normalized = ANY(:llc_names)
                    AND o.doc_date > e.executed_date
                    AND o.doc_date <= e.executed_date + INTERVAL '365 days'
                ORDER BY e.bbl, e.executed_date, o.doc_date
            """),
            {"llc_names": llc_names},
        ).fetchall()
        eviction_then_buy = [
            {
                "bbl": r.bbl,
                "eviction_date": r.eviction_date.isoformat() if r.eviction_date else None,
                "eviction_type": r.eviction_type,
                "acquisition_date": r.acquisition_date.isoformat() if r.acquisition_date else None,
                "acquiring_entity": r.acquiring_entity,
            }
            for r in etb_rows
        ]

    # --- Signal 4: RS unit counts (most recent year per BBL) ---
    rs_units = []
    if bbl_list:
        rs_rows = db.execute(
            text("""
                SELECT DISTINCT ON (bbl) bbl, year, rs_unit_count
                FROM rs_buildings
                WHERE bbl = ANY(:bbl_list) AND rs_unit_count IS NOT NULL
                ORDER BY bbl, year DESC
            """),
            {"bbl_list": bbl_list},
        ).fetchall()
        rs_units = [
            {"bbl": r.bbl, "year": r.year, "rs_unit_count": r.rs_unit_count}
            for r in rs_rows
        ]

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

    # --- Acquisition timeline grouped by year-month ---
    from collections import Counter
    ym_counts: Counter = Counter()
    for acq in acquisitions:
        if acq["doc_date"]:
            ym_counts[acq["doc_date"][:7]] += 1
    acquisition_timeline = [
        {"year_month": ym, "count": count}
        for ym, count in sorted(ym_counts.items())
    ]

    # --- Related operators (still sourced from _load_audit() JSON) ---
    audit = _load_audit()
    clusters = audit.get("clusters", {})
    by_operator = audit.get("by_operator", {})

    related = []
    for rel in by_operator.get(operator_root, []):
        other_root = rel["related_root"]

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
            "operator_root":       other_root,
            "relationship_type":   rel["relationship_type"],
            "combined_confidence": rel["combined_confidence"],
            "signals":             rel["signals"],
            "shared_properties":   shared_properties,
            "llc_entities":        clusters.get(other_root, {}).get("llc_entities", []),
            "total_properties":    clusters.get(other_root, {}).get("total_properties", 0),
        })

    return {
        "operator_root":        operator_root,
        "slug":                 op_row.slug,
        "display_name":         op_row.display_name,
        "llc_entities":         llc_names,
        "total_properties":     op_row.total_properties,
        "total_acquisitions":   op_row.total_acquisitions,
        "borough_spread":       op_row.borough_spread,
        "highest_displacement_score": (
            float(op_row.highest_displacement_score)
            if op_row.highest_displacement_score is not None
            else None
        ),
        "properties":           properties,
        "hpd_violations":       hpd_violations,
        "eviction_then_buy":    eviction_then_buy,
        "rs_units":             rs_units,
        "recent_acquisitions":  acquisitions,
        "acquisition_timeline": acquisition_timeline,
        "related_operators":    related,
    }
