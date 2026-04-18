"""
Entity resolution audit — cross-cluster affiliate detection across all
operator clusters identified in the 18-month ACRIS window.

Signals checked per cluster pair:
  1. name_embedding    entity in cluster A contains cluster B's root as a word token
  2. root_substring    root A is a strict prefix of root B (normalization artifact)
  3. shared_bbl        same BBL acquired by LLCs from both clusters
  4. zip_jaccard       geographic footprint overlap (Jaccard ≥ 0.60)

Confidence is combined as 1 - prod(1 - p_i) across independent signals.

Thresholds:
  ≥ 0.85  → merge_candidate
  0.65–   → flag_for_manual_review
  < 0.65  → omitted from output

No auto-merges. Produces candidates for manual review only.

Addresses and deed signatories are not stored in ownership_raw
(the ACRIS parties scraper captures only party_name, not addr_1/addr_2),
so address-based resolution requires a separate enrichment pass.

Usage:
    python scripts/entity_resolution_audit.py
"""

import json
import logging
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from itertools import combinations
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db

logger = logging.getLogger(__name__)

OUTPUT_PATH = Path(__file__).parent / "entity_resolution_audit.json"

ANALYSIS_WINDOW_DAYS = 548  # 18 months — matches operator_network_analysis.py

_GENERIC = {
    "THE", "NEW", "ONE", "TWO", "NY", "NYC", "REAL", "URBAN",
    "CITY", "OLD", "EAST", "WEST", "NORTH", "SOUTH", "HOME",
    "LAND", "BAY", "FIRST", "SECOND", "THIRD", "ST", "AVE",
    "STREET", "AVENUE", "BLVD", "PARK", "LLC", "INC", "CORP",
    "LTD", "LP", "LLP", "GROUP", "HOLDINGS", "REALTY",
    "BUSINESS", "AMERICAN", "FLUSHING",
}

_BANK_ROOTS = {
    "BANK", "JPMORGAN", "NATIONSTAR", "MORTGAGE", "MERS", "WILMINGTON",
    "GOLDMAN", "FREEDOM", "SECRETARY", "LAKEVIEW", "NEWREZ", "CITI",
    "CITIBANK", "WELLS", "DEUTSCHE", "HSBC", "CHASE", "FLAGSTAR",
    "PENNYMAC", "LOANDEPOT", "CALIBER", "PHH", "OCWEN", "SERVICER",
    "TRUST", "TRUSTEE", "FEDERAL", "FANNIE", "FREDDIE", "HUD",
    "ELECTRONIC", "LOAN", "SAVINGS", "SACHS", "FARGO", "GSM", "WEBSTER",
    "NATIONAL", "SHELLPOINT", "MORGAN", "CITIZENS", "CARRINGTON", "FIRSTKEY",
    "CITIGROUP", "COMPUTERSHARE", "UNITED", "RCF", "VELOCITY", "NORTHEAST",
    "LOANCARE", "SELENE", "SPS", "BSI", "RUSHMORE", "ROUNDPOINT", "SERVIS",
    "STATEBRIDGE", "SPECIALIZED", "MAE", "CUSTOMERS", "CPC",
    "BATTALION", "FUNDING", "LENDING", "HABIB",
}


def _operator_root(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = re.sub(r"\b(LLC|L\.L\.C|CORP|INC|LTD|LP|LLP)\b\.?$", "", name).strip()
    tokens = cleaned.split()

    first_tok: str | None = None
    first_idx: int = -1
    for i, tok in enumerate(tokens[:2]):
        tok = tok.strip(".,;")
        if (
            len(tok) >= 3
            and tok not in _GENERIC
            and tok not in _BANK_ROOTS
            and not re.match(r"^\d+$", tok)
        ):
            first_tok = tok
            first_idx = i
            break

    if not first_tok:
        return None

    # Compound-brand extension: a short first token (e.g. "ICE") may be the
    # first half of a two-word brand that appears both fused ("ICECAP") and
    # space-separated ("ICE CAP") across different filings.  When the
    # immediately following token is also a short, non-generic brand token,
    # fuse them so both spellings map to the same root.
    if len(first_tok) <= 4 and first_idx + 1 < len(tokens):
        next_tok = tokens[first_idx + 1].strip(".,;")
        if (
            3 <= len(next_tok) <= 4
            and next_tok not in _GENERIC
            and next_tok not in _BANK_ROOTS
            and not re.match(r"^\d+$", next_tok)
        ):
            return first_tok + next_tok

    return first_tok


# ---------------------------------------------------------------------------
# Cluster extraction — all qualifying operators, not just top 20
# ---------------------------------------------------------------------------

def _load_clusters(db) -> list[dict]:
    cutoff: date = (datetime.now(timezone.utc) - timedelta(days=ANALYSIS_WINDOW_DAYS)).date()

    hd_zips: set[str] = set()
    try:
        rows = db.execute(text("SELECT zip_code FROM displacement_scores WHERE score >= 40")).fetchall()
        hd_zips = {r.zip_code for r in rows}
    except Exception as exc:
        logger.warning("displacement_scores unavailable: %s", exc)

    bbl_zip_rows = db.execute(text("""
        SELECT bbl, zip_code FROM (
            SELECT bbl, zip_code FROM violations_raw WHERE bbl IS NOT NULL AND zip_code IS NOT NULL
            UNION
            SELECT bbl, zip_code FROM permits_raw   WHERE bbl IS NOT NULL AND zip_code IS NOT NULL
        ) t
    """)).fetchall()
    bbl_zip: dict[str, str] = {r.bbl: r.zip_code for r in bbl_zip_rows if r.bbl and r.zip_code}

    rows = db.execute(text("""
        SELECT party_name_normalized, bbl, doc_date
        FROM ownership_raw
        WHERE party_type = '2'
          AND doc_date >= :cutoff
          AND party_name_normalized IS NOT NULL
          AND bbl IS NOT NULL
        ORDER BY doc_date
    """), {"cutoff": cutoff}).fetchall()

    logger.info("%d LLC acquisition records in window", len(rows))

    groups: dict[str, dict] = defaultdict(lambda: {"llc_names": set(), "bbls": set(), "acquisitions": []})

    for r in rows:
        root = _operator_root(r.party_name_normalized)
        if not root:
            continue
        g = groups[root]
        g["llc_names"].add(r.party_name_normalized)
        g["bbls"].add(r.bbl)
        g["acquisitions"].append(r.bbl)

    clusters: list[dict] = []
    for root, g in groups.items():
        llc_names    = g["llc_names"]
        acquisitions = g["acquisitions"]

        if len(llc_names) < 2 or len(acquisitions) < 5:
            continue

        avg_per_llc = len(acquisitions) / len(llc_names)
        if avg_per_llc < 2.0:
            continue

        zips_hit = {bbl_zip[bbl] for bbl in g["bbls"] if bbl in bbl_zip}
        hd_count = sum(1 for bbl in acquisitions if bbl_zip.get(bbl) in hd_zips)
        hd_pct   = hd_count / len(acquisitions) if acquisitions else 0.0

        if hd_pct < 0.30:
            continue

        clusters.append({
            "operator_root":      root,
            "llc_entities":       sorted(llc_names),
            "bbls":               g["bbls"],
            "zip_codes":          zips_hit,
            "total_acquisitions": len(acquisitions),
            "total_properties":   len(g["bbls"]),
        })

    logger.info("%d qualifying operator clusters", len(clusters))
    return clusters


# ---------------------------------------------------------------------------
# Signal detectors
# ---------------------------------------------------------------------------

def _combine_confidence(*scores: float) -> float:
    """Bayesian combination of independent signals: 1 - prod(1 - p_i)."""
    result = 1.0
    for s in scores:
        result *= (1.0 - s)
    return round(1.0 - result, 3)


def _check_name_embedding(a: dict, b: dict) -> dict | None:
    """
    Detect LLC names in one cluster that contain the other cluster's root token.

    The canonical example: "MELO Z PHANTOM CAP LLC" (in the MELO cluster) contains
    "PHANTOM" — the exact root of the PHANTOM CAPITAL network. This suggests either
    a co-branded affiliate entity or a principal operating across both networks.
    """
    root_a = a["operator_root"]
    root_b = b["operator_root"]

    def _token_match(entities: list[str], root: str) -> list[str]:
        hits = []
        for entity in entities:
            tokens = re.split(r"[\s\W]+", entity)
            if root in tokens:
                hits.append(entity)
        return hits

    a_has_b = _token_match(a["llc_entities"], root_b)
    b_has_a = _token_match(b["llc_entities"], root_a)

    if not a_has_b and not b_has_a:
        return None

    details = []
    for entity in a_has_b:
        details.append(f"'{entity}' (in {root_a} cluster) embeds root '{root_b}'")
    for entity in b_has_a:
        details.append(f"'{entity}' (in {root_b} cluster) embeds root '{root_a}'")

    return {
        "signal_type": "name_embedding",
        "confidence":  0.90,
        "detail":      "; ".join(details),
    }


def _check_root_substring(a: dict, b: dict) -> dict | None:
    """
    Detect when one operator root is a strict prefix of the other.

    Primary target: normalization splits where a space-variant of the same brand
    name produces two roots — e.g. "ICE CAP" → root "ICE" vs "ICECAP" → root
    "ICECAP". If any entity names collapse to the same string after whitespace
    removal, confidence is upgraded from 0.70 to 0.85.
    """
    root_a = a["operator_root"]
    root_b = b["operator_root"]

    if len(root_a) >= len(root_b):
        longer_root, shorter_root = root_a, root_b
        longer_c,    shorter_c    = a, b
    else:
        longer_root, shorter_root = root_b, root_a
        longer_c,    shorter_c    = b, a

    if len(shorter_root) < 3 or not longer_root.startswith(shorter_root):
        return None

    # Avoid spurious matches where the shorter root is a common English word
    # that happens to prefix a longer brand name (e.g. "ARC" → "ARCADIA")
    if shorter_root in {"ARC", "CAP", "PRO", "MAX", "GEO", "ACE", "ALL", "AMB"}:
        return None

    # Look for collapsed-name matches to confirm normalization split
    def _ws_collapse(s: str) -> str:
        return re.sub(r"\s+", "", s.upper())

    collapsed_a = {_ws_collapse(e) for e in a["llc_entities"]}
    collapsed_b = {_ws_collapse(e) for e in b["llc_entities"]}
    shared_collapsed = collapsed_a & collapsed_b

    confidence = 0.85 if shared_collapsed else 0.70
    detail = (
        f"Root '{shorter_c['operator_root']}' is a prefix of "
        f"'{longer_c['operator_root']}' — possible whitespace normalization split."
    )
    if shared_collapsed:
        examples = sorted(shared_collapsed)[:3]
        detail += f" {len(shared_collapsed)} entity name(s) confirm via whitespace collapse: {examples}"

    return {
        "signal_type": "root_substring",
        "confidence":  confidence,
        "detail":      detail,
    }


def _check_shared_bbl(a: dict, b: dict) -> dict | None:
    """
    Identify properties that appear in both clusters' acquisition sets.

    A shared BBL means the same building changed hands between entities
    associated with two different operator roots — indicating either a
    direct transaction between networks or shared principal ownership.
    """
    shared = a["bbls"] & b["bbls"]
    if not shared:
        return None

    n = len(shared)
    # Confidence curve: 1 shared BBL is weak (co-incidence possible),
    # 4+ shared BBLs across two supposedly unrelated networks is very strong.
    if n == 1:
        confidence = 0.60
    elif n <= 3:
        confidence = 0.75
    else:
        confidence = min(0.92, 0.75 + (n - 3) * 0.04)

    examples = sorted(shared)[:5]
    suffix   = f" (+ {n - 5} more)" if n > 5 else ""

    return {
        "signal_type": "shared_bbl",
        "confidence":  round(confidence, 3),
        "detail":      f"{n} BBL(s) acquired by both clusters: {examples}{suffix}",
    }


def _check_zip_jaccard(a: dict, b: dict) -> dict | None:
    """
    Measure geographic footprint overlap via Jaccard similarity.

    Pairs that target near-identical zip code sets with no other signal present
    may simply share the same market; this signal is most meaningful when it
    reinforces a name or BBL signal rather than standing alone.
    """
    if not a["zip_codes"] or not b["zip_codes"]:
        return None

    intersection = a["zip_codes"] & b["zip_codes"]
    union        = a["zip_codes"] | b["zip_codes"]
    jaccard      = len(intersection) / len(union) if union else 0.0

    if jaccard < 0.60:
        return None

    if jaccard >= 0.90:
        confidence = 0.70
    elif jaccard >= 0.75:
        confidence = 0.65
    else:
        confidence = 0.55

    return {
        "signal_type": "zip_jaccard",
        "confidence":  round(confidence, 3),
        "detail": (
            f"Jaccard={jaccard:.2f} — {len(intersection)} shared / {len(union)} total zips. "
            f"Shared: {sorted(intersection)}"
        ),
    }


# ---------------------------------------------------------------------------
# Audit orchestration
# ---------------------------------------------------------------------------

def _recommended_action(confidence: float) -> str:
    if confidence >= 0.85:
        return "merge_candidate"
    if confidence >= 0.65:
        return "flag_for_manual_review"
    return "ignore"


def run_audit(db) -> list[dict]:
    clusters = _load_clusters(db)
    findings: list[dict] = []

    total_pairs = len(clusters) * (len(clusters) - 1) // 2
    logger.info("Checking %d cluster pairs (%d clusters)", total_pairs, len(clusters))

    for a, b in combinations(clusters, 2):
        signals = [
            sig for sig in [
                _check_name_embedding(a, b),
                _check_root_substring(a, b),
                _check_shared_bbl(a, b),
                _check_zip_jaccard(a, b),
            ] if sig is not None
        ]

        if not signals:
            continue

        combined = _combine_confidence(*[s["confidence"] for s in signals])
        action   = _recommended_action(combined)

        if action == "ignore":
            continue

        findings.append({
            "cluster_a":              a["operator_root"],
            "cluster_b":              b["operator_root"],
            "cluster_a_entities":     a["llc_entities"],
            "cluster_a_properties":   a["total_properties"],
            "cluster_b_entities":     b["llc_entities"],
            "cluster_b_properties":   b["total_properties"],
            "signals":                signals,
            "combined_confidence":    combined,
            "recommended_action":     action,
        })

    findings.sort(key=lambda x: x["combined_confidence"], reverse=True)
    return findings


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    with get_scraper_db() as db:
        findings = run_audit(db)

    merge_candidates = [f for f in findings if f["recommended_action"] == "merge_candidate"]
    flag_for_review  = [f for f in findings if f["recommended_action"] == "flag_for_manual_review"]

    # Serialize — sets are not JSON-safe, already excluded from output fields
    payload = {
        "generated_at":      date.today().isoformat(),
        "analysis_window":   "18 months",
        "signals_checked":   ["name_embedding", "root_substring", "shared_bbl", "zip_jaccard"],
        "signals_not_checked": [
            "registered_address — party addresses not stored in ownership_raw "
            "(AcrisPartyInput captures only party_name, not addr_1/addr_2)",
            "deed_signatory — signatories are on physical deed instruments, not ACRIS digital records",
        ],
        "total_candidates":  len(findings),
        "merge_candidates":  len(merge_candidates),
        "flag_for_review":   len(flag_for_review),
        "findings":          findings,
    }

    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, default=str))
    logger.info("Saved %s", OUTPUT_PATH)

    print(f"\nEntity resolution audit — {date.today().isoformat()}")
    print(f"  {len(merge_candidates):>3} merge candidates")
    print(f"  {len(flag_for_review):>3} flagged for manual review")
    print(f"\nTop findings (by confidence):\n")
    for f in findings[:15]:
        sig_types = [s["signal_type"] for s in f["signals"]]
        print(
            f"  {f['cluster_a']:<18} ↔ {f['cluster_b']:<18} "
            f"conf={f['combined_confidence']:.2f}  [{', '.join(sig_types)}]"
            f"  → {f['recommended_action']}"
        )


if __name__ == "__main__":
    sys.exit(main())
