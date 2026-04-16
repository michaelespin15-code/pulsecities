"""
Deep displacement profile for the top confirmed operators from operator_network_analysis.py.

For each operator, this script cross-references three additional datasets:
  - displacement_scores: weighted average displacement score for targeted zip codes
  - evictions_raw: eviction-then-buy matches (eviction on BBL ≤ 12 months before acquisition)
  - violations_raw: HPD Class B/C violations on acquired properties

Operators are ranked by a composite displacement evidence score.

Also resolves five "flag for review" operators (MAE, CUSTOMERS, FLUSHING, CPC, BUSINESS)
against their actual LLC entity names, makes a determination, and lists them in the output.

Usage:
    python scripts/top_operators_profiled.py
"""

import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db

logger = logging.getLogger(__name__)

OUTPUT_PATH  = Path(__file__).parent / "top_operators_profiled.json"
SOURCE_PATH  = Path(__file__).parent / "operator_network_analysis.json"

# Operators to profile in depth
PROFILE_ROOTS = {"OCEANVIEW", "ICECAP", "BREDIF", "TOORAK", "BATTALION", "HABIB", "PHANTOM"}

# Window for eviction-then-buy: eviction executed within N days before deed transfer
EVICTION_LOOKBACK_DAYS = 365


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_source() -> dict[str, dict]:
    """Return operator_root → raw network analysis record."""
    payload = json.loads(SOURCE_PATH.read_text())
    return {op["operator_root"]: op for op in payload["operators"]}


def _load_displacement_scores(db) -> dict[str, float]:
    rows = db.execute(text("SELECT zip_code, score FROM displacement_scores")).fetchall()
    return {r.zip_code: float(r.score) for r in rows}


def _load_evictions_by_bbl(db) -> dict[str, list[date]]:
    """Return BBL → sorted list of eviction execution dates."""
    rows = db.execute(text(
        "SELECT bbl, executed_date FROM evictions_raw "
        "WHERE bbl IS NOT NULL AND executed_date IS NOT NULL"
    )).fetchall()
    result: dict[str, list[date]] = {}
    for r in rows:
        result.setdefault(r.bbl, []).append(r.executed_date)
    for bbl in result:
        result[bbl].sort()
    return result


def _load_violations_by_bbl(db) -> dict[str, dict]:
    """Return BBL → {total, class_b, class_c, class_a} violation counts."""
    rows = db.execute(text(
        "SELECT bbl, violation_class, COUNT(*) AS cnt "
        "FROM violations_raw "
        "WHERE bbl IS NOT NULL "
        "GROUP BY bbl, violation_class"
    )).fetchall()
    result: dict[str, dict] = {}
    for r in rows:
        bbl = r.bbl
        vc  = (r.violation_class or "").upper()
        if bbl not in result:
            result[bbl] = {"total": 0, "class_a": 0, "class_b": 0, "class_c": 0}
        result[bbl]["total"] += r.cnt
        if vc in ("A", "B", "C"):
            result[bbl][f"class_{vc.lower()}"] += r.cnt
    return result


def _fetch_operator_bbls(db, llc_entities: list[str]) -> list[dict]:
    """
    Pull all acquisitions for an operator's LLC entities from ownership_raw.
    Returns list of {bbl, doc_date, doc_amount, party_name}.
    """
    placeholders = ", ".join(f":name_{i}" for i in range(len(llc_entities)))
    params       = {f"name_{i}": name for i, name in enumerate(llc_entities)}
    rows = db.execute(text(f"""
        SELECT bbl, doc_date, doc_amount, party_name_normalized
        FROM ownership_raw
        WHERE party_type = '2'
          AND party_name_normalized IN ({placeholders})
          AND bbl IS NOT NULL
        ORDER BY doc_date
    """), params).fetchall()
    return [
        {
            "bbl":        r.bbl,
            "doc_date":   r.doc_date,
            "doc_amount": float(r.doc_amount) if r.doc_amount else None,
            "entity":     r.party_name_normalized,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Core profiler
# ---------------------------------------------------------------------------

def _profile_operator(
    root: str,
    base: dict,
    acquisitions: list[dict],
    displacement_scores: dict[str, float],
    bbl_zip: dict[str, str],
    evictions_by_bbl: dict[str, list[date]],
    violations_by_bbl: dict[str, dict],
) -> dict:

    unique_bbls: set[str] = {a["bbl"] for a in acquisitions}

    # --- Zip / displacement score stats ---
    zip_scores: list[float] = []
    zip_breakdown: dict[str, int] = {}
    for bbl in unique_bbls:
        z = bbl_zip.get(bbl)
        if not z:
            continue
        zip_breakdown[z] = zip_breakdown.get(z, 0) + 1
        if z in displacement_scores:
            zip_scores.append(displacement_scores[z])

    weighted_avg_score = round(sum(zip_scores) / len(zip_scores), 1) if zip_scores else 0.0

    # Score tier distribution across acquired properties
    tier_counts = {"score_80_plus": 0, "score_60_79": 0, "score_40_59": 0, "score_under_40": 0}
    for s in zip_scores:
        if   s >= 80: tier_counts["score_80_plus"] += 1
        elif s >= 60: tier_counts["score_60_79"]   += 1
        elif s >= 40: tier_counts["score_40_59"]   += 1
        else:         tier_counts["score_under_40"] += 1

    # --- Eviction-then-buy ---
    eviction_matches: list[dict] = []
    for a in acquisitions:
        bbl      = a["bbl"]
        acq_date = a["doc_date"]
        if not acq_date or bbl not in evictions_by_bbl:
            continue
        cutoff = acq_date - timedelta(days=EVICTION_LOOKBACK_DAYS)
        prior  = [d for d in evictions_by_bbl[bbl] if cutoff <= d < acq_date]
        if prior:
            eviction_matches.append({
                "bbl":              bbl,
                "acquisition_date": acq_date.isoformat() if acq_date else None,
                "evictions_before": len(prior),
                "latest_eviction":  max(prior).isoformat(),
                "days_gap":         (acq_date - max(prior)).days,
            })

    # --- HPD violations ---
    total_violations = 0
    class_b_total    = 0
    class_c_total    = 0
    bbls_with_bc     = 0
    for bbl in unique_bbls:
        v = violations_by_bbl.get(bbl, {})
        total_violations += v.get("total",   0)
        b = v.get("class_b", 0)
        c = v.get("class_c", 0)
        class_b_total += b
        class_c_total += c
        if b + c > 0:
            bbls_with_bc += 1

    n_props = len(unique_bbls) or 1
    violations_per_property = round(total_violations / n_props, 2)
    bc_per_property         = round((class_b_total + class_c_total) / n_props, 2)

    # --- Acquisition timeline ---
    dates = sorted(a["doc_date"] for a in acquisitions if a["doc_date"])
    if len(dates) >= 2:
        span     = max((dates[-1] - dates[0]).days, 1)
        velocity = round(len(dates) / (span / 30), 2)
    else:
        velocity = 0.0

    total_val = sum(a["doc_amount"] for a in acquisitions if a["doc_amount"])
    avg_price = round(total_val / len(acquisitions), 2) if acquisitions else 0.0

    # --- Composite displacement evidence score (0–100) ---
    # Weights:
    #   30 pts — HD zip concentration (% acquisitions in displacement zip codes)
    #   25 pts — Eviction-then-buy rate (% BBLs with prior eviction)
    #   25 pts — Weighted average displacement score (normalized to 0–25)
    #   20 pts — Class B+C violation density (bc_per_property, capped at 10)
    hd_pct      = base.get("high_displacement_pct", 0) / 100
    etb_rate    = len(eviction_matches) / n_props
    norm_score  = min(weighted_avg_score / 100, 1.0)
    norm_bc     = min(bc_per_property / 10, 1.0)

    evidence_score = round(
        hd_pct   * 30 +
        etb_rate * 25 +
        norm_score * 25 +
        norm_bc  * 20,
        1,
    )

    return {
        "operator_root":               root,
        "llc_entities":                base["llc_entities"],
        "llc_count":                   base["llc_count"],
        "total_properties":            len(unique_bbls),
        "total_acquisitions":          len(acquisitions),
        "total_portfolio_value":       round(total_val, 2),
        "avg_acquisition_price":       avg_price,
        "acquisitions_per_month":      velocity,
        "first_acquisition":           dates[0].isoformat() if dates else None,
        "last_acquisition":            dates[-1].isoformat() if dates else None,
        "displacement_score": {
            "weighted_avg":            weighted_avg_score,
            "tier_distribution":       tier_counts,
            "zip_concentration":       dict(sorted(zip_breakdown.items(), key=lambda x: -x[1])[:10]),
        },
        "high_displacement_pct":       base.get("high_displacement_pct", 0),
        "eviction_then_buy": {
            "matched_properties":      len(eviction_matches),
            "match_rate_pct":          round(len(eviction_matches) / n_props * 100, 1),
            "lookback_days":           EVICTION_LOOKBACK_DAYS,
            "matches":                 sorted(eviction_matches, key=lambda x: x["days_gap"]),
        },
        "hpd_violations": {
            "total":                   total_violations,
            "class_b":                 class_b_total,
            "class_c":                 class_c_total,
            "bbls_with_class_bc":      bbls_with_bc,
            "violations_per_property": violations_per_property,
            "class_bc_per_property":   bc_per_property,
        },
        "evidence_score":              evidence_score,
    }


# ---------------------------------------------------------------------------
# Manual review determination
# ---------------------------------------------------------------------------

_REVIEW_VERDICTS = {
    "MAE": {
        "entities_sampled":  ["FANNIE MAE", "FANNIE MAE C/O BERKADIA COMMERCIAL MORTGAGE LLC"],
        "determination":     "exclude",
        "reason":            "Fannie Mae — federal GSE. ACRIS records are multifamily mortgage assignments, not residential acquisitions. Root 'MAE' slips through because 'FANNIE' blocks first.",
        "recommended_fix":   "Add MAE to _BANK_ROOTS",
    },
    "CUSTOMERS": {
        "entities_sampled":  ["CUSTOMERS BANK", "CUSTOMERS BANK A PENNSYLVANIA STATE CHARTEREDBANK"],
        "determination":     "exclude",
        "reason":            "Customers Bank — Pennsylvania-chartered commercial bank. All 3 LLC variants are the same institution.",
        "recommended_fix":   "Add CUSTOMERS to _BANK_ROOTS",
    },
    "FLUSHING": {
        "entities_sampled":  ["FLUSHING BANK", "FLUSHING INVESTORS GROUP LLC", "FLUSHING DEVELOPERS LLC", "3629 FLUSHING LLC"],
        "determination":     "exclude",
        "reason":            "Mixed false positive. 'FLUSHING' is a Queens neighborhood name — the 23 LLC entities include both Flushing Savings Bank mortgages and unrelated address-named LLCs. No evidence of coordinated acquisition network.",
        "recommended_fix":   "Add FLUSHING to _GENERIC (geographic place name, not an operator brand)",
    },
    "CPC": {
        "entities_sampled":  ["CPC MORTGAGE COMPANY LLC", "CPC CONVERSION SPV LLC"],
        "determination":     "exclude",
        "reason":            "Community Preservation Corporation — nonprofit affordable housing lender. High HD-zip concentration (69.6%) reflects their mission, not displacement activity.",
        "recommended_fix":   "Add CPC to _BANK_ROOTS",
    },
    "BUSINESS": {
        "entities_sampled":  ["WEBSTER BUSINESS BANK", "WEBSTER BUSINESS CREDIT A DIVISION OF WEBSTER BANK"],
        "determination":     "exclude",
        "reason":            "Webster Bank / Webster Business Credit — commercial bank. 22 variant names for the same institution. Slips through because 'WEBSTER' (already in _BANK_ROOTS) blocks first token, leaving 'BUSINESS' as the extracted root.",
        "recommended_fix":   "Add BUSINESS to _GENERIC",
    },
}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    source = _load_source()

    with get_scraper_db() as db:
        logger.info("Loading lookups...")
        displacement_scores = _load_displacement_scores(db)
        bbl_zip             = _load_bbl_zips(db)
        evictions_by_bbl    = _load_evictions_by_bbl(db)
        violations_by_bbl   = _load_violations_by_bbl(db)

        logger.info(
            "%d displacement scores, %d BBL→zip mappings, "
            "%d BBLs with evictions, %d BBLs with violations",
            len(displacement_scores), len(bbl_zip),
            len(evictions_by_bbl),    len(violations_by_bbl),
        )

        profiles = []
        for root in PROFILE_ROOTS:
            if root not in source:
                logger.warning("Operator %s not found in source JSON — skipping", root)
                continue

            base        = source[root]
            llc_entities = base["llc_entities"]
            logger.info("Profiling %s (%d LLCs)...", root, len(llc_entities))

            acquisitions = _fetch_operator_bbls(db, llc_entities)
            logger.info("  %d acquisition records found", len(acquisitions))

            profile = _profile_operator(
                root, base, acquisitions,
                displacement_scores, bbl_zip,
                evictions_by_bbl, violations_by_bbl,
            )
            profiles.append(profile)

    # Rank by composite evidence score
    profiles.sort(key=lambda x: x["evidence_score"], reverse=True)

    payload = {
        "generated_at":     date.today().isoformat(),
        "profiling_method": {
            "eviction_lookback_days":         EVICTION_LOOKBACK_DAYS,
            "displacement_score_threshold":   40,
            "evidence_score_weights": {
                "hd_zip_concentration_pct":   "30 pts",
                "eviction_then_buy_rate":      "25 pts",
                "weighted_avg_displacement":   "25 pts",
                "class_bc_violation_density":  "20 pts",
            },
        },
        "operators_profiled": len(profiles),
        "operators":          profiles,
        "flagged_for_review": _REVIEW_VERDICTS,
    }

    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, default=str))
    logger.info("Saved %s", OUTPUT_PATH)

    print(f"\nOperator displacement profile — ranked by evidence score:\n")
    print(f"  {'#':<3} {'Operator':<12} {'Score':>5}  {'Props':>5}  "
          f"{'HD%':>5}  {'ETB%':>5}  {'B+C/prop':>8}  {'Avg DS':>6}")
    print(f"  {'-'*3} {'-'*12} {'-'*5}  {'-'*5}  {'-'*5}  {'-'*5}  {'-'*8}  {'-'*6}")
    for i, op in enumerate(profiles, 1):
        print(
            f"  {i:<3} {op['operator_root']:<12} "
            f"{op['evidence_score']:>5.1f}  "
            f"{op['total_properties']:>5}  "
            f"{op['high_displacement_pct']:>5.1f}  "
            f"{op['eviction_then_buy']['match_rate_pct']:>5.1f}  "
            f"{op['hpd_violations']['class_bc_per_property']:>8.2f}  "
            f"{op['displacement_score']['weighted_avg']:>6.1f}"
        )


def _load_bbl_zips(db) -> dict[str, str]:
    rows = db.execute(text("""
        SELECT bbl, zip_code FROM (
            SELECT bbl, zip_code FROM violations_raw WHERE bbl IS NOT NULL AND zip_code IS NOT NULL
            UNION
            SELECT bbl, zip_code FROM permits_raw   WHERE bbl IS NOT NULL AND zip_code IS NOT NULL
        ) t
    """)).fetchall()
    return {r.bbl: r.zip_code for r in rows if r.bbl and r.zip_code}


if __name__ == "__main__":
    sys.exit(main())
