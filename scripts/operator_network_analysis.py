"""
Operator network analysis — finds operators running the MTEK acquisition pattern
across all ACRIS data: multiple LLCs, 5+ properties in 18 months, concentrated
in high-displacement zip codes.

Outputs scripts/operator_network_analysis.json ranked by portfolio size.

Usage:
    python scripts/operator_network_analysis.py
"""

import json
import logging
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db

logger = logging.getLogger(__name__)

OUTPUT_PATH = Path(__file__).parent / "operator_network_analysis.json"

# Tokens that disqualify as an operator root — location words, LLC synonyms.
_GENERIC = {
    "THE", "NEW", "ONE", "TWO", "NY", "NYC", "REAL", "URBAN",
    "CITY", "OLD", "EAST", "WEST", "NORTH", "SOUTH", "HOME",
    "LAND", "BAY", "FIRST", "SECOND", "THIRD", "ST", "AVE",
    "STREET", "AVENUE", "BLVD", "PARK", "LLC", "INC", "CORP",
    "LTD", "LP", "LLP", "GROUP", "HOLDINGS", "REALTY",
    # Generic second-token fallthrough words: when a bank root blocks the first
    # token, these stop the fallthrough from producing a spurious root.
    "BUSINESS",   # Webster BUSINESS Credit — blocked by WEBSTER, falls to this
    "AMERICAN",   # HABIB AMERICAN BANK — blocked by HABIB, falls to this
    # Geographic place names that aggregate unrelated LLCs sharing a neighborhood
    "FLUSHING",   # Queens neighborhood — Flushing Bank + unrelated address LLCs
}

# Financial institutions that appear in ACRIS via securitization/foreclosure activity.
# Their LLC vehicles don't reflect the speculative residential acquisition pattern
# we're looking for — exclude them from the operator analysis.
_BANK_ROOTS = {
    "BANK", "JPMORGAN", "NATIONSTAR", "MORTGAGE", "MERS", "WILMINGTON",
    "GOLDMAN", "FREEDOM", "SECRETARY", "LAKEVIEW", "NEWREZ", "CITI",
    "CITIBANK", "WELLS", "DEUTSCHE", "HSBC", "CHASE", "FLAGSTAR",
    "PENNYMAC", "LOANDEPOT", "CALIBER", "PHH", "OCWEN", "SERVICER",
    "TRUST", "TRUSTEE", "FEDERAL", "FANNIE", "FREDDIE", "HUD",
    # Servicers, GSEs, and financial pass-throughs — appear in ACRIS via
    # securitization, foreclosure, and note-sale activity, not acquisitions.
    "ELECTRONIC", "LOAN", "SAVINGS", "SACHS", "FARGO", "GSM", "WEBSTER",
    "NATIONAL", "SHELLPOINT", "MORGAN", "CITIZENS", "CARRINGTON", "FIRSTKEY",
    "CITIGROUP", "COMPUTERSHARE", "UNITED", "RCF", "VELOCITY", "NORTHEAST",
    "LOANCARE", "SELENE", "SPS", "BSI", "RUSHMORE", "ROUNDPOINT", "SERVIS",
    "STATEBRIDGE", "SPECIALIZED",
    # GSE fallthrough: FANNIE blocks first token, MAE becomes the extracted root
    "MAE",
    # Customers Bank — Pennsylvania commercial bank
    "CUSTOMERS",
    # Community Preservation Corporation — nonprofit affordable housing lender;
    # high HD-zip concentration reflects their mission, not displacement activity
    "CPC",
    # Mortgage originator/servicer naming patterns — all 92 ACRIS records for
    # BATTALION FUNDING/LENDING/MORTGAGE are ASST (note assignments), zero deeds.
    # Adding FUNDING and LENDING as root-level blocks catches similar operators
    # whose first token is a lending-function word rather than an institution name.
    "BATTALION", "FUNDING", "LENDING",
    # HABIB AMERICAN BANK (a/k/a HAB Bank) — NY-chartered commercial bank;
    # all 40 records are mortgage assignments (ASST), zero deed transfers.
    # Root block on HABIB covers all bank entity variants in one token.
    "HABIB",
}


def _operator_root(name: str | None) -> str | None:
    """
    Extract the identifying brand from a normalized LLC name.

      "MTEK NYC LLC"            -> "MTEK"
      "BROWNSTONE EQUITIES LLC" -> "BROWNSTONE"
      "123 BROADWAY OWNER LLC"  -> None  (numeric lead)

    Returns None for bank/financial roots and common Chinese/Korean/South Asian
    surnames that generate many unrelated single-LLC investors — those patterns
    don't reflect the coordinated multi-LLC acquisition scheme we're tracking.

    Compound-brand extension: short first tokens (≤4 chars) are fused with a
    short non-generic successor token when present, so space-variant spellings
    of the same brand ("ICE CAP" vs "ICECAP") resolve to the same root key.
    """
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


def _load_high_displacement_zips(db) -> set[str]:
    try:
        rows = db.execute(
            text("SELECT zip_code FROM displacement_scores WHERE score >= 40")
        ).fetchall()
        if rows:
            return {r.zip_code for r in rows}
    except Exception as exc:
        logger.warning("displacement_scores unavailable: %s", exc)
    return set()


def _load_bbl_zips(db) -> dict[str, str]:
    """BBL → zip_code from violations and permits tables (both have good coverage)."""
    rows = db.execute(text("""
        SELECT bbl, zip_code FROM (
            SELECT bbl, zip_code FROM violations_raw WHERE bbl IS NOT NULL AND zip_code IS NOT NULL
            UNION
            SELECT bbl, zip_code FROM permits_raw   WHERE bbl IS NOT NULL AND zip_code IS NOT NULL
        ) t
    """)).fetchall()
    return {r.bbl: r.zip_code for r in rows if r.bbl and r.zip_code}


def run_analysis(db) -> list[dict]:
    cutoff: date = (datetime.now(timezone.utc) - timedelta(days=548)).date()  # ~18 months

    hd_zips = _load_high_displacement_zips(db)
    bbl_zip  = _load_bbl_zips(db)

    logger.info("%d high-displacement zips loaded", len(hd_zips))
    logger.info("BBL→zip lookup: %d properties", len(bbl_zip))

    rows = db.execute(text("""
        SELECT party_name_normalized, bbl, doc_date, doc_amount
        FROM ownership_raw
        WHERE party_type = '2'
          AND doc_date >= :cutoff
          AND party_name_normalized IS NOT NULL
          AND bbl IS NOT NULL
        ORDER BY doc_date
    """), {"cutoff": cutoff}).fetchall()

    logger.info("%d LLC acquisitions pulled for last 18 months", len(rows))

    # root -> {llc_names: set, acquisitions: list}
    groups: dict[str, dict] = defaultdict(lambda: {"llc_names": set(), "acquisitions": []})

    for r in rows:
        root = _operator_root(r.party_name_normalized)
        if not root:
            continue
        g = groups[root]
        g["llc_names"].add(r.party_name_normalized)
        g["acquisitions"].append({
            "bbl":        r.bbl,
            "doc_date":   r.doc_date,
            "doc_amount": float(r.doc_amount) if r.doc_amount else None,
            "party_name": r.party_name_normalized,
        })

    results = []
    for root, g in groups.items():
        llc_names   = g["llc_names"]
        acquisitions = g["acquisitions"]

        if len(llc_names) < 2 or len(acquisitions) < 5:
            continue

        # Filter out surname-based clusters: when nearly every LLC is a different
        # single-property investor (e.g. 595 "CHEN X LLC" entities for 700 properties),
        # the root isn't a coordinated operator — it's just a common last name.
        # Coordinated operators like MTEK run ~3-10 properties per LLC.
        avg_per_llc = len(acquisitions) / len(llc_names)
        if avg_per_llc < 2.0:
            continue

        zips_hit = {bbl_zip[a["bbl"]] for a in acquisitions if a["bbl"] in bbl_zip}
        hd_count = sum(1 for a in acquisitions if bbl_zip.get(a["bbl"]) in hd_zips)
        hd_pct   = hd_count / len(acquisitions) if acquisitions else 0.0

        # Require meaningful concentration in high-displacement zips — filters
        # out metro-wide flippers and institutional portfolios with no geographic focus.
        if hd_pct < 0.30:
            continue

        total_val = sum(a["doc_amount"] for a in acquisitions if a["doc_amount"])
        avg_price = total_val / len(acquisitions) if acquisitions else 0.0

        dates = sorted(a["doc_date"] for a in acquisitions if a["doc_date"])
        if len(dates) >= 2:
            span = max((dates[-1] - dates[0]).days, 1)
            velocity = round(len(dates) / (span / 30), 2)
        else:
            velocity = 0.0

        results.append({
            "operator_root":                root,
            "total_properties":             len({a["bbl"] for a in acquisitions}),
            "total_acquisitions":           len(acquisitions),
            "llc_count":                    len(llc_names),
            "llc_entities":                 sorted(llc_names),
            "total_portfolio_value":        round(total_val, 2),
            "avg_acquisition_price":        round(avg_price, 2),
            "acquisitions_per_month":       velocity,
            "first_acquisition":            dates[0].isoformat() if dates else None,
            "last_acquisition":             dates[-1].isoformat() if dates else None,
            "zip_codes_targeted":           sorted(zips_hit),
            "high_displacement_acquisitions": hd_count,
            "high_displacement_pct":        round(hd_pct * 100, 1),
            "targets_high_displacement":    hd_pct >= 0.5,
        })

    results.sort(key=lambda x: (x["total_properties"], x["total_portfolio_value"]), reverse=True)
    return results[:20]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    with get_scraper_db() as db:
        operators = run_analysis(db)

    payload = {
        "generated_at":       date.today().isoformat(),
        "analysis_window":    "18 months",
        "criteria": {
            "min_llc_entities":       2,
            "min_acquisitions":       5,
            "high_displacement_score_threshold": 40,
        },
        "operators_found":    len(operators),
        "operators":          operators,
    }

    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, default=str))
    logger.info("Saved %s", OUTPUT_PATH)

    print(f"\nTop {len(operators)} operators by portfolio size:\n")
    for i, op in enumerate(operators, 1):
        print(
            f"  {i:>2}. {op['operator_root']:<20} "
            f"{op['total_properties']:>3} props  "
            f"{op['llc_count']:>2} LLCs  "
            f"{op['high_displacement_pct']:>5.1f}% in HD zips"
        )


if __name__ == "__main__":
    sys.exit(main())
