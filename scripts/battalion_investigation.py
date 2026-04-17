"""
Full investigation profile for BATTALION — mortgage lending company, complete false positive.

BATTALION FUNDING LLC, BATTALION LENDING LLC, and BATTALION MORTGAGE LLC are a
mortgage origination and servicing operation. All 92 ACRIS records are ASST
(mortgage assignment) with zero recorded consideration. There are zero deed transfers
in the dataset — BATTALION has never acquired real property; it assigns mortgage notes.

This script documents the false positive and provides a recommendation for excluding
BATTALION from the displacement monitoring pipeline.

Produces:
  scripts/battalion_investigation.json  — machine-readable dossier
  scripts/battalion_summary.txt         — plain English narrative

Usage:
    python scripts/battalion_investigation.py
"""

import json
import logging
import sys
import textwrap
from collections import defaultdict
from datetime import date
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db

logger = logging.getLogger(__name__)

JSON_OUTPUT = Path(__file__).parent / "battalion_investigation.json"
TXT_OUTPUT  = Path(__file__).parent / "battalion_summary.txt"

BATTALION_ENTITIES = [
    "BATTALION FUNDING LLC",
    "BATTALION LENDING LLC",
    "BATTALION MORTGAGE LLC",
]


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _fetch_all_records(db) -> list[dict]:
    """All ACRIS records where any BATTALION entity is grantee."""
    placeholders = ", ".join(f":e{i}" for i in range(len(BATTALION_ENTITIES)))
    params       = {f"e{i}": e for i, e in enumerate(BATTALION_ENTITIES)}
    rows = db.execute(text(f"""
        SELECT bbl, document_id, doc_type, doc_date, doc_amount, party_name_normalized AS entity
        FROM ownership_raw
        WHERE party_type = '2'
          AND party_name_normalized IN ({placeholders})
          AND bbl IS NOT NULL
        ORDER BY doc_date, bbl
    """), params).fetchall()
    return [
        {
            "bbl":         r.bbl,
            "document_id": r.document_id,
            "doc_type":    r.doc_type,
            "doc_date":    r.doc_date.isoformat() if r.doc_date else None,
            "doc_amount":  float(r.doc_amount) if r.doc_amount else None,
            "entity":      r.entity,
        }
        for r in rows
    ]


def _fetch_bbl_addresses(db, bbls: list[str]) -> dict[str, str]:
    if not bbls:
        return {}
    placeholders = ", ".join(f":b{i}" for i in range(len(bbls)))
    params       = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (bbl) bbl, address
        FROM (
            SELECT bbl, address, inspection_date AS d FROM violations_raw
            WHERE bbl IN ({placeholders}) AND address IS NOT NULL
            UNION ALL
            SELECT bbl, address, filing_date AS d FROM permits_raw
            WHERE bbl IN ({placeholders}) AND address IS NOT NULL
        ) t
        ORDER BY bbl, d DESC NULLS LAST
    """), params).fetchall()
    return {r.bbl: r.address for r in rows if r.address}


def _fetch_bbl_zips(db, bbls: list[str]) -> dict[str, str]:
    if not bbls:
        return {}
    placeholders = ", ".join(f":b{i}" for i in range(len(bbls)))
    params       = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (bbl) bbl, zip_code FROM (
            SELECT bbl, zip_code FROM violations_raw WHERE bbl IN ({placeholders}) AND zip_code IS NOT NULL
            UNION ALL
            SELECT bbl, zip_code FROM permits_raw   WHERE bbl IN ({placeholders}) AND zip_code IS NOT NULL
        ) t ORDER BY bbl
    """), params).fetchall()
    return {r.bbl: r.zip_code for r in rows if r.zip_code}


def _fetch_displacement_scores(db) -> dict[str, float]:
    rows = db.execute(text("SELECT zip_code, score FROM displacement_scores")).fetchall()
    return {r.zip_code: float(r.score) for r in rows}


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def build_investigation(db) -> dict:
    logger.info("Fetching BATTALION records...")
    all_records = _fetch_all_records(db)
    logger.info("  %d records across %d unique BBLs",
                len(all_records), len({r["bbl"] for r in all_records}))

    unique_bbls = sorted({r["bbl"] for r in all_records})

    logger.info("Fetching BBL context (addresses, zips)...")
    addresses = _fetch_bbl_addresses(db, unique_bbls)
    bbl_zips  = _fetch_bbl_zips(db, unique_bbls)
    ds_scores = _fetch_displacement_scores(db)

    # Doc type breakdown
    by_doctype: dict[str, int] = defaultdict(int)
    by_entity:  dict[str, int] = defaultdict(int)
    deed_count = 0
    asst_count = 0
    for r in all_records:
        dt = r["doc_type"] or "UNKNOWN"
        by_doctype[dt] += 1
        by_entity[r["entity"]] += 1
        if dt == "DEED":
            deed_count += 1
        elif dt == "ASST":
            asst_count += 1

    # Monthly distribution of ASST records
    monthly: dict[str, int] = defaultdict(int)
    for r in all_records:
        if r["doc_date"]:
            monthly[r["doc_date"][:7]] += 1

    # Geographic breakdown of BBLs receiving assignments
    zip_counts: dict[str, int] = defaultdict(int)
    for bbl in unique_bbls:
        z = bbl_zips.get(bbl)
        if z:
            zip_counts[z] += 1

    geo_breakdown = []
    for z, cnt in sorted(zip_counts.items(), key=lambda x: -x[1]):
        ds = ds_scores.get(z)
        geo_breakdown.append({
            "zip_code":           z,
            "bbls":               cnt,
            "displacement_score": ds,
        })

    # Displacement score distribution across BBLs (for context)
    scores = [ds_scores[bbl_zips[b]] for b in unique_bbls if bbl_zips.get(b) in ds_scores]
    avg_ds = round(sum(scores) / len(scores), 1) if scores else 0.0
    high_ds_count = sum(1 for s in scores if s >= 40)
    high_ds_pct   = round(high_ds_count / len(scores) * 100, 1) if scores else 0.0

    return {
        "generated_at":  date.today().isoformat(),
        "subject":       "BATTALION",
        "classification": "FALSE POSITIVE — mortgage lending company, no property acquisitions",
        "entities":      BATTALION_ENTITIES,
        "total_records": len(all_records),
        "unique_bbls":   len(unique_bbls),
        "deed_count":    deed_count,
        "asst_count":    asst_count,
        "entity_breakdown":  dict(by_entity),
        "doctype_breakdown": dict(by_doctype),
        "monthly_pace":      dict(sorted(monthly.items())),
        "geographic_context": {
            "avg_displacement_score": avg_ds,
            "high_displacement_pct":  high_ds_pct,
            "geo_breakdown":          geo_breakdown[:10],
        },
        "determination": "exclude",
        "reason": (
            "BATTALION FUNDING, LENDING, and MORTGAGE LLC are a mortgage origination "
            "and servicing operation. All 92 ACRIS records are ASST (mortgage assignment) "
            "with zero recorded consideration. BATTALION has never acquired real property "
            "via deed transfer — it assigns and receives mortgage notes. The 41.3% "
            "high-displacement-zip rate in the profiling output reflects where its "
            "borrowers live, not where BATTALION operates as a landlord. "
            "Recommended fix: add BATTALION to the mortgage-lender exclusion list."
        ),
        "recommended_fix": "Add BATTALION to _BANK_ROOTS or create _MORTGAGE_ROOTS exclusion set in operator_network_analysis.py",
    }


# ---------------------------------------------------------------------------
# Plain-English summary
# ---------------------------------------------------------------------------

def write_summary(data: dict, path: Path) -> None:
    entity_lines = [
        f"    • {e}: {cnt} record(s)"
        for e, cnt in data["entity_breakdown"].items()
    ]

    doctype_lines = [
        f"    • {dt}: {cnt}"
        for dt, cnt in data["doctype_breakdown"].items()
    ]

    monthly_lines = [
        f"    {ym}: {'█' * min(count, 60)} ({count})"
        for ym, count in sorted(data["monthly_pace"].items())
    ]

    geo = data["geographic_context"]
    geo_lines = []
    for g in geo["geo_breakdown"][:8]:
        ds_str = f"{g['displacement_score']:.1f}" if g["displacement_score"] is not None else "N/A"
        geo_lines.append(f"    {g['zip_code']}: {g['bbls']} BBL(s)  (displacement score {ds_str})")

    summary = textwrap.dedent(f"""\
    BATTALION OPERATOR INVESTIGATION
    Generated: {data['generated_at']}
    ══════════════════════════════════════════════════════════════════════

    CLASSIFICATION: {data['classification']}

    OVERVIEW
    --------
    BATTALION FUNDING LLC, BATTALION LENDING LLC, and BATTALION MORTGAGE LLC
    are a mortgage origination and servicing operation. Of the {data['total_records']} ACRIS
    records where a BATTALION entity appears as grantee (party_type='2'), all
    {data['asst_count']} are document type ASST (mortgage assignment). There are zero
    deed transfers — BATTALION has never acquired real property.

    When a mortgage lender appears as grantee on an ASST document in ACRIS, it
    means the lender received a mortgage note assignment, not a deed to the property.
    This is routine loan servicing activity. The high-displacement-zip concentration
    flagged in the operator profile (41.3%) reflects where BATTALION's borrowers live,
    not where BATTALION operates as a landlord.

    ENTITIES
    --------
{chr(10).join(entity_lines)}

    DOCUMENT TYPE BREAKDOWN
    -----------------------
{chr(10).join(doctype_lines)}

    Deed count:  {data['deed_count']}  ← zero acquisitions
    ASST count:  {data['asst_count']}  ← mortgage assignments only

    ASSIGNMENT PACE (monthly)
    -------------------------
{chr(10).join(monthly_lines)}

    GEOGRAPHIC CONTEXT (zip codes of assigned properties)
    -------------------------------------------------------
{chr(10).join(geo_lines) if geo_lines else '    No zip code data available.'}

    Average displacement score across BBL zip codes: {geo['avg_displacement_score']}
    BBLs in high-displacement zips (score ≥40):      {geo['high_displacement_pct']}%

    Note: these metrics describe BATTALION's borrower geography, not operator behavior.
    A mortgage company making loans in high-displacement neighborhoods is not itself
    a displacement actor.

    ASSESSMENT
    ----------
    BATTALION is a clean false positive. The naming convention (Funding, Lending,
    Mortgage) is an unambiguous signal that operator_network_analysis.py's current
    exclusion logic does not catch. The script strips common terms like BANK and REALTY
    but does not strip FUNDING, LENDING, or MORTGAGE as operator roots.

    No eviction-then-buy analysis is possible or relevant — there are no deed
    acquisitions to cross-reference against eviction records.

    RECOMMENDATION: Exclude BATTALION from all operator monitoring. Add FUNDING,
    LENDING, and MORTGAGE as root-level exclusion terms alongside BANK in the
    operator detection pipeline.
    ══════════════════════════════════════════════════════════════════════
    """)

    path.write_text(summary)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    with get_scraper_db() as db:
        data = build_investigation(db)

    JSON_OUTPUT.write_text(json.dumps(data, indent=2, default=str))
    logger.info("Saved JSON: %s", JSON_OUTPUT)

    write_summary(data, TXT_OUTPUT)
    logger.info("Saved summary: %s", TXT_OUTPUT)

    print(f"\nBATTALION Investigation — {data['classification']}\n")
    print(f"  Entities:       {', '.join(data['entities'])}")
    print(f"  Total records:  {data['total_records']} ({data['asst_count']} ASST, {data['deed_count']} DEED)")
    print(f"  Unique BBLs:    {data['unique_bbls']}")
    print(f"  Determination:  {data['determination'].upper()}")
    print(f"  Fix:            {data['recommended_fix']}")


if __name__ == "__main__":
    sys.exit(main())
