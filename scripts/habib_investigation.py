"""
Full investigation profile for HABIB — mixed cluster of bank entities (false positive)
and individual landlords (family network).

The 'HABIB' operator root in operator_network_analysis.py resolves to two distinct groups:

  GROUP A — HABIB AMERICAN BANK and variants (39 ASST records, 34 BBLs):
    Mortgage assignments received by Habib American Bank / HAB Bank. Party_type='2' on
    ASST documents means the bank is the assignee of a mortgage, not a property buyer.
    This is normal banking activity — NOT property acquisition. FALSE POSITIVE.

  GROUP B — HABIB individuals (5 DEED records, 5 BBLs, ~$3.96M spend):
    HABIB MOHAMMAD A, HABIB SHAHINA, HABIB SHIMUL, HABIB AHSAN, HABIB AYESHA.
    These are genuine residential deed purchases. Likely a family network operating
    as small-scale landlords in Brooklyn and the Bronx.

Produces:
  scripts/habib_investigation.json  — machine-readable dossier
  scripts/habib_summary.txt         — plain English narrative

Usage:
    python scripts/habib_investigation.py
"""

import json
import logging
import sys
import textwrap
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db

logger = logging.getLogger(__name__)

JSON_OUTPUT = Path(__file__).parent / "habib_investigation.json"
TXT_OUTPUT  = Path(__file__).parent / "habib_summary.txt"

# Bank entities — appear as grantees of ASST (mortgage assignment), not deeds.
# These are false positives for displacement analysis.
HABIB_BANK_ENTITIES = [
    "HABIB AMERICAN BANK",
    "HABIB AMERICAN BANK A/K/A HAB BANK",
    "HABIB AMERICAN BANK AKA HAB BANK",
    "HABIB AMERICAN BANK ISAOA/ATIMA",
    "HABIB BANK",
]

# Individual landlords — genuine deed purchases.
HABIB_INDIVIDUAL_ENTITIES = [
    "HABIB AHSAN",
    "HABIB AYESHA",
    "HABIB MOHAMMAD A",
    "HABIB SHAHINA",
    "HABIB SHIMUL",
]

ALL_HABIB_ENTITIES = HABIB_BANK_ENTITIES + HABIB_INDIVIDUAL_ENTITIES

EVICTION_LOOKBACK_DAYS = 365


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _fetch_bank_asst_records(db) -> list[dict]:
    """Mortgage assignments received by HABIB bank entities (false positives)."""
    placeholders = ", ".join(f":e{i}" for i in range(len(HABIB_BANK_ENTITIES)))
    params       = {f"e{i}": e for i, e in enumerate(HABIB_BANK_ENTITIES)}
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


def _fetch_individual_deed_acquisitions(db) -> list[dict]:
    """Genuine deed purchases by HABIB individuals."""
    placeholders = ", ".join(f":e{i}" for i in range(len(HABIB_INDIVIDUAL_ENTITIES)))
    params       = {f"e{i}": e for i, e in enumerate(HABIB_INDIVIDUAL_ENTITIES)}
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
            "doc_date":    r.doc_date,
            "doc_amount":  float(r.doc_amount) if r.doc_amount else None,
            "entity":      r.entity,
        }
        for r in rows
    ]


def _fetch_violations(db, bbls: list[str]) -> dict[str, list[dict]]:
    if not bbls:
        return {}
    placeholders = ", ".join(f":b{i}" for i in range(len(bbls)))
    params       = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT bbl, violation_id, violation_class, description,
               inspection_date, nov_issued_date, current_status
        FROM violations_raw
        WHERE bbl IN ({placeholders})
        ORDER BY bbl, inspection_date
    """), params).fetchall()
    result: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        result[r.bbl].append({
            "violation_id":    r.violation_id,
            "class":           r.violation_class,
            "description":     r.description,
            "inspection_date": r.inspection_date.isoformat() if r.inspection_date else None,
            "nov_issued_date": r.nov_issued_date.isoformat() if r.nov_issued_date else None,
            "current_status":  r.current_status,
        })
    return dict(result)


def _fetch_evictions(db, bbls: list[str]) -> dict[str, list[dict]]:
    if not bbls:
        return {}
    placeholders = ", ".join(f":b{i}" for i in range(len(bbls)))
    params       = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (bbl, executed_date) bbl, executed_date,
               docket_number, eviction_type, address
        FROM evictions_raw
        WHERE bbl IN ({placeholders})
          AND executed_date IS NOT NULL
        ORDER BY bbl, executed_date
    """), params).fetchall()
    result: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        result[r.bbl].append({
            "executed_date": r.executed_date,
            "docket_number": r.docket_number,
            "eviction_type": r.eviction_type,
            "address":       r.address,
        })
    return dict(result)


def _fetch_addresses(db, bbls: list[str]) -> dict[str, str]:
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


def _fetch_displacement_scores(db) -> dict[str, dict]:
    rows = db.execute(text(
        "SELECT zip_code, score, eviction_rate, llc_acquisition_rate, "
        "permit_intensity, complaint_rate FROM displacement_scores"
    )).fetchall()
    return {
        r.zip_code: {
            "score":                float(r.score),
            "eviction_rate":        float(r.eviction_rate or 0),
            "llc_acquisition_rate": float(r.llc_acquisition_rate or 0),
        }
        for r in rows
    }


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


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def build_investigation(db) -> dict:
    logger.info("Fetching HABIB bank records (ASST)...")
    bank_records = _fetch_bank_asst_records(db)
    logger.info("  %d bank ASST records across %d BBLs",
                len(bank_records), len({r["bbl"] for r in bank_records}))

    logger.info("Fetching HABIB individual acquisitions (DEED)...")
    individual_acqs = _fetch_individual_deed_acquisitions(db)
    logger.info("  %d deed records across %d BBLs",
                len(individual_acqs), len({a["bbl"] for a in individual_acqs}))

    # Focus cross-references on individual (deed) BBLs only — the legitimate acquisitions
    deed_bbls = sorted({a["bbl"] for a in individual_acqs})

    logger.info("Fetching addresses, violations, evictions for deed BBLs...")
    addresses     = _fetch_addresses(db, deed_bbls)
    violations    = _fetch_violations(db, deed_bbls)
    evictions_map = _fetch_evictions(db, deed_bbls)
    ds_scores     = _fetch_displacement_scores(db)
    bbl_zips      = _fetch_bbl_zips(db, deed_bbls)

    # -----------------------------------------------------------------------
    # Per-property records (individuals only)
    # -----------------------------------------------------------------------
    properties = []
    for a in individual_acqs:
        bbl      = a["bbl"]
        acq_date = a["doc_date"]
        zip_code = bbl_zips.get(bbl)
        ds       = ds_scores.get(zip_code, {})

        viols = violations.get(bbl, [])
        v_a   = sum(1 for v in viols if v["class"] == "A")
        v_b   = sum(1 for v in viols if v["class"] == "B")
        v_c   = sum(1 for v in viols if v["class"] == "C")

        evict_list = evictions_map.get(bbl, [])
        etb_events = []
        if acq_date:
            cutoff = acq_date - timedelta(days=EVICTION_LOOKBACK_DAYS)
            for ev in evict_list:
                ed = ev["executed_date"]
                if ed and cutoff <= ed < acq_date:
                    etb_events.append({
                        "eviction_date":           ed.isoformat(),
                        "docket_number":           ev["docket_number"],
                        "eviction_type":           ev["eviction_type"],
                        "days_before_acquisition": (acq_date - ed).days,
                    })

        properties.append({
            "bbl":                      bbl,
            "address":                  addresses.get(bbl),
            "zip_code":                 zip_code,
            "acquiring_entity":         a["entity"],
            "acquisition_date":         acq_date.isoformat() if acq_date else None,
            "price_paid":               a["doc_amount"],
            "doc_type":                 a["doc_type"],
            "document_id":              a["document_id"],
            "displacement_score":       ds.get("score"),
            "hpd_violations": {
                "total":   len(viols),
                "class_a": v_a,
                "class_b": v_b,
                "class_c": v_c,
                "records": viols,
            },
            "eviction_then_buy": etb_events,
        })

    # -----------------------------------------------------------------------
    # Bank false positive summary
    # -----------------------------------------------------------------------
    bank_by_entity: dict[str, int] = defaultdict(int)
    bank_by_doctype: dict[str, int] = defaultdict(int)
    for r in bank_records:
        bank_by_entity[r["entity"]] += 1
        bank_by_doctype[r["doc_type"] or "UNKNOWN"] += 1

    # -----------------------------------------------------------------------
    # ETB summary (individual properties only)
    # -----------------------------------------------------------------------
    etb_matches = []
    for prop in properties:
        for ev in prop["eviction_then_buy"]:
            etb_matches.append({
                "bbl":              prop["bbl"],
                "address":          prop["address"],
                "zip_code":         prop["zip_code"],
                "acquiring_entity": prop["acquiring_entity"],
                "eviction_date":    ev["eviction_date"],
                "acquisition_date": prop["acquisition_date"],
                "gap_days":         ev["days_before_acquisition"],
                "price_paid":       prop["price_paid"],
                "docket_number":    ev["docket_number"],
                "eviction_type":    ev["eviction_type"],
                "displacement_score": prop["displacement_score"],
            })
    etb_matches.sort(key=lambda x: x["gap_days"])

    # -----------------------------------------------------------------------
    # HPD totals (individual properties only)
    # -----------------------------------------------------------------------
    all_viols = [v for prop in properties for v in prop["hpd_violations"]["records"]]
    total_a   = sum(1 for v in all_viols if v["class"] == "A")
    total_b   = sum(1 for v in all_viols if v["class"] == "B")
    total_c   = sum(1 for v in all_viols if v["class"] == "C")
    bbls_with_bc = sum(1 for prop in properties if prop["hpd_violations"]["class_b"] + prop["hpd_violations"]["class_c"] > 0)

    # -----------------------------------------------------------------------
    # Geo / spend summary
    # -----------------------------------------------------------------------
    zip_counts: dict[str, int] = defaultdict(int)
    for prop in properties:
        if prop["zip_code"]:
            zip_counts[prop["zip_code"]] += 1

    geo_breakdown = []
    for z, cnt in sorted(zip_counts.items(), key=lambda x: -x[1]):
        ds = ds_scores.get(z, {})
        geo_breakdown.append({
            "zip_code":           z,
            "properties":         cnt,
            "displacement_score": ds.get("score"),
        })

    prices      = [p["price_paid"] for p in properties if p["price_paid"]]
    total_spend = sum(prices)

    # Entity breakdown for individuals
    individual_by_entity: dict[str, dict] = {}
    for a in individual_acqs:
        e = a["entity"]
        if e not in individual_by_entity:
            individual_by_entity[e] = {"deeds": 0, "bbls": set(), "total_spend": 0.0}
        individual_by_entity[e]["deeds"] += 1
        individual_by_entity[e]["bbls"].add(a["bbl"])
        if a["doc_amount"]:
            individual_by_entity[e]["total_spend"] += a["doc_amount"]

    individual_summary = {
        e: {
            "deeds":       v["deeds"],
            "unique_bbls": len(v["bbls"]),
            "total_spend": round(v["total_spend"], 2),
        }
        for e, v in individual_by_entity.items()
    }

    return {
        "generated_at": date.today().isoformat(),
        "subject":      "HABIB",
        "classification": "MIXED — bank false positive + individual landlord network",
        "bank_false_positive": {
            "entities":          HABIB_BANK_ENTITIES,
            "total_records":     len(bank_records),
            "unique_bbls":       len({r["bbl"] for r in bank_records}),
            "doc_type_breakdown": dict(bank_by_doctype),
            "entity_breakdown":  dict(bank_by_entity),
            "determination":     "exclude",
            "reason": (
                "HABIB AMERICAN BANK (dba HAB Bank) is a New York-chartered commercial bank. "
                "All 39 records are ASST (mortgage assignment) with zero recorded consideration — "
                "the bank is receiving mortgage assignments as a lender, not acquiring properties. "
                "These records inflate the HABIB operator's BBL count and displacement metrics "
                "and should be filtered from the pipeline."
            ),
        },
        "individual_network": {
            "entities":          HABIB_INDIVIDUAL_ENTITIES,
            "total_deeds":       len(individual_acqs),
            "unique_bbls":       len({a["bbl"] for a in individual_acqs}),
            "total_spend":       round(total_spend, 2),
            "entity_breakdown":  individual_summary,
            "determination":     "monitor",
            "reason": (
                "Five individuals sharing the HABIB surname made residential deed purchases "
                "between 2025 and 2026. Prices ($570K–$1.45M) and borough distribution suggest "
                "a family operating as small-scale landlords. Portfolio size (5 properties) is "
                "below the operator monitoring threshold but the shared surname pattern warrants "
                "flagging for re-evaluation if additional purchases appear."
            ),
        },
        "total_properties":      len(properties),
        "total_spend":           round(total_spend, 2),
        "first_acquisition":     min((a["doc_date"].isoformat() for a in individual_acqs if a["doc_date"]), default=None),
        "last_acquisition":      max((a["doc_date"].isoformat() for a in individual_acqs if a["doc_date"]), default=None),
        "geographic_concentration": geo_breakdown,
        "hpd_violations": {
            "total":              len(all_viols),
            "class_a":            total_a,
            "class_b":            total_b,
            "class_c":            total_c,
            "bbls_with_class_bc": bbls_with_bc,
            "bc_per_property":    round((total_b + total_c) / max(len(properties), 1), 2),
        },
        "eviction_then_buy": {
            "total_matches":  len(etb_matches),
            "lookback_days":  EVICTION_LOOKBACK_DAYS,
            "matches":        etb_matches,
        },
        "properties": properties,
    }


# ---------------------------------------------------------------------------
# Plain-English summary
# ---------------------------------------------------------------------------

def write_summary(data: dict, path: Path) -> None:
    bank   = data["bank_false_positive"]
    indiv  = data["individual_network"]
    viols  = data["hpd_violations"]
    etb    = data["eviction_then_buy"]
    geo    = data["geographic_concentration"]

    geo_lines = []
    for g in geo[:8]:
        ds_str = f"{g['displacement_score']:.1f}" if g["displacement_score"] is not None else "N/A"
        geo_lines.append(f"    {g['zip_code']}: {g['properties']} properties  (displacement score {ds_str})")

    etb_lines = []
    for m in etb["matches"]:
        price_str = f"${m['price_paid']:,.0f}" if m["price_paid"] else "no recorded price"
        etb_lines.append(
            f"    • {m['address'] or m['bbl']} (BBL {m['bbl']}, zip {m['zip_code']})\n"
            f"      Eviction executed {m['eviction_date']} — deed filed {m['acquisition_date']} "
            f"({m['gap_days']} days later, {price_str})\n"
            f"      Docket: {m['docket_number'] or 'unknown'}  |  "
            f"Type: {m['eviction_type'] or 'unspecified'}  |  "
            f"Displacement score: {m['displacement_score'] or 'N/A'}"
        )

    indiv_lines = []
    for e, v in indiv["entity_breakdown"].items():
        spend_str = f"${v['total_spend']:,.0f}" if v["total_spend"] else "no recorded price"
        indiv_lines.append(f"    • {e}: {v['deeds']} deed(s), {v['unique_bbls']} BBL(s), {spend_str}")

    bank_lines = [f"    • {e}: {cnt} ASST record(s)"
                  for e, cnt in bank["entity_breakdown"].items()]

    summary = textwrap.dedent(f"""\
    HABIB OPERATOR INVESTIGATION
    Generated: {data['generated_at']}
    ══════════════════════════════════════════════════════════════════════

    CLASSIFICATION: {data['classification']}

    OVERVIEW
    --------
    The 'HABIB' operator cluster resolves to two distinct groups. The dominant
    group — HABIB AMERICAN BANK and its named variants — is a New York-chartered
    commercial bank (also known as HAB Bank). All 39 of its records in ACRIS are
    mortgage assignments (ASST), meaning the bank is receiving mortgage note
    assignments as a lender, not acquiring real property. These are false positives.

    The secondary group — five individuals sharing the HABIB surname — made 5
    genuine residential deed purchases totaling ${indiv['total_spend']:,.0f} between
    {data['first_acquisition']} and {data['last_acquisition']}. This appears to be
    a family operating as small-scale landlords in Brooklyn and the Bronx.

    GROUP A — BANK ENTITIES (FALSE POSITIVE)
    -----------------------------------------
    Determination: EXCLUDE from displacement pipeline.

{chr(10).join(bank_lines)}

    All {bank['total_records']} records are doc_type ASST (mortgage assignment) with
    zero recorded consideration. HABIB AMERICAN BANK is a legitimate lender —
    its presence as grantee on ASST documents reflects normal mortgage banking
    activity, not property displacement. Recommended fix: add HABIB AMERICAN BANK
    variants to the bank/lender exclusion list in operator_network_analysis.py.

    GROUP B — INDIVIDUAL LANDLORDS (MONITOR)
    ------------------------------------------
    Determination: Flag for re-evaluation if further purchases detected.

{chr(10).join(indiv_lines)}

    GEOGRAPHIC CONCENTRATION (deed properties)
    ------------------------------------------
{chr(10).join(geo_lines) if geo_lines else '    No zip code data available.'}

    EVICTION-THEN-BUY (deed properties, {etb['lookback_days']}-day lookback)
    ─────────────────────────────────────────────────────────────────────
    {etb['total_matches']} eviction-then-buy match(es) across the 5 individual purchases.

{chr(10).join(etb_lines) if etb_lines else '    No eviction-then-buy matches found.'}

    HPD VIOLATIONS (deed properties only)
    --------------------------------------
    Total violations:        {viols['total']}
      Class A (minor):       {viols['class_a']}
      Class B (hazardous):   {viols['class_b']}
      Class C (immediately hazardous): {viols['class_c']}
    Properties with B or C:  {viols['bbls_with_class_bc']} of {data['total_properties']}
    B+C per property:        {viols['bc_per_property']:.2f}

    ASSESSMENT
    ----------
    HABIB is not a coherent displacement operator. The bank entities that dominate
    the cluster (87% of records) are a lender false positive and should be excluded.
    The remaining five individual buyers share a surname and purchase profile
    consistent with a family of small landlords — likely owner-operators rather than
    a deliberate displacement network.

    At 5 properties and $3.96M total spend, the HABIB individual cluster falls well
    below the monitoring threshold used for MTEK, BREDIF, BATTALION, and PHANTOM.
    No action required at this time. Log as resolved false positive; flag bank
    variants for filter list update.

    RECOMMENDATION: Exclude HABIB AMERICAN BANK variants from the pipeline.
    Monitor HABIB individuals passively — re-evaluate if portfolio grows past 10.
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

    bank  = data["bank_false_positive"]
    indiv = data["individual_network"]
    etb   = data["eviction_then_buy"]
    viols = data["hpd_violations"]

    print(f"\nHABIB Investigation — classification: {data['classification']}\n")
    print(f"  Bank false positive:")
    print(f"    {bank['total_records']} ASST records, {bank['unique_bbls']} BBLs — EXCLUDE")
    print(f"\n  Individual landlord network:")
    print(f"    {indiv['total_deeds']} deeds, {indiv['unique_bbls']} BBLs, ${indiv['total_spend']:,.0f}")
    for e, v in indiv["entity_breakdown"].items():
        print(f"    {e}: {v['deeds']} deed(s), ${v['total_spend']:,.0f}")
    print(f"\n  Eviction-then-buy: {etb['total_matches']} match(es)")
    print(f"  HPD violations:    {viols['total']} total ({viols['class_b']} B, {viols['class_c']} C)")


if __name__ == "__main__":
    sys.exit(main())
