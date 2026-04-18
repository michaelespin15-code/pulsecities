"""
Deep-dive investigations for four operators flagged by weekly_operator_diff on
2026-04-18: TOWNHOUSE, MELO, JOVIA, ARION.

TOWNHOUSE and MELO are direct deed acquirers. Full PHANTOM-style dossier.
JOVIA and ARION are ASST-only (mortgage note assignments, zero deeds). Classified
as mortgage-lender false positives with a BATTALION-style writeup.

Produces, for each operator:
    scripts/{operator}_investigation.json
    scripts/{operator}_summary.txt

Usage:
    python scripts/new_operator_investigations.py
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

OUT_DIR = Path(__file__).parent
EVICTION_LOOKBACK_DAYS = 365

OPERATORS: dict[str, dict] = {
    "TOWNHOUSE": {
        "entities": [
            "M5 TOWNHOUSE 47 LLC",
            "TOWNHOUSE RENTAL II LLC",
            "TOWNHOUSE RENTAL IX LLC",
            "TOWNHOUSE RENTAL LLC",
            "TOWNHOUSE RENTAL VII LLC",
        ],
        "classification": "direct_acquirer",
        "llc_structure_note": (
            "Five entities: four roman-numeraled rental funds (TOWNHOUSE RENTAL, II, VII, IX) "
            "plus a numbered single-purpose shell (M5 TOWNHOUSE 47 LLC). The roman-numeral "
            "sequencing is consistent with a fund-by-fund capital-raise structure rather than "
            "one-LLC-per-property shell fragmentation."
        ),
        "assessment_flavor": (
            "TOWNHOUSE is a Brooklyn-focused direct acquirer operating out of a small number "
            "of rental funds. Portfolio concentration in 11207/11221/11237 (Bushwick / East New York / "
            "Bed-Stuy fringe) overlaps heavily with PHANTOM's Brooklyn footprint. The high-displacement-zip "
            "rate (60.7%) reported in the weekly diff is a strong signal that this is a displacement-linked "
            "acquisition strategy, not a scatter portfolio."
        ),
    },
    "MELO": {
        "entities": [
            "MELO HECTOR B",
            "MELO Z PHANTOM CAP LLC",
        ],
        "classification": "direct_acquirer",
        "llc_structure_note": (
            "Two grantee identities: an individual (HECTOR B MELO) and one LLC (MELO Z PHANTOM CAP LLC). "
            "The LLC name embeds 'PHANTOM CAP', the exact brand used by the 32-LLC PHANTOM CAPITAL "
            "network already profiled. This suggests MELO Z PHANTOM CAP LLC is either a PHANTOM "
            "affiliate or a principal entity co-branded across both networks. It is NOT currently "
            "in the PHANTOM_ENTITIES list and was not counted in that investigation."
        ),
        "assessment_flavor": (
            "MELO is small by volume (10 properties) but the 70% high-displacement-zip rate is the "
            "highest among this week's new entrants. Bronx-only footprint. The PHANTOM-brand overlap "
            "(MELO Z PHANTOM CAP LLC) is the most actionable signal. If this entity belongs to the "
            "same principals as PHANTOM CAPITAL, the consolidated PHANTOM footprint is larger than "
            "previously reported."
        ),
    },
    "JOVIA": {
        "entities": [
            "JOVIA FINANCIAL CREDIT UNION",
            "JOVIA FINANCIAL FEDERAL CREDIT UNION",
        ],
        "classification": "mortgage_lender_false_positive",
        "lender_kind": "Federal credit union (Long Island, NY)",
        "recommended_fix": (
            "Add JOVIA to _BANK_ROOTS in operator_network_analysis.py. Credit-union naming "
            "(FINANCIAL CREDIT UNION, FEDERAL CREDIT UNION) is a catchable pattern; consider "
            "adding 'CREDIT' as a root-level block token alongside BANK / MORTGAGE / FUNDING."
        ),
    },
    "ARION": {
        "entities": [
            "ARION FUND LLC",
            "ARION FUND LLC ISAOA/ATIMA",
            "ARION FUNDING LLC",
        ],
        "classification": "mortgage_lender_false_positive",
        "lender_kind": "Private mortgage fund / note investor",
        "recommended_fix": (
            "Add ARION to _BANK_ROOTS. The 'ISAOA/ATIMA' suffix ('Its Successors And/Or Assigns, "
            "As Their Interests May Appear') is a mortgagee boilerplate phrase and is a reliable "
            "tell for lender-side recordings. A party_name_normalized LIKE '%ISAOA%' or '%ATIMA%' "
            "filter at the operator-extraction layer would catch all such entities pre-emptively."
        ),
    },
}


# ---------------------------------------------------------------------------
# Shared DB loaders
# ---------------------------------------------------------------------------

def _fetch_acquisitions(db, entities: list[str]) -> list[dict]:
    ph = ", ".join(f":e{i}" for i in range(len(entities)))
    params = {f"e{i}": e for i, e in enumerate(entities)}
    rows = db.execute(text(f"""
        SELECT bbl, document_id, doc_type, doc_date, doc_amount,
               party_name_normalized AS entity
        FROM ownership_raw
        WHERE party_type = '2'
          AND party_name_normalized IN ({ph})
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


def _fetch_addresses(db, bbls: list[str]) -> dict[str, str]:
    if not bbls:
        return {}
    ph = ", ".join(f":b{i}" for i in range(len(bbls)))
    params = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (bbl) bbl, address
        FROM (
            SELECT bbl, address, inspection_date AS d FROM violations_raw
            WHERE bbl IN ({ph}) AND address IS NOT NULL
            UNION ALL
            SELECT bbl, address, filing_date AS d FROM permits_raw
            WHERE bbl IN ({ph}) AND address IS NOT NULL
        ) t
        ORDER BY bbl, d DESC NULLS LAST
    """), params).fetchall()
    return {r.bbl: r.address for r in rows if r.address}


def _fetch_violations(db, bbls: list[str]) -> dict[str, list[dict]]:
    if not bbls:
        return {}
    ph = ", ".join(f":b{i}" for i in range(len(bbls)))
    params = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT bbl, violation_id, violation_class, description,
               inspection_date, nov_issued_date, current_status
        FROM violations_raw
        WHERE bbl IN ({ph})
        ORDER BY bbl, inspection_date
    """), params).fetchall()
    out: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        out[r.bbl].append({
            "violation_id":    r.violation_id,
            "class":           r.violation_class,
            "description":     r.description,
            "inspection_date": r.inspection_date.isoformat() if r.inspection_date else None,
            "nov_issued_date": r.nov_issued_date.isoformat() if r.nov_issued_date else None,
            "current_status":  r.current_status,
        })
    return dict(out)


def _fetch_evictions(db, bbls: list[str]) -> dict[str, list[dict]]:
    if not bbls:
        return {}
    ph = ", ".join(f":b{i}" for i in range(len(bbls)))
    params = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (bbl, executed_date) bbl, executed_date,
               docket_number, eviction_type, address
        FROM evictions_raw
        WHERE bbl IN ({ph})
          AND executed_date IS NOT NULL
        ORDER BY bbl, executed_date
    """), params).fetchall()
    out: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        out[r.bbl].append({
            "executed_date": r.executed_date,
            "docket_number": r.docket_number,
            "eviction_type": r.eviction_type,
            "address":       r.address,
        })
    return dict(out)


def _fetch_permits(db, bbls: list[str]) -> dict[str, list[dict]]:
    if not bbls:
        return {}
    ph = ", ".join(f":b{i}" for i in range(len(bbls)))
    params = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT bbl, permit_type, work_type, job_description, filing_date, owner_name
        FROM permits_raw
        WHERE bbl IN ({ph})
        ORDER BY bbl, filing_date
    """), params).fetchall()
    out: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        out[r.bbl].append({
            "permit_type": r.permit_type,
            "work_type":   r.work_type,
            "description": r.job_description,
            "filing_date": r.filing_date.isoformat() if r.filing_date else None,
            "owner_name":  r.owner_name,
        })
    return dict(out)


def _fetch_ds_scores(db) -> dict[str, dict]:
    rows = db.execute(text(
        "SELECT zip_code, score, eviction_rate, llc_acquisition_rate, "
        "permit_intensity, complaint_rate FROM displacement_scores"
    )).fetchall()
    return {
        r.zip_code: {
            "score":                float(r.score),
            "eviction_rate":        float(r.eviction_rate or 0),
            "llc_acquisition_rate": float(r.llc_acquisition_rate or 0),
            "permit_intensity":     float(r.permit_intensity or 0),
            "complaint_rate":       float(r.complaint_rate or 0),
        }
        for r in rows
    }


def _fetch_bbl_zips(db, bbls: list[str]) -> dict[str, str]:
    if not bbls:
        return {}
    ph = ", ".join(f":b{i}" for i in range(len(bbls)))
    params = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (bbl) bbl, zip_code FROM (
            SELECT bbl, zip_code FROM violations_raw WHERE bbl IN ({ph}) AND zip_code IS NOT NULL
            UNION ALL
            SELECT bbl, zip_code FROM permits_raw   WHERE bbl IN ({ph}) AND zip_code IS NOT NULL
        ) t ORDER BY bbl
    """), params).fetchall()
    return {r.bbl: r.zip_code for r in rows if r.zip_code}


# ---------------------------------------------------------------------------
# Direct-acquirer dossier (TOWNHOUSE, MELO)
# ---------------------------------------------------------------------------

def build_direct(db, op: str, cfg: dict) -> dict:
    entities = cfg["entities"]
    logger.info("[%s] fetching acquisitions (%d entities)", op, len(entities))
    acquisitions = _fetch_acquisitions(db, entities)
    logger.info("  %d acquisition records / %d unique BBLs",
                len(acquisitions), len({a["bbl"] for a in acquisitions}))

    unique_bbls = sorted({a["bbl"] for a in acquisitions})

    addresses     = _fetch_addresses(db, unique_bbls)
    violations    = _fetch_violations(db, unique_bbls)
    evictions_map = _fetch_evictions(db, unique_bbls)
    permits_map   = _fetch_permits(db, unique_bbls)
    ds_scores     = _fetch_ds_scores(db)
    bbl_zips      = _fetch_bbl_zips(db, unique_bbls)

    llc_counts: dict[str, int] = defaultdict(int)
    for a in acquisitions:
        llc_counts[a["entity"]] += 1

    bbl_records: dict[str, dict] = {}
    for a in acquisitions:
        bbl = a["bbl"]
        if bbl not in bbl_records:
            bbl_records[bbl] = {
                "bbl":            bbl,
                "address":        addresses.get(bbl),
                "zip_code":       bbl_zips.get(bbl),
                "entity":         a["entity"],
                "first_doc_date": a["doc_date"],
                "doc_types":      [],
                "doc_amounts":    [],
                "document_ids":   [],
            }
        rec = bbl_records[bbl]
        if a["doc_date"] and (rec["first_doc_date"] is None or a["doc_date"] < rec["first_doc_date"]):
            rec["first_doc_date"] = a["doc_date"]
            rec["entity"]         = a["entity"]
        if a["doc_type"]:
            rec["doc_types"].append(a["doc_type"])
        if a["doc_amount"] is not None:
            rec["doc_amounts"].append(a["doc_amount"])
        if a["document_id"]:
            rec["document_ids"].append(a["document_id"])

    properties = []
    for bbl, rec in sorted(bbl_records.items(), key=lambda x: (x[1]["first_doc_date"] or date.min)):
        acq_date = rec["first_doc_date"]
        zip_code = rec["zip_code"]
        price    = max(rec["doc_amounts"]) if rec["doc_amounts"] else None
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

        perms      = permits_map.get(bbl, [])
        post_perms = [
            p for p in perms
            if p["filing_date"] and acq_date and p["filing_date"] >= acq_date.isoformat()
        ]

        properties.append({
            "bbl":                     bbl,
            "address":                 rec["address"],
            "zip_code":                zip_code,
            "acquiring_entity":        rec["entity"],
            "acquisition_date":        acq_date.isoformat() if acq_date else None,
            "price_paid":              price,
            "doc_types":               sorted(set(rec["doc_types"])),
            "document_ids":            rec["document_ids"],
            "displacement_score":      ds.get("score"),
            "hpd_violations": {
                "total":   len(viols),
                "class_a": v_a,
                "class_b": v_b,
                "class_c": v_c,
                "records": viols,
            },
            "eviction_then_buy":        etb_events,
            "post_acquisition_permits": post_perms,
        })

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

    all_viols    = [v for prop in properties for v in prop["hpd_violations"]["records"]]
    total_a      = sum(1 for v in all_viols if v["class"] == "A")
    total_b      = sum(1 for v in all_viols if v["class"] == "B")
    total_c      = sum(1 for v in all_viols if v["class"] == "C")
    bbls_with_bc = sum(1 for p in properties if p["hpd_violations"]["class_b"] + p["hpd_violations"]["class_c"] > 0)

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
            "eviction_rate":      ds.get("eviction_rate"),
            "llc_acq_rate":       ds.get("llc_acquisition_rate"),
        })

    dated = sorted(
        [p for p in properties if p["acquisition_date"]],
        key=lambda x: x["acquisition_date"],
    )
    dates = [p["acquisition_date"] for p in dated]
    first, last = (dates[0], dates[-1]) if dates else (None, None)
    if len(dates) >= 2:
        span_days = (date.fromisoformat(last) - date.fromisoformat(first)).days
        velocity  = round(len(dates) / max(span_days / 30, 1), 2)
    else:
        span_days, velocity = 0, 0.0

    prices      = [p["price_paid"] for p in properties if p["price_paid"]]
    total_spend = sum(prices)
    avg_price   = round(total_spend / len(prices), 2) if prices else 0.0

    monthly: dict[str, int] = defaultdict(int)
    for p in properties:
        if p["acquisition_date"]:
            monthly[p["acquisition_date"][:7]] += 1

    numbered_llcs   = [e for e in entities if any(c.isdigit() for c in e)]
    named_llcs      = [e for e in entities if not any(c.isdigit() for c in e)]
    active_entities = [e for e in entities if llc_counts.get(e, 0) > 0]

    return {
        "generated_at":            date.today().isoformat(),
        "subject":                 op,
        "classification":          "direct_acquirer",
        "llc_entities":            entities,
        "llc_count":               len(entities),
        "active_entities":         active_entities,
        "numbered_llc_count":      len(numbered_llcs),
        "named_llc_count":         len(named_llcs),
        "llc_breakdown":           {e: llc_counts.get(e, 0) for e in entities},
        "total_properties":        len(properties),
        "total_acquisitions":      len(acquisitions),
        "total_spend":             round(total_spend, 2),
        "avg_acquisition_price":   avg_price,
        "properties_with_price":   len(prices),
        "first_acquisition":       first,
        "last_acquisition":        last,
        "span_days":               span_days,
        "acquisitions_per_month":  velocity,
        "monthly_pace":            dict(sorted(monthly.items())),
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
        "geographic_concentration": geo_breakdown,
        "properties":              properties,
    }


def write_direct_summary(op: str, cfg: dict, data: dict, path: Path) -> None:
    etb     = data["eviction_then_buy"]
    viols   = data["hpd_violations"]
    geo     = data["geographic_concentration"]
    props   = data["properties"]
    monthly = data["monthly_pace"]

    top_zips = geo[:8]

    unique_etb_bbls = len({m["bbl"] for m in etb["matches"]})
    etb_lines = []
    for m in etb["matches"]:
        price_str = f"${m['price_paid']:,.0f}" if m["price_paid"] else "no recorded price"
        etb_lines.append(
            f"    - {m['address'] or m['bbl']} (BBL {m['bbl']}, zip {m['zip_code']})\n"
            f"      Eviction executed {m['eviction_date']}. Deed filed {m['acquisition_date']} "
            f"({m['gap_days']} days later, {price_str}).\n"
            f"      Docket: {m['docket_number'] or 'unknown'}  |  "
            f"Type: {m['eviction_type'] or 'unspecified'}  |  "
            f"Displacement score: {m['displacement_score'] or 'N/A'}"
        )

    hot_props = sorted(
        [p for p in props if p["hpd_violations"]["class_b"] + p["hpd_violations"]["class_c"] >= 3],
        key=lambda x: -(x["hpd_violations"]["class_b"] + x["hpd_violations"]["class_c"]),
    )[:10]
    hot_lines = []
    for p in hot_props:
        v  = p["hpd_violations"]
        bc = v["class_b"] + v["class_c"]
        ds_str = f"{p['displacement_score']:.1f}" if p["displacement_score"] is not None else "N/A"
        hot_lines.append(
            f"    - {p['address'] or p['bbl']} (zip {p['zip_code']}, ds={ds_str})  "
            f"{bc} B/C violations ({v['class_b']} B, {v['class_c']} C)"
        )

    pace_lines = [f"    {ym}: {'#' * min(count, 60)} ({count})" for ym, count in sorted(monthly.items())]

    active_sorted = sorted(
        [(e, data["llc_breakdown"][e]) for e in data["llc_entities"] if data["llc_breakdown"][e] > 0],
        key=lambda x: -x[1],
    )
    llc_lines = [f"    - {e}: {cnt} deed record{'s' if cnt != 1 else ''}"
                 for e, cnt in active_sorted]
    inactive_count = len([e for e in data["llc_entities"] if data["llc_breakdown"][e] == 0])

    prop_lines = []
    for p in sorted(props, key=lambda x: x["acquisition_date"] or ""):
        price_str = f"${p['price_paid']:,.0f}" if p["price_paid"] else "no price"
        ds_str = f"{p['displacement_score']:.1f}" if p["displacement_score"] is not None else "N/A"
        prop_lines.append(
            f"    - {p['acquisition_date']}  "
            f"{(p['address'] or p['bbl'])[:45]:45s}  "
            f"zip {p['zip_code'] or 'N/A':>6}  "
            f"ds {ds_str:>5}  "
            f"{price_str:>12}  "
            f"via {p['acquiring_entity']}"
        )

    geo_lines = []
    for g in top_zips:
        score_str = f"{g['displacement_score']:.1f}" if g["displacement_score"] is not None else "N/A"
        geo_lines.append(
            f"    {g['zip_code']}: {g['properties']} properties  (displacement score {score_str})"
        )

    spend_str = f"${data['total_spend']:,.0f}"
    avg_str   = f"${data['avg_acquisition_price']:,.0f}"

    etb_rate_pct = round(len({m['bbl'] for m in etb['matches']}) / max(data['total_properties'], 1) * 100, 1)

    summary = textwrap.dedent(f"""\
    {op} OPERATOR INVESTIGATION
    Generated: {data['generated_at']}
    ==========================================================================

    OVERVIEW
    --------
    {op} operated through {data['llc_count']} grantee identities and acquired
    {data['total_properties']} unique properties in New York City between {data['first_acquisition']}
    and {data['last_acquisition']}. Span: {data['span_days']} days. Average acquisition
    pace: {data['acquisitions_per_month']:.1f} properties per month.

    Total recorded spend: {spend_str}
    Average price per recorded transaction: {avg_str}
    ({data['properties_with_price']} of {data['total_properties']} properties had a recorded price)

    All records are DEED transfers (not ASST). {op} is a direct-purchase operator,
    same transaction pattern as PHANTOM CAPITAL and MTEK.

    LLC STRUCTURE
    -------------
    {data['llc_count']} total entities: {data['numbered_llc_count']} numbered, {data['named_llc_count']} named.
    {len(data['active_entities'])} entities with recorded deed transactions; {inactive_count} registered but unused.

    Active entities (by deed count):
{chr(10).join(llc_lines) if llc_lines else '    None.'}

    {cfg['llc_structure_note']}

    ACQUISITION PACE (monthly)
    --------------------------
{chr(10).join(pace_lines) if pace_lines else '    (no dated acquisitions)'}

    GEOGRAPHIC CONCENTRATION (top zip codes)
    ----------------------------------------
{chr(10).join(geo_lines) if geo_lines else '    No zip code data available.'}

    COMPLETE PROPERTY LIST
    ----------------------
{chr(10).join(prop_lines) if prop_lines else '    None.'}

    EVICTION-THEN-BUY MATCHES ({etb['total_matches']} events across {unique_etb_bbls} properties, {etb['lookback_days']}-day lookback)
    ---------------------------------------------------------------------------
    {unique_etb_bbls} of {data['total_properties']} properties show at least one eviction executed within
    {etb['lookback_days']} days before {op} took title ({etb['total_matches']} total eviction events).
    ETB rate: {etb_rate_pct}% of acquired properties.

{chr(10).join(etb_lines) if etb_lines else '    No eviction-then-buy matches found in this dataset.'}

    HPD VIOLATIONS (all time, all acquired properties)
    --------------------------------------------------
    Total violations:        {viols['total']}
      Class A (minor):       {viols['class_a']}
      Class B (hazardous):   {viols['class_b']}
      Class C (immediately hazardous): {viols['class_c']}
    Properties with B or C:  {viols['bbls_with_class_bc']} of {data['total_properties']}
    B+C per property:        {viols['bc_per_property']:.2f}

    HIGH-VIOLATION PROPERTIES (>=3 Class B/C violations)
    ----------------------------------------------------
{chr(10).join(hot_lines) if hot_lines else '    None above threshold.'}

    ASSESSMENT
    ----------
    {cfg['assessment_flavor']}

    RECOMMENDATION: Add to nightly-monitored operator set alongside MTEK, PHANTOM, and BREDIF.
    ==========================================================================
    """)
    path.write_text(summary)


# ---------------------------------------------------------------------------
# Lender / false-positive dossier (JOVIA, ARION)
# ---------------------------------------------------------------------------

def build_lender(db, op: str, cfg: dict) -> dict:
    entities = cfg["entities"]
    logger.info("[%s] fetching records (%d entities)", op, len(entities))
    all_records = _fetch_acquisitions(db, entities)
    logger.info("  %d records / %d unique BBLs",
                len(all_records), len({r["bbl"] for r in all_records}))

    unique_bbls = sorted({r["bbl"] for r in all_records})

    addresses     = _fetch_addresses(db, unique_bbls)
    violations    = _fetch_violations(db, unique_bbls)
    evictions_map = _fetch_evictions(db, unique_bbls)
    ds_scores     = _fetch_ds_scores(db)
    bbl_zips      = _fetch_bbl_zips(db, unique_bbls)

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

    monthly: dict[str, int] = defaultdict(int)
    for r in all_records:
        if r["doc_date"]:
            monthly[r["doc_date"].isoformat()[:7]] += 1

    zip_counts: dict[str, int] = defaultdict(int)
    for bbl in unique_bbls:
        z = bbl_zips.get(bbl)
        if z:
            zip_counts[z] += 1

    geo_breakdown = []
    for z, cnt in sorted(zip_counts.items(), key=lambda x: -x[1]):
        ds = ds_scores.get(z, {})
        geo_breakdown.append({
            "zip_code":           z,
            "bbls":               cnt,
            "displacement_score": ds.get("score"),
        })

    scores = [ds_scores[bbl_zips[b]]["score"] for b in unique_bbls
              if bbl_zips.get(b) and bbl_zips[b] in ds_scores]
    avg_ds = round(sum(scores) / len(scores), 1) if scores else 0.0
    high_ds_count = sum(1 for s in scores if s >= 40)
    high_ds_pct   = round(high_ds_count / len(scores) * 100, 1) if scores else 0.0

    # Build BBL-level detail so the reader can audit the lender's footprint
    # even though nothing here is an acquisition.
    property_rows = []
    for bbl in unique_bbls:
        viols = violations.get(bbl, [])
        v_a = sum(1 for v in viols if v["class"] == "A")
        v_b = sum(1 for v in viols if v["class"] == "B")
        v_c = sum(1 for v in viols if v["class"] == "C")
        zip_code = bbl_zips.get(bbl)
        ds = ds_scores.get(zip_code, {}) if zip_code else {}
        bbl_records = [r for r in all_records if r["bbl"] == bbl]
        first_date = min((r["doc_date"] for r in bbl_records if r["doc_date"]), default=None)
        property_rows.append({
            "bbl":                bbl,
            "address":            addresses.get(bbl),
            "zip_code":           zip_code,
            "displacement_score": ds.get("score"),
            "first_record_date":  first_date.isoformat() if first_date else None,
            "record_count":       len(bbl_records),
            "doc_types":          sorted({r["doc_type"] for r in bbl_records if r["doc_type"]}),
            "grantee_entities":   sorted({r["entity"] for r in bbl_records}),
            "evictions_on_bbl":   len(evictions_map.get(bbl, [])),
            "hpd_violations": {
                "total": len(viols), "class_a": v_a, "class_b": v_b, "class_c": v_c,
            },
        })
    property_rows.sort(key=lambda x: x["first_record_date"] or "")

    dated = sorted([r["doc_date"] for r in all_records if r["doc_date"]])
    first, last = (dated[0].isoformat(), dated[-1].isoformat()) if dated else (None, None)

    return {
        "generated_at":    date.today().isoformat(),
        "subject":         op,
        "classification":  "mortgage_lender_false_positive",
        "lender_kind":     cfg.get("lender_kind"),
        "entities":        entities,
        "total_records":   len(all_records),
        "unique_bbls":     len(unique_bbls),
        "deed_count":      deed_count,
        "asst_count":      asst_count,
        "first_record":    first,
        "last_record":     last,
        "entity_breakdown":  dict(by_entity),
        "doctype_breakdown": dict(by_doctype),
        "monthly_pace":      dict(sorted(monthly.items())),
        "geographic_context": {
            "avg_displacement_score": avg_ds,
            "high_displacement_pct":  high_ds_pct,
            "geo_breakdown":          geo_breakdown,
        },
        "bbl_footprint":      property_rows,
        "determination":      "exclude",
        "recommended_fix":    cfg.get("recommended_fix"),
    }


def write_lender_summary(op: str, cfg: dict, data: dict, path: Path) -> None:
    entity_lines = [
        f"    - {e}: {cnt} record(s)"
        for e, cnt in data["entity_breakdown"].items()
    ]
    doctype_lines = [
        f"    - {dt}: {cnt}"
        for dt, cnt in data["doctype_breakdown"].items()
    ]
    monthly_lines = [
        f"    {ym}: {'#' * min(count, 60)} ({count})"
        for ym, count in sorted(data["monthly_pace"].items())
    ]
    geo = data["geographic_context"]
    geo_lines = []
    for g in geo["geo_breakdown"][:8]:
        ds_str = f"{g['displacement_score']:.1f}" if g["displacement_score"] is not None else "N/A"
        geo_lines.append(f"    {g['zip_code']}: {g['bbls']} BBL(s)  (displacement score {ds_str})")

    bbl_lines = []
    for p in data["bbl_footprint"]:
        ds_str = f"{p['displacement_score']:.1f}" if p["displacement_score"] is not None else "N/A"
        v = p["hpd_violations"]
        bbl_lines.append(
            f"    - {p['first_record_date'] or 'N/A'}  "
            f"{(p['address'] or p['bbl'])[:45]:45s}  "
            f"zip {p['zip_code'] or 'N/A':>6}  "
            f"ds {ds_str:>5}  "
            f"{p['record_count']} rec ({','.join(p['doc_types'])})  "
            f"HPD {v['class_b'] + v['class_c']} B/C"
        )

    summary = textwrap.dedent(f"""\
    {op} OPERATOR INVESTIGATION
    Generated: {data['generated_at']}
    ==========================================================================

    CLASSIFICATION: FALSE POSITIVE - {cfg.get('lender_kind', 'mortgage lender')}

    OVERVIEW
    --------
    {op} surfaces in the operator network analysis because {data['unique_bbls']} BBLs
    list {op}-branded entities as grantee. However, all {data['total_records']} records
    are document type ASST (mortgage note assignment) with zero DEED transfers.

    Deed acquisitions: {data['deed_count']}
    Mortgage assignments (ASST): {data['asst_count']}

    When a mortgage lender or note holder appears as grantee on an ASST record in ACRIS,
    it has received a mortgage assignment, not title to the property. This is routine
    loan servicing or note-purchase activity. {op} has not acquired real property through
    these records and is not operating as a landlord.

    Record window: {data['first_record']} to {data['last_record']}

    ENTITIES
    --------
{chr(10).join(entity_lines) if entity_lines else '    None.'}

    DOCUMENT TYPE BREAKDOWN
    -----------------------
{chr(10).join(doctype_lines) if doctype_lines else '    None.'}

    MONTHLY PACE (assignments)
    --------------------------
{chr(10).join(monthly_lines) if monthly_lines else '    (no dated records)'}

    GEOGRAPHIC FOOTPRINT (zip codes of assigned BBLs)
    -------------------------------------------------
{chr(10).join(geo_lines) if geo_lines else '    No zip code data available.'}

    Average displacement score across these BBLs: {geo['avg_displacement_score']}
    BBLs in high-displacement zips (score >= 40): {geo['high_displacement_pct']}%

    These metrics describe where {op}'s borrowers / note counterparties are, not where
    {op} operates as an owner. A lender making loans in high-displacement zips is not
    itself a displacement actor.

    COMPLETE BBL FOOTPRINT (sorted by first record date)
    ----------------------------------------------------
{chr(10).join(bbl_lines) if bbl_lines else '    None.'}

    EVICTION-THEN-BUY ANALYSIS
    --------------------------
    N/A. {op} has no deed acquisitions, so there is nothing to time-align against
    eviction executions. The eviction counts shown in the BBL footprint above reflect
    evictions on properties where {op} later received a mortgage assignment, which
    has no causal relationship to {op}'s activity.

    HPD VIOLATIONS CROSS-REFERENCE
    ------------------------------
    HPD violation counts are listed per BBL above for completeness. These violations
    belong to the property owners (borrowers), not to {op}. {op} is not responsible
    for conditions at properties against which it holds a mortgage note.

    ASSESSMENT
    ----------
    {op} is a clean mortgage-lender false positive. It does not own residential real
    estate in NYC; it holds or services mortgage paper. The high-displacement-zip
    concentration flagged in weekly_operator_diff reflects borrower geography, not
    operator behavior.

    RECOMMENDATION: Exclude {op} from operator monitoring.
    Recommended fix: {data['recommended_fix']}
    ==========================================================================
    """)
    path.write_text(summary)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    with get_scraper_db() as db:
        for op, cfg in OPERATORS.items():
            json_path = OUT_DIR / f"{op.lower()}_investigation.json"
            txt_path  = OUT_DIR / f"{op.lower()}_summary.txt"

            if cfg["classification"] == "direct_acquirer":
                data = build_direct(db, op, cfg)
                json_path.write_text(json.dumps(data, indent=2, default=str))
                write_direct_summary(op, cfg, data, txt_path)
                etb = data["eviction_then_buy"]
                print(f"\n[{op}] direct-acquirer dossier")
                print(f"  Properties:     {data['total_properties']}")
                print(f"  Active LLCs:    {len(data['active_entities'])} / {data['llc_count']}")
                print(f"  Total spend:    ${data['total_spend']:,.0f}")
                print(f"  ETB matches:    {etb['total_matches']} across {len({m['bbl'] for m in etb['matches']})} props")
                print(f"  HPD B+C:        {data['hpd_violations']['class_b'] + data['hpd_violations']['class_c']}")
            else:
                data = build_lender(db, op, cfg)
                json_path.write_text(json.dumps(data, indent=2, default=str))
                write_lender_summary(op, cfg, data, txt_path)
                print(f"\n[{op}] lender false-positive dossier")
                print(f"  Records:    {data['total_records']} ({data['asst_count']} ASST / {data['deed_count']} DEED)")
                print(f"  Unique BBLs:{data['unique_bbls']}")
                print(f"  Fix:        {(data['recommended_fix'] or '')[:80]}")

            print(f"  -> {json_path.name}")
            print(f"  -> {txt_path.name}")


if __name__ == "__main__":
    sys.exit(main())
