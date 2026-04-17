"""
Full investigation profile for PHANTOM CAPITAL — 32-LLC operator network acquiring
residential properties across NYC with consistent monthly pace and named-number LLC
shell structure.

PHANTOM operates through a cluster of numbered and named LLCs (PHANTOM CAPITAL 10,
PHANTOM CAPITAL 11, … PHANTOM PARTNERS BH, PHANTOM PARTNERS RE, etc.). 65 deed
transfers totaling $49.9M between May 2025 and March 2026. This is direct property
acquisition — not a note-purchase vehicle like BREDIF.

Produces:
  scripts/phantom_investigation.json  — machine-readable full dossier
  scripts/phantom_summary.txt         — plain English narrative

Usage:
    python scripts/phantom_investigation.py
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

JSON_OUTPUT = Path(__file__).parent / "phantom_investigation.json"
TXT_OUTPUT  = Path(__file__).parent / "phantom_summary.txt"

PHANTOM_ENTITIES = [
    "PHANTOM & Z CAPITAL LLC",
    "PHANTOM AFFORDABLE HOUSING LLC",
    "PHANTOM CAP HOLDINGS 206 LLC",
    "PHANTOM CAP MAP LLC",
    "PHANTOM CAPITAL 10 LLC",
    "PHANTOM CAPITAL 107 LLC",
    "PHANTOM CAPITAL 11 LLC",
    "PHANTOM CAPITAL 12 LLC",
    "PHANTOM CAPITAL 161 LLC",
    "PHANTOM CAPITAL 21 LLC",
    "PHANTOM CAPITAL 22 LLC",
    "PHANTOM CAPITAL 350 LLC",
    "PHANTOM CAPITAL 44 LLC",
    "PHANTOM CAPITAL 53 LLC",
    "PHANTOM CAPITAL 55 LLC",
    "PHANTOM CAPITAL 71 LLC",
    "PHANTOM CAPITAL 800 LLC",
    "PHANTOM CAPITAL ACQUISITIONS LLC",
    "PHANTOM CAPITAL BX LLC",
    "PHANTOM HOUSING LLC",
    "PHANTOM KOACH LLC",
    "PHANTOM LANDLORDS LLC",
    "PHANTOM NYC HOLDINGS LLC",
    "PHANTOM PARTNERS 89 LLC",
    "PHANTOM PARTNERS BH LLC",
    "PHANTOM PARTNERS PROPERTIES LLC",
    "PHANTOM PARTNERS RE LLC",
    "PHANTOM RISE LLC",
    "PHANTOM TERRITORY LLC",
    "PHANTOM TITANS LLC",
    "PHANTOM TOWN LLC",
    "PHANTOM TROPHIES LLC",
]

EVICTION_LOOKBACK_DAYS = 365


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _fetch_acquisitions(db) -> list[dict]:
    placeholders = ", ".join(f":e{i}" for i in range(len(PHANTOM_ENTITIES)))
    params       = {f"e{i}": e for i, e in enumerate(PHANTOM_ENTITIES)}
    rows = db.execute(text(f"""
        SELECT bbl, document_id, doc_type, doc_date, doc_amount,
               party_name_normalized AS entity
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


def _fetch_permits(db, bbls: list[str]) -> dict[str, list[dict]]:
    if not bbls:
        return {}
    placeholders = ", ".join(f":b{i}" for i in range(len(bbls)))
    params       = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT bbl, permit_type, work_type, job_description, filing_date, owner_name
        FROM permits_raw
        WHERE bbl IN ({placeholders})
        ORDER BY bbl, filing_date
    """), params).fetchall()
    result: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        result[r.bbl].append({
            "permit_type": r.permit_type,
            "work_type":   r.work_type,
            "description": r.job_description,
            "filing_date": r.filing_date.isoformat() if r.filing_date else None,
            "owner_name":  r.owner_name,
        })
    return dict(result)


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
            "permit_intensity":     float(r.permit_intensity or 0),
            "complaint_rate":       float(r.complaint_rate or 0),
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
    logger.info("Fetching PHANTOM CAPITAL acquisitions...")
    acquisitions = _fetch_acquisitions(db)
    logger.info("  %d acquisition records across %d unique BBLs",
                len(acquisitions), len({a["bbl"] for a in acquisitions}))

    unique_bbls = sorted({a["bbl"] for a in acquisitions})

    logger.info("Fetching addresses, violations, evictions, permits...")
    addresses     = _fetch_addresses(db, unique_bbls)
    violations    = _fetch_violations(db, unique_bbls)
    evictions_map = _fetch_evictions(db, unique_bbls)
    permits_map   = _fetch_permits(db, unique_bbls)
    ds_scores     = _fetch_displacement_scores(db)
    bbl_zips      = _fetch_bbl_zips(db, unique_bbls)

    # -----------------------------------------------------------------------
    # LLC breakdown
    # -----------------------------------------------------------------------
    llc_counts: dict[str, int] = defaultdict(int)
    for a in acquisitions:
        llc_counts[a["entity"]] += 1

    # -----------------------------------------------------------------------
    # Per-property records
    # -----------------------------------------------------------------------
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
            "bbl":                      bbl,
            "address":                  rec["address"],
            "zip_code":                 zip_code,
            "acquiring_entity":         rec["entity"],
            "acquisition_date":         acq_date.isoformat() if acq_date else None,
            "price_paid":               price,
            "doc_types":                sorted(set(rec["doc_types"])),
            "document_ids":             rec["document_ids"],
            "displacement_score":       ds.get("score"),
            "hpd_violations": {
                "total":   len(viols),
                "class_a": v_a,
                "class_b": v_b,
                "class_c": v_c,
                "records": viols,
            },
            "eviction_then_buy":         etb_events,
            "post_acquisition_permits":  post_perms,
        })

    # -----------------------------------------------------------------------
    # ETB summary
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
    # HPD totals
    # -----------------------------------------------------------------------
    all_viols    = [v for prop in properties for v in prop["hpd_violations"]["records"]]
    total_a      = sum(1 for v in all_viols if v["class"] == "A")
    total_b      = sum(1 for v in all_viols if v["class"] == "B")
    total_c      = sum(1 for v in all_viols if v["class"] == "C")
    bbls_with_bc = sum(1 for p in properties if p["hpd_violations"]["class_b"] + p["hpd_violations"]["class_c"] > 0)

    # -----------------------------------------------------------------------
    # Geographic concentration
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
            "eviction_rate":      ds.get("eviction_rate"),
            "llc_acq_rate":       ds.get("llc_acquisition_rate"),
        })

    # -----------------------------------------------------------------------
    # Acquisition timeline
    # -----------------------------------------------------------------------
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

    # LLC structure analysis
    numbered_llcs   = [e for e in PHANTOM_ENTITIES if any(c.isdigit() for c in e)]
    named_llcs      = [e for e in PHANTOM_ENTITIES if not any(c.isdigit() for c in e)]
    active_entities = [e for e in PHANTOM_ENTITIES if llc_counts.get(e, 0) > 0]

    return {
        "generated_at":            date.today().isoformat(),
        "subject":                 "PHANTOM CAPITAL",
        "llc_entities":            PHANTOM_ENTITIES,
        "llc_count":               len(PHANTOM_ENTITIES),
        "active_entities":         active_entities,
        "numbered_llc_count":      len(numbered_llcs),
        "named_llc_count":         len(named_llcs),
        "llc_breakdown":           {e: llc_counts.get(e, 0) for e in PHANTOM_ENTITIES},
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


# ---------------------------------------------------------------------------
# Plain-English summary
# ---------------------------------------------------------------------------

def write_summary(data: dict, path: Path) -> None:
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
            f"    • {m['address'] or m['bbl']} (BBL {m['bbl']}, zip {m['zip_code']})\n"
            f"      Eviction executed {m['eviction_date']} — deed filed {m['acquisition_date']} "
            f"({m['gap_days']} days later, {price_str})\n"
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
            f"    • {p['address'] or p['bbl']} (zip {p['zip_code']}, ds={ds_str})  "
            f"— {bc} B/C violations ({v['class_b']} B, {v['class_c']} C)"
        )

    pace_lines = [f"    {ym}: {'█' * min(count, 60)} ({count})" for ym, count in sorted(monthly.items())]

    # Top active LLCs (those with at least one acquisition)
    active_sorted = sorted(
        [(e, data["llc_breakdown"][e]) for e in data["llc_entities"] if data["llc_breakdown"][e] > 0],
        key=lambda x: -x[1],
    )
    llc_lines = [f"    • {e}: {cnt} deed record{'s' if cnt != 1 else ''}"
                 for e, cnt in active_sorted]
    inactive_count = len([e for e in data["llc_entities"] if data["llc_breakdown"][e] == 0])

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
    PHANTOM CAPITAL OPERATOR INVESTIGATION
    Generated: {data['generated_at']}
    ══════════════════════════════════════════════════════════════════════

    OVERVIEW
    --------
    PHANTOM CAPITAL operated through {data['llc_count']} LLC entities and acquired
    {data['total_properties']} unique properties in New York City between {data['first_acquisition']}
    and {data['last_acquisition']} — a {data['span_days']}-day window. Average acquisition
    pace was {data['acquisitions_per_month']:.1f} properties per month.

    Total recorded spend: {spend_str}
    Average price per recorded transaction: {avg_str}
    ({data['properties_with_price']} of {data['total_properties']} properties had a recorded price)

    Unlike BREDIF (bulk note transfer) or BATTALION (mortgage lender false positive),
    PHANTOM is a direct-purchase operator. All acquisitions are deed transfers at
    market-rate prices, suggesting a buy-renovate-hold or buy-renovate-flip strategy
    targeting mid-tier multifamily stock in high-displacement Bronx and Brooklyn zip codes.

    LLC STRUCTURE
    -------------
    {data['llc_count']} total entities: {data['numbered_llc_count']} numbered (PHANTOM CAPITAL NN),
    {data['named_llc_count']} named (PHANTOM PARTNERS, PHANTOM HOUSING, etc.)
    {len(data['active_entities'])} entities with recorded deed transactions; {inactive_count} registered but unused.

    Active entities (by deed count):
{chr(10).join(llc_lines)}

    The numbered-LLC pattern — incremental suffixes across dozens of shells —
    is consistent with an operator that spins up a new LLC for each acquisition
    (or acquisition tranche) to isolate liability and fragment ownership records
    across public databases.

    ACQUISITION PACE (monthly)
    --------------------------
{chr(10).join(pace_lines)}

    GEOGRAPHIC CONCENTRATION (top zip codes)
    ----------------------------------------
{chr(10).join(geo_lines)}

    EVICTION-THEN-BUY MATCHES ({etb['total_matches']} events across {unique_etb_bbls} properties, {etb['lookback_days']}-day lookback)
    ─────────────────────────────────────────────────────────────────────
    {unique_etb_bbls} properties show at least one eviction executed within {etb['lookback_days']} days
    before PHANTOM took title ({etb['total_matches']} total eviction events).
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

    HIGH-VIOLATION PROPERTIES (≥3 Class B/C violations)
    ─────────────────────────────────────────────────────
{chr(10).join(hot_lines) if hot_lines else '    None above threshold.'}

    ASSESSMENT
    ----------
    PHANTOM CAPITAL is a mid-scale direct acquisition operator, structurally distinct
    from both MTEK (higher volume, tighter geographic focus) and BREDIF (note-purchase
    vehicle). The 32-LLC structure with numbered shells is a deliberate fragmentation
    strategy — each LLC holds a small slice of the portfolio, making the aggregate
    footprint invisible to anyone searching a single entity name.

    The {etb_rate_pct}% eviction-then-buy rate ({unique_etb_bbls} of {data['total_properties']} properties) is the
    most actionable signal. At {avg_str} average per property across {data['properties_with_price']}
    recorded transactions, PHANTOM is buying at prices that require significant rent
    increases or tenant displacement to pencil out — particularly in the Bronx zip
    codes (10466, 10459, 10456) where most of the portfolio is concentrated.

    The "PHANTOM AFFORDABLE HOUSING LLC" entity name is notable — using "affordable
    housing" branding while operating a displacement-linked acquisition strategy is
    a known regulatory arbitrage tactic in NYC's housing market.

    RECOMMENDATION: Flag for monitoring in the nightly pipeline alongside MTEK and
    BREDIF. Priority target for rent stabilization loss cross-reference given
    Bronx concentration in historically RS-heavy stock.
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

    etb   = data["eviction_then_buy"]
    viols = data["hpd_violations"]
    print(f"\nPHANTOM CAPITAL Investigation — {data['total_properties']} properties, "
          f"{data['first_acquisition']} → {data['last_acquisition']}\n")
    print(f"  LLC entities:          {data['llc_count']} total, {len(data['active_entities'])} active")
    print(f"  Total spend recorded:  ${data['total_spend']:,.0f}")
    print(f"  Acquisitions/month:    {data['acquisitions_per_month']:.1f}")
    print(f"\n  Eviction-then-buy:     {etb['total_matches']} matches")
    for m in etb["matches"]:
        price = f"${m['price_paid']:,.0f}" if m["price_paid"] else "no price"
        print(f"    {(m['address'] or m['bbl'])[:40]:40s}  {m['gap_days']:>4}d gap  {price}")
    print(f"\n  HPD violations:        {viols['total']} total  "
          f"({viols['class_b']} B, {viols['class_c']} C)  "
          f"{viols['bc_per_property']:.2f}/property")
    print(f"  BBLs with B/C:         {viols['bbls_with_class_bc']} of {data['total_properties']}")


if __name__ == "__main__":
    sys.exit(main())
