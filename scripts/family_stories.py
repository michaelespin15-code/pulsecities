"""
Story profiles for every entity family, so the next portfolio trade is found by
running this rather than by noticing it.

The FLGSP pitch came out of reading one family's deed record by hand: 82
buildings, one date, one price. `api/entity_families.py` now finds families
automatically; this reads each one for the shapes a housing desk would care
about and ranks them.

Four shapes, in the order they tend to be worth writing about:

    bulk trade   many buildings moving on ONE deed date, both sides numbered
                 companies, which is one deal wearing many names
    unwind       a family that has sold more than it holds
    assembly     steady accumulation, no single bulk date
    hold         everything bought long ago and still held

Each profile carries the things that survive a reporter's check: unit counts,
DHCR rent-stabilized registrations, open violations, executed evictions split
by whether they PREDATE the family's own deed (they usually do, and saying so
is the difference between a story and a libel risk), the counterparty names,
and per-building deed document IDs for ACRIS.

Produces:
  scripts/family_stories.json  — every family, machine-readable
  scripts/family_stories.txt   — ranked plain-English read

Usage:
    python scripts/family_stories.py [--slug SLUG]
"""

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from api.entity_families import compute_families
from models.database import get_scraper_db

logger = logging.getLogger(__name__)

JSON_OUTPUT = Path(__file__).parent / "family_stories.json"
TXT_OUTPUT = Path(__file__).parent / "family_stories.txt"

# Same list the property and LLC pages use, so a count here matches the site.
VIOLATION_RESOLVED = ("VIOLATION CLOSED", "VIOLATION DISMISSED")

# A deed date carrying at least this many buildings is a portfolio moving, not
# a landlord buying a building. FLGSP was 82; the threshold is deliberately low
# enough to catch the next one while it is still small.
BULK_MIN = 5


def _building_key(bbl: str) -> str:
    """Condo unit lots collapse to the building they sit in. Without this, one
    24-unit condo bought whole at 817 West End Avenue reads as a 24-building
    portfolio trade, which is the single most misleading thing this file could
    print. Same rule as the LLC page and the family clustering."""
    return bbl[:6] + ("0000" if bbl[6:10] >= "1001" else bbl[6:10])


def _deeds(db, names):
    """Every DEED row either side of the family, one row per (document, bbl)."""
    return db.execute(text("""
        SELECT o.document_id, o.bbl, o.doc_date, o.doc_amount, o.party_type,
               o.party_name_normalized AS name
        FROM ownership_raw o
        WHERE o.doc_type = 'DEED'
          AND o.party_type IN ('1', '2')
          AND o.document_id IN (
              SELECT document_id FROM ownership_raw
              WHERE doc_type = 'DEED' AND party_name_normalized = ANY(:names)
          )
    """), {"names": list(names)}).fetchall()


def _parcels(db, bbls):
    if not bbls:
        return {}
    rows = db.execute(text("""
        SELECT bbl, address, zip_code, units_res, units_total, year_built
        FROM parcels WHERE bbl = ANY(:bbls)
    """), {"bbls": list(bbls)}).fetchall()
    return {r.bbl: r for r in rows}


def _rs_units(db, bbls):
    """Latest DHCR registration per building, and the year it is from.

    Filtered to source='dhcr'. The table also holds an `hpd_jurisdiction` row
    per building for 2026, which is HPD's much broader rent-stabilized
    jurisdiction file rather than a registration count, and mixing the two
    inflates a portfolio by a third. A pitch that says "DHCR-registered" has to
    be the DHCR number."""
    if not bbls:
        return {}
    rows = db.execute(text("""
        SELECT DISTINCT ON (bbl) bbl, year, rs_unit_count
        FROM rs_buildings
        WHERE bbl = ANY(:bbls) AND rs_unit_count > 0 AND source = 'dhcr'
        ORDER BY bbl, year DESC
    """), {"bbls": list(bbls)}).fetchall()
    return {r.bbl: (r.year, r.rs_unit_count) for r in rows}


def _violations(db, bbls):
    if not bbls:
        return {}
    rows = db.execute(text("""
        SELECT bbl, count(*) FILTER (WHERE current_status NOT IN :resolved) AS open
        FROM violations_raw WHERE bbl = ANY(:bbls) GROUP BY 1
    """), {"bbls": list(bbls), "resolved": VIOLATION_RESOLVED}).fetchall()
    return {r.bbl: int(r.open or 0) for r in rows}


def _evictions(db, bbls):
    if not bbls:
        return defaultdict(list)
    rows = db.execute(text("""
        SELECT bbl, executed_date FROM evictions_raw
        WHERE bbl = ANY(:bbls) AND executed_date IS NOT NULL
    """), {"bbls": list(bbls)}).fetchall()
    out = defaultdict(list)
    for r in rows:
        out[r.bbl].append(r.executed_date)
    return out


def _shape(bought_by_date, held, sold):
    """Which of the four shapes this family is, and the evidence for it."""
    biggest = max((len({_building_key(b) for b in bs})
                   for bs in bought_by_date.values()), default=0)
    if biggest >= BULK_MIN:
        return "bulk trade"
    if sold > held:
        return "unwind"
    if held >= BULK_MIN:
        return "assembly"
    return "hold"


def profile(db, fam):
    names = fam["entities"]
    rows = _deeds(db, names)

    # A document is the family's own only where one of its entities is a party.
    # party_type 2 is the buyer, 1 the seller.
    side = {}          # document_id -> 'buy' | 'sell'
    doc_bbls = defaultdict(set)
    doc_date = {}
    doc_amount = {}
    counterparty = defaultdict(set)
    nameset = set(names)
    for r in rows:
        doc_bbls[r.document_id].add(r.bbl)
        if r.doc_date:
            doc_date[r.document_id] = r.doc_date
        if r.doc_amount and r.doc_amount > 0:
            doc_amount[r.document_id] = float(r.doc_amount)
        if r.name in nameset:
            side[r.document_id] = "buy" if r.party_type == "2" else "sell"
        else:
            counterparty[r.document_id].add(r.name)

    bought_by_date = defaultdict(set)
    sold_by_date = defaultdict(set)
    bought, sold_bbls = set(), set()
    for doc, s in side.items():
        d = doc_date.get(doc)
        for b in doc_bbls[doc]:
            (bought if s == "buy" else sold_bbls).add(b)
            (bought_by_date if s == "buy" else sold_by_date)[d].add(b)

    allb = bought | sold_bbls
    parcels = _parcels(db, allb)
    rs = _rs_units(db, allb)
    viol = _violations(db, allb)
    evict = _evictions(db, allb)

    # The date the family took title to each building, for the
    # evictions-predate-the-sale split.
    took_title = {}
    for doc, s in side.items():
        if s != "buy":
            continue
        for b in doc_bbls[doc]:
            d = doc_date.get(doc)
            if d and (b not in took_title or d < took_title[b]):
                took_title[b] = d

    before = after = unknown = 0
    for b, dates in evict.items():
        t = took_title.get(b)
        for d in dates:
            if t is None:
                unknown += 1
            elif d < t:
                before += 1
            else:
                after += 1

    zips = Counter()
    units = rs_units = pre74 = 0
    rs_years = Counter()
    for b in bought or allb:
        p = parcels.get(b)
        if not p:
            continue
        if p.zip_code:
            zips[p.zip_code] += 1
        units += int(p.units_res or 0)
        if p.year_built and 0 < p.year_built < 1974:
            pre74 += 1
        if b in rs:
            y, n = rs[b]
            rs_units += int(n or 0)
            rs_years[y] += 1

    buildings = []
    for b in sorted(allb, key=lambda x: -(int(parcels[x].units_res or 0) if x in parcels else 0)):
        p = parcels.get(b)
        doc = next((d for d, bs in doc_bbls.items() if b in bs and d in side), None)
        buildings.append({
            "bbl": b,
            "address": (p.address if p else None),
            "zip": (p.zip_code if p else None),
            "units_res": int(p.units_res or 0) if p else None,
            "year_built": int(p.year_built or 0) if p else None,
            "rs_units": rs.get(b, (None, None))[1],
            "open_violations": viol.get(b, 0),
            "evictions": len(evict.get(b, [])),
            "side": "held" if b in bought else "sold",
            "deed_date": str(took_title[b]) if b in took_title else None,
            "deed_doc": doc,
        })

    def _bulk(by_date):
        counted = ((d, len({_building_key(b) for b in bs})) for d, bs in by_date.items() if d)
        return sorted(((d, n) for d, n in counted if n >= BULK_MIN), key=lambda x: -x[1])

    bulk_dates = _bulk(bought_by_date)
    sold_bulk = _bulk(sold_by_date)

    counter_names = Counter()
    for doc, s in side.items():
        for n in counterparty.get(doc, ()):
            counter_names[n] += 1

    return {
        "slug": fam["slug"],
        "label": fam["label"],
        "entities": names,
        "entity_count": len(names),
        "shape": _shape(bought_by_date, fam["buildings"], fam["sold"]),
        "held": fam["buildings"],
        "sold": fam["sold"],
        "volume": fam["volume"],
        "last_deed": str(fam["last_deed"]) if fam["last_deed"] else None,
        "bulk_buy_dates": [{"date": str(d), "buildings": n} for d, n in bulk_dates[:5]],
        "bulk_sell_dates": [{"date": str(d), "buildings": n} for d, n in sold_bulk[:5]],
        "units_res": units,
        "rs_units": rs_units,
        "rs_year": (rs_years.most_common(1)[0][0] if rs_years else None),
        "rs_buildings": sum(rs_years.values()),
        "pre_1974_buildings": pre74,
        "open_violations": sum(viol.get(b, 0) for b in allb),
        "evictions_total": before + after + unknown,
        "evictions_before_purchase": before,
        "evictions_after_purchase": after,
        "evictions_unknown": unknown,
        "zips": zips.most_common(8),
        "addresses": [list(a) for a in fam["addresses"]],
        "counterparties": counter_names.most_common(10),
        "buildings": buildings,
    }


def _rank(p):
    """Bulk trades first, then size. What a desk would open first."""
    bulk = max([b["buildings"] for b in p["bulk_buy_dates"]] +
               [b["buildings"] for b in p["bulk_sell_dates"]] + [0])
    return (-bulk, -(p["rs_units"] or 0), -(p["units_res"] or 0))


def _render(profiles):
    out = [f"Entity family story profiles, {date.today()}", ""]
    out.append(f"{len(profiles)} families. Ranked by biggest single-day transfer, "
               "then by rent-stabilized units.")
    out.append("")
    for p in profiles:
        out.append("=" * 72)
        out.append(f"{p['label']}  (/network/{p['slug']})  [{p['shape']}]")
        out.append("-" * 72)
        out.append(f"  {p['entity_count']} companies, {p['held']} buildings held, "
                   f"{p['sold']} sold, ${p['volume']:,.0f} stated consideration")
        if p["bulk_buy_dates"]:
            d = p["bulk_buy_dates"][0]
            out.append(f"  BULK BUY: {d['buildings']} buildings on {d['date']}")
        if p["bulk_sell_dates"]:
            d = p["bulk_sell_dates"][0]
            out.append(f"  BULK SELL: {d['buildings']} buildings on {d['date']}")
        out.append(f"  {p['units_res']:,} residential units, "
                   f"{p['rs_units']:,} rent-stabilized across {p['rs_buildings']} "
                   f"buildings (DHCR {p['rs_year']})")
        out.append(f"  {p['pre_1974_buildings']} built before 1974, "
                   f"{p['open_violations']:,} open violations")
        out.append(f"  evictions: {p['evictions_total']} total, "
                   f"{p['evictions_before_purchase']} predate the family's deed, "
                   f"{p['evictions_after_purchase']} after")
        if p["zips"]:
            out.append("  ZIPs: " + ", ".join(f"{z} ({n})" for z, n in p["zips"][:6]))
        if p["counterparties"]:
            out.append("  counterparties: " + ", ".join(
                f"{n} ({c})" for n, c in p["counterparties"][:5]))
        big = [b for b in p["buildings"] if b["units_res"]][:5]
        if big:
            out.append("  largest buildings:")
            for b in big:
                out.append(f"    {b['address'] or b['bbl']:<34} {b['zip'] or '':<6} "
                           f"{b['units_res'] or 0:>4}u  {b['open_violations']:>4} open  "
                           f"{b['side']}  {b['deed_date'] or ''}")
        out.append("")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", help="profile one family only")
    args = ap.parse_args()

    from api.routes.frontend import _is_buyer_entity

    with get_scraper_db() as db:
        fams = compute_families(db, _is_buyer_entity)
        wanted = [f for f in fams.values() if not args.slug or f["slug"] == args.slug]
        profiles = [profile(db, f) for f in wanted]

    profiles.sort(key=_rank)
    JSON_OUTPUT.write_text(json.dumps(profiles, indent=2, default=str))
    text_out = _render(profiles)
    TXT_OUTPUT.write_text(text_out)
    print(text_out)
    print(f"\nwrote {JSON_OUTPUT} and {TXT_OUTPUT}")


if __name__ == "__main__":
    main()
