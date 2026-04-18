"""
Renovation-to-eviction pipeline detector.

Scans all LLC acquisitions in the last 180 days and stages each building:

  Stage 1 — Acquired, no permits filed post-acquisition
  Stage 2 — Acquired + permits filed within 90 days of acquisition
  Stage 3 — Acquired + permits + evictions filed after acquisition

Stage 3 is the clearest displacement signal: buy, renovate, push out tenants.

Usage:
    python scripts/reno_eviction_pipeline.py
"""

import json
import logging
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db

logger = logging.getLogger(__name__)

OUTPUT_PATH = Path(__file__).parent / "reno_eviction_pipeline.json"

ACQUISITION_WINDOW_DAYS = 180
PERMIT_WINDOW_DAYS      = 90   # permit filed within N days of acquisition


def _fetch_recent_llc_acquisitions(db, cutoff: date) -> list[dict]:
    rows = db.execute(text("""
        SELECT
            o.bbl,
            o.party_name_normalized  AS entity,
            o.doc_date               AS acquisition_date,
            o.doc_amount             AS price
        FROM ownership_raw o
        WHERE o.party_type = '2'
          AND o.doc_date >= :cutoff
          AND o.party_name_normalized ILIKE '%LLC%'
          AND o.bbl IS NOT NULL
        ORDER BY o.doc_date, o.bbl
    """), {"cutoff": cutoff}).fetchall()
    return [
        {
            "bbl":              r.bbl,
            "entity":           r.entity,
            "acquisition_date": r.acquisition_date,
            "price":            float(r.price) if r.price else None,
        }
        for r in rows
    ]


def _fetch_permits_for_bbls(db, bbls: list[str]) -> dict[str, list[dict]]:
    if not bbls:
        return {}
    placeholders = ", ".join(f":b{i}" for i in range(len(bbls)))
    params       = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT bbl, permit_type, work_type, job_description, filing_date, owner_name
        FROM permits_raw
        WHERE bbl IN ({placeholders})
          AND filing_date IS NOT NULL
        ORDER BY bbl, filing_date
    """), params).fetchall()

    result: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        result[r.bbl].append({
            "permit_type":  r.permit_type,
            "work_type":    r.work_type,
            "description":  r.job_description,
            "filing_date":  r.filing_date.isoformat() if r.filing_date else None,
            "owner_name":   r.owner_name,
        })
    return dict(result)


def _fetch_evictions_for_bbls(db, bbls: list[str]) -> dict[str, list[dict]]:
    if not bbls:
        return {}
    placeholders = ", ".join(f":b{i}" for i in range(len(bbls)))
    params       = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (bbl, executed_date)
            bbl, executed_date, docket_number, eviction_type, address
        FROM evictions_raw
        WHERE bbl IN ({placeholders})
          AND executed_date IS NOT NULL
        ORDER BY bbl, executed_date
    """), params).fetchall()

    result: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        result[r.bbl].append({
            "executed_date": r.executed_date.isoformat() if r.executed_date else None,
            "docket_number": r.docket_number,
            "eviction_type": r.eviction_type,
            "address":       r.address,
        })
    return dict(result)


def _fetch_addresses_and_zips(db, bbls: list[str]) -> dict[str, dict]:
    if not bbls:
        return {}
    placeholders = ", ".join(f":b{i}" for i in range(len(bbls)))
    params       = {f"b{i}": b for i, b in enumerate(bbls)}
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (bbl) bbl, address, zip_code
        FROM (
            SELECT bbl, address, zip_code, inspection_date AS d FROM violations_raw
            WHERE bbl IN ({placeholders}) AND address IS NOT NULL
            UNION ALL
            SELECT bbl, address, zip_code, filing_date AS d FROM permits_raw
            WHERE bbl IN ({placeholders}) AND address IS NOT NULL
        ) t
        ORDER BY bbl, d DESC NULLS LAST
    """), params).fetchall()
    return {r.bbl: {"address": r.address, "zip_code": r.zip_code} for r in rows}


def run_analysis(db) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ACQUISITION_WINDOW_DAYS)).date()
    logger.info("Scanning LLC acquisitions since %s", cutoff)

    raw_acqs = _fetch_recent_llc_acquisitions(db, cutoff)
    logger.info("%d raw acquisition records", len(raw_acqs))

    # Deduplicate: one record per BBL, earliest deed date wins.
    bbl_acq: dict[str, dict] = {}
    for a in raw_acqs:
        bbl = a["bbl"]
        if bbl not in bbl_acq:
            bbl_acq[bbl] = a
        elif a["acquisition_date"] and (
            bbl_acq[bbl]["acquisition_date"] is None
            or a["acquisition_date"] < bbl_acq[bbl]["acquisition_date"]
        ):
            bbl_acq[bbl] = a

    unique_bbls = list(bbl_acq.keys())
    logger.info("%d unique BBLs acquired by LLCs in window", len(unique_bbls))

    permits_map   = _fetch_permits_for_bbls(db, unique_bbls)
    evictions_map = _fetch_evictions_for_bbls(db, unique_bbls)
    meta_map      = _fetch_addresses_and_zips(db, unique_bbls)

    stage1, stage2, stage3 = [], [], []

    for bbl, acq in bbl_acq.items():
        acq_date = acq["acquisition_date"]
        acq_iso  = acq_date.isoformat() if acq_date else None

        # Permits filed within PERMIT_WINDOW_DAYS of acquisition
        all_permits  = permits_map.get(bbl, [])
        early_permits = []
        if acq_date:
            permit_deadline = acq_date + timedelta(days=PERMIT_WINDOW_DAYS)
            early_permits = [
                p for p in all_permits
                if p["filing_date"] and acq_iso <= p["filing_date"] <= permit_deadline.isoformat()
            ]

        # Evictions after acquisition date
        all_evictions  = evictions_map.get(bbl, [])
        post_evictions = []
        if acq_date:
            post_evictions = [
                e for e in all_evictions
                if e["executed_date"] and e["executed_date"] >= acq_iso
            ]

        meta = meta_map.get(bbl, {})
        record = {
            "bbl":              bbl,
            "address":          meta.get("address"),
            "zip_code":         meta.get("zip_code"),
            "acquiring_entity": acq["entity"],
            "acquisition_date": acq_iso,
            "price":            acq["price"],
            "permits_in_window": early_permits,
            "post_acquisition_evictions": post_evictions,
        }

        if post_evictions and early_permits:
            record["stage"] = 3
            stage3.append(record)
        elif early_permits:
            record["stage"] = 2
            stage2.append(record)
        else:
            record["stage"] = 1
            stage1.append(record)

    # Sort each stage: stage 3 by eviction count desc, others by acquisition date
    stage3.sort(key=lambda x: (-len(x["post_acquisition_evictions"]), x["acquisition_date"] or ""))
    stage2.sort(key=lambda x: (-len(x["permits_in_window"]), x["acquisition_date"] or ""))
    stage1.sort(key=lambda x: x["acquisition_date"] or "")

    return {
        "generated_at":            date.today().isoformat(),
        "acquisition_window_days": ACQUISITION_WINDOW_DAYS,
        "permit_window_days":      PERMIT_WINDOW_DAYS,
        "acquisition_cutoff":      cutoff.isoformat(),
        "total_buildings_scanned": len(unique_bbls),
        "summary": {
            "stage1_count": len(stage1),
            "stage2_count": len(stage2),
            "stage3_count": len(stage3),
        },
        "stage1_acquired_no_permits":               stage1,
        "stage2_acquired_with_permits":             stage2,
        "stage3_acquired_permits_and_evictions":    stage3,
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    with get_scraper_db() as db:
        results = run_analysis(db)

    OUTPUT_PATH.write_text(json.dumps(results, indent=2, default=str))
    logger.info("Saved %s", OUTPUT_PATH)

    s = results["summary"]
    print(f"\nRenovation-to-eviction pipeline — {results['acquisition_cutoff']} → today")
    print(f"  {results['total_buildings_scanned']} LLC acquisitions scanned\n")
    print(f"  Stage 1 (acquired, no permits):          {s['stage1_count']:>5}")
    print(f"  Stage 2 (acquired + permits):            {s['stage2_count']:>5}")
    print(f"  Stage 3 (acquired + permits + evictions):{s['stage3_count']:>5}")

    stage3 = results["stage3_acquired_permits_and_evictions"]
    if stage3:
        print(f"\n  Stage 3 buildings (highest risk):")
        for b in stage3[:20]:
            n_evict  = len(b["post_acquisition_evictions"])
            n_permit = len(b["permits_in_window"])
            label    = b["address"] or b["bbl"]
            print(f"    {label:45s}  {n_evict} eviction{'s' if n_evict != 1 else ''}  {n_permit} permit{'s' if n_permit != 1 else ''}")


if __name__ == "__main__":
    sys.exit(main())
