"""
One-time repair: unstamp the assessments PLUTO dated but never read.

scrapers/pluto.py wrote an assessment_history row for every parcel carrying an
assessed value and dated all of them with the calendar year of the run. It
covers 858,602 lots. 918,338 carry a value, so 59,423 rows were dated by a
scraper that had never seen the lot. Those lots come from scrapers/dof.py, a
frozen DOF archive that stops at fiscal 2018/19, and their values are from
years between 2010/11 and 2018/19.

/property reads `max(tax_year)` for the lot and printed the result as fact:

    The city's Department of Finance assessed this lot at $287K
    for the 2026 tax year.

The lot is 420 East 58 Street, unit 1075. The figure is its 2014/15 assessment.
10,197 sitemapped pages carried a sentence of that shape.

The loader no longer stamps what it did not read, and DOF now files each row
under the fiscal year it came from. This clears the rows written before both.
It deletes nothing that any scraper would not rewrite: run
`python -m scrapers.dof` afterwards to refile the real years.

    venv/bin/python -m scripts.repair_assessment_years --dry-run
    venv/bin/python -m scripts.repair_assessment_years
"""
import argparse
import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import text

from models.database import get_scraper_db

logger = logging.getLogger(__name__)

# A lot MapPLUTO has never described. Every PLUTO-owned column is NULL, which
# is true of the DOF-only lots and of nothing else: the population it leaves
# out is 858,558 lots against the 858,602 MapPLUTO reports processing.
NEVER_SEEN_BY_PLUTO = """
    p.units_res IS NULL AND p.year_built IS NULL AND p.lot_area IS NULL
    AND p.bldg_area IS NULL AND p.land_use IS NULL
"""


def run(db, dry_run: bool = False) -> dict:
    snapshot_year = datetime.now(timezone.utc).year

    before = db.execute(
        text(f"""
            SELECT COUNT(*) FROM assessment_history ah
            JOIN parcels p ON p.bbl = ah.bbl
            WHERE ah.tax_year = :y AND {NEVER_SEEN_BY_PLUTO}
        """),
        {"y": snapshot_year},
    ).scalar() or 0

    logger.info("%d rows dated %d for lots MapPLUTO has never seen", before, snapshot_year)

    if dry_run:
        sample = db.execute(
            text(f"""
                SELECT ah.bbl, ah.assessed_total, p.address FROM assessment_history ah
                JOIN parcels p ON p.bbl = ah.bbl
                WHERE ah.tax_year = :y AND {NEVER_SEEN_BY_PLUTO}
                ORDER BY md5(ah.bbl) LIMIT 5
            """),
            {"y": snapshot_year},
        ).fetchall()
        for row in sample:
            logger.info("  %s  %s  %s", row[0], row[1], row[2])
        return {"deleted": 0, "would_delete": before}

    result = db.execute(
        text(f"""
            DELETE FROM assessment_history ah
            USING parcels p
            WHERE p.bbl = ah.bbl AND ah.tax_year = :y AND {NEVER_SEEN_BY_PLUTO}
        """),
        {"y": snapshot_year},
    )
    logger.info("deleted %d rows", result.rowcount)
    return {"deleted": result.rowcount, "would_delete": before}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="count and sample, change nothing")
    args = parser.parse_args()

    with get_scraper_db() as db:
        result = run(db, dry_run=args.dry_run)
        if args.dry_run:
            db.rollback()

    if args.dry_run:
        print(f"Would delete {result['would_delete']} rows. Nothing was changed.")
    else:
        print(f"Deleted {result['deleted']} rows. "
              f"Run `python -m scrapers.dof` to refile the real fiscal years.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
