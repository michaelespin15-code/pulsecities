"""
One-time backfill of HPD class-I (informational) violations.

The scraper quarantined class I from launch until 2026-07-11, so the raw table
has none of the history. Class I carries vacate orders (64% of the rejected
rows), which building timelines and watch alerts should show. This fetches
class I from Socrata over the same 365-day window the nightly scraper started
from and upserts through the scraper's own parse/dedupe path, so a re-run is
harmless. The nightly watermark run keeps the class current from here on.

    PYTHONPATH=. venv/bin/python -m scripts.backfill_class_i_violations [--since YYYY-MM-DD]
"""

import argparse
import logging
from datetime import date, timedelta

from config.logging_config import configure_logging
from models.database import get_scraper_db  # loads .env as a side effect
from scrapers.violations import ViolationsScraper

configure_logging()
logger = logging.getLogger(__name__)


def run(since: date) -> None:
    scraper = ViolationsScraper()
    where = f"`class`='I' AND inspectiondate >= '{since.isoformat()}T00:00:00'"

    processed = 0
    failed = 0
    batch: list[dict] = []

    with get_scraper_db() as db:
        for raw in scraper.paginate(where, order="inspectiondate ASC"):
            row = scraper._parse(db, raw)
            if row is None:
                failed += 1
                continue
            batch.append(row)
            if len(batch) >= 1_000:
                processed += scraper._upsert_batch(db, batch)
                batch = []
        if batch:
            processed += scraper._upsert_batch(db, batch)

    logger.info("Class-I backfill done: %d upserted, %d failed parse, since %s",
                processed, failed, since.isoformat())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill HPD class-I violations")
    parser.add_argument("--since", type=date.fromisoformat,
                        default=date.today() - timedelta(days=365),
                        help="earliest inspectiondate to fetch (default: 365 days back)")
    args = parser.parse_args()
    run(args.since)
