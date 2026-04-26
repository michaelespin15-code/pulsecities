"""
DCWP historical license refresh.

Fetches and upserts DCWP licenses within a specific license_creation_date range,
catching status changes, renewals, expiry updates, and address corrections on
licenses that are too old to appear in the 14-day incremental window.

Each record is upserted via the uq_dcwp_license_nbr unique constraint, so
re-running a date range is always safe (idempotent).

Usage:
    # Refresh a specific date range in one shot:
    python scripts/dcwp_refresh_historical.py --since 2023-01-01 --until 2023-03-31

    # Refresh the same range in monthly chunks (lower peak memory):
    python scripts/dcwp_refresh_historical.py --since 2023-01-01 --until 2023-12-31 --chunk-months 1

    # Dry-run: print the chunks that would be refreshed, then exit:
    python scripts/dcwp_refresh_historical.py --since 2023-01-01 --until 2023-12-31 --dry-run

Recommended refresh schedule (add to crontab separately from the nightly pipeline):

    Daily   — nightly pipeline handles incremental + 14-day lookback automatically
    Weekly  — refresh the quarter ending ~2 weeks ago (outside the rolling window):
                0 3 * * 0 cd /opt/pulsecities && venv/bin/python scripts/dcwp_refresh_historical.py \\
                    --since $(date -d '3 months ago' +%Y-%m-%d) \\
                    --until $(date -d '14 days ago' +%Y-%m-%d)
    Monthly — walk back one year in quarterly chunks:
                0 4 1 * * cd /opt/pulsecities && venv/bin/python scripts/dcwp_refresh_historical.py \\
                    --since $(date -d '13 months ago' +%Y-%m-%d) \\
                    --until $(date -d '3 months ago' +%Y-%m-%d) \\
                    --chunk-months 3
    Annually — full sweep from dataset inception:
                python scripts/dcwp_refresh_historical.py --since 2019-01-01 \\
                    --until $(date -d '3 months ago' +%Y-%m-%d) --chunk-months 3

Goal: every active license should be rechecked at least once every 60-90 days.
Track coverage via source_last_refreshed_at:
    SELECT date_trunc('month', source_last_refreshed_at) AS month, count(*)
    FROM dcwp_licenses
    GROUP BY 1 ORDER BY 1;
"""

import argparse
import calendar
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.database import get_scraper_db
from scrapers.dcwp_licenses import DcwpScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _add_months(d: date, n: int) -> date:
    """Add n calendar months to d, clamping the day to the last day of the result month."""
    month = d.month - 1 + n
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _date_chunks(since: date, until: date, chunk_months: int):
    """Yield (chunk_since, chunk_until) pairs covering [since, until] in chunk_months steps."""
    cursor = since
    while cursor <= until:
        chunk_end = min(_add_months(cursor, chunk_months) - timedelta(days=1), until)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def run_refresh(
    since: date,
    until: date,
    chunk_months: int = 0,
    dry_run: bool = False,
) -> bool:
    """
    Run historical refresh for [since, until], optionally in monthly/quarterly chunks.
    Returns True if all chunks succeeded, False if any chunk failed.
    """
    chunks = list(_date_chunks(since, until, chunk_months)) if chunk_months else [(since, until)]

    logger.info(
        "DCWP historical refresh: %s to %s | %d chunk(s) | dry_run=%s",
        since, until, len(chunks), dry_run,
    )

    if dry_run:
        for i, (cs, ce) in enumerate(chunks, 1):
            logger.info("  chunk %d: %s to %s", i, cs, ce)
        return True

    scraper = DcwpScraper()
    all_ok = True

    for i, (chunk_since, chunk_until) in enumerate(chunks, 1):
        logger.info("chunk %d/%d: %s to %s", i, len(chunks), chunk_since, chunk_until)
        try:
            with get_scraper_db() as db:
                processed, failed, inserted, changed = scraper.refresh_historical_range(
                    db, chunk_since, chunk_until,
                )
            logger.info(
                "chunk %d done: processed=%d failed=%d inserted=%d changed=%d",
                i, processed, failed, inserted, changed,
            )
            if failed > 0:
                logger.warning("chunk %d: %d records quarantined", i, failed)
        except Exception as exc:
            logger.error("chunk %d failed: %s", i, exc)
            all_ok = False

    return all_ok


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh DCWP licenses for a historical date range.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--since",
        required=True,
        metavar="YYYY-MM-DD",
        help="Start of license_creation_date range (inclusive)",
    )
    parser.add_argument(
        "--until",
        required=True,
        metavar="YYYY-MM-DD",
        help="End of license_creation_date range (inclusive)",
    )
    parser.add_argument(
        "--chunk-months",
        type=int,
        default=0,
        metavar="N",
        help="Split range into N-month chunks (0 = single query, default)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print chunks without fetching or writing anything",
    )
    args = parser.parse_args()

    try:
        since = date.fromisoformat(args.since)
        until = date.fromisoformat(args.until)
    except ValueError as exc:
        logger.error("Invalid date: %s", exc)
        sys.exit(1)

    if since > until:
        logger.error("--since must be before --until")
        sys.exit(1)

    if args.chunk_months < 0:
        logger.error("--chunk-months must be >= 0")
        sys.exit(1)

    ok = run_refresh(since, until, chunk_months=args.chunk_months, dry_run=args.dry_run)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
