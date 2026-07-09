"""
Backfill score_history for the past N days by replaying compute_scores()
against historical raw data.

Each date is scored with a rolling 365-day window ending on that date,
matching exactly how the nightly pipeline would have scored it. Existing
score_history rows are silently skipped (ON CONFLICT DO NOTHING), so the
script is safe to re-run.

Displacement_scores and neighborhoods.current_score are NOT modified —
compute_scores() skips those tables when as_of_date is set.

Usage:
    python scripts/backfill_score_history.py           # defaults to 180 days
    python scripts/backfill_score_history.py --days 90
"""

import argparse
import logging
import sys
from datetime import date, timedelta

from sqlalchemy import text

from models.database import get_scraper_db
from scoring.compute import compute_scores

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def backfill(days: int = 180, replace: bool = False) -> None:
    today = date.today()
    # oldest first so score_history grows in chronological order
    dates = [today - timedelta(days=i) for i in range(days, 0, -1)]

    logger.info(
        "Backfilling score_history for %d dates (%s → %s)%s",
        len(dates),
        dates[0].isoformat(),
        dates[-1].isoformat(),
        " [replace]" if replace else "",
    )

    with get_scraper_db() as db:
        if replace:
            # Recompute mode: drop the snapshots in range so compute_scores'
            # ON CONFLICT DO NOTHING can't silently keep the old rows. Today's
            # nightly snapshot is never in `dates` and stays untouched.
            deleted = db.execute(
                text(
                    "DELETE FROM score_history"
                    " WHERE scored_at >= :start AND scored_at <= :end"
                ),
                {"start": dates[0], "end": dates[-1]},
            ).rowcount
            db.commit()
            logger.info("Deleted %d existing snapshot rows in range", deleted)

        existing = {
            r[0]
            for r in db.execute(
                text(
                    "SELECT DISTINCT scored_at FROM score_history"
                    " WHERE scored_at >= :start"
                ),
                {"start": dates[0]},
            ).fetchall()
        }
        if existing:
            logger.info(
                "%d date(s) already populated — will skip",
                len(existing),
            )

        skipped = 0
        for i, target in enumerate(dates, start=1):
            if target in existing:
                skipped += 1
                continue
            n = compute_scores(db, as_of_date=target)
            logger.info("[%d/%d] %s — %d zips scored", i, len(dates), target, n)

    logger.info(
        "Done. %d date(s) backfilled, %d skipped (already present).",
        len(dates) - skipped,
        skipped,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill score_history table")
    parser.add_argument(
        "--days",
        type=int,
        default=180,
        help="Number of calendar days to backfill (default: 180)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing snapshots in range and recompute them",
    )
    args = parser.parse_args()

    if args.days < 1 or args.days > 730:
        print("--days must be between 1 and 730", file=sys.stderr)
        sys.exit(1)

    backfill(args.days, replace=args.replace)
    sys.exit(0)
