"""
Row-count reconciliation against the source of truth.

Every ingestion bug this project has hit was invisible to the checks watching for
it, because those checks asked "did the scraper run" and "how fresh is the newest
record". Neither question notices that a day we already ingested is quietly short
a few hundred rows.

  - 311 lost 1-3.5% of every day to a watermark that only moves forward, and the
    loss grew with age. Freshness was green throughout.
  - A 16-hour outage on 2026-08-15 dropped a full day of ingest. Nothing said so,
    because every monitor runs on the box that was down.
  - A short Socrata page used to end a walk early and record the run a success.

One question catches all three: does our count for a given day match the source's?
That is what this does, per feed and per day, and it is the only check here that
can find an ingestion bug nobody has thought of yet.

Healing works by moving the watermark, not by re-implementing ingest. When a day
has drifted, --heal rewinds the scraper's stored watermark to just before it, and
the next nightly run re-reads that range through the normal code path. Every
scraper upserts on a natural key, so re-reading a window writes only what is
genuinely missing.

Usage:
    python -m scripts.reconcile_upstream                 # report only
    python -m scripts.reconcile_upstream --days 30       # wider window
    python -m scripts.reconcile_upstream --feed 311_complaints
    python -m scripts.reconcile_upstream --heal          # rewind drifted feeds
    python -m scripts.reconcile_upstream --heal --run    # rewind, then scrape now
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import requests
from sqlalchemy import text

from config.logging_config import configure_logging
from config.nyc import SOCRATA_BASE_URL
from models.database import get_scraper_db
from scheduler.alerts import flush_alerts, send_alert

configure_logging()
logger = logging.getLogger(__name__)

# A day is only worth flagging once the source has stopped filling it in. Feeds
# publish on a lag, so the newest days are always short and that is not drift.
DEFAULT_WINDOW_DAYS = 14

# Below this, treat the difference as normal churn rather than a defect. 311
# sat at 1.2% while genuinely broken, so this is deliberately tight.
DRIFT_ALERT_PCT = 0.005

# Never rewind further than this in one go, so a bad reconciliation cannot
# order a re-scan of the entire dataset.
MAX_REWIND_DAYS = 60


@dataclass(frozen=True)
class Feed:
    scraper_name: str
    dataset_id: str
    upstream_field: str
    table: str
    column: str
    settle_days: int          # days of publishing lag before a day is judged
    upstream_is_text: bool = False   # date stored as MM/DD/YYYY text

    def upstream_where(self, day: date) -> str:
        if self.upstream_is_text:
            return f"{self.upstream_field}='{day.strftime('%m/%d/%Y')}'"
        nxt = day + timedelta(days=1)
        return (
            f"{self.upstream_field}>='{day.isoformat()}T00:00:00'"
            f" AND {self.upstream_field}<'{nxt.isoformat()}T00:00:00'"
        )


# permits stores filing_date as MM/DD/YYYY text upstream, so it is matched by
# exact string rather than a range. dobrundate is the calendar_date on that
# dataset, but it dates the DOB export, not the filing we key on.
#
# settle_days is how long a feed needs before a shortfall means something, and
# it is calibrated per feed against measured parity rather than guessed. Every
# scraper here already re-reads some trailing window, so a day inside that window
# being short is work in progress, not a defect. Judging too early is how a
# monitor teaches you to ignore it:
#
#   311        7d lookback   parity at  3d
#   violations 10d lookback  parity at 12d
#   evictions  45d lookback  parity throughout
#   permits    re-reads 3 years each run, parity from ~25d
FEEDS = [
    Feed("311_complaints", "erm2-nwe9", "created_date",    "complaints_raw", "created_date",    settle_days=3),
    Feed("hpd_violations", "wvxf-dwi5", "inspectiondate",  "violations_raw", "inspection_date", settle_days=12),
    Feed("evictions",      "6z8x-wfk4", "executed_date",   "evictions_raw",  "executed_date",   settle_days=14),
]

# dob_permits is deliberately absent. Counting rows only works when our table
# holds one row per upstream row, and permits_raw does not: the unique index
# uq_permits_raw_bbl_date_type_work collapses several genuine filings that share
# a BBL, day, permit type and work type. On 2026-07-15 the source published 39
# rows which reduce to exactly the 29 distinct tuples we hold. Reconciling it by
# count reports a permanent 25% shortfall that is the dedupe working as designed.
#
# Reconciling permits needs a comparison against distinct keys rather than rows,
# which SoQL cannot express for this shape. Left out rather than left crying wolf.
UNRECONCILABLE = {
    "dob_permits": "permits_raw dedupes on (bbl, filing_date, permit_type, work_type)",
}


def _token_params() -> dict:
    token = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")
    return {"$$app_token": token} if token else {}


def upstream_count(feed: Feed, day: date) -> int | None:
    """Rows the source holds for this day, or None if the probe failed.

    None is deliberately not zero: a failed probe must never be reported as a
    day where the source has nothing, which would read as us holding a surplus.
    """
    params = _token_params()
    params.update({"$select": "COUNT(*) AS n", "$where": feed.upstream_where(day)})
    try:
        resp = requests.get(f"{SOCRATA_BASE_URL}/{feed.dataset_id}.json",
                            params=params, timeout=30)
        resp.raise_for_status()
        rows = resp.json()
        return int(rows[0]["n"]) if rows else 0
    except Exception as exc:  # noqa: BLE001 — a dead probe is a skipped day, not a crash
        logger.warning("%s upstream count failed for %s: %s", feed.scraper_name, day, exc)
        return None


def local_count(db, feed: Feed, day: date) -> int:
    return db.execute(
        text(f"SELECT COUNT(*) FROM {feed.table} WHERE {feed.column}::date = :d"),
        {"d": day},
    ).scalar() or 0


def reconcile_feed(db, feed: Feed, window_days: int) -> dict:
    """Compare our counts against the source, day by day, oldest first."""
    today = date.today()
    newest = today - timedelta(days=feed.settle_days)
    days = [newest - timedelta(days=i) for i in range(window_days)]
    days.reverse()

    rows = []
    for day in days:
        up = upstream_count(feed, day)
        if up is None:
            rows.append({"day": day, "upstream": None, "ours": None,
                         "gap": None, "pct": None, "drifted": False})
            continue
        ours = local_count(db, feed, day)
        gap = up - ours
        pct = (gap / up) if up else 0.0
        rows.append({
            "day": day, "upstream": up, "ours": ours, "gap": gap, "pct": pct,
            # Only a shortfall counts. Holding more than the source usually means
            # the source retracted rows, which is not an ingestion defect.
            "drifted": gap > 0 and pct > DRIFT_ALERT_PCT,
        })
    return {"feed": feed, "rows": rows}


def earliest_drift(result: dict) -> date | None:
    drifted = [r["day"] for r in result["rows"] if r["drifted"]]
    return min(drifted) if drifted else None


def rewind_watermark(db, scraper_name: str, to_day: date) -> datetime | None:
    """Move this scraper's stored watermark back so the next run re-reads from
    `to_day`. Returns the new watermark, or None when there was nothing to move.

    Rewinding rather than re-implementing the fetch means healed rows go through
    exactly the same parse, quarantine and upsert path as a normal night.
    """
    floor = datetime.now(timezone.utc) - timedelta(days=MAX_REWIND_DAYS)
    target = datetime.combine(to_day, datetime.min.time(), tzinfo=timezone.utc)
    target = max(target, floor)

    row = db.execute(text("""
        SELECT id, watermark_timestamp FROM scraper_runs
        WHERE scraper_name = :n AND status = 'success'
          AND watermark_timestamp IS NOT NULL
        ORDER BY started_at DESC LIMIT 1
    """), {"n": scraper_name}).fetchone()
    if row is None:
        logger.warning("%s has no successful run to rewind", scraper_name)
        return None
    if row.watermark_timestamp <= target:
        logger.info("%s watermark already at or behind %s", scraper_name, target.date())
        return None

    db.execute(
        text("UPDATE scraper_runs SET watermark_timestamp = :wm WHERE id = :id"),
        {"wm": target, "id": row.id},
    )
    db.commit()
    logger.info("%s watermark rewound %s -> %s",
                scraper_name, row.watermark_timestamp, target)
    return target


def print_report(results: list[dict]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n=== PulseCities upstream reconciliation | {now} ===\n")
    for res in results:
        feed = res["feed"]
        print(f"  {feed.scraper_name}  (settles after {feed.settle_days}d)")
        print(f"    {'day':<12} {'upstream':>9} {'ours':>9} {'gap':>7} {'pct':>7}")
        print("    " + "-" * 48)
        for r in res["rows"]:
            if r["upstream"] is None:
                print(f"    {r['day'].isoformat():<12} {'probe failed':>35}")
                continue
            mark = "  <<" if r["drifted"] else ""
            print(f"    {r['day'].isoformat():<12} {r['upstream']:>9,} {r['ours']:>9,} "
                  f"{r['gap']:>7,} {r['pct']*100:>6.2f}%{mark}")
        drifted = [r for r in res["rows"] if r["drifted"]]
        total_gap = sum(r["gap"] for r in drifted)
        print(f"    {len(drifted)} day(s) drifted, {total_gap:,} rows short\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile our row counts against upstream")
    parser.add_argument("--days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--feed", help="limit to one scraper_name")
    parser.add_argument("--heal", action="store_true",
                        help="rewind the watermark of any drifted feed")
    parser.add_argument("--run", action="store_true",
                        help="with --heal, run the scraper immediately instead of waiting for cron")
    args = parser.parse_args()

    feeds = [f for f in FEEDS if not args.feed or f.scraper_name == args.feed]
    if not feeds:
        print(f"no such feed: {args.feed}", file=sys.stderr)
        return 2

    with get_scraper_db() as db:
        results = [reconcile_feed(db, f, args.days) for f in feeds]

    print_report(results)

    healed: list[str] = []
    drifted_feeds = [r for r in results if earliest_drift(r)]

    if args.heal:
        with get_scraper_db() as db:
            for res in drifted_feeds:
                start = earliest_drift(res)
                name = res["feed"].scraper_name
                if rewind_watermark(db, name, start - timedelta(days=1)):
                    healed.append(f"{name} rewound to {start.isoformat()}")
        if args.run and healed:
            _run_scrapers([r["feed"].scraper_name for r in drifted_feeds])
            # Re-measure before deciding whether to alert. Reporting the numbers
            # we opened with would email a drift warning about rows that were
            # just recovered, which is how an alert channel stops being read.
            with get_scraper_db() as db:
                results = [reconcile_feed(db, r["feed"], args.days) for r in drifted_feeds]
            drifted_feeds = [r for r in results if earliest_drift(r)]
            print("\n=== after healing ===")
            print_report(results)

    for res in drifted_feeds:
        feed = res["feed"]
        drifted = [r for r in res["rows"] if r["drifted"]]
        worst = max(drifted, key=lambda r: r["pct"])
        send_alert(
            f"Ingestion drift: {feed.scraper_name}",
            f"{len(drifted)} of {len(res['rows'])} settled days are short against "
            f"{feed.dataset_id}. Worst: {worst['day']} missing {worst['gap']:,} rows "
            f"({worst['pct']*100:.1f}%). Total {sum(r['gap'] for r in drifted):,} rows.\n\n"
            + ("Healed: watermark rewound, next run re-reads the range."
               if args.heal else
               "Re-read the range with:\n"
               f"  python -m scripts.reconcile_upstream --feed {feed.scraper_name} --heal --run"),
        )
    flush_alerts()

    for line in healed:
        print(f"  healed: {line}")

    return 1 if drifted_feeds and not args.heal else 0


def _run_scrapers(names: list[str]) -> None:
    """Run the named scrapers now, through the pipeline's own retry wrapper."""
    from scheduler.pipeline import _run_scraper_with_retry
    from scrapers.complaints import ComplaintsScraper
    from scrapers.evictions import EvictionsScraper
    from scrapers.permits import PermitsScraper
    from scrapers.violations import ViolationsScraper

    classes = {
        "311_complaints": ComplaintsScraper,
        "hpd_violations": ViolationsScraper,
        "evictions": EvictionsScraper,
        "dob_permits": PermitsScraper,
    }
    for name in names:
        cls = classes.get(name)
        if cls is None:
            continue
        logger.info("re-running %s after rewind", name)
        _run_scraper_with_retry(name, cls)


if __name__ == "__main__":
    sys.exit(main())
