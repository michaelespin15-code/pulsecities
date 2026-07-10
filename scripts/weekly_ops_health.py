"""
Weekly ops-health email for PulseCities.

A proactive heartbeat, distinct from the nightly pipeline alert (which only
fires on failure). Every Sunday this reports the handful of numbers that,
left unwatched, let drift accumulate silently until an audit finds it:

  - feed watermarks: latest record date per source, and days since
  - dupe guards: total vs distinct-identity rows on the tables that had the
    NULL-bbl re-ingestion bug (0 = healthy)
  - subscriber totals: confirmed ZIP / citywide / operator followers
  - backup: age of the newest dump and the last restore-test result

The subject line says ALL CLEAR or NEEDS ATTENTION so the inbox is scannable.
Never raises; emails through the same Resend account as the digest.

Usage:
    python -m scripts.weekly_ops_health            # compute and email
    python -m scripts.weekly_ops_health --dry-run  # print, no email
"""

import argparse
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

from sqlalchemy import text

from config.logging_config import configure_logging
from models.database import get_scraper_db  # imports load_dotenv() as a side effect
from scheduler.alerts import send_ops_email

configure_logging()
logger = logging.getLogger(__name__)

RESTORE_RESULT = Path("/var/log/pulsecities/backup_restore_test.json")
BACKUP_DIR = Path("/var/backups/pulsecities")

# (label, table, date column). Deed data is expected stale while the ACRIS
# feed is paused upstream, so its threshold is wider.
WATERMARKS = [
    ("311 complaints", "complaints_raw", "created_date", 3),
    ("DOB permits", "permits_raw", "filing_date", 4),
    ("Evictions", "evictions_raw", "executed_date", 5),
    ("HPD violations", "violations_raw", "inspection_date", 4),
    ("ACRIS deeds", "ownership_raw", "doc_date", 60),
]

# Tables whose unique key includes a nullable bbl — the NULL-bbl bug class.
DUPE_GUARDS = [
    ("evictions_raw", "COALESCE(bbl, ''), executed_date, docket_number"),
    ("permits_raw", "COALESCE(bbl, ''), filing_date, permit_type, work_type"),
]


def gather(db) -> dict:
    attention: list[str] = []
    lines: list[str] = []

    lines.append("FEED WATERMARKS")
    today = date.today()
    for label, table, col, max_age in WATERMARKS:
        latest = db.execute(text(f"SELECT MAX({col})::date FROM {table}")).scalar()
        if latest is None:
            lines.append(f"  {label:<16} no data")
            attention.append(f"{label}: table empty")
            continue
        age = (today - latest).days
        flag = ""
        if age > max_age:
            flag = f"  << stale (>{max_age}d)"
            attention.append(f"{label}: {age}d behind")
        lines.append(f"  {label:<16} {latest.isoformat()}  ({age}d ago){flag}")

    lines.append("")
    lines.append("DUPE GUARDS  (total vs distinct identity; equal = healthy)")
    for table, key in DUPE_GUARDS:
        total, distinct = db.execute(text(
            f"SELECT COUNT(*), COUNT(DISTINCT ({key})) FROM {table}"
        )).fetchone()
        dupes = total - distinct
        flag = ""
        if dupes > 0:
            flag = f"  << {dupes} duplicate rows"
            attention.append(f"{table}: {dupes} duplicate rows")
        lines.append(f"  {table:<16} {total:>9,} rows / {distinct:>9,} distinct{flag}")

    lines.append("")
    lines.append("SUBSCRIBERS  (confirmed)")
    subs = db.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE confirmed AND zip_code IS NOT NULL AND NOT is_citywide) AS zip,
            COUNT(*) FILTER (WHERE confirmed AND is_citywide) AS citywide,
            COUNT(*) FILTER (WHERE confirmed AND operator_slug IS NOT NULL) AS operator,
            COUNT(*) FILTER (WHERE NOT confirmed) AS pending
        FROM subscribers
    """)).fetchone()
    lines.append(f"  ZIP {subs.zip}   citywide {subs.citywide}   operator {subs.operator}   pending {subs.pending}")

    lines.append("")
    lines.append("BACKUP")
    dumps = sorted(BACKUP_DIR.glob("pulsecities_*.sql.gz"))
    if not dumps:
        lines.append("  no dumps found")
        attention.append("no database backups on disk")
    else:
        newest = dumps[-1]
        age_h = (datetime.now().timestamp() - newest.stat().st_mtime) / 3600
        size_gb = newest.stat().st_size / 1e9
        flag = "  << older than 48h" if age_h > 48 else ""
        lines.append(f"  newest {newest.name}  {size_gb:.1f}GB  ({age_h:.0f}h old){flag}")
        if age_h > 48:
            attention.append("newest backup older than 48h")

    if RESTORE_RESULT.exists():
        r = json.loads(RESTORE_RESULT.read_text())
        status = r.get("status", "unknown")
        lines.append(f"  restore-test: {status.upper()} — {r.get('detail', '')}")
        if status not in ("pass", "skipped"):
            attention.append(f"backup restore-test {status}")
        elif status == "skipped":
            attention.append("backup restore-test skipped (see detail)")
    else:
        lines.append("  restore-test: never run")
        attention.append("backup restore-test has never run")

    return {"attention": attention, "body": "\n".join(lines)}


def run(dry_run: bool = False) -> None:
    with get_scraper_db() as db:
        report = gather(db)

    attention = report["attention"]
    header = (
        "ALL CLEAR — no ops issues this week."
        if not attention
        else "NEEDS ATTENTION:\n" + "\n".join(f"  - {a}" for a in attention)
    )
    body = f"PulseCities weekly ops health, {date.today().isoformat()}\n\n{header}\n\n{report['body']}\n"
    subject = "Weekly ops health: ALL CLEAR" if not attention else f"Weekly ops health: {len(attention)} item(s) need attention"

    if dry_run:
        print(f"Subject: {subject}\n\n{body}")
    else:
        send_ops_email(subject, body)
        logger.info("Weekly ops-health email sent (%d attention items)", len(attention))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weekly ops-health email")
    parser.add_argument("--dry-run", action="store_true", help="print, do not email")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
