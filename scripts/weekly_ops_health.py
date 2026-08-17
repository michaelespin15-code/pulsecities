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
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from sqlalchemy import text

from api.freshness import through_sql
from config.logging_config import configure_logging
from models.database import get_scraper_db  # imports load_dotenv() as a side effect
from scheduler.alerts import active_snoozes, send_ops_email, stale_snoozes

configure_logging()
logger = logging.getLogger(__name__)

RESTORE_RESULT = Path("/var/log/pulsecities/backup_restore_test.json")
BACKUP_DIR = Path("/var/backups/pulsecities")

# Written by backup_offsite.sh after each verified push. Reading its record
# rather than listing R2 keeps the bucket credentials in one place.
OFFSITE_SLOTS = Path("/var/log/pulsecities/backup_offsite_slots.json")

# The seven daily slots rotate weekly, so a healthy slot is at most 7 days old.
# One day of slack absorbs a late run without crying wolf.
OFFSITE_SLOT_MAX_AGE_DAYS = 8

_WEEKDAY_SLOTS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

# A snooze is an alert we have chosen not to hear. Past this age, say so weekly:
# the DCWP entry outlived its own justification and nothing was watching it.
SNOOZE_REVIEW_DAYS = 30

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
        # Same future-date exclusion as /api/status and the nightly checks. A
        # bare MAX() here printed "ACRIS deeds 2026-08-27 (-10d ago)" off two
        # filer-typed dates, and a negative age can never trip a threshold.
        latest = db.execute(text(through_sql(table, col))).scalar()
        if isinstance(latest, datetime):
            latest = latest.date()
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
    lines.append("SNOOZED ALERTS  (deliberate blind spots)")
    snoozes = active_snoozes()
    if not snoozes:
        lines.append("  none")
    else:
        stale_patterns = {p for p, _set_on, _age in stale_snoozes(SNOOZE_REVIEW_DAYS)}
        for pattern, set_on in snoozes:
            if set_on is None:
                lines.append(f"  {pattern}  (no set date)  << undated, cannot be reviewed")
                attention.append(f"snooze has no set date: {pattern}")
                continue
            age = (today - set_on).days
            flag = ""
            if pattern in stale_patterns:
                flag = f"  << set {age}d ago, still needed?"
                attention.append(f"snooze {age}d old: {pattern}")
            lines.append(f"  {pattern}  (set {set_on.isoformat()}){flag}")

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
        # The producer builds this JSON with shell interpolation; treat a bad
        # parse as a failing check, not a crash that kills the whole report.
        try:
            r = json.loads(RESTORE_RESULT.read_text())
        except (json.JSONDecodeError, OSError):
            r = {"status": "unreadable", "detail": f"could not parse {RESTORE_RESULT}"}
        status = r.get("status", "unknown")
        lines.append(f"  restore-test: {status.upper()}: {r.get('detail', '')}")
        if status not in ("pass", "skipped"):
            attention.append(f"backup restore-test {status}")
        elif status == "skipped":
            attention.append("backup restore-test skipped (see detail)")
    else:
        lines.append("  restore-test: never run")
        attention.append("backup restore-test has never run")

    lines.extend(_offsite_lines(attention))

    return {"attention": attention, "body": "\n".join(lines)}


def _offsite_lines(attention: list) -> list:
    """Age each R2 weekday slot, appending anything worth a look to attention.

    The offsite copy rotates through seven daily slots, so a single weekday that
    stops pushing leaves one slot rotting while the other six look current. Every
    check we had reported the newest object, which stays fresh throughout: the
    'sat' slot sat nine days stale and nothing said so. Only a per-slot age can
    see it.
    """
    lines = ["", "OFFSITE (R2)"]

    if not OFFSITE_SLOTS.exists():
        lines.append("  no slot pushes recorded yet")
        attention.append("offsite slot ages not recorded yet")
        return lines

    try:
        state = json.loads(OFFSITE_SLOTS.read_text())
        if not isinstance(state, dict):
            raise ValueError("slot state is not an object")
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        lines.append(f"  slot state unreadable: {exc}")
        attention.append("offsite slot state unreadable")
        return lines

    now = datetime.now(timezone.utc)
    ages = {}
    for key, rec in state.items():
        stamp = (rec or {}).get("pushed_at")
        if not stamp:
            continue
        try:
            ages[key] = (now - datetime.fromisoformat(stamp)).days
        except (TypeError, ValueError):
            continue

    # Until a full rotation has been observed, an unseen slot means "not yet
    # recorded", not "failing". Reporting it as a fault on day one would train
    # the reader to skip this section during the week it is least reliable.
    tracking_days = max(ages.values()) if ages else 0
    rotation_observed = tracking_days >= OFFSITE_SLOT_MAX_AGE_DAYS

    for day in _WEEKDAY_SLOTS:
        key = f"daily/{day}.sql.gz"
        age = ages.get(key)
        if age is None:
            if rotation_observed:
                lines.append(f"  daily/{day}  never pushed  << slot has never landed")
                attention.append(f"offsite slot {day} has never been pushed")
            else:
                lines.append(f"  daily/{day}  not yet recorded")
            continue
        flag = ""
        if age > OFFSITE_SLOT_MAX_AGE_DAYS:
            flag = f"  << {age}d old, that weekday's push is failing"
            attention.append(f"offsite slot {day} is {age}d old")
        lines.append(f"  daily/{day}  {age}d old{flag}")

    monthly = sorted(k for k in ages if k.startswith("monthly/"))
    if monthly:
        newest = monthly[-1]
        lines.append(f"  {newest}  {ages[newest]}d old")

    return lines


def run(dry_run: bool = False) -> None:
    with get_scraper_db() as db:
        report = gather(db)

    attention = report["attention"]
    header = (
        "ALL CLEAR. No ops issues this week."
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
    try:
        run(dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001
        # A silently missing heartbeat is the weakest possible failure signal
        # for the health system itself. Say so instead.
        logger.error("ops-health report crashed: %s", exc, exc_info=True)
        send_ops_email(
            "Weekly ops health: report FAILED to generate",
            f"weekly_ops_health.py crashed before it could report:\n\n{exc}\n\n"
            f"  tail -50 /var/log/pulsecities/ops_health.log",
        )
        sys.exit(1)
