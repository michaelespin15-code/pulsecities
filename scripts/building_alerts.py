"""
Building watch alerts — daily email when new records land on a watched BBL.

Subscribers with a bbl watch exactly one building. After each nightly scrape
this scan looks for records ingested since the last run (created_at, not the
document date, so ACRIS backfills still alert) across four tables: deeds,
permits, evictions, and HPD violations. One email per watch per run, listing
everything new at that address. Quiet nights send nothing.

    PYTHONPATH=. venv/bin/python -m scripts.building_alerts [--dry-run]

Cron runs this daily after the scrape (see deploy/pulsecities.cron). State is
a single watermark in building_alerts_state.json; a missing file starts the
window 24 hours back, so a fresh deploy can't flood anyone with history.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import resend
from sqlalchemy import text

from config.logging_config import configure_logging
from models.database import get_scraper_db

resend.api_key = os.getenv("RESEND_API_KEY", "")

configure_logging()
logger = logging.getLogger(__name__)

STATE_PATH = Path(__file__).parent / "building_alerts_state.json"

# Per-table caps keep a violation-heavy building from producing a novel.
MAX_EVENTS_PER_KIND = 10

_EVENT_WHERE = {
    "deed": "FROM ownership_raw WHERE bbl = :bbl AND created_at > :since"
            " AND doc_type IN ('DEED', 'DEEDP') AND party_type = '2'",
    "permit": "FROM permits_raw WHERE bbl = :bbl AND created_at > :since",
    "eviction": "FROM evictions_raw WHERE bbl = :bbl AND created_at > :since",
    "violation": "FROM violations_raw WHERE bbl = :bbl AND created_at > :since",
}

_EVENT_SQL = {
    "deed": f"""
        SELECT document_id AS ref, doc_date AS event_date, party_name, doc_amount
        {_EVENT_WHERE['deed']}
        ORDER BY doc_date DESC LIMIT :cap
    """,
    "permit": f"""
        SELECT permit_type AS ref, filing_date AS event_date, work_type, job_description
        {_EVENT_WHERE['permit']}
        ORDER BY filing_date DESC LIMIT :cap
    """,
    "eviction": f"""
        SELECT docket_number AS ref, executed_date AS event_date, eviction_type
        {_EVENT_WHERE['eviction']}
        ORDER BY executed_date DESC LIMIT :cap
    """,
    "violation": f"""
        SELECT violation_id AS ref,
               COALESCE(nov_issued_date, inspection_date) AS event_date,
               violation_class, description
        {_EVENT_WHERE['violation']}
        ORDER BY COALESCE(nov_issued_date, inspection_date) DESC NULLS LAST LIMIT :cap
    """,
}


def _fmt_date(d) -> str:
    return d.strftime("%b %-d, %Y") if d else "date not on record"


def _money(n) -> str:
    return f"${float(n):,.0f}" if n else "no amount on record"


def _describe(kind: str, row) -> str:
    if kind == "deed":
        who = (row.party_name or "unknown party").strip()
        return f"Deed transfer to {who} for {_money(row.doc_amount)}, dated {_fmt_date(row.event_date)}."
    if kind == "permit":
        what = (row.work_type or row.ref or "work").strip()
        desc = (row.job_description or "").strip()
        line = f"Permit filed ({what}), {_fmt_date(row.event_date)}."
        if desc:
            line += f" {desc[:140]}"
        return line
    if kind == "eviction":
        return f"{(row.eviction_type or 'Eviction').strip()} eviction executed, {_fmt_date(row.event_date)}, docket {row.ref}."
    cls = f"class {row.violation_class}" if row.violation_class else "unclassified"
    desc = (row.description or "").strip()
    line = f"HPD violation issued ({cls}), {_fmt_date(row.event_date)}."
    if desc:
        line += f" {desc[:140]}"
    return line


def scan(db, since: datetime) -> list[dict]:
    """One entry per watch with new records: subscriber, building, event lines."""
    watches = db.execute(text("""
        SELECT s.email, s.bbl, s.unsubscribe_token, p.address, p.zip_code
        FROM subscribers s
        LEFT JOIN parcels p ON p.bbl = s.bbl
        WHERE s.bbl IS NOT NULL AND s.confirmed = true
        ORDER BY s.email, s.bbl
    """)).fetchall()

    alerts = []
    for w in watches:
        events, total = [], 0
        for kind, sql in _EVENT_SQL.items():
            params = {"bbl": w.bbl, "since": since}
            total += db.execute(
                text(f"SELECT count(*) {_EVENT_WHERE[kind]}"), params
            ).scalar() or 0
            rows = db.execute(text(sql), {**params, "cap": MAX_EVENTS_PER_KIND}).fetchall()
            events.extend({"kind": kind, "line": _describe(kind, r)} for r in rows)
        if events:
            alerts.append({
                "email": w.email,
                "bbl": w.bbl,
                "address": ((w.address or "").strip().title() or f"BBL {w.bbl}"),
                "token": w.unsubscribe_token,
                "events": events,
                "total": total,
            })
    return alerts


# Same paper case-file shell as the digests and welcome notes.
_ALERT_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>New records at {address}</title>
</head>
<body style="margin:0;padding:0;background:#EFEBE2;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#EFEBE2;padding:36px 16px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:540px;">
        <tr><td style="background:#FBFAF7;border:1px solid #D9D4C9;border-top:3px solid #E4590F;padding:28px 28px 26px;">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr><td style="padding-bottom:10px;border-bottom:2px solid #1C2430;">
              <table width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:14px;font-weight:700;color:#1C2430;letter-spacing:0.2em;">PULSECITIES</td>
                <td align="right" style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;color:#6D7480;letter-spacing:0.14em;">BUILDING WATCH</td>
              </tr></table>
            </td></tr>
            <tr><td style="padding:10px 0 22px;">
              <span style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;color:#9A948A;letter-spacing:0.14em;text-transform:uppercase;">{address} &middot; BBL {bbl} &middot; {count_line}</span>
            </td></tr>
            {event_rows}
            <tr><td style="padding-top:16px;">
              <p style="margin:0;font-family:Georgia,'Times New Roman',serif;font-size:15px;color:#1C2430;line-height:1.7;">The building's full record:<br><a href="https://pulsecities.com/property/{bbl}" style="color:#C2410C;">pulsecities.com/property/{bbl}</a></p>
            </td></tr>
          </table>
        </td></tr>
        <tr><td style="padding:16px 6px 0;">
          <p style="margin:0;font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;color:#8A8578;line-height:1.7;">You're watching this building at pulsecities.com. <a href="https://pulsecities.com/api/unsubscribe?token={token}" style="color:#8A8578;">Stop watching</a> anytime, one click.</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>
""".strip()

_EVENT_ROW = ('<tr><td style="padding-bottom:14px;"><p style="margin:0;'
              "font-family:Georgia,'Times New Roman',serif;font-size:15px;"
              'color:#1C2430;line-height:1.7;">{}</p></td></tr>')


def _esc(s: str) -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def build_email(alert: dict) -> tuple[str, str, str]:
    """Returns (subject, html, text) for one watch alert."""
    total = alert.get("total") or len(alert["events"])
    shown = len(alert["events"])
    count_line = f"{total} new record{'s' if total != 1 else ''}"
    subject = f"New at {alert['address']}: {count_line}"

    rows = "".join(_EVENT_ROW.format(_esc(e["line"])) for e in alert["events"])
    if total > shown:
        rows += _EVENT_ROW.format(
            f"And {total - shown} more, all on the building page below.")
    html = (_ALERT_HTML
            .replace("{address}", _esc(alert["address"]))
            .replace("{bbl}", _esc(alert["bbl"]))
            .replace("{count_line}", count_line)
            .replace("{event_rows}", rows)
            .replace("{token}", alert["token"]))

    text_lines = [f"New public records at {alert['address']} (BBL {alert['bbl']}):", ""]
    text_lines += [f"- {e['line']}" for e in alert["events"]]
    if total > shown:
        text_lines += [f"- And {total - shown} more, all on the building page below."]
    text_lines += ["", f"Full record: https://pulsecities.com/property/{alert['bbl']}", "",
                   "Stop watching: https://pulsecities.com/api/unsubscribe?token=" + alert["token"]]
    return subject, html, "\n".join(text_lines)


def _wait_for_pipeline(max_wait_s: int = 2700, poll_s: int = 60) -> bool:
    """
    The 03:25 cron slot assumes the 02:00 pipeline has finished. When a backlog
    run overshoots (an upstream source unfreezing, say), scanning mid-ingest
    advances the watermark past rows that commit minutes later, and a watcher
    permanently misses the deed on their building. Wait for the lock to clear;
    give up after max_wait_s so a wedged pipeline can't hang the cron forever.
    """
    import time
    lock = Path("/tmp/pulsecities_pipeline.lock")
    waited = 0
    while waited <= max_wait_s:
        if not lock.exists():
            return True
        try:
            pid = int(lock.read_text().strip())
            os.kill(pid, 0)
        except (ValueError, OSError, ProcessLookupError):
            return True  # stale lock; the pipeline itself cleans these up
        if waited == 0:
            logger.info("Nightly pipeline still running (lock pid %s); waiting", pid)
        time.sleep(poll_s)
        waited += poll_s
    logger.error("Pipeline lock still held after %ds; skipping this alert run "
                 "(watermark not advanced, tomorrow's run covers the gap)", max_wait_s)
    return False


def run(dry_run: bool = False) -> None:
    if not dry_run and not resend.api_key:
        logger.error("RESEND_API_KEY not set. Aborting building alerts.")
        sys.exit(1)

    if not _wait_for_pipeline():
        sys.exit(1)

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=24)
    if STATE_PATH.exists():
        # A corrupt state file must not become a permanent crash loop; fall
        # back to the bounded 24h window and let this run rewrite it.
        try:
            state = json.loads(STATE_PATH.read_text())
            if state.get("last_run"):
                since = datetime.fromisoformat(state["last_run"])
        except (ValueError, OSError):
            logger.warning("Unreadable state file %s; using the 24h window", STATE_PATH)

    with get_scraper_db() as db:
        alerts = scan(db, since)

    logger.info("Building-alert scan complete: %d watch(es) with new records since %s",
                len(alerts), since.isoformat())

    any_failed = False
    for alert in alerts:
        subject, html, text_body = build_email(alert)
        if dry_run:
            print(f"--- would send to {alert['email']}: {subject}")
            print(text_body)
            print()
            continue
        try:
            resend.Emails.send({
                "from": "PulseCities <alerts@pulsecities.com>",
                "to": [alert["email"]],
                "subject": subject,
                "html": html,
                "text": text_body,
                "headers": {
                    "List-Unsubscribe": f"<https://pulsecities.com/api/unsubscribe?token={alert['token']}>",
                    "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
                },
            })
            logger.info("Alert sent to %s for %s (%d events)",
                        alert["email"], alert["bbl"], len(alert["events"]))
        except Exception:
            any_failed = True
            logger.exception("Failed to send building alert to %s", alert["email"])

    if not dry_run:
        if any_failed:
            # Keep the old watermark so the failed alerts are retried next
            # run. The successful recipients get a duplicate, which beats a
            # watcher silently missing a deed on their building.
            logger.warning("Send failures; watermark not advanced, next run retries.")
            return
        tmp = STATE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"last_run": now.isoformat()}))
        os.replace(tmp, STATE_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily building-watch alert scan")
    parser.add_argument("--dry-run", action="store_true",
                        help="print alerts instead of sending, don't advance the watermark")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
