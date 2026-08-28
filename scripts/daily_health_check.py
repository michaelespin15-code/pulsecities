"""
PulseCities upstream freshness check.

Queries each key Socrata dataset directly for its max date column and compares
against our internal DB tables.  Fires alerts through scheduler.alerts.send_alert
when either the upstream source or our own table is stale.

Writes a machine-readable audit to audits/freshness_YYYYMMDD.json and prints
a human-readable summary to stdout.

Usage:
    python -m scripts.daily_health_check
    python scripts/daily_health_check.py
"""

import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from sqlalchemy import text

from api.freshness import db_through_sql, staleness_days
from config.nyc import SOCRATA_BASE_URL
from models.database import get_scraper_db
from scheduler.alerts import flush_alerts, send_alert

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# (scraper_name, dataset_id, upstream date column). The threshold and the
# matching local query both come from api/freshness.py: this job alerts, it does
# not get to hold its own opinion about how stale a feed is.
FRESHNESS_CHECKS = [
    ("acris_ownership", "bnx9-e6tj", "recorded_datetime"),
    # DOB NOW, not legacy BIS. Both write permits_raw so fetch_db_max reads the
    # same value either way, which means checking BIS would be green by
    # construction: DOB NOW's dates would keep the union fresh however dead BIS
    # got. Checking the source that actually carries the permits is the only
    # version of this check that can fail.
    ("dob_now_permits", "w9ak-ipjd", "first_permit_date"),
    ("evictions",       "6z8x-wfk4", "executed_date"),
    ("311_complaints",  "erm2-nwe9", "created_date"),
    ("hpd_violations",  "wvxf-dwi5", "inspectiondate"),
]


# A source that has stopped publishing gets re-reported on this cadence instead
# of nightly. Long enough that a weeks-long upstream freeze cannot train anyone
# to filter the ops mailbox, short enough that it cannot be forgotten either.
UPSTREAM_REALERT_DAYS = 7

# Which feeds are mid-alert, so an unchanged upstream freeze is not re-sent every
# night. A nightly artifact, not source: see .gitignore.
STATE_PATH = Path(__file__).parent / "freshness_alert_state.json"


def _read_alert_state():
    """Per-feed alert bookkeeping, or {} when it is missing or unreadable.

    A corrupt state file must not stop the checks from running. Losing it means
    re-sending one alert, which is the safe direction to fail.
    """
    try:
        state = json.loads(STATE_PATH.read_text())
        return state if isinstance(state, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("freshness alert state unreadable (%s); treating as empty", exc)
        return {}


def _write_alert_state(state):
    try:
        tmp = STATE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, indent=1, sort_keys=True))
        os.replace(tmp, STATE_PATH)
    except Exception:
        # Bookkeeping only. Failing to record it re-sends an alert next run;
        # raising here would lose the checks that already passed.
        logger.exception("could not write freshness alert state")


def _should_alert(scraper_name, cause, today, state):
    """Whether to email about this feed now, and the updated state entry.

    An ingest gap alerts every run. An upstream freeze alerts when first seen
    and then on UPSTREAM_REALERT_DAYS, carrying its age so the repeat reads as
    an age report rather than as news.
    """
    prior = state.get(scraper_name) or {}
    first_seen = prior.get("first_seen") if prior.get("cause") == cause else None
    entry = {
        "cause": cause,
        "first_seen": first_seen or today.isoformat(),
        "last_alerted": prior.get("last_alerted"),
    }

    if cause == "ingest":
        entry["last_alerted"] = today.isoformat()
        return True, entry

    last = prior.get("last_alerted") if prior.get("cause") == cause else None
    if last:
        try:
            if (today - date.fromisoformat(last)).days < UPSTREAM_REALERT_DAYS:
                return False, entry
        except ValueError:
            pass  # Unparseable timestamp: alert and rewrite it.

    entry["last_alerted"] = today.isoformat()
    return True, entry


def _app_token_params():
    token = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")
    if token:
        return {"$$app_token": token}
    return {}


def fetch_upstream_max(dataset_id, date_col):
    """
    Return the upstream max date for a dataset as a date object, or None on failure.

    Every dataset takes the same MAX() aggregate. The permits feed used to get a
    bespoke branch, on the theory that all of ipu4-2q9a's date columns are text
    and so MAX() would sort lexicographically. dobrundate, the column actually
    checked here, is a calendar_date. Worse, the workaround ordered by
    issuance_date, which really is text, so it reproduced the exact bug it was
    written to avoid and reported a one-day-old feed as 1,974 days stale.
    """
    params = _app_token_params()
    params.update({"$select": f"MAX({date_col}) AS max_dt", "$limit": 1})
    url = f"{SOCRATA_BASE_URL}/{dataset_id}.json"
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            return None
        raw = rows[0].get("max_dt")
        if raw is None:
            return None
        raw = str(raw).strip()
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        logger.warning("%s: could not parse date value %r", dataset_id, raw)
        return None
    except Exception as exc:
        logger.warning("%s upstream fetch failed: %s", dataset_id, exc)
        return None


def fetch_db_max(db, scraper_name):
    """Newest record we hold for this feed that is not dated in the future.

    A bare MAX() trusted two ACRIS rows carrying a filer-typed doc_date ten days
    out, which put db_stale_days at -10. No threshold can be exceeded by a
    negative number, so this half of the check was green by construction and
    would have stayed green through a total ingest failure.
    """
    try:
        row = db.execute(text(db_through_sql(scraper_name))).scalar()
        if row is None:
            return None
        if isinstance(row, datetime):
            return row.date()
        if isinstance(row, date):
            return row
        return None
    except Exception as exc:
        logger.warning("%s db freshness query failed: %s", scraper_name, exc)
        return None


def stale_days(max_date, today):
    if max_date is None:
        return None
    return (today - max_date).days


def classify_status(upstream_days, db_days, threshold):
    """
    ok     — both sources within threshold
    warn   — one source is None (unknown) but nothing is confirmed stale
    stale  — either source exceeds threshold
    """
    if upstream_days is not None and upstream_days > threshold:
        return "stale"
    if db_days is not None and db_days > threshold:
        return "stale"
    if upstream_days is None or db_days is None:
        return "warn"
    return "ok"


def stale_cause(upstream_days, db_days, threshold):
    """Why a feed is stale: 'upstream', 'ingest', or None when it isn't.

    These are different findings that happen to look identical from here. When
    the source itself has stopped publishing, our matching lag is a consequence
    of that and there is nothing on this box to fix; the finding is true on the
    first night and unchanged on the thirtieth. When the source is current and
    we are behind it, that is our bug and it is worth waking someone for.

    Collapsing both into one alert is what makes a nightly page ignorable, and
    an ignored channel is the failure that hid the 2026-08-15 outage. The caller
    uses this to keep the second case loud and put the first on a slow cadence.
    """
    if upstream_days is not None and upstream_days > threshold:
        return "upstream"
    if db_days is not None and db_days > threshold:
        return "ingest"
    return None


def run_checks(db):
    today = date.today()
    results = []
    state = _read_alert_state()

    for scraper_name, dataset_id, date_col in FRESHNESS_CHECKS:
        threshold = staleness_days(scraper_name)
        upstream_date = fetch_upstream_max(dataset_id, date_col)
        db_date = fetch_db_max(db, scraper_name)

        up_days = stale_days(upstream_date, today)
        db_days = stale_days(db_date, today)
        status  = classify_status(up_days, db_days, threshold)

        cause = stale_cause(up_days, db_days, threshold) if status == "stale" else None

        alert_fired = False
        if cause:
            alert_fired, entry = _should_alert(scraper_name, cause, today, state)
            state[scraper_name] = entry
            if alert_fired:
                send_alert(*_stale_alert(
                    scraper_name, cause, upstream_date, db_date,
                    up_days, db_days, threshold, entry, today,
                ))
        else:
            # Recovered, or never stale. Drop the entry so the next freeze is
            # reported as new rather than being silenced by a stale timestamp.
            state.pop(scraper_name, None)

        results.append({
            "scraper_name":      scraper_name,
            "upstream_max_date": upstream_date.isoformat() if upstream_date else None,
            "db_max_date":       db_date.isoformat() if db_date else None,
            "upstream_stale_days": up_days,
            "db_stale_days":     db_days,
            "threshold_days":    threshold,
            "status":            status,
            "stale_cause":       cause,
            "alert_fired":       alert_fired,
        })

    _write_alert_state(state)
    return results


def _stale_alert(scraper_name, cause, upstream_date, db_date,
                 up_days, db_days, threshold, entry, today):
    """Build the (subject, body) for a stale feed, naming which failure it is."""
    if cause == "upstream":
        held_since = entry.get("first_seen")
        age = ""
        if held_since:
            try:
                days = (today - date.fromisoformat(held_since)).days
                age = f" Reported for {days}d." if days else ""
            except ValueError:
                pass
        return (
            f"Upstream feed frozen: {scraper_name}",
            f"{scraper_name} last published {upstream_date} ({up_days}d ago, "
            f"threshold {threshold}d). Our ingest is current with the source, so "
            f"there is nothing to fix here until the publisher resumes.{age} "
            f"Re-reported every {UPSTREAM_REALERT_DAYS}d while it stays frozen."
        )

    return (
        f"Ingest behind live source: {scraper_name}",
        f"{scraper_name} publishes through {upstream_date} but our newest record "
        f"is {db_date} ({db_days}d old, threshold {threshold}d). The source is "
        f"current, so this gap is ours."
    )


def print_report(results):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n=== PulseCities Upstream Freshness | {now_str} ===\n")
    print(f"  {'scraper':<22} {'status':<8} {'upstream':<12} {'db':<12} {'up_days':>7} {'db_days':>7} {'thr':>4}  {'cause':<9} alert")
    print("  " + "-" * 92)
    for r in results:
        # A stale feed with alert=no is on the re-alert cadence, not unnoticed.
        # Printing the cause keeps that readable without opening the state file.
        alert = "YES" if r["alert_fired"] else ("held" if r.get("stale_cause") else "no")
        print(
            f"  {r['scraper_name']:<22} {r['status']:<8} "
            f"{(r['upstream_max_date'] or 'N/A'):<12} "
            f"{(r['db_max_date'] or 'N/A'):<12} "
            f"{(str(r['upstream_stale_days']) if r['upstream_stale_days'] is not None else '?'):>7} "
            f"{(str(r['db_stale_days']) if r['db_stale_days'] is not None else '?'):>7} "
            f"{r['threshold_days']:>4}  "
            f"{(r.get('stale_cause') or '-'):<9} "
            f"{alert}"
        )
    print()

    stale = [r for r in results if r["status"] == "stale"]
    warn  = [r for r in results if r["status"] == "warn"]
    ok    = [r for r in results if r["status"] == "ok"]
    print(f"  ok={len(ok)}  warn={len(warn)}  stale={len(stale)}")
    print()


def write_audit(results):
    audit_dir = Path(__file__).parent.parent / "audits"
    audit_dir.mkdir(exist_ok=True)
    fname = audit_dir / f"freshness_{date.today().strftime('%Y%m%d')}.json"
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "checks": results,
    }
    fname.write_text(json.dumps(payload, indent=2))
    print(f"  Audit written: {fname}")
    print()


def main():
    with get_scraper_db() as db:
        results = run_checks(db)

    print_report(results)
    write_audit(results)

    stale_count = sum(1 for r in results if r["status"] == "stale")
    # send_alert only buffers for the ops email; without this flush the
    # staleness alerts never leave the box.
    flush_alerts()
    sys.exit(1 if stale_count else 0)


if __name__ == "__main__":
    main()
