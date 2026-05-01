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

from config.nyc import SOCRATA_BASE_URL
from models.database import get_scraper_db
from scheduler.alerts import send_alert

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# (scraper_name, dataset_id, date_column, threshold_days, db_table, db_date_col)
FRESHNESS_CHECKS = [
    ("acris_ownership", "bnx9-e6tj", "recorded_datetime", 7,  "ownership_raw",  "doc_date"),
    ("dob_permits",     "ipu4-2q9a", "dobrundate",        10, "permits_raw",    "filing_date"),
    ("evictions",       "6z8x-wfk4", "executed_date",     14, "evictions_raw",  "executed_date"),
    ("311_complaints",  "erm2-nwe9", "created_date",       10, "complaints_raw", "created_date"),
    ("hpd_violations",  "wvxf-dwi5", "inspectiondate",    10, "violations_raw", "inspection_date"),
]

# dobrundate is a plain text field in the permits dataset — MAX() on text sorts lexicographically,
# which happens to be correct for YYYYMMDD strings, but the API rejects MAX() on text columns.
# Query by ORDER DESC instead and parse the returned value.
DOBRUNDATE_DATASET = "ipu4-2q9a"


def _app_token_params():
    token = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")
    if token:
        return {"$$app_token": token}
    return {}


def fetch_upstream_max(dataset_id, date_col):
    """
    Return the upstream max date for a dataset as a date object, or None on failure.

    For dobrundate (text field in ipu4-2q9a), uses ORDER DESC + LIMIT 1 instead
    of MAX() to avoid Socrata rejecting an aggregate on a text column.
    """
    params = _app_token_params()

    if dataset_id == DOBRUNDATE_DATASET:
        # All date columns in ipu4-2q9a are stored as text (MM/DD/YYYY).
        # MAX() on text sorts lexicographically and returns wrong results.
        # Order by issuance_date DESC and read the filing_date field from the
        # first result — both columns track the same activity.
        params.update({"$order": "issuance_date DESC", "$limit": 1,
                       "$select": "filing_date"})
        url = f"{SOCRATA_BASE_URL}/{dataset_id}.json"
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                return None
            raw = rows[0].get("filing_date")
            if raw is None:
                return None
            raw = str(raw).strip()
            for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(raw, fmt).date()
                except ValueError:
                    continue
            logger.warning("dob_permits: could not parse filing_date value %r", raw)
            return None
        except Exception as exc:
            logger.warning("dob_permits upstream fetch failed: %s", exc)
            return None

    # Standard path: MAX() aggregate via $select
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


def fetch_db_max(db, table, date_col):
    """Return the max value of date_col from table as a date object, or None."""
    try:
        row = db.execute(
            text(f"SELECT MAX({date_col}) FROM {table}")  # noqa: S608 — table names are hardcoded
        ).scalar()
        if row is None:
            return None
        if isinstance(row, datetime):
            return row.date()
        if isinstance(row, date):
            return row
        return None
    except Exception as exc:
        logger.warning("%s.%s db query failed: %s", table, date_col, exc)
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


def run_checks(db):
    today = date.today()
    results = []

    for scraper_name, dataset_id, date_col, threshold, db_table, db_date_col in FRESHNESS_CHECKS:
        upstream_date = fetch_upstream_max(dataset_id, date_col)
        db_date = fetch_db_max(db, db_table, db_date_col)

        up_days = stale_days(upstream_date, today)
        db_days = stale_days(db_date, today)
        status  = classify_status(up_days, db_days, threshold)

        alert_fired = False
        if status == "stale":
            parts = []
            if up_days is not None and up_days > threshold:
                parts.append(
                    f"upstream max date {upstream_date} is {up_days}d old (threshold {threshold}d)"
                )
            if db_days is not None and db_days > threshold:
                parts.append(
                    f"DB table {db_table} max date {db_date} is {db_days}d old (threshold {threshold}d)"
                )
            body = "; ".join(parts) if parts else "source data stale — check manually"
            send_alert(f"Source data stale: {scraper_name}", body)
            alert_fired = True

        results.append({
            "scraper_name":      scraper_name,
            "upstream_max_date": upstream_date.isoformat() if upstream_date else None,
            "db_max_date":       db_date.isoformat() if db_date else None,
            "upstream_stale_days": up_days,
            "db_stale_days":     db_days,
            "threshold_days":    threshold,
            "status":            status,
            "alert_fired":       alert_fired,
        })

    return results


def print_report(results):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n=== PulseCities Upstream Freshness | {now_str} ===\n")
    print(f"  {'scraper':<22} {'status':<8} {'upstream':<12} {'db':<12} {'up_days':>7} {'db_days':>7} {'thr':>4}  alert")
    print("  " + "-" * 82)
    for r in results:
        print(
            f"  {r['scraper_name']:<22} {r['status']:<8} "
            f"{(r['upstream_max_date'] or 'N/A'):<12} "
            f"{(r['db_max_date'] or 'N/A'):<12} "
            f"{(str(r['upstream_stale_days']) if r['upstream_stale_days'] is not None else '?'):>7} "
            f"{(str(r['db_stale_days']) if r['db_stale_days'] is not None else '?'):>7} "
            f"{r['threshold_days']:>4}  "
            f"{'YES' if r['alert_fired'] else 'no'}"
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
    sys.exit(1 if stale_count else 0)


if __name__ == "__main__":
    main()
