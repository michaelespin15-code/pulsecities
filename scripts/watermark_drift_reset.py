"""
Watermark drift checker and auto-reset for PulseCities scrapers.

For each incremental scraper:
  1. Reads the current watermark_timestamp from scraper_runs (last successful run).
  2. Queries the Socrata source dataset for MAX(event_date_field) to detect drift.
     Falls back to MAX(local_table_col) when Socrata is unavailable or inapplicable.
  3. Drift condition: watermark > max_source_date + 1 day.
  4. Reset: inserts a synthetic scraper_run row with
     watermark_timestamp = MAX(local_event_col) - 7 days.
     This synthetic row becomes the new "most recent successful run" that
     get_watermark() picks up, resetting the incremental fetch window.

Full-refresh scrapers (dhcr_rs, mappluto, dof_assessments) are skipped — they
always return None as their watermark and do not use incremental fetching.

Notes:
  - DOB permits (dob_permits): filing_date is stored as MM/DD/YYYY text in
    Socrata, so SoQL MAX() is lexicographic and meaningless. Drift is checked
    against the local permits_raw table only.
  - ACRIS ownership (acris_ownership): recorded_datetime (the watermark field)
    is not stored in ownership_raw locally. Socrata MAX(recorded_datetime) is
    used for drift detection; local MAX(created_at) is the ingestion-time proxy
    for the reset target.

Output: scripts/watermark_drift_reset.json

Usage:
    python scripts/watermark_drift_reset.py [--dry-run]
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.database import get_scraper_db
from models.scraper import ScraperRun

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

OUTPUT = Path(__file__).parent / "watermark_drift_reset.json"
SOCRATA_BASE = "https://data.cityofnewyork.us/resource"
DRIFT_TOLERANCE = timedelta(days=1)
RESET_BUFFER_DAYS = 7
FALLBACK_LOOKBACK_DAYS = 30

_APP_TOKEN = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")

# Full-refresh scrapers (dhcr_rs, mappluto, dof_assessments) are excluded —
# they never set a watermark and always re-fetch the entire dataset.
SCRAPER_CONFIG = [
    {
        "name": "311_complaints",
        "socrata_dataset": "erm2-nwe9",
        "socrata_date_field": "created_date",
        "local_table": "complaints_raw",
        "local_date_col": "created_date",
    },
    {
        "name": "dob_permits",
        # filing_date is stored as MM/DD/YYYY text in Socrata; SoQL MAX() on
        # a text field sorts lexicographically — skip the Socrata check entirely.
        "socrata_dataset": "ipu4-2q9a",
        "socrata_date_field": None,
        "local_table": "permits_raw",
        "local_date_col": "filing_date",
    },
    {
        "name": "acris_ownership",
        "socrata_dataset": "bnx9-e6tj",
        "socrata_date_field": "recorded_datetime",
        # recorded_datetime is not persisted locally; created_at is the
        # ingestion-time proxy used as the reset target.
        "local_table": "ownership_raw",
        "local_date_col": "created_at",
    },
    {
        "name": "evictions",
        "socrata_dataset": "6z8x-wfk4",
        "socrata_date_field": "executed_date",
        "local_table": "evictions_raw",
        "local_date_col": "executed_date",
    },
    {
        "name": "dcwp_licenses",
        "socrata_dataset": "w7w3-xahh",
        "socrata_date_field": "license_creation_date",
        "local_table": "dcwp_licenses",
        "local_date_col": "license_creation_date",
    },
    {
        "name": "hpd_violations",
        "socrata_dataset": "wvxf-dwi5",
        "socrata_date_field": "inspectiondate",
        "local_table": "violations_raw",
        "local_date_col": "inspection_date",
    },
]


def _socrata_max_date(dataset_id: str, date_field: str) -> datetime | None:
    """Return MAX(date_field) from a Socrata dataset. Returns None on failure."""
    url = f"{SOCRATA_BASE}/{dataset_id}.json"
    headers = {"Accept": "application/json"}
    if _APP_TOKEN:
        headers["X-App-Token"] = _APP_TOKEN
    try:
        resp = requests.get(
            url,
            params={"$select": f"max({date_field})", "$limit": 1},
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        # Socrata returns {"max_<field_name>": "2025-04-10T00:00:00.000"}
        raw = data[0].get(f"max_{date_field}") or next(iter(data[0].values()), None)
        if not raw:
            return None
        return datetime.fromisoformat(raw.rstrip("Z")).replace(tzinfo=timezone.utc)
    except Exception as exc:
        logger.warning("Socrata MAX query failed for %s/%s: %s", dataset_id, date_field, exc)
        return None


def _local_max_date(db, table: str, col: str) -> datetime | None:
    """Return MAX(col)::timestamptz from a local table. None if the table is empty."""
    # Table and column names are internal constants, never user-supplied.
    row = db.execute(  # noqa: S608
        text(f"SELECT MAX({col})::timestamptz AS max_date FROM {table}")
    ).fetchone()
    if not row or row.max_date is None:
        return None
    ts = row.max_date
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _current_watermark(db, scraper_name: str) -> tuple[int | None, datetime | None]:
    """Return (run_id, watermark_timestamp) for the most recent successful run."""
    row = db.execute(
        text("""
            SELECT id, watermark_timestamp
            FROM scraper_runs
            WHERE scraper_name = :name
              AND status = 'success'
              AND watermark_timestamp IS NOT NULL
            ORDER BY started_at DESC
            LIMIT 1
        """),
        {"name": scraper_name},
    ).fetchone()
    if not row:
        return None, None
    ts = row.watermark_timestamp
    if ts and not ts.tzinfo:
        ts = ts.replace(tzinfo=timezone.utc)
    return row.id, ts


def _insert_reset_run(db, scraper_name: str, new_watermark: datetime) -> None:
    """
    Insert a synthetic scraper_run so the next get_watermark() call sees the
    reset value. The original run history is preserved unchanged.
    """
    now = datetime.now(timezone.utc)
    db.add(
        ScraperRun(
            scraper_name=scraper_name,
            started_at=now,
            completed_at=now,
            status="success",
            records_processed=0,
            records_failed=0,
            watermark_timestamp=new_watermark,
            error_message="watermark reset by watermark_drift_reset.py",
        )
    )


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def main(dry_run: bool) -> None:
    now = datetime.now(timezone.utc)
    results = []

    with get_scraper_db() as db:
        for cfg in SCRAPER_CONFIG:
            name = cfg["name"]
            logger.info("Checking %s ...", name)

            _run_id, watermark = _current_watermark(db, name)
            local_max = _local_max_date(db, cfg["local_table"], cfg["local_date_col"])

            socrata_max: datetime | None = None
            if cfg["socrata_date_field"]:
                socrata_max = _socrata_max_date(cfg["socrata_dataset"], cfg["socrata_date_field"])

            # Prefer Socrata as the authoritative source; fall back to local
            source_max = socrata_max if socrata_max is not None else local_max

            entry: dict = {
                "scraper": name,
                "current_watermark": _iso(watermark),
                "socrata_max_event_date": _iso(socrata_max),
                "local_max_event_date": _iso(local_max),
                "drifted": False,
                "action": None,
            }

            if watermark is None:
                entry["action"] = "skip — no watermark (scraper has never run successfully)"
                logger.info("%s: no prior watermark, skipping", name)
                results.append(entry)
                continue

            if source_max is None:
                entry["action"] = "skip — could not determine source max date (Socrata unavailable and local table empty)"
                logger.warning("%s: source max unknown, skipping drift check", name)
                results.append(entry)
                continue

            drifted = watermark > source_max + DRIFT_TOLERANCE
            entry["drifted"] = drifted

            if not drifted:
                gap_days = (source_max - watermark).total_seconds() / 86400
                entry["action"] = "ok"
                logger.info(
                    "%s: ok  watermark=%s  source_max=%s  gap=%.1fd",
                    name, _iso(watermark), _iso(source_max), gap_days,
                )
                results.append(entry)
                continue

            # Drift detected — compute reset target
            if local_max is not None:
                new_watermark = local_max - timedelta(days=RESET_BUFFER_DAYS)
            else:
                new_watermark = now - timedelta(days=FALLBACK_LOOKBACK_DAYS)
                logger.warning("%s: local table empty, falling back to %d-day lookback", name, FALLBACK_LOOKBACK_DAYS)

            drift_days = (watermark - source_max).total_seconds() / 86400
            entry["drift_gap_days"] = round(drift_days, 2)
            entry["reset_target"] = _iso(new_watermark)

            logger.warning(
                "%s: DRIFTED  watermark=%s  source_max=%s  gap=%.1fd  reset_to=%s",
                name, _iso(watermark), _iso(source_max), drift_days, _iso(new_watermark),
            )

            if dry_run:
                entry["action"] = "dry_run — no changes written"
            else:
                _insert_reset_run(db, name, new_watermark)
                entry["action"] = f"reset — synthetic run inserted with watermark={_iso(new_watermark)}"

            results.append(entry)

    report = {
        "generated_at": now.isoformat(),
        "dry_run": dry_run,
        "scrapers": results,
        "summary": {
            "total_checked": len(results),
            "drifted": sum(1 for r in results if r["drifted"]),
            "reset": sum(1 for r in results if "reset — synthetic" in (r["action"] or "")),
        },
    }
    OUTPUT.write_text(json.dumps(report, indent=2))
    logger.info("Report written to %s", OUTPUT)

    drifted_count = report["summary"]["drifted"]
    if drifted_count:
        verb = "would reset" if dry_run else "reset"
        logger.warning("%d scraper(s) had drifted watermarks — %s", drifted_count, verb)
        if dry_run:
            logger.info("Re-run without --dry-run to apply resets")
    else:
        logger.info("All scrapers nominal")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report drift without writing any resets to the database",
    )
    args = parser.parse_args()
    main(args.dry_run)
