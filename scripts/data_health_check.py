"""
Data freshness and volume audit for all PulseCities scrapers.

Checks:
  - Last successful run per scraper vs expected cadence
  - Flags any daily scraper silent for >48h
  - Week-over-week row count growth per table
  - Quarantine volume spikes (bad upstream data signal)
  - Stale "running" locks from crashed pipeline runs (auto-fixable)

Autonomous fixes applied:
  - Stale scraper_runs rows stuck in "running" status are marked "failure"

All other issues are written to TODO.md for manual follow-up.

Output: scripts/data_health_check.json

Usage:
    python scripts/data_health_check.py
"""

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

OUTPUT   = Path(__file__).parent / "data_health_check.json"
TODO_MD  = Path(__file__).parent.parent / "TODO.md"

NOW = datetime.now(timezone.utc)


# ─── scraper registry ─────────────────────────────────────────────────────────

SCRAPERS = [
    {
        "name":        "311_complaints",
        "display":     "311 Complaints",
        "table":       "complaints_raw",
        "frequency":   "daily",
        "stale_after": timedelta(hours=48),
    },
    {
        "name":        "dob_permits",
        "display":     "DOB Permits",
        "table":       "permits_raw",
        "frequency":   "daily",
        "stale_after": timedelta(hours=48),
    },
    {
        "name":        "evictions",
        "display":     "Evictions (OCA)",
        "table":       "evictions_raw",
        "frequency":   "weekly",
        "stale_after": timedelta(days=8),
    },
    {
        "name":        "acris_ownership",
        "display":     "ACRIS Ownership",
        "table":       "ownership_raw",
        "frequency":   "daily",
        "stale_after": timedelta(hours=48),
    },
    {
        "name":        "dcwp_licenses",
        "display":     "DCWP Licenses",
        "table":       "dcwp_licenses",
        "frequency":   "daily",
        "stale_after": timedelta(hours=48),
    },
    {
        "name":        "dhcr_rs",
        "display":     "DHCR Rent Stabilization",
        "table":       "rs_buildings",
        "frequency":   "annual",
        "stale_after": timedelta(days=400),
    },
    {
        "name":        "mappluto",
        "display":     "MapPLUTO (parcels)",
        "table":       "parcels",
        "frequency":   "quarterly",
        "stale_after": timedelta(days=95),
    },
    {
        "name":        "dof_assessments",
        "display":     "DOF Tax Assessments",
        "table":       "parcels",      # writes into same parcels table
        "frequency":   "annual",
        "stale_after": timedelta(days=400),
    },
]

SCRAPER_NAMES = [s["name"] for s in SCRAPERS]

# Tables that have a created_at column we can slice for week-over-week counts.
# parcels is counted once even though two scrapers write to it.
AUDITED_TABLES = [
    "complaints_raw",
    "permits_raw",
    "evictions_raw",
    "ownership_raw",
    "dcwp_licenses",
    "rs_buildings",
    "parcels",
    "score_history",
    "scraper_quarantine",
]

STALE_LOCK_THRESHOLD = timedelta(hours=2)


# ─── queries ──────────────────────────────────────────────────────────────────

LAST_SUCCESS_QUERY = text("""
    SELECT DISTINCT ON (scraper_name)
        scraper_name,
        started_at,
        completed_at,
        records_processed,
        records_failed,
        expected_min_records,
        watermark_timestamp
    FROM scraper_runs
    WHERE status = 'success'
      AND scraper_name = ANY(:names)
    ORDER BY scraper_name, started_at DESC
""")

LAST_RUN_QUERY = text("""
    SELECT DISTINCT ON (scraper_name)
        scraper_name,
        started_at,
        completed_at,
        status,
        records_processed,
        records_failed,
        error_message
    FROM scraper_runs
    WHERE scraper_name = ANY(:names)
    ORDER BY scraper_name, started_at DESC
""")

STALE_LOCKS_QUERY = text("""
    SELECT id, scraper_name, started_at
    FROM scraper_runs
    WHERE status = 'running'
      AND started_at < :cutoff
""")

FIX_STALE_LOCK_QUERY = text("""
    UPDATE scraper_runs
    SET status        = 'failure',
        completed_at  = :now,
        error_message = 'stale run: automatically marked failed by data_health_check.py'
    WHERE id = ANY(:ids)
""")

QUARANTINE_WEEK_QUERY = text("""
    SELECT
        scraper_name,
        COUNT(*) AS total_this_week
    FROM scraper_quarantine
    WHERE created_at >= NOW() - INTERVAL '7 days'
    GROUP BY scraper_name
""")


def _row_count_query(table: str) -> text:
    return text(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') AS last_7_days,
            COUNT(*) FILTER (
                WHERE created_at >= NOW() - INTERVAL '14 days'
                  AND created_at <  NOW() - INTERVAL '7 days'
            ) AS prior_7_days
        FROM {table}
    """)  # noqa: S608 — table name from internal constant, not user input


# ─── scraper health ───────────────────────────────────────────────────────────

def _scraper_status(
    cfg: dict,
    last_success: dict | None,
    last_run: dict | None,
) -> tuple[str, list[str]]:
    """
    Returns (status, flags) for a single scraper.

    Statuses:
      healthy   — ran successfully within expected cadence
      stale     — no successful run within stale_after window
      failed    — last run ended in failure
      never_run — no rows in scraper_runs at all
    """
    flags: list[str] = []

    if last_run is None:
        return "never_run", ["no scraper_runs rows found"]

    if last_success is None:
        flags.append("no successful run on record")
        return "failed", flags

    age = NOW - last_success["started_at"].replace(tzinfo=timezone.utc)
    hours_old = age.total_seconds() / 3600

    if age > cfg["stale_after"]:
        flags.append(
            f"last success {hours_old:.1f}h ago "
            f"(threshold: {cfg['stale_after'].total_seconds()/3600:.0f}h)"
        )

    if last_run["status"] == "failure":
        flags.append(f"last run failed: {last_run['error_message'] or 'no error message'}")

    exp = last_success.get("expected_min_records")
    proc = last_success.get("records_processed", 0)
    low_records = False
    if exp and proc < exp * 0.5:
        flags.append(
            f"record count anomaly: {proc} processed, expected ≥{exp} "
            f"(got {proc/exp*100:.0f}% of minimum)"
        )
        low_records = True

    if age > cfg["stale_after"]:
        return "stale", flags
    if last_run["status"] == "failure":
        return "failed", flags
    if low_records:
        return "warning", flags
    return "healthy", flags


def _dt_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


# ─── main ────────────────────────────────────────────────────────────────────

def run() -> dict:
    fixes_applied: list[str] = []
    issues: list[str] = []

    with get_scraper_db() as db:
        # ── last successful and last-any run per scraper ──────────────────────
        success_rows = {
            r["scraper_name"]: dict(r)
            for r in db.execute(LAST_SUCCESS_QUERY, {"names": SCRAPER_NAMES}).mappings()
        }
        last_run_rows = {
            r["scraper_name"]: dict(r)
            for r in db.execute(LAST_RUN_QUERY, {"names": SCRAPER_NAMES}).mappings()
        }

        # ── stale lock cleanup ────────────────────────────────────────────────
        stale_cutoff = NOW - STALE_LOCK_THRESHOLD
        stale_locks = db.execute(STALE_LOCKS_QUERY, {"cutoff": stale_cutoff}).mappings().all()
        stale_lock_ids = [r["id"] for r in stale_locks]

        if stale_lock_ids:
            db.execute(FIX_STALE_LOCK_QUERY, {"ids": stale_lock_ids, "now": NOW})
            for r in stale_locks:
                age_h = (NOW - r["started_at"].replace(tzinfo=timezone.utc)).total_seconds() / 3600
                msg = (
                    f"Cleared stale 'running' lock for {r['scraper_name']} "
                    f"(started {age_h:.1f}h ago, run id #{r['id']})"
                )
                logger.warning(msg)
                fixes_applied.append(msg)

        # ── quarantine counts this week ───────────────────────────────────────
        quarantine_week = {
            r["scraper_name"]: r["total_this_week"]
            for r in db.execute(QUARANTINE_WEEK_QUERY).mappings()
        }

        # ── per-table row counts ──────────────────────────────────────────────
        table_stats: list[dict] = []
        seen_tables: set[str] = set()
        for table in AUDITED_TABLES:
            if table in seen_tables:
                continue
            seen_tables.add(table)
            try:
                row = db.execute(_row_count_query(table)).mappings().one()
                total      = row["total"]
                last_7     = row["last_7_days"]
                prior_7    = row["prior_7_days"]
                wow_delta  = last_7 - prior_7
                wow_pct    = (wow_delta / prior_7 * 100) if prior_7 > 0 else None

                # Quiet tables (full-refresh, quarterly, annual) don't need new rows.
                # "shrinking" is omitted — WoW delta is unreliable when a bulk
                # historical import landed in the prior window.
                quiet_tables = {"parcels", "rs_buildings"}
                if total == 0:
                    vol_status = "empty"
                elif last_7 == 0 and table not in quiet_tables:
                    vol_status = "no_recent_rows"
                elif last_7 > 0:
                    vol_status = "growing"
                else:
                    vol_status = "static"

                table_stats.append({
                    "table":           table,
                    "total_rows":      total,
                    "rows_last_7d":    last_7,
                    "rows_prior_7d":   prior_7,
                    "wow_delta":       wow_delta,
                    "wow_pct":         round(wow_pct, 1) if wow_pct is not None else None,
                    "volume_status":   vol_status,
                })
            except Exception as exc:
                logger.warning("Could not count %s: %s", table, exc)
                table_stats.append({"table": table, "error": str(exc)})

    # ── build scraper health records ──────────────────────────────────────────
    scraper_records: list[dict] = []
    unhealthy: list[dict] = []

    for cfg in SCRAPERS:
        name        = cfg["name"]
        last_ok     = success_rows.get(name)
        last_any    = last_run_rows.get(name)
        status, flags = _scraper_status(cfg, last_ok, last_any)

        hours_since = None
        if last_ok:
            age = NOW - last_ok["started_at"].replace(tzinfo=timezone.utc)
            hours_since = round(age.total_seconds() / 3600, 1)

        rec = {
            "name":              name,
            "display":           cfg["display"],
            "frequency":         cfg["frequency"],
            "stale_threshold_h": cfg["stale_after"].total_seconds() / 3600,
            "status":            status,
            "flags":             flags,
            "hours_since_success": hours_since,
            "last_success": {
                "started_at":        _dt_iso(last_ok["started_at"])        if last_ok else None,
                "completed_at":      _dt_iso(last_ok["completed_at"])      if last_ok else None,
                "records_processed": last_ok["records_processed"]          if last_ok else None,
                "records_failed":    last_ok["records_failed"]             if last_ok else None,
                "watermark":         _dt_iso(last_ok["watermark_timestamp"]) if last_ok else None,
            },
            "last_run": {
                "started_at":    _dt_iso(last_any["started_at"])    if last_any else None,
                "status":        last_any["status"]                  if last_any else None,
                "error_message": last_any["error_message"]           if last_any else None,
            },
            "quarantine_last_7d": quarantine_week.get(name, 0),
        }
        scraper_records.append(rec)

        if status in ("stale", "failed", "never_run", "warning"):
            unhealthy.append(rec)

    # ── flag volume problems ──────────────────────────────────────────────────
    for ts in table_stats:
        if ts.get("volume_status") in ("empty", "no_recent_rows"):
            issues.append(
                f"Table `{ts['table']}` has no recent rows "
                f"(total rows: {ts.get('total_rows', '?')}, "
                f"added last 7d: {ts.get('rows_last_7d', '?')})"
            )

    # ── collect unfixable scraper issues ─────────────────────────────────────
    for rec in unhealthy:
        issue_lines = [f"Scraper `{rec['name']}` ({rec['display']}) — status: {rec['status']}"]
        for flag in rec["flags"]:
            issue_lines.append(f"  - {flag}")
        if rec["last_run"]["error_message"]:
            issue_lines.append(f"  - last error: {rec['last_run']['error_message']}")
        issues.append("\n".join(issue_lines))

    # ── determine overall status ──────────────────────────────────────────────
    daily_unhealthy = [
        r for r in unhealthy if r["frequency"] == "daily"
    ]
    if any(r["status"] in ("failed", "never_run") for r in scraper_records):
        overall = "critical"
    elif any(r["status"] == "warning" for r in scraper_records) or daily_unhealthy:
        overall = "degraded"
    elif unhealthy:
        overall = "warning"
    else:
        overall = "healthy"

    # ── write TODO.md if there are unresolvable issues ────────────────────────
    if issues:
        _write_todo(issues, fixes_applied)

    result = {
        "generated_at":    NOW.isoformat(),
        "overall_status":  overall,
        "summary": {
            "total_scrapers":    len(scraper_records),
            "healthy":           sum(1 for r in scraper_records if r["status"] == "healthy"),
            "warning":           sum(1 for r in scraper_records if r["status"] == "warning"),
            "stale":             sum(1 for r in scraper_records if r["status"] == "stale"),
            "failed":            sum(1 for r in scraper_records if r["status"] == "failed"),
            "never_run":         sum(1 for r in scraper_records if r["status"] == "never_run"),
            "stale_locks_fixed": len(fixes_applied),
        },
        "scrapers":         scraper_records,
        "tables":           table_stats,
        "fixes_applied":    fixes_applied,
        "open_issues":      issues,
    }

    OUTPUT.write_text(json.dumps(result, indent=2))
    logger.info("Saved → %s", OUTPUT)
    return result


def _write_todo(issues: list[str], fixes_applied: list[str]) -> None:
    timestamp = NOW.strftime("%Y-%m-%d %H:%M UTC")
    header = f"## Data Health Issues — {timestamp}\n\n"

    if fixes_applied:
        header += "**Auto-fixed:**\n"
        for fix in fixes_applied:
            header += f"- {fix}\n"
        header += "\n"

    header += "**Requires manual investigation:**\n\n"
    body = "\n\n".join(f"- [ ] {issue}" for issue in issues)
    block = header + body + "\n\n---\n\n"

    if TODO_MD.exists():
        existing = TODO_MD.read_text()
        TODO_MD.write_text(block + existing)
    else:
        preamble = "# TODO\n\nItems flagged for manual follow-up by automated health checks.\n\n---\n\n"
        TODO_MD.write_text(preamble + block)

    logger.info("Issues written → %s", TODO_MD)


def _print_report(result: dict) -> None:
    s = result["summary"]
    status_icon = {"healthy": "✓", "degraded": "!", "warning": "~", "critical": "✗"}.get(
        result["overall_status"], "?"
    )

    print(f"\nData Health Check  [{result['overall_status'].upper()}] {status_icon}")
    print(f"  {s['total_scrapers']} scrapers: "
          f"{s['healthy']} healthy, {s['warning']} warning, {s['stale']} stale, "
          f"{s['failed']} failed, {s['never_run']} never run")
    if s["stale_locks_fixed"]:
        print(f"  Auto-fixed {s['stale_locks_fixed']} stale lock(s)")

    print(f"\n{'Scraper':<28} {'Status':<10} {'Last Success':<22} {'Records':<10} Quarantine/7d")
    print("─" * 85)
    for r in result["scrapers"]:
        last_ok = r["last_success"]["started_at"]
        age_str = f"{r['hours_since_success']:.0f}h ago" if r["hours_since_success"] is not None else "never"
        recs    = r["last_success"]["records_processed"]
        recs_s  = f"{recs:,}" if recs is not None else "—"
        quar    = r["quarantine_last_7d"]
        quar_s  = f"{quar:,}" if quar else "—"
        status  = r["status"]
        marker  = " !" if status in ("stale", "failed", "never_run", "warning") else "  "
        print(f"{marker}{r['display']:<26} {status:<10} {age_str:<22} {recs_s:<10} {quar_s}")
        for flag in r["flags"]:
            print(f"    > {flag}")

    print(f"\n{'Table':<25} {'Total':>12} {'Last 7d':>10} {'Prior 7d':>10} {'WoW Δ':>10}  Status")
    print("─" * 80)
    for t in result["tables"]:
        if "error" in t:
            print(f"  {t['table']:<23}  ERROR: {t['error']}")
            continue
        wow = t["wow_delta"]
        sign = "+" if wow >= 0 else ""
        marker = " !" if t["volume_status"] in ("empty", "no_new_rows", "shrinking") else "  "
        print(
            f"{marker}{t['table']:<23} "
            f"{t['total_rows']:>12,} "
            f"{t['rows_last_7d']:>10,} "
            f"{t['rows_prior_7d']:>10,} "
            f"{sign}{wow:>9,}  "
            f"{t['volume_status']}"
        )

    if result["open_issues"]:
        print(f"\n  {len(result['open_issues'])} issue(s) written to TODO.md")

    print(f"\n  Output: {OUTPUT}")


def main() -> int:
    result = run()
    _print_report(result)
    return 0 if result["overall_status"] in ("healthy", "warning") else 1


if __name__ == "__main__":
    sys.exit(main())
