"""
Nightly scraper pipeline orchestrator.

Run order (sequential — no parallel to avoid DB contention):
  1. MapPLUTO         — reference data (quarterly, skip if run < 30 days ago)
  1b. DOF Assessments — annual full-refresh, skip if run < 30 days ago
  2. 311 Complaints   — daily
  3. DOB Permits      — daily
  4. Evictions        — weekly (lags 2-4 weeks by design)
  5. ACRIS Ownership  — daily
  6. DCWP Licenses    — incremental
  7. DHCR RS          — annual snapshot
  8. HPD Violations   — daily (Class B+C, 90-day scoring window)

After all scrapers complete:
  9. Scoring engine   — recomputes displacement scores per zip code
  10. MTEK monitor    — flags new violations/permits/evictions on MTEK portfolio

Each scraper is wrapped with tenacity retries (3 attempts).
A failing scraper logs the failure to ScraperRun and continues — we do not
abort the whole pipeline because one source is down.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from models.database import get_scraper_db
from models.scraper import ScraperRun
from scheduler.alerts import send_alert
from scrapers.complaints import ComplaintsScraper
from scrapers.dcwp_licenses import DcwpScraper
from scrapers.dhcr_rs import DhcrRsScraper
from scrapers.evictions import EvictionsScraper
from scrapers.ownership import OwnershipScraper
from scrapers.dof import DOFScraper
from scrapers.permits import PermitsScraper
from scrapers.pluto import PlutoScraper
from scrapers.violations import ViolationsScraper
from scripts.mtek_monitor import run_mtek_monitor
from scoring.compute import snapshot_scores  # re-exported for test imports

logger = logging.getLogger(__name__)

# PLUTO and DOF are infrequent full-refresh scrapers — skip if run within this window
PLUTO_MIN_INTERVAL_DAYS = 30
DOF_MIN_INTERVAL_DAYS = 30


def _cleanup_stale_runs(db) -> None:
    """
    Mark any scraper_runs rows stuck in 'running' status as 'failure'.
    These occur when a process is killed (OOM, SIGKILL) before the finally
    block in BaseScraper.run() can update the status — leaving rows with no
    error_message and status='running' indefinitely.
    Stale threshold: 2 hours (longest expected scraper runtime is well under 1h).
    """
    stale_cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    result = db.execute(
        text(
            "UPDATE scraper_runs SET status='failure', "
            "error_message='Process killed before completion (OOM or SIGKILL)', "
            "completed_at=NOW() "
            "WHERE status='running' AND started_at < :cutoff"
        ),
        {"cutoff": stale_cutoff},
    )
    db.commit()
    if result.rowcount:
        logger.warning(
            "Cleaned up %d stale 'running' scraper_run rows (process was killed)",
            result.rowcount,
        )


def run_nightly_pipeline() -> bool:
    """
    Entry point called by the nightly cron job at 2:00 AM UTC.
    Runs all scrapers in sequence, then triggers scoring.
    Returns True if all scrapers succeeded, False if any failed.
    """
    logger.info("=== Nightly pipeline started ===")
    started = datetime.now(timezone.utc)
    had_failures = False

    with get_scraper_db() as db:
        _cleanup_stale_runs(db)

    with get_scraper_db() as db:
        if not _run_pluto_if_due(db):
            had_failures = True

    with get_scraper_db() as db:
        if not _run_dof_if_due(db):
            had_failures = True

    scrapers = [
        ("311_complaints", ComplaintsScraper),
        ("dob_permits", PermitsScraper),
        ("evictions", EvictionsScraper),
        ("acris_ownership", OwnershipScraper),
        ("dcwp_licenses", DcwpScraper),
        ("dhcr_rs", DhcrRsScraper),
        ("hpd_violations", ViolationsScraper),
    ]

    for scraper_name, ScraperClass in scrapers:
        if not _run_scraper_with_retry(scraper_name, ScraperClass):
            had_failures = True

    # Scoring engine runs after all scrapers complete
    _run_scoring()

    # MTEK portfolio monitor — needs fresh violations/permits/evictions data
    _run_mtek_monitor()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info("=== Nightly pipeline complete in %.0fs ===", elapsed)
    return not had_failures


def _run_scraper_with_retry(scraper_name: str, ScraperClass) -> bool:
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=30, max=300),
        reraise=True,   # outer try/except catches; this makes intent clear
    )
    def _attempt():
        with get_scraper_db() as db:
            scraper = ScraperClass()
            scraper_run = scraper.run(db)
            return scraper_run

    try:
        scraper_run = _attempt()
        if scraper_run is not None:
            # Quarantine rate check
            total = (scraper_run.records_processed or 0) + (scraper_run.records_failed or 0)
            if total > 0:
                quarantine_rate = (scraper_run.records_failed or 0) / total
                if quarantine_rate > 0.10:
                    send_alert(
                        f"High quarantine rate: {scraper_name}",
                        f"{scraper_run.records_failed}/{total} records quarantined "
                        f"({quarantine_rate:.1%}) — possible upstream schema change. "
                        f"Threshold: 10%.",
                    )
            # Warning status alert — fires when records=0 despite expectation.
            # Catches source freezes, API outages, and silent data gaps that
            # do not raise exceptions and would otherwise go unreported.
            if scraper_run.status == "warning":
                send_alert(
                    f"Scraper anomaly: {scraper_name}",
                    f"status=warning | records={scraper_run.records_processed} | "
                    f"{scraper_run.warning_message or 'no detail'}",
                )
        return True
    except Exception as exc:
        # All retries exhausted — log and continue to next scraper
        logger.error(
            "Scraper %s failed after all retries: %s", scraper_name, exc
        )
        send_alert(
            f"Scraper failed: {scraper_name}",
            f"All retries exhausted. Error: {exc}",
        )
        return False


def _run_pluto_if_due(db) -> bool:
    """Only run PLUTO if it hasn't completed successfully in the last 30 days."""
    last_pluto = (
        db.query(ScraperRun)
        .filter(
            ScraperRun.scraper_name == "mappluto",
            ScraperRun.status == "success",
        )
        .order_by(ScraperRun.started_at.desc())
        .first()
    )

    cutoff = datetime.now(timezone.utc) - timedelta(days=PLUTO_MIN_INTERVAL_DAYS)
    if last_pluto and last_pluto.started_at > cutoff:
        logger.info(
            "PLUTO run skipped — last successful run was %s (within %d-day window)",
            last_pluto.started_at.date(),
            PLUTO_MIN_INTERVAL_DAYS,
        )
        return True  # skip counts as success

    logger.info("PLUTO run is due — starting...")
    return _run_scraper_with_retry("mappluto", PlutoScraper)


def _run_dof_if_due(db) -> bool:
    """Only run DOF assessments if it hasn't completed successfully in the last 30 days.
    DOF is an annual full-refresh dataset — running more often wastes API quota with no new data."""
    last_dof = (
        db.query(ScraperRun)
        .filter(
            ScraperRun.scraper_name == "dof_assessments",
            ScraperRun.status == "success",
        )
        .order_by(ScraperRun.started_at.desc())
        .first()
    )

    cutoff = datetime.now(timezone.utc) - timedelta(days=DOF_MIN_INTERVAL_DAYS)
    if last_dof and last_dof.started_at > cutoff:
        logger.info(
            "DOF run skipped — last successful run was %s (within %d-day window)",
            last_dof.started_at.date(),
            DOF_MIN_INTERVAL_DAYS,
        )
        return True  # skip counts as success

    logger.info("DOF run is due — starting...")
    return _run_scraper_with_retry("dof_assessments", DOFScraper)


def _run_scoring() -> None:
    """
    Trigger the scoring engine after all scrapers complete.
    compute_scores() handles both displacement_scores upsert and score_history
    snapshot in a single pass (Step 6 and Step 7 of compute.py).
    """
    try:
        logger.info("Scoring engine: starting...")
        from scoring.compute import compute_scores
        with get_scraper_db() as db:
            n = compute_scores(db)
            if n == 0:
                send_alert(
                    "Scoring engine: zero zip codes scored",
                    "compute_scores() returned 0. Either no data in DB or >50% of zips "
                    "failed sanity checks. Check scoring/compute.py logs for details.",
                )
        logger.info("Scoring engine: scored and snapshotted %d zip codes", n)
    except Exception as exc:
        logger.error("Scoring engine failed: %s", exc)


def _run_mtek_monitor() -> None:
    try:
        with get_scraper_db() as db:
            n = run_mtek_monitor(db)
        logger.info("MTEK monitor: %d new alerts", n)
    except Exception as exc:
        logger.error("MTEK monitor failed (non-fatal): %s", exc)
        send_alert("MTEK monitor failed", str(exc))
