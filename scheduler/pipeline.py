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
  9. Reconciliation   — compares our row counts against each source and rewinds
                        any feed that drifted, so tomorrow's run re-reads it
  10. Scoring engine  — recomputes displacement scores per zip code
  11. MTEK monitor    — flags new violations/permits/evictions on MTEK portfolio

Each scraper is wrapped with tenacity retries (3 attempts).
A failing scraper logs the failure to ScraperRun and continues — we do not
abort the whole pipeline because one source is down.
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from models.database import get_scraper_db
from models.scraper import ScraperRun
from scheduler.alerts import flush_alerts, notify_ops, send_alert
from scrapers.complaints import ComplaintsScraper
from scrapers.dcwp_licenses import DcwpScraper
from scrapers.dhcr_rs import DhcrRsScraper
from scrapers.evictions import EvictionsScraper
from scrapers.ownership import OwnershipScraper
from scrapers.dof import DOFScraper
from scrapers.dob_now_permits import DobNowPermitsScraper
from scrapers.permits import PermitsScraper
from scrapers.pluto import PlutoScraper
from scrapers.violations import ViolationsScraper
from scripts.mtek_monitor import run_mtek_monitor
from scripts.reconcile_upstream import (
    FEEDS,
    earliest_drift,
    reconcile_feed,
    rewind_watermark,
)

logger = logging.getLogger(__name__)

# PLUTO and DOF are infrequent full-refresh scrapers — skip if run within this window
PLUTO_MIN_INTERVAL_DAYS = 30
DOF_MIN_INTERVAL_DAYS = 30
# The HPD building registry (kj4p-ruqc, ingested by the scraper still named
# dhcr_rs) is a slow-moving annual-ish list, and its rows are stamped
# source='hpd_jurisdiction', which every consumer filters out. Re-walking all
# 350k rows nightly was 92.5% of the pipeline's wall clock for data nothing
# reads that night. data_health_check already classifies this feed as annual.
HPD_REGISTRY_MIN_INTERVAL_DAYS = 30

# How far back the nightly reconciliation compares our row counts against the
# source. Each feed only judges days older than its own settle window, so this is
# an outer bound rather than the number of days actually checked.
RECONCILE_WINDOW_DAYS = 14


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


def _prune_quarantine(db) -> None:
    """
    Quarantine is a triage buffer, not an archive: rows older than 90 days have
    either been acted on or accepted as noise, and the table otherwise grows
    without bound (it hit 284 MB of one known-benign HPD class-I pattern before
    retention existed, bloating every nightly backup).
    """
    result = db.execute(
        text("DELETE FROM scraper_quarantine WHERE created_at < NOW() - INTERVAL '90 days'")
    )
    db.commit()
    if result.rowcount:
        logger.info("Pruned %d quarantine rows older than 90 days", result.rowcount)


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
        _prune_quarantine(db)

    with get_scraper_db() as db:
        if not _run_pluto_if_due(db):
            had_failures = True

    with get_scraper_db() as db:
        if not _run_dof_if_due(db):
            had_failures = True

    scrapers = [
        ("311_complaints", ComplaintsScraper),
        ("dob_permits", PermitsScraper),
        # Both permit feeds run. dob_permits reads legacy BIS, which still
        # publishes a trickle of late permits on old jobs; dob_now_permits reads
        # DOB NOW, which is where 96% of current permits actually are. They write
        # to the same table under different `source` values and different unique
        # indexes, so neither can overwrite the other.
        ("dob_now_permits", DobNowPermitsScraper),
        ("evictions", EvictionsScraper),
        ("acris_ownership", OwnershipScraper),
        ("dcwp_licenses", DcwpScraper),
        ("hpd_violations", ViolationsScraper),
    ]

    for scraper_name, ScraperClass in scrapers:
        if not _run_scraper_with_retry(scraper_name, ScraperClass):
            had_failures = True

    with get_scraper_db() as db:
        if not _run_hpd_registry_if_due(db):
            had_failures = True

    # Repair before scoring, so the scores reflect anything recovered. This runs
    # after the scrapers because a rewind issued before them would be overwritten
    # by the same run's watermark.
    with get_scraper_db() as db:
        _safe_reconcile(db)

    # Scoring engine runs after all scrapers complete
    if not _run_scoring():
        had_failures = True

    # The panel read, generated off the request path. Scoring has just finished,
    # so this writes the reads for whatever moved before anyone opens a panel.
    _run_read_precompute()

    # Render the expensive pages once each so the first real visitor does not.
    _warm_heavy_pages()

    # MTEK portfolio monitor — needs fresh violations/permits/evictions data
    _run_mtek_monitor()

    # One combined ops email for every anomaly the run raised.
    flush_alerts()

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


def _run_hpd_registry_if_due(db) -> bool:
    """Run the HPD building registry at most monthly. It is a full-table walk
    of ~350k rows whose output no query reads on a nightly cadence."""
    last = (
        db.query(ScraperRun)
        .filter(
            ScraperRun.scraper_name == "dhcr_rs",
            ScraperRun.status == "success",
        )
        .order_by(ScraperRun.started_at.desc())
        .first()
    )

    cutoff = datetime.now(timezone.utc) - timedelta(days=HPD_REGISTRY_MIN_INTERVAL_DAYS)
    if last and last.started_at > cutoff:
        logger.info(
            "HPD registry run skipped — last successful run was %s (within %d-day window)",
            last.started_at.date(),
            HPD_REGISTRY_MIN_INTERVAL_DAYS,
        )
        return True  # skip counts as success

    logger.info("HPD registry run is due — starting...")
    return _run_scraper_with_retry("dhcr_rs", DhcrRsScraper)


def _safe_reconcile(db) -> None:
    """Run the repair step without ever letting it take the run down.

    The scrape has already happened by this point. A reconciliation that cannot
    reach Socrata is a repair we skip tonight, not a failed pipeline.
    """
    try:
        _reconcile_and_heal(db)
    except Exception as exc:  # noqa: BLE001 — a repair step must not fail the run
        logger.error("Reconciliation failed (non-fatal): %s", exc, exc_info=True)


def _reconcile_and_heal(db) -> None:
    """Compare our row counts against each source and rewind whatever drifted.

    This asks the one question none of the other checks ask. Freshness, pipeline
    health and the weekly heartbeat all measure how new the newest record is, so
    all three stayed green while 311 quietly lost 1 to 3.5% of every day to a
    watermark that only moves forward.

    Rewinding only, deliberately. Tonight's scrapers have already run, so the
    re-read happens on tomorrow's pass through the normal path. That keeps the
    nightly wall clock flat and means healed rows go through the same parse,
    quarantine and upsert as any other night.

    Silent on the first night a feed drifts: a rewind that lands is a fix in
    progress. When the watermark is already behind the drifted range and the gap
    is still there, last night's heal did not work, and that is worth an email.
    """
    for feed in FEEDS:
        result = reconcile_feed(db, feed, RECONCILE_WINDOW_DAYS)
        start = earliest_drift(result)
        if start is None:
            continue

        drifted = [r for r in result["rows"] if r["drifted"]]
        short = sum(r["gap"] for r in drifted)
        worst = max(drifted, key=lambda r: r["pct"])

        moved = rewind_watermark(db, feed.scraper_name, start - timedelta(days=1))
        if moved:
            logger.info(
                "%s drifted %d day(s), %d rows short; watermark rewound to %s, "
                "tomorrow's run re-reads the range",
                feed.scraper_name, len(drifted), short, start,
            )
            continue

        send_alert(
            f"Ingestion drift persists: {feed.scraper_name}",
            f"{len(drifted)} settled day(s) short against {feed.dataset_id}, "
            f"{short:,} rows total. Worst: {worst['day']} missing {worst['gap']:,} "
            f"({worst['pct']*100:.1f}%).\n\n"
            f"The watermark is already behind {start}, so a rewind cannot widen "
            f"what the next run reads. Automatic healing has not closed this gap.\n\n"
            f"  python -m scripts.reconcile_upstream --feed {feed.scraper_name} --days 30",
        )


def _run_scoring() -> bool:
    """
    Trigger the scoring engine after all scrapers complete.
    compute_scores() handles both displacement_scores upsert and score_history
    snapshot in a single pass (Step 6 and Step 7 of compute.py).
    Returns False on crash, zero-scored batch, or a missing snapshot — all
    three leave the site serving stale data and must fail the pipeline loudly.
    """
    try:
        logger.info("Scoring engine: starting...")
        from scoring.compute import compute_scores
        with get_scraper_db() as db:
            n = compute_scores(db)
            if n == 0:
                notify_ops(
                    "Scoring engine: zero zip codes scored",
                    "compute_scores() returned 0. Either no data in DB or >50% of zips "
                    "failed sanity checks. Check scoring/compute.py logs for details.",
                )
                return False
            # Snapshot invariant: today's score_history must both cover every
            # scored zip and agree with what the map is serving. Counting rows
            # alone missed the 2026-08-28 divergence entirely, because 177 stale
            # rows count exactly like 177 fresh ones: the nightly run scored at
            # 02:07, a post-backfill recompute landed at 02:21, and score_history
            # kept the first set under an ON CONFLICT DO NOTHING it no longer
            # uses. Eight ZIPs read one band in the chart and another on the map.
            snapshotted, diverged = db.execute(
                text(
                    """
                    SELECT COUNT(*),
                           COUNT(*) FILTER (
                               WHERE d.score IS NOT NULL
                                 AND abs(h.composite_score - d.score) > 0.05
                           )
                    FROM score_history h
                    LEFT JOIN displacement_scores d USING (zip_code)
                    WHERE h.scored_at = CURRENT_DATE
                    """
                )
            ).one()
            if snapshotted < n:
                notify_ops(
                    "Scoring engine: score_history snapshot incomplete",
                    f"Scored {n} zips but score_history has {snapshotted} rows for "
                    f"today. Recover by re-running the scorer (idempotent same-day): "
                    f"venv/bin/python -m scoring.compute",
                )
                return False
            if diverged:
                notify_ops(
                    "Scoring engine: history disagrees with the map",
                    f"{diverged} of {snapshotted} ZIPs have a score_history row for "
                    f"today that differs from displacement_scores. The trend chart "
                    f"and the map are showing different numbers for the same day. "
                    f"Re-run the scorer to reconcile, which now refreshes the "
                    f"history row: cd /root/pulsecities && "
                    f"venv/bin/python -m scoring.compute",
                )
                return False
        logger.info("Scoring engine: scored and snapshotted %d zip codes", n)
        return True
    except Exception as exc:
        logger.error("Scoring engine failed: %s", exc, exc_info=True)
        notify_ops(
            "Scoring engine crashed",
            f"compute_scores() raised and the site is now serving yesterday's "
            f"scores:\n\n{exc}\n\n  tail -200 /var/log/pulsecities/scraper.log",
        )
        return False


def _run_read_precompute() -> None:
    """Warm the AI read for every ZIP whose score moved enough to change it.

    Never fatal and never alerts. The read is an editorial layer over the
    deterministic summary: if this does not run, the panel falls back exactly as
    it does when the model is unreachable, and the next visitor pays a
    generation instead. A pipeline that fails the night because a paragraph is
    missing would be a worse trade than the paragraph.
    """
    try:
        from scripts.precompute_reads import main as precompute
        import sys as _sys
        argv = _sys.argv
        _sys.argv = ["precompute_reads"]
        try:
            precompute()
        finally:
            _sys.argv = argv
    except Exception as exc:  # noqa: BLE001
        logger.warning("Read precompute failed (non-fatal): %s", exc)


# The pages whose cold render costs seconds rather than milliseconds, and which
# a visitor can land on directly from search. Each holds its own in-process cache
# for an hour, so this both fills that and pulls the underlying rows into the
# Postgres buffer cache, which is most of the difference between a 1-second
# render and a 14-second one after a night of scraping has churned the buffers.
_HEAVY_PAGES = ("/flips", "/this-week", "/evictions", "/displacement", "/radar")

# Each worker has its own cache, so one request per page warms one worker. Three
# passes over two workers is enough to leave neither cold without turning a
# warmup into a load test.
_WARM_PASSES = 3


def _warm_heavy_pages() -> None:
    """Ask the running site for its slowest pages, through its own socket.

    Never fatal. This is a courtesy to the first visitor after a nightly run,
    not a step the night depends on, and gunicorn may legitimately be reloading
    underneath it.
    """
    import socket
    import urllib.request

    sock_path = "/tmp/gunicorn.sock"
    if not os.path.exists(sock_path):
        logger.info("Page warmup skipped: no socket at %s", sock_path)
        return

    class _UnixHandler(urllib.request.AbstractHTTPHandler):
        def http_open(self, req):
            return self.do_open(_UnixConnection, req)

    import http.client

    class _UnixConnection(http.client.HTTPConnection):
        def connect(self):
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.settimeout(60)
            self.sock.connect(sock_path)

    opener = urllib.request.build_opener(_UnixHandler)
    warmed = 0
    for _ in range(_WARM_PASSES):
        for path in _HEAVY_PAGES:
            started = time.monotonic()
            try:
                with opener.open(f"http://localhost{path}", timeout=60) as resp:
                    resp.read(1024)
                elapsed = time.monotonic() - started
                if elapsed > 2.0:
                    logger.info("warmed %s in %.1fs", path, elapsed)
                warmed += 1
            except Exception as exc:  # noqa: BLE001
                logger.info("warmup of %s skipped: %s", path, exc)
    logger.info("Page warmup: %d of %d renders",
                warmed, _WARM_PASSES * len(_HEAVY_PAGES))


def _run_mtek_monitor() -> None:
    try:
        with get_scraper_db() as db:
            n = run_mtek_monitor(db)
        logger.info("MTEK monitor: %d new alerts", n)
    except Exception as exc:
        logger.error("MTEK monitor failed (non-fatal): %s", exc)
        send_alert("MTEK monitor failed", str(exc))
