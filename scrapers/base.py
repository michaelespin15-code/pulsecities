"""
Abstract base class for all NYC Open Data Socrata scrapers.

Every concrete scraper inherits from this class and implements _run().

Provides:
- Socrata SODA 2.1 REST API with pagination (50k rows/page)
- Watermark-based incremental fetching (last successful ScraperRun)
- Tenacity retry on HTTP errors (3 attempts, exponential backoff)
- ScraperRun audit log written on every completion (success or failure)
- ScraperQuarantine dead letter table for invalid records, never silently drop
- Anomaly detection: warns if record count < 50% of expected minimum
"""

import logging
import os
import urllib.parse
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

import requests
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from config.nyc import SCRAPER_EXPECTED_MIN_RECORDS, SOCRATA_BASE_URL
from models.scraper import ScraperQuarantine, ScraperRun

# Rolling average window for the record-count anomaly check.
# 14 days gives ~14 data points for daily scrapers; weekly scrapers get ~2,
# which falls below _ROLLING_MIN_SAMPLES so the check is skipped for them.
_ROLLING_WINDOW_DAYS = 14
_ROLLING_MIN_SAMPLES = 3

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    # Subclass must define both of these
    SCRAPER_NAME: str
    DATASET_ID: str

    # How far back to look on the very first run (no watermark in DB yet)
    INITIAL_LOOKBACK_DAYS: int = 365

    # Extra lookback applied on top of the 10-minute clock-skew buffer when a
    # watermark exists.  Datasets with known ingestion lag (e.g. OCA evictions)
    # override this so late-arriving records aren't silently skipped.
    WATERMARK_EXTRA_LOOKBACK_DAYS: int = 0

    PAGE_SIZE = 50_000
    PAGE_TIMEOUT: int = 60  # subclasses may override (e.g. slow Socrata endpoints)

    def __init__(self) -> None:
        self.app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")
        if not self.app_token:
            logger.warning(
                "%s: NYC_OPEN_DATA_APP_TOKEN not set, requests will be throttled",
                self.SCRAPER_NAME,
            )
        self.base_url = f"{SOCRATA_BASE_URL}/{self.DATASET_ID}.json"
        self._http = requests.Session()
        self._http.headers.update(
            {
                "Accept": "application/json",
                **({"X-App-Token": self.app_token} if self.app_token else {}),
            }
        )

    # ------------------------------------------------------------------ #
    # Socrata fetch — retry wrapper around raw HTTP GET                   #
    # ------------------------------------------------------------------ #

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _fetch_page(
        self,
        where: str,
        select: str | None = None,
        order: str = ":id",
        limit: int = PAGE_SIZE,
        offset: int = 0,
    ) -> list[dict]:
        params: dict = {
            "$where": where,
            "$limit": limit,
            "$offset": offset,
            "$order": order,
        }
        if select:
            params["$select"] = select
        logger.debug("%s: GET %s?%s", self.SCRAPER_NAME, self.base_url, urllib.parse.urlencode(params))
        resp = self._http.get(self.base_url, params=params, timeout=self.PAGE_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def paginate(
        self,
        where: str,
        select: str | None = None,
        order: str = ":id",
    ):
        """Yields every record matching where, page by page."""
        offset = 0
        while True:
            page = self._fetch_page(where, select, order, self.PAGE_SIZE, offset)
            if not page:
                break
            yield from page
            if len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

    # ------------------------------------------------------------------ #
    # Watermark helpers                                                   #
    # ------------------------------------------------------------------ #

    def get_watermark(self, db) -> datetime | None:
        """Return the watermark timestamp from the last successful run."""
        run = (
            db.query(ScraperRun)
            .filter(
                ScraperRun.scraper_name == self.SCRAPER_NAME,
                ScraperRun.status == "success",
                ScraperRun.watermark_timestamp.isnot(None),
            )
            .order_by(ScraperRun.started_at.desc())
            .first()
        )
        return run.watermark_timestamp if run else None

    def build_where_since(self, date_field: str, db) -> str:
        """
        Build a SoQL $where clause for incremental fetch.
        First run: looks back INITIAL_LOOKBACK_DAYS.
        Subsequent runs: uses stored watermark minus 10 minutes (clock-skew buffer).
        """
        watermark = self.get_watermark(db)
        if watermark:
            since = watermark - timedelta(minutes=10) - timedelta(days=self.WATERMARK_EXTRA_LOOKBACK_DAYS)
        else:
            since = datetime.now(timezone.utc) - timedelta(days=self.INITIAL_LOOKBACK_DAYS)
        return f"{date_field} > '{since.strftime('%Y-%m-%dT%H:%M:%S.000')}'"

    # ------------------------------------------------------------------ #
    # Anomaly detection                                                  #
    # ------------------------------------------------------------------ #

    def _compute_rolling_avg(self, db, current_started_at: datetime) -> float | None:
        """
        Return the mean records_processed over the prior _ROLLING_WINDOW_DAYS days
        of successful runs for this scraper, excluding the current run.
        Returns None when fewer than _ROLLING_MIN_SAMPLES exist (e.g. first few
        runs, weekly/annual scrapers with sparse history).
        """
        row = db.execute(
            text("""
                SELECT AVG(records_processed) AS avg_val, COUNT(*) AS n
                FROM scraper_runs
                WHERE scraper_name   = :name
                  AND status         = 'success'
                  AND started_at     >= :window_start
                  AND started_at     < :current_started_at
            """),
            {
                "name": self.SCRAPER_NAME,
                "window_start": current_started_at - timedelta(days=_ROLLING_WINDOW_DAYS),
                "current_started_at": current_started_at,
            },
        ).fetchone()
        if not row or row.n < _ROLLING_MIN_SAMPLES:
            return None
        return float(row.avg_val)

    # ------------------------------------------------------------------ #
    # Quarantine                                                         #
    # ------------------------------------------------------------------ #

    def quarantine(self, db, raw_data: dict, reason: str) -> None:
        """Route an invalid record to scraper_quarantine instead of dropping it."""
        db.add(
            ScraperQuarantine(
                scraper_name=self.SCRAPER_NAME,
                raw_data=raw_data,
                reason=reason,
                created_at=datetime.now(timezone.utc),
            )
        )

    # ------------------------------------------------------------------ #
    # Run entry point — called by the scheduler                          #
    # ------------------------------------------------------------------ #

    def run(self, db) -> ScraperRun:
        """
        Public entry point called by the scheduler pipeline.
        Wraps _run() with ScraperRun logging and anomaly detection.
        Always writes a ScraperRun row, even on failure.
        """
        started_at = datetime.now(timezone.utc)
        scraper_run = ScraperRun(
            scraper_name=self.SCRAPER_NAME,
            started_at=started_at,
            status="running",
            expected_min_records=SCRAPER_EXPECTED_MIN_RECORDS.get(self.SCRAPER_NAME),
        )
        db.add(scraper_run)
        db.commit()
        run_id = scraper_run.id  # capture before _run() commits can expire the instance

        records_processed = 0
        records_failed = 0
        new_watermark = None

        try:
            records_processed, records_failed, new_watermark = self._run(db)

            # Re-fetch by pk after long-running _run() — the session may have
            # committed/rolled back internally, leaving this instance detached.
            fresh = db.get(ScraperRun, run_id)
            if isinstance(fresh, ScraperRun):
                scraper_run = fresh

            warnings: list[str] = []

            expected_min = SCRAPER_EXPECTED_MIN_RECORDS.get(self.SCRAPER_NAME)
            if expected_min and records_processed < expected_min * 0.5:
                warnings.append(
                    f"count {records_processed} < 50% of static minimum {expected_min}"
                )

            rolling_avg = self._compute_rolling_avg(db, started_at)
            if rolling_avg is not None and records_processed < rolling_avg * 0.5:
                warnings.append(
                    f"count {records_processed} < 50% of {_ROLLING_WINDOW_DAYS}-day "
                    f"rolling average {rolling_avg:.0f}"
                )

            if warnings:
                msg = "; ".join(warnings)
                # Low counts can reflect temporary NYC Open Data ingestion lag
                # (e.g. HPD daily bulk upload not yet available) rather than a
                # scraper defect — confirm watermark advanced before investigating.
                logger.warning("%s: anomaly: %s", self.SCRAPER_NAME, msg)
                scraper_run.warning_message = msg

            if records_processed == 0 and rolling_avg is not None and rolling_avg > 100:
                scraper_run.status = "warning"
            else:
                scraper_run.status = "success"
            logger.info(
                "%s: done: %d processed, %d quarantined",
                self.SCRAPER_NAME,
                records_processed,
                records_failed,
            )
        except Exception as exc:
            # Rollback FIRST: a DB error in _run() leaves the connection in an
            # aborted transaction state.  Calling db.get() before rollback raises
            # InFailedSqlTransaction and replaces the original exception, leaving
            # the ScraperRun row stuck at status='running'.
            try:
                db.rollback()
            except Exception:
                pass
            # Re-fetch after rollback — the instance may have been expired or
            # detached by commits inside _run().
            fresh = db.get(ScraperRun, run_id)
            if isinstance(fresh, ScraperRun):
                scraper_run = fresh
            # Set status and error AFTER rollback.  SQLAlchemy clears dirty
            # tracking on rollback, so anything set before rollback is silently
            # dropped from the next flush.  Setting them here ensures the
            # finally commit writes them.
            scraper_run.status = "failure"
            scraper_run.error_message = str(exc)
            logger.exception("%s: failed: %s", self.SCRAPER_NAME, exc)
            raise
        finally:
            scraper_run.records_processed = records_processed
            scraper_run.records_failed = records_failed
            scraper_run.completed_at = datetime.now(timezone.utc)
            scraper_run.watermark_timestamp = new_watermark
            db.add(scraper_run)
            db.commit()

        return scraper_run

    @abstractmethod
    def _run(self, db) -> tuple[int, int, datetime | None]:
        """
        Subclass implements the actual scraping and persistence logic.

        Returns:
            (records_processed, records_failed, new_watermark)
            new_watermark: max date seen this run, stored in ScraperRun
                           so the next run knows where to continue from.
        """
        ...
