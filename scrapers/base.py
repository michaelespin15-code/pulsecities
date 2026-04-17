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
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from config.nyc import SCRAPER_EXPECTED_MIN_RECORDS, SOCRATA_BASE_URL
from models.scraper import ScraperQuarantine, ScraperRun

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

        records_processed = 0
        records_failed = 0
        new_watermark = None

        try:
            records_processed, records_failed, new_watermark = self._run(db)

            expected_min = SCRAPER_EXPECTED_MIN_RECORDS.get(self.SCRAPER_NAME)
            if expected_min and records_processed < expected_min * 0.5:
                logger.warning(
                    "%s: only %d records processed (expected >= %d). "
                    "Possible upstream API issue or data gap; review quarantine table.",
                    self.SCRAPER_NAME,
                    records_processed,
                    expected_min,
                )

            scraper_run.status = "success"
            logger.info(
                "%s: done: %d processed, %d quarantined",
                self.SCRAPER_NAME,
                records_processed,
                records_failed,
            )
        except Exception as exc:
            scraper_run.status = "failure"
            scraper_run.error_message = str(exc)
            logger.exception("%s: failed: %s", self.SCRAPER_NAME, exc)
            # A mid-scraper DB error (e.g. DataError) leaves PostgreSQL in an
            # aborted transaction state.  Roll back now so the finally block
            # can open a fresh transaction to persist the failure metadata.
            try:
                db.rollback()
            except Exception:
                pass
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
