"""
Unit tests for BaseScraper.run() — scrapers/base.py.
"""

import pytest
from unittest.mock import MagicMock, patch


class TestBaseScraperDetachedInstance:
    """Bug 2: run() must re-fetch ScraperRun by pk after _run() to prevent detached-instance error."""

    def test_run_refetches_scraper_run_after_detach(self):
        """
        Simulate a long-running _run() that leaves the original ScraperRun detached.
        run() must call db.get(ScraperRun, pk) after _run() and use the fresh instance.
        """
        from scrapers.base import BaseScraper
        from models.scraper import ScraperRun

        class _Scraper(BaseScraper):
            SCRAPER_NAME = "test_detach"
            DATASET_ID = "xxxx-0000"

            def _run(self, db):
                return 5, 0, None

        fresh_run = ScraperRun(scraper_name="test_detach", status="running")

        db = MagicMock()
        db.get.return_value = fresh_run

        with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
            scraper = _Scraper()

        with patch("scrapers.base.SCRAPER_EXPECTED_MIN_RECORDS", {}), \
             patch.object(scraper, "_compute_rolling_avg", return_value=None):
            result = scraper.run(db)

        assert db.get.called, "db.get() must be called to re-fetch the ScraperRun after _run()"
        assert result is fresh_run, "run() must return the re-fetched instance, not the original stale object"
        assert result.status == "success"
        assert result.records_processed == 5
