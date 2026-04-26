"""
Unit tests for BaseScraper.run() — scrapers/base.py.

Covers three bugs fixed in the failure-path exception handler:

  Bug 1 (DCWP): When _run() raises a non-DB error (e.g. HTTP 400), the old code
    set status="failure" then called db.rollback(). SQLAlchemy clears dirty tracking
    on rollback, so the status was silently dropped and the row stayed as "running".

  Bug 2 (DHCR): When _run() raises a DB error (e.g. DataError from a mid-run
    upsert), PostgreSQL aborts the transaction. The old code called db.get() BEFORE
    db.rollback(), which raised InFailedSqlTransaction and replaced the original
    exception, leaving the row stuck at "running".

  Bug 3 (HPD): Same pattern as DHCR — db.get() before db.rollback() in the
    failure path.

Fix: rollback first (clears aborted transaction), then re-fetch (clean connection),
then set status/error (dirty in the fresh transaction, survives the finally commit).
"""

import pytest
from unittest.mock import MagicMock, patch, call


class TestBaseScraperDetachedInstance:
    """Success path: run() must re-fetch ScraperRun by pk after _run()."""

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


class TestBaseScraperFailurePath:
    """Failure path: run() must write status='failure' even when _run() raises."""

    def _make_scraper(self, exc):
        from scrapers.base import BaseScraper

        class _FailScraper(BaseScraper):
            SCRAPER_NAME = "test_fail"
            DATASET_ID = "xxxx-0001"

            def _run(self, db):
                raise exc

        with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
            return _FailScraper()

    def _make_db(self, fresh_run):
        db = MagicMock()
        db.get.return_value = fresh_run
        return db

    def test_failure_writes_status_on_http_error(self):
        """Bug 1 (DCWP): HTTP 400 from _run() must produce status='failure', not 'running'."""
        import requests
        from models.scraper import ScraperRun

        fresh_run = ScraperRun(scraper_name="test_fail", status="running")
        db = self._make_db(fresh_run)
        scraper = self._make_scraper(requests.HTTPError("400 Bad Request"))

        with pytest.raises(requests.HTTPError):
            with patch("scrapers.base.SCRAPER_EXPECTED_MIN_RECORDS", {}):
                scraper.run(db)

        assert fresh_run.status == "failure", (
            "status must be 'failure' after HTTP error — not left as 'running'"
        )
        assert fresh_run.error_message is not None
        assert "400" in fresh_run.error_message

    def test_failure_writes_status_on_db_error(self):
        """Bug 2/3 (DHCR/HPD): DB error from _run() must produce status='failure', not 'running'."""
        from sqlalchemy.exc import OperationalError
        from models.scraper import ScraperRun

        fresh_run = ScraperRun(scraper_name="test_fail", status="running")
        db = self._make_db(fresh_run)
        scraper = self._make_scraper(OperationalError("stmt", {}, Exception("aborted")))

        with pytest.raises(OperationalError):
            with patch("scrapers.base.SCRAPER_EXPECTED_MIN_RECORDS", {}):
                scraper.run(db)

        assert fresh_run.status == "failure", (
            "status must be 'failure' after DB error — not left as 'running'"
        )
        assert fresh_run.error_message is not None

    def test_rollback_precedes_refetch_in_failure_path(self):
        """
        Bug 2/3: rollback must happen before db.get() in the failure path.
        On an aborted transaction, db.get() before rollback raises InFailedSqlTransaction,
        masking the original error and leaving the row stuck at 'running'.
        """
        from models.scraper import ScraperRun

        call_order: list[str] = []
        fresh_run = ScraperRun(scraper_name="test_order", status="running")

        db = MagicMock()
        db.rollback.side_effect = lambda: call_order.append("rollback")
        # db.get() is called in both success and failure paths; track each call
        db.get.side_effect = lambda *a, **kw: (call_order.append("get"), fresh_run)[1]

        class _ErrScraper:
            pass

        from scrapers.base import BaseScraper

        class _FailScraper(BaseScraper):
            SCRAPER_NAME = "test_order"
            DATASET_ID = "xxxx-0002"

            def _run(self, db):
                raise RuntimeError("simulated failure")

        with patch.dict("os.environ", {"NYC_OPEN_DATA_APP_TOKEN": "test"}):
            scraper = _FailScraper()

        with pytest.raises(RuntimeError):
            with patch("scrapers.base.SCRAPER_EXPECTED_MIN_RECORDS", {}):
                scraper.run(db)

        assert "rollback" in call_order, "db.rollback() must be called in the failure path"
        assert "get" in call_order, "db.get() must be called to re-fetch ScraperRun"

        rollback_idx = next(i for i, c in enumerate(call_order) if c == "rollback")
        get_idx = next(i for i, c in enumerate(call_order) if c == "get")
        assert rollback_idx < get_idx, (
            f"rollback (pos {rollback_idx}) must precede db.get() (pos {get_idx}) "
            "so the aborted transaction is cleared before re-fetching"
        )
