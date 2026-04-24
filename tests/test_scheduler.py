"""Tests for scheduler exit code behavior (SCHED-02)."""
import sys
from unittest.mock import MagicMock, patch
import pytest


class TestPipelineFailureTracking:
    def test_scraper_failure_returns_false(self):
        """_run_scraper_with_retry returns False when scraper raises an exception."""
        from scheduler.pipeline import _run_scraper_with_retry

        # Scraper class whose run() raises
        failing_scraper = MagicMock()
        failing_scraper.return_value.run.side_effect = Exception("network error")

        with patch("scheduler.pipeline.get_scraper_db") as mock_db:
            mock_db.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            # tenacity will retry 3 times then raise; our function catches and returns False
            result = _run_scraper_with_retry("test_scraper", failing_scraper)

        assert result is False

    def test_scraper_success_returns_true(self):
        """_run_scraper_with_retry returns True when scraper runs without exception."""
        from scheduler.pipeline import _run_scraper_with_retry

        success_scraper = MagicMock()
        success_scraper.return_value.run.return_value = None  # no exception

        with patch("scheduler.pipeline.get_scraper_db") as mock_db:
            mock_ctx = MagicMock()
            mock_db.return_value.__enter__ = MagicMock(return_value=mock_ctx)
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            result = _run_scraper_with_retry("test_scraper", success_scraper)

        assert result is True

    def test_pipeline_returns_false_if_any_scraper_fails(self):
        """run_nightly_pipeline returns False when at least one scraper fails."""
        with patch("scheduler.pipeline._run_pluto_if_due", return_value=True), \
             patch("scheduler.pipeline._run_scraper_with_retry") as mock_retry, \
             patch("scheduler.pipeline._run_scoring"), \
             patch("scheduler.pipeline.get_scraper_db") as mock_db:
            mock_db.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            # Simulate: complaints OK, permits FAIL, evictions OK, acris OK, dcwp OK, dhcr OK
            mock_retry.side_effect = [True, False, True, True, True, True]

            from scheduler.pipeline import run_nightly_pipeline
            result = run_nightly_pipeline()

        assert result is False


class TestSchedulerMain:
    def test_main_exits_nonzero_on_pipeline_failure(self):
        """main() calls sys.exit(1) when run_nightly_pipeline returns False."""
        with patch("scheduler.main.run_nightly_pipeline", return_value=False), \
             patch("scheduler.main.configure_logging"):
            from scheduler.main import main
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_main_exits_zero_on_success(self):
        """main() does not call sys.exit when run_nightly_pipeline returns True."""
        with patch("scheduler.main.run_nightly_pipeline", return_value=True), \
             patch("scheduler.main.configure_logging"):
            from scheduler.main import main
            # Should complete without raising SystemExit
            main()
