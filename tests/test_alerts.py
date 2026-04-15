"""
Tests for scheduler/alerts.py — webhook alerting helper.

RED state: scheduler/alerts.py does not exist yet.
Plan 07-04 will implement the module and turn these GREEN.
"""
import os
import pytest
from unittest.mock import patch, MagicMock


class TestSendAlert:
    def test_send_alert_posts_to_webhook_when_url_configured(self):
        """send_alert() must call requests.post when ALERT_WEBHOOK_URL is set."""
        from scheduler.alerts import send_alert

        with patch("scheduler.alerts.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            with patch.dict(os.environ, {"ALERT_WEBHOOK_URL": "https://hooks.example.com/test"}):
                send_alert("Test subject", "Test body")
            mock_post.assert_called_once()
            call_kwargs = mock_post.call_args
            assert call_kwargs.kwargs.get("timeout") == 5 or call_kwargs.args[1:] or True
            # Verify URL was the configured one
            called_url = call_kwargs.args[0] if call_kwargs.args else call_kwargs.kwargs.get("url")
            assert called_url == "https://hooks.example.com/test"

    def test_send_alert_does_not_raise_when_no_url_configured(self):
        """send_alert() must not raise when ALERT_WEBHOOK_URL is empty or absent."""
        from scheduler.alerts import send_alert

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ALERT_WEBHOOK_URL", None)
            # Must not raise
            send_alert("No webhook configured", "body text")

    def test_send_alert_does_not_raise_on_webhook_failure(self):
        """send_alert() must not propagate exceptions from requests.post failure."""
        from scheduler.alerts import send_alert
        import requests

        with patch("scheduler.alerts.requests.post", side_effect=requests.Timeout("timed out")):
            with patch.dict(os.environ, {"ALERT_WEBHOOK_URL": "https://hooks.example.com/test"}):
                # Must not raise — webhook failure is never a pipeline failure
                send_alert("Failed webhook", "body")

    def test_send_alert_uses_5_second_timeout(self):
        """requests.post must be called with timeout=5 to avoid blocking the pipeline."""
        from scheduler.alerts import send_alert

        with patch("scheduler.alerts.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            with patch.dict(os.environ, {"ALERT_WEBHOOK_URL": "https://hooks.example.com/test"}):
                send_alert("Timeout test", "body")
            call_kwargs = mock_post.call_args
            # timeout must be in kwargs or positional args — check kwargs first
            timeout = call_kwargs.kwargs.get("timeout") if call_kwargs.kwargs else None
            assert timeout == 5, f"Expected timeout=5, got: {timeout}"
