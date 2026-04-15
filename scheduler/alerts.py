"""
Webhook alerting helper for the PulseCities nightly pipeline.

Sends fire-and-forget POST notifications to a configured webhook URL
(Slack or Discord) when the pipeline detects anomalies:
- Scraper quarantine rate exceeds 10% (possible upstream schema change)
- Scraper records processed < 50% of expected minimum (data gap)
- Scoring engine returns 0 zip codes (systemic scoring failure)

Configuration:
    ALERT_WEBHOOK_URL — Slack or Discord incoming webhook URL.
                        If empty or absent, alerts are logged as WARNING only.
    ALERT_WEBHOOK_FORMAT — "slack" (default) or "discord".
                           Slack uses {"text": ...}, Discord uses {"content": ...}.

Failure contract:
    send_alert() NEVER raises an exception. Webhook failure is always logged
    as WARNING and silently swallowed — it must never cause a pipeline failure.
"""
import logging
import os

import requests

logger = logging.getLogger(__name__)

ALERT_WEBHOOK_URL: str = os.getenv("ALERT_WEBHOOK_URL", "")
ALERT_WEBHOOK_FORMAT: str = os.getenv("ALERT_WEBHOOK_FORMAT", "slack")


def send_alert(subject: str, body: str) -> None:
    """
    POST a JSON alert payload to ALERT_WEBHOOK_URL.

    If ALERT_WEBHOOK_URL is not configured, logs a WARNING instead.
    Never raises — all exceptions are caught and logged.

    Args:
        subject: Short alert title (e.g., "Scraper quarantine rate high")
        body: Detail text (e.g., "311_complaints: 45% quarantine rate (threshold: 10%)")
    """
    # Read env vars at call time so patch.dict(os.environ, ...) works in tests
    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "")
    webhook_format = os.getenv("ALERT_WEBHOOK_FORMAT", "slack")

    message = f"*PulseCities Alert* — {subject}\n{body}"

    if not webhook_url:
        logger.warning("ALERT (no webhook configured): %s — %s", subject, body)
        return

    if webhook_format == "discord":
        payload = {"content": message}
    else:
        # Default: Slack incoming webhook format
        payload = {"text": message}

    try:
        requests.post(
            webhook_url,
            json=payload,
            timeout=5,
        )
    except Exception as exc:
        logger.warning(
            "Failed to send webhook alert (non-fatal): %s",
            exc,
        )
