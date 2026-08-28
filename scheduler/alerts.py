"""
Webhook alerting helper for the PulseCities nightly pipeline.

Sends fire-and-forget POST notifications to a configured webhook URL
(Slack or Discord) when the pipeline detects anomalies:
- Scraper quarantine rate exceeds 10% (possible upstream schema change)
- Scraper records processed < 50% of expected minimum (data gap)
- Scoring engine returns 0 zip codes (systemic scoring failure)

Configuration:
    ALERT_WEBHOOK_URL — Slack or Discord incoming webhook URL. Optional; alerts
                        also buffer for a single end-of-run ops email, so they
                        reach a human even with no webhook configured.
    ALERT_WEBHOOK_FORMAT — "slack" (default) or "discord".
                           Slack uses {"text": ...}, Discord uses {"content": ...}.
    ALERT_SNOOZE — comma-separated substrings. Alerts whose subject or body
                   matches one are logged but not emailed or webhooked. For
                   acknowledged, long-running upstream conditions (a stalled
                   source that would otherwise warn every night).
                   Match the full alert subject, not a bare scraper name: a
                   pattern of 'dcwp_licenses' also silences that scraper's
                   hard failures and quarantine spikes. Append '@YYYY-MM-DD'
                   to record when the entry was added; weekly_ops_health
                   surfaces anything older than 30 days for review.

Failure contract:
    send_alert(), flush_alerts(), and notify_ops() NEVER raise. Delivery
    failure is always logged as WARNING and swallowed — alerting must never
    cause a pipeline failure.
"""
import logging
import os
import time
from datetime import date

import requests

logger = logging.getLogger(__name__)

ALERT_WEBHOOK_URL: str = os.getenv("ALERT_WEBHOOK_URL", "")
ALERT_WEBHOOK_FORMAT: str = os.getenv("ALERT_WEBHOOK_FORMAT", "slack")

# Anomalies raised during a pipeline run, drained by flush_alerts() into one
# ops email. Batching keeps a run with several warnings to a single message.
_pending: list[tuple[str, str]] = []


def _parse_snoozes() -> list[tuple[str, "date | None"]]:
    """Parse ALERT_SNOOZE into (pattern, date it was set).

    An entry may carry an `@YYYY-MM-DD` suffix recording when it was added. The
    date is metadata for review only: matching always uses the pattern alone.
    An entry with no date, or an unparseable one, reports None so it shows up in
    stale_snoozes() rather than quietly living forever.
    """
    entries: list[tuple[str, date | None]] = []
    for raw in os.getenv("ALERT_SNOOZE", "").split(","):
        raw = raw.strip()
        if not raw:
            continue
        pattern, sep, suffix = raw.rpartition("@")
        if not sep:
            entries.append((raw, None))
            continue
        try:
            entries.append((pattern.strip(), date.fromisoformat(suffix.strip())))
        except ValueError:
            # The '@' was part of the pattern, not a date suffix.
            entries.append((raw, None))
    return entries


def active_snoozes() -> list[tuple[str, "date | None"]]:
    """Every configured snooze, for reporting. See scripts/weekly_ops_health.py."""
    return _parse_snoozes()


def stale_snoozes(max_age_days: int = 30) -> list[tuple[str, "date | None", int | None]]:
    """Snoozes old enough to deserve a second look, as (pattern, set_on, age_days).

    A snooze is a deliberate blind spot. It is fine to have one, and not fine to
    forget it: the DCWP entry outlived its own justification and would have kept
    swallowing alerts after the feed thawed. Undated entries are always returned,
    since nobody can say whether they are still needed.
    """
    today = date.today()
    stale: list[tuple[str, date | None, int | None]] = []
    for pattern, set_on in _parse_snoozes():
        if set_on is None:
            stale.append((pattern, None, None))
            continue
        age = (today - set_on).days
        if age > max_age_days:
            stale.append((pattern, set_on, age))
    return stale


def _snoozed(subject: str, body: str) -> bool:
    """True when this alert matches a configured snooze.

    Patterns match against the subject and body together, so they must be
    specific enough to name the condition being silenced and nothing else. A
    bare scraper name here silences that scraper's hard failures and quarantine
    spikes alongside the benign anomaly it was meant for.
    """
    haystack = f"{subject}\n{body}"
    return any(pattern in haystack for pattern, _set_on in _parse_snoozes())


def send_alert(subject: str, body: str) -> None:
    """
    Report a pipeline anomaly. Posts to ALERT_WEBHOOK_URL when configured, and
    always buffers the alert for the end-of-run ops email (see flush_alerts),
    so anomalies reach a human either way.

    Never raises — all exceptions are caught and logged.

    Args:
        subject: Short alert title (e.g., "Scraper quarantine rate high")
        body: Detail text (e.g., "311_complaints: 45% quarantine rate (threshold: 10%)")
    """
    # Read env vars at call time so patch.dict(os.environ, ...) works in tests
    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "")
    webhook_format = os.getenv("ALERT_WEBHOOK_FORMAT", "slack")

    if _snoozed(subject, body):
        logger.info("ALERT (snoozed via ALERT_SNOOZE): %s — %s", subject, body)
        return

    _pending.append((subject, body))
    logger.warning("ALERT: %s — %s", subject, body)
    _post_webhook(subject, body, webhook_url, webhook_format)


def _post_webhook(subject: str, body: str, webhook_url: str, webhook_format: str) -> None:
    if not webhook_url:
        return

    message = f"*PulseCities Alert* — {subject}\n{body}"
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


def flush_alerts() -> None:
    """Email every alert buffered by send_alert() during this run as one ops
    message, then clear the buffer. Call once at the end of a pipeline run.
    No-ops when nothing was buffered. Never raises."""
    if not _pending:
        return
    try:
        count = len(_pending)
        lines = []
        for subject, body in _pending:
            lines.append(f"- {subject}\n  {body}")
        send_ops_email(
            f"{count} pipeline {'anomaly' if count == 1 else 'anomalies'} this run",
            "The nightly pipeline raised the following alerts:\n\n"
            + "\n\n".join(lines)
            + "\n\nFull context: tail -200 /var/log/pulsecities/scraper.log",
        )
    except Exception as exc:  # noqa: BLE001 — alerting must never break the pipeline
        logger.warning("Failed to flush buffered alerts (non-fatal): %s", exc)
    finally:
        _pending.clear()


def notify_ops(subject: str, body: str) -> None:
    """Severe-path escalation: webhook (when configured) plus an immediate ops
    email. For failures that must reach a human tonight, not in tomorrow's
    batch — scoring crashes, probe outages. Skips the flush buffer so the
    message is never sent twice, and deliberately ignores ALERT_SNOOZE.
    Never raises."""
    logger.warning("ALERT (escalated): %s — %s", subject, body)
    _post_webhook(
        subject, body,
        os.getenv("ALERT_WEBHOOK_URL", ""),
        os.getenv("ALERT_WEBHOOK_FORMAT", "slack"),
    )
    send_ops_email(subject, body)


def send_ops_email(subject: str, body: str) -> None:
    """Email an operator when the nightly pipeline fails, using the same Resend
    account the digest already uses. This is the self-monitoring hook: without
    it, a broken pipeline only leaves a log line no one reads.

    Recipient is ALERT_EMAIL (defaults to the ops inbox). No-ops quietly when
    Resend is not configured. Never raises.
    """
    # Comma-separated recipients; defaults to the ops inbox plus Michael's
    # personal address. Override with ALERT_EMAIL.
    raw = os.getenv("ALERT_EMAIL", "nycdisplacement@gmail.com,michaelespin15@gmail.com")
    recipients = [addr.strip() for addr in raw.split(",") if addr.strip()]
    api_key = os.getenv("RESEND_API_KEY", "")
    if not api_key or not recipients:
        logger.warning("ops-email skipped (no RESEND_API_KEY or ALERT_EMAIL): %s", subject)
        return

    try:
        import resend
    except Exception as exc:
        logger.warning("Failed to import resend (non-fatal): %s", exc)
        return

    resend.api_key = api_key
    # Through the shared gate for the recipient, subject and unfilled-placeholder
    # checks, but with min_words=0 and no content_items. An ops alert is allowed
    # to be terse: "acris_ownership: source frozen 27d" is nine words and is the
    # single most important message this system can send. A content floor that
    # silenced it would be far worse than the empty email the floor exists to
    # stop. The retry ladder moved into the mailer with it; it was added after
    # the 2026-08-05 outage email died on one TLS error to api.resend.com.
    try:
        ok = mailer.send(
            to=recipients,
            subject=f"[PulseCities] {subject}",
            text=body,
            sender="PulseCities Ops <alerts@pulsecities.com>",
            min_words=0,
            retries=(2, 8),
        )
    except mailer.EmailRefused as exc:
        logger.warning("ops email refused (non-fatal): %s", exc)
        return
    if ok:
        logger.info("ops-email sent to %s: %s", ", ".join(recipients), subject)
    else:
        logger.warning("Failed to send ops email after 3 attempts (non-fatal)")
