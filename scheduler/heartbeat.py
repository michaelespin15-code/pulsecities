"""
Outbound heartbeat to an external dead-man's switch.

Every monitor this project has runs on the box it watches. That is fine for the
failures we anticipated and useless for the one we did not: on 2026-08-15 the
box stopped, and so did every check that would have said so. Silence read as
health, which is the fifth shape in docs/ops/failure_patterns.md.

A dead-man's switch inverts the test. Instead of asking a local process to
notice a problem and send mail, it asks a service elsewhere to expect a ping on
a schedule and complain when one does not arrive. A pipeline that dies, a box
that reboots, a cron that gets removed, a network that drops: all of them look
the same from outside, which is the point.

Setup (one time, on the provider):
    1. Create a check with a period matching the job's cron and a grace window.
    2. Point HEARTBEAT_BASE_URL at the ping root, e.g. https://hc-ping.com/<key>
    3. Name each check to match the slug passed to ping() below.

With HEARTBEAT_BASE_URL unset every call here is a no-op, so this is inert until
the URL is configured and can never become the reason a run fails.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

# Short. This is a status report about work that already finished; a provider
# outage must not extend the nightly wall clock or hold the pipeline lock.
_TIMEOUT_SECONDS = 5


def _base_url() -> str:
    # Read at call time so tests and a mid-life .env edit both take effect.
    return os.getenv("HEARTBEAT_BASE_URL", "").rstrip("/")


def ping(slug: str, *, ok: bool = True, detail: str = "") -> None:
    """Report that `slug` finished. ok=False records a failure without waiting.

    Never raises and never blocks for long. A monitoring call that can break the
    job it monitors is worse than no monitoring, because it also looks like the
    job's own fault.

    Sending the failure ping matters as much as the success one: it turns a job
    that ran and failed into an immediate signal, instead of waiting out the
    grace period alongside a box that burned down.
    """
    base = _base_url()
    if not base:
        logger.debug("heartbeat skipped for %s (HEARTBEAT_BASE_URL unset)", slug)
        return

    url = f"{base}/{slug}" if ok else f"{base}/{slug}/fail"
    try:
        requests.post(url, data=detail[:1000].encode("utf-8"), timeout=_TIMEOUT_SECONDS)
        logger.info("heartbeat sent: %s (ok=%s)", slug, ok)
    except Exception as exc:
        # A missed ping is exactly what the switch is built to catch, so failing
        # quietly here still surfaces as an alert from the other side.
        logger.warning("heartbeat failed for %s: %r", slug, exc)
