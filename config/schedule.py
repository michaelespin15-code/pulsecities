"""
Digest send schedule — single source of truth.

The cron entry in /etc/cron.d/pulsecities must match DIGEST_CRON.
The confirmation email in api/routes/subscribe.py uses DIGEST_SEND_DAY.
The frontend reads /api/schedule to build its success message.
"""

DIGEST_SEND_DAY      = "Sunday"
DIGEST_CRON          = "0 9 * * 0"
DIGEST_SEND_TIMEZONE = "UTC"
