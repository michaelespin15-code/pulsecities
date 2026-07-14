"""
Digest send schedule: single source of truth.

DIGEST_CRON is expressed in DIGEST_SEND_TIMEZONE (America/New_York). The actual
/etc/cron.d/pulsecities entry fires at the two UTC slots that straddle 18:00 ET
(22:00 and 23:00) and guards on the Eastern clock, so the send stays at 6 PM ET
across daylight saving. Keep this file and that cron entry in step.

The confirmation email (api/routes/subscribe.py) and the frontend success
message (via /api/schedule) read DIGEST_SEND_DAY.
"""

DIGEST_SEND_DAY      = "Sunday"
DIGEST_CRON          = "0 18 * * 0"
DIGEST_SEND_TIMEZONE = "America/New_York"
