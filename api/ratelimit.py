"""Shared rate-limit sizing.

slowapi keeps its counters in each gunicorn worker's own memory, so a route
declaring "60/minute" is enforced 60 times *per worker* and the real per-IP
ceiling is workers x limit. It shows up in the response headers as
X-RateLimit-Remaining bouncing (59, 58, 59, 57...) rather than counting down,
because requests round-robin between workers and each keeps its own tally.

nginx enforces the actual per-IP budget on /api/, where the state is shared
across workers and survives a reload. The per-route slowapi limits stay for
per-endpoint shape. That split is fine while exceeding a limit only costs a
database read; for routes that spend money, 2x the intended ceiling is a bill,
so those declare the whole-process number and divide it here.
"""

import os

# Gunicorn reads WEB_CONCURRENCY natively when --workers is absent, so the unit
# file sets it once and both the server and this module agree on the count.
WEB_CONCURRENCY = max(1, int(os.getenv("WEB_CONCURRENCY", "2")))


def per_worker(total: int, period: str) -> str:
    """Convert an intended whole-process rate limit into one worker's share.

    per_worker(20, "hour") on a 2-worker box yields "10/hour", so the process
    as a whole allows the 20 that was meant.
    """
    return f"{max(1, total // WEB_CONCURRENCY)}/{period}"
