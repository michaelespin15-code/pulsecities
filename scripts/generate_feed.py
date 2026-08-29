"""
RSS feed of the weekly editions.

The README names this site's audience as journalists, tenant organisations,
planners and residents watching their block. Every one of those is a subscriber
by habit, and until now there was nothing to subscribe to: no feed, no
`<link rel="alternate">`, nothing at /feed.xml or /rss.xml.

Items are the completed weekly editions at /week/{slug}, which have stable URLs
and never change once past. The in-progress week at /this-week is deliberately
excluded: it is rewritten every night, so it has no fixed identity to give a
reader, and a feed that reissues the same item daily trains people to ignore it.

Enumeration and copy both come from api/routes/frontend.py rather than being
rebuilt here. The feed has to say what the page says, and a second reader of
"which weeks exist" is the shape that drifts.

Served straight from disk: nginx has a catch-all `location /` rooted at
frontend/, so this needs no location block of its own. It goes out as text/xml
rather than application/rss+xml for the same reason, which every reader accepts.

Cron: alongside the sitemap, into the same log, the way gen_llms_txt is.

    python -m scripts.generate_feed
"""
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from html import escape
from pathlib import Path

from api.routes.frontend import (
    _completed_weeks,
    _counts_between,
    _week_range_label,
    _week_slug,
)
from models.database import get_scraper_db
from scripts.generate_sitemap import _write_atomic

logger = logging.getLogger(__name__)

FRONTEND = Path(__file__).resolve().parent.parent / "frontend"
OUT = FRONTEND / "feed.xml"
SITE = "https://pulsecities.com"
MAX_ITEMS = 20

# RFC 822 wants English abbreviations. strftime("%a")/("%b") reads the process
# locale, which is not ours to assume in a cron with no LANG set.
_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def rfc822(d: date) -> str:
    return (f"{_DAYS[d.weekday()]}, {d.day:02d} {_MONTHS[d.month - 1]} {d.year} "
            f"12:00:00 +0000")


def _summary(counts) -> str:
    """What the week held, in the order the edition page reports it."""
    return (f"{counts.evictions:,} residential evictions executed, "
            f"{counts.permits:,} permit filings, "
            f"{counts.complaints:,} displacement complaints and "
            f"{counts.violations:,} housing violations are dated to this week.")


def build(db) -> str:
    weeks = _completed_weeks(db)[:MAX_ITEMS]   # newest first
    if not weeks:
        raise RuntimeError(
            "no completed weekly editions; refusing to write an empty feed"
        )

    items = []
    for monday, sunday in weeks:
        counts = _counts_between(db, monday, sunday + timedelta(days=1))
        url = f"{SITE}/week/{_week_slug(monday)}"
        items.append(
            "    <item>\n"
            f"      <title>{escape('NYC displacement, ' + _week_range_label(monday, sunday))}</title>\n"
            f"      <link>{url}</link>\n"
            f"      <guid isPermaLink=\"true\">{url}</guid>\n"
            f"      <pubDate>{rfc822(sunday)}</pubDate>\n"
            f"      <description>{escape(_summary(counts))}</description>\n"
            "    </item>"
        )

    built = datetime.now(timezone.utc)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">\n'
        "  <channel>\n"
        "    <title>PulseCities: this week in NYC displacement</title>\n"
        f"    <link>{SITE}/this-week</link>\n"
        "    <description>A weekly record of deed transfers, evictions, permit "
        "filings and housing violations across New York City, rebuilt from the "
        "public record.</description>\n"
        "    <language>en-us</language>\n"
        f"    <lastBuildDate>{rfc822(built.date())}</lastBuildDate>\n"
        f'    <atom:link href="{SITE}/feed.xml" rel="self" type="application/rss+xml"/>\n'
        + "\n".join(items) + "\n"
        "  </channel>\n"
        "</rss>\n"
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        with get_scraper_db() as db:
            body = build(db)
    except Exception as exc:
        # Leave the previous feed in place. A stale week is a smaller lie than
        # an empty publication, and the cron log carries the reason.
        logger.error("feed generation failed, keeping the existing file: %s", exc)
        return 1
    _write_atomic(OUT, body)
    logger.info("wrote %s (%d bytes)", OUT, len(body))
    return 0


if __name__ == "__main__":
    sys.exit(main())
