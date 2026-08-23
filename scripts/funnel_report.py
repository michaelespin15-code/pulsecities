"""
Search-to-signup funnel, per landing template.

The site has four capture points and no way to see which one earns its place.
Plausible records that a signup happened; it does not tie a search landing on
/property to the watch that came out of it, so without this the only number
available is the total, and the total cannot tell you whether /llc converts
half as well as /property or fifty times worse.

Both halves are already on the box, they were just never joined:

  - landings come from the nginx access logs, counted as unique client IPs
    carrying a search-engine referrer. Counting requests instead of IPs
    overstates by roughly 20x here, because crawlers and vulnerability
    scanners are the bulk of the traffic and one visitor pulls several
    subresources.
  - signups come from subscribers, bucketed by which target column is set.
    That column IS the template: bbl is only offered on /property,
    entity_slug only on /llc, family_slug only on /network, zip_code only on
    /neighborhood. No attribution column is needed to know where a row was
    born.

Usage:
    python -m scripts.funnel_report                # last 14 days
    python -m scripts.funnel_report --days 30
    python -m scripts.funnel_report --json         # machine-readable
"""

import argparse
import gzip
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from config.logging_config import configure_logging
from models.database import get_scraper_db  # imports load_dotenv() as a side effect

configure_logging()
logger = logging.getLogger(__name__)

LOG_DIR = Path("/var/log/nginx")

# "IP - - [23/Aug/2026:05:04:27 +0000] "GET /path HTTP/1.1" 200 1234 "ref" "ua""
_LINE = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] "(?P<method>[A-Z]+) (?P<path>\S+)[^"]*" '
    r'(?P<status>\d{3}) \S+ "(?P<ref>[^"]*)" "(?P<ua>[^"]*)"'
)
_SEARCH_REF = re.compile(r'https?://[^/]*\b(google|bing|duckduckgo|yandex|ecosia|search\.brave)\.',
                         re.I)
_ASSET = re.compile(r'\.(css|js|png|jpg|jpeg|svg|webp|ico|woff2?|gz|xml|txt|map|json|pbf|mvt)$', re.I)
_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}

# Landing template -> the subscribers column a signup there writes. Order
# matters: the first matching prefix wins, so /llc must be tested before the
# bare-page fallback.
TEMPLATES = [
    ("/property/", "property", "bbl"),
    ("/llc/",      "llc",      "entity_slug"),
    ("/neighborhood/", "neighborhood", "zip_code"),
    ("/network/",  "network",  "family_slug"),
    ("/operator/", "operator",  "operator_slug"),
]


def _parse_ts(raw: str) -> datetime | None:
    # 23/Aug/2026:05:04:27 +0000
    try:
        d, mon, rest = raw.split("/", 2)
        y, hh, mm, ss = rest.split(":", 3)
        return datetime(int(y), _MONTHS[mon], int(d), int(hh), int(mm),
                        int(ss.split()[0]), tzinfo=timezone.utc)
    except (ValueError, KeyError, IndexError):
        return None


def _template_for(path: str) -> str | None:
    path = path.split("?", 1)[0]
    for prefix, name, _col in TEMPLATES:
        if path.startswith(prefix) and len(path) > len(prefix):
            return name
    return None


def count_landings(days: int) -> dict[str, set[str]]:
    """Unique search-referred client IPs per template, over the window.

    Reads the rotated logs as well as the live one. logrotate keeps about two
    weeks here, so a longer --days silently measures only what survives; the
    report prints the window it actually covered rather than the one asked
    for."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    seen: dict[str, set[str]] = defaultdict(set)
    oldest: datetime | None = None

    files = sorted(LOG_DIR.glob("access.log*"), key=lambda p: p.stat().st_mtime)
    for f in files:
        opener = gzip.open if f.suffix == ".gz" else open
        try:
            with opener(f, "rt", errors="replace") as fh:
                for line in fh:
                    m = _LINE.match(line)
                    if not m or not _SEARCH_REF.match(m["ref"] or ""):
                        continue
                    if _ASSET.search(m["path"].split("?", 1)[0]):
                        continue
                    ts = _parse_ts(m["ts"])
                    if ts is None or ts < cutoff:
                        continue
                    tmpl = _template_for(m["path"])
                    if tmpl:
                        seen[tmpl].add(m["ip"])
                        if oldest is None or ts < oldest:
                            oldest = ts
        except OSError as exc:
            logger.warning("could not read %s: %s", f, exc)

    seen["_oldest"] = {oldest.isoformat()} if oldest else set()
    return seen


def count_signups(db, days: int) -> dict[str, int]:
    """Confirmed signups per template in the window, from the target column."""
    out: dict[str, int] = {}
    for _prefix, name, col in TEMPLATES:
        out[name] = db.execute(text(f"""
            SELECT count(*) FROM subscribers
            WHERE {col} IS NOT NULL
              AND created_at > now() - make_interval(days => :d)
        """), {"d": days}).scalar() or 0
    return out


def build(days: int) -> dict:
    landings = count_landings(days)
    oldest = next(iter(landings.pop("_oldest", set())), None)
    with get_scraper_db() as db:
        signups = count_signups(db, days)
        totals = db.execute(text("""
            SELECT count(*) AS all_rows,
                   count(*) FILTER (WHERE created_at > now() - make_interval(days => :d)) AS in_window
            FROM subscribers
        """), {"d": days}).fetchone()

    rows = []
    for _prefix, name, col in TEMPLATES:
        land = len(landings.get(name, ()))
        sign = signups.get(name, 0)
        rows.append({
            "template": name,
            "column": col,
            "landings": land,
            "signups": sign,
            "rate_pct": round(100.0 * sign / land, 2) if land else None,
        })
    rows.sort(key=lambda r: r["landings"], reverse=True)
    return {
        "days": days,
        "oldest_log_entry": oldest,
        "rows": rows,
        "subscribers_total": totals.all_rows,
        "subscribers_in_window": totals.in_window,
    }


def render(report: dict) -> str:
    out = [f"Search-to-signup funnel, last {report['days']} days",
           f"logs reach back to {report['oldest_log_entry'] or 'no matching entries'}",
           ""]
    out.append(f"  {'template':14} {'landings':>9} {'signups':>8} {'rate':>8}   capture column")
    out.append(f"  {'-'*14} {'-'*9:>9} {'-'*8:>8} {'-'*8:>8}   {'-'*14}")
    for r in report["rows"]:
        rate = "n/a" if r["rate_pct"] is None else f"{r['rate_pct']:.2f}%"
        out.append(f"  {r['template']:14} {r['landings']:>9,} {r['signups']:>8,} {rate:>8}   {r['column']}")
    tot_l = sum(r["landings"] for r in report["rows"])
    tot_s = sum(r["signups"] for r in report["rows"])
    rate = f"{100.0 * tot_s / tot_l:.2f}%" if tot_l else "n/a"
    out.append(f"  {'-'*14} {'-'*9:>9} {'-'*8:>8} {'-'*8:>8}")
    out.append(f"  {'all':14} {tot_l:>9,} {tot_s:>8,} {rate:>8}")
    out.append("")
    out.append(f"  subscribers in window: {report['subscribers_in_window']:,}"
               f"   all time: {report['subscribers_total']:,}")
    out.append("")
    out.append("  Landings are unique search-referred client IPs, not requests.")
    out.append("  A template with landings and no capture column cannot convert,")
    out.append("  however much traffic it takes.")
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description="PulseCities search-to-signup funnel")
    ap.add_argument("--days", type=int, default=14, help="window in days (default 14)")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = ap.parse_args()

    report = build(args.days)
    print(json.dumps(report, indent=2) if args.json else render(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
