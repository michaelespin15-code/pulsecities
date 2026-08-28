"""One answer to "what date is our data good through", and one answer to how far
behind a feed may fall before we say so.

Three endpoints answered the first question three different ways and two of the
answers were impossible. ACRIS instruments carry a doc_date typed in by the
filer, so a handful arrive dated in the future: two rows out of ~198,000 were
enough to make /api/status advertise data through a date thirteen days away,
and that value fed the sitewide freshness chip on /about and /status.

The rule is one line, so it lives in one place: a feed is never fresher than
its newest record that has actually happened. Note that clamping the outlier
down to today is a different fix and a worse one. It reports the feed as
current through today when the real ceiling is two weeks back, which is a
false freshness claim on a site that exists to be checkable.

The second question went the same way. Four files carried an ACRIS staleness
threshold and three of them disagreed, so on a night when deeds were 17 days
behind the public page said ok, pipeline_health exited CRITICAL, and the
nightly check emailed a stale alert. Thresholds live here now and the schedulers
read them.
"""

# Feed slug, the scraper that fills it, the table and column that date it, and
# the staleness threshold in days. Every entry filters future dates rather than
# trusting the source, since any of these columns is only as careful as whoever
# typed it in.
#
# 21 days for ACRIS. Deeds publish with a natural lag of about two weeks, so the
# old 7-day threshold reported a healthy feed as stale most of the time. A flag
# that is usually on says nothing when the feed really stops, which is the
# failure it exists to catch.
FRESHNESS_SOURCES = [
    ("acris",      "acris_ownership", "ownership_raw",  "doc_date",         21),
    # Anchored on dob_now_permits, not dob_permits. Both scrapers write
    # permits_raw.filing_date so the data-through query is unchanged, but the
    # feed's health is DOB NOW's health: BIS carries 4% of current permits and
    # is decaying on purpose, so checking it would report a feed as sick for
    # doing exactly what it is expected to do. The old name stays resolvable
    # below.
    ("permits",    "dob_now_permits", "permits_raw",    "filing_date",      10),
    ("evictions",  "evictions",       "evictions_raw",  "executed_date",    14),
    ("complaints", "311_complaints",  "complaints_raw", "created_date",     10),
    ("violations", "hpd_violations",  "violations_raw", "inspection_date",  10),
]

# Both naming schemes resolve to the same feed: the API keys by slug, the
# schedulers key by ScraperRun.scraper_name.
_BY_NAME = {}
for _slug, _scraper, _table, _column, _days in FRESHNESS_SOURCES:
    for _key in (_slug, _scraper):
        _BY_NAME[_key] = (_table, _column, _days)

# Scraper names that fill a feed listed above without being the one that names
# it. FRESHNESS_SOURCES carries one row per feed because every consumer renders
# one card per row, so a second scraper on the same table is an alias here
# rather than a second entry that would draw the permits card twice.
for _alias, _feed in (("dob_permits", "permits"),):
    _BY_NAME[_alias] = _BY_NAME[_feed]


def real_date(column: str, created: str = "created_at") -> str:
    """Predicate: this row's date actually happened, and happened before we
    wrote the row down.

    Extracted because `through_sql` was not the only reader of these columns and
    was the only guarded one. An audit on 2026-08-27 found seventeen other
    `max(doc_date)` and `max(executed_date)` queries across the API, the entity
    clustering and the sitemap generator, none of them carrying this rule. The
    cost was not hypothetical: two rows typed 2026-08-27 onto a deed recorded
    2026-07-29 put a future `<lastmod>` on 200 hub URLs and on the typo row's own
    property page, every night for sixteen nights, in the file Google and Bing
    read to decide what to recrawl.

    Callers pass their own aliases: `real_date("o.doc_date", "o.created_at")`.
    """
    return (f"{column} < CURRENT_DATE + INTERVAL '1 day' "
            f"AND {column} <= {created}")


def through_sql(table: str, column: str) -> str:
    """Newest record in `table` that is not dated in the future and that we
    actually held on the day it claims.

    The bound is `< CURRENT_DATE + 1 day` rather than `<= CURRENT_DATE`, which
    matters for the timestamp columns: CURRENT_DATE widens to midnight, so
    `created_date <= CURRENT_DATE` drops every 311 record filed so far today and
    reports the feed a day staler than it is. Keeping the column bare on the left
    also leaves it sargable, so this stays an index scan on the 5M-row tables
    rather than the seq scan a `column::date` cast would force.

    The future bound alone expires. Two ACRIS rows typed 2026-08-27 onto a deed
    recorded 2026-07-29 sat harmlessly ahead of the calendar for sixteen days,
    and on 2026-08-27 the calendar reached them: every freshness reader in the
    codebase went from "frozen 27d" to "current today" on a feed that had not
    moved since July 31, and /api/status advertised it. `column <= created_at`
    is the durable form of the same rule, since a record we hold cannot have
    happened after we wrote it down. It costs nothing: the future bound still
    drives the index scan and this filters the handful of rows it walks.
    """
    return f"SELECT MAX({column}) FROM {table} WHERE {real_date(column)}"


def staleness_days(name: str) -> int:
    """Days this feed may fall behind before it counts as stale.

    Accepts either the feed slug or the scraper name. Raises on anything else:
    a typo that silently returned a permissive default would disarm the check
    it was meant to configure.
    """
    return _BY_NAME[name][2]


def db_through_sql(name: str) -> str:
    """Data-through query for a feed, by slug or scraper name."""
    table, column, _days = _BY_NAME[name]
    return through_sql(table, column)


# /api/status anchors ACRIS to the table rather than the scraper watermark,
# because the watermark tracks recorded_datetime from the feed and can run
# ahead of the doc dates that actually persisted.
ACRIS_THROUGH_SQL = db_through_sql("acris")


def feed_anchor(db, name: str = "acris"):
    """The last day this feed actually published, for windowing.

    Reads the same query /api/status publishes, so a page and the status
    endpoint cannot disagree about where a feed stops.
    """
    from sqlalchemy import text as _text
    from datetime import date as _date
    try:
        return db.execute(_text(db_through_sql(name))).scalar() or _date.today()
    except Exception:  # noqa: BLE001 - a windowing helper must not take a page down
        return _date.today()


def window_sql(column: str, days: int, param: str = "anchor") -> str:
    """The last `days` days of *published* data, not the last `days` days.

    A window ending at CURRENT_DATE over a lagging feed spends its tail on days
    that cannot contain a record, and the shortfall is not proportional to the
    lag, it is proportional to the lag over the window. ACRIS was 28 days behind
    on 2026-08-28:

        window   deeds found, to today   to the last published deed
        30 days                      1                         558
        90 days       4 radar clusters             11 radar clusters
        365 days            639 flips                   700 flips

    A thirty-day deed count reading 1 instead of 558 is not a rounding error. It
    made `llc_acquisitions` zero for every ZIP in /api/stats, which meant the
    dominant-signal label on the site's central ranking could never say an LLC
    was buying, however many were.

    Callers bind the anchor once per query and share it across every predicate
    on the same feed:

        sql = f"... WHERE {window_sql('o.doc_date', 30)} ..."
        db.execute(text(sql), {"anchor": feed_anchor(db, "acris")})

    Only worth applying to a feed that lags. Evictions, violations and 311 were
    all within two days of the calendar when this was written; ACRIS is the one
    that freezes for weeks, and tests/test_window_anchors.py greps for a bare
    CURRENT_DATE window on its columns.
    """
    return (f"{column} > (:{param})::date - INTERVAL '{days} days' "
            f"AND {column} <= (:{param})::date")
