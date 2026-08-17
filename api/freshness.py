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
    ("permits",    "dob_permits",     "permits_raw",    "filing_date",      10),
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


def through_sql(table: str, column: str) -> str:
    """Newest record in `table` that is not dated in the future."""
    return f"SELECT MAX({column}) FROM {table} WHERE {column} <= CURRENT_DATE"


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
