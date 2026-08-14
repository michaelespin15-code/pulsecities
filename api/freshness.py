"""One answer to "what date is our data good through".

Three endpoints answered this question three different ways and two of the
answers were impossible. ACRIS instruments carry a doc_date typed in by the
filer, so a handful arrive dated in the future: two rows out of ~198,000 were
enough to make /api/status advertise data through a date thirteen days away,
and that value fed the sitewide freshness chip on /about and /status.

The rule is one line, so it lives in one place: a feed is never fresher than
its newest record that has actually happened. Note that clamping the outlier
down to today is a different fix and a worse one. It reports the feed as
current through today when the real ceiling is two weeks back, which is a
false freshness claim on a site that exists to be checkable.
"""

# Feed key, the table and column that date it, and the staleness threshold in
# days. Every entry filters future dates rather than trusting the source, since
# any of these columns is only as careful as whoever typed it in.
FRESHNESS_SOURCES = [
    # 21 days for ACRIS, matching the tolerance in api/routes/status.py. Deeds
    # publish with a natural lag of about two weeks, so the old 7-day threshold
    # reported a healthy feed as stale most of the time. A flag that is usually
    # on says nothing when the feed really stops, which is the failure it exists
    # to catch.
    ("acris",      "ownership_raw",  "doc_date",         21),
    ("permits",    "permits_raw",    "filing_date",      10),
    ("evictions",  "evictions_raw",  "executed_date",    14),
    ("complaints", "complaints_raw", "created_date",     10),
    ("violations", "violations_raw", "inspection_date",  10),
]


def through_sql(table: str, column: str) -> str:
    """Newest record in `table` that is not dated in the future."""
    return f"SELECT MAX({column}) FROM {table} WHERE {column} <= CURRENT_DATE"


# /api/status anchors ACRIS to the table rather than the scraper watermark,
# because the watermark tracks recorded_datetime from the feed and can run
# ahead of the doc dates that actually persisted.
ACRIS_THROUGH_SQL = through_sql("ownership_raw", "doc_date")
