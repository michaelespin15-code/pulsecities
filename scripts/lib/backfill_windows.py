"""Rows we imported are not rows that happened.

The daily building alert and the monthly block digest both window on ingest
time rather than event date, and deliberately: ACRIS publishes deeds a median
of 47 days after they are signed and HPD releases violations 234 days late at
the 90th percentile, so a watcher keyed on event dates would never hear about
the deed on their own building. That is the right design and it has one hole.

It cannot tell a source publishing late from us importing history.

On 2026-08-28 the DOB NOW backfill loaded 485,443 permits going back to 2021.
The 03:25 alert run read them as records that had just landed and emailed a real
subscriber "New at 1062 Elton Street: 5 new records", listing five permits filed
in 2023 and 2024. Nothing about that email was true except the addresses.

An age ceiling was measured as the fix and rejected: the lag distributions above
mean any cutoff tight enough to catch a 2023 permit also drops deeds and
violations that are genuinely news. The precise question is not "is this record
old" but "did this row arrive because a source published it, or because we asked
for it", and we already record the answer. BaseScraper marks a windowed
historical walk with status 'backfill', so its run window names exactly the rows
to skip.

Backfills are rare and deliberate. Excluding the minutes one occupied costs
nothing; the alternative costs somebody's trust in the one email this system
exists to send.
"""

from datetime import timedelta

from sqlalchemy import text

# Which scrapers fill which raw table. A backfill of any of them can put history
# into that table's ingest window.
TABLE_SCRAPERS = {
    "permits_raw":    ("dob_permits", "dob_now_permits"),
    "ownership_raw":  ("acris_ownership",),
    "violations_raw": ("hpd_violations",),
    "complaints_raw": ("311_complaints",),
    "evictions_raw":  ("evictions",),
}

# created_at is stamped by the database inside the run, so the run's own bounds
# already contain every row it wrote. The pad is for clock skew between the
# application clock that writes started_at and the server clock behind now().
PAD = timedelta(minutes=1)


def backfill_ranges(db, table: str) -> list[tuple]:
    """Time ranges during which `table` was being backfilled, newest first.

    Returns [] for a table nobody has backfilled, which is the normal case and
    makes `exclusion()` a no-op rather than a special case at every call site.
    """
    scrapers = TABLE_SCRAPERS.get(table)
    if not scrapers:
        return []
    rows = db.execute(text("""
        SELECT started_at, COALESCE(completed_at, now()) AS ended_at
        FROM scraper_runs
        WHERE status = 'backfill' AND scraper_name = ANY(:names)
        ORDER BY started_at DESC
    """), {"names": list(scrapers)}).fetchall()
    return [(r.started_at - PAD, r.ended_at + PAD) for r in rows]


def exclusion(db, table: str, alias: str = "", param_prefix: str = "bf") -> tuple[str, dict]:
    """SQL fragment and bind params that skip rows a backfill wrote.

    Returns ("", {}) when the table has never been backfilled, so callers can
    concatenate unconditionally:

        skip, params = exclusion(db, "permits_raw", "pr")
        db.execute(text(f"... WHERE pr.created_at > :since {skip}"), {**p, **params})

    Bound rather than interpolated because these are timestamps from the
    database and belong in parameters, not in a formatted string.
    """
    ranges = backfill_ranges(db, table)
    if not ranges:
        return "", {}
    col = f"{alias}.created_at" if alias else "created_at"
    clauses, params = [], {}
    for i, (start, end) in enumerate(ranges):
        s, e = f"{param_prefix}{i}_s", f"{param_prefix}{i}_e"
        clauses.append(f"AND NOT ({col} >= :{s} AND {col} <= :{e})")
        params[s], params[e] = start, end
    return " " + " ".join(clauses), params
