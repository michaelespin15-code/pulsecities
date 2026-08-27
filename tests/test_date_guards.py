"""
No query may take the max of a record date without asking whether that date
actually happened.

api/freshness.py has carried this rule since 2026-08-14 and was, until
2026-08-27, the only reader that applied it. An audit that morning found
seventeen other `max(doc_date)` and `max(executed_date)` queries across the API,
the entity clustering and the sitemap generator, none of them guarded.

The cost was measurable rather than theoretical. Two ACRIS rows carry a
filer-typed doc_date of 2026-08-27 on a deed recorded 2026-07-29. They were
ingested on 2026-08-11, so for sixteen consecutive nights `scripts/generate_
sitemap.py` wrote `<lastmod>2026-08-27</lastmod>` -- a date in the future -- onto
all 200 hub URLs and onto the typo row's own property page, in the file Google
and Bing read to decide what is worth recrawling. A lastmod a crawler can prove
wrong is worse than no lastmod, because the lesson it teaches is to stop
believing the field.

This file greps rather than executes because that is the only way to catch the
eighteenth one. A test that exercises today's call sites passes forever while
someone adds a new unguarded query beside them.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# The columns that carry "when this really happened" for each feed. A max() over
# any of them is a freshness claim whether or not it is called one.
DATED_COLUMNS = ("doc_date", "executed_date", "filing_date",
                 "inspection_date", "created_date")

# Files that are allowed to take a bare max: freshness.py defines the rule, and
# the tests that prove the rule have to be able to state the unguarded form.
EXEMPT = {"api/freshness.py", "tests/test_date_guards.py",
          "tests/test_freshness_contract.py"}

# Call sites that predate this guard, by file. Everything that renders a date to
# a crawler or an API consumer was fixed on 2026-08-27: both sitemap lastmod
# queries, the /ops freshness panel, /api/stats, the /llc ledger, the radar
# clusters and the family clustering. What is left below reads dates into email
# copy and into per-building summaries, where a filer typo is visible but not
# load-bearing.
#
# This list may shrink and must never grow. A new unguarded query in one of
# these files still passes, which is the cost of introducing a lint to a
# codebase that predates it; a new unguarded query anywhere else fails, which is
# the point. Clearing a file means deleting its line, not raising its count.
KNOWN_UNGUARDED = {
    "api/routes/frontend.py":            12,
    "scripts/weekly_digest.py":           4,
    "scripts/weekly_eviction_flips.py":   1,
}

SEARCH_DIRS = ("api", "scripts", "scoring", "scrapers", "scheduler")

_MAX = re.compile(r"\bmax\s*\(\s*([\w.]*(?:" + "|".join(DATED_COLUMNS) + r"))\s*\)",
                  re.IGNORECASE)


def _sql_files():
    for d in SEARCH_DIRS:
        for path in sorted((REPO / d).rglob("*.py")):
            rel = path.relative_to(REPO).as_posix()
            if rel in EXEMPT or "__pycache__" in rel:
                continue
            yield rel, path.read_text()


def _guarded(block: str, column: str) -> bool:
    """Does the statement around this max() carry the rule?

    Either form counts: a real_date() call, or the predicate written out. The
    bare column name is what matters, since call sites alias their tables.
    """
    bare = column.split(".")[-1]
    if "real_date(" in block:
        return True
    has_future_bound = re.search(
        rf"{re.escape(bare)}\s*(<\s*CURRENT_DATE|<=\s*CURRENT_DATE)", block, re.I)
    has_ingest_bound = re.search(
        rf"{re.escape(bare)}\s*<=\s*[\w.]*created_at", block, re.I)
    return bool(has_future_bound and has_ingest_bound)


def _unguarded_by_file() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for rel, src in _sql_files():
        lines = src.splitlines()
        for i, line in enumerate(lines):
            m = _MAX.search(line)
            if not m:
                continue
            # The surrounding statement, generously bounded: these are
            # multi-line SQL strings and the WHERE clause sits below the SELECT.
            block = "\n".join(lines[max(0, i - 12):i + 25])
            if not _guarded(block, m.group(1)):
                found.setdefault(rel, []).append(f"{rel}:{i + 1}  max({m.group(1)})")
    return found


def test_no_new_file_takes_an_unguarded_max():
    """A file not on the list has no excuse."""
    found = _unguarded_by_file()
    fresh = sorted(f for f in found if f not in KNOWN_UNGUARDED)
    assert not fresh, (
        "max() over a record date without the happened-yet rule, in a file that "
        "had none. Use api.freshness.real_date(column, created_at) in the WHERE "
        "clause; a filer-typed future date otherwise becomes this query's "
        "answer:\n  "
        + "\n  ".join(l for f in fresh for l in found[f])
    )


def test_the_known_unguarded_list_only_shrinks():
    """The backlog is allowed to be a backlog. It is not allowed to grow, and a
    file that has been cleared has to leave the list rather than sit at zero."""
    found = _unguarded_by_file()
    grew, cleared = [], []
    for rel, allowed in KNOWN_UNGUARDED.items():
        n = len(found.get(rel, []))
        if n > allowed:
            grew.append(f"{rel}: {n} unguarded, {allowed} allowed")
        elif n == 0:
            cleared.append(rel)
    assert not grew, "new unguarded max() in a file with a known backlog:\n  " + "\n  ".join(grew)
    assert not cleared, (
        "these files are clean now. Delete their KNOWN_UNGUARDED lines so the "
        "next regression in them fails:\n  " + "\n  ".join(cleared)
    )


def test_real_date_states_both_halves_of_the_rule():
    """The future bound alone expires when the calendar reaches the bad row.
    The ingest bound alone would miss a row backdated before its own insert."""
    from api import freshness

    sql = freshness.real_date("doc_date")
    assert "CURRENT_DATE" in sql, "real_date lost the future bound"
    assert "<= created_at" in sql, "real_date lost the ingest bound"


def test_real_date_honours_caller_aliases():
    """Call sites join several tables, so a hardcoded created_at would either
    fail to compile or silently bind the wrong table's column."""
    from api import freshness

    sql = freshness.real_date("o.doc_date", "o.created_at")
    assert "o.doc_date" in sql and "o.created_at" in sql
