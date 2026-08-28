"""
An upsert that refreshes a row must say it refreshed the row.

SQLAlchemy's `onupdate=utcnow` on TimestampMixin fires for an ORM UPDATE. It
does not reach `INSERT ... ON CONFLICT DO UPDATE`, whose SET list is exactly the
`set_` dict the caller writes, so leaving `updated_at` out of that dict freezes
the column at first insert while every other field keeps refreshing.

Found on 2026-08-28: `mappluto` had completed successfully on 2026-08-12 having
processed 858,602 records, and the newest `updated_at` in the 918k-row parcels
table read 2026-07-09. The parcel data was current and the column that says when
it was current was five weeks stale. Nothing user-facing read it, which is why it
sat there; it is the same false-freshness shape as the ACRIS doc_date bug, where
the cost was a wrong `<lastmod>` on 200 hub URLs for sixteen nights.

Two of four call sites had it right, which is what makes this a grep and not a
fix. A rule with more readers than enforcers drifts.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SEARCH_DIRS = ("scrapers", "scripts", "scoring", "api", "scheduler")

# Statements written as raw SQL already spell the column out or deliberately
# omit it, and this grep reads Python `set_={...}` blocks. Raw-SQL upserts are
# covered by reading them: scoring/compute.py and scripts/backfill_operators.py
# both assign updated_at in their ON CONFLICT clauses.
_UPSERT = re.compile(r"on_conflict_do_update\((.*?)\n\s*\)\s*\)?", re.S)


def _call_sites():
    for d in SEARCH_DIRS:
        for path in sorted((REPO / d).rglob("*.py")):
            if "__pycache__" in path.as_posix():
                continue
            src = path.read_text()
            if "on_conflict_do_update" not in src:
                continue
            for match in _UPSERT.finditer(src):
                yield path.relative_to(REPO).as_posix(), match.group(1)


class TestUpsertsRefreshTheirTimestamp:
    def test_every_do_update_sets_updated_at(self):
        offenders = [
            path for path, body in _call_sites()
            if "set_=" in body and "updated_at" not in body
        ]
        assert not offenders, (
            "these refresh a row without moving updated_at, so the column that "
            "says when the row was last correct will lie:\n  "
            + "\n  ".join(sorted(set(offenders)))
            + "\nAdd \"updated_at\": text(\"now()\") to the set_ dict."
        )

    def test_the_grep_actually_finds_the_known_call_sites(self):
        """A guard that matches nothing passes forever. These four are the ones
        that existed when it was written."""
        found = {path for path, _ in _call_sites()}
        for expected in ("scrapers/pluto.py", "scrapers/dof.py",
                         "scrapers/violations.py", "scripts/load_zcta.py"):
            assert expected in found, f"{expected} no longer matches the grep"

    def test_pluto_is_fixed(self):
        """The one that was actually wrong, pinned so it cannot regress."""
        src = (REPO / "scrapers" / "pluto.py").read_text()
        body = _UPSERT.search(src).group(1)
        assert "updated_at" in body
