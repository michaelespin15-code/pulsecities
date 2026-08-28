"""
The eviction signal counts executions, so nothing may call them filings.

`evictions_raw.executed_date` is the date a **city marshal carried out a
warrant**. A filing is the start of a housing court case and most filings never
reach an execution, so the two words describe different populations and the
smaller one is the one we hold.

This was fixed once, on 2026-08-28, across the panel label, the count line, the
lag note, the deterministic summary, the weekly brief, the og-image and both
digests. The guard written that day asserted on the /this-week body alone, so
seven surfaces kept the wrong word, every one of them reading executed_date:

    api/routes/frontend.py     the /property meta description, ~97,000 pages
    api/routes/briefs.py       an entity brief's heading and its body sentence
    api/routes/stats.py        the homepage stat chip's own description
    frontend/index.html        the JSON-LD signal list search engines read
    frontend/app.html          og:description and twitter:description
    frontend/operator.html     the flagged-properties note, and its i18n twin
    scripts/weekly_content_brief.py

Precision here is not pedantry. The pitch material turns on 93 of 94 evictions
predating a sale, and a claim about who did what to whom cannot rest on a word
that names the wrong event. Housing-court petitions are genuinely filings and
say so ("petitions filed"); this grep does not touch them.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SEARCH = (
    ("api", "*.py"),
    ("scripts", "*.py"),
    ("scoring", "*.py"),
    ("scheduler", "*.py"),
    ("frontend", "*.html"),
)

# "eviction filing", "eviction filings", "Eviction Filings" -- any casing, and
# tolerant of a line break between the two words.
_BANNED = re.compile(r"eviction\s+filings?\b", re.I)

# Python adjacent-string concatenation puts a quote, a newline and an f-prefix
# between two halves of one sentence. Collapsing those joins first is the whole
# reason this grep sees the /property description, where the source reads
# `"... deed transfers, eviction "` / `f"filings, and renovation permits ..."`.
_CONCAT = re.compile(r"[\"']\s*\n\s*[frbu]*[\"']", re.I)


def _join_concatenations(src: str) -> str:
    """
    Collapse the join but keep every newline, so reported line numbers still
    point at the real source line. Dropping the newlines with the quotes moved
    every offender in api/routes/frontend.py up by 281 lines.
    """
    return _CONCAT.sub(lambda m: "\n" * m.group(0).count("\n"), src)


def _offenders():
    """
    Whole-file, not line-by-line. The /property description is written as an
    adjacent-string concatenation that puts "eviction " and "filings," on
    separate source lines, and a per-line grep walks straight past the one
    surface that carries ~97,000 pages.
    """
    for d, glob in SEARCH:
        root = REPO / d
        if not root.exists():
            continue
        for path in sorted(root.rglob(glob)):
            if "__pycache__" in path.as_posix() or ".min." in path.name:
                continue
            src = _join_concatenations(path.read_text(errors="ignore"))
            for m in _BANNED.finditer(src):
                line_no = src.count("\n", 0, m.start()) + 1
                context = src[max(0, m.start() - 45):m.end() + 45].replace("\n", " ⏎ ")
                yield f"{path.relative_to(REPO)}:{line_no}", context.strip()


def test_nothing_calls_an_executed_eviction_a_filing():
    hits = list(_offenders())
    assert not hits, (
        "The eviction signal reads evictions_raw.executed_date, which is a marshal "
        "carrying out a warrant, not a case being filed:\n  "
        + "\n  ".join(f"{loc}  {txt}" for loc, txt in hits)
    )


def test_the_grep_would_catch_a_regression():
    """A guard that cannot fail is not a guard."""
    assert _BANNED.search("eviction filings, and renovation permits")
    assert _BANNED.search("Eviction Filings Before Acquisition")
    assert not _BANNED.search("housing court petitions filed in the ZIP")
    assert not _BANNED.search("executed residential evictions")
    # The concatenation shape that hid the biggest surface.
    joined = _join_concatenations('"deed transfers, eviction "\n            f"filings, and permits"')
    assert _BANNED.search(joined), "the concatenation shape that hid ~97,000 pages"
