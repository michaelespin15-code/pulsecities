"""
Guards the "177 NYC ZIP codes" claim wherever the site makes it.

The social card shipped "all 178 NYC ZIP codes" for months. 178 was never a
stale figure, it was the wrong query: the neighborhoods table carries a 99999
sentinel row for records whose ZIP could not be matched, and counting rows
counts the sentinel. Everything that mattered already filtered it -- the
directory, the sitemap, /neighborhood/99999 returns 404, oca_ingest.py has a
comment about it -- so the miscount survived in the two places nothing queries:
a hand-made PNG and a 404 message in briefs.py.

The number appears in a dozen strings across page copy, meta descriptions,
JSON-LD and both languages, and the boundary set really does change when the
city republishes MODZCTA. So rather than trusting a dozen hand-edits to stay in
step, this reads the count from the database and holds every claim to it, using
the same helper the card generator uses so there is one definition of the
figure and not two that can disagree.
"""

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# Number, then the noun it is counting. Anchored on the noun so unrelated
# integers (timeouts, cache ages, setSelectionRange(0, 99999)) cannot match.
CLAIM = re.compile(
    r"\b(\d{2,4})\s+"
    r"(?:scored\s+|residential\s+|crawlable\s+)?"
    r"(?:NYC\s+)?"
    r"(ZIP\s+codes?|ZIP\b|neighbou?rhoods?|neighbou?rhood\s+pages|c[oó]digos\s+postales)",
    re.IGNORECASE,
)

SOURCES = sorted(
    [p for p in (ROOT / "frontend").glob("*.html")]
    + [p for p in (ROOT / "api" / "routes").glob("*.py")]
    + [ROOT / "scripts" / "gen_llms_txt.py"]
)


@pytest.fixture(scope="module")
def expected():
    from scripts.generate_og_image import scored_zip_count
    return scored_zip_count()


@pytest.mark.integration
def test_scored_zip_count_excludes_the_sentinel(expected):
    """99999 is an unmatched-ZIP placeholder, not a neighbourhood."""
    from models.database import SessionLocal
    from sqlalchemy import text

    db = SessionLocal()
    try:
        rows = db.execute(text("SELECT count(*) FROM neighborhoods")).scalar()
    finally:
        db.close()
    assert expected < rows, (
        "the scored count equals the raw neighborhoods row count, so the 99999 "
        "sentinel is being counted again"
    )


@pytest.mark.integration
def test_no_page_claims_the_wrong_zip_count(expected):
    wrong = []
    for path in SOURCES:
        text_ = path.read_text(encoding="utf-8", errors="replace")
        for m in CLAIM.finditer(text_):
            claimed = int(m.group(1))
            # Small numbers here are counting something else ("6 signals",
            # "3 ZIP codes in this cluster"); only citywide totals are in range.
            if claimed < 100:
                continue
            if claimed != expected:
                line = text_[: m.start()].count("\n") + 1
                wrong.append(
                    f"{path.relative_to(ROOT)}:{line} claims {claimed}, "
                    f"database says {expected}: {m.group(0)!r}"
                )
    assert not wrong, "\n".join(wrong)
