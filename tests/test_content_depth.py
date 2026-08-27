"""
Content-depth guards for the two mass templates.

/property (1,792 sitemapped) and /llc (122) are the highest-volume pages on the
site and were its thinnest: 100 and 84-210 visible words, with two LLC pages
sharing 92% of their vocabulary. Thin near-duplicates at that multiple are what
gets a zero-authority domain's URLs excluded rather than indexed, so depth and
distinctness are load-bearing here, not cosmetic.

The word floor comes from docs/seo/PLAN.md. The duplication measure does not:
the plan proposed unique-vocabulary overlap, and that metric turned out to be
unusable. Measured on this site, /neighborhood scores 92-97% on it, and
/neighborhood is the template the plan holds up as the good one. Any two pages
in one language on one subject share nearly all their word *types*; that is a
property of English, not of duplication.

Overlap here is therefore n-gram containment, which is what near-duplicate
detection actually runs on, over tokens that include digits, since on a records
site the dates and dollar figures are the content. Calibration, measured across
180 live pages: hand-written hubs 0-1%, deepened /property and /llc mean 49-50%
and max 63-66%, /neighborhood 68-69%. The ceiling sits at the level of the
template already indexing fine, so it fires on boilerplate padding rather than
on ordinary variation between two buildings.
"""

import json
import re
import warnings

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.main import app
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)

MIN_WORDS = 450
MAX_OVERLAP = 0.70
SHINGLE = 5

# Chrome is identical on every page by design; counting it would hide exactly
# the near-duplication these tests exist to catch.
_CHROME = re.compile(r"<(script|style|nav|footer|head)\b.*?</\1>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")
_ENTITY = re.compile(r"&[a-z]+;|&#\d+;")
_WORD = re.compile(r"[A-Za-z][A-Za-z'-]+")
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9'$%,.-]*")


def _body(html: str) -> str:
    body = html.split("<body", 1)[-1]
    return _ENTITY.sub(" ", _TAG.sub(" ", _CHROME.sub(" ", body)))


def _visible_words(html: str) -> list[str]:
    return _WORD.findall(_body(html))


def _shingles(html: str) -> set[tuple]:
    toks = [t.lower() for t in _TOKEN.findall(_body(html))]
    return {tuple(toks[i:i + SHINGLE]) for i in range(max(len(toks) - SHINGLE + 1, 0))}


def _overlap(a: set, b: set) -> float:
    if not a or not b:
        return 1.0
    return len(a & b) / min(len(a), len(b))


def _fetch(path: str) -> str:
    r = client.get(path)
    assert r.status_code == 200, f"{path} returned {r.status_code}"
    return r.text


def _ld_types(html: str) -> list[str]:
    """Every @type in every JSON-LD block, flattened through @graph."""
    found = []
    for block in re.findall(
        r'<script type="application/ld\+json">(.*?)</script>', html, re.S
    ):
        try:
            obj = json.loads(block)
        except json.JSONDecodeError:
            pytest.fail(f"invalid JSON-LD block: {block[:200]}")
        for node in obj.get("@graph", [obj]) if isinstance(obj, dict) else obj:
            if isinstance(node, dict) and node.get("@type"):
                found.append(node["@type"])
    return found


def _faq_questions(html: str) -> list[dict]:
    for block in re.findall(
        r'<script type="application/ld\+json">(.*?)</script>', html, re.S
    ):
        obj = json.loads(block)
        nodes = obj.get("@graph", [obj]) if isinstance(obj, dict) else obj
        for node in nodes:
            if isinstance(node, dict) and node.get("@type") == "FAQPage":
                return node.get("mainEntity", [])
    return []


# --- fixtures pulled from the live record ------------------------------------

def _sitemapped_properties(limit: int = 4) -> list[str]:
    """BBLs that clear the property sitemap gate (a deed and an eviction),
    each in a different ZIP so the pairing tests near-duplication across the
    template rather than across one neighbourhood's shared context."""
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT DISTINCT ON (p.zip_code) p.bbl
            FROM parcels p
            WHERE p.address IS NOT NULL AND p.zip_code IS NOT NULL
              AND EXISTS (SELECT 1 FROM ownership_raw o WHERE o.bbl = p.bbl)
              AND EXISTS (SELECT 1 FROM evictions_raw e WHERE e.bbl = p.bbl)
            ORDER BY p.zip_code, p.bbl
            LIMIT :n
        """), {"n": limit}).fetchall()
        return [r.bbl for r in rows]
    finally:
        db.close()


def _sitemapped_llcs(limit: int = 4) -> list[str]:
    """Slugs that clear the LLC sitemap gate, smallest first: the thin end of
    the template is where a word floor actually bites."""
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT btrim(regexp_replace(lower(party_name_normalized),
                                        '[^a-z0-9]+', '-', 'g'), '-') AS slug
            FROM ownership_raw
            WHERE doc_type = 'DEED' AND party_type = '2'
              AND party_name_normalized LIKE '%LLC%'
            GROUP BY 1, party_name_normalized
            HAVING count(DISTINCT bbl) >= 3
               AND count(DISTINCT substring(bbl, 1, 6)) >= 2
            ORDER BY count(DISTINCT bbl), 1
            LIMIT :n
        """), {"n": limit}).fetchall()
        return [r.slug for r in rows]
    finally:
        db.close()


# --- depth --------------------------------------------------------------------

class TestPropertyDepth:
    def test_property_pages_clear_the_word_floor(self):
        bbls = _sitemapped_properties()
        if not bbls:
            pytest.skip("no sitemap-eligible property in current data")
        thin = []
        for bbl in bbls:
            n = len(_visible_words(_fetch(f"/property/{bbl}")))
            if n < MIN_WORDS:
                thin.append(f"/property/{bbl}: {n} words")
        assert not thin, (
            f"property pages under the {MIN_WORDS}-word floor:\n  " + "\n  ".join(thin)
        )

    def test_property_pages_are_not_near_duplicates(self):
        bbls = _sitemapped_properties()
        if len(bbls) < 2:
            pytest.skip("need two sitemap-eligible properties")
        grams = {b: _shingles(_fetch(f"/property/{b}")) for b in bbls}
        dupes = []
        for i, a in enumerate(bbls):
            for b in bbls[i + 1:]:
                o = _overlap(grams[a], grams[b])
                if o >= MAX_OVERLAP:
                    dupes.append(f"{a} vs {b}: {o:.0%}")
        assert not dupes, (
            "property pages are near-duplicates of each other "
            f"(limit {MAX_OVERLAP:.0%}):\n  " + "\n  ".join(dupes)
        )

    def test_property_page_answers_questions_in_schema(self):
        bbls = _sitemapped_properties(1)
        if not bbls:
            pytest.skip("no sitemap-eligible property in current data")
        html = _fetch(f"/property/{bbls[0]}")
        assert "FAQPage" in _ld_types(html), "property template declares no FAQPage"
        questions = _faq_questions(html)
        assert len(questions) >= 3, f"only {len(questions)} FAQ entries"
        for q in questions:
            assert q.get("name"), "FAQ question with no name"
            answer = (q.get("acceptedAnswer") or {}).get("text", "")
            assert len(answer.split()) >= 8, f"stub answer for {q.get('name')!r}"


class TestLlcDepth:
    def test_llc_pages_clear_the_word_floor(self):
        slugs = _sitemapped_llcs()
        if not slugs:
            pytest.skip("no sitemap-eligible LLC in current data")
        thin = []
        for slug in slugs:
            n = len(_visible_words(_fetch(f"/llc/{slug}")))
            if n < MIN_WORDS:
                thin.append(f"/llc/{slug}: {n} words")
        assert not thin, (
            f"LLC pages under the {MIN_WORDS}-word floor:\n  " + "\n  ".join(thin)
        )

    def test_llc_pages_are_not_near_duplicates(self):
        slugs = _sitemapped_llcs()
        if len(slugs) < 2:
            pytest.skip("need two sitemap-eligible LLCs")
        grams = {s: _shingles(_fetch(f"/llc/{s}")) for s in slugs}
        dupes = []
        for i, a in enumerate(slugs):
            for b in slugs[i + 1:]:
                o = _overlap(grams[a], grams[b])
                if o >= MAX_OVERLAP:
                    dupes.append(f"{a} vs {b}: {o:.0%}")
        assert not dupes, (
            "LLC pages are near-duplicates of each other "
            f"(limit {MAX_OVERLAP:.0%}):\n  " + "\n  ".join(dupes)
        )

    def test_llc_page_answers_questions_in_schema(self):
        slugs = _sitemapped_llcs(1)
        if not slugs:
            pytest.skip("no sitemap-eligible LLC in current data")
        html = _fetch(f"/llc/{slugs[0]}")
        assert "FAQPage" in _ld_types(html), "LLC template declares no FAQPage"
        questions = _faq_questions(html)
        assert len(questions) >= 3, f"only {len(questions)} FAQ entries"
        for q in questions:
            assert q.get("name"), "FAQ question with no name"
            answer = (q.get("acceptedAnswer") or {}).get("text", "")
            assert len(answer.split()) >= 8, f"stub answer for {q.get('name')!r}"

    def test_llc_page_speaks_the_deed_research_vocabulary(self):
        """The words the people who find this page actually search for.

        The 2026-08-27 console export put this template at position 5 to 9 on
        roughly 1,700 impressions of queries carrying grantor, grantee, chain of
        title, conveyance and ACRIS, and it converted none of them. The page
        contained none of those words: the title said "NYC property purchases,
        deed history". Ranking for a vocabulary the snippet never speaks is how
        a page-one result earns nothing, so the vocabulary is a fixture now.
        """
        slugs = _sitemapped_llcs(1)
        if not slugs:
            pytest.skip("no sitemap-eligible LLC in current data")
        html = _fetch(f"/llc/{slugs[0]}")
        body = html.lower()
        missing = [w for w in ("grantor", "grantee", "chain of title",
                               "conveyance", "acris", "document id",
                               "registered agent", "beneficial owner",
                               "managing member") if w not in body]
        assert not missing, (
            f"/llc/{slugs[0]} no longer uses the deed-research vocabulary it "
            f"ranks for: {missing}"
        )

    def test_llc_title_leads_with_the_deed_vocabulary(self):
        """The title is the snippet. It carried none of the query terms."""
        slugs = _sitemapped_llcs(1)
        if not slugs:
            pytest.skip("no sitemap-eligible LLC in current data")
        m = re.search(r"<title>(.*?)</title>", _fetch(f"/llc/{slugs[0]}"), re.S)
        assert m, "LLC page has no title"
        title = m.group(1).lower()
        assert "acris" in title and "grantee" in title, (
            f"LLC title does not name the record it serves: {m.group(1)!r}"
        )

    def test_llc_rows_cite_the_acris_document_id(self):
        """A deed row a reader cannot check against ACRIS is a claim, not a
        citation, and the document ID is also what they search."""
        slugs = _sitemapped_llcs(1)
        if not slugs:
            pytest.skip("no sitemap-eligible LLC in current data")
        html = _fetch(f"/llc/{slugs[0]}")
        assert re.search(r'class="rec-doc">ACRIS \d', html), (
            "LLC deed rows no longer print their ACRIS document ID"
        )

    def test_llc_directory_declares_its_list(self):
        # Every other directory page on the site declares ItemList; this one
        # lists 100 entities and declared only a breadcrumb.
        assert "ItemList" in _ld_types(_fetch("/llc"))


class TestInternalLinking:
    """With no external backlinks the internal graph is the only authority
    routing available, and both templates were dead ends upward."""

    def test_llc_page_links_to_the_neighbourhoods_it_buys_in(self):
        slugs = _sitemapped_llcs(1)
        if not slugs:
            pytest.skip("no sitemap-eligible LLC in current data")
        html = _fetch(f"/llc/{slugs[0]}")
        assert re.search(r'href="/neighborhood/\d{5}"', html), \
            "LLC page links to none of the ZIPs its buildings sit in"

    def test_property_page_links_out_to_records_and_context(self):
        bbls = _sitemapped_properties(1)
        if not bbls:
            pytest.skip("no sitemap-eligible property in current data")
        html = _fetch(f"/property/{bbls[0]}")
        assert re.search(r'href="/neighborhood/\d{5}"', html)
        assert 'href="/is-my-building-rent-stabilized"' in html
