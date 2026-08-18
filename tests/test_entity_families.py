"""
Guards for entity families and their /network/{slug} hubs.

The risk here is not a thin page, it is a false claim. Grouping companies that
share a filing address would put APPLEBAUM SPENCER, CHEN DOROTHY and GOPSTEIN
SHELDON, who share an attorney at 525 6th Avenue, under one landlord who does
not exist. So the clustering needs two independent signals to agree, and these
tests exist mostly to keep that rule from being loosened by accident.

The SEO plan predicted 49 hubs from numbered name stems alone. Measured against
the live record, name stems yield 21 groups covering 108 entities, shared
addresses yield 193 groups of which 139 fail corroboration, and what survives
both is 8 families. Fewer pages, each one defensible.
"""

import re
import warnings

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.entity_families import _addr_key, _stem, _tokens, compute_families
from api.main import app
from api.routes.frontend import _is_buyer_entity
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)

MIN_WORDS = 420
MAX_OVERLAP = 0.70
SHINGLE = 5

_CHROME = re.compile(r"<(script|style|nav|footer|head)\b.*?</\1>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")
_WORD = re.compile(r"[A-Za-z][A-Za-z'-]+")
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9'$%,.-]*")


def _families():
    db = SessionLocal()
    try:
        return compute_families(db, _is_buyer_entity)
    finally:
        db.close()


FAMS = _families()


def _text(html: str) -> str:
    return " ".join(_TAG.sub(" ", _CHROME.sub(" ", html.split("<body", 1)[-1])).split())


def _shingles(html: str) -> set:
    toks = [t.lower() for t in _TOKEN.findall(_text(html))]
    return {tuple(toks[i:i + SHINGLE]) for i in range(max(len(toks) - SHINGLE + 1, 0))}


class TestClusteringIsConservative:
    def test_families_exist_at_all(self):
        assert FAMS, "clustering produced nothing; the two-signal rule is too tight"

    def test_no_family_groups_a_bank_or_a_person(self):
        """'BANK' as a shared token merged FLAGSTAR BANK FSB with US BANK
        NATIONAL ASSOCIATION. Members must pass the buyer-entity gate."""
        offenders = []
        for f in FAMS.values():
            for n in f["entities"]:
                if not _is_buyer_entity(n) or "LLC" not in n:
                    offenders.append(f"{f['slug']}: {n}")
        assert not offenders, "non-buyer entities in a family:\n  " + "\n  ".join(offenders)

    def test_uncorroborated_address_groups_are_rejected(self):
        """Entities sharing only an address, with no shared distinctive token,
        must not end up in the same family. 525 6th Avenue is the case: an
        attorney's office filing for unrelated people."""
        db = SessionLocal()
        try:
            rows = db.execute(text("""
                SELECT max(party_addr_1) AS addr, max(party_zip) AS pzip,
                       party_name_normalized AS name
                FROM ownership_raw
                WHERE doc_type = 'DEED' AND party_type = '2'
                  AND party_addr_1 IS NOT NULL
                GROUP BY 3
            """)).fetchall()
        finally:
            db.close()
        by_addr: dict = {}
        for r in rows:
            by_addr.setdefault((r.addr.strip().upper(), r.pzip), []).append(r.name)

        member_family = {n: f["slug"] for f in FAMS.values() for n in f["entities"]}
        bad = []
        for addr, names in by_addr.items():
            if len(names) < 3:
                continue
            # No token shared by even two of them means nothing corroborates.
            counts: dict = {}
            for n in names:
                for t in _tokens(n):
                    counts[t] = counts.get(t, 0) + 1
            if counts and max(counts.values()) >= 2:
                continue
            fams_here = {member_family[n] for n in names if n in member_family}
            if len(fams_here) == 1 and len(
                [n for n in names if n in member_family]
            ) >= 3:
                bad.append(f"{addr[0]}: {sorted(fams_here)}")
        assert not bad, (
            "entities grouped on a shared address with nothing corroborating "
            "it:\n  " + "\n  ".join(bad)
        )

    def test_filing_address_variants_collapse_to_one_key(self):
        """Four spellings of the Summit management address split a group of 46
        into 42, 3 and two singletons, and the two singletons fell out of the
        family entirely."""
        variants = [
            "C/O: SUMMIT MALLS MANAGEMENT, LLC",
            "C/O: SUMMIT MALLS MANAGEMENT LLC",
            "C/O: SUMMIT MALLS MANAGEMENT , LLC",
            "C/O SUMMIT MALLS MANAGEMENT LLC",
            "c/o summit malls management, llc.",
        ]
        keys = {_addr_key(v) for v in variants}
        assert len(keys) == 1, f"filing address variants did not collapse: {keys}"
        assert _addr_key("525 6TH AVENUE") != _addr_key("520 FIFTH AVENUE")

    def test_no_entity_is_stranded_from_its_own_family(self):
        """FLGSP 2400 NOSTRAND AVE LLC filed from "SUMMIT MALL MANAGEMENT" where
        its 81 siblings filed from "SUMMIT MALLS MANAGEMENT", so it sat outside
        the family and /network/flgsp rendered two buildings and $15.4M short.

        An entity carrying a family's own coined label, filing from a ZIP that
        family already files from, belongs to it."""
        db = SessionLocal()
        try:
            rows = db.execute(text("""
                SELECT party_name_normalized AS name, max(party_zip) AS pzip
                FROM ownership_raw
                WHERE doc_type = 'DEED' AND party_type IN ('1', '2')
                  AND party_name_normalized IS NOT NULL
                GROUP BY 1
            """)).fetchall()
        finally:
            db.close()
        member_of = {n: f["slug"] for f in FAMS.values() for n in f["entities"]}
        stranded = []
        for f in FAMS.values():
            label = f["label"]
            if " " in label or len(label) < 4:
                continue
            zips = {z for _, z in f["addresses"]}
            for r in rows:
                if r.name in member_of or not r.pzip or r.pzip not in zips:
                    continue
                if "LLC" not in (r.name or "") or not _is_buyer_entity(r.name):
                    continue
                if label in _tokens(r.name):
                    stranded.append(f"{f['slug']}: {r.name}")
        assert not stranded, (
            "entities carrying a family's coined label, filing from its ZIP, "
            "left outside it:\n  " + "\n  ".join(stranded)
        )

    def test_adopted_entities_still_share_the_family_name(self):
        """Adoption must not become a third way in for an unrelated company:
        every member has to share a token or a stem with the family label."""
        loose = []
        for f in FAMS.values():
            label_tokens = set(f["label"].split())
            for n in f["entities"]:
                if label_tokens & _tokens(n) or f["label"] in _stem(n):
                    continue
                loose.append(f"{f['slug']}: {n}")
        assert not loose, (
            "family members sharing nothing with the family name:\n  "
            + "\n  ".join(loose)
        )

    def test_every_family_clears_the_building_floor(self):
        """Qualifies on whichever side is larger: a family that has sold its
        whole portfolio held those buildings, and holds none now."""
        thin = [
            f"{f['slug']}: held {f['buildings']}, sold {f.get('sold', 0)}, "
            f"{len(f['entities'])} entities"
            for f in FAMS.values()
            if max(f["buildings"], f.get("sold", 0)) < 5 or len(f["entities"]) < 3
        ]
        assert not thin, "families under the floor:\n  " + "\n  ".join(thin)

    def test_condo_units_do_not_inflate_a_family(self):
        """250 EAST 25TH ST UNIT 10F / 1A / ... are units of one building.
        Counting them as five buildings is the bug this guards."""
        for f in FAMS.values():
            db = SessionLocal()
            try:
                real = db.execute(text("""
                    SELECT count(DISTINCT (substring(bbl, 1, 6) ||
                           CASE WHEN substring(bbl, 7, 4) >= '1001'
                                THEN '0000' ELSE substring(bbl, 7, 4) END))
                    FROM ownership_raw
                    WHERE doc_type = 'DEED' AND party_type = '2'
                      AND party_name_normalized = ANY(:names)
                """), {"names": f["entities"]}).scalar()
            finally:
                db.close()
            assert f["buildings"] == real, (
                f"{f['slug']} claims {f['buildings']} buildings, record has {real}"
            )

    def test_single_word_labels_are_names_not_common_words(self):
        """'FIRST', 'BEACH' and 'ARM' each merged three unrelated owners on a
        common noun. A one-word label must be nearly unique to its family."""
        db = SessionLocal()
        try:
            rows = db.execute(text("""
                SELECT party_name_normalized AS name FROM ownership_raw
                WHERE doc_type = 'DEED' AND party_type = '2'
                  AND party_name_normalized IS NOT NULL
                GROUP BY 1
            """)).fetchall()
        finally:
            db.close()
        freq: dict = {}
        for r in rows:
            if "LLC" in (r.name or "") and _is_buyer_entity(r.name):
                for t in _tokens(r.name):
                    freq[t] = freq.get(t, 0) + 1
        loose = []
        for f in FAMS.values():
            if " " in f["label"]:
                continue
            citywide = freq.get(f["label"].upper(), 0)
            if citywide > len(f["entities"]) * 2:
                loose.append(f"{f['label']}: {citywide} entities citywide, "
                             f"family has {len(f['entities'])}")
        assert not loose, "generic one-word family labels:\n  " + "\n  ".join(loose)

    def test_stem_strips_the_serial_not_the_name(self):
        assert _stem("PHANTOM CAPITAL 14 LLC") == "PHANTOM CAPITAL"
        assert _stem("TOWNHOUSE RENTAL II LLC") == "TOWNHOUSE RENTAL"
        assert _stem("NORWORTH HOLDINGS LLC") == "NORWORTH HOLDINGS"


class TestFamilyHubs:
    def _live(self):
        return [(s, f) for s, f in FAMS.items()
                if client.get(f"/network/{s}", follow_redirects=False).status_code == 200]

    def test_hubs_render_and_are_substantial(self):
        live = self._live()
        assert live, "no family hub renders"
        thin = []
        for slug, _ in live:
            n = len(_WORD.findall(_text(client.get(f"/network/{slug}").text)))
            if n < MIN_WORDS:
                thin.append(f"/network/{slug}: {n} words")
        assert not thin, "thin family hubs:\n  " + "\n  ".join(thin)

    def test_hubs_are_not_near_duplicates(self):
        live = self._live()
        if len(live) < 2:
            pytest.skip("need two live hubs")
        grams = {s: _shingles(client.get(f"/network/{s}").text) for s, _ in live}
        slugs = list(grams)
        dupes = []
        for i, a in enumerate(slugs):
            for b in slugs[i + 1:]:
                o = len(grams[a] & grams[b]) / min(len(grams[a]), len(grams[b]))
                if o >= MAX_OVERLAP:
                    dupes.append(f"{a} vs {b}: {o:.0%}")
        assert not dupes, (
            f"family hubs are near-duplicates (limit {MAX_OVERLAP:.0%}):\n  "
            + "\n  ".join(dupes)
        )

    def test_hub_links_every_one_of_its_entities(self):
        slug, fam = self._live()[0]
        html = client.get(f"/network/{slug}").text
        missing = []
        for n in fam["entities"]:
            ent_slug = re.sub(r"[^a-z0-9]+", "-", n.lower()).strip("-")
            if f'/llc/{ent_slug}"' not in html:
                missing.append(n)
        assert not missing, (
            f"/network/{slug} omits {len(missing)} of its own entities, which is "
            f"the internal linking the hub exists to provide"
        )

    def test_entity_pages_link_back_to_their_family(self):
        slug, fam = self._live()[0]
        name = fam["entities"][0]
        ent_slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        html = client.get(f"/llc/{ent_slug}").text
        assert f'/network/{slug}' in html, \
            f"/llc/{ent_slug} does not link to its family"

    def test_curated_operators_are_not_duplicated_by_a_hub(self):
        """PHANTOM already has /operator/phantom-capital. Two pages about one
        operation would compete with each other."""
        db = SessionLocal()
        try:
            curated = {r.name for r in db.execute(text(
                "SELECT jsonb_array_elements_text(llc_entities) AS name FROM operators "
                "WHERE operator_class = 'operator' AND llc_entities IS NOT NULL"
            )).fetchall()}
        finally:
            db.close()
        for slug, fam in FAMS.items():
            if set(fam["entities"]) & curated:
                resp = client.get(f"/network/{slug}", follow_redirects=False)
                assert resp.status_code == 301, (
                    f"/network/{slug} overlaps a curated operator but returns "
                    f"{resp.status_code} instead of redirecting to it"
                )

    def test_unknown_family_is_404(self):
        assert client.get("/network/nosuchfamily").status_code == 404


class TestSellerSideFamilies:
    """Clustering buyers only made half of every portfolio trade invisible.
    The 82-building sale of 2026-03-31 is the case: the buyers were 82 FLGSP
    companies and the sellers were 1023/1038/1042 REALTY LLC and siblings,
    which is just as much a family. A family that has sold everything still
    held those buildings, and the exit is usually the more interesting half."""

    def _seller_only(self):
        return [(s, f) for s, f in FAMS.items()
                if f.get("sold", 0) and not f["buildings"]]

    def test_a_family_may_qualify_on_the_sold_side(self):
        assert any(f.get("sold", 0) for f in FAMS.values()), \
            "no family has a sold side; clustering is buyer-only again"

    def test_seller_only_families_do_not_claim_zero_buildings(self):
        """The page said 'holding 0 buildings between them' and titled itself
        '0 NYC buildings', which reads as a broken page rather than a portfolio
        that has been sold on."""
        broken = []
        for slug, fam in self._seller_only():
            resp = client.get(f"/network/{slug}", follow_redirects=False)
            if resp.status_code != 200:
                continue
            body = _text(resp.text)
            title = re.search(r"<title>(.*?)</title>", resp.text)
            if re.search(r"(?<![1-9])0 buildings", body):
                broken.append(f"{slug}: body says 0 buildings")
            if title and re.search(r"(?<![1-9])0 NYC buildings", title.group(1)):
                broken.append(f"{slug}: title says 0 NYC buildings")
        assert not broken, "\n  ".join(broken)

    def test_seller_only_families_still_link_their_buildings(self):
        """The holdings query filtered to party_type='2', so a family that had
        sold everything rendered with no building links at all."""
        for slug, fam in self._seller_only():
            resp = client.get(f"/network/{slug}", follow_redirects=False)
            if resp.status_code != 200:
                continue
            links = set(re.findall(r'href="/property/\d+"', resp.text))
            assert links, f"/network/{slug} sold {fam['sold']} buildings and links to none"

    def test_entity_wording_matches_the_side(self):
        for slug, fam in self._seller_only():
            resp = client.get(f"/network/{slug}", follow_redirects=False)
            if resp.status_code != 200:
                continue
            body = _text(resp.text)
            assert "appears as a buyer of record" not in body, \
                f"/network/{slug} calls its sellers buyers"
