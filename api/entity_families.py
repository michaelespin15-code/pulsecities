"""
Entity families: the buying entities the deed record links to each other.

NYC property is held one building at a time, each in its own LLC. That is
ordinary practice, and the side effect is that one operation appears in the
public record as thirty unrelated strangers. Reassembling them is the point.

**Why two signals and not one.** Clustering on a shared filing address alone
looks tempting and is wrong: 525 6th Avenue files deeds for APPLEBAUM SPENCER,
CHEN DOROTHY and GOPSTEIN SHELDON, and 520 Fifth Avenue for 26 entities with
nothing in common. Those are attorneys and title companies, not landlords.
Measured on the live record, 139 of 193 shared-address groups fail
corroboration and are dropped here.

So an edge needs two independent things to agree:

    A. a shared naming stem across numbered siblings
       PHANTOM CAPITAL 14 / 16 / 25 is one operation by construction
    B. a shared filing address AND a distinctive token shared by at least half
       the entities filing from it
       FLGSP 11 HILLSIDE AVE LLC and FLGSP 1023 CARROLL ST LLC, both filed
       through a Summit management address

Groups whose dominant token matches are then merged across addresses, which is
what reunites FLGSP's 42 entities at one management address with its 35 at
another.

Members must be LLC-form buyers, so every one has a /llc/{slug} page for the
hub to link to, and the existing `_is_buyer_entity` gate keeps out servicers,
trustees, referees and the banks that would otherwise cluster on the token
"BANK". Families need three entities across three distinct buildings, which
drops a condominium whose unit deeds look like a portfolio.

Nothing here asserts common ownership. It reports what the filings share.
"""

from __future__ import annotations

import collections
import re

from sqlalchemy import text

# Tokens that carry no identity: entity forms, street types, the boroughs, and
# the generic real-estate vocabulary every third LLC in the city uses. Without
# this list "REALTY" alone would merge hundreds of unrelated companies.
_STOP = set("""
THE OF AND FOR NEW YORK NYC LLC PLLC LLP LP INC CORP CO LTD FSB NA
AVE AVENUE STREET ST ROAD RD BLVD BOULEVARD PLACE PL DRIVE DR COURT CT
LANE LN PARKWAY PKWY TERRACE TER HIGHWAY HWY SQUARE SQ WAY CIRCLE UNIT APT
REALTY PROPERTY PROPERTIES HOLDINGS HOLDING GROUP MANAGEMENT ASSOCIATES
PARTNERS VENTURES CAPITAL EQUITIES EQUITY ESTATE REAL DEVELOPMENT BUILDERS
INVESTMENTS INVESTMENT ENTERPRISES ACQUISITIONS PORTFOLIO
BROOKLYN QUEENS BRONX MANHATTAN STATEN ISLAND NORTH SOUTH EAST WEST
TRUST IRREVOCABLE OWNER APARTMENTS BUILDING HOUSE HOMES RESIDENCES
RENTAL RENTALS LEASING MANAGEMENT REALTY LAND
BANK NATIONAL ASSOCIATION FEDERAL SAVINGS MORTGAGE
""".split())

_FORM_RE = re.compile(r"\b(LLC|PLLC|LLP|LP|INC|CORP|CO|LTD)\b")
_TRAILING_SERIAL_RE = re.compile(r"\b(?:[IVX]+|\d+[A-Z]?)\b\s*$")
_MIN_ENTITIES = 3
# Five, not three. A three-entity family renders about 430 words, most of them
# the shared explanation of how the clustering works, and measured 75-79%
# 5-gram overlap against its siblings: above /neighborhood's 68-69%, which is
# the line for a template that indexes cleanly. The families that clear five
# buildings have enough of their own record to carry a page.
_MIN_BUILDINGS = 5

# A condo records one deed per unit; its unit lots (1001+) collapse to the
# block they share. Same rule as the LLC page's building count.
_BUILDING_KEY_SQL = (
    "substring(bbl, 1, 6) || CASE WHEN substring(bbl, 7, 4) >= '1001' "
    "THEN '0000' ELSE substring(bbl, 7, 4) END"
)


def _tokens(name: str) -> set[str]:
    return {
        w for w in re.sub(r"[^A-Z0-9 ]", " ", name.upper()).split()
        if w not in _STOP and not w.isdigit() and len(w) > 2
    }


def _stem(name: str) -> str:
    """The name with its entity form and trailing serial removed, so
    PHANTOM CAPITAL 14 LLC and PHANTOM CAPITAL 25 LLC land on one key."""
    t = _FORM_RE.sub("", name).strip()
    t = _TRAILING_SERIAL_RE.sub("", t).strip()
    return re.sub(r"\s+", " ", t).strip(" -,&")


def family_slug(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")


class _Union:
    def __init__(self, items):
        self.parent = {i: i for i in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def compute_families(db, is_buyer_entity) -> dict[str, dict]:
    """Returns {slug: family}. `is_buyer_entity` is injected rather than
    imported so this module stays independent of the route layer."""
    rows = db.execute(text(f"""
        SELECT party_name_normalized AS name,
               max(party_addr_1) AS addr,
               max(party_zip) AS pzip,
               count(DISTINCT bbl) AS lots,
               array_agg(DISTINCT ({_BUILDING_KEY_SQL})) AS building_keys,
               max(doc_date) AS last_deed,
               sum(doc_amount) FILTER (WHERE doc_amount > 0) AS volume
        FROM ownership_raw
        WHERE doc_type = 'DEED' AND party_type = '2'
          AND party_name_normalized IS NOT NULL
        GROUP BY 1
    """)).fetchall()

    # LLC-form buyers only: every member must have a /llc/ page to link to.
    members = [r for r in rows if "LLC" in (r.name or "") and is_buyer_entity(r.name)]
    if not members:
        return {}
    info = {r.name: r for r in members}
    uf = _Union(info)

    # Signal A: numbered siblings under one stem.
    by_stem: dict[str, list[str]] = collections.defaultdict(list)
    for r in members:
        st = _stem(r.name)
        if len(st) >= 6 and " " in st:
            by_stem[st].append(r.name)
    for names in by_stem.values():
        if len(names) >= _MIN_ENTITIES:
            for n in names[1:]:
                uf.union(names[0], n)

    # Signal B: a shared filing address, corroborated by a distinctive token
    # that at least half the entities at that address carry.
    by_addr: dict[tuple, list] = collections.defaultdict(list)
    for r in members:
        if r.addr and r.pzip:
            by_addr[(r.addr.strip().upper(), r.pzip)].append(r)

    token_groups: dict[str, list[str]] = collections.defaultdict(list)
    for group in by_addr.values():
        if len(group) < _MIN_ENTITIES:
            continue
        counts: collections.Counter = collections.Counter()
        for r in group:
            counts.update(_tokens(r.name))
        threshold = max(_MIN_ENTITIES, len(group) * 0.5)
        for tok, c in counts.items():
            if c < threshold:
                continue
            names = [r.name for r in group if tok in _tokens(r.name)]
            if len(names) < _MIN_ENTITIES:
                continue
            for n in names[1:]:
                uf.union(names[0], n)
            token_groups[tok].append(names[0])

    # One operation can file through more than one management address. Where
    # two corroborated groups share a dominant token, they are the same family.
    for anchors in token_groups.values():
        for a in anchors[1:]:
            uf.union(anchors[0], a)

    # How common each token is across every buyer entity, for the
    # single-word-label test below.
    token_frequency: collections.Counter = collections.Counter()
    for r in members:
        token_frequency.update(_tokens(r.name))

    comps: dict[str, list[str]] = collections.defaultdict(list)
    for name in info:
        comps[uf.find(name)].append(name)

    families: dict[str, dict] = {}
    for names in comps.values():
        if len(names) < _MIN_ENTITIES:
            continue
        keys = {k for n in names for k in (info[n].building_keys or [])}
        buildings = len(keys)
        if buildings < _MIN_BUILDINGS:
            continue

        # Label with the name a reader would actually use. The shared stem
        # comes first: REGO PARK PORTFOLIO I/II/III is "REGO PARK PORTFOLIO",
        # not "PARK", which is what picking the shared token alone produced.
        stems = collections.Counter(_stem(n) for n in names)
        top_stem, top_count = stems.most_common(1)[0]
        label = ""
        if top_count == len(names) and len(top_stem) >= 4:
            label = top_stem
        else:
            counts: collections.Counter = collections.Counter()
            for n in names:
                counts.update(_tokens(n))
            shared = [t for t, c in counts.items() if c == len(names)]
            if shared:
                label = max(shared, key=len)
            elif len(top_stem) >= 4:
                label = top_stem
        label = label.strip()
        if not label:
            continue

        slug = family_slug(label)
        if not slug or len(slug) < 2:
            continue

        # A one-word label has to be a name, not a word. "FIRST", "BEACH" and
        # "ARM" each merged three unrelated owners who happened to share a
        # common noun; "LBUZ" and "FLGSP" are coined and belong to one
        # operation. The test is data-driven rather than a word list: count how
        # many buyer entities citywide carry the token, and require that the
        # family accounts for most of them.
        if " " not in label:
            citywide = token_frequency.get(label, 0)
            if citywide > len(names) * 2:
                continue
        # Two unrelated families cannot share a slug; keep the larger.
        if slug in families and families[slug]["buildings"] >= buildings:
            continue

        addrs = sorted({
            (info[n].addr.strip(), info[n].pzip)
            for n in names if info[n].addr and info[n].pzip
        })
        dates = [info[n].last_deed for n in names if info[n].last_deed]
        families[slug] = {
            "slug": slug,
            "label": label,
            "entities": sorted(names),
            "buildings": buildings,
            "lots": sum(int(info[n].lots or 0) for n in names),
            "volume": float(sum(info[n].volume or 0 for n in names)),
            "addresses": addrs,
            "last_deed": max(dates) if dates else None,
        }
    return families
