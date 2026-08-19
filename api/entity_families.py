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
import time

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
_CARE_OF_RE = re.compile(r"^\s*C\s*[/.]?\s*O\b[\s:.,-]*")
# Compiled once: these run a few hundred thousand times per clustering pass.
_NON_ALNUM_RE = re.compile(r"[^A-Z0-9 ]")
_NON_DIGIT_RE = re.compile(r"[^0-9]")
_WS_RE = re.compile(r"\s+")
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


def _zip5(z: str | None) -> str:
    """ZIP+4 and the bare ZIP are the same address. 10,638 party rows carry the
    nine-digit form, and keying on it split "THE CARLYLE GROUP, 20004" from
    "THE CARLYLE GROUP, 200042505"."""
    digits = _NON_DIGIT_RE.sub("", z or "")
    return digits[:5] if len(digits) >= 5 else digits


def _addr_key(addr: str) -> str:
    """One filing address, one key. The same management company reaches ACRIS
    as "C/O: SUMMIT MALLS MANAGEMENT, LLC", "C/O: SUMMIT MALLS MANAGEMENT LLC"
    and "C/O: SUMMIT MALLS MANAGEMENT , LLC"; keyed on the raw string those are
    three addresses, and a group of 42 sheds the four entities that filed with
    a comma in a different place."""
    a = _CARE_OF_RE.sub("", addr.upper())
    a = _NON_ALNUM_RE.sub(" ", a)
    a = _FORM_RE.sub(" ", a)
    return _WS_RE.sub(" ", a).strip()


def _tokens(name: str) -> set[str]:
    return {
        w for w in _NON_ALNUM_RE.sub(" ", name.upper()).split()
        if w not in _STOP and not w.isdigit() and len(w) > 2
    }


def _stem(name: str) -> str:
    """The name with its entity form and trailing serial removed, so
    PHANTOM CAPITAL 14 LLC and PHANTOM CAPITAL 25 LLC land on one key."""
    t = _FORM_RE.sub("", name).strip()
    t = _TRAILING_SERIAL_RE.sub("", t).strip()
    return _WS_RE.sub(" ", t).strip(" -,&")


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
    # Both sides of the deed, not just the buyer.
    #
    # Clustering buyers only made half of every portfolio trade invisible. The
    # 82-building sale of 2026-03-31 is the case: the buyers were 82 companies
    # named FLGSP, and the sellers were 1023 REALTY LLC, 1038 REALTY LLC and
    # their siblings, which is just as much a family and was nowhere on the
    # site. A seller family is often the more interesting one, because it is a
    # portfolio being assembled or unwound rather than merely held.
    rows = db.execute(text(f"""
        SELECT party_name_normalized AS name,
               max(party_addr_1) AS addr,
               max(party_zip) AS pzip,
               count(DISTINCT bbl) FILTER (WHERE party_type = '2') AS lots,
               coalesce(array_agg(DISTINCT ({_BUILDING_KEY_SQL}))
                        FILTER (WHERE party_type = '2'), '{{}}') AS building_keys,
               coalesce(array_agg(DISTINCT ({_BUILDING_KEY_SQL}))
                        FILTER (WHERE party_type = '1'), '{{}}') AS sold_keys,
               max(doc_date) AS last_deed,
               sum(doc_amount) FILTER (WHERE doc_amount > 0 AND party_type = '2') AS volume
        FROM ownership_raw
        WHERE doc_type = 'DEED'
          AND party_type IN ('1', '2')
          AND party_name_normalized IS NOT NULL
          -- The LLC-form gate below is the same substring test, and applying it
          -- here instead cuts the group from 116,261 party names to 23,444.
          -- Uncached, the FLGSP hub took 12s to render and six of those were
          -- this query and the token work over the rows it did not need.
          AND party_name_normalized LIKE '%LLC%'
        GROUP BY 1
    """)).fetchall()

    # LLC-form parties only: every member must have a /llc/ page to link to.
    # is_buyer_entity is the same gate either way; it excludes servicers,
    # trustees and referees, which are exactly the parties that appear on the
    # selling side of a foreclosure without being an owner in any real sense.
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
        if r.addr and _zip5(r.pzip):
            by_addr[(_addr_key(r.addr), _zip5(r.pzip))].append(r)

    # How common each token is across every buyer entity. Used twice: to decide
    # whether a token is coined enough to merge two addresses, and by the
    # single-word-label test below.
    token_frequency: collections.Counter = collections.Counter()
    for r in members:
        token_frequency.update(_tokens(r.name))

    token_groups: dict[str, list[str]] = collections.defaultdict(list)
    token_members: dict[str, set[str]] = collections.defaultdict(set)
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
            token_members[tok].update(names)

    # One operation can file through more than one management address. Where
    # two corroborated groups share a dominant token, they are the same family.
    #
    # The token has to be coined for that to hold. "PARK" corroborates fine
    # inside one address, where MARINE PARK 3001 and MARINE PARK 3004 file
    # together, but merging on it across addresses put MARINE PARK in Rockaway,
    # DSA on West 72nd and 1 PARK ROW in Grand Rapids under one landlord who
    # does not exist. So the cross-address step applies the same exclusivity
    # test the single-word label rule uses: most entities citywide carrying the
    # token have to be these ones.
    for tok, anchors in token_groups.items():
        if len(anchors) < 2:
            continue
        if token_frequency.get(tok, 0) > len(token_members[tok]) * 2:
            continue
        for a in anchors[1:]:
            uf.union(anchors[0], a)

    comps: dict[str, list[str]] = collections.defaultdict(list)
    for name in info:
        comps[uf.find(name)].append(name)

    # Signal C: a coined token that belongs to one group and to nothing else.
    #
    # FLGSP 2400 NOSTRAND AVE LLC filed from "C/O: SUMMIT MALL MANAGEMENT" and
    # its 81 siblings from "SUMMIT MALLS MANAGEMENT", so neither the stem nor
    # the address reached it and the largest portfolio trade in the record
    # rendered two buildings and $15.4M short. Adoption still needs two signals
    # to agree: the token has to be effectively exclusive to the group citywide,
    # which is the test the single-word label rule already applies, and the
    # orphan has to file from a ZIP the group already files from.
    grouped = {n for names in comps.values() if len(names) >= _MIN_ENTITIES for n in names}
    orphans = [r for r in members if r.name not in grouped]
    if orphans:
        # Memoised, because this loop is the whole cost of the function. Calling
        # _zip5 inside it ran the regex 1.2 million times and took 5.5 of the
        # 6 seconds the FLGSP hub spent rendering cold.
        zip_of = {r.name: _zip5(r.pzip) for r in members}
        tokens_of = {r.name: _tokens(r.name) for r in members}
        orphan_keys = [(r.name, zip_of[r.name], tokens_of[r.name])
                       for r in orphans if zip_of[r.name]]
        adopted = False
        for names in list(comps.values()):
            if len(names) < _MIN_ENTITIES:
                continue
            shared = set.intersection(*(tokens_of[n] for n in names))
            coined = {t for t in shared
                      if len(t) >= 4 and token_frequency.get(t, 0) <= len(names) * 2}
            if not coined:
                continue
            zips = {zip_of[n] for n in names if zip_of[n]}
            for name, pzip, toks in orphan_keys:
                if pzip in zips and coined & toks:
                    uf.union(names[0], name)
                    adopted = True
        if adopted:
            comps = collections.defaultdict(list)
            for name in info:
                comps[uf.find(name)].append(name)

    families: dict[str, dict] = {}
    for names in comps.values():
        if len(names) < _MIN_ENTITIES:
            continue
        keys = {k for n in names for k in (info[n].building_keys or [])}
        sold = {k for n in names for k in (info[n].sold_keys or [])}
        buildings = len(keys)
        # A family that has sold everything still held those buildings, and is
        # often the story. Qualify on whichever side is larger.
        if max(buildings, len(sold)) < _MIN_BUILDINGS:
            continue

        # Label with the name a reader would actually use. The shared stem
        # comes first: REGO PARK PORTFOLIO I/II/III is "REGO PARK PORTFOLIO",
        # not "PARK", which is what picking the shared token alone produced.
        stems = collections.Counter(_stem(n) for n in names)
        top_stem, top_count = stems.most_common(1)[0]
        counts: collections.Counter = collections.Counter()
        for n in names:
            counts.update(_tokens(n))
        shared = [t for t, c in counts.items() if c == len(names)]

        candidates = []
        if top_count == len(names) and len(top_stem) >= 4:
            candidates.append(top_stem)
        if shared:
            candidates.append(max(shared, key=len))
        # A stem most of the family shares, for when the only token every
        # member carries is a common word. TOWNHOUSE RENTAL II/V/VI/VII picked
        # up three siblings named BROOKLYN TOWNHOUSE PROPERTY OWNER, leaving
        # "TOWNHOUSE" as the one shared token; 25 companies citywide are called
        # something TOWNHOUSE, so labelling on it is wrong and dropping the
        # family loses a real one. The name seven of the eight share is right.
        if len(top_stem) >= 4 and top_count >= max(_MIN_ENTITIES, len(names) * 0.5):
            candidates.append(top_stem)

        # A one-word label has to be a name, not a word. "FIRST", "BEACH" and
        # "ARM" each merged three unrelated owners who happened to share a
        # common noun; "LBUZ" and "FLGSP" are coined and belong to one
        # operation. The test is data-driven rather than a word list: count how
        # many buyer entities citywide carry the token, and require that the
        # family accounts for most of them.
        label = ""
        for c in candidates:
            c = c.strip()
            if not c:
                continue
            if " " not in c and token_frequency.get(c, 0) > len(names) * 2:
                continue
            label = c
            break
        if not label:
            continue

        slug = family_slug(label)
        if not slug or len(slug) < 2:
            continue

        # Two unrelated families cannot share a slug; keep the larger.
        if slug in families and families[slug]["buildings"] >= buildings:
            continue

        addrs = sorted({
            (info[n].addr.strip(), _zip5(info[n].pzip))
            for n in names if info[n].addr and _zip5(info[n].pzip)
        })
        dates = [info[n].last_deed for n in names if info[n].last_deed]
        families[slug] = {
            "slug": slug,
            "label": label,
            "entities": sorted(names),
            "buildings": buildings,
            "sold": len(sold),
            "sold_keys": sold,
            "lots": sum(int(info[n].lots or 0) for n in names),
            "volume": float(sum(info[n].volume or 0 for n in names)),
            "addresses": addrs,
            "last_deed": max(dates) if dates else None,
        }

    _drop_internal_transfers(db, families)
    return families


def _drop_internal_transfers(db, families: dict[str, dict]) -> None:
    """A deed from TOWNHOUSE RENTAL II to TOWNHOUSE RENTAL V is a company moving
    a building between its own pockets, and counting it as a sale had the page
    say the family "sold 9 more" when it had sold nothing. Buildings whose only
    disposal is to another member of the same family come back out of the sold
    count."""
    all_names = sorted({n for f in families.values() for n in f["entities"]})
    if not all_names:
        return
    rows = db.execute(text("""
        SELECT document_id, party_type, party_name_normalized AS name, bbl
        FROM ownership_raw
        WHERE doc_type = 'DEED' AND party_type IN ('1', '2')
          AND party_name_normalized = ANY(:names)
    """), {"names": all_names}).fetchall()

    doc_sellers: dict[str, set[str]] = collections.defaultdict(set)
    doc_buyers: dict[str, set[str]] = collections.defaultdict(set)
    doc_bbls: dict[str, set[str]] = collections.defaultdict(set)
    for r in rows:
        (doc_buyers if r.party_type == "2" else doc_sellers)[r.document_id].add(r.name)
        doc_bbls[r.document_id].add(r.bbl)

    for f in families.values():
        member = set(f["entities"])
        internal: set[str] = set()
        external: set[str] = set()
        for doc, sellers in doc_sellers.items():
            if not sellers & member:
                continue
            keys = {_building_key(b) for b in doc_bbls[doc]}
            (internal if doc_buyers[doc] & member else external).update(keys)
        # A building sold internally and later sold on for real still counts.
        f["sold"] = len(f.pop("sold_keys") - (internal - external))

    # The floor has to be re-applied: a family that only qualified on its sold
    # side, where every one of those sales was a transfer to itself, has not
    # actually put five buildings through the record.
    for slug in [s for s, f in families.items()
                 if max(f["buildings"], f["sold"]) < _MIN_BUILDINGS]:
        del families[slug]


def _building_key(bbl: str) -> str:
    """Condo unit lots collapse to the building they sit in, matching
    _BUILDING_KEY_SQL."""
    return bbl[:6] + ("0000" if bbl[6:10] >= "1001" else bbl[6:10])


# The clustering reads every buyer entity, so it is memoised rather than run
# per request. The cache lives here rather than in the page module because
# three callers need the same answer: the hub pages, the subscribe endpoint
# validating a follow, and the weekly digest resolving one.
_cache: tuple[dict, float] | None = None
_TTL = 21600


def families_cached(db, is_buyer_entity, ttl: float = _TTL) -> dict:
    """`compute_families`, memoised for `ttl` seconds."""
    global _cache
    if _cache and time.monotonic() < _cache[1]:
        return _cache[0]
    fams = compute_families(db, is_buyer_entity)
    _cache = (fams, time.monotonic() + ttl)
    return fams


def reset_cache() -> None:
    """Drop the memo. Tests that build families over a fixture DB need this;
    so does anything that re-runs the clustering in-process."""
    global _cache
    _cache = None
