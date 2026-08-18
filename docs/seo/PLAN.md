# SEO plan — written 2026-08-18, act on this directly

**Read this file and `docs/seo/baseline_2026-08-18.md`. Nothing else is needed to start.**
Michael pulled Google Search Console, Bing Webmaster and the Bing crawl export on
2026-08-18; every claim below was verified against the database or the live site, not
taken from the exports. Where a claim was checked and found wrong, that is recorded too.

---

## The diagnosis in three sentences

The site is being **shown** for the right queries and gets **no clicks**, because it
ranks 5–10 with **zero backlinks on every URL**. Its two highest-volume templates —
`/property` (1,792 pages, 165 words) and `/llc` (122 pages, 178 words, 82% shared
vocabulary) — are its thinnest content, and Bing already excludes 74 URLs. So the job
is **depth on the pages that exist**, not more pages, and authority is a distribution
problem no code change touches.

---

## Do these, in this order

### 1. Deepen `/property` and `/llc`, with FAQ blocks as the vehicle

These are 1,914 of 2,159 sitemap URLs and the thinnest templates on the site. Their
demand also arrives phrased as questions ("who owns this building", "how much did
water view castle llc purchase 1341 ocan parkway brooklyn ny for", "53 west 174th st …
eviction cases"). One change serves both: add a **FAQPage** block answered from that
building's or entity's own records.

Do it as one task, not two. The FAQ *is* the prose these pages lack.

- `api/routes/frontend.py::_build_property_page` — target ~600 words (the
  `/neighborhood` depth). Add: ownership chain as prose ("bought 2019 for $X, resold
  2024 for $Y, N% in M months"), rent-stabilized status from `rs_buildings`, this
  building vs its ZIP, eviction history in sentences, other buildings by the same owner.
  Data is already queried; the page is nearly all tables and no sentences.
- `/llc/{slug}` — target ~500 words. Add: portfolio summary (buildings, ZIPs,
  first/last acquisition, total consideration), acquisition timeline, neighbourhoods
  with links, and **whether any building carried an eviction before purchase** — the
  site's own thesis, currently invisible on the entity page.
- Follow the existing FAQPage pattern in `/evictions`, `/who-owns-my-building`,
  `/neighborhood`, `/is-my-building-rent-stabilized`.

**Acceptance:** both templates >450 visible words; two different LLC pages share
<50% of unique vocabulary (82% today); FAQPage validates.

### 2. Build entity-family hubs — the one genuinely new page type

`phantom-capital-14/16/25/30/33` are five isolated thin pages. They are one operation.
Measured: **PHANTOM CAPITAL = 28 sibling entities / 59 buildings**, **BREDIF = 4 / 134**,
and **49 numbered-sibling families with 3+ entities** sitewide.

A family hub is substantial by construction, gives the 122 orphaned LLC pages real
internal links, matches demand Bing already logged by name (`bredif ms seller llc`,
`bredif wb high point llc`), and is the site's investigative thesis made navigable.

**Acceptance:** 49 hubs; every LLC page links to its family; family pages >600 words.

### 3. Fix internal linking (free authority routing, nothing external needed)

Verified: an LLC page links only to nav plus its own properties. **It links to no
sibling entity and to none of the neighbourhoods its buildings sit in.** With zero
external backlinks, the internal graph is the only authority distribution available and
it is currently shallow. Property → LLC → family → neighbourhood → borough should all
be reciprocal.

### 4. Eviction-by-ZIP pages — the best *new* demand

~200 impressions, 0 clicks, ~35 neighbourhood variants, and nothing targets it. Bing
names the pivot exactly: `ny marshall eviction list by zipcode`. We hold marshal
evictions per BBL/ZIP from 2024-04-12.

**Build at ~600 words from the start.** Do not ship these at property-page depth or
they become the next thin-content problem.

### 5. Only now, loosen the sitemap gates

`scripts/generate_sitemap.py`. Both gates are over-tight and both exclude pages people
demonstrably search for:

- **Property** requires a deed **AND** an eviction (1,792 of 918,338). Every queried
  address I sampled already has a working page and none were sitemapped. 82,756 parcels
  have a deed; 19,448 have an eviction.
- **LLC** requires `count(DISTINCT bbl) >= 3 AND count(DISTINCT substring(bbl,1,6)) >= 2`.
  The `blocks >= 2` half excludes `norworth-holdings-llc` — 3 buildings on 1 block —
  **which earned 3 of the site's 5 total clicks**. 122 sitemapped vs 1,557 grantees
  with 2+ deeds.

**Expand only as far as the deepened template can carry.** Gate on "has enough records
to fill the new template", not on an arbitrary count.

### 6. Cheap leftovers

- `/operators` is **130 words**, the thinnest page on the site, and ranks for
  "biggest landlords in nyc". Add prose and rows.
- `/llc` is the only directory page with no `ItemList` schema.
- Retitle `/flips` ("Flip Watch") and `/radar` ("Speculation Radar") — brand names
  nobody searches. Compare `/who-owns-my-building`, literally a query.
- Sitemap `lastmod`: 2,111 of 2,159 URLs claim the same date, and property pages
  declare `changefreq weekly` while only changing when a record lands. Per-URL truth
  is a better freshness signal than a blanket stamp.

---

## Do NOT do these

- **Do not expand the sitemap before step 1.** Adding ~1,400 near-duplicate 170-word
  pages to a zero-authority domain already excluding 74 URLs makes it worse. This
  reverses advice given earlier the same day, on the crawl data.
- **Do not chase the category term.** `/map` is titled verbatim "NYC Displacement Risk
  Map" and still averages position 8.5–9. On-page is already correct; that is authority.
- **Do not rewrite titles for length.** Three templates exceed 70 chars (~1,914 pages),
  but all lead with the query-matching text, so only the tail and ` | PulseCities`
  truncate. Google truncates on pixel width anyway.
- **Do not "fix" the og:title mismatches** on /map and /neighborhood — word order and a
  pipe separator, social cards only.
- **Do not report "property pages don't show sale prices" as a gap.** They do, via the
  Ownership transfers table's Amount column ($26.7M style); 82,460 of 137,570 deeds
  carry a `doc_amount`. The two BBLs first sampled simply had no transfers in window.
- **Do not chase `/status` "Document size 0"** — it serves 13,560 bytes; stale Bing record.

---

## Upgrades worth considering (not yet decided)

- **Backlinks are the ceiling and the only lever is outreach.** Concrete targets the
  data itself suggests: JustFix (a user searched `justfix rent history` and landed
  here), ANHD (already cited in `/methodology`), NYC Open Data / Socrata showcase,
  civic-tech and housing-org directories. The verified MTEK/PHANTOM press pitch is
  still unsent — that is the single highest-value action on any list in this repo.
- **An answer-page tier.** The exports are full of informational phrasings the site
  half-serves: "how to find out who owns a property in nyc", "how to check if apartment
  is rent stabilized", "what is a marshal eviction". Informational queries reward depth
  and relevance more than authority, so they are winnable at 0 backlinks — unlike the
  category term.
- **Spanish ZIP-lookup intent is real and unserved** (`queens codigo postal`,
  `codigo postal far rockaway`, `codigo postal staten island`). The ES pages already
  earn impressions; a deliberate ES ZIP-lookup surface is a small, defensible build.
- **Duplicate-content risk rises with step 2.** 49 families of near-identical entities
  need a canonical story before expansion, not after.
- **Positioning:** `laggy accris` — someone searching for ACRIS to be faster. The site
  is functionally a faster ACRIS for the questions people actually ask. That is a
  homepage and press framing, not a build.

---

## How to know it worked

Compare against `docs/seo/baseline_2026-08-18.md`, not against impressions of progress.

1. LLC/property visible words >450 (from 178/165).
2. LLC page pairwise vocabulary overlap <50% (from 82%).
3. Bing excluded URLs falling from 74; indexed rising from 133.
4. Clicks above 5/28d on Google. **This is the real number.** Everything above is a
   means to it, and none of it beats a single good backlink.
