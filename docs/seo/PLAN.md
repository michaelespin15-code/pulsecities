# SEO plan — written 2026-08-18, act on this directly

## Status: steps 1-6 are DONE and live (2026-08-18). Only outreach is left.

| step | state |
|---|---|
| 1. Deepen /property and /llc | DONE. 479-640 and 450-777 words, FAQPage on both |
| 2. Entity-family hubs | DONE, but **8 hubs, not 49**. See below |
| 3. Internal linking | DONE for property, LLC, family, eviction and neighbourhood pages |
| 4. Eviction-by-ZIP | DONE as **127 per-neighbourhood pages**, 515-716 words |
| 5. Sitemap gates | DONE. 2,159 -> 65,810 URLs, split into a sitemap index |
| 6. Leftovers | DONE. /operators 72 -> 482 words, /flips and /radar retitled, per-URL lastmod, ItemList on /llc |

**The biggest single finding was not on this list.** The property page decided
robots with `... or score is not None`, and the score is ZIP-level, so
**596,432 parcels with no deed, eviction, violation or permit were rendering
`index, follow`** on ~429 words running 81% identical to each other. This plan
read the sitemap as the gate; the sitemap was never the gate. Fixed, with a
test that checks the invariant directly rather than by proxy.

Also fixed while working: the Ownership transfers table rendered one row per
ACRIS *party*, so every deed appeared twice and the seller sat under a column
headed "Buyer" on all 82,756 parcels with a deed. And `complaints_raw` had no
`(bbl, created_date)` index, so one property page took **85.8 seconds** and
Googlebot 504'd on it four times; now 48ms.



Shipped 2026-08-18, same day: `/property` and `/llc` deepened with FAQ blocks,
`ItemList` on `/llc`, and the internal linking of step 3 as far as those two
templates reach. Measured across 180 live pages: property 479-640 visible words
(from 100), LLC 450-777 (from 84-210), no page under the 450 floor, FAQPage on
both. Guarded by `tests/test_content_depth.py`.

**One acceptance criterion below was wrong and has been replaced.** "Two LLC
pages share <50% of unique vocabulary" is not achievable and does not measure
duplication: `/neighborhood`, the template this plan holds up as the good one,
scores **92-97%** on it. Any two pages in one language on one subject share
nearly all their word types. The replacement is 5-gram containment over tokens
that include digits, which is what near-duplicate detection actually runs on.
On that measure: hand-written hubs 0-1%, deepened property/LLC mean 49-50%,
`/neighborhood` 68-69%. Do not re-derive the vocabulary number; it is noise.

Two data findings from doing the work, both of which change later steps:

- **Step 2 yields 8 families, not 49, and that is the honest number.** Name
  stems give 21 groups covering 108 entities. Filing addresses cover 867, and
  clustering on them alone is wrong: 525 6th Avenue files for APPLEBAUM SPENCER,
  CHEN DOROTHY and GOPSTEIN SHELDON, an attorney's office. Requiring two
  independent signals drops 139 of 193 address groups. What survives is
  defensible, and the largest is a real story: **FLGSP is 80 companies, 80
  buildings, $435.9M, 97 executed evictions, every deed recorded on one day.**
- **ACRIS party addresses are populated**, contrary to the note that killed
  entity resolution step 3. 19,511 buyer-side deed rows carry one, and they
  cluster hard: 42 separate entities file from one Midtown suite, 35 from
  another, 27 from 520 Fifth Avenue. **This is a better spine for the step 2
  family hubs than numbered name stems**, and it is the unblock for entity
  resolution. LLC pages now use it, printing the street line only where two or
  more entities share it.
- **17,114 of 64,849 deed BBLs (26%) are condo unit lots absent from PLUTO**,
  so a quarter of the deed record joined to no address, ZIP, or neighbourhood.
  LLC pages now recover the ZIP from the tax block where the block sits in one
  ZIP (92% of blocks) and say plainly when a lot resolves to no building file.
  The addresses are still missing and that is a real ingestion gap.

---

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

### 1. Deepen `/property` and `/llc`, with FAQ blocks as the vehicle  — DONE 2026-08-18

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

**Acceptance (met):** both templates >450 visible words; FAQPage validates. The
vocabulary criterion was replaced, see the status note at the top.

What shipped beyond the list above, because the record was already there and
nothing was reading it: open violations by HPD/DOB class, a building-against-its-ZIP
comparison, the deed-record end date on both templates (the `/flips` honesty rule,
which these 1,914 pages were breaking), and, on LLC pages, the portfolio's own
eviction and violation record, the filing-address cluster, and the ZIP
displacement scores. Two correctness fixes fell out of it: the transfers table
rendered one row per ACRIS *party*, so every deed appeared twice and the seller
sat under a column headed "Buyer" on all 82,756 parcels with a deed; and entity
names were being `str.title()`d into "Llc".

### 2. Build entity-family hubs — the one genuinely new page type

`phantom-capital-14/16/25/30/33` are five isolated thin pages. They are one operation.
Measured: **PHANTOM CAPITAL = 28 sibling entities / 59 buildings**, **BREDIF = 4 / 134**,
and **49 numbered-sibling families with 3+ entities** sitewide.

A family hub is substantial by construction, gives the 122 orphaned LLC pages real
internal links, matches demand Bing already logged by name (`bredif ms seller llc`,
`bredif wb high point llc`), and is the site's investigative thesis made navigable.

**Acceptance:** 49 hubs; every LLC page links to its family; family pages >600 words.

### 3. Fix internal linking (free authority routing, nothing external needed)  — PARTLY DONE

Done: every LLC page now links to the neighbourhoods it buys in (verified on 120
of them, none without), and property pages link out to the rent-stabilization
guide and to sibling buildings in the same owner network. **Still missing: the
sibling-entity link**, which belongs with the family hubs in step 2.

Original finding: an LLC page linked only to nav plus its own properties. **It
linked to no sibling entity and to none of the neighbourhoods its buildings sit
in.** With zero
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
- ~~`/llc` is the only directory page with no `ItemList` schema.~~ DONE 2026-08-18.
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
