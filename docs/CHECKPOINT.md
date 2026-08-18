# PulseCities checkpoint, 2026-08-18 (session 3) — reading the families for the next FLGSP

The handoff asked for one thing: read the nine new entity families and find
another story the size of FLGSP. Two came out of it, and the reading turned up
four clustering bugs that were quietly wrong on the live site.

## The two stories

**A Washington private-equity firm is buying Brooklyn's small buildings.**
`docs/outreach/pitch-carlyle-townhouses.md`. Ten LLCs have taken title to 42
small Brooklyn buildings since April 2025, 38 of them bought from outside
sellers for **$197.5M**. Seven are called TOWNHOUSE RENTAL plus a roman numeral;
five deed party rows give the filing address as **THE CARLYLE GROUP, Washington
DC 20004**, and two adjacent names (BROOKLYN TOWNHOUSE PROPERTY OWNER III and
IV, 202 8TH STREET OWNER) file from the same place. 202 units, 35 of 42 built
before 1974, **zero evictions**, 47 open violations. The story is what kind of
building institutional money is buying, not harm: there is none on the record.
The pitch says plainly that an ACRIS party address is what the filer typed.

**$549.6M of nursing home real estate moved in five weeks.**
`docs/outreach/pitch-snf-nursing-homes.md`. Nine properties changed hands on one
day, 2026-03-13, for **$474.6M**, each bought by a company named
`<address> SNF REALTY LLC`, all nine filing from one Lakewood NJ address. A
tenth on the same pattern paid $75M for a Flushing property two weeks earlier.
Corroboration: PLUTO classes all ten as institutional, and on the same day as
each deed the mortgage was assigned to **Huntington National Bank**, off
Greystone Funding, M&T and Bank Hapoalim. This is a health-care desk story, not
a housing one, and the pitch says so.

**The FLGSP pitch was wrong and is now corrected.** It said $436M and 94
evictions. The real figures are **$451,300,000** (the 82 per-building prices sum
to that exactly) and **99 evictions, 98 of them predating the sale**. The old
numbers were low because two FLGSP companies were missing from the family, for
the reason below.

## `scripts/family_stories.py`, so the next one is found by running something

Profiles every family for story shape: bulk trade, unwind, assembly or hold;
units, DHCR registrations, open violations, evictions split by whether they
predate the family's own deed, counterparties, per-building deed IDs. Writes
`family_stories.json` and a ranked `family_stories.txt`. Rerun it after every
clustering change.

## Four clustering bugs the reading exposed

1. **Filing-address variants split a group.** `C/O: SUMMIT MALLS MANAGEMENT,
   LLC` and `C/O: SUMMIT MALL MANAGEMENT, LLC` keyed as different addresses, so
   two of the 82 FLGSP companies never joined the family and /network/flgsp was
   two buildings and $15.4M short. `_addr_key()` now normalises care-of,
   punctuation and entity form; `_zip5()` normalises the 10,638 ZIP+4 rows.
2. **No third way in for an orphan.** Adoption now pulls in an entity carrying a
   family's coined label when it files from a ZIP that family already uses, on
   the same exclusivity test the label rule applies.
3. **Cross-address merging on a common token invented a landlord.** MARINE PARK
   in Rockaway, DSA on West 72nd and 1 PARK ROW in Grand Rapids were one family
   on the shared token "PARK". /network/1-park-row-commercial is gone; the merge
   now requires the token to be coined.
4. **Transfers to yourself counted as sales.** TOWNHOUSE RENTAL "sold 9" when
   all nine moved between its own companies. Intra-family transfers are now
   excluded and the five-building floor is re-applied afterwards, which also
   retired /network/ddg.

Families went 19 to 26. New hubs: JOBER (Bronstein Properties), THOR (Thor
Equities), RSS (Rialto), KEYSTONE, LBUZ, MEEKER (Cedar Park Capital, and it
bought the BSP Greenpoint portfolio), LORDAE, HOLDINGZ, RAVE, FEROZE. Every
existing slug is preserved except the two that were wrong.

## Family pages carry their own record now

Building rows show units, rent-stabilized count, year built, open violations,
price and date instead of an address and a date. Added a price range, a
portfolio totals sentence, a "Who was on the other side" section naming
counterparties, and an explicit line when the deeds are internal transfers
(REDROCK's nine same-day deeds are exactly that). This also cleared the
near-duplicate guard, which the thinner new families were failing at 70-71%.

Copy fixes on the way through: `_entity_title` printed "Flgsp" (the vowel-less
rule only matched 4 letters) and "Carroll ST"; stat chips read "1 ZIP codes" and
"1 buildings sold"; a seller-only family had a heading reading "Where X buys";
four user-visible British spellings of "neighbourhood" against 131 American ones.

## IndexNow shipped too (handoff item 3)

The one Bing-specific lever, and the last WARN in `scripts/crawl_audit.py`.
Key `4494ce2738a74028c1babaef305aec53`, served at
`frontend/<key>.txt` (the catch-all nginx location picks it up, no config
change) and named in robots.txt. `scripts/indexnow_submit.py` reads the
generated sitemaps for {url: lastmod}, submits only what moved against
`indexnow_state.json`, core URLs first, capped at 5,000 a run so the first pass
does not dump 65,962 URLs at an endpoint that rate-limits for exactly that. Cron
at 03:25, after the 03:15 sitemap. **First real submission returned HTTP 202**,
which is accept-with-key-validation-pending; the backlog drains over the next
nights. `tests/test_indexnow.py` guards that the key file, robots.txt and the
script agree, that the file stays world-readable, and that unchanged URLs are
never resubmitted.

## Data notes worth keeping

- `rs_buildings` holds two sources. `dhcr` is registrations; `hpd_jurisdiction`
  is HPD multiple-dwelling apartment counts and **is not rent stabilization**.
  Filter `source='dhcr'` for any claim with the word stabilized in it. Mixing
  them inflated a portfolio by a third in the first draft of this work.
- PLUTO `units_res` can be badly stale: 438 4th Avenue reads 17 units against 51
  DHCR-registered units in 2022 and 2023.
- Deed amounts are per document. Where one price appears on two documents from
  the same seller on the same day, decide which reading you mean before
  printing a total (the Carlyle pitch gives both).

## New guards

`tests/test_entity_families.py` gained: filing-address variants collapse to one
key; no entity carrying a family's coined label from a family ZIP is left
outside it; every member shares a token or stem with the label. 20 tests there,
suite green.

# PulseCities checkpoint, 2026-07-15 (later) — #8 shared SSR nav DONE

The one real refactor the prior handoff queued is done and live (faedc35).

**What shipped:** a single `_ssr_nav(active, lang, toggle_html, track)` helper
(next to `_FOOTER_HTML`) now renders the top nav on EVERY SSR page. Before this
each page hand-rolled its own `<nav>` with a different link subset and none but
the homepage surfaced `/displacement` or `/this-week`. All 11 navs now carry the
full hub set: Map, Displacement, Neighborhoods, Operators, Flips, Radar, This
week, Methodology. The current page's link is brightened + `aria-current`.
- English pages pass `_LANG_TOGGLE_BTN` (JS EN/ES button) or nothing (this-week,
  week, property).
- Bilingual pages (neighborhoods dir, borough, **and the individual
  `/neighborhood/{zip}` page** — folded in beyond the recipe since it is the
  highest-value organic surface, 177 pages) pass a server-toggle anchor built
  from `alt_url` + the page's `LL`/`L` labels; ES labels come from
  `_SSR_NAV_LABELS["es"]`. localStorage `pc-lang` still auto-redirects ES users,
  so dropping the per-link `?lang=es` suffix does not regress ES persistence.
- Displacement keeps its `Showcase Nav` plausible funnel via `track=True` (7
  outbound links tracked; the self-link is active/untracked).
- Property gained the full hub nav (was brand-only); its `.nav-inner` CSS got
  `justify-content:space-between` + the mobile-wrap media query.

**Guard:** `tests/test_ssr_nav.py` (16 tests) — helper unit tests (hub set,
active marker, ES labels, toggle/track) + integration tests asserting every SSR
route's top nav carries all 8 hub links, incl. ES variants and week editions.
Adjacent guards re-run green: footer/analytics/breadcrumbs/displacement (19),
frontend_routes incl. palette (57), watch-cta/nbhd-flips/operator-seo (7),
ui-copy incl. em-dash (10). Live-verified via nginx: all pages 8/8 hub links.

**#9 progress (this session):**
- **Footer parity DONE** (ef987a0): all static footers (about/methodology/press/
  status/operator/developers) + the brief footer (`briefs.py`) now carry
  `/neighborhoods`, `/displacement`, and LinkedIn to match the SSR
  `_FOOTER_HTML`. `test_footer_consistency` CANON was tightened to require
  `/neighborhoods` + `/displacement`, so future drift fails.
- **Neighborhood lateral-link sections DONE** (d32a068): each `/neighborhood/{zip}`
  now renders "Operators active in {name}" (non-noise operators holding parcels
  in the ZIP -> `/operator/{slug}`), "More {borough} neighborhoods" (borough ZIPs
  by score -> `/neighborhood/{zip}`, self excluded), and an always-on
  `/displacement` CTA. List sections render only when non-empty; bilingual via
  `_NB_L`. Guarded by `test_neighborhood_lateral.py`.

- **nginx trailing-slash 301s DONE** (d75a989): a regex `location` 301s the
  slash form of every content route (`/flips/`, `/brooklyn/`, `/this-week/archive/`,
  etc.) to the canonical slash-less URL, query preserved. Copied to /etc, `nginx -t`
  clean, reloaded; 15 routes verified 301 -> 200. `test_infra_guards` still green.
- **/property in the sitemap DONE** (1e31c7c): only the ~1.5k buildings with BOTH
  a deed transfer AND an eviction (the arc the site documents) are listed, at
  priority 0.5 — NOT the ~912k parcels that merely inherit a ZIP score (doorway
  flood). `test_sitemap.py` guards the flood cap + that every listed property is
  200 + index,follow. sitemap regenerated live (1800 urls); the file is a cron
  artifact so only the generator + test were committed.

**#8, #9, AND P3 polish are now COMPLETE.** P3 this session:
- **/displacement sec-h -> h2** (e8f81df): the four section headers are now
  semantic `<h2 class="sec-h">` (identical render; reset zeroes margins).
- **"biggest NYC landlords" keyword** (e8f81df): /operators title is now
  "Biggest NYC Landlords by Acquisition Volume", H1 "The biggest NYC landlords"
  (server + JS EN/ES), meta rewritten; the sub-desc keeps the accurate
  "ownership clusters from ACRIS deeds" framing. Individual operator pages left
  alone (claiming "biggest" on a small operator would be dishonest).
- **Dynamic OG cards** (c56ef67): `_render_headline()` + `/og/borough/{slug}.png`
  and `/og/this-week/card.png` (slashed path so /og/{zip}.png can't swallow it).
  Borough card = tracked/avg/top; this-week card = score-history deltas
  (risers 7d, high-pressure count, citywide avg) NOT raw filings, which lag ~2wk
  and read 0 mid-lag. Pages point og/twitter:image at the cards.
  `test_og_cards.py` guards dynamic-not-default + references.

The full 2026-07-15 growth+SEO backlog (#8, #9, P3) is now shipped and
live-verified. Nothing left queued from it. Not pushed (Michael runs `git push`).
New guard tests this run: test_ssr_nav, test_neighborhood_lateral, test_sitemap,
test_og_cards. All adjacent suites green.

---

# PulseCities checkpoint, 2026-07-15 — growth levers + full SEO build

## Session outcome (consolidated)

Marathon session: growth levers, then a full 3-agent SEO audit and its fixes.
~21 commits, all live (working tree IS prod), all tested, NOT pushed.

**Live-verified this pass** (curl via nginx, 200 + content): `/`, `/displacement`,
`/property/{bbl}`, `/operator/{slug}`, `/this-week`, `/neighborhood/{zip}`,
`/flips`, `/radar`, `/operators`, `/neighborhoods` all 200. `/displacement`
serves real content + CollectionPage/BreadcrumbList; `/property` renders a real
address H1 (not the old map shell); all 12 SSR pages load Plausible once.

**Shipped (high-impact, done + live):**
- `/displacement` flagship; Plausible on every SSR page; watch-block conversion
  CTA (EN/ES); per-ZIP recent-flips section; weekly post-pack automation.
- Real `/property/{bbl}` SSR bodies (Place + BreadcrumbList, noindex when thin).
- Operator pages: Dataset + BreadcrumbList schema, addresses/ZIPs link to
  `/property` + `/neighborhood`.
- `/this-week` schema (NewsArticle+ItemList+BreadcrumbList); BreadcrumbList on
  `/displacement /operators /neighborhoods /flips /flips-editions /radar` via
  `_crumbs()`.
- On-page: neighborhood titles un-truncated (177 pages), `/map` title, trimmed
  descriptions, `/status` noindex, `/map` sitemap priority 0.9->0.6.
- Homepage Organization schema; `/displacement` in homepage nav + footer + all 13
  SSR footers.
- Guard tests added throughout (displacement, ssr-analytics, watch-cta,
  neighborhood-flips, property, operator-seo, breadcrumbs).

**Internal linking to new pages (verified):**
- `/displacement`: homepage nav + homepage footer + EVERY SSR page footer.
  NOT yet in the individual SSR page TOP navs (that is the pending nav refactor).
- `/property`: linked from `/flips`, `/radar`, `/this-week`, `/flips/editions`,
  `/displacement`, the neighborhood recent-flips section, and operator pages.

**NOT fully optimized — remaining (queued, tasks #8/#9):**
- #8: shared SSR nav constant. `/displacement` is in footers but not the SSR page
  top navs; each nav is hand-rolled and inconsistent. Real refactor across ~10
  page heads; deliberately left for a fresh session.
- #9: neighborhood lateral-link sections (operators-buying-here,
  nearby-neighborhoods); `/displacement` in the about/methodology/press footers;
  nginx trailing-slash 301s; `/property` in the sitemap (index file for volume).
- P3: dynamic OG images (borough/this-week); `/displacement` sec-h `<div>`->`<h2>`;
  "biggest NYC landlords" keyword weave.

**Push:** pushed to GitHub (`main`, private repo `michaelespin15-code/pulsecities`)
through fe460e1 on 2026-07-15. Agents cannot push; Michael runs `! git push`.

---

## For a NEW chat session: environment, patterns, gotchas

**Deploy model (unchanged, critical):** the working tree IS production.
- Static files (`frontend/*.html`, sitemap.xml, robots.txt) are served by nginx
  straight from disk — edits are instantly live, no reload.
- Python (FastAPI/gunicorn) runs as systemd unit `pulsecities`. After editing
  any `.py`, `systemctl reload pulsecities` (import-check FIRST:
  `venv/bin/python -c "from api.main import app"` — a syntax error left unreloaded
  is invisible; a bad reload crash-loops).
- nginx config lives in `deploy/nginx-pulsecities.conf`. To change routing: edit
  it, `cp deploy/nginx-pulsecities.conf /etc/nginx/sites-enabled/pulsecities`,
  `nginx -t`, `systemctl reload nginx`.
- **New SSR routes need an nginx `location = /route` proxy block** — SSR pages are
  individually allow-listed; an unlisted path falls through to static and 404s.
  This bit /displacement; don't forget it for new routes.
- The nightly-generated files (`frontend/sitemap.xml`, `frontend/llms.txt`,
  `scripts/*_state.json`, `eviction_flips_editions.json`) show as `M` in git
  every session — they are cron artifacts, NOT your edits. Do not commit them
  with feature work.
- Box is 4GB/2CPU; run pytest in targeted subsets, not the whole suite at once.

**Verify a page:** `curl -s -k -H "Host: pulsecities.com" https://127.0.0.1/PATH`
or TestClient (`from fastapi.testclient import TestClient`). Both hit the prod DB
(single-DB box) — reads are safe; NEVER let a test commit to prod tables (use
dry_run + rollback; see the audit-2026-07-11 note below).

**SSR page pattern (all in `api/routes/frontend.py`, ~4000 lines):** each page
is a full-HTML f-string with inline `<style>` (dark theme `#0f172a`), built with
these module helpers: `_jsonld(obj)`, `_set_meta()`, `_tier_info(score)`,
`_FOOTER_HTML` / `_FOOTERS[lang]`, `_crumbs(*(name,path))` (BreadcrumbList for a
JSON-LD @graph), and `{_PLAUSIBLE}` injected right after the
`<script type="application/ld+json">{jsonld}</script>` line (this is how every
SSR page loads analytics — keep the pattern on new pages).
- Property page: `_build_property_page()`. Neighborhood: `_build_neighborhood_page()`
  (bilingual via `_NB_L[lang]`, `?lang=es`). Displacement: `displacement_page()`
  with `_approved_flip_arcs()` (named eviction-flip arcs come ONLY from approved
  editions — the human review gate; do not bypass it).
- **Palette guard:** never use the retired bright greens (`#4ade80`, `#22c55e`,
  `#16a34a`, `#eab308`) anywhere in `frontend.py`/`briefs.py` — `test_frontend_routes
  ::TestCanonicalTierBands` fails the build. Use canonical `#3E6B54` (positive),
  `#C08B2D`, `#F97316`, `#EF4444`.
- **Footer guard:** `test_footer_consistency` requires the CANON link subset on
  every footer; the SSR footer is `_FOOTER_HTML`. Extra links are allowed.
- **No em dashes in UI copy** (Michael's standing rule).

**Tests added this session:** test_displacement_page, test_ssr_analytics,
test_watch_cta, test_neighborhood_flips, test_property_page, test_operator_seo,
test_breadcrumbs, test_flips_postpack, plus the base-scraper source-unchanged
guards.

## Remaining work — executable recipes

**#8 shared SSR nav constant (the one real refactor left).** Every SSR page
hand-rolls its own `<nav>` and they disagree (different link subsets; none but
the homepage include /displacement). Build one `_SSR_NAV` constant (mirror
`_FOOTER_HTML`) with the full hub set — Map, Displacement, Neighborhoods,
Operators, Flips, Radar, This-week, Methodology — and interpolate it into each
page's nav: operators, neighborhoods, borough, flips, flips/editions, radar,
this-week, week (`_week_nav_html`), displacement, property. Verify each renders
and run the frontend + footer + analytics tests. ~10 nav blocks; do it carefully.

**#9 remainder:**
- Neighborhood lateral sections in `_build_neighborhood_page()` (render only when
  non-empty, like the flips section): "Operators buying in {name}" (query
  `operators` via `operator_parcels`→`parcels` filtered to the ZIP, link
  `/operator/{slug}`), "Nearby neighborhoods" (same-borough ZIPs by score, link
  `/neighborhood/{zip}`), and a `/displacement` CTA link. Bilingual copy keys in
  `_NB_L`.
- Add `/displacement` (+ LinkedIn for parity) to the about/methodology/press
  static footers.
- nginx trailing-slash 301s: a regex `location` returning 301 for the exact-match
  content routes (currently `/flips/` etc. 404). Place before the catch-all.
- `/property` in the sitemap: `generate_sitemap.py`, query BBLs that have signals
  (index the substantive ones only), split into a sitemap index if volume is large.

**P3 polish:** dynamic OG images for `/borough` + `/this-week` (infra exists in
`api/routes/og_images.py`); `/displacement` section headers `<div class="sec-h">`
-> `<h2 class="sec-h">`; weave "biggest NYC landlords" into operator titles/H1.

## Action items on Michael (only he can do)
- **Anthropic credits**: top up, or the AI read (`/api/summary`) keeps returning
  503 to visitors (it degrades gracefully with a cooldown, so no crash).
- **Before the repo goes PUBLIC** (part of the job-search "ship the proof" plan):
  scrub `DATABASE_URL` from git history (`git filter-repo`) and rotate the
  Postgres password — it was committed in the April initial commit. Anthropic key
  + NYC token in that old `.env` were placeholders; Resend/R2 keys were never
  committed. Fine while the repo stays private.
- Distribution: post this week's flip post-pack; send the 153% reporter tip.

## Bigger-picture backlog (pre-existing, not this session)
Growth bottleneck is distribution, not features (~4 real external subscribers,
near-zero human traffic, Googlebot crawling fine). Parked directions: "ship the
proof" (repo public + builder write-up), automate-the-drop (DONE this session),
conversion instrumentation (Plausible now on all pages — build the funnel views).

---

# PulseCities checkpoint, 2026-07-14 — digest retime, evictions guard, drop automation, /displacement showcase

## Growth build (later session): /displacement + SEO push

Michael picked "build, don't post" and asked to build the three growth levers
plus optimize SEO all around. Standing prefs honored (autonomous, no prompts).
Progress this session, tracked in the task list (#1 done, #2-#5 pending):

- **/displacement flagship SHIPPED** (e39f58b). One SSR destination pulling the
  strongest signals into a narrative: eviction-to-resale arcs, highest-pressure
  neighborhoods, largest landlords, buying clusters. Each section deep-links out
  (/flips/editions, /neighborhoods, /operators, /radar). Live at
  https://pulsecities.com/displacement. Full meta/OG/JSON-LD (CollectionPage),
  dark editorial theme matching the other SSR pages, cached _PAGE_TTL.
  - **Approval gate held**: named eviction-flip arcs come only from APPROVED
    editions via `_approved_flip_arcs()` (17 approved arcs today); the 3 pending
    W28 arcs stay off. test_displacement_page.py guards this + rendering.
  - **Plausible wired on this page** (nav/section/CTA events) as the first SSR
    page with funnel tracking. Data queries reuse existing shapes: displacement_
    scores for hot ZIPs, operators table for landlords, query_flips/query_radar.
  - New route needed a **nginx `location = /displacement` proxy block** (SSR
    pages are individually allow-listed; unknown paths 404 as static). Edited
    deploy/nginx-pulsecities.conf, cp'd to /etc, nginx -t, reloaded. Registered
    in generate_sitemap.py (priority 0.9).
- **Homepage links to it** (04cd936): nav_displacement in desktop nav + More
  menu, EN + ES. Later also added to the shared SSR footer, EN + ES (b9f6649),
  so all 13 SSR pages link it. Was otherwise an orphan (sitemap-only).

- **#2 Plausible on every SSR page DONE** (e06b55a). All 12 SSR page heads build
  their own <!DOCTYPE> and were untracked; now a shared `_PLAUSIBLE` head const
  is injected after the JSON-LD line across all of them (+ /this-week via its
  canonical anchor, no JSON-LD block). test_ssr_analytics.py fails if any SSR
  page ships without analytics or double-injects. Funnel events live on the
  showcase; the subscribe conversion event ships with #3.
- **#3 watch-this-block CTA DONE** (8f05615). Bilingual (EN/ES) subscribe card
  on the neighborhood SSR pages, the organic landing surface. POSTs the page ZIP
  to /api/subscribe, fires plausible('Subscribe') + 'Neighborhood Watch Submit',
  closing search -> view -> subscribe. JS built with json.dumps for safe ZIP/copy
  interpolation; success uses canonical #3E6B54 (the stale-green palette guard
  test_ssr_tier_colors caught #4ade80 on the first pass). test_watch_cta.py
  guards presence/wiring/localization/palette. OG cards + copy-link already
  existed on these pages, so per-page share/OG was already covered.

- **#4 recent-renovation-flips section DONE** (484b4d5). Folded per-ZIP flips
  (LLC deed + A1/A2 permit within 60d, past 365d) into neighborhood SSR pages as
  unique indexable content + /property internal links, bilingual, renders only
  when non-empty. Chose this over new per-ZIP pages precisely for the doorway
  risk: flips are sparse (top ZIP ~2). test_neighborhood_flips.py guards it.
- **#5 sitewide SEO pass — partial** (b9f6649, 466e85d). See audit below.

## Full SEO audit (3 parallel agents, 2026-07-14) + fixes applied

Michael: "do as much SEO as needed, use agents, full audit." Dispatched 3
general-purpose agents (technical / on-page+schema / content+internal-linking),
all verified against live pages + source. Headline: **technical SEO is healthy**
(no criticals — robots/canonicals/hreflang/redirects/404s/sitemap all correct).
The real wins are on-page schema and internal linking.

Applied this session (466e85d):
- Neighborhood <title> dropped borough: 73 -> ~68 chars, stops SERP truncation
  across 177 pages (EN+ES). /displacement desc 204 -> 167. /map title
  'Explore' -> 'NYC Displacement Risk Map | PulseCities'. /status noindex.
  /map sitemap priority 0.9 -> 0.6.

Audit fixes shipped this session:
- **#6 /property/{bbl} real SSR bodies DONE** (a0d5dc6). Was the app.html map
  shell (H1 "PulseCities", identical across parcels). Now renders per-building
  public-record body (address H1, area score, ownership transfers, evictions,
  permits, 311 volume) + Place + BreadcrumbList JSON-LD + Plausible + up-links to
  /neighborhood, owning /operator, borough. Buildings with no records/score are
  noindex,follow so thin pages don't dilute the index. _build_property_page().
  test_property_page.py guards real-content + noindex gate.
- **#7 operator pages DONE** (44a10c5). Added Dataset + BreadcrumbList JSON-LD
  (was zero schema); acquisition addresses now link to /property and ZIPs to
  /neighborhood in the server HTML; client portfolio table addresses link to
  /property too. test_operator_seo.py guards it.
- **#9 partial** (f1272f2): homepage Organization schema (logo/founder/sameAs)
  added; /displacement added to the homepage footer.

- **#8 mostly done** (b0e9204, 6bf7348): /this-week now emits NewsArticle +
  ItemList + BreadcrumbList (was zero); BreadcrumbList folded into /displacement,
  /operators, /neighborhoods, /flips, /flips/editions, /radar via a shared
  _crumbs() helper (JSON-LD @graph). test_breadcrumbs.py guards coverage.

Still queued:
- **#8 remainder** (riskier, left for a fresh session): ONLY the shared SSR nav
  constant is left — every SSR page has a different hand-rolled nav that omits
  /displacement. It is a real refactor across ~10 page heads; do it deliberately,
  not at the tail of a marathon session.
- **#9 remainder**: neighborhood lateral-link sections (operators-buying-here,
  nearby-neighborhoods, /displacement CTA); /displacement + LinkedIn in the
  about/methodology/press footers; homepage operator/signal module rows as links;
  nginx trailing-slash 301s; /property in the sitemap (index file for volume).
- Minor/P3: dynamic OG images (borough/this-week); og:image dims on ~11 pages;
  /displacement sec-h divs -> h2; "biggest NYC landlords" keyword weave.

Session commits (SEO push): e39f58b, 04cd936, e06b55a, 8f05615, b9f6649, 484b4d5,
466e85d, a0d5dc6, 44a10c5, f1272f2 (+ checkpoints). All live (working tree is
prod); nginx cp'd to /etc and reloaded; NOT pushed (Michael runs git push).

## Earlier session (three questions -> drop automation)

Michael opened with three questions (where to post the flip email, why the weekly
digest sends 5am Sunday, why a pipeline-anomaly email fired), then "what else can
we optimize"; he chose "automate the drop." Three shipped, two commits.

## Shipped

1. **Digest retimed to Sunday 6:00 PM ET** (f48d7e6). Was `0 9 * * 0` UTC = 5am
   EDT, the worst open window. Now DST-pinned like the donna cron: fires at the
   22:00/23:00 UTC slots that straddle 18:00 Eastern and guards on the Eastern
   clock, so it stays 6pm ET across DST. config/schedule.py (DIGEST_CRON
   `0 18 * * 0`, tz America/New_York) and deploy/pulsecities.cron updated in
   step; deployed to /etc/cron.d, gunicorn reloaded, /api/schedule verified live.
   Only the digest moved; flips scan (09:30) and ops-health (09:45) are internal
   emails and stayed put.
2. **Evictions "0 records" anomaly was a false positive, now suppressed**
   (f48d7e6). Not a break: source has 961 records/30d, max executed_date 07-07;
   we pulled ~189/night 07-06..10 draining a catch-up backfill and are now caught
   up, so on_conflict_do_nothing returns 0 new. base.run() now compares
   new_watermark to the last successful watermark: a 0-record run whose watermark
   did not advance is steady state (INFO, status success, no page); a 0-record
   run with no evidence the source advanced still warns. Generic in base.py, so
   any lookback scraper benefits; no evictions.py change. **ACRIS still alerts by
   design** (genuine 14-day upstream freeze, watermark stuck at 06-30; that is a
   real outage worth surfacing, not weekly-cadence quiet).
3. **Drop automation** (05b59d6). The weekly flip email now appends a
   ready-to-post pack: X thread (numbered, budgeted under 280 incl. the k/N
   suffix, ACRIS docs dropped first on overflow), Bluesky post on the biggest
   gain (under 300), reporter tip with the deed numbers + buyer portfolio scale.
   Ships in the existing Sunday 09:30 flips cron, only when there are new arcs.
   Nothing auto-posts; it lands in the review email. test_flips_postpack.py
   guards char limits, thread shape, receipts, and the no-em-dash rule.

## Traffic reality check (the real bottleneck)

- **~4 real external subscribers.** 8 distinct rows in `subscribers`; the rest
  are Michael (michaelespin15 x2, michael.e@caprium, mespin@caprium) and one
  mailinator audit account. Real external: jhonsassler, hbpmes0730, jvxnyc,
  pulgarinkevin73.
- **Near-zero human traffic.** Today's requests are dominated by /api/health and
  bots; a handful of real content page views. Googlebot IS crawling (38 hits),
  so SEO plumbing works. The product out-features every competitor; the missing
  piece is attention, not another signal.
- Directions Michael did NOT pick this session (parked): **ship the proof**
  (make repo public after a git-history secrets scrub, builder write-up, reporter
  pitch) and **fix conversion** (funnel events via the wired Plausible, a
  watch-your-block hook, per-page share/OG).
- **On Michael:** confirm the Anthropic credit balance. The AI read degrades to
  503 on a failed model call; if credits are exhausted the headline feature is
  dark to any visitor who clicks it. Not re-verified this session (billing).

## Verification

test_base_scraper.py 7 pass, test_flips_postpack.py 7 pass, evictions+ownership
88 pass, pipeline_health+status 75 pass. Post-pack rendered end-to-end against
this week's three real arcs (all tweets 168-205 chars, Bluesky 232). Nothing
else touched.

---

# PulseCities checkpoint, 2026-07-11 (early morning) — full audit #2 closed

## Build sessions (~05:00–06:00 UTC, Michael approved "lets do the next build sessions")

1. **LLC-to-LLC filter: already shipped 2026-04-20** (77d9419). v2_roadmap.md was
   stale and is now marked; the 38% churn figure describes what the live filter
   excludes. No re-score happened or was needed.
2. **Offsite backups LIVE** (225d4a4): backup_offsite.sh pushes the newest dump
   to R2 nightly 04:10 (vs-archive bucket, pulsecities-backups/ prefix, borrowed
   violation-leads token; PULSECITIES_R2_* env vars switch to a dedicated bucket).
   Weekday slots + monthly pin = zero-maintenance retention. Uploads via rclone
   (apt-installed): curl 7.81 cannot sign streamed bodies and cannot slurp 1.6GB.
   First push byte-verified. False-alarm ops email fired during testing from the
   sandboxed shell; the real push succeeded.
3. **Vacate orders surfaced** (in f26e489): "Vacated by city order" section on
   neighborhood pages (distinct buildings + orders + latest month, 365d window,
   display-only). Bed-Stuy shows 8 buildings / 10 orders at launch.
4. **Spanish SSR shipped for the whole ranking funnel** (f26e489, f94faa0):
   /neighborhood/{zip}, /neighborhoods, and all five borough pages render fully
   in Spanish at ?lang=es (titles, metas, generated summary via bilingual
   _build_summary, FAQ + FAQPage JSON-LD, dates, footer). English is the
   parameterless canonical; hreflang en/es/x-default on both; EN/ES toggle
   stores pc-lang (site-wide key) and English pages honor a stored 'es'.
   /this-week already had a client-side ES layer on the same key; nothing needed.
   nginx borough proxy now forwards query strings ($is_args$args) — it was
   silently dropping them.

## Post-audit batch (~04:30–04:50 UTC, Michael approved)

- **HPD class-I violations now ingest** (ed66cb1): scraper accepts A/B/C/I;
  365-day backfill landed 68,898 rows incl. 1,742 vacate orders. Scoring
  stays B/C only — TestClassIGate pins the filter at every B/C-labeled
  surface. Watch alerts and operator monitors now include class I, so a
  vacate order on a watched building emails the watcher.
- **AI-read failure cooldown** (30db10f): 10 min per worker after a failed
  model call; panels get an instant 503 instead of a 3.5s doomed round-trip.
  /week/{current} 302s to /this-week.
- **Incident, 90s**: the cooldown's first deploy had `global` after use;
  gunicorn crash-looped 04:39:50–04:41. The NEW health probe caught the 502
  at 04:40:03 and escalated (first real proof the alert path delivers; a
  recovery all-clear follows). Rule reaffirmed: import-check every touched
  module BEFORE `systemctl reload pulsecities`.

Suite green (884 passed / 2 skipped before the fixes; re-run green after, plus 18
new guard tests). Site verified live end to end after every change. Box was
resized: 4GB RAM / 2 vCPU now (hostname still says 1gb). Tests still run in two
halves by convention.

## The headline finding (root-caused and fixed)

**The test suite was mutating production score data on every run.**
- `test_scoring_guard.py` ran `DELETE FROM score_history WHERE scored_at = today`
  and committed it — every post-scrape test run silently destroyed that day's
  snapshot. This is why score_history was missing 2026-07-10 and 2026-07-11.
- `TestOrphanCleanup` ran a real `compute_scores(force=True)` against prod
  mid-suite; `test_dhcr_scraper` transiently rewrote ZIP 10026 with synthetic
  data (visitors could see score 50 for Harlem for a few seconds).
- All three now run inside uncommitted transactions (`dry_run=True` +
  rollback); verified byte-identical DB state across a full run.
- Both missing snapshot days were recomputed via the history-only backfill path
  (177 rows each, averages continuous: 30.27 / 30.59 / 30.57).
- The nightly pipeline now has a snapshot invariant gate: scored count must be
  in today's score_history or the run fails loudly.

## Also fixed this session (2026-07-11, ~03:10–04:20 UTC)

1. **Monitoring last mile** (the June-outage class): `send_alert` buffered
   anomalies now flush into ONE ops email per pipeline run; `notify_ops()` is
   the severe path (webhook + immediate email); scoring crash / zero-scored /
   missing snapshot all fail loudly; health probe dedupes (one alert per
   outage + 6h re-alert + recovery all-clear). `ALERT_SNOOZE=dcwp_licenses` in
   .env silences the known upstream stall (remove when it recovers).
2. **Perf**: cold `/api/stats` was 10–37s, now **0.87s**. Causes: stale
   visibility map on violations_raw (VACUUM ANALYZE + per-table autovacuum
   tuning on the 4 big tables) and a missing complaints (created_date, zip)
   partial index (migration `b9e4f2a7c1d8`, which also drops two redundant
   indexes). Do not remove the autovacuum reloptions.
3. **llms.txt honesty**: generator fetched the 1h-cached HTTP endpoint, so the
   file quoted yesterday's scores every morning and hardcoded "high
   displacement pressure" for all five entries. Now reads the DB through
   `compute_top_risk()` (extracted from stats.py, shared) and takes tier words
   from `_tier_info`. Atomic writes with 0644 (mkstemp is 0600 → nginx 403,
   found live) for llms.txt + sitemap.
4. **nginx**: security headers were missing on every static page (add_header
   inheritance) — now a snippet included per location + server, with HSTS
   max-age=86400. tailwind.min.css no longer 30d-immutable (1h
   must-revalidate). Doubled Cache-Control (expires+add_header) cleaned up.
5. **logrotate** signalled nonexistent gunicorn.service; would have silently
   ended gunicorn logging at the first 50M rotation. Fixed, mirrored to
   deploy/pulsecities.logrotate, and every cron log is now rotated.
6. **Script robustness**: backup dumps to .tmp + gzip -t + mv (no more
   truncated "newest backup"); flips scan state corrupt-guard + atomic write,
   and it refuses to clobber the editions archive on a bad read; OCA ingest
   refuses a >20% shrunken upstream extract and filters the 99999 sentinel
   (19 phantom rows purged); ops-health can't crash silently; building alerts
   wait for the pipeline lock (up to 45 min) before advancing the watermark;
   missing RESEND_API_KEY exits 1 so cron sees the failure.
7. **API**: key middleware survives DB outage (401, not raw 500) and the cache
   prune race; ops token constant-time compare; ops log tail bounded (64KB
   seek); /api/health accepts HEAD (uptime monitors); search escapes LIKE
   wildcards; CORS comment resolved (deliberately open, documented).
8. **UI copy**: six `—` em-dash connectors in app.html JS strings became
   middle dots / rephrases; operator page shows a friendly EN/ES notice on
   hydrate failure (was silently blank), sets title/canonical AFTER slug
   resolution (was clobbering SSR and self-canonicalizing raw params);
   methodology signals table scrolls in a wrapper; homepage chip now says
   "LLC transfers in the last 90 days" (was "on record").
9. **Data hygiene**: scraper_quarantine 284MB → 4MB (208k known-benign HPD
   class-I rejects >30d pruned, VACUUM FULL) + 90-day retention in the nightly
   pipeline; digest citywide trigger now uses the canonical High=67 (was 75);
   dead jobs.sqlite removed.
10. **New regression guards**: sitewide em-dash test (catches `—` and
    `&mdash;`, comments and placeholder glyphs excluded, ops.html exempt);
    llms.txt-vs-stats consistency (structural + live); deploy/ vs /etc drift
    tests; logrotate-covers-every-cron-log test.

## NEEDS MICHAEL (priority order)

1. **Anthropic credits still EXHAUSTED** (verified live 03:15, fresh 400). Map
   AI read fails politely; Sunday's digest goes out WITHOUT AI narratives
   unless topped up before 09:00 UTC. console.anthropic.com.
2. **`git push`** — 40+ commits ahead of origin (env blocks the agent's push).
3. **Search Console submission**, then the press pitch (ACRIS thaw makes the
   data fresher than any time in 6 weeks — good week to send).
4. ~~Class-I decision~~ — RESOLVED: ingested + displayed, never scored (see
   post-audit batch above). Press angle now available: vacate-order counts
   by ZIP are quotable numbers nothing else on the site captured before.

## News

- **ACRIS thawed 2026-07-11**: 15,806 ownership rows overnight, watermark
  2026-06-30 (a day earlier than DOF's estimate). Expect big ingests for a few
  nights while the 43-day backlog clears; scores will move.
- DOF CardinalityViolation (4 failed nights 07-06..09) was already fixed in
  c015b50 (2026-07-09, batch de-dupe) — verified, no action.

## Watch

- **Sunday 2026-07-12**: digest 09:00 (dry-run validated this session: 6 ZIP +
  2 citywide render clean), flips scan 09:30 (quiet = expected), restore-test
  05:00, ops-health 09:45 (now also the end-to-end proof that ops email
  delivers — if no email arrives Sunday morning, that's itself the finding).
  Mon 04:15 first OCA cron (now with shrink guard).
- Tonight's building-alerts first cron ran clean (0 watches with new records).
- Swap is ~75% used steady-state; box shares with other services. Fine today.

## Open decisions (parked, Michael's call)

- OCA petitions as 7th score signal (breaks 187d comparability — deliberate or
  not at all). LLC-to-LLC filter (v2 roadmap): measured this session — **38% of
  the 180d LLC-acquisition signal is corp-to-corp churn** (3,235 of 8,517).
  Strongest signal-quality improvement available; also breaks comparability,
  so consider bundling both re-scores into one announced methodology change.
- Spanish SSR pages (/neighborhood, /this-week, borough pages are EN-only, the
  most shareable pages drop ES readers). Offsite backups (everything dies with
  the disk). Plausible upgrade. Per-key API tiers. Gunicorn access-log
  timestamps (needs unit edit + restart, not just reload).

## Facts the next session should not re-derive

- Deploy model unchanged: working tree IS production; `systemctl reload
  pulsecities` for Python; nginx: edit deploy/, cp to /etc, nginx -t, reload;
  push blocked for agents, Michael runs `! git push`.
- Integration tests hit the PRODUCTION DB by design (single-DB box). The rule
  that keeps this safe: any test that writes score tables stays inside an
  uncommitted transaction (dry_run + rollback). Never add a test that commits
  to prod tables.
- llms.txt + sitemap generators must chmod 0644 after mkstemp or nginx 403s.
- Canonical palette + thresholds unchanged (Low<34, Moderate<67, High<85,
  Critical 85+); digest, llms, ai_summary, frontend all pin to _tier_info.
- The concurrent `claude --resume` sessions on this box belong to other
  projects (/root/michaelespin, /root/violation-leads); check
  `readlink /proc/PID/cwd` before assuming they touch pulsecities.

## 2026-08-07 — Full audit sweep + de-AI design pass (session end)

Five-agent audit (backend, frontend, copy, visual, infra) and same-day fix
pass; 15 commits, 25ac829..8c8d185. What changed that later sessions build on:

- **Palette is now house-owned.** Every framework hex is retuned (accent
  #ed6317, bg #111823, muted #93a1ad, link #6fb1d8, stamp #e4483b); Tailwind
  stops overridden in tailwind.config.js so utilities follow. Ramp Low
  #3E6B54 / Moderate #C08B2D unchanged. A stock #f97316 anywhere is a
  regression now.
- **Design language tightened:** stat-card grids on /displacement and
  /this-week are ledger strips; landing modules are ruled columns; hero
  pulse-trace draws the top-risk ZIP's real 90-day line; map preview re-shot
  from the live app (was the old bright-green ramp); neighborhood h1 is
  "Name ZIP" under a kicker, no title-tag pipe; trend chart ticks on human
  steps.
- **Live bugs fixed:** renovation-flip endpoint 500 (psycopg int, _days
  guard), map app back-button trap, /displacement literal &middot;, press.html
  false "feed paused" claim, ops.html XSS, ~500 lines dead hero code purged.
- **Ops hardened:** nginx server_tokens off, Referrer-Policy, /ops.html 404,
  week+detail trailing-slash 301s, limit_req on /property /brief /og (all
  mirrored to deploy/); OOMScoreAdjust -500 on the app unit, -800 drop-in for
  postgres (applies at its next restart); daily_health_check + pipeline_health
  finally in cron (03:40/03:45) and their alerts actually flush; Resend ops
  email retries; ownership scraper retries party/legal fetches and fails loud
  so the watermark can't skip batches.
- **Repo hygiene:** sitemap.xml, llms.txt, state JSONs untracked (nightly
  churn); tree stays clean.
- **Deferred (needs Michael):** purge .env from git history or rotate the DB
  password BEFORE the repo goes public; CSP report-only; shared_buffers raise;
  dedicated PULSECITIES_R2_* creds (offsite currently borrows
  violation-leads'); og-image.png still says 178 ZIPs.

## 2026-08-07 (later) — Traction pages

/evictions (citywide marshal-eviction tracker, hub nav, FAQ schema, ES
strings) and /who-owns-my-building (tenant-intent landing, top buyers,
official registry links) shipped in 3abd296, driven by Search Console
demand. Notes for later sessions: new SSR routes 404 until nginx gets a
`location =` block (deploy copy synced); hub nav is 9 links, .nav-inner
960px; _addr_title() fixes str.title() ordinal mangling on all-caps
addresses; property titles now lead with the records promise, not score
jargon (874dd52). Remaining traction backlog lives in the traction-pages
memory note.

---

# PulseCities checkpoint, 2026-08-18 — typography, CSP, tier bands, raw_data staged, and the first real Search Console read

## Shipped (7 commits, 96c87ad..ceb20ab, all live, suite green 1,282 passed)

- **96c87ad** tabular figures + `text-wrap: balance`. The premise needed narrowing:
  Google serves DM Sans with **no `tnum` table**, so `font-variant-numeric` is dead
  CSS on it, and JetBrains Mono is already fixed-pitch. Only `.arc-gain`/`.row-val`
  on /displacement qualified (Bricolage is proportional *and* ships tnum). Measured
  live: gain chips spanned 53.6–74.7px before, 72.38px each after.
- **0a582a1** CSP report-only + Tailwind content-hash stamping. `npm run build:css`
  now also runs `scripts/stamp_asset_hash.py`; **commit app.html with
  tailwind.min.css** and never run `tailwindcss` directly.
- **1ca54ed** the OG card said "178 NYC ZIP codes". Not staleness — the wrong
  query: `neighborhoods` carries a **99999 sentinel** row. Canonical is **177**.
  `scripts/generate_og_image.py` regenerates it from the DB.
- **0cf5e08** tier bands (85/67/34) were hand-written in **ten** places; now
  `scoring/tiers.py`. Colour deliberately NOT centralised (dark page / risk ramp /
  paper ink differ on purpose). Verified behaviour-identical: 7,007 comparisons.
- **1ea21e4** the raw_data archive's R2 upload had never worked. **R2 layout:
  bucket `vs-archive`, prefix `pulsecities-backups/`**; derivation now lives once
  in `scripts/lib/r2_creds.sh`, shared with backup_offsite.sh.
- **870e2a4** /flips and /radar claimed windows their deeds no longer covered.
  Both now carry the /evictions-style through-line, EN + ES.
- **ceb20ab** raw_data drop staged (see below) plus small backlog.

## The one thing waiting on a maintenance window

    ./venv/bin/python -m alembic upgrade head   # applies b8e30d5c1746
    scripts/retire_raw_data.sh drop             # runs the migration + VACUUM FULL

Archives are done and verified offsite (5,148,918 + 2,070,570 rows). Expect
16GB -> ~7GB, and the 1.7GB nightly dump falls with it. **Do not restore the old
ordering**: migration a1f4c07b9e52 made the columns nullable first because both
were NOT NULL with no server default, and the scrapers build plain dicts for a
Core insert — deploying the code before the drop would have failed the 02:00
scrape. Also do `pg_stat_statements` in the same window (config is installed at
`/etc/postgresql/14/main/conf.d/`, needs a **restart**, then `CREATE EXTENSION`).

## Corrections — believed true, actually false. Do not act on these again.

- **LLC-to-LLC filter is already live in scoring** (`NOT EXISTS` on an LLC grantor
  in `_aggregate_llc_acquisitions`). The parked "38% corp-to-corp churn" decision
  is stale.
- Of five "unused" deps, only **APScheduler** was dead. numpy <- shapely; xlrd,
  openpyxl, tqdm <- nycdb, used as a CLI by backfill_rs_history.py.
- **idx_parcels_geometry stays.** 74MB / zero scans is true, but it is the GiST
  index on a mapping product's geometry column and worth <1% post-drop.
- A health_check.log CRITICAL dated **before 2026-08-17 16:38 UTC** is a false
  alarm from the old local 14-day ACRIS threshold (fixed in 278a8ee).
- `scripts/data_health_check.json` is tracked and reports "critical" from
  **2026-04-18**; dof_assessments has succeeded twice since. The script is not in
  cron. Revive or delete, don't half-keep.

## ACRIS is frozen upstream at 2026-07-31

Verified against the city's API — newest `recorded_datetime` genuinely is 07-31.
Real threshold is 21 days, so a **genuine** CRITICAL fires ~2026-08-21. That means
"NYC stopped publishing", not "we broke something".

## Search Console — first real read (315 queries, ~5 clicks). READ THIS BEFORE SEO WORK.

Michael pasted the query export on 2026-08-18. Headline: **impressions are real and
CTR is ~0.** This is a coverage-and-CTR problem, not a demand problem. Four findings,
in priority order. All numbers below are impressions unless noted.

### 1. "eviction marshal {neighborhood}" is the biggest cluster and nothing targets it

~35 distinct neighborhood variants, **~200 impressions, 0 clicks**:
`nyc marshal eviction list` **52**, `eviction marshal wakefield` 15,
`... mott haven` 11, `nyc eviction williamsbridge` 10, `marshal eviction list` 8,
`... bushwick` 8, `... washington heights` 7, `... midtown` 7, `... ozone park` 7,
`... upper east side` 6, `... east village` 6, then a long tail of st. george,
highbridge, east new york, queens, lower east side, bronx, upper west side,
brownsville, east harlem, borough park, bay ridge, flatbush, co-op city, crown
heights, richmond hill, manhattan, brooklyn, bed-stuy, jackson heights, east
flatbush. Plus `nyc marshal docket number search`, `nyc marshal list`.

We **have** this data (marshal evictions, 2024-04-12 onward, per BBL/ZIP) and
`/evictions` is citywide only. **The build: neighborhood-level eviction pages** on
the query's own shape. This is the clearest opportunity in the export. Guard against
the doorway-page trap by making each page carry real counts, dates and marshal
detail, not a ZIP score restated.

### 2. The property sitemap gate excludes the addresses people actually search

Address queries are a large slice: `134 macon st brooklyn ny` **35**,
`1339 lincoln pl brooklyn ny 11213` 23, `882 morris ave bronx ny` 7,
`2258 morris ave bronx ny` 7, `286 audubon avenue` 7, `265 west 34th street` 7,
`161 veronica place` 6, `1905 atlantic ave` 6, `303 troutman street` 5, and ~60 more
at 1–4. Also `3009970039` — **37 impressions, 1 click — that is a raw BBL.**

**Every address I sampled already has a working /property page. None are in the
sitemap.** `generate_sitemap.py` requires a deed **AND** an eviction (~1,792 of
918,338 parcels); these have deeds and no eviction. The rule was written to avoid
912k thin doorway pages, which is right in spirit and too narrow in practice — a
parcel with 6 recorded deeds is not thin. Revisit the threshold (e.g. N records of
any kind) rather than the intersection. Scale to weigh: 82,756 parcels have a deed,
19,448 have an eviction.

### 3. LLC pages convert best, and the gate excludes the best one

`norworth holdings llc` — 5 impressions, **3 clicks. That is 3 of the site's 5 total
clicks**, and `/llc/norworth-holdings-llc` renders 200 but is **NOT sitemapped**: it
has 3 BBLs on 1 block and the gate is `count(DISTINCT bbl) >= 3 AND
count(DISTINCT substring(bbl,1,6)) >= 2`. The `blocks >= 2` half is excluding the
highest-CTR page on the site. Also `mf blue valley apartments llc` 12/1 click,
`terra developers` 12/1 click.

Sitemapped LLC pages: **122**. LLC grantees with 2+ deeds: **1,557**. Total distinct
LLC grantees: 17,161. Loosening the gate is the highest-confidence lever here.

Caveat, do not over-promise: some demand is unanswerable. `15 west 26th street llc
deed acris` (32) and `wooster street llc ...` (16+9+1) are **not in ownership_raw at
all** — our slice is 198,446 rows, not a full ACRIS mirror.

### 4. Pages that already rank and get no clicks — a title/snippet problem

- **Rent-stabilized**: ~24 distinct phrasings, ~30 impressions, **0 clicks**, and
  `/is-my-building-rent-stabilized` exists. `is my building rent stabilized` 6,
  `how to check if apartment is rent stabilized nyc` 2, plus twenty near-identical
  long-tail phrasings. Ranking or snippet, not coverage.
- **"who owns"**: ~20 impressions across `who owns this` 4, `find building owner` 2,
  `who owns my building` 2, `nyc landlord search`, `who owns what in nyc`, etc. Page
  exists. 0 clicks.
- **Landlords**: `biggest landlords in nyc` 3, `largest landlords in nyc` 1.
  /operators is already titled for this. 0 clicks.
- **ZIP lookups** are a real intent we half-serve: the `10032` family alone is ~16
  impressions across ten phrasings; also 11219, 11413, 11355, 11427, 11104, 10044.
- **Spanish is surfacing**: `queens codigo postal` 3, `codigo postal far rockaway` 2,
  `codigo postal staten island` 1+1, `codigo de queens` 1. The ES pages earn
  impressions; worth serving ZIP-lookup intent in Spanish deliberately.

### Also worth noting

- `/flips` and `/radar` are titled **"Flip Watch"** and **"Speculation Radar"** —
  brand names nobody searches. Compare `/who-owns-my-building`, which is literally a
  query and was built from Search Console demand. Retitling those two costs nothing.
- `nyc displacement risk map` 6 and `vulnerability assessment nyc` 6 are the only
  product-category queries in the whole export.
- Ignore the `1_1751...` / `2_1729...` numeric queries; they are tracking IDs, not
  human intent.

### Suggested order when Michael returns

P1 neighborhood eviction pages (new demand, we have the data).
P2 sitemap gates: property (intersection -> substantive-record threshold) and LLC
(drop `blocks >= 2`). Cheapest wins, pages already exist and render.
P3 CTR pass on rent-stabilized / who-owns / operators — they rank and nobody clicks.
P4 retitle /flips and /radar to intent-bearing titles.

## Bing Webmaster read (2026-08-18) — small sample, but it supplies the missing variable

34 impressions and 1 click across three months (May 18 – Aug 16), against Google's
hundreds in 28 days. **Do not optimise for Bing.** Its value here is two things
Google's export did not give us: an independent sample, and **average position**.

### Position is the missing diagnosis: we rank 5–10, not 1–3

Every meaningful query sits on the bottom of page 1 or on page 2:

    nyc displacement map                      6 imp   pos 8.50
    53 west 174th st ... eviction cases       8 imp   pos 6.88
    nyc displacement risk map                 2 imp   pos 9.00
    marshal evictions nyc                     1 imp   pos 10.0
    ny marshall eviction list by zipcode      1 imp   pos 5.00
    displacement risk by neighbohood nyc      1 imp   pos 8.00
    bronx gs properties llc                   1 imp   pos 2.00
    michael espin                             1 imp   pos 6.00  -> 1 click (100% CTR)

This revises what I told Michael from the Google export. I said the zero-click
clusters were "ranking or snippet"; the evidence now leans **ranking**. The sharpest
case: `/map` is titled exactly **"NYC Displacement Risk Map"**, which is verbatim the
query, and it still averages position 8.5–9. On-page targeting is already correct, so
rewriting titles will not move that term. That is a domain-authority problem, and it
is the same "distribution, not features" conclusion the earlier checkpoint reached,
now visible in the data.

The exception is LLC-name queries: `bronx gs properties llc` ranks **position 2**.
Entity-name pages rank well because nobody else competes for them. That is consistent
with LLC pages being the best converter on Google (3 of 5 clicks) and is the strongest
argument for loosening the LLC sitemap gate.

### Independent confirmation of all four Google clusters

Address (`971 dean street nyc`), address+eviction, LLC (`bredif ms seller llc`,
`bredif wb high point llc`, `water view castle llc`), and **raw BBL**
(`3068410001`, `6469640028`) all reappear in a separate engine's sample. Two
independent samples agreeing means these patterns are real, not a Google artifact.
Note `/property/3009970039` — Google's 37-impression BBL query — renders 200 and is
**still not sitemapped**, same gate as finding #2.

### New query shapes worth building for

- **`ny marshall eviction list by zipcode`** — the eviction cluster asked for
  explicitly *by ZIP*. That is the pivot P1 should use.
- **`53 west 174th st bronx ny 10453 eviction cases`** (top Bing query, 8) — address
  **+** eviction in one query. Supports P1 and P2 simultaneously: per-address eviction
  history is a page shape we already have data for.
- **`how much did water view castle llc purchase 1341 ocan parkway brooklyn ny for`**
  — natural-language transaction question. **Already answerable**: property pages
  carry an "Ownership transfers" table with an Amount column rendering `$26.7M` style
  figures (82,460 of 137,570 deeds have a `doc_amount`). I initially recorded this as
  a content gap and was wrong — the first two BBLs I sampled simply had no transfers
  in window. It is a coverage/ranking problem, not a missing-content one.
- **`laggy accris`** — someone searching for ACRIS being slow. A positioning angle
  rather than a build: the site is, functionally, a faster ACRIS for the questions
  people actually ask.
- **`michael espin`** produced the only Bing click, at 100% CTR. Personal-brand search
  converts; relevant given what the project is for.

### What this does not change

P1–P4 in the Google section still stand, in the same order. Bing sharpens *why*: P2
and P3 (sitemap gates, entity pages) target query shapes where we can realistically
rank, because entity and address names have little competition. The category term
("nyc displacement map") is the one we are targeting correctly and still losing, and
no amount of on-page work will win it.

## Bing URL/crawl data (2026-08-18) — this REVERSES the "expand the sitemap" advice

Two facts from the crawl export change the plan.

### 1. Zero backlinks. Every URL, every folder.

`Backlinks 0` on all 76 rows exported, and `-` at folder level. That is the ceiling,
and it explains position 5–10 on terms we target correctly. **No amount of page work
overcomes it.** The MTEK/PHANTOM press pitch (verified, unsent) is the only lever on
this list that touches authority.

### 2. The site's indexable mass is its thinnest content

Visible word counts, measured 2026-08-18:

    evictions hub    1,897     x1
    neighborhood       643     x177
    who-owns           562     x1
    displacement       476     x1
    rent-stabilized    469     x1
    radar              362     x1
    flips              348     x1
    borough            223     x5
    LLC entity         178     x122      <- 
    property           165     x1,792    <- 
    operators          130     x1        <- thinnest page on the site

**property + LLC are 1,914 of the 2,159 sitemap URLs and the two thinnest types.**
Worse, they are near-duplicates: two different LLC pages share **94 of 115 unique
words (82%)**, so unique content is roughly 20–40 words per page. Bing's own counters
agree — of ~314 URLs known: **133 indexed, 107 warning, 74 excluded**.

**So do not loosen the sitemap gates yet.** The previous section recommended adding
~1,400 LLC pages and thousands of property pages. Doing that now would multiply
near-duplicate 170-word pages on a zero-authority domain that is already excluding 74
URLs. **Deepen the templates first, then expand.** The gates were over-tight for the
wrong reason, but loosening them before the pages carry substance makes it worse.

### What to add — we already hold the data

**Property pages (165 -> target ~600, the /neighborhood depth):** ownership chain as
prose ("bought 2019 for $X, resold 2024 for $Y, N% in M months"), rent-stabilized
status from `rs_buildings`, this building vs its ZIP, eviction history in sentences,
other buildings by the same owner. The tables are already there; the page has almost
no prose.

**LLC pages (178 -> target ~500):** portfolio summary (buildings, ZIPs, first/last
acquisition, total consideration), acquisition timeline, neighbourhoods with links,
and whether any building carried an eviction *before* purchase — the site's own thesis,
currently invisible on the entity page.

### New page type worth creating: entity families

The crawl list shows `phantom-capital-14/16/25/30/33` as five isolated thin pages.
They are one operation. Measured:

    PHANTOM CAPITAL       28 sibling entities,  59 buildings
    BREDIF                 4 sibling entities, 134 buildings
    numbered-sibling families with 3+ entities sitewide:  49

**49 family hubs**, each genuinely substantial (28 entities and 59 buildings is an
investigation, not a doorway page), each giving the 122 orphaned LLC pages real
internal links, and each matching live demand — Bing recorded `bredif ms seller llc`
and `bredif wb high point llc` as separate queries. This is the one "create new pages"
idea in the export that adds substance rather than surface area.

### Revised order

1. Deepen the property and LLC templates (they are the mass, and they are thin).
2. Build the 49 entity-family hubs; link the thin LLC pages into them.
3. Eviction-by-ZIP pages (still the best new demand) — built at ~600 words from the
   start, not at property-page depth.
4. Only then loosen the sitemap gates.
5. `/operators` is 130 words and ranks for "biggest landlords in nyc". Cheapest fix.
6. Backlinks are the actual ceiling. That is the press pitch, not a code task.

Ignore two things in the export: `/status` shows `Document size 0` but serves 13,560
bytes (stale Bing record), and the single "Title too long >70 chars" warning.

## Structured data + titles audit (2026-08-18)

Bing's validator said "1 markup type found" on an LLC page; that is just the first
block it rendered. Actual coverage, measured across every template:

    /                       Dataset, Organization, WebSite, SearchAction, Place, SoftwareApplication...
    /neighborhood/{zip}     Dataset, FAQPage, Place, PropertyValue, Breadcrumb
    /evictions              Dataset, FAQPage, Organization, Breadcrumb
    /who-owns-my-building   FAQPage, Breadcrumb
    /is-my-building-...     FAQPage, Breadcrumb
    /llc/{slug}             Organization, Breadcrumb
    /property/{bbl}         Place, PostalAddress, Breadcrumb
    /operators /flips /radar /brooklyn    ItemList, Breadcrumb
    /llc                    Breadcrumb ONLY          <- gap

Coverage is good. Two real gaps and two non-issues.

**Gap 1: `/llc` is the only directory page without `ItemList`.** /operators, /flips,
/radar and /brooklyn all declare one; the LLC directory lists 122 entities and
declares nothing. Straight inconsistency, cheap fix.

**Gap 2 — and this is the one that matters: no `FAQPage` on the two mass templates.**
`/property` (x1,792) and `/llc` (x122) are the thinnest pages on the site AND the ones
whose queries are phrased as questions. From the exports: "who owns this building",
"how much did water view castle llc purchase 1341 ocan parkway brooklyn ny for",
"find the owner of a building", "53 west 174th st ... eviction cases". The site
already uses FAQPage well on four other templates, so the pattern exists.

Adding a FAQ block to the property and LLC templates, answered from that building's or
entity's own records, does three jobs at once: it supplies the prose those pages lack
(165 and 178 words today), it makes them eligible for FAQ rich results, and it matches
the observed query phrasing verbatim. **This is the same work as "deepen the
templates" in the section above, not a separate task** — do it as one change.

**Non-issue 1: titles over 70 chars.** Three templates exceed it, and because two are
the mass types it is really ~1,914 pages, not the "1 instance" Bing reported. But all
three lead with the query-matching text — `65 Broadway, Manhattan NY 10006: ...`,
`BRONX GS PROPERTIES LLC: ...`, `Is my building rent stabilized? ...` — so what
truncates is the tail and the ` | PulseCities` suffix. Google truncates on pixel width
anyway. Low priority; do not spend the deepening effort here.

**Non-issue 2: og:title mismatches** on /map (`PulseCities | NYC Displacement Risk Map`
vs the title's reverse order) and /neighborhood (a pipe separator). Cosmetic. og:title
drives social cards, not ranking, and brand-first is defensible there.

## >>> SEO work: read docs/seo/PLAN.md, not the four sections above <<<

The Google / Bing / crawl / schema analysis appended above was written in four passes
as the data arrived, and the later passes **reverse** the earlier ones. Do not act on
them in sequence. `docs/seo/PLAN.md` is the consolidated, ordered version and
`docs/seo/baseline_2026-08-18.md` is the fixed baseline to measure against
(commit 3040269). The sections above are kept only as the reasoning trail.

Headline: shown for the right queries, ranks 5–10, **zero backlinks on every URL**, and
the two highest-volume templates (`/property` 165 words x1,792, `/llc` 178 words x122)
are the thinnest content on the site. **Depth before expansion.**

## 2026-08-18 (later) — SEO plan step 1 shipped: both mass templates deepened

`docs/seo/PLAN.md` is updated in place and carries the detail. Headlines only here.

**Shipped, live, suite green (1,174 passed).** `/property` (1,792 sitemapped) and
`/llc` (122) went from 100 and 84-210 visible words to 479-640 and 450-777,
measured across 180 live pages. FAQPage on both, answered from each building's or
entity's own records rather than boilerplate. `ItemList` on `/llc`. Guarded by
`tests/test_content_depth.py`.

**The plan's duplication metric was wrong. Do not use it again.** "Unique-word
overlap <50%" is unachievable and does not measure duplication: `/neighborhood`,
the template the plan calls good, scores 92-97% on it. Replaced with 5-gram
containment over digit-bearing tokens. Measured: hand-written hubs 0-1%,
property/LLC mean 49-50% max 63-66%, `/neighborhood` 68-69%.

**Two correctness bugs found by reading the rendered page, not by testing.**
1. The Ownership transfers table rendered one row per ACRIS *party*, so every
   deed showed twice and the seller sat under a column headed "Buyer". All
   82,756 parcels with a deed. Now grouped by document, seller in the sub-line.
2. Entity names went through `str.title()`, printing "Llc", "Bronx Gs". Fixed
   with `_entity_title()` (acronym set plus a vowel-less-token rule); it also
   strips the stray trailing punctuation on 27,900 PLUTO owner names.

**Two data findings that change later work.**
1. **ACRIS party addresses ARE populated** (19,511 buyer-side deed rows), which
   contradicts `project_entity_resolution_status`. They cluster: 42 entities at
   one Midtown suite, 35 at another, 27 at 520 Fifth Ave. This is a better spine
   for the step 2 family hubs than numbered name stems, and it unblocks entity
   resolution step 3.
2. **17,114 of 64,849 deed BBLs (26%) are condo unit lots absent from PLUTO**, so
   a quarter of the deed record joins to no address, ZIP or neighbourhood. LLC
   pages now recover the ZIP from the tax block where that block sits in one ZIP
   (92% of blocks) and say plainly when a lot has no building file. **The
   addresses are still missing. That is an ingestion gap worth its own pass.**

**Perf note.** The ZIP peer-comparison query is per-ZIP data that was being run
per-building (402ms on 11207, and there are ZIPs half again that size). Memoised
in `_zip_context()`: 434ms for the first building in a ZIP, 66ms for every one
after. ~180 ZIPs, so it is bounded.

**Gotcha for the next session.** `_long_date(d, lang)` already existed; defining a
one-arg `_long_date` shadowed it and 500'd every neighbourhood page. Only the test
suite caught it. `frontend.py` is ~6,000 lines in one namespace, so grep before
naming a helper.

## 2026-08-18 (session 2) — SEO plan steps 1-6 shipped

`docs/seo/PLAN.md` carries the table. Only the outreach item is left, and no
code change touches it.

**The finding that mattered most was not in the plan.** `_build_property_page`
decided robots with `bool(owners or evicts or permits) or score is not None`.
The score is ZIP-level, so every parcel in a scored ZIP rendered `index,
follow`: **596,432 parcels with no building record at all**, ~429 words each,
81% identical by 5-gram containment. The plan looked at the sitemap (1,792
URLs) and concluded the gate was tight. The sitemap was never the gate.

**Shipped, live, suite green (1,216 passed).**
- /property and /llc deepened, FAQPage on both, ItemList on /llc.
- Robots gate now requires a building-level record.
- Sitemap 2,159 -> 65,810 URLs, split into a sitemap index (spec caps a file at
  50k), per-URL lastmod from each page's newest record (751 distinct values in
  the first property file, against 2,111 URLs sharing one date before).
- /evictions/{neighbourhood}: 127 pages, 515-716 words, keyed on NAME not ZIP
  because three ZIPs are called Bushwick.
- /network/{slug}: 8 entity families. FLGSP = 80 companies / 80 buildings.
- /operators 72 -> 482 words; /flips and /radar retitled off their brand names.
- `complaints_raw(bbl, created_date DESC)` + `violations_raw(bbl,
  inspection_date DESC)`: the 85.8s property page is now 48ms.

**Correctness bugs found by reading rendered output, not by testing.**
1. Ownership transfers rendered one row per ACRIS *party*: every deed twice,
   seller under a column headed "Buyer", on 82,756 parcels.
2. `str.title()` printed "Llc" and "Bronx Gs" on every entity name.
3. `_plural` produced "addresss", "entitys", "buildingss".
4. "BANK" as a family token merged Flagstar with US Bank; summing per-entity
   building counts read five condo units as five buildings.
5. An LLC-page sentence claimed a shared filing address is "how the same
   operation appears as many names". At 525 6th Avenue that is an attorney's
   office. Corrected to state the fact and name both readings.

**Method note worth keeping.** Unique-word overlap is not a duplication
measure: /neighborhood, the template the plan calls good, scores 92-97% on it.
5-gram containment over digit-bearing tokens separates correctly (hand-written
hubs 0-1%, /neighborhood 68-69%) and is what every content test here uses.
Calibrate a metric against a page you already believe is fine before optimising
toward it.

**Crawl reality, measured from nginx logs.** Googlebot fetched 988 distinct
/property and 999 distinct /llc URLs in 14 days, so crawl budget is not the
bottleneck and this work will be seen in days to weeks. But ~877 of the 999 LLC
URLs it crawls were noindex, which the widened gate now fixes.

**Still open:** `retire_raw_data.sh` has never been run (9.3GB of a 16GB DB),
17,114 condo-unit deed BBLs have no address in PLUTO, and the MTEK/PHANTOM
press pitch is unsent. Backlinks remain the ranking ceiling.

## 2026-08-18 (session 2, later) — the ACRIS address block was a typo

`scripts/backfill_party_addresses.py` asked Socrata 636b-3b5g for `addr_1`. The
column is `address_1`. Every fetch returned no address, every update wrote NULL,
and the project recorded "ACRIS has no party address data" and parked entity
resolution on it for months. The scraper read `address_1` correctly the whole
time, which is why rows from May 2026 on have addresses and the April backfill
left 141,354 empty.

A second bug: it matched Socrata rows to DB rows by document_id alone, so a
buyer's address could land on the seller's row. Now matched on
(document_id, party_type, name).

**The backfill COMPLETED.** 141,154 rows, then a second pass for 750 rows that
five Socrata 503s had skipped. It is idempotent (only touches
`party_addr_1 IS NULL`), so re-running is always safe.

    buyer deed rows with an address   19,511 -> 68,223   (28% -> 99%)
    LLC buyers with NO address        11,160 -> 0        (71% -> 0%)

**The clustering was re-run and the site redeployed on the new data:**

    entity families   10 -> 19        entities covered   179 -> 256

New families that did not exist before, all of them invisible while the
addresses were missing: BRICKS (22 entities, 8 held + 17 sold), TSADIK (16),
REDROCK (14), 1 PARK ROW COMMERCIAL (10), BSP (9), SNF (9), SCHUMAN, BAHM,
MBSD, DDG. PHANTOM also grew from 27 entities / 57 buildings to 35 / 70.
Sitemap carries 18 `/network/` URLs; suite green at 1,227.


## >>> START HERE after /clear (2026-08-18, session 2 end) <<<

Everything below is done, deployed, committed and green (1,227 passed). Nothing
is half-finished and no process is running.

**Read first:** `docs/seo/PLAN.md` (all 6 steps DONE),
`docs/outreach/pitch-flgsp-portfolio.md` (unsent),
`docs/seo/crawler_readiness.md`, `docs/seo/resubmit_sitemap.md`.

**Michael resubmitted the sitemap to Search Console and Bing on 2026-08-18, and
it worked.** Within the hour, from real remote crawler IPs: Googlebot fetched
the sitemap 4x, bingbot 10x, and both began crawling the page types that did
not exist that morning (`/evictions/wakefield`, `/network/flgsp`,
`/network/bsp-smk`). 365 crawler responses, 364 of them 200, **zero rate-limit
rejections**. Every 429 in that day's log came from 127.0.0.1 and was this
session's own load testing.

**The single highest-value action remains unsent and needs no code:** the FLGSP
pitch. 82 buildings, 4,941 units of which 4,793 are DHCR-registered
rent-stabilized, traded in one day for ~$435.9M, 10,350 open violations. Live at
`/network/flgsp`. Three caveats are in the doc and must survive into any
version: 93 of the 94 evictions PREDATE the sale, the deeds do not say who is
behind FLGSP, and check whether the trade was already covered when it closed.

**Next build, in order:**
1. **Read the 9 new families for another FLGSP.** BRICKS sold 17 buildings and
   holds 8; BSP holds nothing and sold 11. A portfolio being unwound is the
   shape that produced the last story. This is the pipeline from data to press,
   and press is the only lever on the backlink ceiling.
2. **`retire_raw_data.sh`** — 9.3GB of a 16GB database, never run. Two phases;
   needs a code commit removing `raw_data` from the models and scrapers FIRST,
   then gunicorn reload, then `drop`. Wants a maintenance window.
3. **IndexNow** — the only Bing-specific lever, and the single WARN left in
   `scripts/crawl_audit.py`.
4. **17,114 condo-unit deed BBLs are absent from PLUTO**, so a quarter of the
   deed record resolves to no address. LLC pages recover the ZIP from the tax
   block where that block is unambiguous; the addresses are still missing.
5. **A third clustering signal**: the FLGSP *sellers* (1023/1038/1042 REALTY
   LLC) still do not form a family, because they share only "REALTY" and a
   street number and REALTY is a stop word. Linking them needs
   "counterparties in one bulk transfer", which is a different claim from
   sharing an identity. Deliberately not done.

**Method notes worth keeping.** Unique-word overlap is not a duplication
measure (`/neighborhood` scores 92-97% on it); use 5-gram containment over
digit-bearing tokens, calibrated against a page you already trust. And every
content bug this session was found by reading rendered output, not by reasoning
about code: doubled deed rows, "Llc", "addresss", a false claim about shared
filing addresses, and 127 eviction pages shipped without their borough.
