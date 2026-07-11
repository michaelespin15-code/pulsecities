# PulseCities checkpoint, 2026-07-10 (end of day) — full audit closed, all clear

Everything committed on main through `c94c817`; working tree clean; suite 884 passed /
2 skipped (run tests in two halves on this box: a single full run alongside a browser
gets OOM-killed, exit 137, on 1GB+swap). Site verified live end to end.

## What shipped today (one day, in order)

1. **Homepage redesign published** (a7465c3..d945764): preview ported to /, muted green
   palette on every tier surface + palette/threshold tripwire tests, teaser + og-image
   regenerated at ?v=3, seven SSR footers collapsed into `_FOOTER_HTML`, llms.txt
   regenerated + nightly cron 03:20, sitemap crawl all-200.
2. **Docket honesty + first edition** (17b4f64, cff48a6): eyebrow says "Featured
   finding" for the fallback, switches to "This week's finding" only when an approved
   edition renders. First flips edition 2026-W28 (17 arcs) scanned early, Michael
   approved; homepage now renders the +232% Queens Village arc (104-06 104 Avenue,
   $292k → $969k, ACRIS-verified). Sunday's flips cron will be quiet BY DESIGN.
3. **Building watch alerts** (96b1c4c): subscribers.bbl, watch card on block panel
   (= /property pages), daily 03:25 cron scans deeds/permits/evictions/violations by
   created_at. Michael's email watches BBL 2050840054 as live artifact.
4. **/developers API docs + partner keys** (c658982): docs page (sitemap, llms.txt,
   footer guard, docs-accuracy tripwire test), api_keys table (SHA-256), mint script,
   X-API-Key middleware. Michael's owner key = PULSECITIES_OWNER_API_KEY in .env.
5. **/flips/editions archive** (2fc1b3c): approved editions only, SSR + EN/ES,
   self-extends weekly. **/preview retired** (c5f8bf7). **Email tier colors** joined
   the canonical palette (b8f12de) — muted inks are higher-contrast on the paper bg.
6. **OCA petitions early warning** (c16614d): weekly Mon 04:15 ingest streams the
   700MB extract via SQLite spill; 907k residential filings → zip-month aggregates;
   "Early warning" section on neighborhood pages. CC BY-NC-SA: display-only, NEVER on
   API or in the composite score.
7. **Old audit LATER backlog closed** (577d020): per-record ACRIS doc IDs on operator
   rows + verify note, score-cache retry fix, replay-stop on popstate. Several items
   verified already-closed.
8. **Full parallel audit run** (3 subagents: backend code, live site, ops/data) and
   every actionable finding fixed same day (6dbbc26, c94c817):
   - api-key middleware: DB lookups off the event loop, sane cache eviction, per-IP
     junk-key lookup caps (was a 2-worker DoS vector).
   - subscribe: 10 new subscriptions/email/day cap (bbl targets made confirmation
     emails an email-bomb vector against sender reputation).
   - OCA comparison window is calendar-true for gap-month ZIPs (generate_series).
   - editions API: field whitelist (internal keys no longer echo), malformed-file
     degrades instead of 500s, atomic writes; building_alerts state crash-safe,
     watermark only advances when all sends succeed; classification clamped;
     date-fallback escapes.
   - llms.txt says 177 (not 178 — a '99999' sentinel row in neighborhoods inflated
     it) and sources its top-risk list from /api/stats so surfaces can't disagree;
     /map metas and neighborhood-page notes also 177 now.
   - Root crontab weekly_content_brief had NO `cd` — silently broken 11 weeks; fixed
     in the live crontab (note: root crontab, not /etc/cron.d/pulsecities).
   - Neighborhood pages: signal table now scrolls in .table-wrap (was panning the
     whole page on mobile — press-blocking). Methodology source-ids wrap.
   - ES i18n: app shell pulse headers ('pulse' dict group, not 'ui'!), methodology
     modal, block-panel action prompts, operator disclaimer/chips/lot rows,
     /operators sub-line + CTAs. Dicts were translated; elements weren't wired.

## NEEDS MICHAEL (in priority order)

1. **Anthropic API credits are EXHAUSTED** — the map panel's AI read is failing live
   ("credit balance is too low", ai_summary warnings in journal). Top up at
   console.anthropic.com. Only user-facing breakage on the site.
2. **Search Console submission** (10 min; should precede press).
3. **Send the press pitch** (all assets verified; +232% Queens Village arc is the
   strongest cold-open) and cross-post the weekly finding (LinkedIn + Bluesky).
4. **ACRIS email** — drafted in the 2026-07-10 session: thanks DOF, asks whether the
   Open Data ingestion changes also explain DCWP licenses (w7w3-xahh, stalled since
   Apr 24 upstream — verified upstream, not our scraper). ACRIS itself: DOF says
   delayed into next week, then normal schedule.

## Watch (no action unless something reads wrong)

- **Sunday 2026-07-12**: digest 09:00 (first real send since the RESEND_API_KEY fix;
  failed silently May 3–Jul 5, so CHECK digest.log), flips scan 09:30 (quiet =
  expected), restore-test 05:00, ops-health 09:45. Mon 04:15 first OCA cron;
  daily 03:25 building alerts (Michael watches Furman Ave).
- **Memory headroom**: 1GB box + tooling; a full pytest run alongside a browser OOMs.
  Run suite in halves. Swap was 98% during the session, recovered after.
- Cold /api/neighborhoods/top-risk query took 37s once (cached after); index or
  materialize before a press spike if it recurs.

## Open decisions (parked, Michael's call)

- OCA petitions as a 7th weighted score signal (changes all scores + breaks 187-day
  history comparability — deliberate decision or not at all).
- GitHub repo public + method write-up as README; Plausible $9/mo upgrade;
  per-key API rate tiers (when first partner asks); email-bomb double-opt-in if the
  10/day cap ever proves insufficient.

## Facts the next session should not re-derive

- Deploy model: working tree IS production; nginx serves static from disk instantly;
  `systemctl reload pulsecities` for Python changes; deploy branch = main; commits
  are NOT pushed to origin (push blocked by env policy; Michael runs `! git push`).
- Canonical palette: low #3E6B54, moderate #C08B2D, high #F97316, critical #EF4444;
  fills/chips carry palette, low-as-TEXT stays slate. Tripwires pin it.
- app.html i18n: id → t(group, key) map around line ~2050; groups include ui, pulse,
  civic, hero, count_label, signal_short, risk_signal, summary. operator.html uses
  flat data-i18n + t(key). SSR pages carry small page-local i18n dicts.
- The editions file human-approval gate: flip approved:true in
  scripts/eviction_flips_editions.json; homepage + /flips/editions read it live.
