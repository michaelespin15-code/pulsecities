# PulseCities checkpoint, 2026-07-10 — homepage redesign preview, pre-publish

Session ended mid-redesign by design: preview approved in spirit, publishing waits on a few
more runthroughs. This file is the handoff. (Prior checkpoint, 2026-07-09 email/radar
session, lives in git history of this file.)

## Where things stand

**/preview is the finished design candidate** (noindex, unlinked; live homepage untouched).
Commits: `dc696c8` (page + palette renders + footer guard), `cf656c8` (split hero after
Michael flagged the map below the fold), `9416606` (More menu, restored nav destinations,
working EN/ES sharing the live `pc-lang` key).

Decisions locked:
- **Palette: muted green (option A)** — low `#3E6B54`, moderate `#C08B2D`, high `#F97316`,
  critical `#EF4444`. Michael picked it from three real MapLibre renders
  (`frontend/preview-palette-{a,b,current}.jpg`).
- **Structure:** split hero (headline+search left, clickable map card right with legend) →
  evidence-docket featured finding (Furman arc, ACRIS doc IDs) → 3-module row (rankings /
  operators / live signals, reserved heights) → unified footer. Nav: Map, Operators, Press,
  Methodology, More (Flip Watch, Radar, Neighborhoods, About, Status), EN/ES.

## Next: runthroughs, then publish

Michael wants more passes over /preview before promoting. When he says publish:

1. **Port preview → index.html**: full page swap, keeping /preview until verified. The preview
   i18n dict is page-local; merge into index.html's larger dict pattern. No Tailwind classes
   were used in preview.html (inline styles) — if the port adds any to HTML files that use
   Tailwind, run `npm run build:css` and commit `tailwind.min.css`.
2. **Docket feed**: wire the featured finding to approved editions in
   `scripts/eviction_flips_editions.json` (first edition lands Sunday 2026-07-12 from the
   09:30 flips cron; `approved: false` until Michael reviews). Hardcoded Furman arc is the
   fallback when no approved edition exists.
3. **Map palette**: apply muted green to `app.html` — the `neighborhood-fill` step expression
   (~line 2120), the legend chips (~line 440), and check other tier surfaces (commit a7410ee
   "align every client tier surface to the canonical bands" maps where tier colors live).
   Also `scoreColor()` in index.html if ported, and badge/og tier colors if any render green.
4. **Regenerate visual assets**: map teaser (`map-preview*.{jpg,webp}`) and `og-image.png`
   from the recolored map; bust the 30-day immutable cache with a new `?v=` query (currently
   v=2 per e867c67).
5. **Footer constant extraction**: collapse the 7 inline SSR footers in
   `api/routes/frontend.py` (+ static pages) into one shared block.
   `tests/test_footer_consistency.py` guards the link set either way.
6. **Verify**: full suite, mobile + desktop screenshots of / and /map, ES toggle spot-check,
   Playwright console clean, then retire or keep /preview.

## Also true as of this checkpoint

- Press outreach assets ready: /press live, paper-trail CSVs on operator pages, pitch numbers
  verified (MTEK 42/42/11, PHANTOM 74/75/39, BREDIF 67/68/4). Pitch send is Michael's move.
- Sunday 2026-07-12 firsts: digest 09:00 (verified via dry-run), flips scan 09:30 (will email
  17 seed arcs), restore-test 05:00 (manual run passed with exact parity), ops-health 09:45.
- Eviction dedupe done (18,402 duplicate rows purged), scores + 187-day history recomputed,
  DB-driven guards in tests. Suite ~848 passing.
- Full backlog: memory `project_next_steps.md`. OCA petition data is ZIP-only (researched,
  not parcel-joinable) — don't re-litigate.
