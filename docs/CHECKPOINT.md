# PulseCities checkpoint, 2026-07-10 — homepage redesign PUBLISHED

The preview design is live at / as of commits `a7465c3`..`d945764`. (Prior checkpoint,
the pre-publish handoff, lives in git history of this file.)

## What shipped

- **Homepage**: preview ported to index.html in full. Split hero (headline+search left,
  live map card right with legend), evidence docket, three-module row, unified footer.
  SEO head, JSON-LD, and Plausible events (`Search`, `Explore Map`) carried over. i18n is
  the two-dict en/es pattern; API-rendered strings translate and dates localize on toggle.
- **Docket feed**: `/api/flips/editions/latest` serves the strongest arc of the newest
  `approved: true` edition in `scripts/eviction_flips_editions.json`, enriched with
  neighborhood and borough. The verified Furman Avenue arc is the markup fallback until
  the first edition is approved (first scan lands Sunday 2026-07-12, 09:30 cron;
  Michael flips `approved` after review).
- **Muted green palette** on every tier surface: map fill + legend, methodology dots,
  og cards, ops distribution, brief chips, SSR `_tier_info`/`_idx_color`. Low #3E6B54,
  moderate #C08B2D, high #F97316, critical #EF4444. Low-as-text stays slate for contrast.
  briefs.py tier thresholds were drifted (76/56) and now match canonical 85/67/34.
  Tripwire tests pin the palette across app.html, index.html, and SSR routes.
- **Assets**: teaser (`map-preview*` jpg/webp, both sizes) and `og-image.png` regenerated
  from the recolored live map, homepage srcset at `?v=3`.
- **Footer**: seven inline SSR footers collapsed into `_FOOTER_HTML` in
  api/routes/frontend.py, carrying the full static-page link set.
- **SEO/AI**: all 233 sitemap URLs verified 200 (before and after the SSR changes).
  llms.txt regenerated from live data and scheduled nightly at 03:20 (the generator was
  never in cron, so the published file had stale scores). robots.txt already sound.

## Verified

Full suite 863 passed / 2 skipped. Desktop + mobile homepage screenshots clean, zero
console errors on / and /map, ES toggle spot-checked live (including dynamic strings),
search hands off to the map (`/map?q=11216` resolved to the neighborhood view), gunicorn
reloaded, editions endpoint answering `{"week":null,"arc":null}` until Sunday.

## Open

- **/preview retirement**: still live and noindexed; Michael decides when to drop the
  nginx location and file. Note it still references `preview-palette-{a,b,current}.jpg`.
- **Email tier colors**: weekly_digest.py still uses the old bright green/gold
  (#22c55e/#eab308) for tier chips. Left alone deliberately: the email design shipped
  2026-07-09 and dark-bg contrast for #3E6B54 text needs a design pass, not a blind swap.
- **Sunday 2026-07-12 firsts** (all expected, none alarms): digest 09:00, flips scan
  09:30 (17 seed arcs, writes the first edition), restore-test 05:00, ops-health 09:45.
- Backlog: memory `project_next_steps.md`.
