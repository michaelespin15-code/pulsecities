# PulseCities checkpoint, 2026-07-09

Working state after the July 8-9 build sessions. Everything below "Shipped"
is live on production and covered by the test suite (787 passing).

## Shipped and live

- Flip Watch (`/flips`), LLC network graph on operator profiles, grounded AI
  read in the map panel (claude-opus-4-8, credits active), six-month score
  replay on the choropleth (`/map?replay=1`), weekly review page
  (`/this-week`), embeddable ZIP badges (`/badge/{zip}.svg`), operator OG
  share cards, operator-follow email alerts, since-your-last-visit banner.
- Brand unified across all pages (pulse mark + Bricolage Grotesque display
  face), hero redesigned around the tagline, canonical tier bands
  (34/67/85) enforced on every surface and pinned by tripwire tests.
- Score history verified: single-run backfill with the current algorithm,
  junk-ZIP rows purged, Jan 30 step confirmed as faithful batch data.
- Email pipeline repaired end to end: subscriptions born confirmed, digest
  cron now has its Resend key, operator digest block added. First real
  digest send is Sunday 2026-07-12 09:00 UTC. Verify it with:
  `python -m scripts.weekly_digest --dry-run` (or `--email you@x.com` for a
  targeted real send).

## To complete

1. **Map sidebar hierarchy pass.** Deliberately deferred: taste-level work
   on the most-used surface; deserves a screenshot-iterate loop. Order to
   aim for: score, AI read, what changed, evidence, actions.
2. **Pipeline failure alerting.** `scheduler.alerts` logs
   "no webhook configured" on scraper failures, so breakage is silent
   until someone reads logs. Wire a webhook or plain email alert.
3. **Confirm Sunday's digest actually sends** (first live test of the
   repaired path) and that the operator block renders in a real client.
4. **ACRIS feed watch.** Upstream paused since 2026-05-27; when NYC Open
   Data resumes, confirm the watermark advances and Flip Watch picks up
   new deeds.
5. **Entity resolution step 3** (operator party addresses) still blocked on
   ACRIS having no address data; NY DOS corporate filings are the future
   source.
6. **GitHub repo public** + restore repo links on methodology/about;
   Plausible Starter upgrade when traction warrants; press kit page.

## Feature ideas, ranked by innovation-per-effort

1. **Speculation Radar.** Flag any LLC that takes 3+ deeds in one ZIP
   within 90 days, before entity resolution even names the cluster. Early
   warning nobody else has; one SQL view + a feed section on /this-week.
2. **Building watchlist.** Follow a BBL the way you follow an operator;
   subscribers table and digest infra are already generalized. Tenant-first
   retention.
3. **AI weekly narrative.** A grounded paragraph on /this-week written from
   that week's exact numbers, same guardrails as the AI read. Turns the
   weekly page into something quotable.
4. **Landlord report card.** Per-operator violations-per-building and
   eviction-filing rate vs borough median, on profiles and briefs.
   Accountability metric journalists can cite directly.
5. **Public API + docs page.** Keyed access to scores/operators/flips.
   Groundwork for the B2B tier and makes the project legible to technical
   employers.
6. **Embeddable replay widget.** Iframe-able mini choropleth replay for
   newsroom embeds; badges already prove the embed distribution path.
7. **Housing-court lead time.** OCA/NYSCEF filings run weeks ahead of
   executed evictions; adding them would move the eviction signal earlier
   in the timeline, which is the whole product thesis.

## Operating notes

- Deploy model: working tree is production. `systemctl reload pulsecities`
  after any Python or HTML change (SSR templates are cached in-process);
  `nginx -t && systemctl reload nginx` after config changes.
- Push from the terminal: `git push origin main` (agent pushes are blocked
  by the environment policy).
- Tier bands are canonical at 34/67/85; the tripwire tests in
  `tests/test_frontend_routes.py` fail if any surface drifts.
