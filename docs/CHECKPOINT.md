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
  menu, EN + ES. It was otherwise an orphan (sitemap-only).

**Remaining growth tasks (pending in task list, NOT started):**
- #2 Plausible on the other SSR money pages + full funnel. Deferred: 12 SSR page
  heads each build their own <!DOCTYPE>; only 1 (displacement) has Plausible.
  operator.html/app.html/index.html already load it; neighborhood/flips/radar/
  operators-directory/borough/week pages do NOT. A shared `_PLAUSIBLE` head const
  injected per page (or a guarded HTML-response middleware) is the fix; it's a
  multi-edit job, do it as its own unit.
- #3 watch-your-block CTA + per-page share/OG. #4 per-ZIP biggest-flips pages
  (WATCH: avoid thin/doorway pages — only emit for ZIPs with real flips, or fold
  as a section into existing neighborhood pages). #5 sitewide SEO pass (footer
  is test-enforced consistent across static + SSR: adding /displacement to
  _FOOTER_HTML means updating every static page footer too).

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
