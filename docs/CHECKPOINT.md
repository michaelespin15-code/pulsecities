# PulseCities checkpoint, 2026-07-11 (early morning) — full audit #2 closed

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
