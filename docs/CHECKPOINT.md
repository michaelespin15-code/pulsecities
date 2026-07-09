# PulseCities checkpoint, 2026-07-09 (evening)

Working state after the July 9 email-and-radar session. Everything under
"Shipped" is live on production and covered by the test suite (833 passing).
One commit may be unpushed; check `git status` / `git log origin/main..main`.

## Shipped and live this session

- **Speculation Radar** (`/radar` + `/api/radar`): one LLC taking the deed on
  3+ distinct buildings in the same ZIP within 90 days, detected from ACRIS
  before entity resolution. Found real clusters at launch, including a
  4-building $14.6M same-day run in Park Slope. Noise filter shared with
  Flip Watch; nav links on landing + Flip Watch pages.
- **Weekly digest, case-file redesign.** The site is the dark instrument; the
  email is the printed record it produces. Paper sheet, registered masthead
  (issue no. = ISO week), filed-date line, tier stamp, dotted-leader ledgers,
  "The Record" docket, hidden preheader. Per-ZIP 90-day pulse trace PNG from
  `/og/spark/{zip}.png` (special key `nyc` = citywide average; never 404s).
- **AI narrative in the digest** (`scripts/digest_narrative.py`): one grounded
  paragraph per ZIP per run plus a citywide edition, rendered as the serif
  lede, labeled AI-written. Fails to None so the email always sends; one call
  per key per run, failures cached.
- **Citywide digest rebuilt as the city's week**: biggest 7-day score movers
  with signed deltas, week counts vs 8-week baselines, fresh Speculation
  Radar clusters (section hides when empty), AI lede, citywide trace, CTA to
  `/this-week`. No longer a static standings table.
- **Ingest-time counting fix (correctness).** Every weekly count window in
  the digest now keys on `created_at` (newly on the record), not event dates.
  City feeds lag, so event-dated weeks always undercounted and the narrative
  reported a fake quiet week. Labels and grounding say "records added".
- **Welcome emails rebuilt twice into final form**: paper case-file note
  ("Watch opened <date>" file line), zero images/buttons, plain-text part,
  one-click unsubscribe in body + List-Unsubscribe header (digest sends carry
  the header too), em-dash-free subjects. Driven by a real Gmail Promotions
  misfile of the old dark-card version.
- **Email pipeline proven live**: real sends delivered to two inboxes; all
  subscribers intact and confirmed. First automated send Sunday 2026-07-12
  09:00 UTC via `/etc/cron.d/pulsecities`, logs to
  `/var/log/pulsecities/digest.log`.
- **AI cost controls**: `PULSE_AI_MODEL` env var flips every narrative
  surface's model (default `claude-opus-4-8`); per-call token usage logged as
  "narrative usage" lines. Current all-in AI spend is under $1/month.

## To complete

1. **Add the DMARC record (Michael, at Cloudflare).** DNS → Records → Add:
   TXT, name `_dmarc`, content `v=DMARC1; p=none;`. SPF + DKIM already pass
   via Resend; DMARC is the missing third of Gmail's sender guidelines.
2. **Confirm Sunday's digest** (2026-07-12): digest.log shows sent>0, the
   narratives read honestly, and note Primary vs Promotions placement.
3. **Operator digest restyle** — the last email surface still on the old
   dark template; move it onto the paper system (`_WELCOME_SHELL`-style
   tokens live at the top of `weekly_digest.py`).
4. **ACRIS feed watch, now visible in the emails**: 0 new deed records this
   week vs ~1,363 typical. When NYC Open Data resumes, confirm the watermark
   advances and Flip Watch + Radar pick up new deeds.
5. **Map sidebar hierarchy pass.** Still deferred: taste-level work on the
   most-used surface; deserves a screenshot-iterate loop. Order to aim for:
   score, AI read, what changed, evidence, actions.
6. **Pipeline failure alerting.** `scheduler.alerts` still logs "no webhook
   configured"; scraper breakage is silent until someone reads logs.
7. **Threshold review after the ingest-time switch.** The per-ZIP absolute
   thresholds (HPD_ABS etc.) were tuned against event-dated counts;
   ingest-time counts run higher, so more weeks will trigger sends. Watch a
   couple of Sundays and retune if every week fires.
8. **Michael's own subscriptions**: currently citywide only (clicked a live
   unsubscribe link in a test sample). Re-subscribe to 11216 if wanted.
9. Longer-standing: entity resolution step 3 (NY DOS as future source),
   GitHub repo public + restore repo links, Plausible upgrade on traction.

## Feature ideas, ranked by innovation-per-effort

1. **Map summary upgrade**: feed the AI read the 90-day trajectory and
   borough median so it interprets instead of restating the chips. Small
   change to `_build_facts` in `api/routes/ai_summary.py`.
2. **Building watchlist.** Follow a BBL the way you follow an operator;
   subscriber infra already generalized. Tenant-first retention.
3. **Landlord report card.** Per-operator violations-per-building and
   eviction-filing rate vs borough median, on profiles and briefs.
4. **Radar on /this-week + digest cross-links.** The radar feeds the
   citywide email already; surface it on the weekly page too.
5. **Public API + docs page.** Keyed access to scores/operators/flips/radar.
   B2B groundwork, employer-legible artifact.
6. **Embeddable replay widget.** Iframe-able mini choropleth for newsrooms.
7. **Housing-court lead time** (OCA/NYSCEF filings): moves the eviction
   signal weeks earlier, which is the product thesis.

## Operating notes

- Deploy model: working tree is production. `systemctl reload pulsecities`
  after any Python change (SSR pages and gunicorn workers cache in-process;
  give workers a few seconds to recycle); `nginx -t && systemctl reload
  nginx` after config changes.
- Push from the terminal: `git push origin main` (agent pushes are blocked
  by the environment policy).
- Digest smoke test: `python -m scripts.weekly_digest --dry-run`, or
  `--email you@x.com` for a targeted real send. `--email` filters existing
  subscribers; it does not send to arbitrary addresses.
- Tier bands are canonical at 34/67/85; tripwire tests fail on drift.
- Sample emails sent during testing carry live unsubscribe tokens.
