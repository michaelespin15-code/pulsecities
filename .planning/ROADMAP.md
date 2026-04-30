# Roadmap: PulseCities

## Milestones

- ✅ **v1.0 NYC Displacement Intelligence** — Phases 1–7 (shipped 2026-04-16)
- ✅ **v2.0 Public Launch + Operator Intelligence** — Phases 8–10 (shipped 2026-04-30)

## Phases

<details>
<summary>✅ v1.0 NYC Displacement Intelligence (Phases 1–7) — SHIPPED 2026-04-16</summary>

- [x] Phase 1: Data Foundation (3/3 plans) — PostgreSQL + PostGIS schema, Alembic migrations, BBL normalization, 178 ZCTA geometries
- [x] Phase 2: End-to-End Slice (4/4 plans) — DOB permits scraper → displacement score → GeoJSON API → MapLibre choropleth
- [x] Phase 3: Remaining Scrapers (5/5 plans) — 311, ACRIS (3-dataset join, LLC normalization), evictions, DOF, scraper audit log
- [x] Phase 4: Score Engine (3/3 plans) — Six-signal ANHD composite score, per-unit normalization, signal breakdown API
- [x] Phase 5: Hardening & Frontend Polish (4/4 plans) — OS cron, rate limiting, block drill-down, signal breakdown UI, responsive layout
- [x] Phase 6: Product Evolution (6/6 plans) — Sparklines, Pulse feed, renovation-flip detector, EN/ES i18n, DHCR RS + DCWP scrapers
- [x] Phase 7: Pre-Ship Hardening (6/6 plans) — CI, DB constraints, score sanity checks, Pydantic models on all 8 scrapers

See `.planning/milestones/v1.0-ROADMAP.md` for full phase details.

</details>

**v2.0 Public Launch + Operator Intelligence**

- [x] **Phase 8: Production Deployment** — pulsecities.com live over HTTPS with gunicorn + Nginx, systemd auto-restart, runbook
- [x] **Phase 9: Operator Data + API** — DB schema, backfill, and REST endpoints for operator portfolio intelligence
- [x] **Phase 10: Operator UI + Map Layer** — Profile pages, LLC linking from block events, and toggleable operator map layer

## Phase Details

### Phase 8: Production Deployment
**Goal**: pulsecities.com is live, HTTPS-only, production-grade, and survives a reboot without manual intervention
**Depends on**: Phase 7 (v1.0 complete)
**Requirements**: DEPLOY-01, DEPLOY-02, DEPLOY-03, DEPLOY-04, DEPLOY-05
**Success Criteria** (what must be TRUE):
  1. Visiting http://pulsecities.com redirects to https://pulsecities.com and the map loads with a valid TLS cert (no browser warning)
  2. A simulated server reboot brings gunicorn back up automatically via the systemd unit — no SSH required
  3. curl -I https://pulsecities.com/api/neighborhoods returns HTTP 200 with X-RateLimit headers present, confirming per-IP rate limiting is active through Nginx
  4. A rate limit test (61 rapid requests from one IP) returns HTTP 429 on request 61, confirming slowapi sees the real client IP via X-Forwarded-For
  5. The runbook shell script documents the deploy, rollback, and cert-renewal procedures and can be executed by someone with SSH access and no prior context
**Plans**: 3 plans
**UI hint**: yes

Plans:
- [x] 08-01-PLAN.md — Add ProxyHeadersMiddleware to FastAPI app, install and enable pulsecities.service systemd unit with 2 gunicorn workers
- [x] 08-02-PLAN.md — Write Nginx virtual host config (HTTPS redirect, proxy headers, gzip, static caching), provision Let's Encrypt cert, configure certbot auto-renewal
- [x] 08-03-PLAN.md — Write scripts/runbook.sh operational script (deploy/rollback/renew-cert/smoke-test), add tests/test_rate_limiting.py

### Phase 9: Operator Data + API
**Goal**: Operator portfolio data is in the database and queryable via a clean REST API
**Depends on**: Phase 8
**Requirements**: OPAPI-01, OPAPI-02, OPAPI-03, OPAPI-04
**Success Criteria** (what must be TRUE):
  1. curl https://pulsecities.com/api/operators returns a JSON array containing MTEK, PHANTOM CAPITAL, and BREDIF with portfolio_size, borough_spread, and highest displacement score populated
  2. curl https://pulsecities.com/api/operators/mtek-nyc returns a full profile: property list with per-BBL displacement scores, an acquisition timeline, HPD violation counts by class, eviction-then-buy matches, and RS unit counts per property
  3. curl "https://pulsecities.com/api/search?q=mtek" returns results grouped by type with MTEK appearing in the Operators group
  4. All three operator slugs (mtek-nyc, phantom-capital, bredif) resolve correctly and return 404 for an unknown slug like /api/operators/nobody
**Plans**: 3 plans

Plans:
- [ ] 09-01-PLAN.md — operators/operator_parcels migration, backfill from operator_network_analysis.json, wave-0 test scaffold
- [ ] 09-02-PLAN.md — GET /api/operators list + GET /api/operators/{slug} detail with per-BBL displacement, violations, eviction-then-buy, RS units
- [ ] 09-03-PLAN.md — GET /api/search?q= grouped search endpoint returning operators and properties

### Phase 10: Operator UI + Map Layer
**Goal**: Journalists can navigate from the map to a named LLC to a full operator profile and back, entirely within the browser
**Depends on**: Phase 9
**Requirements**: OPUI-01, OPUI-02, OPUI-03, OPUI-04
**Success Criteria** (what must be TRUE):
  1. Navigating to /operators/mtek-nyc renders the operator profile page directly (deep link works); pressing browser Back returns to the previous page without a full reload
  2. The operator profile page displays: a portfolio table with per-property displacement scores, an acquisition timeline chart, HPD violation counts by class, an eviction-then-buy match list, and RS unit counts per property
  3. An LLC name in the block events panel (e.g., "MTEK REALTY LLC") renders as a clickable link that navigates to /operators/mtek-nyc
  4. Toggling the operator map layer shows all operator-controlled parcels colored by operator; clicking any parcel navigates to that operator's profile page; toggling again hides the layer
**Plans**: TBD
**UI hint**: yes

## Progress

| Phase | Milestone | Plans | Status | Completed |
|-------|-----------|-------|--------|-----------|
| 1. Data Foundation | v1.0 | 3/3 | Complete | 2026-04-08 |
| 2. End-to-End Slice | v1.0 | 4/4 | Complete | 2026-04-09 |
| 3. Remaining Scrapers | v1.0 | 5/5 | Complete | 2026-04-10 |
| 4. Score Engine | v1.0 | 3/3 | Complete | 2026-04-11 |
| 5. Hardening & Frontend Polish | v1.0 | 4/4 | Complete | 2026-04-12 |
| 6. Product Evolution | v1.0 | 6/6 | Complete | 2026-04-12 |
| 7. Pre-Ship Hardening | v1.0 | 6/6 | Complete | 2026-04-13 |
| 8. Production Deployment | v2.0 | 3/3 | Complete | 2026-04-28 |
| 9. Operator Data + API | v2.0 | 3/3 | Complete | 2026-04-29 |
| 10. Operator UI + Map Layer | v2.0 | 3/3 | Complete | 2026-04-30 |

## Backlog

### Phase 999.1: Homepage UX — Address-First Entry with Live Stat Chips (BACKLOG)

**Goal:** Address-first homepage: map renders in background behind hero overlay, centered search bar as hero, 3 editorial stat chips (LLC transfers 30d, top-risk ZIP, evictions 30d), cascade animation, leaderboard with raw count + citywide percentile, h1-as-home-button, no em dashes.
**Requirements:** TBD
**Plans:** 3/4 plans executed

Plans:
- [x] 999.1-01-PLAN.md — New GET /api/stats endpoint + add raw_count/percentile_tier to top-risk endpoint
- [x] 999.1-02-PLAN.md — Hero overlay HTML/CSS structure, show/hide state machine, i18n keys, subscribe em dash fix
- [x] 999.1-03-PLAN.md — initHeroStats() animation, hero search handler, ZIP cards, hover sparkline/tooltips
- [ ] 999.1-04-PLAN.md — Leaderboard _buildTopRiskItem() update + human-verify checkpoint

### Backlog: DOF assessments scraper cleanup

`dof_assessments` scraper fails nightly with:
  `ON CONFLICT DO UPDATE command cannot affect row a second time`

Root cause: duplicate rows in the DOF batch share the same conflict key,
causing PostgreSQL to reject the upsert. Annual scraper so not urgent —
data was loaded correctly on last full run (2026-04-12, 858k parcels).

Fix when convenient: deduplicate within the batch before upsert, or
switch to `on_conflict_do_nothing` if duplicate rows are expected from
the source.
