# PulseCities Live-Site Audit

- **Target:** https://pulsecities.com (production)
- **Method:** Headless Chromium via Playwright (Python). Script: `scripts/audit/live_site_audit.py`. Raw capture: `scripts/audit/live_site_audit_result.json`.
- **Run at:** 2026-06-23 01:13 UTC
- **Scope:** Read-only. No application code changed.

## Headline finding (supersedes the brief)

**The site is in a total backend outage. Every dynamic route returns HTTP 500.** This is not a slug-vs-root routing bug and not a classification-gate leak. The production API cannot reach its database, so every endpoint that opens a DB session — `/api/health` included — throws an unhandled exception and returns Starlette's default `Internal Server Error` (text/plain, 21 bytes), proxied through nginx with a `500`.

The operator-detail hypothesis in the brief (page requests by root, API keys by slug → 404) **could not be reached**: no request got far enough to 404. Everything 500s upstream of that logic. See "Once the DB is back" for the code-level read on that hypothesis.

What still serves: the five static HTML shells (`/`, `/map`, `/methodology`, `/about`, `/status`) return 200 because they are file responses with no DB dependency. They render their chrome, then every data fetch inside them 500s, so they sit on "Loading…", empty cards, "No score computed yet", and "Status is temporarily unavailable."

A journalist hitting this site right now sees: a working landing shell with no live numbers, a map with no choropleth and no scores, and a plain-text "Internal Server Error" on every operator page, the operators directory, and every neighborhood deep link.

---

## Per-URL results

Legend: **PASS** = expected content rendered; **FAIL** = did not. "Doc status" is the status of the page document itself.

### A) Operator detail pages

| URL | Doc status | Console errors | Failed requests | Render |
|---|---|---|---|---|
| /operator/MTEK | 500 | 1 (`Failed to load resource … 500`) | the document itself = 500 | **FAIL** — body is plain `Internal Server Error`; `operator.html` never loads, so no `/api/operators/*` call is ever fired |
| /operator/PHANTOM | 500 | 1 | document = 500 | **FAIL** — same |
| /operator/BREDIF | 500 | 1 | document = 500 | **FAIL** — same |
| /operator/TOWNHOUSE | 500 | 1 | document = 500 | **FAIL** — same |
| /operator/MELO | 500 | 1 | document = 500 | **FAIL** — same |

Captured API call per page: **none.** The 500 happens server-side in the `operator_page` SSR handler (`api/routes/frontend.py:581`) at its first DB query, before any HTML is returned. Because the HTML document is a 500, the browser never parses `operator.html` and never runs the client `fetch('/api/operators/…')` logic. Stats grid and acquisitions table are absent (not even the "—" placeholder renders). The slug-vs-root question is therefore unverifiable from the network right now.

### B) "False-positive" operators (suppression-gate cases)

| URL | Doc status | Result |
|---|---|---|
| /operator/OCEANVIEW | **500** | FAIL — plain `Internal Server Error`. (Brief predicted a 500 here; it is a 500, but for the site-wide outage reason, not a gate-specific one.) |
| /operator/RIDGEWOOD | 500 | FAIL — same |
| /operator/VALLEY | 500 | FAIL — same |
| /operator/COMMUNITY | 500 | FAIL — same |
| /operator/METROPOLITAN | 500 | FAIL — same |
| /operator/TOORAK | 500 | FAIL — same |
| /operator/JOVIA | 500 | FAIL — same |
| /operator/BATTALION | 500 | FAIL — same |
| /operator/ARION | 500 | FAIL — same |
| /operator/HABIB | 500 | FAIL — same |

**None resolved, none 404'd, all 500.** Whether the classification gate actually suppresses these (minimal page vs full profile vs 404) cannot be tested until the database is reachable. The gate logic itself is reviewed below.

### C) Operators directory — /operators

- **Doc status: 500.** Body: `Internal Server Error`.
- Rows rendered: **0** (`.op-row` count = 0). Links: none. Count label: none.
- **FAIL.** `operators_directory` (`api/routes/frontend.py:689`) queries `operators` for the slug map and 500s. No directory list to inspect, so bank/insurer/duplicate-cluster flagging is deferred to the code review below.

### D) Core journalist / tenant flows

| URL | Doc status | Render | Notes |
|---|---|---|---|
| / | 200 | **PARTIAL FAIL** | Shell + hero render. Stat chips stuck (2 "Loading" elements remain). Failed: `/api/status`, `/api/stats`, `/api/neighborhoods/top-movers?limit=8`, `/api/operators/top?limit=3` — all 500. Operator cards do not populate. No watchlist section detected. |
| /map | 200 | **PARTIAL FAIL** | MapLibre canvas renders. Choropleth cannot paint: `/api/neighborhoods/top-risk`, `/api/stats`, `/api/schedule`, `/api/neighborhoods` all 500. 4 console errors. |
| /map?q=11216 | 200 | **FAIL** | Autosearch fires and the panel opens, but `/api/neighborhoods/11216/score` returns 500. Panel shows "No score computed yet"; no signal breakdown. 6 console errors incl. `Score fetch returned 500`. |
| /neighborhood/11216 | **500** | **FAIL** | SSR `neighborhood_page` (`api/routes/frontend.py:395`) 500s on its DB query. Plain `Internal Server Error`; deep link does not route into the app. |
| /status | 200 | **PARTIAL FAIL** | Page renders but shows "Status is temporarily unavailable" — `/api/status` 500s. No per-source freshness dates could be captured (the data they come from is down). Degrades gracefully, at least. |
| /methodology | 200 | **PASS** | Clean. 0 console errors, 0 failed requests. Static content only. |
| /about | 200 | **PASS (content)** | Renders, h1 "About PulseCities". One failed request: `/api/status` (the shared status widget) 500s, but page content is intact. |

**Search box (address + landlord "MTEK"):** the search path depends on the DB-backed search API, which is part of the same outage. The `/map?q=11216` autosearch above is the live demonstration — the score fetch returned 500 and the panel rendered "No score computed yet." Address and landlord searches will fail identically until the database is restored. Not separately PASS-able right now.

---

## Root causes

### 1. Primary: production database is unreachable → total 500 outage (P0)

Every failing route shares one trait: a FastAPI `Depends(get_db)` dependency.

- `models/database.py` builds the engine once at import with `pool_pre_ping=True`. `get_db()` calls `SessionLocal()` (lazy — no connection yet) and yields it. The connection is established on the **first query execution** inside each handler.
- When the database is down/unreachable/refusing auth, that first `db.execute(...)` raises. None of these handlers wrap their queries, so the exception propagates to Starlette's `ServerErrorMiddleware`, which returns the bare `Internal Server Error` (text/plain, 21 bytes) we see on the wire.
- `/api/health` (`api/routes/health.py:33`) is itself `Depends(get_db)` and runs a query, so the health check fails with the same fault it is supposed to detect. It cannot be used as an up signal.
- nginx is up and proxying (it returns the app's 500, not a 502), so the **uvicorn app process is alive** — this is a DB-connectivity failure (DB process down, connection refused, exhausted pool, bad/rotated `DATABASE_URL` credentials, or disk-full Postgres), not an app crash.

This single fault explains 100% of the 500s in sections A, B, C, and the `/neighborhood/...` and all `/api/...` failures in D.

### 2. The operator slug-vs-root question (code-level, since the network can't answer it)

Reading the actual code, the brief's hypothesis is **largely already handled** and would *not* be the failure mode for the section-A operators once the DB is back:

- `frontend/operator.html:975-1016`: the client reads the path param. If it is not slug-format (e.g. `MTEK`, uppercase), it does **not** call `/api/operators/MTEK`. It fetches the list `/api/operators/`, finds the row by `operator_root`, then fetches `/api/operators/{match.slug}`. So root-name URLs self-resolve to the slug.
- The detail API `get_operator_profile_by_slug` (`api/routes/operators.py:238`) keys strictly on `slug` and 404s otherwise. The client never hits it with a root, so no slug/root 404 occurs for any operator that appears in the list.
- **The real fragility:** the list at `/api/operators/` is filtered to `WHERE operator_class = 'operator'` (`operators.py:166`). Any section-A operator whose row is missing or not classified `'operator'` will be absent from the list, the client `list.find(...)` returns nothing, and the page shows "Operator not found." That is a data/classification dependency, not a URL-keying bug. It must be re-checked per operator (MTEK/PHANTOM/BREDIF/TOWNHOUSE/MELO) once the API responds.

### 3. The classification / suppression gate (where it lives, who calls it)

The gate is enforced in **three** places, and the coverage is actually good:

- **List route** `/api/operators/` (`operators.py:148`): `WHERE operator_class = 'operator'`. Banks/GSEs/government/HDFC never enter the directory data.
- **Detail route** `/api/operators/{slug}` (`operators.py:261`): re-checks `operator_class != 'operator'` → 404, so signal data cannot leak even on a direct API hit.
- **SSR operator page** `/operator/{root}` (`frontend.py:595, 615`): blocks `OPERATOR_NOISE_ROOTS`/`OPERATOR_NOISE_SLUGS` with a 404, and renders a minimal "Not an operator" page for any non-`operator` class.
- **SSR directory** `/operators` (`frontend.py:702`): `WHERE operator_class = 'operator'` for the slug map, so only operators get rows.

`OPERATOR_NOISE_ROOTS` (`operators.py:44`) hardcodes `ICECAP, ICE, BROAD, BROADVIEW, ARBOR, STANDARD, SYMETRA, COMMUNITY, OCEANVIEW, VALLEY`. Note the brief's suspect list also includes `RIDGEWOOD, METROPOLITAN, TOORAK, JOVIA, BATTALION, ARION, HABIB, SYMETRA, STANDARD` — these are **not** in the hardcoded noise set, so their suppression depends entirely on their `operator_class` being set correctly in the DB by `scripts/classify_operators.py`. If any of them is still classified `'operator'`, it will surface once the site is back. **This is the thing to verify the moment the database is reachable** — it cannot be confirmed now because every classification query 500s.

---

## Fix list (prioritized — journalist-first)

**P0 — Restore the database / dynamic backend.** Nothing else on this list is observable until this is fixed. The entire site's data layer is down: all operator pages, the directory, all neighborhood deep links, search, the map choropleth, home stat chips, and the status page.
- Check Postgres is running and reachable from the app host; verify `DATABASE_URL` credentials/host haven't rotated; check for connection-pool exhaustion and Postgres disk space.
- Confirm fix with `curl -s -o /dev/null -w '%{http_code}' https://pulsecities.com/api/health` → expect 200.

**P1 — Make `/api/health` independent of a full DB query, and stop returning bare 500s on DB failure.** Health currently fails for the same reason it should be detecting, so monitoring is blind. Handlers leaking Starlette's default `Internal Server Error` to journalists is also a bad look. (Recommendation only — not applied. A DB-outage path that returns a styled 503 "temporarily unavailable" page, like `/status` already does, would be the journalist-safe behavior.)

**P2 — Re-run the section-A operator-detail check against the restored API.** Confirm MTEK / PHANTOM / BREDIF / TOWNHOUSE / MELO each (a) appear in `/api/operators/`, (b) resolve root→slug, (c) return a profile with real stats and acquisition rows. The slug-vs-root path is sound in code; the risk is a missing or mis-classified `operators` row dropping an operator out of the list.

**P3 — Re-run the section-B/C suppression check against the restored API.** Verify `RIDGEWOOD, METROPOLITAN, TOORAK, JOVIA, BATTALION, ARION, HABIB, SYMETRA, STANDARD` are classified to a non-`operator` class (they are absent from the hardcoded `OPERATOR_NOISE_ROOTS`, so only `operator_class` keeps them out). Re-run `scripts/classify_operators.py` if any still surface, and confirm PHANTOM vs PHANTOM CAPITAL and ICE vs ICECAP are not duplicated in the directory.

**P4 — Add an uptime check that fails when dynamic routes 500.** The static shells return 200 during a full data outage, so a naive "is `/` up?" probe would have shown green through this entire incident. Probe `/api/health` (once P1 makes it meaningful) and one DB-backed SSR route (e.g. `/operators`).

---

## Notes on method / confidence

- Each assertion checked specific DOM state (stat element text, `#acq-rows` row count, `.op-row` names, `content`/`error-state` visibility, MapLibre canvas presence), not just status codes.
- The `getComputedStyle … parameter 1 is not of type 'Element'` eval errors in the raw JSON are **the audit script** probing for `#content`/`#error-state` on pages where those elements don't exist (because the document is a 500 shell). They are an artifact of the harness, not a site defect.
- Findings were cross-checked with direct `curl` (status + headers + body) to confirm the 500s originate from the app (text/plain `Internal Server Error`, `server: nginx/1.18.0`), not from the browser or a CDN.
