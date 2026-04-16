# Project Research Summary

**Project:** PulseCities
**Domain:** Civic intelligence map — displacement risk, property ownership, and public records for NYC
**Researched:** 2026-04-07
**Confidence:** HIGH

## Executive Summary

PulseCities is a civic data intelligence product: a map that synthesizes five streams of NYC public records into a per-block displacement risk score. The domain is well-understood — NYC Open Data, ACRIS, and DOF are stable, documented, heavily scraped by civic technologists — and the recommended approach is a layered monolith that separates data ingestion (scrapers), storage (PostgreSQL + PostGIS), API (FastAPI), and browser client (MapLibre GL JS). The stack is already committed; the primary implementation question is not what to build with but in what order and what guard-rails to put in place before writing the first scraper.

The biggest structural risk is building scrapers before the data model is solid. BBL normalization across NYC data systems is a known failure mode that produces silent join failures at the score computation layer. Alembic migrations and GeoAlchemy2 — both absent from the current `requirements.txt` — must be installed and configured before any schema or scraper is written. The second-biggest risk is scoring bias: normalizing signals per residential unit (not raw counts) is required before any score is meaningful. Data desert bias will make Midtown look like a displacement hotspot on raw counts, which destroys trust with the primary user base of journalists and tenant organizers.

The recommended build order is a strict end-to-end slice before breadth: one working scraper, one score stub, one GeoJSON endpoint, one choropleth on the map. This validates the full pipeline — BBL normalization, PostGIS geometry storage, score computation, GeoJSON serialization, MapLibre rendering — against real data before five parallel scrapers are committed. Breadth (remaining four scrapers, composite score formula) follows once the slice is proven.

---

## Key Findings

### Recommended Stack

The backend stack is locked and correct: Python 3.11, FastAPI 0.135.3, SQLAlchemy 2.0 (ORM only — no raw SQL), PostgreSQL 14 + PostGIS 3.x, psycopg2-binary, requests for scraper HTTP calls, system cron for nightly scheduling. The frontend is locked as vanilla JS + MapLibre GL JS + DaisyUI, all loaded from CDN with no build step. DigitalOcean NYC3 VPS with Nginx is the hosting target.

Four libraries are missing from `requirements.txt` and are blocking: **Alembic** (schema migration), **GeoAlchemy2** (PostGIS geometry columns in SQLAlchemy ORM), **Shapely** (GeoAlchemy2 → GeoJSON serialization), and **tenacity** (HTTP retry/backoff for scrapers). Two additional libraries should be added before public exposure: **slowapi** (FastAPI rate limiting) and eventually **anthropic** (deferred to v2 for AI summaries). The Python venv is currently running 3.11.0rc1 — it must be rebuilt against 3.11 final or 3.12 before any production deployment. MapLibre and DaisyUI CDN URLs must be pinned to specific versions before the frontend scaffold is finalized.

**Core technologies:**
- **FastAPI 0.135.3:** API layer — async-native, Pydantic v2 integration, automatic OpenAPI docs
- **SQLAlchemy 2.0:** ORM — unified async API, declarative models; raw SQL forbidden per spec
- **PostgreSQL 14 + PostGIS 3.x:** System of record — ACID storage plus geospatial query capability (ST_Within, ST_Intersects, point-in-polygon)
- **GeoAlchemy2** (missing): PostGIS geometry columns in SQLAlchemy models — required before any model with geometry is written
- **Alembic** (missing): All schema changes must flow through migrations — must be initialized before the first model is created
- **Shapely** (missing): Converts GeoAlchemy2 geometry → Python dict → GeoJSON for API responses
- **tenacity** (missing): Retry/backoff for all scraper HTTP calls to Socrata and ACRIS
- **MapLibre GL JS:** WebGL map renderer — open-source, no API key, handles NYC-scale vector data
- **requests 2.33.1:** All outbound HTTP calls to NYC Open Data, ACRIS, DOF — simple, synchronous, correct for cron scrapers
- **System cron:** Nightly scraper scheduling at 2am UTC — no external dependency, not coupled to the API process

### Expected Features

The core value proposition — a 1–100 displacement risk score per block — is the only non-negotiable feature. Everything else either serves it or is premature.

**Must have (table stakes):**
- Interactive map with neighborhood overlays and color-coded risk choropleth — without this there is no product
- Displacement risk score per block (1–100) — the stated core value; every other feature serves it
- DOB building permits per block — leading indicator of displacement; simpler Socrata API; recommended as the first scraper
- Eviction filing data per block — primary displacement signal; users will specifically look for it
- 311 complaint data per block — most-searched NYC civic dataset
- ACRIS deed transfer data with LLC detection — reveals who is buying; shell companies are a known displacement pattern
- DOF property assessment records — tax/assessment spikes precede rent pressure; also required to compute per-unit normalization
- Neighborhood-level summary view — users navigate by neighborhood, not block
- Block-level drill-down on click — journalists and organizers need parcel-level specificity
- Data freshness indicator per signal — stale data erodes trust; the eviction dataset has a known 2–4 week reporting lag

**Should have (differentiators):**
- LLC ownership chain visualization — most tools show owner name; PulseCities can show the shell company network (requires graph traversal across ACRIS; high complexity, high journalist value — defer to Phase 3 or v1.1)
- Per-scraper score breakdown in the UI — shows which signals are driving the score; transparency builds trust
- Composite score (not raw data dump) — the synthesis is the product; competitors show raw records

**Defer (v2+):**
- AI neighborhood summaries — adds latency, cost, and hallucination risk before the data foundation is proven
- Sentinel-2 satellite change detection — separate GeoTIFF engineering track; civic records alone tell the story
- Time-series score view — requires historical snapshots; can't build until data has been accumulating for months
- Neighborhood comparison UI — easy to build but only useful once multiple neighborhoods have real data
- User accounts, mobile app, Chicago/Los Angeles expansion — all explicitly out of scope per PROJECT.md

### Architecture Approach

PulseCities uses a layered monolith with hard separation between concerns: scrapers write to the database, the API reads from it, the frontend reads from the API. No scraper logic in API routes. No API calls in scrapers. No database access in the frontend. Displacement scores are pre-computed nightly by a scoring engine that runs after all scrapers complete — the FastAPI route is a single SELECT with no aggregation on the hot path. The scheduler is a standalone cron process, not embedded in the FastAPI server process, ensuring scraper failures don't bring down the API.

**Major components:**
1. **`scrapers/`** — One module per data source with a consistent `ingest(since)` interface; normalize BBL and geometry at ingest time; upsert on BBL + event date
2. **`models/`** — SQLAlchemy ORM models with GeoAlchemy2 geometry columns; Pydantic response schemas; canonical BBL format enforced here
3. **`scoring/compute.py`** — Runs after all scrapers; computes weighted linear composite per block using per-unit rates; writes pre-computed score + signal breakdown to `displacement_scores` table
4. **`api/routes/`** — One router file per domain (neighborhoods, permits, ownership, evictions, score); reads pre-computed data; returns GeoJSON FeatureCollections for map consumption
5. **`frontend/`** — Static HTML/JS served by Nginx; MapLibre GL JS with data-driven paint expressions for the risk choropleth; GeoJSON source from API; no build step

### Critical Pitfalls

1. **BBL normalization mismatch** — ACRIS uses 10-digit zero-padded BBLs (`1000010001`); NYC Open Data uses hyphenated format (`1-00001-0001`). Joining without a `normalize_bbl()` utility produces massive false negatives that silently corrupt every cross-source join. Define canonical format and the utility function in `models/` before any scraper is written.

2. **Missing spatial indexes** — SQLAlchemy does not auto-create GiST indexes on geometry columns. Without them, `ST_Within` and `ST_Intersects` queries do full table scans. At NYC scale, queries that should run in milliseconds take minutes. Add explicit GiST index creation to every Alembic migration that creates a geometry column.

3. **ACRIS doc type filter too narrow** — Filtering ACRIS by `doc_type = 'DEED'` alone misses LLC acquisitions, which commonly use `DEEDP`, `DEED, BARGAIN & SALE`, `DEED, TRUST`, `ASST`, and lease assignment types. This silently undercounts the primary LLC detection signal. Use a broader filter and document the included types.

4. **Data desert bias (raw counts vs. per-unit rates)** — Dense neighborhoods have more permits and complaints in absolute terms simply because they have more buildings. A raw count composite incorrectly flags commercial Midtown over residential Brooklyn. All signals must be normalized per residential unit using DOF unit count data. This creates a hard dependency: DOF scraper must run before score computation can be meaningful.

5. **Socrata App Token omission** — Unauthenticated Socrata requests return HTTP 200 with silently truncated data (1,000 rows/day per IP). Scrapers appear to succeed but produce incomplete datasets. Register a Socrata App Token before the first scraper makes its first production call and pass it via `$$app_token` on every request.

6. **Scheduler silent failure** — System cron exits with code 0 even on partial scraper failure. Without a `scraper_runs` audit table (run start, row count, run end, exit code) and non-zero exit codes on failure, data gaps accumulate silently while the system appears healthy.

---

## Implications for Roadmap

The dependency chain in FEATURES.md is the phase ordering. Nothing in the scoring layer can be built without the schema. No scraper can be validated without the schema. The map can't render displacement data without the API. The API is useless without data. The build order that minimizes wasted work is strict bottom-up with an early end-to-end slice.

### Phase 1: Foundation — Schema, Models, and Infrastructure Setup

**Rationale:** Everything depends on this. BBL normalization failure, missing spatial indexes, wrong SRID, and the absence of Alembic are all catastrophic if discovered after scraper data is in the database. Fixing schema mistakes later means data re-ingestion. Get it right once.

**Delivers:** PostgreSQL + PostGIS database running locally and on DigitalOcean; Alembic migration chain initialized; all ORM models (neighborhoods, blocks, parcels, permits, events, displacement_scores); `normalize_bbl()` utility; GiST indexes on all geometry columns; SRID 4326 enforced across all models; PostGIS installed on VPS.

**Addresses:** All scraper and scoring features (schema is their prerequisite)

**Avoids:** BBL mismatch (Pitfall 6), missing spatial indexes (Pitfall 8), SRID mismatch (Pitfall 9), PostGIS not pre-installed on DO (Pitfall 18), retrofitting Alembic (STACK.md gap)

**Research flag:** No additional research needed — Alembic and GeoAlchemy2 are well-documented; PostGIS spatial index patterns are established.

### Phase 2: End-to-End Slice — DOB Permits Scraper, Score Stub, GeoJSON API, MapLibre Choropleth

**Rationale:** Validates the entire pipeline with real data before committing to four more scrapers. Confirms BBL normalization works, GeoJSON serialization chain works (GeoAlchemy2 → Shapely → dict → FeatureCollection), MapLibre renders the choropleth, and the API responds correctly. Produces a visible map with a single-signal score — a working prototype that can be demoed.

**Delivers:** DOB permits scraper with Socrata App Token, watermark-based incremental fetch, tenacity retry, bulk upsert; single-signal displacement score stub (permit intensity only); `GET /api/neighborhoods/{id}/score` GeoJSON endpoint; MapLibre map with data-driven choropleth; data freshness indicator.

**Addresses:** Interactive map (table stakes), displacement risk score (core value proposition), building permits (leading indicator, recommended first scraper)

**Avoids:** Socrata throttling (Pitfall 1), offset pagination race condition (Pitfall 2), schema drift (Pitfall 3), bulk insert performance (Pitfall 15), dataset URL instability (Pitfall 16), floating MapLibre CDN version (Pitfall 19)

**Research flag:** No additional research needed — DOB Socrata endpoint is well-documented; MapLibre GeoJSON source pattern is established.

### Phase 3: Remaining Scrapers — 311, ACRIS, Evictions, DOF

**Rationale:** Add the remaining four data sources incrementally now that the scraper pattern is validated. ACRIS is the most complex (three joined Socrata datasets, LLC name normalization, broader doc type filter) and should be built last within this phase. DOF must be completed in this phase because its unit count data is required before the composite score can be per-unit normalized.

**Delivers:** 311 complaints scraper; eviction filings scraper (with data lag documentation); ACRIS deed transfer scraper (with broad doc type filter, party name normalization, LLC detection); DOF property assessment scraper (including residential unit counts); `scraper_runs` audit table and non-zero exit codes on all scrapers.

**Addresses:** 311 data (table stakes), eviction data (table stakes), ACRIS / LLC ownership (table stakes + differentiator), DOF assessment data (table stakes)

**Avoids:** ACRIS doc type filter too narrow (Pitfall 4), LLC name normalization failure (Pitfall 5), ACRIS full re-scrape cost (Pitfall 7), scheduler silent failure (Pitfall 14), eviction data lag displayed without context (Pitfall 17)

**Research flag:** ACRIS may need phase-level research — the three-dataset join (master, legals, parties), the full doc type list, and the party name normalization strategy are non-trivial. Recommend a focused research pass before the ACRIS scraper is scoped.

### Phase 4: Composite Score Engine and Signal Breakdown

**Rationale:** Only now is all source data flowing. The composite score formula requires all five signal values and DOF unit counts. This phase computes the real 1–100 score, stores signal breakdowns alongside the composite, and exposes the breakdown in the API response and UI.

**Delivers:** `scoring/compute.py` with weighted linear composite; per-unit normalization for all signals; `displacement_scores` table with raw signal values + composite; updated API response schema including `signal_breakdown`; UI panel showing which signals drive the score; methodology documentation published.

**Addresses:** Composite displacement score (core value proposition), per-scraper score breakdown (differentiator), score opacity / trust failure

**Avoids:** Data desert bias from raw counts (Pitfall 10), composite score opacity / trust failure (Pitfall 11)

**Research flag:** Score weighting formula is judgment-based. Initial weights should be simple and equal; tuning requires real data and domain feedback. Flag for validation once the map is running with real data.

### Phase 5: Scheduler, Production Hardening, and API Completeness

**Rationale:** Productionizes the pipeline. Wires the nightly cron, adds failure alerting, hardens the API with rate limiting, completes remaining map features (block drill-down, neighborhood summary). Makes the prototype demo-ready and pressure-testable.

**Delivers:** Crontab configuration (2am UTC); scraper_runs audit table; cron stderr logging to rotated log file; slowapi rate limiting on all API endpoints; block-level drill-down on map click; neighborhood summary roll-up; simplified neighborhood polygon geometry (ST_Simplify) to control GeoJSON payload size; Python venv rebuilt against 3.11 final; MapLibre and DaisyUI CDN URLs pinned to specific versions.

**Addresses:** All remaining table stakes (block drill-down, neighborhood summary, data freshness indicators)

**Avoids:** Scheduler silent failure (Pitfall 14), large GeoJSON payload at initial load (Pitfall 12), MapLibre expression performance (Pitfall 13), floating CDN version (Pitfall 19)

**Research flag:** No additional research needed — patterns are well-established.

### Phase Ordering Rationale

- Schema before scrapers: BBL normalization and spatial indexes must be designed once, correctly — retrofitting after data is loaded means re-ingestion
- End-to-end slice before breadth: validates every layer of the stack with real data before multiplying scraper complexity by five
- ACRIS after simpler scrapers: ACRIS is the most complex source (three-dataset join, LLC tracing, name normalization); build scraper confidence on DOB and 311 first
- DOF within Phase 3 (not Phase 4): DOF unit counts are required input for per-unit normalization in the score — DOF must be complete before the scoring engine is meaningful
- Score engine after all scrapers: the composite requires all five signals; a partial composite would require re-computation anyway
- Hardening last: rate limiting, cron wiring, and payload optimization are risk mitigation for a running system, not prerequisites for development

### Research Flags

Needs research during planning:
- **Phase 3 (ACRIS scraper):** The three-dataset join structure (master, legals, parties tables), the complete document type list for LLC acquisition coverage, and party name normalization strategy are complex enough to warrant a focused research pass before scoping the scraper.
- **Phase 4 (Score weights):** Initial linear weights are a judgment call. Document the rationale, but flag for empirical tuning once real data is available. This is not a blocker for shipping.

Standard patterns (skip research-phase):
- **Phase 1 (Schema):** Alembic, GeoAlchemy2, PostGIS spatial indexes — all well-documented with established patterns.
- **Phase 2 (DOB slice):** Socrata SODA API pagination, MapLibre GeoJSON source, Shapely serialization — standard patterns.
- **Phase 5 (Hardening):** Cron, slowapi, nginx — operational patterns with no research uncertainty.

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Read directly from pinned requirements.txt; confirmed gaps (Alembic, GeoAlchemy2, Shapely, tenacity) are well-understood |
| Features | HIGH | NYC Open Data sources are publicly documented; feature set is coherent with the domain; MVP recommendation aligns with architecture dependencies |
| Architecture | HIGH | Layered monolith with pre-computed scores is the correct pattern for this data cadence; patterns are well-established in civic tech |
| Pitfalls | HIGH | BBL normalization, ACRIS doc type, spatial indexes, and data desert bias are documented failure modes from civic data projects; not speculative |

**Overall confidence:** HIGH

### Gaps to Address

- **Score weight calibration:** The composite formula uses a linear weighted sum, but initial weights are placeholder values. The weighting must be validated against known high-displacement neighborhoods once real data is flowing. Flag as a validation task after Phase 4 data is live.
- **DigitalOcean VPS sizing:** 2 vCPU / 2 GB RAM is sufficient for prototyping. At full NYC parcel load (millions of permit and event records), the scraper batch and scoring compute may require a larger Droplet or a read replica. Monitor nightly job runtime after Phase 3 is complete.
- **MapPLUTO parcel geometry dataset:** The architecture references ACRIS BBL-to-parcel spatial joins against a MapPLUTO parcels table. This dataset requires a separate one-time load step that is not yet in the scraper list. Address in Phase 1 schema planning — MapPLUTO is a free NYC DCP dataset downloaded as a shapefile and bulk-loaded into PostGIS.
- **ACRIS rate limits:** ACRIS is the most data-intensive source and may have undocumented rate limits beyond the standard Socrata App Token tier. Validate during Phase 3 ACRIS scraper development.

---

## Sources

### Primary (HIGH confidence)
- `/root/pulsecities/requirements.txt` — confirmed installed package versions
- `/root/pulsecities/.planning/codebase/ARCHITECTURE.md` — existing scaffold structure
- NYC Open Data Socrata SODA API: https://dev.socrata.com/ — pagination, app token, dataset ID patterns
- GeoAlchemy2 docs: https://geoalchemy-2.readthedocs.io/ — PostGIS geometry column integration
- MapLibre GL JS docs: https://maplibre.org/maplibre-gl-js/docs/ — data-driven styling, GeoJSON source

### Secondary (MEDIUM confidence)
- NYC Open Data DOB permits endpoint: https://data.cityofnewyork.us/Housing-Development/DOB-Permit-Issuance/ipu4-2q9a
- NYC eviction data: https://data.cityofnewyork.us/City-Government/Evictions/6z8x-wfk4
- NYC ACRIS deed records: https://a836-acris.nyc.gov/

### Tertiary (LOW confidence — validate during implementation)
- ACRIS document type list for LLC acquisitions — validate against live ACRIS data during Phase 3
- Score weight calibration — empirical; validate after Phase 4 data is live

---

*Research completed: 2026-04-07*
*Ready for roadmap: yes*
