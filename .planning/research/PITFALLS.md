# Pitfalls

**Domain:** Civic intelligence map — displacement risk, property ownership, and public records for NYC
**Researched:** 2026-04-07 (v1), 2026-04-16 (v2 deployment + operator profiles)

## Socrata / NYC Open Data Scraping

### Pitfall 1: Rate Throttling Without App Token (Silent Data Gaps)

**What happens:** Unauthenticated Socrata requests are rate-limited to ~1 req/sec and ~1,000 rows/day per IP. The API returns HTTP 200 with truncated data — no error, no warning. Scrapers appear to succeed but return incomplete datasets.

**Warning signs:** Row counts significantly lower than expected; block-level data sparse in certain boroughs; score computation looks correct but score values seem low.

**Prevention:** Always pass a registered Socrata App Token via `$$app_token` query param. Register at https://data.cityofnewyork.us/profile/edit/developer_settings. Store token in `.env`. Rate limit disappears for authenticated requests.

**Phase:** Address in Phase 1 (scrapers) — before any scraper makes its first production call.

---

### Pitfall 2: Offset Pagination Race Condition (Missing Records on Live Datasets)

**What happens:** NYC Open Data datasets are updated continuously. Paginating with `$offset` while the dataset is being updated causes records inserted between pagination requests to be skipped or duplicated.

**Warning signs:** Nightly row counts vary unexpectedly; duplicate event records after several months of operation.

**Prevention:** Use `$order` + `$where` with a watermark timestamp instead of `$offset`. Store the `last_ingested_at` timestamp per scraper and use `WHERE updated_at > last_ingested_at` to fetch only new records. This is incremental, safe, and idempotent.

**Phase:** Address in Phase 1 (scrapers) — watermark from the first scraper written.

---

### Pitfall 3: Socrata Schema Drift / Undocumented Column Changes

**What happens:** NYC Open Data dataset schemas change without notice. Column names are renamed, types change, new columns are added. Scrapers fail silently or crash with KeyError.

**Warning signs:** Scraper throws `KeyError` or `ValueError` on a specific row; dataset suddenly has unexpected null values in previously reliable columns.

**Prevention:** Use `.get()` with defaults when reading Socrata JSON rows. Validate critical columns at scraper startup with an explicit check before bulk processing. Add schema version logging (record the column names seen in each run).

**Phase:** Address in Phase 1 (scrapers) — defensive access patterns from the start.

---

## ACRIS-Specific Pitfalls

### Pitfall 4: DEED Filter Too Narrow — Misses LLC Acquisitions

**What happens:** Filtering ACRIS records by `doc_type = 'DEED'` misses LLC shell company acquisitions, which commonly use document types: `DEED, BARGAIN & SALE`, `DEED, TRUST`, `ASSIGNMENT OF LEASE`, `MEMO OF LEASE`. The displacement signal is undercounted.

**Warning signs:** ACRIS scraper returns far fewer records than expected; known high-displacement neighborhoods show low LLC acquisition scores.

**Prevention:** Query ACRIS with a broader document type filter: `doc_type IN ('DEED', 'DEEDP', 'DEED, BARGAIN & SALE', 'DEED, TRUST', 'ASST', 'ASSIGNMENT OF LEASE', 'MEMO OF LEASE')`. Filter at the application layer, not the API query layer.

**Phase:** Address in the ACRIS scraper phase. Reference ACRIS document type list before writing the filter.

---

### Pitfall 5: Party Name Normalization Failure (LLC Portfolio Undercounting ~30%)

**What happens:** The same LLC appears under multiple name variants in ACRIS: `EAST VILLAGE HOLDINGS LLC`, `East Village Holdings, LLC`, `EAST VILLAGE HOLDINGS, L.L.C.`. Without normalization, each variant is counted as a separate entity. LLC portfolios appear fragmented; the displacement signal is understated.

**Warning signs:** Owner names look slightly different across records for the same address; LLC acquisition counts seem low relative to neighborhood-level displacement reporting.

**Prevention:** Normalize party names before persisting: uppercase, strip punctuation (commas, periods), standardize `LLC` / `L.L.C.` / `LIMITED LIABILITY CO` to a canonical form. Consider Levenshtein fuzzy matching for near-duplicates. Store both the raw name and the normalized name.

**Phase:** Address in the ACRIS scraper, specifically the party name normalization step.

---

### Pitfall 6: BBL Linkage Failures (Zero-Padding Mismatch)

**What happens:** Borough-Block-Lot (BBL) numbers appear in different formats across NYC data systems. ACRIS: `1000010001` (10-digit, zero-padded). NYC Open Data (permits, 311): `1-00001-0001` (hyphenated). MapPLUTO: `1000010001` (10-digit). Joining on BBL without normalization produces massive false negatives — records from different sources that belong to the same property don't join.

**Warning signs:** Spatial joins return far fewer matches than expected; scores for known high-activity blocks are unexpectedly low; block-level drill-downs show data from only one source.

**Prevention:** Define a canonical BBL format (10-digit zero-padded: `BBBBBBBBBLL`) in the `models/` layer. Every scraper converts BBL to canonical form before persisting. Create a `normalize_bbl()` utility function used by all scrapers.

**Phase:** Address in Phase 1 (models layer), before any scraper is written.

---

### Pitfall 7: ACRIS Full Re-scrape Cost Without Incremental Logic

**What happens:** ACRIS deed records go back to 1966. A full re-scrape of ACRIS each night takes hours and hammers the Socrata endpoint. Without incremental logic, each run fetches millions of records unnecessarily.

**Warning signs:** Nightly job runtime exceeds acceptable window; Socrata returns 429 or throttles the connection.

**Prevention:** Use `document_date` as the watermark. Only fetch ACRIS records where `document_date > last_ingested_at`. Historical backfill is a one-time operation; the nightly job is incremental.

**Phase:** Address in the ACRIS scraper phase.

---

## PostGIS Pitfalls

### Pitfall 8: Missing GiST Spatial Index (SQLAlchemy Does Not Auto-Create)

**What happens:** SQLAlchemy does not automatically create GiST spatial indexes on `Geometry` columns. Without a spatial index, every `ST_Within`, `ST_Intersects`, or `ST_Contains` query does a full table scan. At NYC scale (millions of permit/event records), queries that should run in milliseconds take minutes.

**Warning signs:** API response times degrade sharply as data volume grows; neighborhood-level queries time out; database CPU spikes on map tile requests.

**Prevention:** Explicitly create GiST indexes in Alembic migrations for every geometry column:

```sql
CREATE INDEX idx_permits_location ON permits USING GIST (location);
CREATE INDEX idx_parcels_geometry ON parcels USING GIST (geometry);
```

**Phase:** Address in the database schema phase (Phase 1) — add to every Alembic migration that creates a geometry column.

---

### Pitfall 9: Geometry CRS/SRID Mismatch (WGS84 vs NY State Plane)

**What happens:** NYC Open Data exports geometry in WGS84 (SRID 4326). Some legacy DOF and MapPLUTO files use NY State Plane (SRID 2263). Storing mixed SRIDs produces silent spatial errors — `ST_Within` checks fail, distance calculations are in the wrong units (feet vs degrees), and the map renders points in the wrong locations.

**Warning signs:** Points appear in the ocean or in the wrong borough; spatial join counts are near-zero despite expected overlap; map renders correctly visually but data appears in wrong neighborhoods.

**Prevention:** Always use SRID 4326 as the canonical storage SRID. Reproject at ingest using `ST_Transform(geom, 4326)` for any source that arrives in a different SRID. Declare SRID in all GeoAlchemy2 column definitions: `Geometry('POINT', srid=4326)`.

**Phase:** Address in Phase 1 (models layer). Enforce in all scraper normalization steps.

---

## Displacement Score Pitfalls

### Pitfall 10: Data Desert Bias (Raw Counts vs Per-Unit Rates)

**What happens:** Dense neighborhoods (Midtown, Lower Manhattan) have more permits, 311 complaints, and eviction filings in absolute terms simply because they have more buildings and units. A raw count composite score incorrectly flags dense commercial areas as high-displacement risk rather than residential neighborhoods with genuine pressure.

**Warning signs:** Midtown shows extreme displacement scores; residential neighborhoods in Brooklyn and Queens with known displacement pressure score lower than commercial zones.

**Prevention:** Normalize all signals per residential unit, not per block. Use DOF property data to get unit counts per block. Signal formula: `eviction_rate = eviction_count / residential_unit_count`. This requires DOF scraper data before the score can be computed — enforce this dependency in the scheduler.

**Phase:** Address in the score computation phase. Requires DOF data to be ingested first.

---

### Pitfall 11: Composite Score Opacity / Trust Failure

**What happens:** Users (especially journalists and tenant organizers) distrust opaque scores. A neighborhood showing "Score: 87" with no explanation is useless — they need to know why. Without transparency, the score is dismissed or misused.

**Warning signs:** User feedback says "I don't understand this score"; journalists ask for the methodology and can't reproduce the number; organizers prefer raw data over the composite.

**Prevention:** Store and expose raw signal values alongside the composite score. Every API response includes `signal_breakdown: {permits: 40, evictions: 30, llc_acquisitions: 25, ...}`. The UI shows the breakdown by default. Document the scoring methodology publicly.

**Phase:** Address in the score computation and API phases simultaneously.

---

## MapLibre Frontend Pitfalls

### Pitfall 12: Large GeoJSON Payload at Initial Load

**What happens:** Loading all NYC neighborhood boundaries + scores as a single GeoJSON FeatureCollection at map init produces a large payload (300KB–2MB depending on geometry detail). On slow connections, this delays the map becoming interactive.

**Warning signs:** Map takes 3+ seconds to show the risk choropleth on first load; mobile users experience blank map for several seconds.

**Prevention:** For v1, simplify neighborhood polygon geometry (use mapshaper or PostGIS `ST_Simplify` to reduce vertex count). Load only the score attribute in the initial payload — don't include event details. Defer block-level GeoJSON to on-click requests. For v2, serve vector tiles (PMTiles) from DigitalOcean Spaces.

**Phase:** Address in the frontend phase. Use simplified geometry from the start.

---

### Pitfall 13: MapLibre Expression Performance on Rapid Filter Updates

**What happens:** Applying complex `interpolate` or `match` expressions on every map interaction (hover, filter, zoom) with large datasets triggers GPU repaints. With thousands of neighborhood polygons and dynamic filter state, frame rate drops below 30fps.

**Warning signs:** Map stutters on hover; filter UI feels laggy; Chrome DevTools shows dropped frames during interaction.

**Prevention:** Use data-driven paint expressions (which MapLibre evaluates on the GPU) instead of JavaScript layer updates. Avoid calling `map.setFilter()` or `map.setPaintProperty()` on every mousemove — debounce interactions. Store score in the GeoJSON properties so the paint expression is static; only update data source, not the layer expression.

**Phase:** Address in the frontend phase. Establish this pattern from the first map layer.

---

## Scheduler / Infrastructure Pitfalls (v1)

### Pitfall 14: Scheduler Silent Failure / No Watermark Persistence

**What happens:** System cron invokes the scraper at 2am. The scraper exits with code 0 (success) but fails halfway through due to a network timeout or Socrata error. No alert is sent. The next night's run uses a stale watermark and re-fetches overlapping data — or worse, skips a day of data silently.

**Warning signs:** Displacement scores don't update despite nightly runs; data freshness indicators on the frontend show stale dates; scraper logs show successful exits but row counts are zero.

**Prevention:** Each scraper must:
1. Write a watermark timestamp to the database **only after successful commit**
2. Exit with non-zero exit code on any failure
3. Log run start, row count, and run end to a scraper_runs table
4. Cron stderr should redirect to a log file with rotation (`2>> /var/log/pulsecities/scraper.log`)

**Phase:** Address in the scheduler phase. Implement the scraper_runs audit table from the first scraper.

---

### Pitfall 15: SQLAlchemy ORM Bulk Insert Performance

**What happens:** Inserting thousands of records using individual `session.add()` calls in a loop is 10–100x slower than bulk operations. A 311 complaints scraper that fetches 50,000 records per night takes minutes instead of seconds if row-by-row.

**Warning signs:** Nightly scraper runtime is measured in tens of minutes; database CPU is high during scraper runs.

**Prevention:** Use `session.bulk_insert_mappings()` or SQLAlchemy Core `insert()` with `on_conflict_do_update()` for batch upserts. Batch sizes of 1,000–5,000 records per commit are typically optimal.

**Phase:** Address in the scraper phase. Use bulk operations from the first scraper written.

---

### Pitfall 16: NYC Open Data URL Instability

**What happens:** NYC Open Data dataset URLs change. Bookmarked direct download URLs and hardcoded endpoint URLs break when the city updates their data catalog. Scrapers fail with 404s.

**Warning signs:** Scraper raises `requests.HTTPError: 404` on an endpoint that worked last week.

**Prevention:** Always use Socrata dataset IDs (e.g., `ipu4-2q9a`), not full URLs. The canonical base URL `https://data.cityofnewyork.us/resource/{dataset_id}.json` is stable. Store dataset IDs as named constants, not URLs.

**Phase:** Address in Phase 1 (scrapers). Use dataset IDs from the first scraper.

---

### Pitfall 17: Eviction Data Lag (OCA Reporting Delays)

**What happens:** NYC eviction data on NYC Open Data comes from OCA (Office of Court Administration) and lags the actual filing date by 2–4 weeks. A displacement spike visible in permits and 311 data today won't appear in eviction data for weeks.

**Warning signs:** Users report eviction filings they know happened that don't appear in the map; displacement scores in rapidly gentrifying areas seem lower than expected.

**Prevention:** Always display data freshness timestamps per signal in the UI. Document the known eviction data lag in the methodology disclosure. Do not penalize the eviction signal's weight in the composite score for this lag — it's a known source characteristic, not a data quality failure.

**Phase:** Address in the API and frontend phases — data freshness display and methodology documentation.

---

### Pitfall 18: DigitalOcean VPS — PostGIS Extension Not Pre-installed

**What happens:** A freshly provisioned DigitalOcean Droplet with PostgreSQL does not have PostGIS installed. The `CREATE EXTENSION postgis;` command fails if the PostGIS package is not installed at the OS level first.

**Warning signs:** Alembic migration fails with `ERROR: could not open extension control file "/usr/share/postgresql/14/extension/postgis.control"`.

**Prevention:** Add PostGIS to the VPS provisioning script:

```bash
sudo apt-get install -y postgresql-14-postgis-3
sudo -u postgres psql -d pulsecities -c "CREATE EXTENSION postgis;"
sudo -u postgres psql -d pulsecities -c "CREATE EXTENSION postgis_topology;"
```

Document this in the project's deployment runbook before the first VPS is provisioned.

**Phase:** Address in the deployment/infrastructure setup phase, before any Alembic migration is run against the production database.

---

### Pitfall 19: MapLibre CDN Version Floating (Breaking Changes on Major Releases)

**What happens:** Loading MapLibre from a CDN URL without a pinned version (e.g., `https://unpkg.com/maplibre-gl/dist/maplibre-gl.js`) automatically pulls the latest version. A MapLibre major release with breaking API changes can break the map silently overnight.

**Warning signs:** Map stops rendering after a CDN update; JavaScript console shows `TypeError: map.addLayer is not a function` or similar; the issue only affects users who loaded the page after the CDN was updated.

**Prevention:** Pin to a specific version in the CDN URL:

```html
<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet" />
```

Review the MapLibre changelog before upgrading. Upgrade deliberately, not automatically.

**Phase:** Address in Phase 1 when the frontend scaffold is written. Never use a floating CDN version.

---

---

## Production Deployment Pitfalls (v2)

### Pitfall 20: Cloudflare SSL/TLS Redirect Loop from Encryption Mode Mismatch

**What goes wrong:**
Your frontend shows `ERR_TOO_MANY_REDIRECTS` after deploying behind Cloudflare. Visitors get stuck in an infinite loop between Cloudflare's proxy and your origin server.

**Why it happens:**
Cloudflare and your origin server's redirect rules conflict. Most commonly:
- Cloudflare SSL/TLS is set to "Flexible" (Cloudflare sends unencrypted HTTP to your origin)
- Your FastAPI app (or Nginx in front of it) redirects all HTTP → HTTPS
- Loop: Client → Cloudflare (HTTPS) → Origin (HTTP) → Origin redirects to HTTPS → Cloudflare (HTTPS) → ...

Alternative loop:
- Cloudflare set to "Full" or "Full (Strict)" but your origin doesn't have a valid SSL certificate
- Origin redirects HTTPS → HTTP
- Loop: Client → Cloudflare (HTTPS) → Origin (HTTPS) → Origin redirects to HTTP → Cloudflare (HTTPS) → ...

**Consequences:**
- Entire application is inaccessible behind Cloudflare proxy
- Not immediately obvious where the problem is; appears to be a server crash rather than configuration issue
- Blocks initial launch and deployment validation
- Expensive to debug: requires understanding Cloudflare's SSL/TLS modes, Let's Encrypt cert validation, and your origin's redirect behavior

**Prevention:**
1. **Before adding Cloudflare:** Ensure your origin (DigitalOcean VPS) has a valid SSL certificate (Let's Encrypt via certbot)
2. **Set Cloudflare SSL/TLS to "Full"** (not "Flexible")
3. **Disable HTTPS redirect in FastAPI** — let Cloudflare handle HTTP → HTTPS redirection
4. **Ensure FastAPI is configured with `--forwarded-allow-ips="*"` or specify Cloudflare's IP range** to trust forwarded headers and generate correct redirect URLs

**Detection:**
- Client reports redirect loop on first load
- Browser shows `ERR_TOO_MANY_REDIRECTS` or `maxRedirects exceeded`
- In browser DevTools Network tab: see chain of 301/307 responses bouncing between http/https
- In origin server logs: no 5xx errors, just repeated requests

**Phase:** Address in Phase 2.1 (deployment hardening) — test before enabling Cloudflare DNS.

---

### Pitfall 21: Nginx Proxy Headers Missing or Misconfigured

**What goes wrong:**
After adding Nginx in front of FastAPI, URLs are generated incorrectly. Rate-limiting sees all requests as coming from 127.0.0.1. FastAPI security checks fail. Redirects point to `http://localhost` instead of `https://yourdomain.com`.

**Why it happens:**
Nginx acts as a reverse proxy but doesn't forward critical headers by default. Without these headers, FastAPI can't determine:
- The client's real IP (for rate limiting and security logs)
- The original request protocol (HTTPS vs HTTP)
- The original host domain

Your slowapi `get_remote_address` limiter (used in current app) reads `X-Forwarded-For` header; without it, all requests appear to come from the same IP (Nginx's local socket/port).

**Consequences:**
- Rate limiting is bypassed (all requests share same "client" IP)
- Abuse detection and DOS protection fail
- URLs in API responses point to wrong domain or protocol
- OpenAPI Swagger UI breaks (tries to load `/api/openapi.json` from `localhost:8000` instead of `yourdomain.com`)
- If you later add auth, IP-based security checks fail

**Prevention:**

Nginx config MUST include:
```
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
proxy_set_header X-Forwarded-Proto $scheme;
proxy_set_header X-Forwarded-Host $host;
proxy_set_header X-Real-IP $remote_addr;
```

FastAPI must start with:
```
gunicorn -w N --forwarded-allow-ips="127.0.0.1,YOUR_NGINX_IP" api.main:app
```

Or trust all IPs in development/staging:
```
gunicorn -w N --forwarded-allow-ips="*" api.main:app
```

Do NOT mount StaticFiles (`/`) before setting up Nginx — FastAPI route matching is first-match; wrong header setup gets masked by StaticFiles serving 404s.

**Detection:**
- Test with `/api/health` or debug endpoint and log `request.client.host` — should show your actual IP, not 127.0.0.1
- Check rate limit behavior: try requests from different IPs; should be rate-limited per IP, not globally
- OpenAPI docs load but API calls fail or show CORS errors
- URLs in responses contain `localhost:8000` or wrong protocol

**Phase:** Address in Phase 2.1 (deployment hardening) — test Nginx config before production deploy.

---

### Pitfall 22: Gunicorn Worker Count + SQLAlchemy Connection Pool Starvation on 2GB RAM

**What goes wrong:**
Your app starts fine but hangs or crashes under load. Database queries timeout. Logs show `(psycopg2.pool.PoolError: QueuePool limit of size 5 overflow 10 reached)` or similar.

**Why it happens:**
Gunicorn uses multiple worker processes (default 2–4 × CPU cores). Each worker has its own SQLAlchemy connection pool. SQLAlchemy pools are NOT safe to share across process boundaries.

With your current config:
- 2GB RAM can support ~200 DB connections total before exhaustion
- If you spawn 4 gunicorn workers with pool_size=5, max_overflow=10, that's 60 potential connections per worker
- 4 workers × 60 connections = 240 connections, but you only have RAM/connection budget for ~200
- Workers compete for scarce connections; under load, workers exhaust their pool and queries queue indefinitely

Additionally, each connection maintains transaction state; if workers hang, connections are held open and never released.

**Consequences:**
- Intermittent hangs during traffic spikes
- Database connection exhaustion errors in some requests
- Cascading failure: slow requests hold connections, new requests wait for free connections, system grinds to halt
- Hard to debug: error appears random and depends on traffic pattern

**Prevention:**

1. **Know your constraint:** 2GB RAM ≈ 200 DB connections max
2. **Size gunicorn workers conservatively:**
   ```
   gunicorn -w 2 -k uvicorn.workers.UvicornWorker api.main:app
   ```
   Start with 2 workers; benchmark and increase only if needed. Monitor RAM usage.

3. **Configure SQLAlchemy connection pool to fit your budget:**
   ```python
   engine = create_engine(
       DATABASE_URL,
       poolclass=QueuePool,
       pool_size=5,      # Persistent connections per worker
       max_overflow=3,   # Temporary overflow connections
       pool_recycle=3600, # Recycle connections after 1 hour (prevents DB idle timeout)
   )
   ```
   With 2 workers: 2 × (5 + 3) = 16 connections total. Safe.

4. **Add pool recycle:** If your DB closes idle connections after N seconds (common in cloud), set `pool_recycle` to a safe margin below that threshold.

5. **Monitor at deployment:**
   ```bash
   # Check active connections
   psql -U user -d database -c "SELECT count(*) FROM pg_stat_activity;"
   ```

**Detection:**
- Errors containing "QueuePool limit" or "pool_size"
- SELECT queries that should be fast (< 100ms) timing out
- Load test with concurrent users (e.g., `ab -n 100 -c 10`) reveals hangs at certain concurrency level
- `free -h` shows system running out of RAM under load

**Phase:** Address in Phase 2.1 (deployment hardening) — benchmark and configure before production.

---

### Pitfall 23: Let's Encrypt Certificate Renewal Blocking Behind Cloudflare HTTP-01 Challenge

**What goes wrong:**
Your SSL certificate (Let's Encrypt) approaches expiration. Automatic renewal fails. After 90 days, cert expires, Cloudflare can't validate, and users see security warnings.

**Why it happens:**
Let's Encrypt uses HTTP-01 challenge: serves a file at `.well-known/acme-challenge/` and Let's Encrypt's servers verify they can reach it. When Cloudflare is in front:
- Let's Encrypt HTTP-01 requests go to Cloudflare's proxy, not your origin
- If Cloudflare blocks `.well-known/*` or redirects HTTP to HTTPS, challenge fails
- Or: you move cert to Cloudflare but don't renew it properly

**Prevention:**
- Use **DNS-01 challenge** (Let's Encrypt validates via DNS record) instead of HTTP-01. Doesn't require HTTP access.
- Or: whitelist `.well-known/acme-challenge/*` in Cloudflare to bypass HTTPS redirect and forward to origin.
- Use certbot with Cloudflare plugin to automate DNS-01.
- Monitor cert expiration date (set calendar reminder or use cert monitoring service).

**Detection:**
- Browser shows security warning (untrusted cert)
- `openssl s_client -connect yourdomain:443` shows cert expiration date < 30 days away
- Certbot renewal logs show HTTP-01 challenge failed

**Phase:** Address in Phase 2.1 (deployment hardening) — configure before cert renewal window.

---

### Pitfall 24: Frontend Static File Caching Behind Cloudflare Breaks Updates

**What goes wrong:**
You deploy a new frontend feature (e.g., operator profile pages in `index.html`), but users' browsers show old cached version. Users see "Feature not found" errors. Issue resolves itself after 24–48 hours (browser cache expires).

**Why it happens:**
Cloudflare caches static files (HTML, JS, CSS) by default. When you deploy new `index.html`, Cloudflare's cache isn't invalidated. Browser cache is stale. Users get old HTML without new routes.

**Consequences:**
- New features don't appear to users immediately after deployment
- Breaks deployments; users report "feature isn't working"
- Mobile users get stuck (longer default cache duration on mobile)
- Rollback becomes harder because old version is served from cache

**Prevention:**
1. **Set Cache-Control headers in Nginx:**
   ```nginx
   location = /index.html {
       add_header Cache-Control "no-cache, no-store, must-revalidate";
   }
   location ~* \.(js|css|svg)$ {
       add_header Cache-Control "public, max-age=31536000";  # 1 year for versioned assets
   }
   ```
2. **Version your static assets** (e.g., `app.abc123.js`) so new versions don't collide with old ones.
3. **Purge Cloudflare cache after deployment:**
   ```bash
   curl -X POST "https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache" \
     -H "X-Auth-Email: {email}" \
     -H "X-Auth-Key: {api_key}" \
     -d '{"files":["https://yourdomain.com/index.html"]}'
   ```

**Detection:**
- User reports new feature isn't showing after you deployed
- Clear browser cache; feature appears
- Check `Cache-Control` header in response: if `public, max-age=3600`, that's the problem

**Phase:** Address in Phase 2.1 (deployment hardening) — set headers before production deploy.

---

## Operator Profile Pitfalls (v2)

### Pitfall 25: Operator Data Staleness from Backfill Scripts Without Continuous Pipeline

**What goes wrong:**
You show operator profile pages with "up-to-date" operator data, but after 3 weeks the profiles are stale. New property acquisitions aren't reflected. Operator "buildings" count stays at backfill date. Users see outdated information.

**Why it happens:**
Your current setup uses backfill scripts (`top_operators_profiled.py`, `operator_network_analysis.py`) that run once during development to populate operator data. The data is written to JSON or database tables, but there's no scheduled pipeline to refresh operator acquisition lists.

Ownership data (ACRIS) and violation data (HPD) are continuously updated in your nightly scrapers, but operator profile aggregations are NOT recalculated. Stale context (outdated operator profiles) is worse than missing context because users trust the data.

**Consequences:**
- Operator profile pages become unreliable; users lose trust
- Business intel becomes dated: operators appear less active than they are
- Can't detect new acquisition patterns (e.g., operator shifts to new ZIP code)
- If you later monetize operator data (B2B API tier), stale profiles are a dealbreaker
- Requires manual re-run of backfill scripts to update, not scalable

**Prevention:**

1. **Integrate operator aggregations into scheduled pipeline:**
   - After nightly ACRIS ownership scrape completes, run operator aggregation step
   - Recalculate operator BBL counts, displacement score rollups, etc.
   - Store results in proper database tables (not JSON files)

2. **Create `operators` and `operator_acquisitions` tables:**
   ```python
   class Operator(Base):
       __tablename__ = "operators"
       operator_root: str  # OCEANVIEW, ICECAP, etc.
       llc_entities: List[str]  # JSON or separate table
       bbl_count: int
       updated_at: datetime  # When this profile was last recalculated
       # Aggregated metrics
       avg_displacement_score: float
       acquisition_trend_3m: int
   ```

3. **Schedule operator refresh in APScheduler:**
   ```python
   scheduler.add_job(
       refresh_operator_profiles,
       trigger="cron",
       hour=2,  # Run after nightly scraper (assume it finishes by 2am)
       id="refresh_operators"
   )
   ```

4. **Add `updated_at` timestamp to profile API responses** so users can see how fresh the data is.

**Detection:**
- Operator profile page shows data `created_at` date that's weeks old
- Compare profile BBL count with manual ACRIS query; numbers don't match
- Check APScheduler logs; operator refresh job doesn't exist or is failing

**Phase:** Address in Phase 2.2 (operator profiles) — integrate before launch.

---

### Pitfall 26: MapLibre GeoJSON Performance with Large Operator BBL Sets

**What goes wrong:**
After launching operator profile pages with map layer showing all operator-controlled parcels, the map stutters on some operators. Pan/zoom becomes slow. High CPU on client browser. Mobile users see blocked UI.

**Why it happens:**
You're trying to render a GeoJSON source with hundreds of BBL polygons (parcels) from ownership_raw directly in MapLibre. MapLibre's default GeoJSON rendering is synchronous; large datasets block the main thread while it parses and renders features.

Operators like OCEANVIEW or BREDIF control 50–200+ BBLs. Each BBL (parcel) is a full polygon geometry. If you load all geometries at once:
- Browser must parse and store all coordinates in memory
- Rendering engine must generate GPU commands for all polygons
- Zoom/pan interactions must re-render all features
- No culling or level-of-detail optimization

**Consequences:**
- Map interaction (pan, zoom, click) is laggy or frozen
- High CPU/memory on client
- Mobile experience is poor (especially on older devices)
- Operator profile pages load slowly
- First paint is delayed while GeoJSON is being processed

**Prevention:**

1. **Reduce GeoJSON payload:**
   - Load full polygon only when needed (e.g., user zooms in or clicks)
   - At default zoom (city level), show simplified polygon or marker at centroid
   - Use `simplify-geojson` or PostGIS `ST_Simplify()` to reduce coordinate count

2. **Split large datasets:**
   - Instead of one GeoJSON source with hundreds of features, split into 2–3 sources
   - Only show sources at appropriate zoom levels
   ```javascript
   map.addSource('operator-parcels-zoom8', {
       type: 'geojson',
       data: geoJsonZoom8  // Simplified, fewer points
   });
   map.addSource('operator-parcels-zoom14', {
       type: 'geojson',
       data: geoJsonZoom14  // Full detail
   });
   ```

3. **Use vector tiles instead of GeoJSON:**
   - For truly large operator datasets (100+ BBLs), serve as vector tiles
   - Use tool like `Martin` or `tileserver-gl` to convert PostGIS → MBTiles/PMTiles
   - Map loads tiles on demand; rendering is much faster

4. **Lazy load:** Only request operator parcel GeoJSON when user opens operator profile or clicks "Show on map".

5. **Benchmark:** Test with largest operator (OCEANVIEW or BREDIF) at production zoom levels. If pan/zoom is > 100ms latency, split or simplify.

**Detection:**
- Operator profile page with map layer is slow to load
- DevTools Profiler shows long "Recalculate Style" or "Layout" phases
- Browser DevTools Performance timeline shows frame drops when panning
- CPU usage spikes when zooming out to see all operator parcels

**Phase:** Address in Phase 2.2 (operator profiles) — test with largest operator before launch.

---

### Pitfall 27: Vanilla JS Routing for Operator Profile Pages in No-Build SPA

**What goes wrong:**
You add operator profile pages, but routing is inconsistent. Users can't bookmark operator profiles. Browser back button doesn't work. Search results linking to operator profiles load the wrong content or land on homepage.

**Why it happens:**
Your current setup serves a single `index.html` with inline JavaScript (no build step, no router library). You use hash-based routing for neighborhoods (e.g., `#/neighborhood/10036`) or query params for drill-down.

Operator profiles are new feature pages that need URLs like `/operator/OCEANVIEW` or `/#/operator/OCEANVIEW`. If routing logic is added as an afterthought without proper URL state management:
- Browser history (back button) doesn't sync with app state
- Direct URL access (bookmarking) loads static HTML without running JS routing logic
- Search result links to operator profile may reload entire page instead of client-side navigation
- Mobile navigation (back button) is unreliable

**Consequences:**
- Users can't share operator profile links
- Bookmarks don't work
- Browser back button takes user off-site instead of to previous page
- Search to operator profile transition is slow (full page reload)
- Analytics/tracking breaks because page views look like navigations

**Prevention:**

1. **Choose routing strategy upfront:**
   - **Hash routing (simplest):** All URLs like `index.html#/operator/OCEANVIEW`. Browser doesn't send hash to server; all traffic hits static files. Works with any server. No SEO, but acceptable for internal drill-down.
   - **Query params:** `index.html?page=operator&id=OCEANVIEW`. Similar to hash; simpler for some use cases.
   - **Clean URLs with History API (best):** URLs like `/operator/OCEANVIEW`. Requires server-side router to fallback to `index.html` for all non-API requests (or Nginx rewrite rule).

2. **For your vanilla JS app, stick with hash routing or implement History API carefully:**

   Hash routing (existing approach, extend it):
   ```javascript
   function routeChange() {
       const route = window.location.hash.slice(1);
       if (route.startsWith('/operator/')) {
           const operatorId = route.split('/')[2];
           loadOperatorProfile(operatorId);
       }
   }
   window.addEventListener('hashchange', routeChange);
   routeChange(); // Handle page load
   ```

3. **If using clean URLs (`/operator/OCEANVIEW`), add Nginx rewrite:**
   ```nginx
   location / {
       try_files $uri $uri/ /index.html;
   }
   ```
   This ensures any non-file URL falls back to `index.html`, and your JS router takes over.

4. **Test routing thoroughly:**
   - Direct URL access (paste `/operator/OCEANVIEW` in address bar, reload)
   - Bookmarking (save, close, reopen)
   - Browser back/forward from operator profile
   - Search result navigation to operator profile

**Detection:**
- User tries to bookmark operator profile; refresh lands on homepage
- Back button from operator profile goes to previous domain, not previous page
- Deep links to operator profiles (from external search) don't work
- Page transition to operator profile involves full page reload (slow)

**Phase:** Address in Phase 2.2 (operator profiles) — define routing strategy before adding profile pages.

---

### Pitfall 28: Search Result Ranking When Mixing Address and Operator Name Results

**What goes wrong:**
Your search bar now returns both address results (buildings/properties) and operator name results. Ranking is poor: an operator with name "123 MAIN ST REALTY" ranks above actual "123 Main St" property. Users get confusing results.

**Why it happens:**
You merge two result types (properties with addresses, operators with names) into one result list. Scoring/ranking is either:
- Naive: alphabetical or by database order
- Keyword-based: relevance score from FTS (full-text search) doesn't account for entity type
- Missing: no distinction between "exact match on property address" vs. "partial match on operator LLC name"

When "123 MAIN ST REALTY" (operator name) and "123 Main Street, NY" (property address) both have similar FTS scores, user gets confused about which is which.

**Consequences:**
- Users struggle to find actual properties
- Operator profiles mixed in with property results create cognitive load
- Search feels unreliable
- May deter users from exploring operator profiles (wrong positioning in results)
- Can't differentiate intent: user searching "123 Main" might want property OR operator

**Prevention:**

1. **Separate result types in UI:**
   Don't merge address and operator results in a single list. Show them in groups:
   ```
   Properties (5 results)
   - 123 Main St (Manhattan)
   - 456 Broadway (Manhattan)
   Operators (2 results)
   - 123 MAIN ST REALTY LLC
   - MAIN ST HOLDINGS
   ```

2. **Rank within type, not across:**
   - Score properties by FTS relevance (proximity of query to address)
   - Score operators separately (exact LLC name match > contains > fuzzy match)
   - Show top N of each type

3. **Use entity type metadata:**
   Tag each result with its type (property, operator, neighborhood) and use that in sorting:
   ```python
   results = [
       {"type": "property", "score": 0.95, "id": "bbl", "text": "123 Main St"},
       {"type": "operator", "score": 0.85, "id": "operator_root", "text": "123 MAIN ST REALTY"},
   ]
   # Sort by type first (properties before operators), then score within type
   ```

4. **Handle ambiguity:**
   If both property and operator have high relevance, show both and let user choose context via result groups.

**Detection:**
- Operator results appearing above obvious property matches
- Users reporting "search is broken, I was looking for the building not the company"
- Low click-through on operator results from search

**Phase:** Address in Phase 2.2 (operator profiles) — design search integration before adding operator search.

---

### Pitfall 29: Operator Data Stored in Ad-Hoc Structures (JSON, Backfill Tables) Instead of Proper Schema

**What goes wrong:**
Operator profiles were backfilled via scripts that wrote JSON or scattered columns. The "operator" entity isn't a first-class table with foreign keys. Adding new operator metrics requires changing backfill scripts and re-running. Querying operators becomes fragile.

**Why it happens:**
Operator profiles were investigative outputs (JSON files, analysis notebooks) first, then retrofitted into the app. Rather than designing proper `operators` and `operator_acquisitions` tables, the data lives in:
- JSON files (`operator_network_analysis.json`, `top_operators_profiled.json`)
- Extra columns in `ownership_raw` (operator_root, operator_network_id)
- Denormalized aggregate tables (if any)

This works for "show static profile" but breaks for "query operators by metric" or "update operators incrementally."

**Consequences:**
- New operator features require data migration or script changes
- Can't efficiently query "operators with > 50 acquisitions in ZIP 10002"
- Operator search can't use database indexes (data is in JSON files)
- Backfill scripts must re-compute everything instead of incremental updates
- Hard to maintain multiple environments (dev, staging, prod) with different operator datasets

**Prevention:**

1. **Migrate operator data to proper tables:**
   ```python
   class Operator(Base):
       __tablename__ = "operators"
       id: int
       operator_root: str  # OCEANVIEW
       llc_entities: str  # JSON or separate table
       bbl_count: int
       created_at: datetime
       updated_at: datetime

   class OperatorAcquisition(Base):
       __tablename__ = "operator_acquisitions"
       id: int
       operator_id: int  # FK to operators
       bbl: str
       doc_date: date
       doc_amount: Decimal
       # ... other deed fields
   ```

2. **Backfill scripts write to database tables, not JSON:**
   ```python
   # Instead of: json.dump(operators, Path('operator_network_analysis.json'))
   db.session.add_all([Operator(**op_dict) for op_dict in operators])
   db.session.commit()
   ```

3. **Add indexes for profile queries:**
   ```python
   Index("idx_operator_root", Operator.operator_root),
   Index("idx_operator_updated_at", Operator.updated_at),  # For freshness
   Index("idx_operator_acq_operator_id", OperatorAcquisition.operator_id),
   ```

4. **Write migrations for any schema changes:**
   Use Alembic to version schema changes, not ad-hoc SQL or script changes.

**Detection:**
- Operator profile routes query JSON files instead of database
- Adding new operator metric requires touching backfill scripts
- Operator search doesn't use database (scans JSON in memory)
- Can't track when operator data was last updated

**Phase:** Address in Phase 2.2 (operator profiles) — migrate before adding profile features.

---

### Pitfall 30: Operator Profile Page Title/Meta Tags Break SEO

**What goes wrong:**
Search engines can't crawl operator profile pages because meta tags aren't updated. Page title is always "PulseCities" instead of "OCEANVIEW — Operator Profile". Open Graph tags don't have operator name/image.

**Why it happens:**
Vanilla JS SPA doesn't update `<title>` or meta tags on client-side route change. Search engines that don't execute JS see boilerplate HTML.

**Prevention:**
- Update `document.title` and meta tags in your router when operator profile loads:
  ```javascript
  function loadOperatorProfile(operatorRoot) {
      document.title = `${operatorRoot} — PulseCities Operator Profile`;
      document.querySelector('meta[name="description"]').content = `Profile of operator ${operatorRoot}...`;
  }
  ```
- Or: pre-render operator profile pages at build time (if you add a build step later).

**Detection:**
- Check site in Google's Structured Data Tester; title is generic
- Open Graph debugging tools show wrong title/image for operator links

**Phase:** Address in Phase 2.2 (operator profiles) — update metadata in routing logic.

---

### Pitfall 31: Slow Initial Load of Operator Data from Database

**What goes wrong:**
Operator profile pages load slowly (> 2 seconds). Database query for operator acquisitions times out if operator has 100+ BBLs.

**Why it happens:**
Operator acquisition endpoint queries `ownership_raw` for all acquisitions for operator's LLC entities. Without proper indexing or pagination, scanning hundreds of rows is slow.

**Prevention:**
- Pre-calculate and store operator metrics in `operators` table (BBL count, acquisition trend, etc.)
- For full acquisition list, use pagination:
  ```python
  @router.get("/api/operators/{operator_root}/acquisitions")
  def get_operator_acquisitions(operator_root: str, skip: int = 0, limit: int = 50):
      acqs = db.query(OperatorAcquisition).filter(...).offset(skip).limit(limit)
  ```
- Index on operator_id and doc_date for fast filtering
- Cache operator profile data (5–60 min TTL) using response headers or Redis

**Detection:**
- Operator profile pages show loading spinner for > 2 seconds
- Database logs show slow query on ownership_raw table
- Response time > 1 second for `/api/operators/{id}` endpoint

**Phase:** Address in Phase 2.2 (operator profiles) — add pagination and caching before launch.

---

### Pitfall 32: CORS Issues After Adding Operator API Endpoint

**What goes wrong:**
Operator profile pages work locally but fail on deployed site. XHR/fetch requests to `/api/operators/*` endpoints return 403 CORS error.

**Why it happens:**
You added operator API endpoint (`/api/operators/{operator_root}`) but didn't update CORS middleware. Current config allows all origins, but:
- CORS preflight (OPTIONS) might be rejected if not explicitly allowed
- If you restrict CORS origins later, operator endpoints forget to include them
- New endpoints aren't automatically covered by CORS; each must be explicitly allowed

**Consequences:**
- Operator profile pages show 403 error when fetching data
- Feature doesn't work in production even though it works in development (localhost)

**Prevention:**
- Keep CORS middleware at app level:
  ```python
  app.add_middleware(
      CORSMiddleware,
      allow_origins=["*"],  # or specific origins: ["https://yourdomain.com"]
      allow_methods=["GET", "POST", "OPTIONS"],  # Include OPTIONS for preflight
      allow_headers=["*"],
  )
  ```
- All routes (including new operator endpoints) inherit CORS config.
- Before launch, tighten to specific origins:
  ```python
  allow_origins=[
      "https://yourdomain.com",
      "https://www.yourdomain.com",
  ],
  ```

**Detection:**
- Browser Network tab shows OPTIONS request returning 403
- Console error: "Access to XMLHttpRequest blocked by CORS policy"
- Feature works locally (http://localhost:8000) but not on deployed domain

**Phase:** Address in Phase 2.2 (operator profiles) — verify CORS before launch.

---

## Phase-Specific Implementation Roadmap

| Phase | Critical Pitfalls | Must-Address | Nice-to-Have |
|-------|-------------------|--------------|--------------|
| **Phase 2.1: Deployment Hardening** | #20–24 (SSL, Nginx, gunicorn, caching) | SSL/TLS config, proxy headers, worker tuning, cache headers | Cert renewal automation, monitoring |
| **Phase 2.2: Operator Profiles** | #25–32 (data staleness, performance, routing, search, schema) | Data freshness pipeline, MapLibre optimization, routing strategy, schema design | SEO meta tags, slow load optimization, pagination |

---

## Sources

- [Cloudflare ERR_TOO_MANY_REDIRECTS Troubleshooting](https://developers.cloudflare.com/ssl/troubleshooting/too-many-redirects/)
- [Cloudflare Community: Redirect Loop Issues](https://community.cloudflare.com/t/website-in-redirect-loop-after-enabling-cloudflare-ssl/452212)
- [FastAPI Behind a Proxy](https://fastapi.tiangolo.com/advanced/behind-a-proxy/)
- [Nginx FastAPI Proxy Configuration](https://medium.com/@adebisiolayinka30/set-up-a-proxy-nginx-for-a-fastapi-application-part-two-26e30f7e9904)
- [Gunicorn Worker Optimization](https://medium.com/@mailtomugeshs/optimizing-gunicorn-balancing-threads-workers-and-connection-pools-for-better-performance-fbc682f731c4)
- [SQLAlchemy Connection Pooling](https://docs.sqlalchemy.org/en/20/core/pooling.html)
- [PostgreSQL Connection Memory Usage](https://goldlapel.com/how-to/connection-pooling)
- [Data Freshness and Stale Data Prevention](https://tacnode.io/post/what-is-stale-data)
- [Backfilling Data Pipelines](https://estuary.dev/blog/what-is-a-data-backfill/)
- [MapLibre Performance Optimization](https://maplibre.org/maplibre-gl-js/docs/guides/large-data/)
- [MapLibre GeoJSON Performance Issues](https://github.com/maplibre/maplibre-gl-js/issues/4364)
- [Vanilla JS SPA Routing Patterns](https://jsdev.space/spa-vanilla-js/)
- [Hash vs. Clean URL Routing](https://dev.to/thedevdrawer/single-page-application-routing-using-hash-or-url-9jh)
- [SQLAlchemy Schema Migrations](https://atlasgo.io/blog/2024/10/09/strategies-for-reliable-migrations)
- [Database Backfill Pitfalls](https://www.getgalaxy.io/learn/glossary/database-backfill)
