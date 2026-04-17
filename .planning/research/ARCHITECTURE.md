# Architecture: v2.0 Deployment Hardening + Operator Profiles

**Domain:** Civic intelligence map with production deployment and operator intelligence features
**Researched:** 2026-04-16
**Baseline:** v1.0 shipped; builds on existing monolith architecture

## Overview

v2.0 adds two major architectural concerns to the v1.0 monolith:

1. **Production Hardening** — Replace dev-mode uvicorn with Gunicorn workers, configure production Nginx with SSL/TLS, set up Let's Encrypt certificate management
2. **Operator Profiles** — New API endpoints for operator data, new database models or tables, new frontend routing for operator-specific pages

Both integrate cleanly into the existing monolith. No microservices, no fundamental restructuring. The core FastAPI + PostgreSQL + MapLibre pattern remains unchanged.

---

## Recommended Architecture

### Layer Diagram

```
[Browser]
    |
    |-- HTTPS request -->
    |
[Nginx reverse proxy (443 HTTPS, port 80 redirect)]
    |
    |-- proxy_pass http://127.0.0.1:8000
    |
[Gunicorn process manager (8000)]
    |
    ├─ Uvicorn worker 1 (async)
    ├─ Uvicorn worker 2 (async)
    └─ Uvicorn worker N (async)
    |
[FastAPI app (api/main.py)]
    |
    ├─ api/routes/neighborhoods.py
    ├─ api/routes/properties.py
    ├─ api/routes/blocks.py
    ├─ api/routes/operators.py  [NEW]
    ├─ api/routes/{other}.py
    |
    └─ frontend/index.html [SPA with client-side routing]
        ├─ Map view (default)
        ├─ Neighborhood panel
        ├─ Block detail panel
        ├─ Operator profile panel [NEW]
        └─ Address search
    |
[PostgreSQL + PostGIS]
    ├─ parcels, neighborhoods
    ├─ ownership_raw, permits_raw, evictions_raw, ...
    ├─ displacement_scores
    ├─ operators [NEW table]
    ├─ operator_addresses [NEW table]
    └─ operator_parcels [NEW]
    |
[OS cron (2am UTC)]
    └─ scheduler/main.py
        └─ scrapers/* + scoring/compute.py
```

### Component Changes

| Component | v1.0 Pattern | v2.0 Changes | Impact |
|-----------|-------------|-------------|--------|
| **App Server** | `uvicorn --reload` (dev mode) | Gunicorn + Uvicorn workers (production) | Configuration files only; no code changes |
| **Reverse Proxy** | None | Nginx with SSL/TLS, Certbot | Infrastructure; configuration-only |
| **Frontend Routing** | URL query params (?zip=, no SPA routing) | Client-side routing via History API | New JS router in frontend/index.html |
| **Static Files** | FastAPI mounts frontend/ with html=True | Same mount, upgraded for nested routes | FastAPI code unchanged if using SpaStaticFiles pattern |
| **API Layer** | 8 routes (neighborhoods, properties, blocks, etc.) | +1 operator routes file | New api/routes/operators.py |
| **Database** | 13 tables (civic data + scores) | +2–3 operator tables | New migrations; no changes to existing schemas |
| **Scheduler** | 8 scrapers + scoring | +0 (operator data backfilled, not nightly) | No changes; operator data is backfill-only for v2 |

---

## Component Boundaries (Updated)

| Component | Responsibility | Communicates With | v2.0 Changes |
|-----------|---------------|-------------------|--------------|
| `api/main.py` | FastAPI initialization, middleware, routing | All routes, models, database | Gunicorn wraps this; no code changes |
| `api/routes/operators.py` | Operator portfolio queries, individual profile data | Models (Operator, OperatorAddress), database | **NEW**: GET /api/operators, GET /api/operators/{slug} |
| `models/operators.py` | SQLAlchemy ORM for operator-related data | PostgreSQL operator tables | **NEW**: Operator, OperatorAddress, OperatorParcel models |
| `frontend/index.html` | SPA with client-side router, all views | API endpoints, map state | **UPDATED**: Add History API routing, operator profile view, router initialization |
| `scheduler/` | Nightly cron orchestration | Scrapers, scoring, database | Unchanged; operator backfill is one-time or manual |

---

## Data Flow: v2.0 Additions

### Operator Data Ingestion (One-Time Backfill)

```
scripts/top_operators_profiled.py (one-time)
    → Load operator investigation results from operator_network_analysis.json
    → Cross-reference with displacement_scores, ownership_raw, violations_raw
    → Generate operator profiles: LLC entities, portfolio size, evidence score, flag status
    → INSERT into operators table (manual or CI trigger)
    |
[operators table]
    ├─ operator_id (PK)
    ├─ operator_root (HABIB, BREDIF, PHANTOM, BATTALION, etc.)
    ├─ slug (habib, bredif, phantom, battalion)
    ├─ llc_count
    ├─ total_properties
    ├─ weighted_avg_displacement_score
    ├─ evidence_score (composite profiling score)
    ├─ metadata (JSON: tier_distribution, zip_concentration, eviction_then_buy_rate, etc.)
    └─ created_at
```

### Operator Profile API Request

```
Browser → GET /api/operators/{slug}
    → FastAPI route handler (api/routes/operators.py)
        → SQLAlchemy query: SELECT * FROM operators WHERE slug = {slug}
        → Join to parcels via ownership_raw for portfolio address list
        → Join to displacement_scores for neighborhood analysis
        → Pydantic serialization
        → JSON response: { operator, portfolio, displacement_analysis, flags }
    → Frontend router shows operator profile panel
        → Render portfolio map layer (all BBLs owned by operator)
        → Signal breakdown aggregated across operator's properties
        → Acquisition timeline
```

### Frontend Routing (Client-Side)

```
Window popstate event → history.pushState was called
    → URL changed from /operators/habib to /map
    → Router detects new pathname
    → Hides operator panel, shows map view
    → OR: URL has /operators/{slug}
    → Shows operator profile panel
    → Fetches operator data from /api/operators/{slug}
```

---

## Production Deployment Architecture

### Gunicorn Configuration

```bash
gunicorn \
    -w 2 \
    -k uvicorn.workers.UvicornWorker \
    --bind 127.0.0.1:8000 \
    --preload-app \
    --max-requests 1000 \
    --max-requests-jitter 50 \
    --graceful-timeout 30 \
    --timeout 60 \
    api.main:app
```

**Rationale:**
- `-w 2`: 2 workers (DigitalOcean Droplet has 2 vCPUs; formula is 1× CPU_cores for async workers)
- `-k uvicorn.workers.UvicornWorker`: ASGI worker for async FastAPI
- `--bind 127.0.0.1:8000`: Listen only on localhost; Nginx forwards from port 443
- `--preload-app`: Load application before forking workers (copy-on-write memory optimization)
- `--max-requests 1000`: Restart workers after 1000 requests to prevent memory leaks
- `--graceful-timeout 30`: Give worker 30s to finish before killing
- `--timeout 60`: Kill worker if request takes >60s (protection against hung requests)

**Process Supervision:** Use systemd service or supervisor to restart Gunicorn if it crashes. Example systemd unit file in `.planning/docs/gunicorn.service`.

### Nginx Configuration

```nginx
upstream fastapi {
    server 127.0.0.1:8000;
}

# Redirect HTTP → HTTPS
server {
    listen 80;
    listen [::]:80;
    server_name pulsecities.com www.pulsecities.com;

    location /.well-known/acme-challenge/ {
        # Certbot verification
        root /var/www/certbot;
    }

    location / {
        return 301 https://$server_name$request_uri;
    }
}

# HTTPS server
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name pulsecities.com www.pulsecities.com;

    # SSL certificates from Let's Encrypt / Certbot
    ssl_certificate /etc/letsencrypt/live/pulsecities.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/pulsecities.com/privkey.pem;

    # Security headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-XSS-Protection "1; mode=block" always;

    # API proxy
    location /api/ {
        proxy_pass http://fastapi;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_redirect off;
    }

    # SPA fallback: all non-API requests → index.html
    location / {
        proxy_pass http://fastapi;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

**Key points:**
- Certbot uses ACME challenges in `/.well-known/acme-challenge/` before redirecting HTTP → HTTPS
- All routes (API and SPA) proxy to Gunicorn on localhost:8000
- FastAPI/Starlette's `html=True` mode on the root mount handles SPA routing (serves index.html for unknown paths)
- Security headers prevent MIME type sniffing, clickjacking, XSS

### SSL/TLS with Let's Encrypt

**Initial Setup:**
```bash
sudo apt-get update && sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot certonly --standalone -d pulsecities.com -d www.pulsecities.com
# or
sudo certbot --nginx -d pulsecities.com -d www.pulsecities.com
```

**Auto-renewal (via systemd timer):**
Certbot installs a systemd timer that runs `certbot renew` daily. Certificates are renewed 30 days before expiration.

**Verification:**
```bash
certbot certificates
```

**Cloudflare Integration Options:**

| Option | SSL Between | Pros | Cons |
|--------|-------------|------|------|
| **Cloudflare-only (flexible)** | Browser–Cloudflare: HTTPS; Cloudflare–Origin: HTTP | No cert on origin | Less secure; data exposed on Cloudflare–origin link |
| **Full (strict)** | Browser–Cloudflare: HTTPS; Cloudflare–Origin: HTTPS + Cloudflare origin cert | Most secure; Cloudflare validates origin cert | Requires Cloudflare origin cert on origin VPS |
| **Let's Encrypt + Full (strict)** | Browser–Cloudflare: HTTPS; Cloudflare–Origin: HTTPS + Let's Encrypt cert | Standard; industry practice | Requires cert renewal automation |

**Recommendation for v2.0:** Use Let's Encrypt + Nginx + Full (strict). This is the simplest, most standard approach and doesn't lock you into Cloudflare's certificate ecosystem.

---

## Frontend Architecture: Client-Side Routing

### SPA Router Implementation

**Current state:** v1.0 uses `URLSearchParams` to read `?zip=` params and `history.pushState` to update URL on click, but no full SPA router.

**v2.0 needs:** A client-side router that:
1. Parses the current URL on page load
2. Handles `popstate` events (back button)
3. Maps routes to view components (map view, operator profile, neighborhood panel, block panel)
4. Updates URL on view changes without full page reload

**Architecture:**
```javascript
// Routes definition
const routes = {
    '/map': showMapView,
    '/map?zip={zipCode}': showNeighborhoodPanel,
    '/blocks/{bbl}': showBlockPanel,
    '/operators': showOperatorList,
    '/operators/{slug}': showOperatorProfile,
    '*': redirectToMap  // default
};

// Router instance
class Router {
    constructor() {
        this.currentPath = window.location.pathname;
        this.currentSearch = window.location.search;
        window.addEventListener('popstate', () => this.render());
        // Route on page load
        this.render();
    }

    navigate(path) {
        history.pushState(null, '', path);
        this.render();
    }

    render() {
        // Parse URL → detect current route
        // Hide all panels, show the one matching current route
        // Fetch API data if needed
    }
}

window.router = new Router();

// Usage: user clicks "View Operator" → router.navigate('/operators/habib')
```

**Why not a library?** For v2.0, a custom lightweight router is sufficient. Operators are a secondary feature; adding a SPA framework (Vue, React) is overkill and would require a build pipeline. Client-side routing is straightforward with vanilla JS.

**SPA Static Files Handling:**

FastAPI already mounts static files at `/` with `html=True`:
```python
app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
```

The `html=True` parameter serves `index.html` for directory requests, which enables SPA routing — requests to `/operators/habib` (a non-existent file) will fall back to `/index.html`, and the JS router takes over.

**Alternative if needed:** Implement a custom `SpaStaticFiles` class that catches 404s and returns `index.html`. This gives more control over fallback behavior (e.g., exclude API routes from fallback).

---

## Operator Data Model

### Database Tables (New)

```sql
-- Operator root entity
CREATE TABLE operators (
    operator_id SERIAL PRIMARY KEY,
    operator_root VARCHAR(100) UNIQUE NOT NULL,  -- HABIB, BREDIF, PHANTOM, BATTALION
    slug VARCHAR(100) UNIQUE NOT NULL,            -- habib, bredif, phantom, battalion
    llc_count INT,
    total_properties INT,
    total_acquisitions INT,
    total_portfolio_value NUMERIC(15, 2),
    avg_acquisition_price NUMERIC(15, 2),
    acquisitions_per_month NUMERIC(8, 2),
    first_acquisition DATE,
    last_acquisition DATE,
    weighted_avg_displacement_score FLOAT,
    evidence_score FLOAT,  -- Composite profiling score (30 pts HD zip, 25 pts eviction-then-buy, etc.)
    metadata JSONB,  -- { tier_distribution, zip_concentration, eviction_then_buy_rate, class_bc_violation_density, etc. }
    status VARCHAR(50),  -- 'active', 'confirmed', 'flag_for_review'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- LLC entities under each operator
CREATE TABLE operator_llcs (
    operator_llc_id SERIAL PRIMARY KEY,
    operator_id INT REFERENCES operators(operator_id),
    llc_name_normalized VARCHAR(200) NOT NULL,  -- From ownership_raw.party_name_normalized
    llc_name_raw VARCHAR(200),  -- Source name variants
    property_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(operator_id, llc_name_normalized),
    INDEX idx_operator_llcs_operator (operator_id),
    INDEX idx_operator_llcs_normalized (llc_name_normalized)
);

-- Operator's properties (parcels)
CREATE TABLE operator_parcels (
    operator_parcel_id SERIAL PRIMARY KEY,
    operator_id INT REFERENCES operators(operator_id),
    bbl VARCHAR(10) REFERENCES parcels(bbl),
    acquisition_date DATE,  -- From ownership_raw.doc_date
    acquisition_price NUMERIC(15, 2),  -- From ownership_raw.doc_amount
    acquiring_llc_id INT REFERENCES operator_llcs(operator_llc_id),
    displacement_score FLOAT,  -- Current score for this BBL's ZIP code
    is_speculation_pattern BOOLEAN,  -- True if eviction on BBL ≤ 12 months before acquisition
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_operator_parcels_operator (operator_id),
    INDEX idx_operator_parcels_bbl (bbl),
    INDEX idx_operator_parcels_date (acquisition_date)
);
```

### Pydantic Response Schemas (New)

```python
# api/routes/operators.py

from pydantic import BaseModel
from typing import List, Optional

class OperatorSummary(BaseModel):
    operator_root: str
    slug: str
    total_properties: int
    weighted_avg_displacement_score: float
    evidence_score: float

class OperatorProfile(BaseModel):
    operator_root: str
    slug: str
    llc_count: int
    total_properties: int
    total_acquisitions: int
    total_portfolio_value: float
    avg_acquisition_price: float
    acquisitions_per_month: float
    first_acquisition: str  # ISO date
    last_acquisition: str   # ISO date
    weighted_avg_displacement_score: float
    evidence_score: float
    status: str
    metadata: dict  # tier_distribution, zip_concentration, eviction_then_buy_rate, etc.

class OperatorAddressRecord(BaseModel):
    bbl: str
    address: str
    zip_code: str
    acquisition_date: str
    acquisition_price: Optional[float]
    displacement_score: Optional[float]
    is_speculation_pattern: bool

class OperatorDetailResponse(BaseModel):
    operator: OperatorProfile
    properties: List[OperatorAddressRecord]
    portfolio_geographic_breakdown: dict  # { zip_code: count }
    portfolio_score_tiers: dict  # { "80_plus": count, "60_79": count, ... }
```

---

## API Endpoints (New)

### GET /api/operators

**Purpose:** List all profiled operators (for future operator search/filter UI)

**Response:**
```json
{
  "operators": [
    {
      "operator_root": "HABIB",
      "slug": "habib",
      "total_properties": 40,
      "weighted_avg_displacement_score": 48.0,
      "evidence_score": 72.5
    },
    ...
  ]
}
```

**Rate limit:** 30/minute

### GET /api/operators/{slug}

**Purpose:** Detailed profile for a specific operator, including full property portfolio and geographic breakdown

**Path parameter:** `slug` — lowercase operator identifier (habib, bredif, phantom, battalion)

**Response:**
```json
{
  "operator": {
    "operator_root": "HABIB",
    "slug": "habib",
    "llc_count": 10,
    "total_properties": 40,
    "total_acquisitions": 45,
    "total_portfolio_value": 3957000.0,
    "avg_acquisition_price": 87933.33,
    "acquisitions_per_month": 4.18,
    "first_acquisition": "2025-04-14",
    "last_acquisition": "2026-03-03",
    "weighted_avg_displacement_score": 48.0,
    "evidence_score": 72.5,
    "status": "active",
    "metadata": {
      "tier_distribution": {
        "score_80_plus": 0,
        "score_60_79": 7,
        "score_40_59": 16,
        "score_under_40": 8
      },
      "zip_concentration": {
        "11212": 4,
        "10458": 3,
        ...
      },
      "eviction_then_buy_rate": 0.32,
      "class_bc_violation_density": 0.18
    }
  },
  "properties": [
    {
      "bbl": "3012340001",
      "address": "796 Sterling Place",
      "zip_code": "11216",
      "acquisition_date": "2025-08-15",
      "acquisition_price": 850000.0,
      "displacement_score": 58.5,
      "is_speculation_pattern": true
    },
    ...
  ],
  "portfolio_geographic_breakdown": {
    "11212": 4,
    "10458": 3,
    "11203": 2,
    ...
  },
  "portfolio_score_tiers": {
    "score_80_plus": 0,
    "score_60_79": 7,
    "score_40_59": 16,
    "score_under_40": 8
  }
}
```

**Rate limit:** 60/minute

**Error handling:**
- 404 if operator slug not found
- 400 if slug invalid format

---

## Build Order & Phase Structure

### Phase 1: Database & API (Dependency foundation)

1. **Create operator migrations** — `alembic revision --autogenerate -m "add operators table"`
   - operators, operator_llcs, operator_parcels tables
   - Indexes on operator_id, bbl, normalized LLC name
   - Backfill: Load from `scripts/top_operators_profiled.json` or via CLI script

2. **Create Pydantic schemas** — `api/models/operators.py` (NOT to be confused with SQLAlchemy models)
   - OperatorProfile, OperatorAddressRecord, OperatorDetailResponse

3. **Create operator routes file** — `api/routes/operators.py`
   - GET /api/operators
   - GET /api/operators/{slug}
   - Query builders, error handling

4. **Wire routes into main.py** — `app.include_router(operators.router, prefix="/api")`

### Phase 2: Frontend Routing & UI

1. **Implement client-side router** in `frontend/index.html`
   - Parse URL on load (History API + popstate)
   - Route definitions (map view, operator profile view, etc.)
   - View state management (which panel is visible)

2. **Add operator profile view panel** to HTML
   - Portfolio grid/table of properties
   - Acquisition timeline
   - Geographic breakdown
   - Score tier distribution

3. **Add operator search/list view** (optional; can defer to v2.1)
   - Fetch /api/operators
   - Show searchable operator list
   - Click → navigate to /operators/{slug}

4. **Update map interaction**
   - User clicks neighborhood → show neighborhood panel (existing)
   - User clicks "View Operator" button in a property → navigate to /operators/{slug}
   - Operator profile view highlights operator's parcels on map

### Phase 3: Production Hardening

1. **Systemd service file for Gunicorn**
   - Create `/etc/systemd/system/pulsecities.service`
   - Restart behavior, user/group, working directory

2. **Nginx configuration**
   - HTTP → HTTPS redirect
   - SSL certificates (Let's Encrypt)
   - Proxy headers, security headers
   - Deploy to `/etc/nginx/sites-available/pulsecities` and enable

3. **Certbot setup**
   - Initial certificate generation
   - Systemd timer for auto-renewal

4. **Environment configuration**
   - Create `.env.production` with `ENVIRONMENT=production`
   - FastAPI code checks environment to disable CORS open-wildcard, enable stricter headers

5. **Test & validation**
   - Hit https://pulsecities.com in browser
   - Verify map loads, neighborhoods render, operator profiles load
   - Check certificate validity: `certbot certificates`

---

## Integration Points with v1.0

### What Changes

| Component | v1.0 | v2.0 | Breaking? |
|-----------|------|------|-----------|
| App server | uvicorn --reload | gunicorn + uvicorn workers | No — just deployment |
| Nginx | None | Added | Infrastructure only |
| FastAPI main.py | 8 routes included | +1 operator route included | No — additive |
| frontend/index.html | URL params (?zip=) | Client-side router + params | No — extends existing |
| PostgreSQL | 13 tables | +3 operator tables | No — additive; migrations handle it |
| Scheduler | 8 scrapers + scoring | No changes | None |

### What Stays the Same

- **Displacement scoring logic** — unchanged; operators don't affect score computation
- **Existing API routes** — neighborhoods, properties, blocks, search, pulse — all unchanged
- **Data ingestion** — nightly cron runs the same scrapers
- **Map rendering** — MapLibre configuration unchanged
- **Frontend styling** — DaisyUI, Tailwind remain the same

---

## Key Architectural Decisions for v2.0

### Decision 1: Gunicorn + Uvicorn Workers Over Other Options

**Chosen:** Gunicorn as process manager with Uvicorn ASGI workers

**Alternatives considered:**
- Uvicorn with multiple processes (--workers flag) — Works, but Gunicorn is more mature for process management, graceful restarts, and resource limits
- APScheduler in-process — Wrong; couples API uptime to scraper job completion; we use system cron
- Docker container — Good for v3; adds complexity for now; VPS manual setup is fine for v2

**Why:** Gunicorn is the industry standard for Python production. The API is I/O-bound (database queries, HTTP proxy), so async Uvicorn workers are efficient. Graceful restarts allow zero-downtime deployments.

### Decision 2: Let's Encrypt Certificates on Origin Over Cloudflare-only

**Chosen:** Let's Encrypt certificates on DigitalOcean VPS; Nginx terminates SSL; Cloudflare set to Full (strict)

**Alternatives considered:**
- Cloudflare-only (flexible SSL) — Simpler DNS config, but less secure; data exposed on Cloudflare–origin link
- Cloudflare origin certificates — Vendor lock-in; Cloudflare can revoke certs
- AWS Certificate Manager — Unnecessary; adds AWS dependency for a simple use case

**Why:** Let's Encrypt is free, open, and doesn't lock you to a vendor. Full (strict) to Cloudflare means the entire chain is encrypted. Certbot auto-renewal is rock-solid. Standard industry practice.

### Decision 3: Client-Side Router in Vanilla JS Over Framework

**Chosen:** Custom lightweight router in vanilla JS, no framework

**Alternatives considered:**
- React/Next.js SPA framework — Overkill for operator profiles; requires build pipeline, npm, bundle.js
- Vue 3 — Same concern; adds tooling complexity
- Hash-based routing (#/operators/habib) — Works, but ugly URLs and bad for SEO

**Why:** Operators are a secondary feature in v2.0. The map UI is straightforward. A custom router in 200 lines of vanilla JS is maintainable, has zero dependencies, and doesn't require a build pipeline. If operator features expand significantly in v3+, then re-evaluate a framework.

### Decision 4: Operator Data as Backfill, Not Nightly Scraper

**Chosen:** Operator data loaded once from `scripts/top_operators_profiled.py` output; no nightly updates

**Alternatives considered:**
- Operator profiling as part of nightly pipeline — Over-engineered; operator intelligence is investigative, not time-sensitive
- Manual UI for operator data entry — Not scalable
- External operator intelligence API — Doesn't exist for NYC

**Why:** Operator profiling is computationally expensive (cross-references ownership, displacements, evictions, violations). The data doesn't change rapidly — operators don't acquire/divest weekly. Backfill once per phase, re-run as needed for updates. Keep nightly pipeline focused on civic signals.

### Decision 5: New Database Tables vs JSONB Columns

**Chosen:** Normalized `operators`, `operator_llcs`, `operator_parcels` tables

**Alternatives considered:**
- Single `operators` table with `parcels` and `llcs` as JSONB arrays — Simpler schema, harder to query
- Flat denormalized table — No; data redundancy leads to sync issues

**Why:** Normalized schema enables:
- Filtering operators by property count, score, etc. via SQL
- Searching for properties by operator (reverse lookup)
- Analytics queries (aggregate portfolio stats)
- Future features (operator comparison, ranking)

JSONB metadata column stores profiling artifacts (tier distribution, zip concentration) that don't need to be queryable at the SQL level.

---

## Pitfalls & Mitigations

### Pitfall 1: Gunicorn Worker Timeouts on Slow Queries

**What goes wrong:** A neighborhood query that aggregates displacement data from multiple sources takes >30s. The load balancer thinks the worker is hung and marks it unhealthy.

**Mitigation:**
- Set `--timeout 60` in Gunicorn config (give slow queries 60s)
- Optimize database queries with proper indexes (already done in v1.0)
- Monitor query performance; add caching for expensive aggregations in v2.1

### Pitfall 2: Let's Encrypt Certificate Expiration on Unmonitored VPS

**What goes wrong:** Certbot renewal fails silently (e.g., DNS not resolving); certificate expires 90 days later; site goes down with "certificate expired" error.

**Mitigation:**
- Enable systemd timer for certbot: `sudo systemctl enable certbot.timer`
- Set up email alerts: Let's Encrypt sends renewal reminders
- CI/CD or monitoring script: monthly check that cert expiration is >30 days away
- Document renewal process in project README

### Pitfall 3: SPA Router Broken by Browser History API Incompatibility

**What goes wrong:** Older browsers or certain edge cases don't support History API. User clicks a link in operator profile, URL changes but content doesn't (or content changes but URL doesn't).

**Mitigation:**
- Use standard History API patterns; avoid Edge cases
- Test in major browsers (Chrome, Firefox, Safari, Edge)
- Fallback: if History API unavailable, degrade to hash-based routing (#/operators/habib)
- Document supported browsers

### Pitfall 4: Operator Data Stale After Backfill

**What goes wrong:** Operator profiles are loaded once. Months later, a tracked operator acquires new properties, but the profile doesn't update — users see outdated info.

**Mitigation:**
- Document that operator data is "as of [backfill date]"
- Add `backfill_date` field to operators table
- Manual re-backfill process (script to re-run `top_operators_profiled.py`)
- v2.1: Add monthly operator re-profiling scraper if data freshness becomes critical

### Pitfall 5: Nginx Proxy Cache Breaking SPA Routes

**What goes wrong:** Nginx caches responses to `/operators/habib` (thinking it's static content), then serves the cached response to requests for `/operators/bredif`.

**Mitigation:**
- Don't enable Nginx caching for dynamic routes (API + SPA)
- Use Cache-Control headers from FastAPI: `Cache-Control: no-cache, no-store`
- If caching is added later, include cache-bust headers (ETag, Last-Modified)

### Pitfall 6: Cloudflare Minification Breaking JavaScript

**What goes wrong:** Cloudflare's auto-minification breaks the frontend JS router (unbalanced braces, identifier collisions).

**Mitigation:**
- Disable Cloudflare's JavaScript auto-minification (Speed → Optimization → Off for now)
- If minification is needed, use explicit minification in build pipeline (v3+)
- Test in Chrome DevTools with Cloudflare enabled

---

## Validation Checklist

- [ ] Gunicorn starts without errors; `ps aux | grep gunicorn` shows 2+ worker processes
- [ ] Nginx forwards HTTP → HTTPS correctly; `curl -I http://pulsecities.com` returns 301 to https://
- [ ] Let's Encrypt certificate is valid; `certbot certificates` shows expiration >30 days away
- [ ] `/api/operators` endpoint returns operator list (200)
- [ ] `/api/operators/{slug}` returns full profile for valid slug (200); 404 for invalid slug
- [ ] Browser loads https://pulsecities.com without certificate warnings
- [ ] Map renders, neighborhoods clickable
- [ ] `/operators/habib` URL loads operator profile panel (no 404, no refresh)
- [ ] Back button works; browser history is correct
- [ ] Operator parcels render on map with distinct highlighting

---

## Deployment Checklist (Phase 3)

- [ ] Create systemd service file at `/etc/systemd/system/pulsecities.service`
- [ ] Create Gunicorn config file (or inline args)
- [ ] Nginx config deployed to `/etc/nginx/sites-available/pulsecities`
- [ ] DNS A record points to VPS static IP (necessary before Certbot)
- [ ] Certbot installed: `sudo apt-get install -y certbot python3-certbot-nginx`
- [ ] Certificate generated: `sudo certbot certonly --standalone -d pulsecities.com -d www.pulsecities.com`
- [ ] Nginx SSL paths updated to point to Certbot certs
- [ ] Nginx reloaded: `sudo systemctl reload nginx`
- [ ] Gunicorn started: `sudo systemctl start pulsecities`
- [ ] Health check passes: `curl https://pulsecities.com/api/health` returns 200
- [ ] Operator API tested: `curl https://pulsecities.com/api/operators` returns JSON
- [ ] Frontend loads: `curl https://pulsecities.com` returns HTML with map

---

## Sources

**FastAPI & Gunicorn:**
- [FastAPI Production Deployment Best Practices](https://render.com/articles/fastapi-production-deployment-best-practices)
- [Mastering Gunicorn and Uvicorn](https://medium.com/@iklobato/mastering-gunicorn-and-uvicorn-the-right-way-to-deploy-fastapi-applications-aaa06849841e)
- [Deploy FastAPI with NGINX and Gunicorn (2025)](https://medium.com/@kevinzeladacl/deploy-a-fastapi-app-with-nginx-and-gunicorn-b66ac14cdf5a)
- [FastAPI Server Workers](https://fastapi.tiangolo.com/deployment/server-workers/)

**Nginx & SSL:**
- [Setting Up FastAPI with NGINX Reverse Proxy](https://dev.to/udara_dananjaya/setting-up-a-fastapi-project-with-nginx-reverse-proxy-on-ubuntu-883)
- [Reverse Proxy with Nginx and Certbot](https://dev.to/01kg/reverse-proxy-host-your-app-with-nginx-and-certbot-gln)
- [Run FastAPI with Uvicorn and Nginx (Ubuntu 24.04)](https://www.hostmycode.com/tutorials/run-fastapi-with-uvicorn-and-nginx-on-ubuntu-2404)

**Client-Side Routing:**
- [Build a Single-Page Application Router in Vanilla JavaScript](https://jsdev.space/spa-vanilla-js/)
- [Client-Side Routing in Vanilla JS](https://www.willtaylor.blog/client-side-routing-in-vanilla-js/)
- [Serving SPAs from Starlette](https://www.crccheck.com/blog/serving-spas-from-starlette/)
- [StaticFiles with html=True](https://fastapi.tiangolo.com/tutorial/static-files/)

**FastAPI Static Files & Routing:**
- [Serving Static Files in FastAPI](https://www.slingacademy.com/article/serving-static-files-in-fastapi/)
- [Developing a Single Page App with FastAPI and Vue.js](https://testdriven.io/blog/developing-a-single-page-app-with-fastapi-and-vuejs/)
