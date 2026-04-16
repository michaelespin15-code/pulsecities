# Stack Research: v2.0 Production Deployment & Operator Profiles

**Project:** PulseCities v2.0 — Production Nginx + SSL + Operator Portfolio Pages
**Researched:** 2026-04-16
**Confidence:** HIGH

## Executive Summary

PulseCities v1.0 has validated the core FastAPI + PostgreSQL + MapLibre stack (documented in STACK.md). For v2.0, production deployment and operator profile pages require **only additions**, not changes to the validated stack.

**Zero breaking changes.** All additions are deployment-layer (Nginx, certbot, systemd) and frontend-only (profile page HTML/JS). Database models, API framework, and ORM remain unchanged.

### Key Addition: Gunicorn Process Manager

Gunicorn 21.2.0 is already in requirements.txt but not yet used. Moving from development (uvicorn --reload) to production requires Gunicorn + Uvicorn workers, which is a deployment concern, not a code change.

### Key Addition for Operator Pages: PostGIS Geometry Serialization

Operator portfolio pages need to render parcel polygons from PLUTO geometries. This requires:
- **GeoAlchemy2 (already v0.14.3)**: Query PostGIS geometry columns
- **func.ST_AsGeoJSON()**: Serialize geometries to GeoJSON in the database query
- **MapLibre GL JS 5.2.0 (already in CDN)**: Render GeoJSON layers

No new Python dependencies needed beyond what v1.0 already validated.

---

## Production Deployment Stack (New Infrastructure)

### Gunicorn + Systemd (Process Management)

| Technology | Version | Purpose | Why This Stack |
|------------|---------|---------|-----------------|
| Gunicorn | 21.2.0 | WSGI/ASGI process manager | Manages worker lifecycle, socket binding, graceful restarts; APScheduler requires single worker |
| Uvicorn worker | 0.44.0 | ASGI worker class | Async request handling; no built-in process management (that's Gunicorn's job) |
| Systemd | native (Ubuntu) | Process supervision | Auto-start on boot, auto-restart on crash, dependency ordering |

### Nginx + Certbot (Reverse Proxy + SSL)

| Technology | Version | Purpose | Why This Stack |
|------------|---------|---------|-----------------|
| Nginx | 1.24+ (apt) | Reverse proxy, static file serving | Standard; fast; efficient SSL termination; connection pooling to Gunicorn |
| Certbot | 2.x (apt) | Let's Encrypt automation | Free SSL, automatic 90-day renewal via systemd timer |
| Let's Encrypt | n/a | Certificate authority | Industry standard; no paid license required |

---

## Installation & Configuration

### 1. Install System Packages

```bash
# On DigitalOcean Ubuntu 22.04+
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx
sudo systemctl enable nginx
sudo systemctl start nginx
```

### 2. Create Gunicorn Systemd Service

Create `/etc/systemd/system/pulsecities.service`:

```ini
[Unit]
Description=PulseCities FastAPI Application
After=network.target postgresql.service

[Service]
Type=notify
User=www-data
Group=www-data
WorkingDirectory=/var/www/pulsecities
Environment="PATH=/var/www/pulsecities/venv/bin"
Environment="DATABASE_URL=postgresql://pulsecities_user:PASSWORD@localhost/pulsecities"
ExecStart=/var/www/pulsecities/venv/bin/gunicorn \
  -w 1 \
  -k uvicorn.workers.UvicornWorker \
  --bind unix:/var/www/pulsecities/pulsecities.sock \
  --timeout 120 \
  --access-logfile /var/log/pulsecities/access.log \
  --error-logfile /var/log/pulsecities/error.log \
  api.main:app
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable pulsecities
sudo systemctl start pulsecities
sudo systemctl status pulsecities
```

**Critical flag: `-w 1`** (single worker)
- APScheduler runs inside the Gunicorn process; multiple workers = multiple scheduler instances = duplicate jobs
- Uvicorn workers handle thousands of concurrent async requests via event loops; single worker is sufficient
- CPU is not the bottleneck; I/O concurrency within the worker is

### 3. Configure Nginx Reverse Proxy

Create `/etc/nginx/sites-available/pulsecities`:

```nginx
upstream pulsecities {
    server unix:/var/www/pulsecities/pulsecities.sock fail_timeout=0;
}

server {
    listen 80;
    server_name pulsecities.com www.pulsecities.com;
    client_max_body_size 10M;

    # Redirect HTTP to HTTPS (after certbot setup)
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name pulsecities.com www.pulsecities.com;
    client_max_body_size 10M;

    # SSL certificates (auto-configured by certbot)
    ssl_certificate /etc/letsencrypt/live/pulsecities.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/pulsecities.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;

    # Proxy to Gunicorn
    location / {
        proxy_pass http://pulsecities;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_buffering off;
        proxy_request_buffering off;
    }

    # Static assets (frontend)
    location /frontend/ {
        alias /var/www/pulsecities/frontend/;
        expires 1h;
        add_header Cache-Control "public, immutable";
    }

    # API endpoints
    location /api/ {
        proxy_pass http://pulsecities;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }
}
```

Enable:
```bash
sudo ln -s /etc/nginx/sites-available/pulsecities /etc/nginx/sites-enabled/pulsecities
sudo nginx -t  # Verify syntax
sudo systemctl reload nginx
```

### 4. Obtain SSL Certificate with Certbot

```bash
# Auto-configure Nginx with Let's Encrypt certificate
sudo certbot --nginx -d pulsecities.com -d www.pulsecities.com

# Verify auto-renewal is configured
sudo systemctl status certbot.timer
sudo certbot renew --dry-run  # Test renewal (no actual cert update)
```

Certbot automatically:
- Updates `/etc/nginx/sites-available/pulsecities` with SSL configuration
- Installs systemd timer for automatic renewal every 12 hours
- Restarts Nginx after successful renewal

---

## Architecture Decisions

### Why Gunicorn + Uvicorn (Not Uvicorn Standalone)

**Uvicorn alone** is a single-process ASGI server. Production requirements:

| Requirement | Uvicorn Alone | Gunicorn + Uvicorn |
|-------------|---------------|-------------------|
| Process management | ❌ No | ✓ Yes |
| Worker restart on crash | ❌ Manual | ✓ Automatic |
| Unix socket binding | ❌ No (TCP only) | ✓ Yes |
| Graceful reload | ❌ No | ✓ Yes (zero-downtime) |
| Process supervision via systemd | ❌ Limited | ✓ Full integration |

Gunicorn provides process management layer; Uvicorn provides async ASGI handling. Together: production-grade.

### Why Single Worker (`-w 1`)

FastAPI + APScheduler + Gunicorn normally would use `-w 2` to `-w 4` (multiple processes for concurrency). **Not here:**

1. **APScheduler is process-global**: If you have 4 Gunicorn workers, APScheduler runs 4 times. Same cron job fires 4 times. Data duplication.
   - Solution: `-w 1` — single Python process, single APScheduler instance.

2. **Async concurrency is within-process**: Uvicorn workers use async/await. A single Uvicorn worker with `asyncio` event loop handles thousands of concurrent requests.
   - Traditional sync frameworks: CPU cores = max concurrent requests; use 4 workers on 4-core CPU
   - Async frameworks: Event loop = thousands of concurrent requests; 1 worker is enough

3. **2 vCPU DigitalOcean is not CPU-limited**: Memory and I/O are the bottlenecks. Single worker is more efficient.

### Why Unix Socket (Not TCP localhost:8000)

| Factor | Unix Socket | TCP Socket |
|--------|-------------|-----------|
| Port allocation | No port needed | Requires port 8000 |
| Inter-process communication | Kernel file descriptor | Network stack overhead |
| Performance | Faster (files are faster) | Slower (network stack) |
| Nginx to Gunicorn | Efficient on same host | Adds latency, port contention |
| Iptables/firewall | Not exposed externally | Could accidentally expose |

For **local communication** (Nginx ↔ Gunicorn on same VPS), Unix socket is superior. Cloudflare handles external SSL; Nginx handles internal HTTPS if needed.

### Why Nginx (Not Traefik, Caddy, Apache)

| Feature | Nginx | Traefik | Caddy | Apache |
|---------|-------|---------|-------|--------|
| Docker-native | No (bare metal) | Yes (container-first) | Yes | No |
| Configuration complexity | Simple | Moderate | Simple | Complex |
| SSL automation | Certbot integration | Native via LetsEncrypt | Native | Manual |
| Static file serving | Excellent | Moderate | Good | Moderate |
| Connection pooling | Configurable | Limited | Good | Limited |

**PulseCities is bare-metal on DigitalOcean**, not containerized. Nginx + Certbot is the standard, battle-tested approach.

---

## Operator Profile Pages: No New Libraries

### Data Flow

```
Browser Request
  ↓
Nginx (static HTML)
  ↓
JavaScript fetches /api/operators/{llc_name}/portfolios
  ↓
Gunicorn → FastAPI Route Handler
  ↓
SQLAlchemy + GeoAlchemy2 query:
  - ownership_raw: Find all BBLs by party_name_normalized
  - properties: Join to get PLUTO geometry (geom column)
  - func.ST_AsGeoJSON(geom): Serialize in database
  ↓
JSON response with GeoJSON FeatureCollection
  ↓
Browser: MapLibre GL JS renders GeoJSON layer with parcel outlines
```

### Database Query Example

```python
from geoalchemy2 import Geometry
from models.ownership import OwnershipRaw
from models.properties import Property
from sqlalchemy import func, distinct

# No new imports needed — all already in v1.0

@router.get("/api/operators/{llc_name}/portfolio-map")
async def operator_portfolio_map(llc_name: str, db: Session = Depends(get_db)):
    # Normalize LLC name (same logic as search endpoint)
    llc_normalized = normalize_party_name(llc_name)
    
    # Get all BBLs owned by this LLC (from ACRIS deeds)
    bbl_subquery = (
        db.query(distinct(OwnershipRaw.bbl))
        .filter(OwnershipRaw.party_name_normalized == llc_normalized)
        .filter(OwnershipRaw.party_type == "GRANTEE")
        .subquery()
    )
    
    # Join to PLUTO for geometry, serialize as GeoJSON in database
    parcels = db.query(
        Property.bbl,
        func.ST_AsGeoJSON(Property.geom).label("geom"),
        Property.address,
        Property.zip_code
    ).filter(Property.bbl.in_(db.query(bbl_subquery))).all()
    
    features = [
        {
            "type": "Feature",
            "geometry": json.loads(parcel.geom),  # func.ST_AsGeoJSON returns JSON string
            "properties": {
                "bbl": parcel.bbl,
                "address": parcel.address,
                "zip_code": parcel.zip_code
            }
        }
        for parcel in parcels
    ]
    
    return {
        "type": "FeatureCollection",
        "features": features
    }
```

**Libraries used:**
- **SQLAlchemy 2.0.49** (already pinned): ORM queries, join syntax
- **GeoAlchemy2 0.14.3** (already pinned): `func.ST_AsGeoJSON()` function
- **Python json** (stdlib): JSON serialization of geometries
- **Shapely 2.0.4** (already pinned): Optional for in-Python geometry calculations (not required here)

### Frontend Rendering

```html
<!-- operator-profile.html -->
<div id="map" style="height: 500px;"></div>

<script src="https://unpkg.com/maplibre-gl@5.2.0/dist/maplibre-gl.js"></script>
<script src="https://unpkg.com/maplibre-gl@5.2.0/dist/maplibre-gl.css"></script>

<script>
const map = new maplibregl.Map({
    container: 'map',
    style: 'https://demotiles.maplibre.org/style.json',
    center: [-73.935242, 40.730610],  // NYC center
    zoom: 10
});

map.on('load', async () => {
    // Fetch operator portfolio from API
    const response = await fetch(`/api/operators/${llcName}/portfolio-map`);
    const geoJson = await response.json();
    
    // Add source
    map.addSource('operator-parcels', {
        type: 'geojson',
        data: geoJson
    });
    
    // Add layer
    map.addLayer({
        id: 'operator-parcels-fill',
        type: 'fill',
        source: 'operator-parcels',
        paint: {
            'fill-color': '#ff6b6b',
            'fill-opacity': 0.5
        }
    });
    
    map.addLayer({
        id: 'operator-parcels-outline',
        type: 'line',
        source: 'operator-parcels',
        paint: {
            'line-color': '#ff6b6b',
            'line-width': 2
        }
    });
});
</script>
```

**Libraries used:**
- **MapLibre GL JS 5.2.0** (already in CDN): Map rendering, GeoJSON layer support
- **Vanilla JavaScript** (no new frameworks): API fetch, event handlers
- **DaisyUI 4.12.24** (already in CDN): Profile card styling, modals

---

## Version Compatibility

### Core Stack (Unchanged from v1.0)

| Package | Version | Compatibility | Notes |
|---------|---------|---|---|
| Python | 3.11 | ✓ Full async support, PostGIS drivers | Upgrade from RC1 before production |
| FastAPI | 0.135.3 | ✓ Gunicorn + Uvicorn tested with all versions | No changes needed |
| SQLAlchemy | 2.0.49 | ✓ Async session support, GeoAlchemy2 0.14.3 compatible | No changes |
| GeoAlchemy2 | 0.14.3 | ✓ ST_AsGeoJSON() available, tested with SQLAlchemy 2.0.49 | No changes |
| Uvicorn | 0.44.0 | ✓ Full Gunicorn uvicorn.workers.UvicornWorker support | No changes |
| PostgreSQL | 14+ | ✓ PostGIS 3.x includes GeoJSON functions | DigitalOcean default is 14+ |
| MapLibre GL JS | 5.2.0 (CDN) | ✓ GeoJSON layer rendering, large polygon support | Pin to 5.2.0; don't float to "latest" |

### New Deployment Stack Compatibility

| Package | Version | Compatibility | Notes |
|---------|---------|---|---|
| Gunicorn | 21.2.0 | ✓ Python 3.7–3.11, uvicorn.workers.UvicornWorker | Already in requirements.txt |
| Nginx | 1.24+ (apt) | ✓ HTTP/1.1, HTTP/2, WebSocket proxying | Standard Ubuntu 22.04 package |
| Certbot | 2.x (apt) | ✓ Python 3.6+, Let's Encrypt ACME v2 | Standard Ubuntu 22.04 package |
| Systemd | native | ✓ Ubuntu 22.04, service file integration | No version pinning needed |

---

## What NOT to Change

| Current Stack | Status | Why |
|---------------|--------|-----|
| FastAPI 0.135.3 | ✓ Keep | Validated in v1.0; no breaking changes needed |
| SQLAlchemy 2.0.49 | ✓ Keep | Async support proven; GeoAlchemy2 integration solid |
| GeoAlchemy2 0.14.3 | ✓ Keep | ST_AsGeoJSON() is stable; no upgrade needed |
| Uvicorn 0.44.0 | ✓ Keep | Paired with Gunicorn; no changes needed |
| PostgreSQL/PostGIS | ✓ Keep | Spatial indexes proven; geospatial queries work |
| MapLibre GL JS 5.2.0 | ✓ Keep | No breaking changes for basic GeoJSON rendering |
| Python 3.11 | ⚠️ Upgrade only version | Upgrade from RC1 to final 3.11.x before production |

---

## Performance Expectations

On DigitalOcean 2 vCPU / 2GB RAM with Nginx + Gunicorn:

| Workload | Expected Throughput | Bottleneck | Notes |
|----------|-------------------|-----------|-------|
| Neighborhoods search (low I/O) | 500–1000 req/sec | SQLAlchemy query complexity + index performance | CPU not limiting |
| Operator portfolio (100 parcels GeoJSON) | 200–500 req/sec | PostGIS ST_AsGeoJSON() + JSON serialization | Small response, mostly DB time |
| Operator portfolio (1000 parcels GeoJSON) | 50–200 req/sec | PostGIS query time + network transfer | Response size grows linearly |
| Static assets (HTML, CSS, JS) | 5000+ req/sec | Not reaching Gunicorn; Nginx handles directly | No Python process involved |
| Rate limiting (slowapi) | Enforced per IP | None (enforced at Nginx level) | 30/minute on search endpoints |

**Single worker + async event loop** handles thousands of concurrent requests efficiently. CPU usage on 2 vCPU VPS will typically be <20% during normal traffic.

---

## Deployment Checklist

### Pre-Deployment (Development)

- [ ] Upgrade Python 3.11 from RC1 to final 3.11.x
- [ ] Test Gunicorn locally: `gunicorn -w 1 -k uvicorn.workers.UvicornWorker api.main:app`
- [ ] Verify APScheduler runs once (check logs for duplicate jobs)
- [ ] Test operator portfolio route: curl `http://localhost:8000/api/operators/MTEK/portfolio-map`
- [ ] Verify GeoJSON serialization: all parcels render correctly

### Deployment (DigitalOcean VPS)

1. Install system packages: `sudo apt install nginx certbot python3-certbot-nginx`
2. Clone repo to `/var/www/pulsecities` with venv
3. Create `/etc/systemd/system/pulsecities.service` (from template above)
4. Create `/etc/nginx/sites-available/pulsecities` (from template above)
5. Enable Nginx site: `sudo ln -s /etc/nginx/sites-available/pulsecities /etc/nginx/sites-enabled/`
6. Test Nginx: `sudo nginx -t`
7. Reload Nginx: `sudo systemctl reload nginx`
8. Enable Gunicorn: `sudo systemctl enable pulsecities && sudo systemctl start pulsecities`
9. Verify Gunicorn: `sudo systemctl status pulsecities`
10. Obtain SSL certificate: `sudo certbot --nginx -d pulsecities.com -d www.pulsecities.com`
11. Verify renewal: `sudo systemctl status certbot.timer && sudo certbot renew --dry-run`
12. Test full flow: `curl https://pulsecities.com/api/health`

### Post-Deployment (Monitoring)

- [ ] Monitor logs: `sudo journalctl -u pulsecities -f` (Gunicorn), `sudo tail -f /var/log/nginx/error.log` (Nginx)
- [ ] Check Certbot renewal: `sudo systemctl status certbot.timer`
- [ ] Monitor disk usage: `df -h` (PostgreSQL backups can grow)
- [ ] Monitor process: `ps aux | grep gunicorn` (should be 1 process)
- [ ] Verify APScheduler: Check logs for single nightly scraper run (not N runs)

---

## Environment Configuration (No Changes)

All existing v1.0 `.env` variables work unchanged:

```bash
DATABASE_URL=postgresql://...
NYC_OPEN_DATA_APP_TOKEN=...
ANTHROPIC_API_KEY=...
# etc.
```

**New:** Pass DATABASE_URL via systemd service file `Environment=` directive (see service template above) so connection string is not stored in .env on production server.

---

## Sources

### Deployment & Process Management
- [FastAPI Server Workers (Gunicorn + Uvicorn)](https://fastapi.tiangolo.com/deployment/server-workers/) — Official FastAPI documentation
- [Deploy FastAPI on Ubuntu 24.04 Gunicorn + Nginx + Certbot](https://www.buanacoding.com/2025/08/deploy-fastapi-ubuntu-24-04-gunicorn-nginx-certbot.html) — 2025 step-by-step guide
- [FastAPI Behind a Proxy](https://fastapi.tiangolo.com/advanced/behind-a-proxy/) — Proxy headers (X-Forwarded-For, etc.)
- [Nginx Reverse Proxy Best Practices](https://www.getpagespeed.com/server-setup/nginx/nginx-reverse-proxy) — 2026 guide to Nginx configuration
- [Nginx Unix Socket Performance](https://kisspeter.github.io/fastapi-performance-optimization/nginx_port_socket.html) — Unix socket vs. TCP comparison
- [DigitalOcean Gunicorn Deployment](https://www.digitalocean.com/community/tutorials/how-to-set-up-django-with-postgres-nginx-and-gunicorn-on-ubuntu) — systemd service files

### Geospatial & API Queries
- [GeoAlchemy2 ORM Tutorial](https://geoalchemy-2.readthedocs.io/en/latest/orm_tutorial.html) — ST_AsGeoJSON(), geometry serialization
- [PostGIS ST_AsGeoJSON](https://postgis.net/docs/ST_AsGeoJSON.html) — Database-side GeoJSON serialization
- [PostGIS Spatial Indexing](https://postgis.net/workshops/postgis-intro/indexing.html) — Bounding box queries, && operator
- [MapLibre GL JS Documentation](https://maplibre.org/maplibre-gl-js/docs/) — GeoJSON layer rendering

### Frontend & Concurrency
- [MapLibre Performance with Large GeoJSON](https://maplibre.org/maplibre-gl-js/docs/guides/large-data/) — Optimization tips for 100–1000+ parcel rendering
- [FastAPI Async + PostgreSQL Connection Pools](https://oneuptime.com/blog/post/2026-02-02-fastapi-async-database/view) — Connection pool sizing, asyncpg performance

---

*Stack additions for: PulseCities v2.0 — Production Deployment + Operator Profiles*
*Researched: 2026-04-16*
*Confidence: HIGH (all sources verified against official documentation)*
