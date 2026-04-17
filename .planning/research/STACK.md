# Technology Stack

**Project:** PulseCities
**Researched:** 2026-04-07
**Status:** Stack is committed — this document records decisions already made and provides rationale plus version-accurate details for roadmap use.

## Overview

The PulseCities stack is not under deliberation. Python 3.11 + FastAPI + SQLAlchemy + PostgreSQL + PostGIS on the backend, vanilla JS + MapLibre GL JS on the frontend, DigitalOcean for hosting. These decisions are locked. This document exists to give the roadmap accurate version data, rationale, missing pieces, and gaps that phases must address.

---

## Committed Stack

### Backend

| Technology | Pinned Version | Purpose | Rationale |
|------------|---------------|---------|-----------|
| Python | 3.11 (RC1 in venv — must upgrade before prod) | All backend logic | Mature async support, strong geospatial library ecosystem, FastAPI requires 3.8+ |
| FastAPI | 0.135.3 | HTTP API framework | Async-native, Pydantic v2 integration, automatic OpenAPI docs, best-in-class DX for Python REST APIs |
| Starlette | 1.0.0 | ASGI foundation | FastAPI dependency — handles routing, middleware, request/response lifecycle |
| Uvicorn | 0.44.0 | ASGI server | Production-ready async Python server; supports workers for concurrency |
| SQLAlchemy | 2.0.49 | ORM + database abstraction | v2 unified API, async support via greenlet, declarative models; raw SQL forbidden per spec |
| Pydantic | 2.12.5 | Request/response validation | FastAPI dependency; v2 is significantly faster than v1; powers all API schemas |
| psycopg2-binary | 2.9.11 | PostgreSQL driver | Synchronous PostgreSQL driver; binary package avoids system libpq dependency |
| requests | 2.33.1 | HTTP client for scrapers | All outbound calls to NYC Open Data, ACRIS, DOF; well-understood, no async required for cron scrapers |
| python-dotenv | 1.2.2 | Environment variable loading | Loads `.env` into `os.environ`; standard pattern for 12-factor apps |

### Database

| Technology | Version | Purpose | Rationale |
|------------|---------|---------|-----------|
| PostgreSQL | 14+ (target) | Primary system of record | ACID, mature, excellent geospatial extension ecosystem |
| PostGIS | 3.x | Geospatial queries and geometry storage | Industry standard for geospatial SQL; required for neighborhood polygon queries, parcel point-in-polygon, ST_Within/ST_Intersects on civic data |

### Frontend

| Technology | Version / Source | Purpose | Rationale |
|------------|-----------------|---------|-----------|
| MapLibre GL JS | CDN (latest stable — must pin) | Interactive map rendering | Open-source fork of Mapbox GL JS; no API key required for renderer; WebGL-accelerated; industry standard for open civic maps |
| DaisyUI | CDN (latest stable) | UI components | Tailwind CSS-based component library; minimal JS, pure CSS; suitable for data-display UIs with no build pipeline |
| Google Fonts (Inter, JetBrains Mono) | CDN | Typography | Inter for UI text, JetBrains Mono for numeric data values |
| Vanilla JS | Browser native | Map interaction and API calls | No framework, no build step; appropriate for a map-centric app with straightforward UI state |

### Infrastructure

| Technology | Purpose | Rationale |
|------------|---------|-----------|
| DigitalOcean VPS (NYC3) | All production hosting | Simple, predictable cost; NYC3 datacenter minimizes latency to NYC Open Data APIs; no managed service complexity for v1 |
| Nginx | Reverse proxy + HTTPS termination | Routes HTTPS traffic to uvicorn on port 8000; Let's Encrypt integration via Certbot |
| System cron | Nightly scraper scheduling | Simplest scheduler for periodic jobs; no external dependency; invoked at 2am UTC |

### Libraries Missing from requirements.txt (Must Add)

| Library | Purpose | When to Add | Priority |
|---------|---------|-------------|----------|
| alembic | Database migration management | Before any model is created — retrofitting is painful | CRITICAL |
| GeoAlchemy2 | PostGIS geometry columns in SQLAlchemy ORM | Before any model with geometry is written | CRITICAL |
| Shapely | Geometry serialization (ORM → GeoJSON) | Same phase as GeoAlchemy2 | CRITICAL |
| tenacity | Retry/backoff for scraper HTTP calls | When building scrapers | HIGH |
| slowapi | FastAPI rate limiting middleware | Before any endpoint is publicly reachable | HIGH |
| anthropic | Claude API SDK for AI summaries | When AI summarization phase begins (v2) | DEFERRED |

---

## Alternatives Considered

| Category | Chosen | Alternative | Why Not |
|----------|--------|-------------|---------|
| Backend framework | FastAPI | Django REST Framework | DRF is heavier; FastAPI's async-native design fits scraper + API workloads better |
| ORM | SQLAlchemy | Raw psycopg2 SQL | Explicitly forbidden per project spec; maintainability concern |
| DB migration | (none yet — add Alembic) | Manual ALTER TABLE | Alembic is the correct choice; absence is a gap, not a deliberate decision |
| Frontend | Vanilla JS | React / Next.js | Build pipeline adds complexity for a map-centric app; vanilla JS is appropriate |
| Map library | MapLibre GL JS | Leaflet | Leaflet lacks WebGL acceleration for large vector datasets; MapLibre handles NYC-scale data significantly better |
| Map library | MapLibre GL JS | Mapbox GL JS | Mapbox requires a paid API key and has proprietary terms; MapLibre is the open-source fork |
| Scheduler | System cron | Celery + Redis | Celery adds significant operational overhead for nightly batch jobs; system cron is correct |
| Scheduler | System cron | APScheduler | APScheduler embedded in FastAPI process creates coupling between API server and scraper process |
| HTTP client | requests | sodapy | sodapy is a leaky abstraction over Socrata with maintenance risk; direct requests calls are cleaner |

---

## Version Pinning Notes

- `requirements.txt` pins top-level packages but has no `pip-tools`-generated transitive lockfile. A fresh install could produce different transitive dependency versions over time.
- Python 3.11.0rc1 is in the current venv — rebuild against Python 3.11 final (or 3.12) before production.
- MapLibre GL JS must be pinned to a specific version in the CDN URL — floating to "latest" risks breaking changes on major releases.

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Backend framework versions | HIGH | Read directly from pinned requirements.txt |
| FastAPI + SQLAlchemy capabilities | HIGH | Well-documented, stable libraries |
| PostGIS suitability | HIGH | Industry standard for geospatial SQL; widely used for civic mapping |
| MapLibre GL JS | HIGH | Correct choice; no API key; open-source |
| Missing GeoAlchemy2/Alembic gaps | HIGH | Confirmed absent; well-understood consequence |
| DigitalOcean sizing | MEDIUM | 2 vCPU / 2 GB RAM sufficient for prototype; may need upgrade at full NYC parcel load |
