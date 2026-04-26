# PulseCities Route Manifest

Every public route and the file that serves it. Update this when adding or
removing routes. Prevents link/route drift across nginx, FastAPI, and HTML.

## Static pages

| Route | File | Served by | Notes |
|---|---|---|---|
| `/` | `frontend/index.html` | nginx catch-all | Landing page, search-first |
| `/index.html` | `frontend/index.html` | nginx catch-all | |
| `/map` | `frontend/app.html` | nginx `location = /map` | Full MapLibre map app |
| `/app.html` | `frontend/app.html` | nginx catch-all | |
| `/methodology` | `frontend/methodology.html` | nginx `location = /methodology` | Standalone editorial page, no i18n |
| `/methodology.html` | `frontend/methodology.html` | nginx catch-all | |
| `/about` | `frontend/about.html` | nginx `location = /about` | About and contact |
| `/about.html` | `frontend/about.html` | nginx catch-all | |

## Dynamic pages (FastAPI + DB)

| Route | Handler | File returned | Notes |
|---|---|---|---|
| `/neighborhood/{zip}` | `api/routes/frontend.py` | `frontend/app.html` | OG/meta injected server-side |
| `/property/{bbl}` | `api/routes/frontend.py` | `frontend/app.html` | OG/meta injected server-side |
| `/operator/{slug}` | `api/routes/frontend.py` | `frontend/operator.html` | OG/meta injected server-side |
| `/operators` | `api/routes/frontend.py` | server-rendered HTML | Operator directory, no static file |

## API routes (FastAPI, prefix `/api/`)

| Route | Purpose |
|---|---|
| `/api/neighborhoods` | GeoJSON for all neighborhoods |
| `/api/neighborhoods/{zip}/score` | Score + signal breakdown |
| `/api/neighborhoods/top-movers` | Weekly score movers |
| `/api/operators/top` | Top operator clusters |
| `/api/stats` | Citywide summary stats |
| `/api/search` | Address + ZIP search |
| `/og/{zip}.png` | Dynamic OG images (nginx-cached) |

## FastAPI dev server parity

When running `uvicorn api.main:app` directly (no nginx), FastAPI also serves
`/map`, `/methodology`, and `/about` via `FileResponse`. This mirrors the
nginx config so local dev and production behave the same.

## Routing rules

- `/methodology` must never fall through to `app.html`. It is a standalone
  English-only page with no i18n. Serving `app.html` there causes language
  bleed when `pc-lang=es` is set in localStorage.
- `/about` must never fall through to `index.html`. The nginx catch-all uses
  `try_files $uri /index.html` (not `$uri.html`) to avoid unintentionally
  exposing internal pages like `ops.html`.
- Dynamic operator/neighborhood routes are proxied through nginx to FastAPI
  for server-side OG/meta injection.
