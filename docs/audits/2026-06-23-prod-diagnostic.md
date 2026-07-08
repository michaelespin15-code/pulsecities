# PulseCities Production Diagnostic — 2026-06-23

- **Host:** ubuntu-s-1vcpu-1gb-nyc3-01 (DigitalOcean), run on the VPS over the local unix socket and psql.
- **Method:** read-only. curl over `unix:/tmp/gunicorn.sock` (bypassing Cloudflare/nginx), `psql`, `journalctl`, log inspection. No application code, schema, or data changed. Throwaway script: `scripts/audit/prod_diagnostic.sh`.
- **Run at:** 2026-06-23 ~01:36 UTC.

## TL;DR

The outage is **not** the database and **not** memory. It is a **broken Python dependency in the long-running app process**: anyio was upgraded on disk at **2026-06-16 16:24:53** *without restarting gunicorn*. The workers (PID 913, up since 16:14:27) hold a half-old, half-new view of anyio, so every request that uses a `Depends(get_db)` dependency throws `ImportError: cannot import name 'TaskHandle' from 'anyio._core._tasks'` and returns 500. **This has been continuous since 2026-06-16 20:38:59 UTC — ~6.5 days, not a fresh 01:13 event.** A `systemctl restart pulsecities` fixes it (proven: a fresh interpreter imports the on-disk anyio cleanly).

The classification gate is **working correctly** — the suspected financial/government "false positives" are all tagged `financial_institution` and are filtered out by both the list API and the SSR directory. They are not public. That premise was never verified; the browser audit only ever saw a 500ing directory.

---

## Section 1 — Box facts

| Fact | Value |
|---|---|
| RAM | 3.8 GiB total (`MemTotal: 4005696 kB`); 763 MiB used, 2.7 GiB available at audit time |
| Swap | 2.0 GiB (`/swapfile`), 673 MiB used |
| CPU | 2 vCPU |
| Disk `/` | 78 GB, 45 GB used, 58% |
| Last reboot / resize window | **2026-06-16 16:14 UTC** (boot 0, continuous since). Prior boot ended 2026-06-16 16:13. Capacity last changed at/before this power cycle. The `1vcpu-1gb` hostname is the original provisioning name; the box is now 2 vCPU / 3.8 GiB, so a resize happened, and the 06-16 cycle is the most recent power event that could have applied it. |
| OOM kills | **None.** Incident window (00:30–01:45) had 198 kernel log lines, zero OOM/`killed process`. Wider sweep since 2026-06-20: zero. No cgroup/systemd OOM either. |

**Memory theory: dead.** App RSS is 32 MiB, Postgres 953 MiB, 2.7 GiB free, no OOM anywhere, and the box never rebooted during the incident. The 01:13 window is **not** consistent with memory pressure — it points at the application process (see Section 5).

## Section 2 — Service & DB health (now)

- `pulsecities.service`: **active (running)** since 2026-06-16 16:14:27, gunicorn master PID 913 + 2 uvicorn workers, **no restarts in 6 days**, memory 32 MiB.
- `postgresql@14-main`: **active (running)** since 2026-06-16 16:14:27, memory 953 MiB, healthy.
- Bind: `--bind unix:/tmp/gunicorn.sock` (no TCP port).
- **Health over the socket (bypasses Cloudflare): `health:500`.** So the 500 originates in the app, not the edge.
- **DB is fully intact** (queried directly with psql — the app's claimed "DB unreachable" is false):

| Table | Rows |
|---|---|
| operators | 20 |
| displacement_scores | 177 |
| ownership_raw | 154,702 |
| score_history | 44,336 |

The app even warmed its caches from this DB successfully at boot (`top-risk cold query 20.06s rows=30`, `neighborhoods warmed count=390416`). The database has never been the problem.

## Section 3 — Operator classification (the "false positives" are already suppressed)

All 20 operator rows, with class:

| operator_root | slug | operator_class | acqs | llc |
|---|---|---|---|---|
| CHURCHILL | churchill | financial_institution | 159 | 5 |
| ICECAP | icecap | financial_institution | 145 | 7 |
| **OCEANVIEW** | oceanview | **financial_institution** | 128 | 2 |
| **TOORAK** | toorak | **financial_institution** | 99 | 2 |
| STANDARD | standard | financial_institution | 90 | 4 |
| BROAD | broad | financial_institution | 76 | 3 |
| **VALLEY** | valley | **financial_institution** | 74 | 6 |
| **COMMUNITY** | community | **financial_institution** | 70 | 11 |
| **BREDIF** | bredif | **operator** | 67 | 3 |
| **PHANTOM** | phantom-capital | **operator** | 65 | 32 |
| **METROPOLITAN** | metropolitan | **financial_institution** | 63 | 11 |
| DERBY | derby | financial_institution | 54 | 8 |
| **SYMETRA** | symetra | **financial_institution** | 52 | 4 |
| **RIDGEWOOD** | ridgewood | **financial_institution** | 50 | 11 |
| **ARBOR** | arbor | **financial_institution** | 48 | 8 |
| ICE | ice | financial_institution | 47 | 4 |
| BROADVIEW | broadview | financial_institution | 42 | 3 |
| **MTEK** | mtek-nyc | **operator** | 37 | 9 |
| CROSS | cross | financial_institution | 37 | 9 |
| EMPORIUM | emporium | financial_institution | 36 | 4 |

- **Only three rows are class `operator`: BREDIF, PHANTOM, MTEK.** Every bank/lender named in the brief — OCEANVIEW, RIDGEWOOD, VALLEY, COMMUNITY, METROPOLITAN, TOORAK, ARBOR, SYMETRA, STANDARD — is correctly `financial_institution`.
- **JOVIA, ARION, BATTALION, HABIB, TOWNHOUSE, MELO are not in the operators table at all** (0 rows).
- Both public surfaces filter on class. List API, `api/routes/operators.py:166`:
  `FROM operators WHERE operator_class = 'operator'`
  SSR directory, `api/routes/frontend.py:702`:
  `SELECT operator_root, slug FROM operators WHERE operator_class = 'operator'`
  The detail API re-checks it (`operators.py:261`: `if operator_class != 'operator': raise 404`), and the SSR operator page renders a minimal "Not an operator" page for non-operator classes (`frontend.py:615`).

**Conclusion:** the gate is applied in every relevant place and the data is classified correctly. The financial false positives **cannot** be appearing in the directory; once the site is back up it will list exactly BREDIF, PHANTOM, MTEK. The earlier "directory is showing them publicly" claim was an assumption — the browser audit was reading a 500 error page, not rendered rows.

## Section 4 — Live endpoint matrix (over the socket)

| URL | Status | Real content? | Cause |
|---|---|---|---|
| /operator/MTEK | 500 | no | anyio `TaskHandle` ImportError |
| /operator/PHANTOM | 500 | no | same |
| /operator/BREDIF | 500 | no | same |
| /operator/TOWNHOUSE | 500 | no | same (and no DB row even if it ran) |
| /operator/MELO | 500 | no | same (no DB row) |
| /operator/OCEANVIEW | 500 | no | same — **not** a distinct bug |
| /operator/RIDGEWOOD | 500 | no | same |
| /operator/TOORAK | 500 | no | same |
| /operator/JOVIA | 500 | no | same (no DB row) |
| /operator/ARION | 500 | no | same (no DB row) |
| /operators | 500 | no | same |
| /api/operators (→307 to /api/operators/) | 307→500 | no | redirect, then same |
| /api/operators/mtek-nyc | 500 | no | same |
| /api/operators/phantom-capital | 500 | no | same |
| /api/operators/bredif | 500 | no | same |
| /api/operators/oceanview | 500 | no | same (would be 404 by the gate if it ran) |
| /api/stats | 500 | no | same |
| /api/neighborhoods/top-risk | 500 | no | same |
| /api/neighborhoods/11216 | 404 | n/a | wrong path (real route is `/api/neighborhoods/11216/score`); not a fault |
| /api/score_history/11216 | 404 | n/a | wrong path; not a fault |
| /api/pulse | 404 | n/a | wrong path; not a fault |
| /api/search?q=MTEK | 307 | n/a | redirect to canonical path |

**Definitive detail-page test (JS removed):** the page's own data source, `GET /api/operators/` (the list the client filters by `operator_root`) returns 500, and every `GET /api/operators/{slug}` returns 500. So the operator detail pages cannot populate for any operator — not because of slug-vs-root keying, but because the API layer is down on the anyio fault. Every 500 body is the identical 21-byte `Internal Server Error`; every matching traceback is the same `TaskHandle` ImportError.

## Section 5 — Root cause of the 500 (and the "OCEANVIEW-class" question)

**There is no distinct OCEANVIEW data-condition bug.** OCEANVIEW 500s for the exact same reason as `/operators`, `/operator/MTEK`, and `/api/health`: the anyio import failure, which happens *before* any handler logic runs.

Traceback (from `/var/log/pulsecities/gunicorn-error.log`, reproduced live):

```
fastapi/dependencies/utils.py: _solve_generator
fastapi/concurrency.py:27       exit_limiter = CapacityLimiter(1)
anyio/_core/_synchronization.py:519  return self._max_value
anyio/_core/_eventloop.py:200   get_async_backend() -> import_module(...)
anyio/_backends/_asyncio.py:95  from .._core._tasks import TaskHandle
ImportError: cannot import name 'TaskHandle' from 'anyio._core._tasks'
   -> then KeyError: 'asyncio'   (backend fails to register)
```

**Why it happens:** FastAPI runs *sync generator dependencies* — `get_db()` in `models/database.py` is exactly one — through `contextmanager_in_threadpool`, which constructs `anyio.CapacityLimiter(1)`, which forces anyio to load its asyncio backend. The on-disk anyio (4.14.0) was rewritten at **2026-06-16 16:24:53** (file mtimes confirm) while gunicorn kept running from **16:14:27**. The workers had already cached an older `anyio._core._tasks` in `sys.modules`; when they later import the new `anyio._backends/_asyncio.py` (which does `from .._core._tasks import TaskHandle`), the cached old `_tasks` has no `TaskHandle`, so the import dies. Pure file routes (`/`, `/map`, `/methodology`, `/about`, `/status`) have no DB dependency, never take this path, and stay 200.

**Onset and continuity (log evidence):**
- First `Exception in ASGI` / first `TaskHandle` failure: **2026-06-16 20:38:59 UTC**.
- `Exception in ASGI` counts per day since: 06-16: 9, 06-17: 34, 06-18: 19, 06-19: 26, 06-20: 26, 06-21: 26, 06-22: 44, 06-23: 64 — continuous.
- 248 `TaskHandle` ImportErrors total in the current log.

**Proof a restart fixes it:** a fresh `venv/bin/python -c "from anyio import CapacityLimiter; CapacityLimiter(1)"` succeeds. The on-disk packages are internally consistent (`pip check` clean, single `anyio-4.14.0.dist-info`). Only the 6-day-old running process is poisoned.

**Secondary, currently-masked bugs (do not confuse with the outage):** the error log also holds older application bugs, all from **April 18–29** and not recurring since — `column o.party_addr_1 does not exist` (7×, the column now exists in `ownership_raw`), `missing FROM-clause entry for table "ds"` (6×), and `'int' object has no attribute 'days'` in `api/routes/pulse.py:266` (16×). These predate the anyio break, are not part of the current outage, and the schema-mismatch ones appear already resolved. The `pulse.py` date-math one is worth a look after recovery but is not why the site is down.

## Where we are (plain language)

The site has been hard-down for every dynamic page since the evening of June 16. A dependency (anyio) got upgraded on the server without bouncing the app, leaving the running workers with a broken half-loaded copy. Every page that touches the database returns "Internal Server Error"; the static landing/map shells still load but show no data. The database is completely healthy and all the data is intact. The operator classification is correct — the banks are properly hidden, contrary to the earlier assumption. The single corrective action is to restart the service; everything else is cleanup and prevention. Nothing was changed by this diagnostic.

## Prioritized fix list (implement nothing here)

- **P0 — Restart the app to reload a consistent anyio.** `systemctl restart pulsecities`. Proven to fix it (fresh import is clean). This restores every operator page, the directory, all `/api/*`, the map choropleth, search, and the status page in one action. Verify with `curl --unix-socket /tmp/gunicorn.sock http://localhost/api/health` → 200.
- **P1 — Pin/verify the dependency set and rebuild the venv cleanly, then restart.** Confirm `anyio`, `fastapi`, `starlette`, `sniffio` are mutually compatible (`pip check`), freeze a known-good `requirements.txt` (note: `requirements.txt` is currently modified in the working tree with a `.bak` sibling), so an ad-hoc `pip install` can't leave the prod venv half-upgraded again.
- **P2 — Never mutate the prod venv under a live process; make deploys restart-safe.** Any `pip install` on this box must be followed by `systemctl restart pulsecities`. Bake this into the deploy/runbook (or isolate the audit/playwright tooling into a separate venv so it can't touch the app's site-packages — that earlier `pip install playwright` is the kind of action that rewrote anyio).
- **P3 — Make `/api/health` independent of the DB session and add an alert on dynamic 500s.** Health currently fails for the same reason it should detect, and the static shells return 200, so a naive uptime probe stayed green through a 6.5-day outage. Probe a DB-backed route and alert on non-200.
- **P4 — Return a styled 503 instead of bare `Internal Server Error` on unhandled exceptions** so a journalist hitting a broken backend sees a graceful page (as `/status` already degrades), not a raw 500.
- **P5 — Fix the latent `pulse.py:266` `'int'.days` bug** (`get_renovation_flip`) surfaced in the logs; harmless to the current outage but a real 500 on `/api/pulse` once traffic resumes. Re-verify the April schema-mismatch errors (`party_addr_1`, table `ds`) are gone after recovery.
