---
phase: 08-production-deployment
plan: "01"
subsystem: deployment
tags: [gunicorn, systemd, proxy-headers, rate-limiting, production]
dependency_graph:
  requires: []
  provides: [pulsecities.service, ProxyHeadersMiddleware, gunicorn-unix-socket]
  affects: [api/main.py, /etc/systemd/system/pulsecities.service]
tech_stack:
  added: [gunicorn-21.2.0-as-process-manager]
  patterns: [unix-socket-proxy, proxy-headers-middleware, systemd-on-failure-restart]
key_files:
  created:
    - deploy/pulsecities.service
  modified:
    - api/main.py
decisions:
  - StartLimitIntervalSec/StartLimitBurst moved to [Unit] section (not [Service]) — required by systemd 230+; [Service] placement was ignored with a warning
  - User=root accepted for now; app lives in /root/, changing ownership requires a larger refactor; documented as future hardening candidate
  - deploy/pulsecities.service added as version-controlled artifact alongside system install at /etc/systemd/system/pulsecities.service
metrics:
  duration: "4 minutes"
  completed: "2026-04-17T00:44:46Z"
  tasks_completed: 2
  files_modified: 2
---

# Phase 08 Plan 01: Production Gunicorn + Systemd Deployment Summary

**One-liner:** ProxyHeadersMiddleware added for real-IP rate limiting behind Nginx; gunicorn 2-worker systemd unit installed and crash-tested on unix:/tmp/gunicorn.sock.

## What Was Built

### Task 1: ProxyHeadersMiddleware in api/main.py

- Added `from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware` import
- Registered `app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=["127.0.0.1"])` before `CORSMiddleware` — outermost middleware sees request first, rewrites `request.client.host` from `127.0.0.1` to the real client IP from `X-Forwarded-For`
- Trusting only `127.0.0.1` prevents clients from spoofing the header; arbitrary client `X-Forwarded-For` values are ignored
- Removed `StaticFiles` mount — Nginx handles frontend serving with correct `Cache-Control` headers
- Updated docstring: production command is now `gunicorn -w 2` (APScheduler was removed in Phase 5; the single-worker constraint no longer applies)

### Task 2: Systemd Unit + Log Directory

- Created `/var/log/pulsecities/` (already existed with existing log files; `.keep` added)
- Wrote `/etc/systemd/system/pulsecities.service`:
  - `ExecStart`: gunicorn with `--workers 2 --worker-class uvicorn.workers.UvicornWorker --bind unix:/tmp/gunicorn.sock --timeout 120 --graceful-timeout 30`
  - `Restart=on-failure`, `RestartSec=5s`
  - `StartLimitIntervalSec=60`, `StartLimitBurst=5` in `[Unit]` section
  - `After=network.target postgresql.service`
- Enabled (`systemctl enable`) and started service
- Confirmed: 1 master + 2 worker processes, unix socket at `/tmp/gunicorn.sock`
- Both workers logged "API starting" from the lifespan handler

## Verification Commands and Results

```
sudo systemctl is-active pulsecities.service   → active
sudo systemctl is-enabled pulsecities.service  → enabled
test -S /tmp/gunicorn.sock                     → exit 0 (socket exists)
ps aux | grep gunicorn | grep -v grep | wc -l  → 3 (1 master + 2 workers)
grep "ProxyHeadersMiddleware" api/main.py      → import line + add_middleware line
grep "StaticFiles" api/main.py                 → (empty — removed)
```

Crash-restart test: `sudo kill -9 <master-PID>` → wait 6 seconds → `systemctl is-active` returned `active`. Service restarted automatically within 5 seconds.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] StartLimitIntervalSec placement corrected to [Unit] section**

- **Found during:** Task 2
- **Issue:** Plan specified `StartLimitIntervalSec=60` and `StartLimitBurst=5` inside `[Service]` section. systemd 230+ requires these in `[Unit]`; in `[Service]` they are silently ignored (warning logged: "Unknown key name 'StartLimitIntervalSec' in section 'Service'"). The restart-loop protection would have been inactive with the plan's original placement.
- **Fix:** Moved both directives to the `[Unit]` section. The `[Install]` section was also present in the corrected file (was missing from plan's service file listing).
- **Files modified:** `/etc/systemd/system/pulsecities.service`, `deploy/pulsecities.service`
- **Commit:** df23280

### Additional Work

**deploy/ directory created:** Added `deploy/pulsecities.service` as a version-controlled copy of the installed systemd unit. The system path `/etc/systemd/system/pulsecities.service` is not tracked by git; keeping a copy in the repo ensures the service definition survives if the VPS is rebuilt.

## Known Stubs

None. This plan installs infrastructure only; no data flow or UI components.

## Threat Flags

No new threat surface beyond what was documented in the plan's threat model. All four STRIDE items (T-08-01-01 through T-08-01-04) were addressed as specified.

## Self-Check: PASSED

Files exist:
- FOUND: /root/pulsecities/api/main.py (contains ProxyHeadersMiddleware)
- FOUND: /etc/systemd/system/pulsecities.service
- FOUND: /root/pulsecities/deploy/pulsecities.service

Commits exist:
- FOUND: 006a3ac (feat(08-01): add ProxyHeadersMiddleware and remove StaticFiles)
- FOUND: df23280 (feat(08-01): install pulsecities.service systemd unit)

Service state verified: active + enabled + socket exists + 3 gunicorn processes.
