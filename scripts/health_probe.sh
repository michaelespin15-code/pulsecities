#!/usr/bin/env bash
# External liveness probe. Cron runs this every 5 minutes. It hits the public
# health endpoint and escalates any non-200 through notify_ops (webhook when
# configured, ops email always), so a backend outage is caught in minutes
# instead of the 6.5 days it went unnoticed in June 2026.
# A plain curl is enough: /api/health gates on a live DB query, so 200 means the
# dynamic stack actually works, not just that a static shell served.
#
# A marker file dedupes the escalation: one alert per outage, refreshed every
# 6 hours while it persists, cleared on recovery (which also sends the
# all-clear so the thread has an end).
set -uo pipefail

APP_DIR="/root/pulsecities"
URL="https://pulsecities.com/api/health"
MARKER="/tmp/pulsecities_health_probe.down"
REALERT_SECONDS=21600  # 6h — remind while the outage persists

code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 20 "$URL" || echo "000")

if [ "$code" = "200" ]; then
    if [ -f "$MARKER" ]; then
        rm -f "$MARKER"
        cd "$APP_DIR"
        "$APP_DIR/venv/bin/python" - <<'PY'
from dotenv import load_dotenv
load_dotenv("/root/pulsecities/.env")
from scheduler.alerts import notify_ops
notify_ops("health probe recovered", "pulsecities.com/api/health is returning 200 again.")
PY
    fi
    exit 0
fi

if [ -f "$MARKER" ]; then
    marker_age=$(( $(date +%s) - $(stat -c %Y "$MARKER") ))
    [ "$marker_age" -lt "$REALERT_SECONDS" ] && exit 0
fi
touch "$MARKER"

ts=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
cd "$APP_DIR"
"$APP_DIR/venv/bin/python" - "$code" "$ts" <<'PY'
import sys
from dotenv import load_dotenv
load_dotenv("/root/pulsecities/.env")
from scheduler.alerts import notify_ops
status, ts = sys.argv[1], sys.argv[2]
notify_ops("health probe failed", "%s returned %s at %s" % ("pulsecities.com/api/health", status, ts))
PY
