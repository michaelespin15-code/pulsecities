#!/usr/bin/env bash
# External liveness probe. Cron runs this every 5 minutes. It hits the public
# health endpoint and fires send_alert on any non-200, so a backend outage is
# caught in minutes instead of the 6.5 days it went unnoticed in June 2026.
# A plain curl is enough: /api/health gates on a live DB query, so 200 means the
# dynamic stack actually works, not just that a static shell served.
set -uo pipefail

APP_DIR="/root/pulsecities"
URL="https://pulsecities.com/api/health"

code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 20 "$URL" || echo "000")
[ "$code" = "200" ] && exit 0

ts=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
cd "$APP_DIR"
"$APP_DIR/venv/bin/python" - "$code" "$ts" <<'PY'
import sys
from dotenv import load_dotenv
load_dotenv("/root/pulsecities/.env")
from scheduler.alerts import send_alert
status, ts = sys.argv[1], sys.argv[2]
send_alert("health probe failed", "%s returned %s at %s" % ("pulsecities.com/api/health", status, ts))
PY
