#!/usr/bin/env bash
# Throwaway read-only production diagnostic for pulsecities.com.
# Lightweight HTTP + shell only. No browser, no app/schema/data changes.
# Re-runnable. Findings written by hand into docs/audits/2026-06-23-prod-diagnostic.md.
set -uo pipefail

SOCK=/tmp/gunicorn.sock
PGURL="postgresql://pulsecities_user:PCities2026NY@localhost/pulsecities"
ERR=/var/log/pulsecities/gunicorn-error.log

echo "########## SECTION 1: box ##########"
free -h; swapon --show; nproc; uptime; who -b
last -x reboot shutdown | head
df -h /
echo "-- OOM in incident window --"
journalctl -k --since "2026-06-23 00:30" --until "2026-06-23 01:45" | grep -iE "oom|out of memory|killed process" || echo "none"
echo "-- OOM wider sweep --"
journalctl -k --since "2026-06-20" | grep -iE "oom|out of memory" || echo "none"

echo "########## SECTION 2: services + DB ##########"
systemctl status pulsecities --no-pager | head -12
systemctl status 'postgresql@14-main' --no-pager | head -6
curl -s -o /dev/null -w "health-via-socket:%{http_code}\n" -m 20 --unix-socket "$SOCK" http://localhost/api/health
psql "$PGURL" -c "select 'operators' t,count(*) from operators
  union all select 'displacement_scores',count(*) from displacement_scores
  union all select 'ownership_raw',count(*) from ownership_raw
  union all select 'score_history',count(*) from score_history;"

echo "########## SECTION 3: operator classification ##########"
psql "$PGURL" -c "select operator_root, slug, operator_class, total_acquisitions,
  jsonb_array_length(llc_entities) llc_count from operators
  order by total_acquisitions desc nulls last;"

echo "########## SECTION 4: endpoint matrix (via socket, bypass CF) ##########"
for u in /operator/MTEK /operator/PHANTOM /operator/BREDIF /operator/OCEANVIEW /operator/RIDGEWOOD \
         /operators /api/operators/ /api/operators/mtek-nyc /api/operators/oceanview \
         /api/stats /api/neighborhoods/top-risk; do
  printf "%-40s %s\n" "$u" "$(curl -s -o /dev/null -w '%{http_code}' -m 25 --unix-socket "$SOCK" "http://localhost$u")"
done

echo "########## SECTION 5: current fault signature ##########"
echo "anyio on-disk fresh import test:"
/root/pulsecities/venv/bin/python -c "from anyio import CapacityLimiter; CapacityLimiter(1); print('on-disk anyio OK -> restart fixes it')"
echo "running-process error (top error classes):"
grep -E "^[A-Za-z.]*Error:" "$ERR" | sort | uniq -c | sort -rn | head -6
echo "first TaskHandle failure (outage onset):"
ln=$(grep -n "cannot import name 'TaskHandle'" "$ERR" | head -1 | cut -d: -f1)
head -n "$ln" "$ERR" | grep -oE "\[20[0-9-]+ [0-9:]+ \+0000\].*Exception in ASGI" | tail -1
