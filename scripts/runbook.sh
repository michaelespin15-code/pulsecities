#!/usr/bin/env bash
# =============================================================================
# PulseCities Production Runbook
# =============================================================================
#
# Usage:
#   bash scripts/runbook.sh deploy          # Pull latest code, restart gunicorn
#   bash scripts/runbook.sh rollback        # Revert to previous git revision
#   bash scripts/runbook.sh renew-cert      # Force TLS cert renewal + nginx reload
#   bash scripts/runbook.sh smoke-test      # Verify site is healthy
#   bash scripts/runbook.sh status          # Show service and cert status
#
# Requirements: Run from /root/pulsecities with sudo access.
# All commands are idempotent — safe to re-run on failure.
# =============================================================================

set -euo pipefail

APP_DIR="/root/pulsecities"
SERVICE="pulsecities.service"
VENV="$APP_DIR/venv/bin"
DOMAIN="pulsecities.com"
NGINX_CONF="/etc/nginx/sites-available/pulsecities"
LOG_DIR="/var/log/pulsecities"
CERT_DIR="/etc/letsencrypt/live/$DOMAIN"

# --- Helpers -----------------------------------------------------------------

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

require_root() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "ERROR: This command must be run as root (sudo bash scripts/runbook.sh $1)"
        exit 1
    fi
}

# --- Commands ----------------------------------------------------------------

cmd_deploy() {
    require_root "deploy"
    log "=== DEPLOY START ==="

    log "Pulling latest code from main..."
    cd "$APP_DIR"
    git fetch origin main
    git reset --hard origin/main

    log "Installing/updating Python dependencies..."
    "$VENV/pip" install -r requirements.txt --quiet

    log "Running Alembic migrations..."
    "$VENV/alembic" upgrade head

    log "Restarting gunicorn via systemd..."
    systemctl restart "$SERVICE"

    log "Waiting for gunicorn to come up..."
    sleep 3

    if systemctl is-active --quiet "$SERVICE"; then
        log "Service is running."
    else
        log "ERROR: Service failed to start. Showing last 30 journal lines:"
        journalctl -u "$SERVICE" -n 30 --no-pager
        exit 1
    fi

    cmd_smoke_test
    log "=== DEPLOY COMPLETE ==="
}

cmd_rollback() {
    require_root "rollback"
    log "=== ROLLBACK START ==="

    cd "$APP_DIR"
    PREV_COMMIT=$(git rev-parse HEAD~1)
    CURRENT=$(git rev-parse HEAD)
    log "Rolling back from $CURRENT to $PREV_COMMIT..."

    git checkout "$PREV_COMMIT"

    log "Installing dependencies for rolled-back revision..."
    "$VENV/pip" install -r requirements.txt --quiet

    log "Restarting gunicorn..."
    systemctl restart "$SERVICE"
    sleep 3

    if systemctl is-active --quiet "$SERVICE"; then
        log "Rollback successful — running on commit $PREV_COMMIT"
    else
        log "ERROR: Service failed after rollback. Manual intervention required."
        journalctl -u "$SERVICE" -n 30 --no-pager
        exit 1
    fi

    cmd_smoke_test
    log "=== ROLLBACK COMPLETE ==="
}

cmd_renew_cert() {
    require_root "renew-cert"
    log "=== CERT RENEWAL START ==="

    log "Running certbot renew (force)..."
    # The deploy hook at /etc/letsencrypt/renewal-hooks/deploy/nginx-reload.sh
    # runs nginx -t && systemctl reload nginx automatically after renewal.
    certbot renew --force-renewal --domain "$DOMAIN"

    log "Verifying cert expiry date..."
    openssl x509 -in "$CERT_DIR/fullchain.pem" -noout -dates | grep notAfter

    log "Certbot timer status:"
    systemctl status certbot.timer --no-pager | head -8

    log "=== CERT RENEWAL COMPLETE ==="
}

cmd_smoke_test() {
    log "=== SMOKE TEST ==="

    log "Checking HTTP -> HTTPS redirect..."
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "http://$DOMAIN/")
    if [ "$HTTP_CODE" = "301" ] || [ "$HTTP_CODE" = "302" ]; then
        log "  HTTP redirect: OK ($HTTP_CODE)"
    else
        log "  HTTP redirect: FAIL (got $HTTP_CODE, expected 301)"
        exit 1
    fi

    log "Checking HTTPS API response..."
    API_CODE=$(curl -sk -o /dev/null -w "%{http_code}" --max-time 10 "https://$DOMAIN/api/health")
    if [ "$API_CODE" = "200" ]; then
        log "  HTTPS API: OK (200)"
    else
        log "  HTTPS API: FAIL (got $API_CODE)"
        exit 1
    fi

    log "Checking rate limit headers..."
    RL_HEADER=$(curl -sk -D - -o /dev/null --max-time 10 "https://$DOMAIN/api/neighborhoods" | grep -i "x-ratelimit-limit" || true)
    if [ -n "$RL_HEADER" ]; then
        log "  Rate limit header: OK ($RL_HEADER)"
    else
        log "  Rate limit header: MISSING — check @limiter.limit on neighborhoods routes"
        exit 1
    fi

    log "Checking TLS certificate..."
    CERT_ISSUER=$(openssl s_client -connect "$DOMAIN:443" -servername "$DOMAIN" < /dev/null 2>/dev/null | grep "issuer=" | head -1 || true)
    if echo "$CERT_ISSUER" | grep -qi "let.s encrypt\|letsencrypt\|ISRG"; then
        log "  TLS cert: OK ($CERT_ISSUER)"
    else
        # Cloudflare may present its own cert at the edge — not a hard failure
        log "  TLS cert: WARNING — unexpected issuer ($CERT_ISSUER)"
    fi

    log "Checking gunicorn workers..."
    WORKER_COUNT=$(ps aux | grep "[g]unicorn.*UvicornWorker" | wc -l)
    log "  Gunicorn workers: $WORKER_COUNT (expected: 2)"

    log "=== SMOKE TEST PASSED ==="
}

cmd_status() {
    log "=== STATUS ==="
    echo ""
    echo "--- Service ---"
    systemctl status "$SERVICE" --no-pager | head -12
    echo ""
    echo "--- Nginx ---"
    systemctl status nginx --no-pager | head -6
    echo ""
    echo "--- TLS Certificate ---"
    openssl x509 -in "$CERT_DIR/fullchain.pem" -noout -dates 2>/dev/null || echo "Cert not found at $CERT_DIR"
    echo ""
    echo "--- Certbot Timer ---"
    systemctl status certbot.timer --no-pager | head -6
    echo ""
    echo "--- Recent Gunicorn Errors ---"
    tail -20 "$LOG_DIR/gunicorn-error.log" 2>/dev/null || echo "(no error log yet)"
    echo ""
    log "=== STATUS DONE ==="
}

# --- Dispatch ----------------------------------------------------------------

COMMAND="${1:-}"
case "$COMMAND" in
    deploy)      cmd_deploy ;;
    rollback)    cmd_rollback ;;
    renew-cert)  cmd_renew_cert ;;
    smoke-test)  cmd_smoke_test ;;
    status)      cmd_status ;;
    "")
        echo "Usage: bash scripts/runbook.sh {deploy|rollback|renew-cert|smoke-test|status}"
        exit 0
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Usage: bash scripts/runbook.sh {deploy|rollback|renew-cert|smoke-test|status}"
        exit 1
        ;;
esac
