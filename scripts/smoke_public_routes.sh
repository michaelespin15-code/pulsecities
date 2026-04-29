#!/usr/bin/env bash
# Automated pre-deploy smoke tests for PulseCities.
# Covers: HTTP status for all public routes, key API endpoints, sitemap,
# and file-level grep guards (em dashes, about route integrity, nav text).
#
# Usage:
#   ./scripts/smoke_public_routes.sh                     # test production
#   BASE=http://localhost:8000 ./scripts/smoke_public_routes.sh  # test local
#
# Exit codes: 0 = all passed, 1 = one or more failures.

set -uo pipefail

BASE="${BASE:-https://pulsecities.com}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PASS=0
FAIL=0

# ── helpers ────────────────────────────────────────────────────────────────

ok()   { echo "OK    $*"; PASS=$((PASS + 1)); }
fail() { echo "FAIL  $*"; FAIL=$((FAIL + 1)); }

# Assert HTTP route returns 200 (follows redirects).
http_ok() {
    local label="$1"
    local url="${BASE}$2"
    local status
    status=$(curl -sL -o /dev/null -w "%{http_code}" --max-time 15 "$url")
    if [[ "$status" == "200" ]]; then
        ok "$label  (HTTP 200)"
    else
        fail "$label  — HTTP $status  ($url)"
    fi
}

# Assert JSON response body contains a substring (follows redirects).
api_contains() {
    local label="$1"
    local route="$2"
    local needle="$3"
    local body
    body=$(curl -sL --max-time 15 "${BASE}${route}")
    local status
    status=$(curl -sL -o /dev/null -w "%{http_code}" --max-time 15 "${BASE}${route}")
    if [[ "$status" != "200" ]]; then
        fail "$label  — HTTP $status"
        return
    fi
    if [[ "$body" == *"$needle"* ]]; then
        ok "$label  — found: $needle"
    else
        fail "$label  — expected substring not found: $needle"
    fi
}

# Assert file does NOT contain a pattern (grep -q).
file_absent() {
    local label="$1"
    local file="$2"
    local pattern="$3"
    if grep -qP "$pattern" "$file" 2>/dev/null; then
        local hits
        hits=$(grep -nP "$pattern" "$file" | head -3)
        fail "$label  — pattern found in $file:\n$hits"
    else
        ok "$label"
    fi
}

# Assert file contains a pattern.
file_present() {
    local label="$1"
    local file="$2"
    local pattern="$3"
    if grep -qP "$pattern" "$file" 2>/dev/null; then
        ok "$label"
    else
        fail "$label  — pattern missing in $file: $pattern"
    fi
}

echo "Smoke testing: $BASE"
echo "Repo root:     $REPO_ROOT"
echo "─────────────────────────────────────────────"

# ── 1. Public HTML routes ─────────────────────────────────────────────────

echo ""
echo "[ Public routes ]"
http_ok "/"                              "/"
http_ok "/map"                           "/map"
http_ok "/operators"                     "/operators"
http_ok "/operator/mtek-nyc"             "/operator/mtek-nyc"
http_ok "/operator/phantom-capital"      "/operator/phantom-capital"
http_ok "/operator/bredif"               "/operator/bredif"
http_ok "/methodology"                   "/methodology"
http_ok "/about"                         "/about"
http_ok "/sitemap.xml"                   "/sitemap.xml"

# ── 2. API routes ─────────────────────────────────────────────────────────

echo ""
echo "[ API routes ]"
http_ok "/api/health"                    "/api/health"
http_ok "/api/neighborhoods/top-risk"    "/api/neighborhoods/top-risk"
http_ok "/api/stats"                     "/api/stats"
http_ok "/api/operators"                 "/api/operators"
http_ok "/api/search?q=mtek"             "/api/search?q=mtek"
http_ok "/api/search?q=11216"            "/api/search?q=11216"

# ── 3. API response content ───────────────────────────────────────────────

echo ""
echo "[ API content ]"
api_contains "top-risk returns neighborhoods" \
    "/api/neighborhoods/top-risk" '"neighborhoods"'

api_contains "search mtek returns operator result" \
    "/api/search?q=mtek" '"operator"'

api_contains "search 11216 returns neighborhood result" \
    "/api/search?q=11216" '"neighborhood"'

api_contains "operators list non-empty" \
    "/api/operators" '"slug"'

api_contains "sitemap references pulsecities.com" \
    "/sitemap.xml" "pulsecities.com"

# ── 4. File-level grep guards ─────────────────────────────────────────────

echo ""
echo "[ File guards ]"

FRONTEND="$REPO_ROOT/frontend"

# No em dashes in visible UI copy. Skip HTML/CSS/JS comment lines.
# grep -n output has a "linenum:" prefix, so anchor filters after the colon.
for f in index.html app.html operator.html methodology.html about.html; do
    hits=$(grep -nP "—" "$FRONTEND/$f" 2>/dev/null \
        | grep -vP ":\s*<!--" \
        | grep -vP "/\*" \
        | grep -vP ":\s*//" \
        | grep -vP "<!--.*—.*-->" \
        | grep -vP "/.*—.*/" \
        || true)
    if [[ -n "$hits" ]]; then
        fail "No em dash in visible copy of $f — hits:\n$(echo "$hits" | head -3)"
    else
        ok "No em dash in visible copy of $f"
    fi
done

# /about route exists in the FastAPI router.
file_present "/about route registered" \
    "$REPO_ROOT/api/routes/frontend.py" '"/about"'

# Static pages must NOT have a mobile bottom nav (#mbn).
# Bottom nav lives only on /map (app.html #mobile-bottom-nav).
for f in index.html methodology.html operator.html about.html; do
    count=$(grep -c 'id="mbn"' "$FRONTEND/$f" 2>/dev/null || true)
    if [[ "$count" -eq 0 ]]; then
        ok "No bottom nav on static page $f"
    else
        fail "$f — unexpected #mbn block found ($count occurrence(s)); bottom nav removed from static pages"
    fi
done

# app.html must still have its map-specific mobile nav (not removed by accident).
map_nav=$(grep -c 'id="mobile-bottom-nav"' "$FRONTEND/app.html" 2>/dev/null || true)
if [[ "$map_nav" -ge 1 ]]; then
    ok "app.html: map mobile nav present"
else
    fail "app.html: #mobile-bottom-nav missing — map nav was accidentally removed"
fi

# app.html mobile nav: no duplicate /map href (the old Search bug).
dup_map=$(grep -c 'href="/map"' "$FRONTEND/app.html" 2>/dev/null || true)
if [[ "$dup_map" -le 2 ]]; then
    ok "app.html: no duplicate /map in mobile nav"
else
    fail "app.html: /map appears $dup_map times — possible Search duplicate"
fi

# Each page that has a bottom nav links to /about.
for f in index.html methodology.html operator.html about.html app.html; do
    file_present "/about link present in $f" "$FRONTEND/$f" 'href="/about"'
done

# ── Summary ───────────────────────────────────────────────────────────────

echo ""
echo "─────────────────────────────────────────────"
echo "Results: $PASS passed, $FAIL failed"
echo ""

if [[ $FAIL -eq 0 ]]; then
    echo "All checks passed."
    exit 0
else
    echo "$FAIL check(s) failed. Fix before deploying."
    exit 1
fi
