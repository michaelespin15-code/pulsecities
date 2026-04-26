#!/usr/bin/env bash
# Smoke test for PulseCities frontend routes.
# Usage:
#   ./scripts/smoke_routes.sh                    # test production
#   BASE=http://localhost:8000 ./scripts/smoke_routes.sh  # test local FastAPI
set -euo pipefail

BASE="${BASE:-https://pulsecities.com}"
PASS=0
FAIL=0

check() {
    local route="$1"
    local marker="$2"
    local url="${BASE}${route}"
    local body
    body=$(curl -s --max-time 10 "$url")
    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$url")

    if [[ "$status" != "200" ]]; then
        echo "FAIL  $route  — HTTP $status"
        FAIL=$((FAIL + 1))
        return
    fi

    # Use bash substring test instead of echo|grep to avoid SIGPIPE/pipefail
    # false positive when grep -q exits early on large responses (app.html ~208KB).
    if [[ "$body" == *"$marker"* ]]; then
        echo "OK    $route  — found: $marker"
        PASS=$((PASS + 1))
    else
        echo "FAIL  $route  — marker not found: $marker"
        FAIL=$((FAIL + 1))
    fi
}

check_absent() {
    local route="$1"
    local marker="$2"
    local url="${BASE}${route}"
    local body
    body=$(curl -s --max-time 10 "$url")

    if [[ "$body" == *"$marker"* ]]; then
        echo "FAIL  $route  — unexpected marker present: $marker"
        FAIL=$((FAIL + 1))
    else
        echo "OK    $route  — correctly absent: $marker"
        PASS=$((PASS + 1))
    fi
}

echo "Smoke testing: $BASE"
echo "---"

# Landing page
check "/"                   "PulseCities"

# Map app
check "/map"                "maplibre"
check "/app.html"           "maplibre"

# Methodology — must be the standalone page, not the app
check "/methodology"        "PulseCities methodology"
check "/methodology.html"   "PulseCities methodology"
check_absent "/methodology" "methodology-modal"   # app.html marker

# About page — must not fall through to index.html
check "/about"              "About PulseCities"
check "/about.html"         "About PulseCities"
check_absent "/about"       "search-input"         # index.html marker

# Operator profile shell
check "/operator/mtek-nyc"          "PulseCities"
check "/operator/phantom-capital"   "PulseCities"
check "/operator/bredif"            "PulseCities"

# Operators directory
check "/operators"          "operator"

echo "---"
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
