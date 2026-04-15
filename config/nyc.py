"""
NYC-specific constants and configuration.
All NYC-hardcoded values live here — never inline in scrapers or API code.
"""

# Borough codes
BOROUGH_CODES = {
    1: "Manhattan",
    2: "Bronx",
    3: "Brooklyn",
    4: "Queens",
    5: "Staten Island",
}

BOROUGH_NAMES = {v: k for k, v in BOROUGH_CODES.items()}

# Valid NYC zip code range
NYC_ZIP_MIN = 10001
NYC_ZIP_MAX = 11697

# Socrata base URL — use dataset IDs, never full download URLs (they change)
SOCRATA_BASE_URL = "https://data.cityofnewyork.us/resource"

# Dataset endpoint IDs — all verified live on NYC Open Data
DATASET_IDS = {
    # Tier 1 — Core displacement signals (all scrapers built)
    "311_complaints":        "erm2-nwe9",
    "dob_permits":           "ipu4-2q9a",
    "hpd_violations":        "wvxf-dwi5",
    "evictions":             "6z8x-wfk4",   # lagging: executed evictions only
    "acris_master":          "bnx9-e6tj",
    "acris_parties":         "636b-3b5g",
    "acris_legals":          "8h5j-fqxa",

    # Tier 2 — Enrichment and reference data
    "pluto":                 "64uk-42ks",
    "hpd_speculation":       "adax-9mit",   # HPD Speculation Watch List — scraper needed
    "hpd_registrations":     "tfm6-9sss",   # building registrations
    "dof_sales":             "usep-8jbt",   # property sales history
    "dcwp_licenses":         "w7w3-xahh",   # business licenses (gentrification signal)

    # Tier 3 — High-value signals requiring additional work
    # DHCR rent stabilization registration list — annual unit counts per building.
    # YoY comparison reveals RS unit loss: the single strongest displacement signal.
    # Scraper must compare current year vs prior year per BBL.
    "dhcr_rs_buildings":     "yn95-5t2d",

    # HPD Certificate of No Harassment (CONH) applications.
    # Buildings that apply for CONH are flagging intent to convert/renovate.
    # High predictive value — these are owners who know they're displacing tenants.
    "hpd_conh":              "7yus-yhjj",

    # HPD Housing Litigation (open housing court cases).
    # Earlier signal than executed evictions. Reflects active court proceedings.
    "hpd_litigation":        "59kj-x8nc",
}

# BBL format: 10-digit zero-padded string — BBBBBBBBBLL
# Borough (1) + Block (5) + Lot (4)
BBL_REGEX = r"^\d{10}$"

# ACRIS document types that indicate property transfer (broader than just DEED)
ACRIS_TRANSFER_DOC_TYPES = (
    "DEED",
    "DEEDP",
    "DEED, BARGAIN & SALE",
    "DEED, TRUST",
    "ASST",
    "ASSIGNMENT OF LEASE",
    "MEMO OF LEASE",
)

# 311 complaint types relevant to displacement risk.
# Applied as a query-time filter in the score engine (NOT at ingest time —
# raw complaints_raw table retains all complaint types for future re-processing).
# Spelling matches NYC Open Data exactly — "HARRASSMENT" uses double-R as in source data.
DISPLACEMENT_COMPLAINT_TYPES = (
    "HEAT/HOT WATER",
    "ELEVATOR",
    "PLUMBING",
    "PAINT/PLASTER",
    "MOLD",
    "ILLEGAL CONVERSION",
    "HARRASSMENT",
)

# Scraper row count minimums — flag WARNING if actual < 50% of these
SCRAPER_EXPECTED_MIN_RECORDS = {
    "311_complaints": 5000,
    "dob_permits":    500,
    "hpd_violations": 1000,
    "evictions":      100,
    # Phase 3 scrapers
    "acris_ownership": 200,       # deed transfers per incremental run; lower bound for watermark-based ingest
    "mappluto":        800000,    # full-refresh dataset; ~900k parcels — <400k signals upstream issue
    "dof_assessments": 500000,    # full-refresh dataset; ~1M parcels — lower bound at 50%
    "dcwp_licenses":   500,       # 69k total rows but incremental runs fetch new licenses only
    "dhcr_rs":         50000,     # full dataset; lower bound
}

# Address → BBL resolution (two-step: Nominatim geocode → PLUTO BBL lookup)
# geosearch.planningitc.gov is dead as of 2026; replaced with OSM Nominatim + SODA PLUTO
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "PulseCities/1.0 (civic displacement mapping tool)"

# Default map center
MAP_DEFAULT_LAT = 40.7128
MAP_DEFAULT_LNG = -74.0060
MAP_DEFAULT_ZOOM = 12
MAP_MIN_ZOOM = 10
MAP_MAX_ZOOM = 18
