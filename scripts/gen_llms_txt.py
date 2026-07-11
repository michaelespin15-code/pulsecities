"""
Generate frontend/llms.txt from live scoring weights and top-risk data.

Reads the signal weights from scoring/compute.py and the current scores from the
database, so the published file always reconciles to what the API returns. Active
weights are the raw weights rescaled over the live active signals (dormant signals
have their mass redistributed each run, matching compute_scores). Run after a
scoring run:

    PYTHONPATH=. venv/bin/python scripts/gen_llms_txt.py
"""

import os
import tempfile
from pathlib import Path

from sqlalchemy import text

from api.routes.frontend import _tier_info
from api.routes.stats import compute_top_risk
from models.database import get_scraper_db
from scoring.compute import (
    WEIGHT_LLC_ACQUISITIONS,
    WEIGHT_PERMITS,
    WEIGHT_COMPLAINTS,
    WEIGHT_EVICTIONS,
    WEIGHT_HPD_VIOLATIONS,
    WEIGHT_RS_UNIT_LOSS,
)

_OUT = Path(__file__).parent.parent / "frontend" / "llms.txt"

# breakdown key, display label, raw weight — in presentation order
SIGNALS = [
    ("llc_acquisitions", "LLC acquisitions", WEIGHT_LLC_ACQUISITIONS),
    ("permits",          "permits",          WEIGHT_PERMITS),
    ("complaint_rate",   "complaints",       WEIGHT_COMPLAINTS),
    ("evictions",        "evictions",        WEIGHT_EVICTIONS),
    ("hpd_violations",   "HPD violations",   WEIGHT_HPD_VIOLATIONS),
    ("rs_unit_loss",     "RS unit loss",     WEIGHT_RS_UNIT_LOSS),
]


def _borough(zip_code):
    n = int(zip_code)
    if 10001 <= n <= 10282: return "Manhattan"
    if 10301 <= n <= 10314: return "Staten Island"
    if 10451 <= n <= 10475: return "Bronx"
    if 11201 <= n <= 11239: return "Brooklyn"
    if (11001 <= n <= 11109) or (11354 <= n <= 11697): return "Queens"
    return "New York City"


def _active_keys(db):
    rows = db.execute(text("""
        SELECT key, max(value_text::float) AS mx
        FROM displacement_scores ds,
             jsonb_each_text(ds.signal_breakdown) AS b(key, value_text)
        GROUP BY key
    """)).fetchall()
    return {r.key for r in rows if r.mx and r.mx > 0.0}


def _top_risk(db, limit=5):
    """The published top-risk list, straight from the same function the
    /api/stats route uses. The old version fetched the HTTP endpoint, whose
    one-hour cache meant this file quoted yesterday's scores for up to an hour
    after every scoring run; a direct call can never drift."""
    _, entries = compute_top_risk(db)
    return [(e["zip_code"], e["name"], e["score"]) for e in entries[:limit]]


def _scored_count(db) -> int:
    return db.execute(text(
        "SELECT COUNT(DISTINCT zip_code) FROM displacement_scores WHERE score IS NOT NULL"
    )).scalar() or 0


def build():
    with get_scraper_db() as db:
        active = _active_keys(db)
        active_sum = sum(w for k, _, w in SIGNALS if k in active) or 1.0
        weights_line = ", ".join(
            f"{label} {round(w / active_sum * 100, 1)}%"
            for k, label, w in SIGNALS if k in active
        )
        dormant = [label for k, label, _ in SIGNALS if k not in active]
        top = _top_risk(db)
        n_scored = _scored_count(db)

    nbhd = "\n".join(
        f"- {name} ({zip_code}, {_borough(zip_code)}) - score {score}, "
        f"{_tier_info(float(score))[0].lower()} displacement pressure"
        for zip_code, name, score in top
    )
    dormant_note = (
        f" {', '.join(dormant)} is currently dormant pending second-year DHCR data, "
        "and its weight is redistributed across the active signals each run."
        if dormant else ""
    )

    return f"""# PulseCities

> Real-time displacement risk intelligence for all {n_scored} NYC neighborhoods, built on public data signals updated daily from city records.

PulseCities tracks housing displacement pressure across New York City by aggregating and scoring signals from public datasets: LLC property acquisitions (ACRIS), renovation permit filings (DOB), tenant complaint rates (311/HPD), residential eviction filings, HPD housing violations, and rent-stabilized unit loss (DHCR). Each of the {n_scored} ZIP code neighborhoods receives a daily composite score from 0 to 100.

## What this site provides

- Displacement risk scores (0 to 100) for all {n_scored} NYC neighborhoods, updated nightly
- Signal breakdown showing which factors drive pressure in each area
- Week-over-week score changes to identify neighborhoods under acute pressure
- LLC operator profiles linking corporate entities to their NYC property portfolios
- Block-level investigation tools: address search, renovation-flip pattern detection, civic event timelines
- Weekly email digests for subscribers tracking specific neighborhoods

## Current top-risk neighborhoods (as of latest scoring run)

{nbhd}

## Data sources

All signals are derived from NYC open data:

- ACRIS (NYC Department of Finance) - deed transfers to LLC entities
- DOB NOW (NYC Department of Buildings) - alteration permit filings on residential buildings
- 311 / HPD (NYC Open Data) - tenant complaints filtered to displacement-relevant types
- NYC Marshal Evictions - residential eviction executions
- HPD Violations (NYC Open Data) - Class B and C violations on 3+ unit buildings
- DHCR Rent Stabilization - rent-stabilized unit counts by building, year over year

Scoring methodology: each signal is normalized 0 to 100 relative to all NYC neighborhoods (per residential unit), then weighted and composited. Active weights: {weights_line}.{dormant_note}

## API

- `GET /api/neighborhoods` - GeoJSON FeatureCollection of all {n_scored} scored neighborhoods with scores
- `GET /api/neighborhoods/{{zip}}/score` - score and signal breakdown for a ZIP code
- `GET /api/neighborhoods/top-risk` - top N neighborhoods by current score
- `GET /api/neighborhoods/top-movers` - neighborhoods with largest week-over-week score increase
- `GET /api/flips` - citywide renovation-flip feed (LLC deed plus renovation permit within 60 days)
- `GET /api/radar` - concentrated-buying clusters (one LLC, 3+ buildings, one ZIP, within 90 days)
- `GET /api/score-history/{{zip}}` - daily composite score snapshots for a ZIP code

## Key pages

- https://pulsecities.com/ - landing page with live stats, the week's verified eviction-flip finding, and top-risk neighborhoods
- https://pulsecities.com/map - interactive heatmap of all NYC neighborhoods
- https://pulsecities.com/neighborhoods - directory of every scored ZIP, grouped by borough and ranked
- https://pulsecities.com/brooklyn (also /manhattan, /queens, /bronx, /staten-island) - borough-level ranked ZIP lists
- https://pulsecities.com/neighborhood/{{zip}} - per-neighborhood detail with signal breakdown and six-month score trend (e.g. /neighborhood/11216)
- https://pulsecities.com/operators - directory of tracked LLC operator networks
- https://pulsecities.com/operator/{{name}} - LLC operator portfolio profiles
- https://pulsecities.com/flips - Flip Watch, citywide feed of buildings bought by an LLC and filed for renovation within 60 days
- https://pulsecities.com/flips/editions - human-reviewed weekly editions of eviction-to-resale arcs with ACRIS document IDs
- https://pulsecities.com/radar - Speculation Radar, one LLC taking 3+ buildings in one ZIP within 90 days
- https://pulsecities.com/this-week - current week in NYC displacement (score movers, new filings, newest flips)
- https://pulsecities.com/this-week/archive - every past weekly edition at /week/{{YYYY-Www}}
- https://pulsecities.com/press - verified findings with ACRIS document IDs and downloadable paper-trail CSVs
- https://pulsecities.com/developers - public API documentation with endpoint reference and usage terms
- https://pulsecities.com/methodology - full scoring methodology

## About

Built by Michael Espin. PulseCities is a free public tool. The underlying data pipeline, scoring engine, and API are open for journalistic and research use.
"""


def main():
    # Atomic replace: nginx serves this file straight from disk, so a crawler
    # must never catch it half-written.
    fd, tmp_path = tempfile.mkstemp(dir=_OUT.parent, prefix=".llms.", suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(build())
    # mkstemp creates 0600; nginx workers need world-read or they serve 403.
    os.chmod(tmp_path, 0o644)
    os.replace(tmp_path, _OUT)
    print("wrote", _OUT)


if __name__ == "__main__":
    main()
