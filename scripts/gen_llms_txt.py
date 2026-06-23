"""
Generate frontend/llms.txt from live scoring weights and top-risk data.

Reads the signal weights from scoring/compute.py and the current scores from the
database, so the published file always reconciles to what the API returns. Active
weights are the raw weights rescaled over the live active signals (dormant signals
have their mass redistributed each run, matching compute_scores). Run after a
scoring run:

    PYTHONPATH=. venv/bin/python scripts/gen_llms_txt.py
"""

from pathlib import Path

from sqlalchemy import text

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
    return db.execute(text("""
        SELECT n.zip_code, n.name, round(ds.score::numeric, 1) AS score
        FROM displacement_scores ds
        JOIN neighborhoods n ON n.zip_code = ds.zip_code
        WHERE ds.score IS NOT NULL
        ORDER BY ds.score DESC
        LIMIT :lim
    """), {"lim": limit}).fetchall()


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

    nbhd = "\n".join(
        f"- {r.name} ({r.zip_code}, {_borough(r.zip_code)}) - score {r.score}, high displacement pressure"
        for r in top
    )
    dormant_note = (
        f" {', '.join(dormant)} is currently dormant pending second-year DHCR data, "
        "and its weight is redistributed across the active signals each run."
        if dormant else ""
    )

    return f"""# PulseCities

> Real-time displacement risk intelligence for all 178 NYC neighborhoods, built on public data signals updated daily from city records.

PulseCities tracks housing displacement pressure across New York City by aggregating and scoring signals from public datasets: LLC property acquisitions (ACRIS), renovation permit filings (DOB), tenant complaint rates (311/HPD), residential eviction filings, HPD housing violations, and rent-stabilized unit loss (DHCR). Each of the 178 ZIP code neighborhoods receives a daily composite score from 0 to 100.

## What this site provides

- Displacement risk scores (0 to 100) for all 178 NYC neighborhoods, updated nightly
- Signal breakdown showing which factors drive pressure in each area
- Week-over-week score changes to identify neighborhoods under acute pressure
- LLC operator profiles linking corporate entities to their NYC property portfolios
- Block-level investigation tools: address search, renovation-flip pattern detection, civic event timelines
- Weekly email digests for subscribers tracking specific neighborhoods

## Current high-risk neighborhoods (as of latest scoring run)

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

- `GET /api/neighborhoods` - GeoJSON FeatureCollection of all 178 neighborhoods with scores
- `GET /api/neighborhoods/{{zip}}/score` - score and signal breakdown for a ZIP code
- `GET /api/neighborhoods/top-risk` - top N neighborhoods by current score
- `GET /api/neighborhoods/top-movers` - neighborhoods with largest week-over-week score increase

## Key pages

- https://pulsecities.com/ - landing page with live stats and top-risk neighborhoods
- https://pulsecities.com/map - interactive heatmap of all 178 neighborhoods
- https://pulsecities.com/neighborhood/{{zip}} - per-neighborhood detail (e.g. /neighborhood/11216)
- https://pulsecities.com/operator/{{name}} - LLC operator portfolio profiles
- https://pulsecities.com/methodology - full scoring methodology

## About

Built by Michael Espin. PulseCities is a free public tool. The underlying data pipeline, scoring engine, and API are open for journalistic and research use.
"""


def main():
    _OUT.write_text(build(), encoding="utf-8")
    print("wrote", _OUT)


if __name__ == "__main__":
    main()
