"""
One-time ZCTA geometry loader.

Fetches 178 Modified ZCTA (MODZCTA) boundary polygons from NYC Open Data
dataset pri4-ifjk and upserts them into the neighborhoods table.

This is NOT a BaseScraper subclass — it is a standalone data-load script
run once (or re-run to refresh boundary data). Idempotent via on_conflict_do_update.

Usage:
    python scripts/load_zcta.py

Environment:
    DATABASE_URL              — required (loaded from .env)
    NYC_OPEN_DATA_APP_TOKEN   — optional but recommended to avoid throttling
"""

import os
import sys
import logging

import json

import requests
import shapely
from dotenv import load_dotenv
from geoalchemy2.shape import from_shape
from sqlalchemy.dialects.postgresql import insert

# Load .env before importing models so DATABASE_URL is available
load_dotenv()

from models.database import get_scraper_db  # noqa: E402 (import after load_dotenv)
from models.neighborhoods import Neighborhood  # noqa: E402

logger = logging.getLogger(__name__)

ZCTA_DATASET_URL = "https://data.cityofnewyork.us/resource/pri4-ifjk.json"
FETCH_LIMIT = 300  # Dataset has 178 records; 300 is safe headroom


def _fetch_zcta_records() -> list[dict]:
    """Fetch all ZCTA records from NYC Open Data."""
    app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")
    headers = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token
    else:
        logger.warning("NYC_OPEN_DATA_APP_TOKEN not set — requests may be throttled")

    params = {"$limit": FETCH_LIMIT}
    resp = requests.get(ZCTA_DATASET_URL, headers=headers, params=params, timeout=60)
    if resp.status_code != 200:
        print(f"ERROR: ZCTA API returned {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
        sys.exit(1)

    records = resp.json()
    logger.info("Fetched %d ZCTA records from API", len(records))
    return records


def _validate_geometry(geom_dict: dict) -> bool:
    """Validate that the geometry dict has required GeoJSON keys."""
    return (
        isinstance(geom_dict, dict)
        and "type" in geom_dict
        and "coordinates" in geom_dict
    )


def _validate_zip_code(modzcta: str) -> bool:
    """Validate that modzcta is a 5-digit string."""
    return (
        isinstance(modzcta, str)
        and len(modzcta) == 5
        and modzcta.isdigit()
    )


def load_zcta(db) -> int:
    """
    Upsert all ZCTA boundary records into the neighborhoods table.

    Returns the number of records processed (not necessarily inserted — some
    may already exist and be updated in-place).
    """
    records = _fetch_zcta_records()

    loaded = 0
    skipped = 0

    for record in records:
        # Extract and validate zip code
        modzcta = record.get("modzcta")
        if not modzcta or not _validate_zip_code(str(modzcta).strip()):
            logger.warning("Skipping record with invalid modzcta: %r", modzcta)
            skipped += 1
            continue

        zip_code = str(modzcta).strip()

        # Extract and validate geometry
        geom_dict = record.get("the_geom")
        if not geom_dict or not _validate_geometry(geom_dict):
            logger.warning(
                "Skipping zip_code=%s — missing or invalid the_geom: %r",
                zip_code,
                geom_dict,
            )
            skipped += 1
            continue

        # Convert GeoJSON dict to PostGIS-compatible WKB via shapely.
        # Use shapely.from_geojson() (shapely 2.x API) rather than
        # shapely.geometry.shape() which has a breaking change in 2.0 for
        # MultiPolygon types returned by the Socrata API.
        try:
            shapely_geom = shapely.from_geojson(json.dumps(geom_dict))
            wkb_geom = from_shape(shapely_geom, srid=4326)
        except Exception as exc:
            logger.warning("Skipping zip_code=%s — geometry parse error: %s", zip_code, exc)
            skipped += 1
            continue

        # Extract optional label field
        name = record.get("label") or None

        # Upsert: insert or update geometry + name on conflict
        stmt = (
            insert(Neighborhood)
            .values(
                zip_code=zip_code,
                geometry=wkb_geom,
                name=name,
            )
            .on_conflict_do_update(
                constraint="uq_neighborhoods_zip_code",
                set_={
                    "geometry": wkb_geom,
                    "name": name,
                },
            )
        )
        db.execute(stmt)
        loaded += 1

    db.commit()

    if skipped > 0:
        logger.warning("Skipped %d records (missing zip or geometry)", skipped)

    return loaded


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    with get_scraper_db() as db:
        count = load_zcta(db)
    print(f"Loaded {count} ZCTA neighborhoods into database")
