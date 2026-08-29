"""
NYC MapPLUTO parcel data loader.
Dataset: 64uk-42ks (NYC Open Data — Socrata API)
Update frequency: quarterly (major), monthly (zoning attributes only)

MapPLUTO is the foundation reference dataset. Every other table joins to
parcels on BBL. Run this BEFORE any other scraper on first setup.

Unlike the event-log scrapers, PLUTO is a reference table that gets
UPSERTED (on_conflict_do_update) — we want the latest parcel attributes,
not append-only history.

Geometry: Socrata returns the_geom as a GeoJSON polygon (lot boundary).
We compute the centroid and store it as a POINT in the parcels table.
This is sufficient for map rendering and spatial joins.

Field mapping (Socrata → model):
  bbl        → bbl          (may need construction from boro+block+lot)
  address    → address
  zipcode    → zip_code
  boro       → borough      (integer 1-5)
  block      → block
  lot        → lot
  unitsres   → units_res
  unitstotal → units_total
  yearbuilt  → year_built
  lotarea    → lot_area
  bldgarea   → bldg_area
  zonedist1  → zoning_dist
  landuse    → land_use
  ownername  → owner_name
  ownertype  → owner_type
  assesstot  → assessed_total
  the_geom   → geometry     (polygon → centroid point)
"""

import logging
from datetime import datetime, timezone

from geoalchemy2.shape import from_shape
from shapely.geometry import shape
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from models.bbl import normalize_bbl
from models.database import get_scraper_db
from models.properties import Parcel
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

DATASET_ID = "64uk-42ks"


class PlutoScraper(BaseScraper):
    """
    MapPLUTO loader — no watermark (full refresh each run).
    Runs quarterly or on-demand, not nightly.
    """

    SCRAPER_NAME = "mappluto"
    DATASET_ID = DATASET_ID
    INITIAL_LOOKBACK_DAYS = 0  # unused — no date watermark for PLUTO

    def _run(self, db) -> tuple[int, int, datetime | None]:
        # PLUTO has no date filter — always fetch all records
        # The dataset is ~900k rows; pagination handles this fine
        where = "bbl IS NOT NULL"

        run_started = datetime.now(timezone.utc)
        records_processed = 0
        records_failed = 0
        batch: list[dict] = []

        for raw in self.paginate(where):
            row = self._parse(db, raw)
            if row is None:
                records_failed += 1
                continue

            batch.append(row)

            if len(batch) >= 2_000:
                records_processed += self._upsert_batch(db, batch)
                batch = []

        if batch:
            records_processed += self._upsert_batch(db, batch)

        self._write_assessment_snapshot(db, run_started)

        logger.info("PLUTO load complete: %d upserted, %d failed", records_processed, records_failed)
        # No watermark for PLUTO — it's a full refresh
        return records_processed, records_failed, None

    def _write_assessment_snapshot(self, db, run_started: datetime) -> None:
        """File this run's assessments under the current year.

        Scoped to the lots this run actually refreshed. It used to read every
        parcel carrying an assessed value, which is 918,338 rows, and MapPLUTO
        covers 858,602 of them. The other 59,423 are condo unit lots that only
        scrapers/dof.py has ever seen, and their assessments are from fiscal
        years between 2010/11 and 2018/19. Stamping the calendar year on those
        is what let /property tell 10,197 indexed pages that a 2014/15 figure
        was "the 2026 tax year". A snapshot may only date what it looked at.

        DO UPDATE, not DO NOTHING. The old comment argued the first run of the
        year should win so a mid-year re-run could not disturb it, which reads
        as caution and is the same policy that kept a half-loaded permits table
        in score_history on 2026-08-28. A re-run is nearly always a correction.
        """
        tax_year = datetime.now(timezone.utc).year
        result = db.execute(
            text("""
                INSERT INTO assessment_history (bbl, assessed_total, tax_year, created_at)
                SELECT bbl, assessed_total, :tax_year, NOW()
                FROM parcels
                WHERE assessed_total IS NOT NULL
                  AND bbl IS NOT NULL
                  AND updated_at >= :run_started
                ON CONFLICT (bbl, tax_year) DO UPDATE
                   SET assessed_total = EXCLUDED.assessed_total
            """),
            {"tax_year": tax_year, "run_started": run_started},
        )
        db.commit()
        logger.info(
            "Assessment history snapshot written for tax_year=%d: %d lots this run touched",
            tax_year, result.rowcount,
        )

    def _parse(self, db, raw: dict) -> dict | None:
        # BBL — try direct field first, then construct from parts
        bbl_raw = raw.get("bbl")
        bbl = normalize_bbl(bbl_raw) if bbl_raw else None

        if bbl is None:
            # Try constructing from boro + block + lot
            try:
                boro = str(int(float(raw["boro"]))).strip()
                block = str(int(float(raw["block"]))).zfill(5)
                lot = str(int(float(raw["lot"]))).zfill(4)
                bbl = normalize_bbl(f"{boro}{block}{lot}")
            except (KeyError, TypeError, ValueError):
                pass

        if bbl is None:
            self.quarantine(db, raw, "invalid_or_missing_bbl")
            return None

        # Geometry: parse GeoJSON polygon → centroid point
        geometry = None
        geom_raw = raw.get("the_geom")
        if geom_raw and isinstance(geom_raw, dict):
            try:
                geom = shape(geom_raw)
                if not geom.is_empty:
                    centroid = geom.centroid
                    geometry = from_shape(centroid, srid=4326)
            except Exception as e:
                logger.debug("Failed to parse geometry for BBL %s: %s", bbl, e)

        return {
            "bbl": bbl,
            "borough": _safe_int(raw.get("boro")),
            "block": str(int(float(raw["block"]))).zfill(5) if raw.get("block") else None,
            "lot": str(int(float(raw["lot"]))).zfill(4) if raw.get("lot") else None,
            "address": (raw.get("address") or "").strip() or None,
            "zip_code": _clean_zip(raw.get("zipcode")),
            "units_res": _safe_int(raw.get("unitsres")),
            "units_total": _safe_int(raw.get("unitstotal")),
            "year_built": _safe_int(raw.get("yearbuilt")),
            "lot_area": _safe_float(raw.get("lotarea")),
            "bldg_area": _safe_float(raw.get("bldgarea")),
            "zoning_dist": (raw.get("zonedist1") or "").strip() or None,
            "land_use": (raw.get("landuse") or "").strip() or None,
            "owner_name": (raw.get("ownername") or "").strip() or None,
            "owner_type": (raw.get("ownertype") or "").strip() or None,
            "assessed_total": _safe_float(raw.get("assesstot")),
            "geometry": geometry,
            "on_speculation_watch_list": False,  # set separately from HPD dataset
        }

    def _upsert_batch(self, db, batch: list[dict]) -> int:
        stmt = (
            insert(Parcel)
            .values(batch)
            .on_conflict_do_update(
                constraint="uq_parcels_bbl",
                set_={
                    # Update all mutable fields on conflict
                    "address": insert(Parcel).excluded.address,
                    "zip_code": insert(Parcel).excluded.zip_code,
                    "units_res": insert(Parcel).excluded.units_res,
                    "units_total": insert(Parcel).excluded.units_total,
                    "year_built": insert(Parcel).excluded.year_built,
                    "lot_area": insert(Parcel).excluded.lot_area,
                    "bldg_area": insert(Parcel).excluded.bldg_area,
                    "zoning_dist": insert(Parcel).excluded.zoning_dist,
                    "land_use": insert(Parcel).excluded.land_use,
                    "owner_name": insert(Parcel).excluded.owner_name,
                    "owner_type": insert(Parcel).excluded.owner_type,
                    "assessed_total": insert(Parcel).excluded.assessed_total,
                    "geometry": insert(Parcel).excluded.geometry,
                    # SQLAlchemy's onupdate=utcnow fires for an ORM UPDATE and
                    # not for ON CONFLICT DO UPDATE, whose SET list is exactly
                    # this dict. Leaving it out froze parcels.updated_at at
                    # first insert: on 2026-08-28 the newest value in a 918k-row
                    # table read 2026-07-09, five weeks after a PLUTO run that
                    # had refreshed every one of them. The row data was right
                    # and the column that says when it was right was not, which
                    # is the false-freshness shape this codebase keeps paying
                    # for. tests/test_upsert_timestamps.py greps for the next one.
                    "updated_at": text("now()"),
                },
            )
        )
        result = db.execute(stmt)
        db.commit()
        return result.rowcount


def _safe_int(value) -> int | None:
    try:
        return int(float(value)) if value not in (None, "", "0") else None
    except (TypeError, ValueError):
        return None


def _safe_float(value) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _clean_zip(value: str | None) -> str | None:
    if not value:
        return None
    z = str(value).strip().split("-")[0]
    return z if len(z) == 5 and z.isdigit() else None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with get_scraper_db() as db:
        scraper = PlutoScraper()
        run = scraper.run(db)
        print(f"Status: {run.status} | Processed: {run.records_processed}")
