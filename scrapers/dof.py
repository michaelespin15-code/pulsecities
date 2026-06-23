"""
NYC Department of Finance (DOF) property assessment data loader.
Dataset: w7rz-68fs (NYC Open Data — Socrata API)
Update frequency: annual (fiscal year assessments)

This is a full-refresh scraper, not incremental. Every run fetches all records
and upserts into the parcels table. There is no datetime watermark because DOF
does not expose a reliable updated_at field — assessment values change once per
fiscal year, not incrementally.

Field ownership (DOF vs MapPLUTO):
  DOF owns:    assessed_total
  MapPLUTO owns: units_res, units_total, geometry, owner_name, owner_type,
                 address, zip_code, year_built, lot_area, bldg_area,
                 zoning_dist, land_use

CRITICAL: The on_conflict_do_update set_ dict MUST include ONLY assessed_total.
If DOF were to overwrite geometry, units_res, or other PLUTO-sourced fields,
a DOF run after PLUTO would destroy parcel geometry and unit counts — breaking
the scoring engine's per-unit normalization.

Residential unit counts (units_res) come from MapPLUTO, not DOF. DOF only provides
assessed values.

Field mapping (Socrata → model):
  bble         → bbl          (10-digit BBL, already canonical in DOF dataset)
  boro         → borough      (int)
  block        → block        (str, zero-padded to 5 digits)
  lot          → lot          (str, zero-padded to 4 digits)
  avtot        → assessed_total (assessed value; primary field)
  fullval      → assessed_total (fallback if avtot is absent or zero)
  year         → (watermark/audit; stored as int of first year, e.g. "2018/19" → 2018)
  staddr       → address      (only if PLUTO address is None — DOF does not own address)
  zip          → zip_code     (only if PLUTO zip_code is None — DOF does not own zip)
"""

import logging
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert

from models.bbl import normalize_bbl
from models.database import get_scraper_db
from models.properties import Parcel
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

DATASET_ID = "w7rz-68fs"


class DOFScraper(BaseScraper):
    """DOF property assessment loader, full refresh each run; updates assessed_total only on conflict."""

    SCRAPER_NAME = "dof_assessments"
    DATASET_ID = DATASET_ID
    INITIAL_LOOKBACK_DAYS = 0  # unused, no date watermark for DOF assessments

    # DOF is a full-refresh dataset with 900K+ records. The default 50K-row
    # pages spike memory enough to trigger OOM on constrained hosts. 5K rows
    # per page keeps peak in-flight memory to ~5MB per fetch instead of ~50MB.
    PAGE_SIZE = 5_000

    def _run(self, db) -> tuple[int, int, datetime | None]:
        # DOF has no date filter, always fetch all records with a valid BBL
        where = "bble IS NOT NULL"

        records_processed = 0
        records_failed = 0
        batch: list[dict] = []

        for raw in self.paginate(where, order="bble ASC"):
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

        logger.info(
            "DOF assessment load complete: %d upserted, %d failed",
            records_processed,
            records_failed,
        )
        # No watermark for DOF, full refresh dataset
        return records_processed, records_failed, None

    def _parse(self, db, raw: dict) -> dict | None:
        # BBL: try bble field first (10-digit canonical), fall back to parts
        bbl_raw = raw.get("bble")
        bbl = None

        if bbl_raw:
            try:
                bbl = normalize_bbl(str(int(float(bbl_raw))))
            except (TypeError, ValueError):
                bbl = None

        if bbl is None:
            # Fallback: construct from boro + block + lot
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

        # Assessed value: prefer avtot, fall back to fullval if avtot is absent/zero
        assessed_total = _safe_float(raw.get("avtot"))
        if assessed_total is None or assessed_total == 0.0:
            fallback = _safe_float(raw.get("fullval"))
            if fallback is not None and fallback > 0.0:
                assessed_total = fallback
            elif assessed_total == 0.0:
                # Both present but zero, treat as None (no valid assessment)
                assessed_total = None

        # Year watermark: extract first 4 digits from "YYYY/YY" string
        year_raw = raw.get("year")
        assessment_year = None
        if year_raw:
            try:
                assessment_year = int(str(year_raw).strip()[:4])
            except (TypeError, ValueError):
                pass

        return {
            "bbl": bbl,
            "borough": _safe_int(raw.get("boro")),
            "block": str(int(float(raw["block"]))).zfill(5) if raw.get("block") else None,
            "lot": str(int(float(raw["lot"]))).zfill(4) if raw.get("lot") else None,
            # DOF may provide address and zip, but only as supplementary data
            # The on_conflict_do_update does NOT include these fields; PLUTO owns them
            "address": (raw.get("staddr") or "").strip() or None,
            "zip_code": _clean_zip(raw.get("zip")),
            "assessed_total": assessed_total,
            "raw_data": raw,
        }

    def _upsert_batch(self, db, batch: list[dict]) -> int:
        # Every row must carry the same columns. A multi-row insert with
        # heterogeneous keys, plus the Python-side defaults on
        # on_speculation_watch_list/created_at/updated_at, makes SQLAlchemy bind
        # the first row's defaults un-suffixed while later rows get _m1.., which
        # fails to compile (the f405 that aborted the nightly pipeline). Supply
        # all columns on every row, and the NOT NULL watch-list flag explicitly.
        now = datetime.now(timezone.utc)
        parcel_rows = []
        for row in batch:
            parcel_rows.append({
                "bbl": row["bbl"],
                "borough": row.get("borough"),
                "block": row.get("block"),
                "lot": row.get("lot"),
                "address": row.get("address"),
                "zip_code": row.get("zip_code"),
                "assessed_total": row["assessed_total"],
                "on_speculation_watch_list": False,
                "created_at": now,
                "updated_at": now,
            })

        stmt = (
            insert(Parcel)
            .values(parcel_rows)
            .on_conflict_do_update(
                constraint="uq_parcels_bbl",
                set_={
                    # DOF owns assessed_total ONLY
                    # Do NOT overwrite units_res, geometry, owner_name, address (PLUTO owns those)
                    "assessed_total": insert(Parcel).excluded.assessed_total,
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
        scraper = DOFScraper()
        run = scraper.run(db)
        print(f"Status: {run.status} | Processed: {run.records_processed}")
