"""
DCWP Business Licenses scraper.
Dataset: w7w3-xahh (DCWP Issued Licenses, NYC Open Data)
Update frequency: hybrid — daily incremental + periodic historical chunk refresh

DCWP covers 48 regulated trade categories only (home improvement contractors,
tow trucks, tobacco dealers, etc.). NOT restaurants, retail, or most businesses.

Primary purpose: enables future contractor-license correlation in the
renovation-flip signal (LLC acquisition + renovation permit + contractor license
on same BBL = high-confidence displacement indicator).

Watermark strategy (license_creation_date):
  Socrata rejects $select/:updated_at and $where/:updated_at on this dataset.
  license_creation_date is the only reliable date field for incremental filtering.
  Because it is a creation-only timestamp, renewals and status changes on older
  licenses are invisible to pure incremental runs.

Hybrid refresh strategy:
  1. Daily incremental: fetch licenses with license_creation_date in the past
     14 days (WATERMARK_EXTRA_LOOKBACK_DAYS).  Catches new licenses and rapid
     status changes on recently issued ones.
  2. Historical chunk refresh: run scripts/dcwp_refresh_historical.py weekly or
     monthly to re-fetch older date ranges and catch status/renewal/expiry
     changes on long-standing licenses.  Each record is upserted; the
     uq_dcwp_license_nbr unique constraint prevents duplicates.

Staleness tracking:
  source_last_seen_at     — updated every time a record is upserted
  source_last_refreshed_at — updated only during historical refresh runs
  source_hash             — SHA-256 of mutable source fields; changes logged

Recommended schedule:
  Daily  : nightly pipeline (incremental + 14-day lookback)
  Weekly : scripts/dcwp_refresh_historical.py --since <3mo_ago> --until <2wk_ago>
  Monthly: scripts/dcwp_refresh_historical.py --since <1yr_ago> --until <3mo_ago>
           in quarterly chunks

Field mapping (Socrata → model):
  license_nbr           → license_nbr (natural upsert key)
  business_name         → business_name
  dba_trade_name        → dba_trade_name
  business_category     → business_category
  license_status        → license_status
  license_creation_date → license_creation_date (Date); also watermark field
  lic_expir_dd          → lic_expir_dd (Date)
  address_building      → address_building
  address_street_name   → address_street_name
  address_zip           → address_zip (nullable)
  address_borough       → address_borough
  latitude              → latitude (Float)
  longitude             → longitude (Float)
  bbl                   → bbl (nullable, 10-digit)
"""

import hashlib
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

from sqlalchemy import text

from models.database import get_scraper_db
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

_BOOTSTRAP_DAYS = 90  # used when no watermark exists, instead of 5-year default

# Fields included in source_hash.  Covers all mutable data; excludes the natural
# key (license_nbr), creation date (immutable), and local DB metadata.
_HASH_FIELDS = (
    "license_status",
    "lic_expir_dd",
    "business_name",
    "dba_trade_name",
    "business_category",
    "address_building",
    "address_street_name",
    "address_zip",
    "address_borough",
    "latitude",
    "longitude",
    "bbl",
)


class _UpsertResult(NamedTuple):
    is_insert: bool     # True when the row did not previously exist
    hash_changed: bool  # True when an existing row's source data changed


class DcwpScraper(BaseScraper):
    SCRAPER_NAME = "dcwp_licenses"
    DATASET_ID = "w7w3-xahh"
    INITIAL_LOOKBACK_DAYS = 365 * 5  # 5 years; only used if bootstrap guard is bypassed
    PAGE_SIZE = 500  # keeps per-page memory flat on the 1.9GB droplet

    # license_creation_date is a creation-only timestamp; renewals and status
    # changes mutate existing rows without changing it.  Re-fetching the prior
    # 14 days on every incremental run ensures recently-issued licenses that
    # were quickly renewed or suspended are recaptured.  The upsert on
    # license_nbr (uq_dcwp_license_nbr) makes re-processing idempotent.
    WATERMARK_EXTRA_LOOKBACK_DAYS = 14

    def _run(self, db) -> tuple[int, int, datetime | None]:
        # Bootstrap guard: null watermark means no prior successful run completed.
        # Use 90 days instead of the 5-year default — the full dataset triggered
        # OOM kills before the watermark could ever be written.
        watermark = self.get_watermark(db)
        if watermark is None:
            logger.warning(
                "%s: watermark null, bootstrapping with %d-day lookback instead of 5-year",
                self.SCRAPER_NAME, _BOOTSTRAP_DAYS,
            )
            since = datetime.now(timezone.utc) - timedelta(days=_BOOTSTRAP_DAYS)
        else:
            since = watermark - timedelta(minutes=10) - timedelta(days=self.WATERMARK_EXTRA_LOOKBACK_DAYS)

        where = f"license_creation_date > '{since.strftime('%Y-%m-%dT%H:%M:%S.000')}'"

        logger.info(
            "%s: incremental fetch since=%s (watermark=%s extra_lookback_days=%d)",
            self.SCRAPER_NAME,
            since.date(),
            watermark.date() if watermark else None,
            self.WATERMARK_EXTRA_LOOKBACK_DAYS,
        )

        records_processed = records_failed = n_inserted = n_changed = 0
        new_watermark: datetime | None = None

        for raw in self.paginate(where, order="license_creation_date ASC"):
            parsed = self._parse(db, raw)
            if parsed is None:
                records_failed += 1
                continue

            result = self._upsert(db, parsed, raw, is_refresh=False)
            records_processed += 1
            if result.is_insert:
                n_inserted += 1
            if result.hash_changed:
                n_changed += 1

            wm_candidate = _parse_dt(raw.get("license_creation_date"))
            if wm_candidate and (new_watermark is None or wm_candidate > new_watermark):
                new_watermark = wm_candidate

        logger.info(
            "%s: incremental run complete — processed=%d failed=%d inserted=%d changed=%d",
            self.SCRAPER_NAME, records_processed, records_failed, n_inserted, n_changed,
        )
        return records_processed, records_failed, new_watermark

    def refresh_historical_range(
        self,
        db,
        since: date,
        until: date,
    ) -> tuple[int, int, int, int]:
        """
        Re-fetch and upsert all DCWP licenses whose license_creation_date falls
        in [since, until].  Catches status changes, renewals, expiry date updates,
        and address/location corrections on old licenses.

        Paginates through results in PAGE_SIZE chunks; never loads the full range
        into memory.  Each record is upserted via the existing uq_dcwp_license_nbr
        unique constraint, so re-running is idempotent.

        Updates source_last_seen_at and source_last_refreshed_at on every upserted
        row, marking it as deliberately rechecked.

        Returns: (processed, failed, inserted, changed)
        """
        where = (
            f"license_creation_date >= '{since.isoformat()}'"
            f" AND license_creation_date <= '{until.isoformat()}'"
        )
        logger.info(
            "%s: historical refresh range=%s to %s",
            self.SCRAPER_NAME, since, until,
        )

        processed = failed = n_inserted = n_changed = 0

        for raw in self.paginate(where, order="license_creation_date ASC"):
            parsed = self._parse(db, raw)
            if parsed is None:
                failed += 1
                continue

            result = self._upsert(db, parsed, raw, is_refresh=True)
            processed += 1
            if result.is_insert:
                n_inserted += 1
            if result.hash_changed:
                n_changed += 1

        logger.info(
            "%s: historical refresh complete — range=%s to %s "
            "processed=%d failed=%d inserted=%d changed=%d",
            self.SCRAPER_NAME, since, until, processed, failed, n_inserted, n_changed,
        )
        return processed, failed, n_inserted, n_changed

    def _parse(self, db, raw: dict) -> dict | None:
        """Parse a raw DCWP record; returns None if license_creation_date is missing."""
        license_creation_date_raw = raw.get("license_creation_date")
        if not license_creation_date_raw:
            self.quarantine(db, raw, "missing_required_field:license_creation_date")
            return None

        license_creation_date = _parse_date(license_creation_date_raw)
        if license_creation_date is None:
            self.quarantine(db, raw, "missing_required_field:license_creation_date")
            return None

        lic_expir_dd = _parse_date(raw.get("lic_expir_dd"))

        lat = _parse_float(raw.get("latitude"))
        lng = _parse_float(raw.get("longitude"))

        # BBL: only keep valid 10-digit values
        bbl_raw = raw.get("bbl")
        bbl = bbl_raw.strip() if bbl_raw else None
        if bbl and not (len(bbl) == 10 and bbl.isdigit()):
            bbl = None

        return {
            "license_nbr": _clean_str(raw.get("license_nbr")),
            "business_name": _clean_str(raw.get("business_name")),
            "dba_trade_name": _clean_str(raw.get("dba_trade_name")),
            "business_category": _clean_str(raw.get("business_category")),
            "license_status": _clean_str(raw.get("license_status")),
            "license_creation_date": license_creation_date,
            "lic_expir_dd": lic_expir_dd,
            "address_building": _clean_str(raw.get("address_building")),
            "address_street_name": _clean_str(raw.get("address_street_name")),
            "address_zip": _clean_str(raw.get("address_zip")),
            "address_borough": _clean_str(raw.get("address_borough")),
            "latitude": lat,
            "longitude": lng,
            "bbl": bbl,
        }

    def _upsert(self, db, parsed: dict, raw: dict, *, is_refresh: bool = False) -> _UpsertResult:
        """
        Upsert a DCWP license by license_nbr.

        Uses a CTE to atomically capture the pre-existing source_hash before the
        INSERT/UPDATE, enabling change detection without a separate SELECT round-trip.

        source_last_seen_at is updated on every call.
        source_last_refreshed_at is updated only when is_refresh=True (historical runs).

        Returns _UpsertResult(is_insert, hash_changed).
        """
        now = datetime.now(timezone.utc)
        new_hash = _compute_source_hash(parsed)
        refreshed_at = now if is_refresh else None

        row = db.execute(
            text("""
                WITH prev AS (
                    SELECT source_hash AS prev_hash
                    FROM dcwp_licenses
                    WHERE license_nbr = :license_nbr
                ),
                upserted AS (
                    INSERT INTO dcwp_licenses (
                        license_nbr, business_name, dba_trade_name, business_category,
                        license_status, license_creation_date, lic_expir_dd,
                        address_building, address_street_name, address_zip, address_borough,
                        latitude, longitude, bbl, raw_data,
                        source_hash, source_last_seen_at, source_last_refreshed_at,
                        created_at, updated_at
                    )
                    VALUES (
                        :license_nbr, :business_name, :dba_trade_name, :business_category,
                        :license_status, :license_creation_date, :lic_expir_dd,
                        :address_building, :address_street_name, :address_zip, :address_borough,
                        :latitude, :longitude, :bbl, CAST(:raw_data AS jsonb),
                        :source_hash, :now, :refreshed_at,
                        :now, :now
                    )
                    ON CONFLICT ON CONSTRAINT uq_dcwp_license_nbr DO UPDATE SET
                        business_name         = EXCLUDED.business_name,
                        dba_trade_name        = EXCLUDED.dba_trade_name,
                        business_category     = EXCLUDED.business_category,
                        license_status        = EXCLUDED.license_status,
                        license_creation_date = EXCLUDED.license_creation_date,
                        lic_expir_dd          = EXCLUDED.lic_expir_dd,
                        address_building      = EXCLUDED.address_building,
                        address_street_name   = EXCLUDED.address_street_name,
                        address_zip           = EXCLUDED.address_zip,
                        address_borough       = EXCLUDED.address_borough,
                        latitude              = EXCLUDED.latitude,
                        longitude             = EXCLUDED.longitude,
                        bbl                   = EXCLUDED.bbl,
                        raw_data              = EXCLUDED.raw_data,
                        source_hash           = EXCLUDED.source_hash,
                        source_last_seen_at   = EXCLUDED.source_last_seen_at,
                        source_last_refreshed_at = COALESCE(
                            EXCLUDED.source_last_refreshed_at,
                            dcwp_licenses.source_last_refreshed_at
                        ),
                        updated_at            = EXCLUDED.updated_at
                    RETURNING
                        source_hash AS new_hash,
                        (xmax::text::bigint > 0) AS was_update
                )
                SELECT
                    upserted.new_hash,
                    upserted.was_update,
                    prev.prev_hash
                FROM upserted
                LEFT JOIN prev ON true
            """),
            {
                **parsed,
                "raw_data": json.dumps(raw),
                "source_hash": new_hash,
                "now": now,
                "refreshed_at": refreshed_at,
            },
        ).fetchone()
        db.commit()

        is_insert = not row.was_update
        hash_changed = (
            bool(row.was_update)
            and row.prev_hash is not None
            and row.prev_hash != row.new_hash
        )

        if hash_changed:
            logger.info(
                "%s: license changed — nbr=%s status=%r expiry=%s name=%r zip=%r",
                self.SCRAPER_NAME,
                parsed.get("license_nbr"),
                parsed.get("license_status"),
                parsed.get("lic_expir_dd"),
                parsed.get("business_name"),
                parsed.get("address_zip"),
            )

        return _UpsertResult(is_insert=is_insert, hash_changed=hash_changed)

    def _checkpoint_watermark(self, db, run_id: int, watermark: datetime) -> None:
        """Retained for potential future OOM-safe checkpointing; not currently called."""
        db.execute(
            text("UPDATE scraper_runs SET watermark_timestamp = :wm WHERE id = :id"),
            {"wm": watermark, "id": run_id},
        )
        db.commit()
        logger.debug("%s: checkpoint watermark=%s", self.SCRAPER_NAME, watermark)


# ---------------------------------------------------------------------------
# Source hash
# ---------------------------------------------------------------------------

def _compute_source_hash(parsed: dict) -> str:
    """
    SHA-256 of the normalized mutable source fields.  Deterministic: same input
    always produces same hash.  Detects status changes, renewals, expiry updates,
    address corrections, and location changes.  Does NOT hash the natural key,
    creation date (immutable), or local DB metadata.
    """
    payload = {
        k: str(parsed[k]) if parsed.get(k) is not None else None
        for k in _HASH_FIELDS
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(value: str | None) -> date | None:
    """Parse ISO date string from Socrata into a date object."""
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _parse_dt(value: str | None) -> datetime | None:
    """Parse Socrata calendar_date ISO timestamp into a timezone-aware datetime."""
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value[:26], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_float(value) -> float | None:
    """Parse a string or numeric value to float, or return None."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _clean_str(value: str | None) -> str | None:
    """Strip whitespace from string values, return None for empty strings."""
    if not value:
        return None
    cleaned = value.strip()
    return cleaned if cleaned else None


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    try:
        with get_scraper_db() as db:
            scraper = DcwpScraper()
            run = scraper.run(db)
            print(f"Status: {run.status} | Processed: {run.records_processed}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
