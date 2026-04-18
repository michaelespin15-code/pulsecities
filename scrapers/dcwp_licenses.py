"""
DCWP Business Licenses scraper.
Dataset: w7w3-xahh (DCWP Issued Licenses, NYC Open Data)
Update frequency: incremental, watermark on Socrata :updated_at

DCWP covers 48 regulated trade categories only (home improvement contractors,
tow trucks, tobacco dealers, etc.). NOT restaurants, retail, or most businesses.

Primary purpose: enables future contractor-license correlation in the
renovation-flip signal (LLC acquisition + renovation permit + contractor license
on same BBL = high-confidence displacement indicator).

Watermark uses Socrata's :updated_at system column (row-level modification time)
rather than license_creation_date.  License renewals and status changes update
existing rows in-place, keeping the original license_creation_date; filtering on
creation date would silently skip all such updates.  :updated_at captures every
mutation regardless of when the license was originally issued.

NOTE: after deploying this change, clear the existing DCWP watermark so the
scraper re-fetches with the 5-year lookback against :updated_at.  Run:
    python scripts/watermark_drift_reset.py
and manually delete the dcwp_licenses row from scraper_runs, or just let the
next run use the old creation-date watermark as a conservative :updated_at floor
(safe — it will just fetch fewer records on first run, then converge).

Field mapping (Socrata → model):
  license_nbr           → license_nbr (natural upsert key)
  business_name         → business_name
  dba_trade_name        → dba_trade_name
  business_category     → business_category
  license_status        → license_status
  license_creation_date → license_creation_date (Date)
  lic_expir_dd          → lic_expir_dd (Date)
  address_building      → address_building
  address_street_name   → address_street_name
  address_zip           → address_zip (nullable, present in most records)
  address_borough       → address_borough
  latitude              → latitude (Float)
  longitude             → longitude (Float)
  bbl                   → bbl (nullable, not always geocoded)
  :updated_at           → watermark only (not stored in model)
"""

import logging
from datetime import date, datetime, timezone

from sqlalchemy import text

from models.database import get_scraper_db
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class DcwpScraper(BaseScraper):
    SCRAPER_NAME = "dcwp_licenses"
    DATASET_ID = "w7w3-xahh"
    INITIAL_LOOKBACK_DAYS = 365 * 5  # 5 years; DCWP has long-lived licenses

    def _run(self, db) -> tuple[int, int, datetime | None]:
        # Filter and order on :updated_at so renewals/status-changes on old
        # licenses are fetched.  Request :updated_at explicitly via $select
        # since Socrata system columns are not returned by default.
        where = self.build_where_since(":updated_at", db)

        records_processed = 0
        records_failed = 0
        new_watermark: datetime | None = None

        for raw in self.paginate(where, select=":updated_at, *", order=":updated_at ASC"):
            parsed = self._parse(db, raw)
            if parsed is None:
                records_failed += 1
                continue

            self._upsert(db, parsed, raw)
            records_processed += 1

            # Track watermark from :updated_at (row-level Socrata modification time)
            updated_at = _parse_dt(raw.get(":updated_at"))
            if updated_at and (new_watermark is None or updated_at > new_watermark):
                new_watermark = updated_at

        return records_processed, records_failed, new_watermark

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

        # Parse numeric fields safely
        lat = _parse_float(raw.get("latitude"))
        lng = _parse_float(raw.get("longitude"))

        # BBL: use as-is (already 10-digit in DCWP dataset per api-verification.md)
        bbl_raw = raw.get("bbl")
        bbl = bbl_raw.strip() if bbl_raw else None
        # Normalize to 10 digits if present — only keep valid BBLs
        if bbl and not (len(bbl) == 10 and bbl.isdigit()):
            bbl = None  # invalid format — leave nullable

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

    def _upsert(self, db, parsed: dict, raw: dict) -> None:
        """Upsert a DCWP license; license_nbr is the natural key, all mutable fields updated on conflict."""
        now = datetime.now(timezone.utc)
        db.execute(
            text("""
                INSERT INTO dcwp_licenses (
                    license_nbr, business_name, dba_trade_name, business_category,
                    license_status, license_creation_date, lic_expir_dd,
                    address_building, address_street_name, address_zip, address_borough,
                    latitude, longitude, bbl, raw_data, created_at, updated_at
                )
                VALUES (
                    :license_nbr, :business_name, :dba_trade_name, :business_category,
                    :license_status, :license_creation_date, :lic_expir_dd,
                    :address_building, :address_street_name, :address_zip, :address_borough,
                    :latitude, :longitude, :bbl, CAST(:raw_data AS jsonb), :now, :now
                )
                ON CONFLICT ON CONSTRAINT uq_dcwp_license_nbr
                DO UPDATE SET
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
                    updated_at            = EXCLUDED.updated_at
            """),
            {
                **parsed,
                "raw_data": __import__("json").dumps(raw),
                "now": now,
            },
        )
        db.commit()


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
    """Parse Socrata :updated_at ISO timestamp into a timezone-aware datetime."""
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
