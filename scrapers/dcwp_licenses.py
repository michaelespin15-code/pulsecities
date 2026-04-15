"""
DCWP Business Licenses scraper.
Dataset: w7w3-xahh (DCWP Issued Licenses, NYC Open Data)
Update frequency: incremental, watermark on license_creation_date

DCWP covers 48 regulated trade categories only (home improvement contractors,
tow trucks, tobacco dealers, etc.). NOT restaurants, retail, or most businesses.

Primary purpose: enables future contractor-license correlation in the
renovation-flip signal (LLC acquisition + renovation permit + contractor license
on same BBL = high-confidence displacement indicator).

Field mapping (Socrata → model):
  license_nbr           → license_nbr (natural upsert key)
  business_name         → business_name
  dba_trade_name        → dba_trade_name
  business_category     → business_category
  license_status        → license_status
  license_creation_date → license_creation_date (Date, watermark field)
  lic_expir_dd          → lic_expir_dd (Date)
  address_building      → address_building
  address_street_name   → address_street_name
  address_zip           → address_zip (nullable, present in most records)
  address_borough       → address_borough
  latitude              → latitude (Float)
  longitude             → longitude (Float)
  bbl                   → bbl (nullable, not always geocoded)
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
        where = self.build_where_since("license_creation_date", db)

        records_processed = 0
        records_failed = 0
        new_watermark: datetime | None = None

        for raw in self.paginate(where, order="license_creation_date ASC"):
            parsed = self._parse(db, raw)
            if parsed is None:
                records_failed += 1
                continue

            self._upsert(db, parsed, raw)
            records_processed += 1

            # Track watermark from license_creation_date
            creation_date = parsed.get("license_creation_date")
            if creation_date:
                # Convert date to datetime for watermark comparison
                dt = datetime(
                    creation_date.year,
                    creation_date.month,
                    creation_date.day,
                    tzinfo=timezone.utc,
                )
                if new_watermark is None or dt > new_watermark:
                    new_watermark = dt

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
