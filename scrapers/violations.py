"""
HPD Housing Violations scraper.
Dataset: wvxf-dwi5 (NYC Open Data) — daily updates.
Watermark field: inspectiondate

Class B and C violations are the primary landlord-harassment signal in the
displacement scoring model. Weight these heavily — research shows they are
the most reliable leading indicator of forced displacement.

Field mapping (Socrata → model):
  violationid       → violation_id  (upsert key)
  bbl               → bbl
  housenumber + streetname → address
  zip               → zip_code
  boro              → borough
  class             → violation_class  (A / B / C)
  novdescription    → description
  inspectiondate    → inspection_date  (watermark)
  novissueddate     → nov_issued_date
  currentstatus     → current_status
"""

import logging
from datetime import date, datetime, timezone

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator
from sqlalchemy.dialects.postgresql import insert

from models.bbl import normalize_bbl
from models.database import get_scraper_db
from models.violations import ViolationRaw
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

DATASET_ID = "wvxf-dwi5"
DATE_FIELD = "inspectiondate"

# Pull 2 years of history on first run — gives scoring engine a full baseline
INITIAL_LOOKBACK_DAYS = 730


class ViolationRawInput(BaseModel):
    """Pydantic schema for HPD violations records from Socrata wvxf-dwi5.

    extra="allow" is required — HPD adds/renames fields without notice.
    Address is built from housenumber + streetname at validation time.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    violationid: str | None = None
    bbl: str | None = None
    housenumber: str | None = None
    streetname: str | None = None
    zip: str | None = None
    boro: str | None = None

    # HPD uses "class" which is a Python reserved word — alias it
    violation_class: str | None = None

    novdescription: str | None = None
    inspectiondate: str | None = None
    novissueddate: str | None = None
    currentstatus: str | None = None

    @field_validator("violation_class", mode="before")
    @classmethod
    def pull_class_field(cls, v):
        # "class" can't be a field name in Python; Socrata sends it as "class"
        # but our model_validator below handles the rename before Pydantic sees it
        return v

    @classmethod
    def from_raw(cls, data: dict) -> "ViolationRawInput":
        """Rename reserved field 'class' to 'violation_class' before parsing."""
        d = dict(data)
        if "class" in d:
            d["violation_class"] = d.pop("class")
        return cls.model_validate(d)


class ViolationsScraper(BaseScraper):
    SCRAPER_NAME = "hpd_violations"
    DATASET_ID = DATASET_ID
    INITIAL_LOOKBACK_DAYS = INITIAL_LOOKBACK_DAYS
    # HPD can lag up to 5 days on violation status updates; rescan the last
    # 10 days to catch status changes on previously-ingested records.
    WATERMARK_EXTRA_LOOKBACK_DAYS = 10

    def _run(self, db) -> tuple[int, int, datetime | None]:
        where = self.build_where_since(DATE_FIELD, db)

        records_processed = 0
        records_failed = 0
        new_watermark: datetime | None = None
        batch: list[dict] = []

        for raw in self.paginate(where, order=f"{DATE_FIELD} ASC"):
            row = self._parse(db, raw)
            if row is None:
                records_failed += 1
                continue

            batch.append(row)

            if row.get("inspection_date") and (
                new_watermark is None
                or _date_to_dt(row["inspection_date"]) > new_watermark
            ):
                new_watermark = _date_to_dt(row["inspection_date"])

            if len(batch) >= 1_000:
                records_processed += self._upsert_batch(db, batch)
                batch = []

        if batch:
            records_processed += self._upsert_batch(db, batch)

        return records_processed, records_failed, new_watermark

    def _parse(self, db, raw: dict) -> dict | None:
        try:
            record = ViolationRawInput.from_raw(raw)
        except ValidationError as exc:
            self.quarantine(db, raw, f"schema_validation_error:{exc}")
            return None

        # violation_id is required — it's the upsert key
        violation_id = (record.violationid or "").strip() or None
        if violation_id is None:
            self.quarantine(db, raw, "missing_required_field:violationid")
            return None

        inspection_date = _parse_date(record.inspectiondate)
        if inspection_date is None:
            self.quarantine(db, raw, "missing_required_field:inspectiondate")
            return None

        bbl_raw = record.bbl
        bbl = normalize_bbl(bbl_raw) if bbl_raw else None

        # Build address from housenumber + streetname
        parts = filter(None, [
            (record.housenumber or "").strip(),
            (record.streetname or "").strip(),
        ])
        address = " ".join(parts) or None

        zip_code = _clean_zip(record.zip)
        borough = (record.boro or "").strip() or None
        violation_class = (record.violation_class or "").strip().upper() or None

        # Class must be A, B, or C — anything else is quarantined
        if violation_class and violation_class not in ("A", "B", "C"):
            self.quarantine(db, raw, f"invalid_violation_class:{violation_class}")
            return None

        description = (record.novdescription or "").strip()[:500] or None
        nov_issued_date = _parse_date(record.novissueddate)
        current_status = (record.currentstatus or "").strip()[:50] or None

        return {
            "violation_id": violation_id,
            "bbl": bbl,
            "address": address,
            "zip_code": zip_code,
            "borough": borough,
            "violation_class": violation_class,
            "description": description,
            "inspection_date": inspection_date,
            "nov_issued_date": nov_issued_date,
            "current_status": current_status,
            "raw_data": raw,
        }

    def _upsert_batch(self, db, batch: list[dict]) -> int:
        # Deduplicate within the batch — same violation_id can appear multiple
        # times in a single Socrata page when HPD updates status mid-export.
        # Keep the last occurrence (highest current_status recency).
        seen: dict[str, dict] = {}
        for row in batch:
            seen[row["violation_id"]] = row
        deduped = list(seen.values())

        stmt = (
            insert(ViolationRaw)
            .values(deduped)
            .on_conflict_do_update(
                constraint="uq_violations_raw_violation_id",
                set_={
                    "current_status": insert(ViolationRaw).excluded.current_status,
                    "updated_at": datetime.now(timezone.utc),
                },
            )
        )
        result = db.execute(stmt)
        db.commit()
        return result.rowcount


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[:26], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _date_to_dt(d: date | None) -> datetime | None:
    if d is None:
        return None
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _clean_zip(value: str | None) -> str | None:
    if not value:
        return None
    z = str(value).strip().split("-")[0]
    return z if len(z) == 5 and z.isdigit() else None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with get_scraper_db() as db:
        scraper = ViolationsScraper()
        run = scraper.run(db)
        print(f"Status: {run.status} | Processed: {run.records_processed} | Quarantined: {run.records_failed}")
