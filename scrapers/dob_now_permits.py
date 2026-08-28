"""
DOB NOW: Build job filings — the permit record BIS stopped being.

Dataset: w9ak-ipjd (NYC Open Data)
Update frequency: daily
Watermark field: first_permit_date

Why this exists, since a second permit scraper needs a reason. The other one
reads ipu4-2q9a, the legacy DOB Permit Issuance dataset, and DOB NOW superseded
it. The legacy feed still trickles, which is worse than stopping: the scraper
runs green every night, reports thirty-odd records and looks like a healthy
incremental feed. Measured 2026-08-28:

    our permits, last 365 days       6,501
    DOB NOW, same window           ~170,000        coverage 3.8%
    after the AL / 3+ unit filter      414         across 106 of 177 scored ZIPs

That 414 was carrying 24.7% of the composite displacement score, which is the
second-heaviest effective weight in the number the whole site is built on. At
four permits per ZIP the percentile normalisation is amplifying noise. Removing
the term entirely and renormalising moved ranks by a median of 13 places and a
max of 76, and changed half the top ten, so the sparsity was never cosmetic.

Which dataset, and why not the other one. DOB NOW publishes two: rbx6-tga4
(approved permits) and this one (job filings). rbx6-tga4 is the closer match to
"permit issuance" by name, and it is the wrong choice twice over. It has no
job_type, so there is no way to say "alteration to an occupied building", which
is the whole filter scoring applies. And it emits one row per work permit, so a
job with a general-construction and a structural permit counts twice: 35,556
rows over 29,723 jobs in one year. This dataset carries `job_type` natively and
`first_permit_date`, the day a job's first permit issued, one row per job.

What is deliberately NOT changed here. The scoring filter still reads
`permit_type = 'AL'` on parcels with 3+ residential units, unchanged, because
`job_type` maps onto the legacy short codes below. This scraper fixes the
signal's coverage and leaves its definition alone. DOB NOW also carries
`initial_cost` and existing-vs-proposed dwelling units, either of which would
sharpen the signal considerably, and folding those in at the same time would
confound a coverage fix with a definition change and leave nobody able to say
which one moved the scores.

    PYTHONPATH=. venv/bin/python -m scrapers.dob_now_permits
    PYTHONPATH=. venv/bin/python -m scrapers.dob_now_permits --since 2021-01-01

Field mapping (Socrata -> model):
  job_filing_number  -> source_id     the row's identity, one per job
  bbl                -> bbl           native here; ipu4-2q9a has to build it
  job_type           -> permit_type   mapped to AL / NB / DM / NW, see JOB_TYPE_CODE
  <trade>_work_type_ -> work_type     compact summary, see TRADES
  first_permit_date  -> filing_date   the permit event, and the watermark
  initial_cost       -> job_cost      100% populated; median job is $30,960
  existing_dwelling_units -> units_existing  89% populated
  proposed_dwelling_units -> units_proposed  1,985 jobs a year propose fewer
  house_no + street_name -> address
  owner_first_name + owner_last_name -> owner_name
"""

import argparse
import logging
from datetime import date, datetime, timedelta, timezone

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.dialects.postgresql import insert

from models.bbl import normalize_bbl
from models.database import get_scraper_db
from models.permits import PermitRaw
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

DATASET_ID = "w9ak-ipjd"
DATE_FIELD = "first_permit_date"
SOURCE = "dob_now"

# DOB NOW spells the job type out; BIS used two-letter codes and scoring reads
# those. Mapping here rather than in the scoring query means `permit_type = 'AL'`
# keeps meaning "alteration to an existing building" across both sources and no
# consumer had to change.
#
# "Alteration CO" is an alteration that also changes the certificate of
# occupancy, which is a stronger displacement signal than a plain alteration
# rather than a weaker one, so it maps to AL and not away from it.
#
# "ALT-CO - New Building with Existing Elements to Remain" is a new building
# that keeps a facade. BIS treated that as NB and so does this.
JOB_TYPE_CODE = {
    "ALTERATION": "AL",
    "ALTERATION CO": "AL",
    "NEW BUILDING": "NB",
    "ALT-CO - NEW BUILDING WITH EXISTING ELEMENTS TO REMAIN": "NB",
    "FULL DEMOLITION": "DM",
    "NO WORK": "NW",
}

# The per-trade flags, each "YES" or "NO" upstream, collapsed into one short
# string so `work_type` still says what the job actually involves. Ordered by
# what a reader would want first, and truncated to the column width.
TRADES = [
    ("general_construction_work_type_", "GC"),
    ("structural_work_type_", "STR"),
    ("foundation_work_type_", "FND"),
    ("full_demolition_work_type_", "DEM"),
    ("earth_work_work_type_", "EW"),
    ("support_of_excavation_work_type_", "SOE"),
    ("plumbing_work_type", "PLM"),
    ("mechanical_systems_work_type_", "MECH"),
    ("sprinkler_work_type", "SPR"),
    ("boiler_equipment_work_type_", "BLR"),
    ("solar_work_type_", "SOL"),
    ("green_roof_work_type_", "GRN"),
    ("sidewalk_shed_work_type_", "SHD"),
    ("suspended_scaffold_work_type_", "SCF"),
    ("protection_mechanical_methods_work_type_", "PMM"),
    ("place_of_assembly_work_type_", "POA"),
]

# The day DOB NOW became the system of record rather than a pilot. Filings
# before this exist but thin out fast: 69,516 first permits in 2021 against
# 1,985 in 2017. Anything older is already in the BIS half of permits_raw.
DOB_NOW_ERA_START = date(2021, 1, 1)


class JobFilingInput(BaseModel):
    """Rows from w9ak-ipjd.

    extra="allow" is REQUIRED, as it is on every scraper here: DOB adds columns
    without notice and this dataset already carries 95 of them.
    """
    model_config = ConfigDict(extra="allow")

    job_filing_number: str | None = None
    bbl: str | None = None
    borough: str | None = None
    block: str | None = None
    lot: str | None = None
    bin: str | None = None
    house_no: str | None = None
    street_name: str | None = None
    postcode: str | None = None
    zip: str | None = None
    job_type: str | None = None
    initial_cost: str | None = None
    existing_dwelling_units: str | None = None
    proposed_dwelling_units: str | None = None
    first_permit_date: str | None = None
    filing_date: str | None = None
    job_description: str | None = None
    owner_first_name: str | None = None
    owner_last_name: str | None = None
    owner_s_business_name: str | None = None


def _work_type(raw: dict) -> str:
    """The trades on this job, as a compact code string ("GC+STR").

    Empty string rather than None when no flag is set. The BIS half of the
    table already relies on that: NULL is distinct from NULL in a unique index,
    so a null here would let phantom duplicates through the identity that keys
    on it.
    """
    on = [code for field, code in TRADES
          if (raw.get(field) or "").strip().upper() in ("YES", "Y", "TRUE")]
    return "+".join(on)[:50]


def _bbl(record: JobFilingInput) -> str | None:
    """The lot, preferring the dataset's own bbl and rebuilding it when absent.

    99% of rows carry `bbl` directly, which is the one thing this dataset makes
    easier than ipu4-2q9a. The fallback matters anyway: a row with no bbl but a
    usable borough/block/lot is a row the scoring join can still use, and
    quarantining it would lose a real permit over a null column.
    """
    raw_bbl = (record.bbl or "").strip()
    if raw_bbl:
        # Socrata types this as a number, so it can arrive as "3034890001" or
        # "3034890001.0" depending on the serialiser.
        cleaned = raw_bbl.split(".")[0]
        normalized = normalize_bbl(cleaned)
        if normalized:
            return normalized

    borough_code = _BOROUGH_CODE.get((record.borough or "").strip().upper())
    block, lot = (record.block or "").strip(), (record.lot or "").strip()
    if not borough_code or not block or not lot:
        return None
    try:
        return normalize_bbl(f"{borough_code}-{int(block)}-{int(lot)}")
    except ValueError:
        return None


_BOROUGH_CODE = {"MANHATTAN": "1", "BRONX": "2", "BROOKLYN": "3",
                 "QUEENS": "4", "STATEN ISLAND": "5"}


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _number(value: str | None) -> float | None:
    """A Socrata numeric field that is only sometimes a number.

    These arrive as text and a blank, a dash or "N/A" is common. Returning None
    rather than raising keeps one malformed cost from quarantining a permit
    whose date, lot and job type are all fine.
    """
    if value is None:
        return None
    raw = str(value).strip().replace(",", "").replace("$", "")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _whole(value: str | None) -> int | None:
    n = _number(value)
    return int(n) if n is not None and n >= 0 else None


def _clean_zip(value: str | None) -> str | None:
    if not value:
        return None
    z = str(value).strip().split("-")[0].split(".")[0]
    return z if len(z) == 5 and z.isdigit() else None


class DobNowPermitsScraper(BaseScraper):
    SCRAPER_NAME = "dob_now_permits"
    DATASET_ID = DATASET_ID

    # DOB NOW backdates: a job's first_permit_date can be set weeks after the
    # fact when the permit record catches up, and a watermark that only ever
    # moves forward would step over those. Three days of overlap costs nothing,
    # because every row upserts on its filing number.
    WATERMARK_EXTRA_LOOKBACK_DAYS = 3

    def __init__(self, since: date | None = None) -> None:
        super().__init__()
        # Set by the backfill path to ignore the watermark and walk a fixed
        # window instead. None means normal incremental operation.
        self._since_override = since
        # Records the run under BACKFILL_STATUS so its 485k rows stay out of the
        # rolling average the nightly anomaly check reads. See BaseScraper.
        self.is_backfill = since is not None

    def _run(self, db) -> tuple[int, int, datetime | None]:
        if self._since_override:
            self._watermark_field = DATE_FIELD
            where = (f"{DATE_FIELD} > '{self._since_override.isoformat()}T00:00:00.000'")
            logger.info("%s: backfill from %s", self.SCRAPER_NAME, self._since_override)
        else:
            where = self.build_where_since(DATE_FIELD, db)

        # A job with no permit yet is a filing, not a permit event, and counting
        # it would put work in the score that may never break ground.
        where += f" AND {DATE_FIELD} IS NOT NULL"

        records_processed = 0
        records_failed = 0
        new_watermark: datetime | None = None
        batch: list[dict] = []
        seen: set[str] = set()

        # Paginate on :id, the base default. See BaseScraper.paginate: offset
        # paging over a non-unique sort key drops rows silently, and
        # first_permit_date is shared by thousands of jobs a day.
        for raw in self.paginate(where):
            row = self._parse(db, raw)
            if row is None:
                records_failed += 1
                continue

            # Within one walk the same filing number can appear twice, because
            # offset paging is only stable to the extent the source is. A batch
            # holding the same source_id twice makes ON CONFLICT raise
            # "cannot affect row a second time" and fails the whole run.
            if row["source_id"] in seen:
                continue
            seen.add(row["source_id"])

            batch.append(row)
            stamp = _to_dt(row["filing_date"])
            if stamp and (new_watermark is None or stamp > new_watermark):
                new_watermark = stamp

            if len(batch) >= 1_000:
                records_processed += self._upsert_batch(db, batch)
                batch = []
                seen.clear()

        if batch:
            records_processed += self._upsert_batch(db, batch)

        return records_processed, records_failed, new_watermark

    def _parse(self, db, raw: dict) -> dict | None:
        try:
            record = JobFilingInput.model_validate(raw)
        except ValidationError as exc:
            self.quarantine(db, raw, f"schema_validation_error:{exc}")
            return None

        filing_number = (record.job_filing_number or "").strip()
        if not filing_number:
            # Without it there is no identity, so the row would dedupe against
            # nothing and multiply on every run.
            self.quarantine(db, raw, "missing_job_filing_number")
            return None

        bbl = _bbl(record)
        bin_val = (record.bin or "").strip()[:7] or None
        if bbl is None and bin_val is None:
            self.quarantine(db, raw, "missing_bbl_and_bin")
            return None

        permit_date = _parse_date(record.first_permit_date)
        if permit_date is None:
            self.quarantine(db, raw, "missing_first_permit_date")
            return None

        job_type = (record.job_type or "").strip().upper()
        permit_type = JOB_TYPE_CODE.get(job_type)
        if permit_type is None:
            # A job type this map has never seen is not silently coded as
            # something else: scoring reads these codes and a wrong one moves a
            # score. Quarantine surfaces the new value instead.
            self.quarantine(db, raw, f"unmapped_job_type:{job_type[:60]}")
            return None

        address = f"{(record.house_no or '').strip()} {(record.street_name or '').strip()}".strip()
        owner = f"{(record.owner_first_name or '').strip()} {(record.owner_last_name or '').strip()}".strip()

        return {
            "bbl": bbl,
            "bin": bin_val,
            "address": address[:200] or None,
            "zip_code": _clean_zip(record.postcode or record.zip),
            "borough": (record.borough or "").strip()[:20] or None,
            "source": SOURCE,
            "source_id": filing_number[:40],
            "permit_type": permit_type,
            "work_type": _work_type(raw),
            "owner_name": (owner or (record.owner_s_business_name or "").strip())[:200] or None,
            # Scale, and whether the job removes housing. The whole reason the
            # scraper carries these: half of all "alterations" are under
            # $31,000, so counting a boiler swap and a gut renovation as one
            # thing each is most of what is wrong with the permit signal.
            "job_cost": _number(record.initial_cost),
            "units_existing": _whole(record.existing_dwelling_units),
            "units_proposed": _whole(record.proposed_dwelling_units),
            "filing_date": permit_date,
            "expiration_date": None,
            "job_description": record.job_description,
            "raw_data": raw,
        }

    def _upsert_batch(self, db, batch: list[dict]) -> int:
        # Targeted at this source's own identity, not a bare do-nothing. The
        # bare form would also swallow a collision on the BIS index, and a DOB
        # NOW row can no longer hit that one: it is partial on
        # `source_id IS NULL`. Naming the target means an unexpected conflict
        # raises rather than disappearing.
        stmt = insert(PermitRaw).values(batch).on_conflict_do_nothing(
            index_elements=["source", "source_id"],
            index_where=PermitRaw.source_id.isnot(None),
        )
        result = db.execute(stmt)
        db.commit()
        return result.rowcount


def _to_dt(d: date | None) -> datetime | None:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc) if d else None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DOB NOW Build job filings scraper")
    parser.add_argument("--since", help="backfill from this date (YYYY-MM-DD), "
                                        "ignoring the watermark")
    parser.add_argument("--era", action="store_true",
                        help=f"backfill the whole DOB NOW era, from {DOB_NOW_ERA_START}")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    since = None
    if args.era:
        since = DOB_NOW_ERA_START
    elif args.since:
        since = date.fromisoformat(args.since)

    with get_scraper_db() as db:
        run = DobNowPermitsScraper(since=since).run(db)
        print(f"Status: {run.status} | Processed: {run.records_processed} | "
              f"Quarantined: {run.records_failed}")
