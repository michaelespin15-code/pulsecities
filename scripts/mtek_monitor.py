"""
Nightly monitor for new HPD violations, DOB permits, and eviction filings
on MTEK-owned properties. Runs after scrapers complete in pipeline.py.

Watermark stored in scraper_runs (scraper_name="mtek_monitor"):
  - First run: backfills all events since earliest MTEK acquisition
  - Subsequent runs: only looks at raw rows created since last watermark

ON CONFLICT DO NOTHING means re-running is always safe.
"""

import logging
from datetime import date, datetime, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from config.mtek import MTEK_BBLS, MTEK_EARLIEST_ACQUISITION, MTEK_PORTFOLIO
from models.database import get_scraper_db
from models.mtek_alerts import MtekAlert
from models.scraper import ScraperRun

logger = logging.getLogger(__name__)

SCRAPER_NAME = "mtek_monitor"


def _last_watermark(db) -> datetime | None:
    run = (
        db.query(ScraperRun)
        .filter(
            ScraperRun.scraper_name == SCRAPER_NAME,
            ScraperRun.status == "success",
            ScraperRun.watermark_timestamp.isnot(None),
        )
        .order_by(ScraperRun.started_at.desc())
        .first()
    )
    return run.watermark_timestamp if run else None


def _bbl_in(bbls: frozenset[str]) -> str:
    return ", ".join(f"'{b}'" for b in bbls)


def _scan_violations(db, bbl_in: str, watermark: datetime | None) -> list[dict]:
    if watermark:
        clause = "AND v.created_at > :ts"
        params: dict = {"ts": watermark}
    else:
        clause = "AND v.inspection_date >= :cutoff"
        params = {"cutoff": MTEK_EARLIEST_ACQUISITION}

    rows = db.execute(text(f"""
        SELECT violation_id, bbl, address, violation_class,
               description, inspection_date, created_at
        FROM violations_raw v
        WHERE bbl IN ({bbl_in})
          {clause}
          AND violation_id IS NOT NULL
        ORDER BY inspection_date
    """), params).fetchall()

    out = []
    for r in rows:
        meta = MTEK_PORTFOLIO.get(r.bbl, {})
        acquired: date = meta.get("acquired", MTEK_EARLIEST_ACQUISITION)
        if r.inspection_date and r.inspection_date < acquired:
            continue
        out.append({
            "bbl": r.bbl,
            "address": r.address or meta.get("address"),
            "entity": meta.get("entity"),
            "alert_type": "hpd_violation",
            "violation_class": r.violation_class,
            "event_date": r.inspection_date,
            "detail": (r.description or "")[:500] or None,
            "source_id": r.violation_id,
        })
    return out


def _scan_permits(db, bbl_in: str, watermark: datetime | None) -> list[dict]:
    if watermark:
        clause = "AND p.created_at > :ts"
        params: dict = {"ts": watermark}
    else:
        clause = "AND p.filing_date >= :cutoff"
        params = {"cutoff": MTEK_EARLIEST_ACQUISITION}

    rows = db.execute(text(f"""
        SELECT id, bbl, address, permit_type, work_type,
               job_description, filing_date, created_at
        FROM permits_raw p
        WHERE bbl IN ({bbl_in})
          {clause}
        ORDER BY filing_date
    """), params).fetchall()

    out = []
    for r in rows:
        meta = MTEK_PORTFOLIO.get(r.bbl, {})
        acquired: date = meta.get("acquired", MTEK_EARLIEST_ACQUISITION)
        if r.filing_date and r.filing_date < acquired:
            continue
        parts = [r.permit_type or "", r.work_type or ""]
        detail = " — ".join(p for p in parts if p) or r.job_description or None
        out.append({
            "bbl": r.bbl,
            "address": r.address or meta.get("address"),
            "entity": meta.get("entity"),
            "alert_type": "dob_permit",
            "violation_class": None,
            "event_date": r.filing_date,
            "detail": detail[:500] if detail else None,
            "source_id": f"permit_{r.id}",
        })
    return out


def _scan_evictions(db, bbl_in: str, watermark: datetime | None) -> list[dict]:
    if watermark:
        clause = "AND e.created_at > :ts"
        params: dict = {"ts": watermark}
    else:
        clause = "AND e.executed_date >= :cutoff"
        params = {"cutoff": MTEK_EARLIEST_ACQUISITION}

    rows = db.execute(text(f"""
        SELECT id, bbl, address, eviction_type,
               executed_date, docket_number, created_at
        FROM evictions_raw e
        WHERE bbl IN ({bbl_in})
          {clause}
        ORDER BY executed_date
    """), params).fetchall()

    out = []
    for r in rows:
        meta = MTEK_PORTFOLIO.get(r.bbl, {})
        acquired: date = meta.get("acquired", MTEK_EARLIEST_ACQUISITION)
        if r.executed_date and r.executed_date < acquired:
            continue
        out.append({
            "bbl": r.bbl,
            "address": r.address or meta.get("address"),
            "entity": meta.get("entity"),
            "alert_type": "eviction",
            "violation_class": None,
            "event_date": r.executed_date,
            "detail": r.eviction_type or None,
            "source_id": r.docket_number or f"eviction_{r.id}",
        })
    return out


def _insert(db, alerts: list[dict]) -> int:
    if not alerts:
        return 0
    stmt = (
        insert(MtekAlert)
        .values(alerts)
        .on_conflict_do_nothing(constraint="uq_mtek_alerts_bbl_type_source")
    )
    result = db.execute(stmt)
    db.commit()
    return result.rowcount


def run_mtek_monitor(db) -> int:
    """Scan for new MTEK property events. Returns count of new alerts inserted."""
    started_at = datetime.now(timezone.utc)
    scraper_run = ScraperRun(
        scraper_name=SCRAPER_NAME,
        started_at=started_at,
        status="running",
    )
    db.add(scraper_run)
    db.commit()

    inserted = 0
    try:
        watermark = _last_watermark(db)
        bbl_in = _bbl_in(MTEK_BBLS)

        if watermark:
            logger.info("mtek_monitor: incremental since %s", watermark.date())
        else:
            logger.info("mtek_monitor: first run, backfilling from %s", MTEK_EARLIEST_ACQUISITION)

        violations = _scan_violations(db, bbl_in, watermark)
        permits    = _scan_permits(db, bbl_in, watermark)
        evictions  = _scan_evictions(db, bbl_in, watermark)

        inserted = _insert(db, violations + permits + evictions)

        logger.info(
            "mtek_monitor: %dv / %dp / %de checked — %d new alerts",
            len(violations), len(permits), len(evictions), inserted,
        )

        scraper_run.status = "success"
        scraper_run.records_processed = inserted
        scraper_run.records_failed = 0
        scraper_run.watermark_timestamp = datetime.now(timezone.utc)

    except Exception as exc:
        scraper_run.status = "failure"
        scraper_run.error_message = str(exc)
        logger.exception("mtek_monitor failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        scraper_run.completed_at = datetime.now(timezone.utc)
        db.add(scraper_run)
        db.commit()

    return inserted


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO)
    with get_scraper_db() as db:
        n = run_mtek_monitor(db)
        print(f"New alerts: {n}")
