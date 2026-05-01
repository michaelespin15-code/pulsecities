"""
Backfill script: populate operators and operator_parcels from operator_network_analysis.json.

Reads operator entries from the JSON file. Each candidate runs through the
classification gate before touching the DB:
  - PUBLIC_OPERATOR: upsert into operators, populate operator_parcels.
  - SUPPRESSED: delete existing operators row if present (cascade deletes parcels).
    Raw ACRIS records in ownership_raw are never touched.
  - REVIEW: skip without writing. Logged for human review.

Designed to be idempotent.

Usage:
    python scripts/backfill_operators.py
"""

import json
import logging
import os
from datetime import datetime, timezone

from sqlalchemy import text

from models.database import get_scraper_db
from scoring.operator_classification import OperatorClass, classify_operator_candidate

logger = logging.getLogger(__name__)

_JSON_PATH = os.path.join(os.path.dirname(__file__), "operator_network_analysis.json")

_SLUG_OVERRIDES = {
    "MTEK": "mtek-nyc",
    "PHANTOM": "phantom-capital",
    "BREDIF": "bredif",
}


def _make_slug(operator_root: str) -> str:
    if operator_root in _SLUG_OVERRIDES:
        return _SLUG_OVERRIDES[operator_root]
    return operator_root.lower().replace(" ", "-").replace("_", "-")


def _display_name(operator_root: str) -> str:
    if operator_root == "PHANTOM":
        return "Phantom Capital"
    return operator_root.title()


def _fetch_cluster_stats(db, llc_entities: list) -> dict:
    """Compute behavioral stats for a cluster from ownership_raw."""
    if not llc_entities:
        return {}
    row = db.execute(
        text("""
            SELECT
                COUNT(*)                                            AS acquisition_count,
                COUNT(DISTINCT bbl)                                AS property_count,
                COUNT(*) FILTER (WHERE doc_amount IS NULL OR doc_amount = 0)
                                                                   AS null_amount_count,
                CASE WHEN COUNT(*) > 0
                     THEN COUNT(*) FILTER (WHERE doc_amount IS NULL OR doc_amount = 0)::float / COUNT(*)
                     ELSE NULL END                                 AS null_amount_ratio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY doc_amount)
                                                                   AS median_doc_amount,
                COUNT(DISTINCT party_name_normalized)              AS distinct_grantor_count
            FROM ownership_raw
            WHERE party_type = '1'
              AND bbl IN (
                  SELECT DISTINCT bbl FROM ownership_raw
                  WHERE party_type = '2'
                    AND party_name_normalized = ANY(:names)
              )
        """),
        {"names": llc_entities},
    ).fetchone()
    if not row:
        return {}
    return {
        "acquisition_count": int(row.acquisition_count or 0),
        "property_count": int(row.property_count or 0),
        "null_amount_count": int(row.null_amount_count or 0),
        "null_amount_ratio": float(row.null_amount_ratio) if row.null_amount_ratio is not None else None,
        "median_doc_amount": float(row.median_doc_amount) if row.median_doc_amount is not None else None,
        "distinct_grantor_count": int(row.distinct_grantor_count or 0),
    }


def _delete_operator(db, operator_root: str) -> bool:
    """Delete operator row (cascade removes parcels). Returns True if a row existed."""
    result = db.execute(
        text("DELETE FROM operators WHERE operator_root = :root RETURNING id"),
        {"root": operator_root},
    )
    return result.rowcount > 0


def backfill() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    with open(_JSON_PATH) as f:
        data = json.load(f)

    operators = data["operators"]
    total_parcels = 0
    promoted = 0
    suppressed_deleted = 0
    review_skipped = 0
    now = datetime.now(timezone.utc)

    with get_scraper_db() as db:
        for op in operators:
            root = op["operator_root"]
            llc_entities = op.get("llc_entities", [])

            # Use the display name as the classification target. Individual entity
            # names were already filtered in operator_network_analysis.py, so by the
            # time a cluster appears in the JSON, its entities are clean. The
            # cluster-level check catches behavioral anomalies and any edge cases
            # that slipped past the entity-level filter.
            stats = _fetch_cluster_stats(db, llc_entities)
            # Supply acquisition_count and property_count from JSON when DB query
            # returns nothing useful (cluster may have no grantor data).
            stats.setdefault("acquisition_count", op.get("total_acquisitions", 0))
            stats.setdefault("property_count", op.get("total_properties", 0))

            # Classify using the display name (title-cased root). Name-based patterns
            # won't fire on clean roots like "MTEK" or "PHANTOM"; behavioral signals
            # from DB stats are the primary gate at this level.
            cls = classify_operator_candidate(_display_name(root), stats)

            if cls.operator_class == OperatorClass.SUPPRESSED:
                deleted = _delete_operator(db, root)
                suppressed_deleted += 1 if deleted else 0
                logger.info(
                    "SUPPRESSED %s (reasons: %s)%s",
                    root,
                    cls.reasons,
                    " — deleted existing row" if deleted else "",
                )
                continue

            if cls.operator_class == OperatorClass.REVIEW:
                review_skipped += 1
                logger.info("REVIEW %s (reasons: %s) — skipped", root, cls.reasons)
                continue

            # PUBLIC_OPERATOR — write to DB.
            slug = _make_slug(root)
            display = _display_name(root)

            db.execute(
                text("""
                    INSERT INTO operators
                        (slug, operator_root, display_name, llc_entities,
                         total_properties, total_acquisitions,
                         operator_class, classification_reasons,
                         classification_confidence, classified_at,
                         created_at, updated_at)
                    VALUES
                        (:slug, :operator_root, :display_name, CAST(:llc_entities AS jsonb),
                         :total_properties, :total_acquisitions,
                         :operator_class, CAST(:classification_reasons AS jsonb),
                         :classification_confidence, :classified_at,
                         :now, :now)
                    ON CONFLICT (operator_root) DO UPDATE SET
                        slug                      = EXCLUDED.slug,
                        display_name              = EXCLUDED.display_name,
                        llc_entities              = EXCLUDED.llc_entities,
                        total_properties          = EXCLUDED.total_properties,
                        total_acquisitions        = EXCLUDED.total_acquisitions,
                        operator_class            = EXCLUDED.operator_class,
                        classification_reasons    = EXCLUDED.classification_reasons,
                        classification_confidence = EXCLUDED.classification_confidence,
                        classified_at             = EXCLUDED.classified_at,
                        updated_at                = EXCLUDED.updated_at
                """),
                {
                    "slug": slug,
                    "operator_root": root,
                    "display_name": display,
                    "llc_entities": json.dumps(llc_entities),
                    "total_properties": op.get("total_properties", 0),
                    "total_acquisitions": op.get("total_acquisitions", 0),
                    "operator_class": cls.operator_class.value,
                    "classification_reasons": json.dumps(cls.reasons),
                    "classification_confidence": float(cls.confidence),
                    "classified_at": now,
                    "now": now,
                },
            )

            operator_id = db.execute(
                text("SELECT id FROM operators WHERE operator_root = :root"),
                {"root": root},
            ).scalar()

            if not llc_entities:
                promoted += 1
                continue

            parcels = db.execute(
                text("""
                    SELECT DISTINCT ON (bbl)
                        bbl,
                        party_name_normalized  AS acquiring_entity,
                        doc_date               AS acquisition_date,
                        doc_amount             AS acquisition_price
                    FROM ownership_raw
                    WHERE party_type = '2'
                      AND party_name_normalized = ANY(:llc_names)
                    ORDER BY bbl, doc_date DESC
                """),
                {"llc_names": llc_entities},
            ).fetchall()

            for parcel in parcels:
                db.execute(
                    text("""
                        INSERT INTO operator_parcels
                            (operator_id, bbl, acquiring_entity, acquisition_date,
                             acquisition_price, created_at, updated_at)
                        VALUES
                            (:operator_id, :bbl, :acquiring_entity, :acquisition_date,
                             :acquisition_price, :now, :now)
                        ON CONFLICT (operator_id, bbl) DO NOTHING
                    """),
                    {
                        "operator_id": operator_id,
                        "bbl": parcel.bbl,
                        "acquiring_entity": parcel.acquiring_entity,
                        "acquisition_date": parcel.acquisition_date,
                        "acquisition_price": parcel.acquisition_price,
                        "now": now,
                    },
                )
            total_parcels += len(parcels)

            borough_spread = db.execute(
                text("""
                    SELECT COUNT(DISTINCT SUBSTRING(op.bbl, 1, 1))
                    FROM operator_parcels op
                    WHERE op.operator_id = :operator_id
                """),
                {"operator_id": operator_id},
            ).scalar()

            highest_score = db.execute(
                text("""
                    SELECT MAX(ds.score)
                    FROM operator_parcels op
                    JOIN parcels p ON p.bbl = op.bbl
                    JOIN displacement_scores ds ON ds.zip_code = p.zip_code
                    WHERE op.operator_id = :operator_id
                """),
                {"operator_id": operator_id},
            ).scalar()

            db.execute(
                text("""
                    UPDATE operators SET
                        borough_spread             = :borough_spread,
                        highest_displacement_score = :highest_score,
                        updated_at                 = :now
                    WHERE id = :operator_id
                """),
                {
                    "borough_spread": borough_spread,
                    "highest_score": highest_score,
                    "operator_id": operator_id,
                    "now": now,
                },
            )

            promoted += 1
            logger.info("PROMOTED %s (%d parcels)", root, len(parcels))

    print(
        f"Done. promoted={promoted}  suppressed/deleted={suppressed_deleted}"
        f"  review={review_skipped}  parcels={total_parcels}"
    )


if __name__ == "__main__":
    backfill()
