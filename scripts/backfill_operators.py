"""
Backfill script: populate operators and operator_parcels from operator_network_analysis.json.

Reads all 20 operator entries from the JSON file, upserts each into the operators table,
then populates operator_parcels from ownership_raw by matching on the operator's LLC entity
names. After inserting parcels, computes and caches borough_spread and
highest_displacement_score per operator.

Designed to be idempotent — safe to re-run with ON CONFLICT DO NOTHING for parcels and
ON CONFLICT DO UPDATE for operator rows.

Usage:
    python scripts/backfill_operators.py
"""

import json
import os
from datetime import datetime, timezone

from sqlalchemy import text, cast
from sqlalchemy.dialects.postgresql import JSONB

from models.database import get_scraper_db

# JSON file location relative to repo root
_JSON_PATH = os.path.join(os.path.dirname(__file__), "operator_network_analysis.json")

# Explicit slug overrides for operators whose names need special casing
_SLUG_OVERRIDES = {
    "MTEK": "mtek-nyc",
    "PHANTOM": "phantom-capital",   # operator_root in JSON is PHANTOM
    "BREDIF": "bredif",
}


def _make_slug(operator_root: str) -> str:
    if operator_root in _SLUG_OVERRIDES:
        return _SLUG_OVERRIDES[operator_root]
    return operator_root.lower().replace(" ", "-").replace("_", "-")


def _display_name(operator_root: str) -> str:
    """Human-readable display name — title-case with PHANTOM expanded."""
    if operator_root == "PHANTOM":
        return "Phantom Capital"
    return operator_root.title()


def backfill() -> None:
    with open(_JSON_PATH) as f:
        data = json.load(f)

    operators = data["operators"]
    total_parcels = 0

    with get_scraper_db() as db:
        for op in operators:
            root = op["operator_root"]
            slug = _make_slug(root)
            display = _display_name(root)
            llc_entities = op.get("llc_entities", [])

            # Upsert operator row — pass llc_entities as a JSON string, cast to JSONB
            # inside the query using explicit CAST to avoid psycopg2 binding ambiguity.
            db.execute(
                text(
                    """
                    INSERT INTO operators
                        (slug, operator_root, display_name, llc_entities,
                         total_properties, total_acquisitions, created_at, updated_at)
                    VALUES
                        (:slug, :operator_root, :display_name, CAST(:llc_entities AS jsonb),
                         :total_properties, :total_acquisitions, :now, :now)
                    ON CONFLICT (operator_root) DO UPDATE SET
                        slug               = EXCLUDED.slug,
                        display_name       = EXCLUDED.display_name,
                        llc_entities       = EXCLUDED.llc_entities,
                        total_properties   = EXCLUDED.total_properties,
                        total_acquisitions = EXCLUDED.total_acquisitions,
                        updated_at         = EXCLUDED.updated_at
                    """
                ),
                {
                    "slug": slug,
                    "operator_root": root,
                    "display_name": display,
                    "llc_entities": json.dumps(llc_entities),
                    "total_properties": op.get("total_properties", 0),
                    "total_acquisitions": op.get("total_acquisitions", 0),
                    "now": datetime.now(timezone.utc),
                },
            )

            # Fetch operator ID
            operator_id = db.execute(
                text("SELECT id FROM operators WHERE operator_root = :root"),
                {"root": root},
            ).scalar()

            if not llc_entities:
                continue

            # Pull distinct BBLs from ownership_raw for this operator's LLC entities
            parcels = db.execute(
                text(
                    """
                    SELECT DISTINCT ON (bbl)
                        bbl,
                        party_name_normalized  AS acquiring_entity,
                        doc_date               AS acquisition_date,
                        doc_amount             AS acquisition_price
                    FROM ownership_raw
                    WHERE party_type = '2'
                      AND party_name_normalized = ANY(:llc_names)
                    ORDER BY bbl, doc_date DESC
                    """
                ),
                {"llc_names": llc_entities},
            ).fetchall()

            # Bulk insert with idempotent conflict handling
            for parcel in parcels:
                db.execute(
                    text(
                        """
                        INSERT INTO operator_parcels
                            (operator_id, bbl, acquiring_entity, acquisition_date,
                             acquisition_price, created_at, updated_at)
                        VALUES
                            (:operator_id, :bbl, :acquiring_entity, :acquisition_date,
                             :acquisition_price, :now, :now)
                        ON CONFLICT (operator_id, bbl) DO NOTHING
                        """
                    ),
                    {
                        "operator_id": operator_id,
                        "bbl": parcel.bbl,
                        "acquiring_entity": parcel.acquiring_entity,
                        "acquisition_date": parcel.acquisition_date,
                        "acquisition_price": parcel.acquisition_price,
                        "now": datetime.now(timezone.utc),
                    },
                )
            total_parcels += len(parcels)

            # Compute and cache borough_spread
            borough_spread = db.execute(
                text(
                    """
                    SELECT COUNT(DISTINCT SUBSTRING(op.bbl, 1, 1))
                    FROM operator_parcels op
                    WHERE op.operator_id = :operator_id
                    """
                ),
                {"operator_id": operator_id},
            ).scalar()

            # Compute and cache highest_displacement_score (via parcels -> zip_code -> score)
            highest_score = db.execute(
                text(
                    """
                    SELECT MAX(ds.score)
                    FROM operator_parcels op
                    JOIN parcels p ON p.bbl = op.bbl
                    JOIN displacement_scores ds ON ds.zip_code = p.zip_code
                    WHERE op.operator_id = :operator_id
                    """
                ),
                {"operator_id": operator_id},
            ).scalar()

            db.execute(
                text(
                    """
                    UPDATE operators SET
                        borough_spread              = :borough_spread,
                        highest_displacement_score  = :highest_score,
                        updated_at                  = :now
                    WHERE id = :operator_id
                    """
                ),
                {
                    "borough_spread": borough_spread,
                    "highest_score": highest_score,
                    "operator_id": operator_id,
                    "now": datetime.now(timezone.utc),
                },
            )

    op_count = len(operators)
    print(f"Seeded {op_count} operators, {total_parcels} parcels total.")


if __name__ == "__main__":
    backfill()
