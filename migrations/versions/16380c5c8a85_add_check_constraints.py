"""add_check_constraints

Revision ID: 16380c5c8a85
Revises: b0b44943f886
Create Date: 2026-04-13 03:38:41.633509

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '16380c5c8a85'
down_revision: Union[str, None] = 'b0b44943f886'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ---- ownership_raw.party_type ----
    # ACRIS party_type values: '1' = grantor, '2' = grantee.
    # Scraper always writes '2' (GRANTEE_PARTY_TYPE constant).
    # Score engine filters: WHERE party_type = '2'.
    # Safe to constrain: existing data verified to be '1' or '2' only.
    op.create_check_constraint(
        "ck_ownership_party_type",
        "ownership_raw",
        "party_type IS NULL OR party_type IN ('1', '2')",
    )

    # ---- evictions_raw.eviction_type ----
    # OCA dataset residential_commercial_ind field values are full words.
    # Actual data in production: 'Residential', 'Commercial'.
    op.create_check_constraint(
        "ck_evictions_eviction_type",
        "evictions_raw",
        "eviction_type IS NULL OR eviction_type IN ('Residential', 'Commercial')",
    )

    # ---- parcels.borough ----
    # NYC boroughs: 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island.
    # Used by _get_zip_borough and _compute_borough_medians in scoring.
    op.create_check_constraint(
        "ck_parcels_borough",
        "parcels",
        "borough IS NULL OR (borough BETWEEN 1 AND 5)",
    )

    # ---- parcels.units_res ----
    # Residential unit counts from PLUTO/DOF — cannot be negative.
    op.create_check_constraint(
        "ck_parcels_units_res_non_negative",
        "parcels",
        "units_res IS NULL OR units_res >= 0",
    )

    # ---- displacement_scores.score ----
    # Invariant in compute.py: score = max(1.0, min(100.0, round(composite, 1)))
    op.create_check_constraint(
        "ck_displacement_scores_score_range",
        "displacement_scores",
        "score IS NULL OR (score >= 1.0 AND score <= 100.0)",
    )

    # ---- score_history.composite_score ----
    # Same invariant as displacement_scores.score (snapshotted nightly).
    op.create_check_constraint(
        "ck_score_history_composite_score_range",
        "score_history",
        "composite_score IS NULL OR (composite_score >= 1.0 AND composite_score <= 100.0)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_score_history_composite_score_range", "score_history")
    op.drop_constraint("ck_displacement_scores_score_range", "displacement_scores")
    op.drop_constraint("ck_parcels_units_res_non_negative", "parcels")
    op.drop_constraint("ck_parcels_borough", "parcels")
    op.drop_constraint("ck_evictions_eviction_type", "evictions_raw")
    op.drop_constraint("ck_ownership_party_type", "ownership_raw")
