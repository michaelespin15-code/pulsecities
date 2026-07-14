"""
Guard: /property/{bbl} serves a real content body (not the map-app shell) with
per-building schema, and thin buildings (no records, no score) are noindex so
they don't dilute the index.
"""

import re
import warnings

import pytest
from sqlalchemy import text
from fastapi.testclient import TestClient

from api.main import app
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)


def _bbl_with_records():
    db = SessionLocal()
    try:
        r = db.execute(text(
            "SELECT o.bbl FROM ownership_raw o JOIN parcels p ON p.bbl = o.bbl "
            "WHERE p.address IS NOT NULL LIMIT 1"
        )).first()
        return r.bbl if r else None
    finally:
        db.close()


def _bbl_without_records():
    db = SessionLocal()
    try:
        r = db.execute(text("""
            SELECT p.bbl FROM parcels p
            LEFT JOIN displacement_scores ds ON ds.zip_code = p.zip_code
            WHERE p.address IS NOT NULL AND ds.score IS NULL
              AND NOT EXISTS(SELECT 1 FROM ownership_raw o WHERE o.bbl = p.bbl)
              AND NOT EXISTS(SELECT 1 FROM evictions_raw e WHERE e.bbl = p.bbl)
              AND NOT EXISTS(SELECT 1 FROM permits_raw pr WHERE pr.bbl = p.bbl)
            LIMIT 1
        """)).first()
        return r.bbl if r else None
    finally:
        db.close()


def test_property_page_is_real_content_not_map_shell():
    bbl = _bbl_with_records()
    if not bbl:
        pytest.skip("no property with records in current data")
    body = client.get(f"/property/{bbl}").text
    h1 = re.search(r"<h1>(.*?)</h1>", body)
    assert h1 and h1.group(1) != "PulseCities", "H1 must be the address, not the app shell"
    assert '"@type": "Place"' in body
    assert '"@type": "BreadcrumbList"' in body
    assert "/neighborhood/" in body  # links up to the ZIP page
    assert re.search(r'name="robots" content="index, follow"', body)


def test_thin_property_is_noindex():
    bbl = _bbl_without_records()
    if not bbl:
        pytest.skip("no recordless parcel found")
    body = client.get(f"/property/{bbl}").text
    assert re.search(r'name="robots" content="noindex, follow"', body), \
        "a building with no records/score must be noindex"
