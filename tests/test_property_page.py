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


def _bbl_with_deeds_and_assessment():
    db = SessionLocal()
    try:
        r = db.execute(text("""
            SELECT o.bbl FROM ownership_raw o
            JOIN parcels p ON p.bbl = o.bbl
            WHERE o.doc_type = 'DEED' AND p.address IS NOT NULL
              AND p.assessed_total > 0 AND p.units_res > 1
            GROUP BY o.bbl HAVING count(*) > 1
            LIMIT 1
        """)).first()
        return r.bbl if r else None
    finally:
        db.close()


def test_headings_use_the_phrases_people_search():
    """Move 06 from the 2026-08-27 search read.

    "sales history", "taxes" and "owner" are live queries on this template
    ranking 12 to 24, and the page answered every one of them while calling its
    sections "Ownership transfers" and "The ownership chain". A heading pass
    that quietly reverts to house jargon takes the ranking with it.
    """
    bbl = _bbl_with_deeds_and_assessment()
    if not bbl:
        pytest.skip("no deeded, assessed, multi-unit parcel in current data")
    headings = " | ".join(re.findall(
        r"<h2[^>]*>(.*?)</h2>", client.get(f"/property/{bbl}").text))
    for phrase in ("Who owns", "Sales and ownership history",
                   "Taxes and assessed value"):
        assert phrase in headings, f"missing heading {phrase!r}; have: {headings}"

    # The other two sections only render where the building has those records,
    # so each is checked on a building that does.
    db = SessionLocal()
    try:
        conditional = {
            "Open code violations": """
                SELECT v.bbl FROM violations_raw v JOIN parcels p ON p.bbl = v.bbl
                WHERE v.current_status ILIKE 'OPEN%' AND p.address IS NOT NULL
                LIMIT 1""",
            "311 complaint history": """
                SELECT c.bbl FROM complaints_raw c JOIN parcels p ON p.bbl = c.bbl
                WHERE c.bbl IS NOT NULL AND p.address IS NOT NULL
                  AND c.created_date > CURRENT_DATE - INTERVAL '365 days'
                LIMIT 1""",
        }
        for phrase, sql in conditional.items():
            row = db.execute(text(sql)).first()
            if not row:
                continue
            body = client.get(f"/property/{row.bbl}").text
            assert phrase in body, f"missing heading {phrase!r} on {row.bbl}"
    finally:
        db.close()


def test_the_faq_names_the_signals_the_score_actually_reads():
    """It claimed "assessment spikes", which carries no weight and is NULL for
    every ZIP, and omitted HPD violations, which carries 0.08. A wrong list in
    FAQ schema is a wrong answer an assistant repeats."""
    from scoring.compute import (WEIGHT_COMPLAINTS, WEIGHT_EVICTIONS,
                                 WEIGHT_HPD_VIOLATIONS, WEIGHT_LLC_ACQUISITIONS,
                                 WEIGHT_PERMITS)
    bbl = _bbl_with_records()
    if not bbl:
        pytest.skip("no property with records in current data")
    body = client.get(f"/property/{bbl}").text
    if "displacement risk around" not in body:
        pytest.skip("this building's ZIP is untracked, so the answer is absent")
    assert "assessment spike" not in body.lower(), \
        "the FAQ names a signal the composite score does not read"
    # Every weighted signal has to appear in the sentence that lists them.
    assert all(w > 0 for w in (WEIGHT_LLC_ACQUISITIONS, WEIGHT_PERMITS,
                               WEIGHT_COMPLAINTS, WEIGHT_EVICTIONS,
                               WEIGHT_HPD_VIOLATIONS))
    for phrase in ("LLC acquisition rate", "permit intensity", "311 complaint volume",
                   "executed evictions", "HPD violations"):
        assert phrase in body, f"the score's signal list omits {phrase!r}"
