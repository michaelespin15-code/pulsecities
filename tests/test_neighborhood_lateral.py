"""
Guard: the neighborhood page carries lateral internal links (#9) — operators
with deed activity in the ZIP, the borough's other neighborhoods by score, and a
/displacement CTA. Both list sections render only when they have rows; the CTA
always renders. Data-driven against the live dataset.
"""

import re
import warnings

import pytest
from sqlalchemy import text
from fastapi.testclient import TestClient

from api.main import app
from api.routes.neighborhoods import _borough_from_zip
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)


def _zip_with_operators():
    """A scored ZIP that has at least one non-noise operator holding parcels."""
    from api.routes.operators import OPERATOR_NOISE_ROOTS
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT p.zip_code, o.operator_root, count(DISTINCT p.bbl) AS n
            FROM operators o
            JOIN operator_parcels op ON op.operator_id = o.id
            JOIN parcels p ON p.bbl = op.bbl
            JOIN displacement_scores ds ON ds.zip_code = p.zip_code
            JOIN neighborhoods nb ON nb.zip_code = p.zip_code
            WHERE o.operator_class = 'operator'
              AND COALESCE(jsonb_array_length(o.llc_entities), 0) > 0
              AND p.zip_code IS NOT NULL AND nb.name IS NOT NULL
            GROUP BY p.zip_code, o.operator_root
            ORDER BY n DESC
            LIMIT 50
        """)).fetchall()
    finally:
        db.close()
    for r in rows:
        if r.operator_root not in OPERATOR_NOISE_ROOTS:
            return r.zip_code
    return None


def _nav_and_footer_stripped(html: str) -> str:
    """Body only, so nav/footer hub links don't pollute link-set assertions."""
    html = re.sub(r"<nav\b.*?</nav>", "", html, flags=re.S)
    html = re.sub(r"<footer\b.*?</footer>", "", html, flags=re.S)
    return html


def test_displacement_cta_present_everywhere():
    html = client.get("/neighborhood/11216").text
    assert 'class="disp-cta"' in html
    assert re.search(r'class="disp-cta"[^>]*>\s*<a href="/displacement"', html)


def test_operators_section_links_operator_pages():
    z = _zip_with_operators()
    if not z:
        pytest.skip("no scored ZIP with a non-noise operator in current data")
    body = _nav_and_footer_stripped(client.get(f"/neighborhood/{z}").text)
    assert "Operators active in" in body
    assert re.search(r'class="lat-row"><a href="/operator/[^"]+"', body), \
        f"{z} operators section has no /operator links"


def test_nearby_is_same_borough_and_excludes_self():
    z = "11216"  # Bedford-Stuyvesant, Brooklyn — dense, always has borough peers
    borough = _borough_from_zip(z)
    body = _nav_and_footer_stripped(client.get(f"/neighborhood/{z}").text)
    nearby = re.findall(r'class="lat-row"><a href="/neighborhood/(\d{5})"', body)
    assert nearby, "nearby section rendered no neighborhood links"
    assert z not in nearby, "nearby list must not link the current ZIP"
    for nz in nearby:
        assert _borough_from_zip(nz) == borough, f"{nz} is not in {borough}"


def test_nearby_ranked_by_score_desc():
    body = _nav_and_footer_stripped(client.get("/neighborhood/11216").text)
    scores = [int(s) for s in re.findall(r'class="lat-score"[^>]*>(\d+)<', body)]
    assert scores == sorted(scores, reverse=True), f"nearby not score-desc: {scores}"


def test_spanish_lateral_headers():
    html = client.get("/neighborhood/11216?lang=es").text
    assert "Más vecindarios de" in html
    assert "Ver el panorama de desplazamiento" in html
    # operators header is Spanish when the section renders
    if "Operadores activos en" not in html:
        pytest.skip("11216 has no operators section in current data")
