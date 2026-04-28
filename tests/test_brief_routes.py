"""
Route integrity and content tests for evidence brief pages.

/brief/zip/{zip}       — ZIP code displacement-pressure brief
/brief/operator/{slug} — Operator cluster brief

Static content tests run without a database.
Integration tests (marked) require a live DB.
"""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


class TestBriefRouteStructure:
    """Verify routes exist, bad params are rejected, and noise operators are blocked."""

    @pytest.fixture(scope="class")
    def client(self):
        from api.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

    def test_invalid_zip_returns_400(self, client):
        resp = client.get("/brief/zip/abc")
        assert resp.status_code == 400

    def test_invalid_zip_too_short(self, client):
        resp = client.get("/brief/zip/1234")
        assert resp.status_code == 400

    def test_invalid_operator_slug_returns_400(self, client):
        resp = client.get("/brief/operator/UPPERCASE_INVALID")
        assert resp.status_code == 400

    def test_noise_operator_returns_404(self, client):
        resp = client.get("/brief/operator/valley")
        assert resp.status_code == 404

    def test_noise_operator_icecap_returns_404(self, client):
        resp = client.get("/brief/operator/icecap")
        assert resp.status_code == 404

    def test_unknown_operator_slug_returns_404(self, client):
        resp = client.get("/brief/operator/zzz-does-not-exist")
        assert resp.status_code == 404

    def test_unknown_zip_returns_404(self, client):
        resp = client.get("/brief/zip/00000")
        assert resp.status_code == 404


@pytest.mark.integration
class TestZipBriefContent:
    """Live DB tests for the ZIP code evidence brief."""

    @pytest.fixture(scope="class")
    def client(self):
        from api.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

    @pytest.fixture(scope="class")
    def resp_11216(self, client):
        return client.get("/brief/zip/11216")

    def test_returns_200(self, resp_11216):
        assert resp_11216.status_code == 200

    def test_is_html(self, resp_11216):
        assert "text/html" in resp_11216.headers.get("content-type", "")

    def test_has_zip_in_title(self, resp_11216):
        assert "11216" in resp_11216.text

    def test_has_signal_breakdown_section(self, resp_11216):
        assert "Signal breakdown" in resp_11216.text

    def test_has_disclaimer(self, resp_11216):
        assert "not an allegation of wrongdoing" in resp_11216.text

    def test_has_methodology_link(self, resp_11216):
        assert "/methodology" in resp_11216.text

    def test_has_map_cta(self, resp_11216):
        assert "/map?q=11216" in resp_11216.text

    def test_has_neighborhood_link(self, resp_11216):
        assert "/neighborhood/11216" in resp_11216.text

    def test_has_sources_section(self, resp_11216):
        assert "Sources" in resp_11216.text

    def test_noindex_meta(self, resp_11216):
        assert 'content="noindex"' in resp_11216.text

    def test_has_copy_link_button(self, resp_11216):
        assert "copy-btn" in resp_11216.text


@pytest.mark.integration
class TestOperatorBriefContent:
    """Live DB tests for the operator evidence brief."""

    @pytest.fixture(scope="class")
    def client(self):
        from api.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

    @pytest.fixture(scope="class")
    def resp_bredif(self, client):
        return client.get("/brief/operator/bredif")

    @pytest.fixture(scope="class")
    def resp_phantom(self, client):
        return client.get("/brief/operator/phantom-capital")

    def test_bredif_returns_200(self, resp_bredif):
        assert resp_bredif.status_code == 200

    def test_phantom_returns_200(self, resp_phantom):
        assert resp_phantom.status_code == 200

    def test_has_disclaimer(self, resp_bredif):
        assert "not an allegation of wrongdoing" in resp_bredif.text

    def test_has_key_metrics_section(self, resp_bredif):
        assert "Key metrics" in resp_bredif.text

    def test_has_llc_entities_section(self, resp_bredif):
        assert "LLC entities" in resp_bredif.text

    def test_has_portfolio_section(self, resp_bredif):
        assert "Portfolio sample" in resp_bredif.text

    def test_has_acquisitions_section(self, resp_bredif):
        assert "Recent acquisitions" in resp_bredif.text

    def test_has_sources_section(self, resp_bredif):
        assert "Sources" in resp_bredif.text

    def test_has_methodology_link(self, resp_bredif):
        assert "/methodology" in resp_bredif.text

    def test_has_back_link_to_profile(self, resp_bredif):
        assert "/operator/bredif" in resp_bredif.text

    def test_noindex_meta(self, resp_bredif):
        assert 'content="noindex"' in resp_bredif.text

    def test_phantom_has_name_in_title(self, resp_phantom):
        assert "phantom" in resp_phantom.text.lower() or "PHANTOM" in resp_phantom.text
