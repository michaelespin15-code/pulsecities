"""
Guard: operator profile pages carry structured data and link out to the money
pages. Previously they emitted zero JSON-LD and rendered addresses as plain text.
"""

import warnings

from fastapi.testclient import TestClient

from api.main import app

warnings.filterwarnings("ignore")
client = TestClient(app)

# mtek-nyc is a confirmed operator with acquisitions in the production dataset.
OP = "/operator/mtek-nyc"


def test_operator_has_dataset_and_breadcrumb_schema():
    body = client.get(OP).text
    assert '"@type": "Dataset"' in body, "operator page must emit a Dataset schema"
    assert '"@type": "BreadcrumbList"' in body, "operator page must emit a breadcrumb"


def test_operator_links_out_to_property_and_neighborhood():
    body = client.get(OP).text
    assert 'href="/property/' in body, "acquisition addresses must link to /property"
    assert 'href="/neighborhood/' in body, "acquisition ZIPs must link to /neighborhood"
