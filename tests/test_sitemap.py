"""
Guard: the sitemap lists only substantive property pages, never the ~912k thin
parcels that merely inherit a ZIP score. A regression that widened the property
query would flood the index with doorway pages; this catches it.
"""

import re
import warnings

import pytest
from fastapi.testclient import TestClient

from api.main import app
from scripts.generate_sitemap import build

warnings.filterwarnings("ignore")
client = TestClient(app)

_XML = build()
_PROPERTY_LOCS = re.findall(r"<loc>https://pulsecities\.com(/property/\d+)</loc>", _XML)
_TOTAL = _XML.count("<url>")

# Sitemaps cap at 50k URLs; property pages must stay a bounded, curated subset
# well under that, not an en-masse dump of every parcel.
_FLOOD_CAP = 10_000


def test_sitemap_has_property_pages():
    assert _PROPERTY_LOCS, "no /property pages in sitemap"


def test_property_count_is_bounded():
    assert len(_PROPERTY_LOCS) < _FLOOD_CAP, \
        f"{len(_PROPERTY_LOCS)} property URLs — sitemap is flooding (doorway risk)"
    assert _TOTAL < 50_000, f"{_TOTAL} URLs exceeds the sitemap limit"


def test_property_entries_are_low_priority():
    # Each property <url> block must carry priority 0.5 (secondary to hub pages).
    for m in re.finditer(r"<url>\s*<loc>https://pulsecities\.com/property/\d+</loc>.*?</url>",
                         _XML, re.S):
        assert "<priority>0.5</priority>" in m.group(0)


def test_sitemapped_properties_are_indexable():
    # Sample a handful and confirm each is 200 + index,follow — never noindex.
    sample = _PROPERTY_LOCS[:: max(1, len(_PROPERTY_LOCS) // 5)][:5]
    assert sample
    for path in sample:
        resp = client.get(path)
        assert resp.status_code == 200, f"{path} -> {resp.status_code}"
        robots = re.search(r'<meta name="robots" content="([^"]+)"', resp.text)
        assert robots and "noindex" not in robots.group(1), \
            f"{path} is {robots.group(1) if robots else 'missing robots'} but is in the sitemap"
