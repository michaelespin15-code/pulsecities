"""
Guard: the standing SSR pages emit a BreadcrumbList so they have an explicit
place in the site hierarchy (eligible for breadcrumb rich results).
"""

import json
import re
import warnings

from fastapi.testclient import TestClient

from api.main import app

warnings.filterwarnings("ignore")
client = TestClient(app)

# Pages that should carry a BreadcrumbList somewhere in their JSON-LD.
PAGES = [
    "/displacement",
    "/operators",
    "/neighborhoods",
    "/flips",
    "/flips/editions",
    "/radar",
    "/this-week",
]


def _all_types(html: str) -> set:
    types = set()
    for block in re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.S):
        obj = json.loads(block)
        nodes = obj.get("@graph", [obj])
        for n in nodes:
            if isinstance(n, dict) and n.get("@type"):
                types.add(n["@type"])
    return types


def test_standing_pages_have_breadcrumbs():
    missing = []
    for path in PAGES:
        r = client.get(path)
        if r.status_code != 200:
            continue
        if "BreadcrumbList" not in _all_types(r.text):
            missing.append(path)
    assert not missing, f"pages missing BreadcrumbList: {missing}"
