"""The map and chart libraries must be served from our own origin.

They used to load from unpkg and jsdelivr, which made a third-party outage an
outage of the core feature. app.html has a fallback that keeps the page alive
without MapLibre, but a working fallback is not a reason to depend on someone
else's uptime for the main thing the site does.

Files live in frontend/vendor/ with provenance recorded in its README.
"""

from pathlib import Path

import pytest

_ROOT = Path(__file__).parent.parent
_VENDOR = _ROOT / "frontend" / "vendor"

# Anything that fetches executable code from a host we don't control.
_CDN_HOSTS = ("unpkg.com", "cdn.jsdelivr.net", "cdnjs.cloudflare.com")

_REQUIRED = ("maplibre-gl.js", "maplibre-gl.css", "chart.umd.min.js")


@pytest.mark.parametrize("name", _REQUIRED)
def test_vendored_file_present_and_not_a_stub(name):
    path = _VENDOR / name
    assert path.exists(), f"frontend/vendor/{name} is missing; see vendor/README.md"
    # A failed download writes an HTML error page, which would 200 from nginx
    # and break the map with no obvious cause.
    assert path.stat().st_size > 10_000, f"frontend/vendor/{name} is too small to be the real library"
    assert not path.read_bytes().lstrip().startswith(b"<"), (
        f"frontend/vendor/{name} looks like an HTML error page, not the library"
    )


def test_no_page_loads_scripts_from_a_cdn():
    offenders = []
    for html in (_ROOT / "frontend").glob("*.html"):
        text = html.read_text()
        for host in _CDN_HOSTS:
            if host in text:
                offenders.append(f"{html.name} loads from {host}")
    assert not offenders, (
        "front-end assets must be served from this origin: " + "; ".join(offenders)
    )


def test_vendor_readme_records_provenance():
    readme = _VENDOR / "README.md"
    assert readme.exists(), "frontend/vendor/README.md is missing"
    text = readme.read_text()
    for name in _REQUIRED:
        assert name in text, f"{name} has no upstream recorded in vendor/README.md"
