"""
Guards for the self-hosted fonts.

The faces are the same three they always were. What changed on 2026-08-18 is
that they come from this origin instead of Google's, which took two DNS+TLS
handshakes and a stylesheet round trip out of the critical path: the homepage
LCP element is the H1, and it was re-painting when Bricolage finally swapped in
at 3.2s.

Two ways that regresses quietly. A page gets copied from an older one and
brings a fonts.googleapis.com link back, which reintroduces the handshakes for
that page only. Or a font file is renamed or removed and every face silently
falls back to the system stack, which looks fine to whoever made the change and
wrong to everyone else.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
FONT_DIR = ROOT / "frontend" / "fonts"
SOURCES = sorted(
    [p for p in (ROOT / "frontend").glob("*.html")]
    + [ROOT / "api" / "routes" / "frontend.py", ROOT / "api" / "routes" / "briefs.py"]
)
FONT_URL = re.compile(r"/fonts/([A-Za-z0-9._-]+\.woff2)")
FAMILY = re.compile(r"font-family:\s*'([^']+)'")


class TestSelfHosted:
    @pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
    def test_no_google_fonts_reference(self, path):
        src = path.read_text()
        for host in ("fonts.googleapis.com", "fonts.gstatic.com"):
            assert host not in src, (
                f"{path.name} still loads fonts from {host}; the faces are "
                f"self-hosted in frontend/fonts and referenced as /fonts/*.woff2"
            )

    @pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
    def test_every_referenced_file_exists(self, path):
        missing = [f for f in set(FONT_URL.findall(path.read_text()))
                   if not (FONT_DIR / f).is_file()]
        assert not missing, f"{path.name} references missing font files: {missing}"

    def test_font_files_are_woff2_and_world_readable(self):
        files = sorted(FONT_DIR.glob("*.woff2"))
        assert files, "no font files in frontend/fonts"
        for f in files:
            assert f.read_bytes()[:4] == b"wOF2", f"{f.name} is not woff2"
            assert f.stat().st_mode & 0o044, f"{f.name} is not world-readable; nginx 403s"

    def test_nothing_ships_an_unused_font(self):
        """Every file in the directory is referenced by something. A 41KB font
        nobody links is 41KB in the backup and the deploy for nothing."""
        referenced = set()
        for p in SOURCES:
            referenced |= set(FONT_URL.findall(p.read_text()))
        orphans = [f.name for f in FONT_DIR.glob("*.woff2") if f.name not in referenced]
        assert not orphans, f"font files nothing references: {orphans}"


class TestFaceDeclarations:
    @pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
    def test_declared_faces_use_swap(self, path):
        """Without font-display:swap the text is invisible until the font
        arrives, which trades a re-paint for a blank page."""
        for block in re.findall(r"@font-face\{\{?(.*?)\}\}?", path.read_text()):
            if "/fonts/" not in block:
                continue
            assert "font-display:swap" in block, (
                f"{path.name} declares a face without swap: {FAMILY.search(block)}"
            )

    def test_homepage_preloads_the_lcp_face(self):
        """The homepage LCP element is the H1, which is Bricolage. Discovering
        that font from the inline @font-face rather than a preload put the swap
        after everything else the page asks for."""
        head = (ROOT / "frontend" / "index.html").read_text().split("</head>")[0]
        assert 'rel="preload"' in head and "bricolage-grotesque" in head, (
            "the homepage no longer preloads its display face"
        )
        assert head.count('as="font"') <= 3, (
            "preloading more than a few fonts starves the rest of the page"
        )
