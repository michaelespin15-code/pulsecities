"""
Sitewide UI-copy guards.

House rule: an em dash never appears as a connector in user-facing copy; the
voice uses two sentences, a comma, or a middle dot instead. Earlier guards
checked single surfaces (digest, operator table) and greps looked for the
literal character, which let `\\u2014` escapes in JS strings ship unnoticed.
This scans every frontend page.

A lone quoted em dash ('\\u2014' or '—') is the missing-value placeholder
glyph and is allowed. Comments (HTML, CSS, JS) are not user-visible copy and
are ignored.
"""

import re
from pathlib import Path

import pytest

FRONTEND = Path(__file__).parent.parent / "frontend"

# Placeholder form: the em dash alone inside quotes/backticks.
_LONE_GLYPH = re.compile(r"""(['"`])(?:\\u2014|—)\1""")

# A regex character class like [—\-] parses copy rather than emitting it.
_CHAR_CLASS = re.compile(r"\[[^\]\n]*—[^\]\n]*\]")

_HTML_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

# ops.html is the token-gated internal dashboard; its bare — glyphs are
# loading placeholders between tags, not public copy.
_EXCLUDED = {"ops.html"}


def _strip_non_copy(text: str) -> str:
    text = _HTML_COMMENT.sub("", text)
    text = _BLOCK_COMMENT.sub("", text)
    # Whole-line JS comments. Inline trailing // is left alone: it can sit
    # inside a URL or string, and real copy never lives after one anyway.
    lines = []
    for line in text.splitlines():
        if line.lstrip().startswith(("//", "*")):
            continue
        lines.append(line)
    return "\n".join(lines)


def _pages():
    return sorted(p for p in FRONTEND.glob("*.html") if p.name not in _EXCLUDED)


class TestNoEmDashConnectors:
    def test_frontend_pages_exist(self):
        assert len(_pages()) > 5, f"expected frontend pages in {FRONTEND}"

    @pytest.mark.parametrize("page", _pages(), ids=lambda p: p.name)
    def test_no_em_dash_in_ui_copy(self, page):
        raw = page.read_text(encoding="utf-8", errors="replace")
        cleaned = _strip_non_copy(raw)
        cleaned = _LONE_GLYPH.sub("", cleaned)
        cleaned = _CHAR_CLASS.sub("", cleaned)

        offenders = []
        for lineno, line in enumerate(cleaned.splitlines(), start=1):
            if "—" in line or "\\u2014" in line or "&mdash;" in line:
                offenders.append(f"  ~line {lineno}: {line.strip()[:120]}")
        assert not offenders, (
            f"{page.name} has em-dash connectors in UI copy "
            f"(use two sentences, a comma, or ' · '):\n" + "\n".join(offenders)
        )
