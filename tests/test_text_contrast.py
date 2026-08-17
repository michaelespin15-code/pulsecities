"""
Guards the contrast of text against the page background.

The house palette was never the problem: #93a1ad on #111823 is 6.74:1 and every
token cleared AA on its own. What broke readability was the habit of reaching for
an alpha to de-emphasise something. Alpha on an already-muted grey compounds
twice, and the results were not close calls: the SSR nav shipped at 2.64:1, the
footer at 3.83:1, and `--muted` itself was defined as rgba(147,161,173,.65) in
api/routes/frontend.py — 3.58:1 for the token doing most of the body text on the
177 neighbourhood pages. The same token was a solid 6.74:1 in the static pages,
so the two surfaces had quietly disagreed for months.

That is shape 1 and shape 4 together: a rule written twice, and a de-emphasis
trick applied everywhere without anyone measuring the result. The fix was a
four-stop solid ramp; this test is what stops the alphas coming back.

WCAG AA is 4.5:1 for body text and 3:1 for large text and non-text. Everything
here is held to 4.5:1 because the offending text was small, not large.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
PAGE_BG = "#111823"
AA_BODY = 4.5

# The measured ramp. Every stop is solid on purpose.
RAMP = {
    "--text": "#e4e8ec",
    "--muted": "#93a1ad",
    "--dim": "#85929d",
    "--faint": "#78838d",
}

# Tokens that are not part of the grey ramp but are used for text.
OTHER_TOKENS = {
    "--sky": "#6fb1d8", "--orange": "#ed6317", "--pulse": "#ed6317",
    "--accent": "#ed6317", "--green": "#6fa287", "--amber": "#d9a441",
    "--red": "#ec6a5e", "--bg": "#111823", "--surface": "#1b2534",
    "--stamp": "#e4483b", "--border": "#93a1ad",
}

# Text painted on a coloured chip rather than the page, so the page background is
# the wrong comparison. Each is dark-on-accent and reads fine.
CHIP_TEXT = {"#111823", "#1a1a2e", "#92400e", "var(--bg)"}

# Surfaces a given file actually paints text on. A colour passes if it clears AA
# against any one of them, which is what "is this readable somewhere it is used"
# means. briefs.py is the reason this is not a single constant: it renders the
# dark web page AND a light printable brief from the same module, so measuring
# its ink against #111823 alone reports correct dark-on-white text as a failure.
BACKGROUNDS = {
    "briefs.py": (PAGE_BG, "#ffffff", "#f2f5f7", "#fff7ed"),
}
DEFAULT_BACKGROUNDS = (PAGE_BG, "#1b2534")

# Accepted below 4.5:1, each for a stated reason. Keep this list short and never
# add to it to make a test pass.
ACCEPTED = {
    # Canonical critical/stamp red at 4.49:1, one hundredth under the line and
    # pinned by test_frontend_routes palette guards. Changing it means changing
    # the tier palette on every surface in one commit, not editing it here.
    "#e4483b",
    "var(--stamp)",
}

HTML_FILES = sorted(
    p for p in (ROOT / "frontend").glob("*.html") if p.name != "ops.html"
)


def _rgb(hexs):
    h = hexs.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def _lum(r, g, b):
    def chan(c):
        c /= 255
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
    return 0.2126 * chan(r) + 0.7152 * chan(g) + 0.0722 * chan(b)


def contrast(fg, bg=PAGE_BG):
    """WCAG contrast ratio. fg may be a hex or an rgba() string."""
    bg_rgb = _rgb(bg)
    if fg.startswith("rgba"):
        nums = [float(x) for x in re.findall(r"[\d.]+", fg)]
        if len(nums) != 4:
            return None
        r, g, b, a = nums
        fg_rgb = tuple(a * c + (1 - a) * bc for c, bc in zip((r, g, b), bg_rgb))
    else:
        try:
            fg_rgb = _rgb(fg)
        except ValueError:
            return None
    hi, lo = sorted((_lum(*fg_rgb), _lum(*bg_rgb)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def _text_colours(src):
    """Every `color:` declaration, skipping background/border/fill shorthands."""
    return [
        m.group(1)
        for m in re.finditer(
            r"(?<!-)color:\s*(#[0-9a-fA-F]{3,6}|rgba\([^)]+\)|var\(--[a-z-]+\))", src
        )
    ]


def _resolve(value):
    if value.startswith("var("):
        name = value[4:-1].strip()
        return RAMP.get(name) or OTHER_TOKENS.get(name)
    return value


def _offenders(src, filename):
    """Declared text colours that clear AA on none of the file's backgrounds."""
    backgrounds = BACKGROUNDS.get(filename, DEFAULT_BACKGROUNDS)
    bad = {}
    for value in _text_colours(src):
        if value in CHIP_TEXT or value in ACCEPTED:
            continue
        resolved = _resolve(value)
        if resolved is None:
            continue
        ratios = [contrast(resolved, bg) for bg in backgrounds]
        ratios = [r for r in ratios if r is not None]
        if ratios and max(ratios) < AA_BODY:
            bad[value] = f"{max(ratios):.2f}:1"
    return dict(sorted(bad.items(), key=lambda kv: kv[1]))


class TestRampIsSolidAndMeasured:
    @pytest.mark.parametrize("token,hexv", sorted(RAMP.items()))
    def test_every_ramp_stop_clears_aa(self, token, hexv):
        cr = contrast(hexv)
        assert cr >= AA_BODY, f"{token} {hexv} is {cr:.2f}:1 on {PAGE_BG}"

    def test_ramp_stops_stay_distinguishable(self):
        """Four stops that all pass but look identical would be a pointless ramp."""
        ratios = sorted(contrast(h) for h in RAMP.values())
        gaps = [b - a for a, b in zip(ratios, ratios[1:])]
        assert min(gaps) >= 0.5, f"ramp stops too close together: {ratios}"


class TestNoAlphaTextInSource:
    """Alpha on a muted grey is the specific habit that caused this."""

    @pytest.mark.parametrize("path", HTML_FILES, ids=lambda p: p.name)
    def test_html_declares_no_low_contrast_text(self, path):
        bad = _offenders(path.read_text(), path.name)
        assert not bad, f"{path.name} has text under {AA_BODY}:1: {bad}"

    @pytest.mark.parametrize("module", ["frontend.py", "briefs.py"])
    def test_route_modules_declare_no_low_contrast_text(self, module):
        src = (ROOT / "api" / "routes" / module).read_text()
        bad = _offenders(src, module)
        assert not bad, f"{module} has text under {AA_BODY}:1: {bad}"


class TestTokensAgreeAcrossSurfaces:
    def test_muted_is_the_same_colour_everywhere(self):
        """It was #93a1ad on the static pages and rgba(...,.65) in the SSR ones.

        Same name, 6.74:1 against 3.58:1, and nothing compared them.
        """
        found = set()
        for path in HTML_FILES + [ROOT / "api" / "routes" / "frontend.py"]:
            for m in re.finditer(r"--muted:\s*([^;}\n]+)", path.read_text()):
                found.add(m.group(1).strip())
        assert len(found) == 1, f"--muted is defined {len(found)} ways: {found}"

    def test_faint_is_the_same_colour_everywhere(self):
        found = set()
        for path in HTML_FILES + [ROOT / "api" / "routes" / "frontend.py"]:
            for m in re.finditer(r"--faint:\s*([^;}\n]+)", path.read_text()):
                found.add(m.group(1).strip())
        assert len(found) <= 1, f"--faint is defined {len(found)} ways: {found}"


class TestMinimumFontSize:
    """Contrast was half the problem. The other half was 10px type.

    111 declarations sat under 12px, down to 9.9px on the neighbourhood pages,
    which are the surface a tenant most often reaches from a phone search. A
    12px floor is the least this can be and still be read on a handset.
    """

    MIN_PX = 12.0
    FILES = [ROOT / "api" / "routes" / "frontend.py",
             ROOT / "api" / "routes" / "briefs.py"] + HTML_FILES

    @pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
    def test_no_text_below_the_floor(self, path):
        src = path.read_text()
        small = [f"{v}px" for v in re.findall(r"font-size:\s*(\d+(?:\.\d+)?)px", src)
                 if float(v) < self.MIN_PX]
        small += [f"{v}rem" for v in re.findall(r"font-size:\s*(0?\.\d+)rem", src)
                  if float(v) * 16 < self.MIN_PX]
        assert not small, (
            f"{path.name} declares text under {self.MIN_PX}px: {sorted(set(small))}. "
            f"1rem is 16px here; 0.75rem is the floor."
        )


class TestKnownRegressions:
    """Named so a reviewer can see exactly what shipped broken before."""

    def test_ssr_nav_inactive_links_are_readable(self):
        src = (ROOT / "api" / "routes" / "frontend.py").read_text()
        i = src.index("def _ssr_nav")
        nav = src[i:src.index("\ndef ", i + 10)]
        assert "rgba(147,161,173,0.5)" not in nav, "nav inactive links back at 2.64:1"

    def test_ssr_footer_is_readable(self):
        src = (ROOT / "api" / "routes" / "frontend.py").read_text()
        start = src.index('_FOOTER_HTML = """<footer>')
        footer = src[start:src.index('</footer>"""', start)]
        assert "#677686" not in footer, "footer links back at 3.83:1"
        assert "font-size:11px" not in footer, "footer byline back under the 12px floor"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
