"""
Server-side rendered pages for neighborhood and operator deep links, plus the
operators directory.

GET /neighborhood/{zip_code}  — full SSR civic intelligence card (score, signals, FAQ, CTA)
GET /operator/{root}          — per-operator OG/meta injection into operator.html
GET /operators                — server-side rendered directory of all tracked operators
"""

import html as _html
import json
import logging
import re
import time
from datetime import date, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse, HTMLResponse, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["frontend"])

_FRONTEND = Path(__file__).parent.parent.parent / "frontend"
_app_html: str | None = None
_operator_html: str | None = None
_page_cache: dict[str, tuple[str, float]] = {}   # zip -> (html, expires_at)
_op_page_cache: dict[str, tuple[str, float]] = {}  # root -> (html, expires_at)
_prop_page_cache: dict[str, tuple[str, float]] = {}  # bbl -> (html, expires_at)
_PAGE_TTL = 3600


def _template() -> str:
    global _app_html
    if _app_html is None:
        _app_html = (_FRONTEND / "app.html").read_text()
    return _app_html


def _operator_template() -> str:
    global _operator_html
    if _operator_html is None:
        _operator_html = (_FRONTEND / "operator.html").read_text()
    return _operator_html


_not_found_html: str | None = None


def _not_found() -> HTMLResponse:
    """Real 404 for unknown neighborhood/property paths. Serving the app
    shell with a 200 here reads as a soft 404 to crawlers."""
    global _not_found_html
    if _not_found_html is None:
        _not_found_html = (_FRONTEND / "404.html").read_text()
    return HTMLResponse(_not_found_html, status_code=404)


def _set_meta(html: str, attr: str, attr_val: str, new_content: str) -> str:
    """Replace content="..." on the meta tag identified by attr=attr_val.

    Matches by attribute name and value rather than by the current content
    string, so the replacement survives changes to the default meta values
    in app.html without needing to be updated here.
    """
    pattern = rf'<meta\b[^>]*\b{re.escape(attr)}="{re.escape(attr_val)}"[^>]*>'

    def _swap(m: re.Match) -> str:
        return re.sub(r'content="[^"]*"', f'content="{new_content}"', m.group(0), count=1)

    return re.sub(pattern, _swap, html)


# ---------------------------------------------------------------------------
# Neighborhood page \u2014 SSR civic intelligence card
# ---------------------------------------------------------------------------

_FAQ_Q1 = "What does this displacement score mean?"
_FAQ_A1 = (
    "The score is a 0 to 100 index showing where multiple public-record displacement signals "
    "are elevated at the ZIP level. Each signal is normalized across all 178 NYC ZIP codes so "
    "dense areas are not scored by raw counts alone."
)
_FAQ_Q2 = "What public records are included?"
_FAQ_A2 = (
    "PulseCities uses NYC public records: DOB building permits, HPD housing violations, "
    "311 housing complaints, eviction filings, ACRIS property deed transfers, DHCR "
    "rent-stabilized housing data, and MapPLUTO residential unit counts."
)
_FAQ_Q3 = "Is this a prediction of eviction?"
_FAQ_A3 = (
    "No. PulseCities does not predict individual evictions and is not legal advice. "
    "The score shows neighborhood-level public-record indicators that may be worth reviewing."
)



def _jsonld(obj) -> str:
    """JSON for a <script type="application/ld+json"> block. Escapes < so a
    public-record string containing </script> can't break out of the element."""
    return json.dumps(obj, indent=2).replace("<", "\\u003c")


def _tier_info(score: float) -> tuple[str, str]:
    """
    Returns (display_label, hex_color) for the score tier.
    Bands must match the map legend, weekly digest, and _build_summary:
    Low 0-33, Moderate 34-66, High 67-84, Critical 85+.
    """
    if score >= 85: return "Critical", "#ef4444"
    if score >= 67: return "High",     "#f97316"
    if score >= 34: return "Moderate", "#eab308"
    return "Low", "#64748b"


def _idx_color(v: float) -> str:
    if v >= 70: return "#f97316"
    if v >= 45: return "#eab308"
    return "rgba(148,163,184,0.55)"


def _trend_svg(history: list[tuple[str, float]]) -> str:
    """
    Inline SVG of the daily composite score. Server-rendered so the page
    stays static and the trace shows up in reader modes and print.
    Returns "" when there is not enough history to draw an honest line.
    """
    if not history or len(history) < 7:
        return ""

    scores = [s for _, s in history]
    lo = max(0.0, min(scores) - 2.0)
    hi = min(100.0, max(scores) + 2.0)
    rng = (hi - lo) or 1.0

    w, h = 640.0, 150.0
    px_l, px_r, py_t, py_b = 6.0, 6.0, 18.0, 26.0
    plot_w, plot_h = w - px_l - px_r, h - py_t - py_b

    n = len(scores)
    pts = []
    for i, s in enumerate(scores):
        x = px_l + (i / (n - 1)) * plot_w
        y = py_t + (1 - (s - lo) / rng) * plot_h
        pts.append(f"{x:.1f},{y:.1f}")
    line = " ".join(pts)
    area = f"{px_l:.1f},{py_t + plot_h:.1f} {line} {px_l + plot_w:.1f},{py_t + plot_h:.1f}"
    last_x, last_y = pts[-1].split(",")

    grid = ""
    for frac, val in ((0.0, hi), (0.5, lo + rng / 2), (1.0, lo)):
        gy = py_t + frac * plot_h
        grid += (
            f'<line x1="{px_l}" y1="{gy:.1f}" x2="{px_l + plot_w}" y2="{gy:.1f}" '
            f'stroke="rgba(148,163,184,.12)" stroke-width="1"/>'
            f'<text x="{px_l + 2}" y="{gy - 4:.1f}" font-size="10" '
            f'font-family="JetBrains Mono,monospace" fill="rgba(148,163,184,.45)">{val:.0f}</text>'
        )

    def _md(iso: str) -> str:
        try:
            return date.fromisoformat(iso).strftime("%b %-d")
        except ValueError:
            return iso

    first_lbl, last_lbl = _md(history[0][0]), _md(history[-1][0])

    return (
        f'<svg viewBox="0 0 {w:.0f} {h:.0f}" role="img" '
        f'aria-label="Daily displacement score from {first_lbl} to {last_lbl}" '
        f'style="width:100%;height:auto;display:block;">'
        f'{grid}'
        f'<polygon points="{area}" fill="rgba(249,115,22,.07)"/>'
        f'<polyline points="{line}" fill="none" stroke="#f97316" stroke-width="1.6" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'<circle cx="{last_x}" cy="{last_y}" r="3" fill="#f97316"/>'
        f'<text x="{px_l}" y="{h - 6:.0f}" font-size="10" font-family="JetBrains Mono,monospace" '
        f'fill="rgba(148,163,184,.45)">{first_lbl}</text>'
        f'<text x="{px_l + plot_w:.1f}" y="{h - 6:.0f}" font-size="10" text-anchor="end" '
        f'font-family="JetBrains Mono,monospace" fill="rgba(148,163,184,.45)">{last_lbl}</text>'
        f'</svg>'
    )


def _build_neighborhood_page(
    zip_code: str,
    name: str,
    borough: str | None,
    score: float | None,
    breakdown: dict,
    raw_counts: dict,
    raw_hpd: int,
    summary: str | None,
    last_updated: str | None,
    history: list[tuple[str, float]] | None = None,
) -> str:
    e = _html.escape

    borough_disp = borough or "New York City"
    canonical    = f"https://pulsecities.com/neighborhood/{zip_code}"
    og_image     = f"https://pulsecities.com/og/{zip_code}.png"

    if last_updated:
        try:
            updated_disp = date.fromisoformat(last_updated).strftime("%B %-d, %Y")
        except ValueError:
            updated_disp = last_updated
    else:
        updated_disp = "recently"

    if score is not None:
        tier_label, tier_color = _tier_info(score)
        score_str    = f"{score:.1f}"
        page_title   = f"{name} ({zip_code}), {borough_disp} | Displacement Score {score_str}/100 | PulseCities"
        social_title = f"{name} ({zip_code}) | Displacement Score {score_str}/100 | PulseCities"
        meta_desc    = (
            f"{name} shows {tier_label.lower()} displacement-pressure signals based on NYC public records, "
            f"including LLC acquisitions, eviction filings, 311 complaints, HPD violations, "
            f"permits, and rent-stabilized housing data."
        )
    else:
        tier_label, tier_color = "Unknown", "#64748b"
        score_str    = "N/A"
        page_title   = f"{name} ({zip_code}), {borough_disp} | NYC Displacement Signals | PulseCities"
        social_title = page_title
        meta_desc    = (
            f"Track displacement-pressure signals in {name} ({zip_code}) from NYC public records: LLC acquisitions, "
            f"eviction filings, 311 complaints, HPD violations, permits, and rent-stabilized housing data."
        )

    # (breakdown_key, display_label, window_label, raw_count or None for dormant signals)
    _signals = [
        ("llc_acquisitions", "LLC property acquisitions",               "past 365 days",     raw_counts.get("llc_acquisitions", 0)),
        ("permits",          "Building permits (residential, 3+ units)", "past 365 days",     raw_counts.get("permits", 0)),
        ("evictions",        "Residential eviction filings",             "past 365 days",     raw_counts.get("evictions", 0)),
        ("hpd_violations",   "HPD violations (Class B+C)",               "past 90 days",      raw_hpd),
        ("complaint_rate",   "311 housing complaints",                   "past 365 days",     raw_counts.get("complaint_rate", 0)),
        ("rs_unit_loss",     "Rent-stabilized unit loss",                "annual comparison", None),
    ]

    rows_html = ""
    for key, label, window, count in _signals:
        idx   = breakdown.get(key)
        idx_s = f"{idx:.1f}" if idx is not None else "&mdash;"
        i_col = _idx_color(float(idx) if idx is not None else 0.0)
        if key == "rs_unit_loss":
            cnt_s = "No annual loss recorded in current data"
            c_col = "rgba(148,163,184,0.4)"
        elif count == 0:
            cnt_s = "0"
            c_col = "rgba(148,163,184,0.4)"
        else:
            cnt_s = f"{count:,}"
            c_col = "#f1f5f9"
        rows_html += (
            f'<tr>'
            f'<td class="sc">{e(label)}<span class="sw">{e(window)}</span></td>'
            f'<td class="sr" style="color:{c_col};">{cnt_s}</td>'
            f'<td class="si" style="color:{i_col};">{idx_s}</td>'
            f'</tr>'
        )

    score_block = (
        f'<div class="score-block">'
        f'<span class="score-num" style="color:{tier_color};">{score_str}</span>'
        f'<span class="score-denom">/100</span>'
        f'<span class="score-tier" style="color:{tier_color};">{tier_label.upper()} DISPLACEMENT PRESSURE</span>'
        f'</div>'
        if score is not None
        else '<div class="score-block"><p style="color:rgba(148,163,184,0.5);font-size:0.9rem;">Score data not yet available.</p></div>'
    )
    summary_html = f'<p class="summary">{e(summary)}</p>' if summary else ""

    trend_section = ""
    svg = _trend_svg(history or [])
    if svg:
        n_days = len(history)
        delta = history[-1][1] - history[0][1]
        delta_s = f"{delta:+.1f}" if abs(delta) >= 0.05 else "flat"
        trend_section = (
            f'<section style="margin-bottom:32px;">'
            f'<h2>Score trend</h2>'
            f'<p class="section-sub">Daily composite score, past {n_days} days. '
            f'Change over the window: <span style="font-family:\'JetBrains Mono\',monospace;color:#f1f5f9;">{delta_s}</span></p>'
            f'<div style="border:1px solid var(--border);border-radius:8px;padding:14px 12px 8px;background:rgba(255,255,255,.02);">{svg}</div>'
            f'</section>'
        )

    dataset_ld = _jsonld({
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"Displacement Signals -- {name} ({zip_code}), {borough_disp}, NYC",
        "description": meta_desc,
        "url": canonical,
        "spatialCoverage": {
            "@type": "Place",
            "name": f"{name}, New York City",
            "address": {
                "@type": "PostalAddress",
                "postalCode": zip_code,
                "addressRegion": "NY",
                "addressCountry": "US",
            },
        },
        "creator": {"@type": "Person", "name": "Michael Espin", "url": "https://pulsecities.com"},
        "license": "https://creativecommons.org/licenses/by/4.0/",
        "isBasedOn": [
            "https://data.cityofnewyork.us/City-Government/ACRIS-Real-Property-Master/bnx9-e6tj",
            "https://data.cityofnewyork.us/Housing-Development/Evictions/6z8x-wfk4",
            "https://data.cityofnewyork.us/Housing-Development/Building-Permits/ipu4-2q9a",
        ],
        **({"dateModified": last_updated} if last_updated else {}),
        **({"variableMeasured": {
            "@type": "PropertyValue",
            "name": "Displacement Risk Score",
            "value": round(score, 1),
            "minValue": 0,
            "maxValue": 100,
        }} if score is not None else {}),
    })

    faq_ld = _jsonld({
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {"@type": "Question", "name": _FAQ_Q1, "acceptedAnswer": {"@type": "Answer", "text": _FAQ_A1}},
            {"@type": "Question", "name": _FAQ_Q2, "acceptedAnswer": {"@type": "Answer", "text": _FAQ_A2}},
            {"@type": "Question", "name": _FAQ_Q3, "acceptedAnswer": {"@type": "Answer", "text": _FAQ_A3}},
        ],
    })

    embed_code = (
        f'<a href="https://pulsecities.com/neighborhood/{zip_code}">'
        f'<img src="https://pulsecities.com/badge/{zip_code}.svg" '
        f'alt="PulseCities displacement score for {name} ({zip_code})" '
        f'width="320" height="64"></a>'
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="robots" content="index, follow">
<title>{e(page_title)}</title>
<meta name="description" content="{e(meta_desc)}">
<link rel="canonical" href="{e(canonical)}">
<meta property="og:title" content="{e(social_title)}">
<meta property="og:description" content="{e(meta_desc)}">
<meta property="og:url" content="{e(canonical)}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="{e(og_image)}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{e(social_title)}">
<meta name="twitter:description" content="{e(meta_desc)}">
<meta name="twitter:image" content="{e(og_image)}">
<script type="application/ld+json">{dataset_ld}</script>
<script type="application/ld+json">{faq_ld}</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600&family=JetBrains+Mono:wght@400&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600&family=JetBrains+Mono:wght@400&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{--bg:#0f172a;--border:rgba(148,163,184,.1);--text:#f1f5f9;--muted:rgba(148,163,184,.65);--faint:rgba(148,163,184,.35);--accent:#f97316}}
body{{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;line-height:1.6;overflow-x:hidden}}
a{{color:inherit;text-decoration:none}}
nav{{border-bottom:1px solid var(--border);padding:12px 0}}
.nav-inner{{max-width:720px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}}
.nav-links a{{font-size:.78rem;color:var(--muted);margin-left:16px;transition:color .15s}}
.nav-links a:hover{{color:var(--text)}}
.container{{max-width:720px;margin:0 auto;padding:32px 20px 80px}}
.breadcrumb{{font-size:.78rem;color:var(--muted);margin-bottom:20px}}
.breadcrumb a{{color:var(--muted)}}
.breadcrumb a:hover{{color:var(--text)}}
h1{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.45rem;font-weight:600;line-height:1.3;margin-bottom:6px}}
.subline{{font-size:.82rem;color:var(--muted);margin-bottom:28px}}
.score-block{{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap;padding:20px 24px;background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:8px;margin-bottom:20px}}
.score-num{{font-size:2.8rem;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1}}
.score-denom{{font-size:1rem;color:var(--muted);font-family:'JetBrains Mono',monospace;align-self:flex-end;padding-bottom:4px}}
.score-tier{{font-size:.68rem;font-weight:600;letter-spacing:.08em;align-self:flex-end;padding-bottom:6px;margin-left:8px}}
.summary{{font-size:.92rem;color:var(--muted);line-height:1.7;margin-bottom:32px}}
h2{{font-size:.68rem;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--faint);margin-bottom:8px}}
.section-sub{{font-size:.82rem;color:var(--muted);margin-bottom:14px}}
table{{width:100%;border-collapse:collapse;margin-bottom:12px}}
th{{font-size:.64rem;font-weight:500;text-transform:uppercase;letter-spacing:.06em;color:var(--faint);padding:6px 0;border-bottom:1px solid var(--border)}}
th:not(:first-child){{text-align:right}}
td{{padding:12px 0;border-bottom:1px solid rgba(148,163,184,.06);vertical-align:top}}
.sc{{font-size:.87rem}}
.sw{{display:block;font-size:.71rem;color:var(--faint);margin-top:2px}}
.sr,.si{{font-size:.87rem;font-family:'JetBrains Mono',monospace;text-align:right;white-space:nowrap}}
.data-note{{font-size:.74rem;color:var(--faint);margin-top:10px;margin-bottom:36px;line-height:1.55}}
.faq-list{{margin-bottom:36px}}
.faq-item{{padding:16px 0;border-bottom:1px solid var(--border)}}
.faq-item:first-child{{border-top:1px solid var(--border)}}
.faq-q{{font-size:.88rem;font-weight:600;margin-bottom:6px}}
.faq-a{{font-size:.83rem;color:var(--muted);line-height:1.65}}
.meth-link{{font-size:.82rem;margin-bottom:28px}}
.meth-link a{{color:var(--accent)}}
.meth-link a:hover{{text-decoration:underline}}
.cta-row{{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-top:4px}}
.btn-map{{display:inline-flex;align-items:center;gap:6px;padding:10px 20px;background:var(--accent);color:#fff;border-radius:6px;font-size:.85rem;font-weight:500;transition:opacity .15s}}
.btn-map:hover{{opacity:.88}}
.btn-copy{{display:inline-flex;align-items:center;gap:6px;padding:10px 20px;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:.85rem;cursor:pointer;font-family:inherit;transition:color .15s,border-color .15s}}
.btn-copy:hover{{color:var(--text);border-color:rgba(148,163,184,.3)}}
footer{{border-top:1px solid var(--border);padding:24px 20px calc(env(safe-area-inset-bottom) + 24px);text-align:center}}
.footer-links{{max-width:720px;margin:0 auto;display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
.footer-links a{{font-size:.75rem;color:var(--faint)}}
.footer-links a:hover{{color:var(--muted)}}
@media(max-width:600px){{h1{{font-size:1.2rem}}.score-num{{font-size:2.2rem}}.container{{padding:24px 16px 60px}}.cta-row{{flex-direction:column;align-items:flex-start}}}}
</style>
</head>
<body>
<nav><div class="nav-inner">
  <a href="/" style="display:flex;align-items:center;gap:8px;">
    <svg width="20" height="20" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
    <span style="font-size:.85rem;color:rgba(148,163,184,.55);">PulseCities</span>
  </a>
  <div class="nav-links"><a href="/map">Map</a><a href="/methodology">Methodology</a><a href="/about">About</a></div>
</div></nav>
<main><div class="container">
  <p class="breadcrumb"><a href="/map">&#8592; Back to map</a></p>
  <h1>Displacement Signals | {e(name)} ({zip_code})</h1>
  <p class="subline">{e(borough_disp)}. Updated {e(updated_disp)}.</p>
  {score_block}
  {summary_html}
  {trend_section}
  <section style="margin-bottom:32px;">
    <h2>Signal breakdown</h2>
    <p class="section-sub">Public-record signals used in the neighborhood score.</p>
    <table>
      <thead><tr><th>Signal</th><th>Count</th><th>Index</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    <p class="data-note">All counts from NYC public records. Index values are normalized across 178 NYC ZIP codes. Data is refreshed nightly.</p>
  </section>
  <section style="margin-bottom:32px;">
    <h2>About this data</h2>
    <div class="faq-list">
      <div class="faq-item"><p class="faq-q">{e(_FAQ_Q1)}</p><p class="faq-a">{e(_FAQ_A1)}</p></div>
      <div class="faq-item"><p class="faq-q">{e(_FAQ_Q2)}</p><p class="faq-a">{e(_FAQ_A2)}</p></div>
      <div class="faq-item"><p class="faq-q">{e(_FAQ_Q3)}</p><p class="faq-a">{e(_FAQ_A3)}</p></div>
    </div>
  </section>
  <section style="margin-bottom:32px;">
    <h2>Embed this score</h2>
    <p class="section-sub">A live badge for articles and community pages. It stays current as the score changes and links back to this page.</p>
    <p style="margin-bottom:12px;"><img src="/badge/{zip_code}.svg" alt="PulseCities displacement score badge for {e(name)} ({zip_code})" width="320" height="64" style="display:block;"></p>
    <textarea id="embed-code" readonly rows="3" aria-label="Embed code" style="width:100%;max-width:560px;background:var(--surface);color:var(--muted);border:1px solid var(--border);border-radius:8px;padding:10px 12px;font-size:12px;font-family:SFMono-Regular,Menlo,Consolas,monospace;line-height:1.5;resize:none;">{e(embed_code)}</textarea>
    <p style="margin-top:8px;"><button class="btn-copy" id="copy-embed-btn" onclick="copyEmbed()">Copy embed code</button></p>
  </section>
  <p class="meth-link"><a href="/methodology">Read the methodology &#8594;</a></p>
  <div class="cta-row">
    <a href="/map?q={zip_code}" class="btn-map">Open {zip_code} on the map &#8594;</a>
    <button class="btn-copy" id="copy-btn" onclick="copyLink()">Copy link</button>
    <a href="/brief/zip/{zip_code}" class="btn-copy">Evidence brief</a>
  </div>
</div></main>
<footer><div style="font-size:11px;color:var(--faint);margin-bottom:8px;text-align:center;"><a href="https://www.linkedin.com/in/michaelespin/" target="_blank" rel="noopener noreferrer" style="color:var(--faint);text-decoration:none;">Built by Michael Espin</a></div><div class="footer-links">
  <a href="/">Home</a><a href="/methodology">Methodology</a><a href="/about">About</a><a href="/status">Status</a><a href="mailto:nycdisplacement@gmail.com">Contact</a><a href="https://x.com/PulseCities" target="_blank" rel="noopener noreferrer" aria-label="PulseCities on X" style="display:inline-flex;align-items:center;"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" focusable="false"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg></a>
</div></footer>
<script>
function copyLink() {{
  var url = 'https://pulsecities.com/neighborhood/{zip_code}';
  var btn = document.getElementById('copy-btn');
  function onDone() {{
    btn.textContent = 'Copied!';
    setTimeout(function() {{ btn.textContent = 'Copy link'; }}, 2000);
  }}
  if (navigator.clipboard && navigator.clipboard.writeText) {{
    navigator.clipboard.writeText(url).then(onDone).catch(function() {{ fallback(url, onDone); }});
  }} else {{
    fallback(url, onDone);
  }}
}}
function copyEmbed() {{
  var ta = document.getElementById('embed-code');
  var btn = document.getElementById('copy-embed-btn');
  function onDone() {{
    btn.textContent = 'Copied!';
    setTimeout(function() {{ btn.textContent = 'Copy embed code'; }}, 2000);
  }}
  ta.select();
  if (navigator.clipboard && navigator.clipboard.writeText) {{
    navigator.clipboard.writeText(ta.value).then(onDone).catch(function() {{ document.execCommand('copy'); onDone(); }});
  }} else {{
    document.execCommand('copy');
    onDone();
  }}
}}
function fallback(url, onDone) {{
  var el = document.createElement('textarea');
  el.value = url;
  el.style.cssText = 'position:fixed;opacity:0';
  document.body.appendChild(el);
  el.select();
  el.setSelectionRange(0, 99999);
  try {{ document.execCommand('copy'); onDone(); }} catch(err) {{}}
  document.body.removeChild(el);
}}
</script>
</body>
</html>"""


@router.get("/map", include_in_schema=False)
def map_page():
    return FileResponse(_FRONTEND / "app.html")


@router.get("/methodology", include_in_schema=False)
def methodology_page():
    return FileResponse(_FRONTEND / "methodology.html")


@router.get("/about", include_in_schema=False)
def about_page():
    return FileResponse(_FRONTEND / "about.html")


@router.get("/neighborhood/{zip_code}", include_in_schema=False)
def neighborhood_page(zip_code: str, db: Session = Depends(get_db)):
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        return _not_found()

    cached = _page_cache.get(zip_code)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    row = db.execute(text("""
        SELECT n.name, ds.score, ds.signal_breakdown, ds.cache_generated_at
        FROM neighborhoods n
        LEFT JOIN displacement_scores ds ON n.zip_code = ds.zip_code
        WHERE n.zip_code = :zip
    """), {"zip": zip_code}).fetchone()

    # No row, or a placeholder row with neither name nor score (the table
    # carries at least one junk entry): nothing to render, real 404.
    if not row or (row.name is None and row.score is None):
        return _not_found()

    from api.routes.neighborhoods import _borough_from_zip, _build_summary, _fetch_raw_counts

    name         = row.name or zip_code
    score        = float(row.score) if row.score is not None else None
    borough      = _borough_from_zip(zip_code)
    breakdown    = dict(row.signal_breakdown) if row.signal_breakdown else {}
    last_updated = row.cache_generated_at.date().isoformat() if row.cache_generated_at else None

    raw_counts = _fetch_raw_counts(db, zip_code)

    hpd_row = db.execute(text("""
        SELECT COUNT(*) FROM violations_raw
        WHERE zip_code = :zip
          AND violation_class IN ('B', 'C')
          AND inspection_date >= CURRENT_DATE - INTERVAL '90 days'
    """), {"zip": zip_code}).fetchone()
    raw_hpd = int(hpd_row[0] or 0)

    summary = _build_summary(score, breakdown, raw_counts)

    history_rows = db.execute(text("""
        SELECT scored_at, composite_score
        FROM score_history
        WHERE zip_code = :zip
          AND scored_at >= CURRENT_DATE - INTERVAL '180 days'
        ORDER BY scored_at ASC
    """), {"zip": zip_code}).fetchall()
    history = [(r.scored_at.isoformat(), round(float(r.composite_score), 1)) for r in history_rows]

    page_html = _build_neighborhood_page(
        zip_code, name, borough, score, breakdown, raw_counts, raw_hpd, summary, last_updated, history,
    )
    _page_cache[zip_code] = (page_html, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page_html)


@router.get("/property/{bbl}", include_in_schema=False)
def property_page(bbl: str, db: Session = Depends(get_db)):
    clean = bbl.strip()
    if not clean.isdigit():
        return _not_found()

    cached = _prop_page_cache.get(clean)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    row = db.execute(text("""
        SELECT p.address, p.zip_code,
               CASE
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 10001 AND 10282 THEN 'Manhattan'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 10301 AND 10314 THEN 'Staten Island'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 10451 AND 10475 THEN 'Bronx'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 11201 AND 11239 THEN 'Brooklyn'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 11001 AND 11109 THEN 'Queens'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 11354 AND 11697 THEN 'Queens'
                   ELSE NULL
               END AS borough,
               ds.score
        FROM parcels p
        LEFT JOIN displacement_scores ds ON p.zip_code = ds.zip_code
        WHERE p.bbl = :bbl
        LIMIT 1
    """), {"bbl": clean}).fetchone()

    if not row:
        return _not_found()

    address  = row.address.title() if row.address else clean
    zip_code = row.zip_code or ""
    borough  = row.borough or "NYC"
    score    = float(row.score) if row.score is not None else None

    url = f"https://pulsecities.com/property/{clean}"
    score_part = f" | Displacement Score {score:.1f}/100" if score is not None else ""
    title = f"{address}, {borough}{score_part} | PulseCities"

    if score is not None:
        tier_label, _ = _tier_info(score)
        desc = (
            f"{address} in {borough} shows {tier_label.lower()} displacement pressure with a "
            f"score of {score:.1f}/100. View eviction filings, construction permits, and "
            f"ownership transfers from NYC public records."
        )
    else:
        desc = (
            f"View displacement risk data for {address} in {borough}, NYC. "
            f"Eviction filings, construction permits, and ownership transfers from public records."
        )

    e_title = _html.escape(title, quote=True)
    e_desc  = _html.escape(desc,  quote=True)
    e_url   = _html.escape(url,   quote=True)

    og_image_url = (
        f"https://pulsecities.com/og/{zip_code}.png?d={date.today().strftime('%Y%m%d')}"
        if zip_code else "https://pulsecities.com/og-image.png"
    )
    e_og_image = _html.escape(og_image_url, quote=True)

    html = _template()
    html = html.replace('<title>Explore | PulseCities</title>', f'<title>{e_title}</title>', 1)
    html = html.replace('<link rel="canonical" href="https://pulsecities.com/map">', f'<link rel="canonical" href="{e_url}">', 1)
    html = _set_meta(html, "name",     "description",          e_desc)
    html = _set_meta(html, "property", "og:title",             e_title)
    html = _set_meta(html, "property", "og:description",       e_desc)
    html = _set_meta(html, "property", "og:url",               e_url)
    html = _set_meta(html, "property", "og:image",             e_og_image)
    html = _set_meta(html, "name",     "twitter:title",        e_title)
    html = _set_meta(html, "name",     "twitter:description",  e_desc)
    html = _set_meta(html, "name",     "twitter:image",        e_og_image)

    # Parcels number in the hundreds of thousands; without a cap a crawler
    # walking /property/ URLs grows this dict until the box runs out of memory.
    if len(_prop_page_cache) >= 512:
        now = time.monotonic()
        expired = [k for k, v in _prop_page_cache.items() if now >= v[1]]
        for k in expired:
            del _prop_page_cache[k]
        if len(_prop_page_cache) >= 512:
            _prop_page_cache.clear()
    _prop_page_cache[clean] = (html, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(html)


_CLASS_LABELS = {
    "financial_institution": "Financial institution",
    "government": "Government",
    "nonprofit_hdfc": "Nonprofit / HDFC",
    "unclassified": "Unclassified",
}


def _minimal_operator_page(display_name: str, operator_class: str) -> str:
    """Minimal profile for non-operator clusters: name, class label, one line.

    No portfolio, signals, or analyst note. Keeps lender and institutional
    activity such as foreclosure off the operator profile surface.
    """
    name = _html.escape(display_name or "Entity")
    label = _CLASS_LABELS.get(operator_class, "Not an operator")
    return f"""<!DOCTYPE html>
<html lang="en" style="color-scheme: dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{name} | PulseCities</title>
<meta name="robots" content="noindex">
<link rel="canonical" href="https://pulsecities.com/operators">
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#e2e8f0;line-height:1.7;
       min-height:100vh;display:flex;flex-direction:column}}
  a{{color:#38bdf8;text-decoration:none}}
  a:hover{{text-decoration:underline}}
  nav{{border-bottom:1px solid rgba(148,163,184,0.12);padding:0 24px;height:52px;display:flex;align-items:center;gap:16px}}
  .brand{{font-size:14px;font-weight:600;color:#f97316}}
  .wrap{{flex:1;max-width:620px;margin:0 auto;padding:72px 24px;width:100%}}
  .label{{font-family:'JetBrains Mono',monospace;font-size:11px;letter-spacing:0.1em;text-transform:uppercase;color:#94a3b8;margin-bottom:12px}}
  h1{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:clamp(22px,4vw,28px);font-weight:600;margin-bottom:10px}}
  .klass{{display:inline-block;font-family:'JetBrains Mono',monospace;font-size:12px;color:#cbd5e1;
         border:1px solid rgba(148,163,184,0.25);border-radius:6px;padding:4px 10px;margin-bottom:20px}}
  p{{color:#94a3b8;font-size:15px}}
  .back{{display:inline-block;margin-top:28px;font-size:13px}}
  footer{{border-top:1px solid rgba(148,163,184,0.12);padding:24px;text-align:center;font-size:13px;color:#94a3b8}}
</style>
</head>
<body>
<nav><a href="/" class="brand">PulseCities</a></nav>
<div class="wrap">
  <p class="label">Not an operator profile</p>
  <h1>{name}</h1>
  <div class="klass">{label}</div>
  <p>Lender and institutional activity such as foreclosure is excluded from operator profiles.</p>
  <a class="back" href="/operators">Back to operators</a>
</div>
<footer><a href="/">Home</a></footer>
</body>
</html>"""


def _operator_not_found_page(label: str) -> str:
    """404 body for a slug that does not resolve to a tracked operator.

    Served with HTTP 404 so crawlers treat it as a real not-found, never a
    soft 404 on a 200 shell.
    """
    name = _html.escape(label or "operator")
    return f"""<!DOCTYPE html>
<html lang="en" style="color-scheme: dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Operator not found | PulseCities</title>
<meta name="robots" content="noindex">
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&display=swap"></noscript>
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#e2e8f0;line-height:1.7;
       min-height:100vh;display:flex;flex-direction:column}}
  a{{color:#38bdf8;text-decoration:none}}
  a:hover{{text-decoration:underline}}
  nav{{border-bottom:1px solid rgba(148,163,184,0.12);padding:0 24px;height:52px;display:flex;align-items:center}}
  .brand{{font-size:14px;font-weight:600;color:#f97316}}
  .wrap{{flex:1;max-width:620px;margin:0 auto;padding:72px 24px;width:100%}}
  h1{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:clamp(22px,4vw,28px);font-weight:600;margin-bottom:10px}}
  p{{color:#94a3b8;font-size:15px}}
  .back{{display:inline-block;margin-top:28px;font-size:13px}}
  footer{{border-top:1px solid rgba(148,163,184,0.12);padding:24px;text-align:center;font-size:13px;color:#94a3b8}}
</style>
</head>
<body>
<nav><a href="/" class="brand">PulseCities</a></nav>
<div class="wrap">
  <h1>Operator not found</h1>
  <p>No tracked operator matches "{name}". Browse the full list of tracked operator networks instead.</p>
  <a class="back" href="/operators">Back to operators</a>
</div>
<footer><a href="/">Home</a></footer>
</body>
</html>"""


@router.head("/operator/{root}", include_in_schema=False)
def operator_page_head(root: str):
    return Response(status_code=200)


@router.get("/operator/{root}", include_in_schema=False)
def operator_page(root: str, db: Session = Depends(get_db)):
    root_upper = root.upper().strip()
    if len(root_upper) < 2:
        return HTMLResponse(_operator_not_found_page(root), status_code=404)

    from api.routes.operators import OPERATOR_NOISE_ROOTS, OPERATOR_NOISE_SLUGS

    # Block finance/lender noise operators — they have DB entries but should not
    # render public profiles.  Return 404 so search engines don't index them.
    if root.lower() in OPERATOR_NOISE_SLUGS or root_upper in OPERATOR_NOISE_ROOTS:
        return Response(status_code=404)

    # The path param may be a slug (e.g. "mtek-nyc") or an operator_root (e.g. "MTEK").
    # Look up both directions so title/meta always use the canonical operator_root.
    op_row = db.execute(
        text(
            "SELECT id, operator_root, slug, display_name, operator_class, "
            "total_properties, total_acquisitions, llc_entities, "
            "jsonb_array_length(llc_entities) AS llc_count "
            "FROM operators WHERE operator_root = :root OR slug = :slug LIMIT 1"
        ),
        {"root": root_upper, "slug": root.lower()},
    ).fetchone()
    # An unresolved slug is a real 404, not a 200 shell that a crawler reads as
    # a soft 404.
    if not op_row:
        return HTMLResponse(_operator_not_found_page(root), status_code=404)

    root_upper   = op_row.operator_root  # canonical root for title/meta and cache key
    canonical_id = op_row.slug

    # Read cache under the canonical root so the slug URL and the root URL share
    # one entry. Reading before resolution keyed on the raw path and missed every
    # slug request.
    cached = _op_page_cache.get(root_upper)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    # Classification gate: only real operators get a full profile. Everything
    # else (banks, GSEs, government, HDFC) gets a minimal page so foreclosure
    # and lender activity is never presented as operator behavior.
    if (op_row.operator_class or "unclassified") != "operator":
        # 404, not 200: a bank or GSE is not an operator profile, so the page must
        # not register as live content for crawlers (soft-404). The body already
        # carries noindex; the status code completes the signal.
        return HTMLResponse(
            _minimal_operator_page(op_row.display_name or root_upper, op_row.operator_class),
            status_code=404,
        )

    url = f"https://pulsecities.com/operator/{canonical_id}"

    # Head counts come from the operators row, the same source the body renders,
    # so the title and description never contradict the page.
    acq_count    = op_row.total_acquisitions or 0
    entity_count = op_row.llc_count or 0
    title = f"{root_upper} LLC Network | NYC Property Acquisitions | PulseCities"
    if acq_count and entity_count:
        desc = (
            f"{root_upper}: {acq_count} property "
            f"{'acquisition' if acq_count == 1 else 'acquisitions'} in NYC, "
            f"tracked across {entity_count} LLC "
            f"{'entity' if entity_count == 1 else 'entities'}. "
            "Sourced from ACRIS public deed records."
        )
    elif acq_count:
        desc = (
            f"{root_upper}: {acq_count} property "
            f"{'acquisition' if acq_count == 1 else 'acquisitions'} in NYC. "
            "Sourced from ACRIS public deed records."
        )
    else:
        desc = f"{root_upper} LLC network in NYC. Sourced from ACRIS public deed records."

    e_title = _html.escape(title, quote=True)
    e_desc  = _html.escape(desc,  quote=True)
    e_url   = _html.escape(url,   quote=True)

    # --- Server-rendered body so the page carries real content without JS ---
    # The client JS hydrates the same elements on load (and clears the acquisition
    # rows first), so this is the substantive content a crawler sees, not a shell.
    zip_count = db.execute(
        text(
            "SELECT count(DISTINCT p.zip_code) FROM operator_parcels op "
            "JOIN parcels p ON p.bbl = op.bbl WHERE op.operator_id = :id"
        ),
        {"id": op_row.id},
    ).scalar() or 0

    cutoff = date.today() - timedelta(days=548)  # same 18-month window the profile API uses
    acq_rows = db.execute(
        text(
            "SELECT p.address, o.bbl, p.zip_code, o.party_name_normalized AS buyer, "
            "o.doc_date, o.doc_amount "
            "FROM ownership_raw o JOIN parcels p ON p.bbl = o.bbl "
            "WHERE o.party_type = '2' AND o.party_name_normalized = ANY(:names) "
            "AND o.doc_date >= :cutoff "
            "ORDER BY o.doc_date DESC NULLS LAST LIMIT 20"
        ),
        {"names": op_row.llc_entities or [], "cutoff": cutoff},
    ).fetchall()

    def _e(v):
        return _html.escape(str(v), quote=True)

    summary_html = (
        f"{acq_count} acquisition{'' if acq_count == 1 else 's'} "
        f"across {zip_count} ZIP code{'' if zip_count == 1 else 's'}"
        if acq_count else ""
    )

    acq_body = ""
    for r in acq_rows:
        addr = _e(r.address) if r.address else f"Lot {_e(r.bbl)} (no address on record)"
        doc_date = r.doc_date.isoformat() if r.doc_date else ""
        amount = f"${int(r.doc_amount):,}" if r.doc_amount and float(r.doc_amount) > 0 else "N/A"
        acq_body += (
            "<tr>"
            f'<td style="padding:8px 16px;color:rgba(241,245,249,0.85);">{addr}</td>'
            f'<td class="mono" style="padding:8px 16px;color:rgba(148,163,184,0.75);font-size:0.72rem;">{_e(r.zip_code or "")}</td>'
            f'<td class="mono" style="padding:8px 16px;color:#94a3b8;font-size:0.7rem;">{_e(r.buyer or "")}</td>'
            f'<td class="mono" style="padding:8px 16px;color:rgba(148,163,184,0.75);font-size:0.72rem;">{_e(doc_date)}</td>'
            f'<td class="mono" style="padding:8px 8px 8px 16px;text-align:right;color:#94a3b8;font-size:0.72rem;">{_e(amount)}</td>'
            "</tr>"
        )

    html = _operator_template()
    html = html.replace('<title>Operator Profile | PulseCities</title>', f'<title>{e_title}</title>', 1)
    html = html.replace(
        'content="LLC portfolio and affiliated operator network for a NYC acquisition cluster, sourced from ACRIS public records."',
        f'content="{e_desc}"',
    )
    html = html.replace(
        '<link rel="canonical" id="canonical-url" href="https://pulsecities.com/">',
        f'<link rel="canonical" id="canonical-url" href="{e_url}">',
        1,
    )

    op_og_image = (
        f"https://pulsecities.com/og/operator/{canonical_id}.png"
        f"?d={date.today().strftime('%Y%m%d')}"
    )
    e_op_og = _e(op_og_image)
    og_block = (
        f'    <meta property="og:title" content="{e_title}">\n'
        f'    <meta property="og:description" content="{e_desc}">\n'
        f'    <meta property="og:url" content="{e_url}">\n'
        f'    <meta property="og:type" content="website">\n'
        f'    <meta property="og:site_name" content="PulseCities">\n'
        f'    <meta property="og:image" content="{e_op_og}">\n'
        f'    <meta property="og:image:width" content="1200">\n'
        f'    <meta property="og:image:height" content="630">\n'
        f'    <meta name="twitter:card" content="summary_large_image">\n'
        f'    <meta name="twitter:title" content="{e_title}">\n'
        f'    <meta name="twitter:description" content="{e_desc}">\n'
        f'    <meta name="twitter:image" content="{e_op_og}">'
    )
    html = html.replace('</head>', f'{og_block}\n</head>', 1)

    # Inject the real operator data into the body so the served HTML is
    # substantive on first byte. The client JS overwrites these on hydration.
    html = html.replace(
        '<h1 id="op-root" class="mono accent" style="font-size: 1.8rem; font-weight: 400; letter-spacing: 0.04em;"></h1>',
        f'<h1 id="op-root" class="mono accent" style="font-size: 1.8rem; font-weight: 400; letter-spacing: 0.04em;">{_e(root_upper)}</h1>',
        1,
    )
    html = html.replace(
        '<div id="op-summary" style="font-size: 0.82rem; color: #94a3b8; margin-top: 5px;"></div>',
        f'<div id="op-summary" style="font-size: 0.82rem; color: #94a3b8; margin-top: 5px;">{_e(summary_html)}</div>',
        1,
    )
    html = html.replace('<div class="stat-val" id="stat-properties"></div>',
                        f'<div class="stat-val" id="stat-properties">{op_row.total_properties or 0}</div>', 1)
    html = html.replace('<div class="stat-val" id="stat-acquisitions"></div>',
                        f'<div class="stat-val" id="stat-acquisitions">{acq_count}</div>', 1)
    html = html.replace('<div class="stat-val" id="stat-llcs"></div>',
                        f'<div class="stat-val" id="stat-llcs">{entity_count}</div>', 1)
    html = html.replace('<div class="stat-val" id="stat-zips"></div>',
                        f'<div class="stat-val" id="stat-zips">{zip_count}</div>', 1)
    html = html.replace('<tbody id="acq-rows"></tbody>', f'<tbody id="acq-rows">{acq_body}</tbody>', 1)

    _op_page_cache[root_upper] = (html, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(html)


_SCRIPTS = Path(__file__).parent.parent.parent / "scripts"
_operators_cache: tuple[str, float] | None = None  # cleared on restart


@router.get("/operators", include_in_schema=False)
def operators_directory(db: Session = Depends(get_db)):
    global _operators_cache
    if _operators_cache and time.monotonic() < _operators_cache[1]:
        return HTMLResponse(_operators_cache[0])

    from api.routes.operators import OPERATOR_NOISE_ROOTS

    def _zip_to_borough(z: str) -> str | None:
        try:
            n = int(z)
        except ValueError:
            return None
        if 10001 <= n <= 10282: return "Manhattan"
        if 10301 <= n <= 10314: return "Staten Island"
        if 10451 <= n <= 10475: return "Bronx"
        if 11201 <= n <= 11239: return "Brooklyn"
        if (11001 <= n <= 11109) or (11354 <= n <= 11697): return "Queens"
        return None

    # Read every count from the same DB sources the profile page uses, so the two
    # pages can never disagree: total_acquisitions and the LLC-entity count come
    # straight from the operators row, and the ZIP count is the live
    # operator_parcels -> parcels join (count(DISTINCT zip)) — exactly what
    # operator_page() computes. The audit JSON is no longer consulted for these
    # numbers; it drifts from the DB and was the source of the directory/profile
    # mismatch.
    #
    # Filter: class 'operator' only (the classification gate keeps banks, GSEs,
    # servicers, government, and HDFCs out), a measurable LLC footprint, and not a
    # known finance/lender noise root.
    db_rows = db.execute(
        text(
            "SELECT o.operator_root, o.slug, "
            "       COALESCE(o.total_acquisitions, 0) AS acqs, "
            "       COALESCE(jsonb_array_length(o.llc_entities), 0) AS entities, "
            "       count(DISTINCT p.zip_code) AS zip_count, "
            "       array_agg(DISTINCT p.zip_code) FILTER (WHERE p.zip_code IS NOT NULL) AS zips "
            "FROM operators o "
            "LEFT JOIN operator_parcels op ON op.operator_id = o.id "
            "LEFT JOIN parcels p ON p.bbl = op.bbl "
            "WHERE o.operator_class = 'operator' "
            "  AND COALESCE(jsonb_array_length(o.llc_entities), 0) > 0 "
            "GROUP BY o.id "
            "ORDER BY COALESCE(o.total_acquisitions, 0) DESC, o.operator_root"
        )
    ).fetchall()
    operators = [r for r in db_rows if r.operator_root not in OPERATOR_NOISE_ROOTS]

    rows_html = ""
    list_items = []
    for i, op in enumerate(operators, 1):
        root = op.operator_root
        entities = op.entities
        acqs = op.acqs
        zips = op.zips or []
        boroughs = list(dict.fromkeys(b for z in zips if (b := _zip_to_borough(z))))
        extra = len(boroughs) - 2
        borough_str = ", ".join(boroughs[:2]) + (f" +{extra}" if extra > 0 else "")
        slug = op.slug
        op_link = f"/operator/{_html.escape(slug)}"
        zip_count = op.zip_count
        meta_parts = []
        if acqs:     meta_parts.append(f'{acqs} <span class="op-label-acq">acquisitions</span>')
        if entities: meta_parts.append(f'{entities} LLC{"s" if entities != 1 else ""}')
        if zip_count: meta_parts.append(f'{zip_count} ZIP code{"s" if zip_count != 1 else ""}')
        meta_line = f'<div class="op-meta" data-count="{acqs}">{", ".join(meta_parts)}</div>' if meta_parts else ""
        geo_html  = f'<div class="op-geo">{_html.escape(borough_str)}</div>' if borough_str else ""
        rows_html += (
            f'<li class="op-row" onclick="location.href=\'{op_link}\'">'
            f'<a href="{op_link}">'
            f'<div class="op-rank">#{i}</div>'
            f'<div class="op-body">'
            f'<div class="op-name">{_html.escape(root)}</div>'
            f'{meta_line}'
            f'{geo_html}'
            f'<div class="op-cta">View profile →</div>'
            f'</div>'
            f'</a>'
            f'</li>\n'
        )
        list_items.append({
            "@type": "ListItem",
            "position": i,
            "name": f"{root} LLC Network",
            "url": f"https://pulsecities.com/operator/{slug}",
        })

    n_visible = len(operators)
    title = "NYC Operator Networks | PulseCities"
    desc = (
        f"{n_visible} public-record operator clusters with measurable NYC acquisition activity, "
        "sourced from ACRIS deed records."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": "NYC Operator Networks",
        "description": desc,
        "url": "https://pulsecities.com/operators",
        "numberOfItems": n_visible,
        "itemListElement": list_items,
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="https://pulsecities.com/operators">
<meta property="og:title" content="NYC Operator Networks | PulseCities">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/operators">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="NYC Operator Networks | PulseCities">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.op-list{{list-style:none;padding:0;margin:0}}
.op-row{{border-bottom:1px solid rgba(148,163,184,0.07);cursor:pointer;}}
.op-row:hover{{background:rgba(148,163,184,0.04)}}
.op-row a{{display:flex;align-items:flex-start;gap:12px;padding:14px 0;text-decoration:none;color:inherit;}}
.op-rank{{font-family:'JetBrains Mono',monospace;font-size:0.68rem;color:rgba(148,163,184,0.5);min-width:24px;padding-top:3px;flex-shrink:0;}}
.op-body{{display:flex;flex-direction:column;gap:3px;}}
.op-name{{font-family:'JetBrains Mono',monospace;font-size:0.88rem;color:#e2e8f0;letter-spacing:0.04em;font-weight:500;}}
.op-row:hover .op-name{{color:#f97316;}}
.op-meta{{font-size:0.78rem;color:#94a3b8;}}
.op-geo{{font-size:0.73rem;color:rgba(148,163,184,0.65);}}
.op-cta{{font-size:0.72rem;color:rgba(249,115,22,0.75);font-family:'JetBrains Mono',monospace;margin-top:2px;}}
.op-row:hover .op-cta{{color:rgba(249,115,22,0.95);}}
</style>
</head>
<body>
<nav>
  <div class="nav-inner">
    <a href="/" style="display:flex;align-items:center;gap:8px;color:#f1f5f9;">
      <svg width="22" height="22" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <span style="font-size:0.85rem;color:rgba(148,163,184,0.6);">PulseCities</span>
    </a>
    <div style="display:flex;align-items:center;gap:16px;">
      <a href="/map" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Map</a>
      <a href="/methodology" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Methodology</a>
      <a href="/about" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">About</a>
      <button id="lang-toggle" style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.5);background:none;border:none;cursor:pointer;padding:4px 2px;min-height:32px;">EN / ES</button>
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; Home</a>
  </div>
  <h1 id="dir-heading" style="font-size:1.4rem;font-weight:600;margin-bottom:6px;">NYC Operator Networks</h1>
  <p id="dir-desc" style="font-size:0.82rem;color:#94a3b8;margin-bottom:8px;line-height:1.6;">
    Ownership clusters identified in NYC deed records. Each groups LLCs by naming patterns and acquisition activity. Public records only.
  </p>
  <p style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.55);margin-bottom:28px;">{n_visible} clusters tracked across an 18-month public records window.</p>
  <ul class="op-list">
{rows_html}  </ul>
</div>
<footer>
  <div style="font-size:11px;color:#64748b;margin-bottom:8px;text-align:center;">Built by Michael Espin</div>
  <div class="footer-links">
    <a href="/" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Home</a>
    <a href="/methodology" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Methodology</a>
    <a href="/about" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">About</a>
    <a href="/status" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Status</a>
    <a href="mailto:nycdisplacement@gmail.com" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Contact</a>
    <a href="https://x.com/PulseCities" target="_blank" rel="noopener noreferrer" aria-label="PulseCities on X" style="color:#64748b;text-decoration:none;display:inline-flex;align-items:center;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" focusable="false"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg></a>
  </div>
</footer>
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  var i18n = {{
    en: {{
      heading: 'NYC Operator Networks',
      desc: 'Ownership clusters identified in NYC deed records. Each groups LLCs by naming patterns and acquisition activity. Public records only.',
      acq: 'acquisitions',
      toggle: 'EN / ES'
    }},
    es: {{
      heading: 'Redes de operadores de NYC',
      desc: 'Grupos de propiedad identificados en registros de escrituras de NYC. Cada uno agrupa LLC por patrones de nombres y actividad de adquisición. Solo registros públicos.',
      acq: 'adquisiciones',
      toggle: 'ES / EN'
    }}
  }};
  function applyLang(l) {{
    var s = i18n[l] || i18n.en;
    var h = document.getElementById('dir-heading');
    if (h) h.textContent = s.heading;
    var d = document.getElementById('dir-desc');
    if (d) d.textContent = s.desc;
    document.querySelectorAll('.op-label-acq').forEach(function(el) {{
      el.textContent = s.acq;
    }});
    var btn = document.getElementById('lang-toggle');
    if (btn) btn.textContent = s.toggle;
  }}
  applyLang(lang);
  var btn = document.getElementById('lang-toggle');
  if (btn) btn.addEventListener('click', function() {{
    lang = lang === 'en' ? 'es' : 'en';
    localStorage.setItem('pc-lang', lang);
    applyLang(lang);
  }});
}})();
</script>
</body>
</html>"""

    _operators_cache = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


_nbhd_index_cache: tuple[str, float] | None = None  # cleared on restart


@router.get("/neighborhoods", include_in_schema=False)
def neighborhoods_directory(db: Session = Depends(get_db)):
    """Every scored ZIP page, grouped by borough, ranked by score.

    One crawlable hop from the homepage to all 177 neighborhood pages, and a
    scannable answer to "how does my area compare" without opening the map.
    """
    global _nbhd_index_cache
    if _nbhd_index_cache and time.monotonic() < _nbhd_index_cache[1]:
        return HTMLResponse(_nbhd_index_cache[0])

    from api.routes.neighborhoods import _borough_from_zip

    rows = db.execute(text("""
        SELECT n.zip_code, n.name, ds.score
        FROM neighborhoods n
        JOIN displacement_scores ds ON ds.zip_code = n.zip_code
        WHERE ds.score IS NOT NULL
        ORDER BY ds.score DESC
    """)).fetchall()

    boroughs: dict[str, list] = {}
    for r in rows:
        b = _borough_from_zip(r.zip_code) or "Other"
        boroughs.setdefault(b, []).append(r)

    sections_html = ""
    list_items = []
    pos = 0
    for borough in ("Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "Other"):
        entries = boroughs.get(borough)
        if not entries:
            continue
        rows_html = ""
        for r in entries:
            score = float(r.score)
            _, color = _tier_info(score)
            name = _html.escape(r.name or r.zip_code)
            width = max(2, min(100, score))
            pos += 1
            rows_html += (
                f'<li class="nb-row"><a href="/neighborhood/{r.zip_code}">'
                f'<span class="nb-zip">{r.zip_code}</span>'
                f'<span class="nb-name">{name}</span>'
                f'<span class="nb-score" style="color:{color};">{score:.1f}</span>'
                f'<span class="nb-track"><span class="nb-fill" style="width:{width}%;background:{color};"></span></span>'
                f'</a></li>\n'
            )
            list_items.append({
                "@type": "ListItem",
                "position": pos,
                "name": f"{r.name or r.zip_code} ({r.zip_code}) displacement score",
                "url": f"https://pulsecities.com/neighborhood/{r.zip_code}",
            })
        sections_html += (
            f'<section class="nb-borough">'
            f'<h2>{borough}</h2>'
            f'<ul class="nb-list">\n{rows_html}</ul>'
            f'</section>\n'
        )

    n = len(rows)
    title = "NYC Neighborhoods by Displacement Score | PulseCities"
    desc = (
        f"Displacement-pressure scores for all {n} scored NYC ZIP codes, grouped by borough "
        f"and ranked by current score. Built from public records, refreshed nightly."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": "NYC neighborhoods by displacement score",
        "description": desc,
        "url": "https://pulsecities.com/neighborhoods",
        "numberOfItems": n,
        "itemListElement": list_items,
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="https://pulsecities.com/neighborhoods">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/neighborhoods">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.nb-borough{{margin-bottom:36px}}
.nb-borough h2{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.05rem;font-weight:600;margin-bottom:10px;color:#e2e8f0}}
.nb-list{{list-style:none;padding:0;margin:0}}
.nb-row{{border-bottom:1px solid rgba(148,163,184,0.07)}}
.nb-row:hover{{background:rgba(148,163,184,0.04)}}
.nb-row a{{display:grid;grid-template-columns:56px 1fr 52px;grid-template-rows:auto auto;column-gap:14px;row-gap:5px;align-items:baseline;padding:10px 0}}
.nb-zip{{font-family:'JetBrains Mono',monospace;font-size:0.85rem;font-weight:500;color:#e2e8f0}}
.nb-row:hover .nb-zip{{color:#f97316}}
.nb-name{{font-size:0.82rem;color:#94a3b8;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.nb-score{{font-family:'JetBrains Mono',monospace;font-size:0.85rem;font-weight:600;text-align:right}}
.nb-track{{grid-column:1 / -1;display:block;height:3px;border-radius:2px;background:rgba(148,163,184,0.1);overflow:hidden}}
.nb-fill{{display:block;height:100%;border-radius:2px}}
</style>
</head>
<body>
<nav>
  <div class="nav-inner">
    <a href="/" style="display:flex;align-items:center;gap:8px;color:#f1f5f9;">
      <svg width="22" height="22" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <span style="font-size:0.85rem;color:rgba(148,163,184,0.6);">PulseCities</span>
    </a>
    <div style="display:flex;align-items:center;gap:16px;">
      <a href="/map" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Map</a>
      <a href="/operators" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Operators</a>
      <a href="/flips" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Flips</a>
      <a href="/radar" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Radar</a>
      <a href="/methodology" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Methodology</a>
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; Home</a>
  </div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">NYC neighborhoods by displacement score</h1>
  <p style="font-size:0.82rem;color:#94a3b8;margin-bottom:8px;line-height:1.6;">
    Every scored ZIP in the city, grouped by borough and ranked by current displacement pressure. Each page shows the signal breakdown, the six-month trend, and an embeddable score badge.
  </p>
  <p style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.55);margin-bottom:28px;">{n} ZIP codes scored nightly from public records.</p>
  {sections_html}
</div>
<footer>
  <div style="font-size:11px;color:#64748b;margin-bottom:8px;text-align:center;">Built by Michael Espin</div>
  <div class="footer-links">
    <a href="/" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Home</a>
    <a href="/methodology" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Methodology</a>
    <a href="/about" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">About</a>
    <a href="/status" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Status</a>
    <a href="mailto:nycdisplacement@gmail.com" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Contact</a>
  </div>
</footer>
</body>
</html>"""

    _nbhd_index_cache = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


_flips_cache: tuple[str, float] | None = None  # cleared on restart


def _fmt_amount(v) -> str:
    """Compact money label: $2.4M, $815K. Empty string when the deed had no price."""
    if not v:
        return ""
    v = float(v)
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M".replace(".0M", "M")
    if v >= 1_000:
        return f"${round(v / 1_000)}K"
    return f"${int(v)}"


@router.get("/flips", include_in_schema=False)
def flip_watch_page(db: Session = Depends(get_db)):
    """Flip Watch — citywide renovation-flip feed, server-rendered.

    Same content as /api/flips, rendered as a standing page so the pattern is
    indexable and shareable rather than buried one ZIP at a time.
    """
    global _flips_cache
    if _flips_cache and time.monotonic() < _flips_cache[1]:
        return HTMLResponse(_flips_cache[0])

    from api.routes.flips import query_flips, LOOKBACK_DAYS, FLIP_WINDOW_DAYS
    flips = query_flips(db)

    _MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _short_date(iso: str | None) -> str:
        if not iso:
            return ""
        try:
            y, m, d = iso.split("-")
            return f"{_MONTHS[int(m)]} {int(d)}, {y}"
        except (ValueError, IndexError):
            return iso

    rows_html = ""
    list_items = []
    for i, f in enumerate(flips, 1):
        bbl = _html.escape(str(f["bbl"]))
        addr = _html.escape(f["address"])
        zip_code = _html.escape(str(f["zip_code"]))
        hood = _html.escape(f["neighborhood"] or zip_code)
        geo = f"{hood} &middot; {zip_code}" if f["neighborhood"] else zip_code
        buyer = _html.escape(f["buyer"] or "")
        amount = _fmt_amount(f["doc_amount"])
        days = f["days_between"]
        gap = f"+{days}d" if days is not None else ""
        bought = _short_date(f["transfer_date"])
        amount_html = f'<div class="flip-amount">{amount}</div>' if amount else ""
        prop_link = f"/property/{bbl}"
        rows_html += (
            f'<li class="flip-row" onclick="location.href=\'{prop_link}\'">'
            f'<a href="{prop_link}">'
            f'<div class="flip-main">'
            f'<div class="flip-addr">{addr}</div>'
            f'<div class="flip-geo">{geo}</div>'
            f'<div class="flip-buyer">{buyer}</div>'
            f'<div class="flip-when"><span class="flip-when-label">Bought</span> {bought}</div>'
            f'</div>'
            f'<div class="flip-side">'
            f'<div class="flip-gap">{gap}</div>'
            f'<div class="flip-gap-label">buy &rarr; permit</div>'
            f'{amount_html}'
            f'</div>'
            f'</a>'
            f'</li>\n'
        )
        list_items.append({
            "@type": "ListItem",
            "position": i,
            "name": f"{f['address']} renovation flip",
            "url": f"https://pulsecities.com/property/{f['bbl']}",
        })

    n = len(flips)
    if not rows_html:
        rows_html = (
            '<li class="flip-empty" id="flip-empty">No flips matched the pattern in the '
            'current window. Check back after the next nightly refresh.</li>\n'
        )

    title = "Flip Watch | PulseCities"
    desc = (
        f"{n} NYC buildings where an LLC bought and filed a renovation permit within "
        f"{FLIP_WINDOW_DAYS} days, sourced from ACRIS deeds and DOB permits. Updated nightly."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": "NYC Flip Watch",
        "description": desc,
        "url": "https://pulsecities.com/flips",
        "numberOfItems": n,
        "itemListElement": list_items,
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="https://pulsecities.com/flips">
<meta property="og:title" content="Flip Watch | PulseCities">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/flips">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="Flip Watch | PulseCities">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.flip-list{{list-style:none;padding:0;margin:0}}
.flip-row{{border-bottom:1px solid rgba(148,163,184,0.07);cursor:pointer;}}
.flip-row:hover{{background:rgba(148,163,184,0.04)}}
.flip-row a{{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;padding:16px 0;text-decoration:none;color:inherit;}}
.flip-main{{display:flex;flex-direction:column;gap:3px;min-width:0;}}
.flip-addr{{font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#e2e8f0;letter-spacing:0.03em;font-weight:500;}}
.flip-row:hover .flip-addr{{color:#f97316;}}
.flip-geo{{font-size:0.76rem;color:rgba(148,163,184,0.7);}}
.flip-buyer{{font-family:'JetBrains Mono',monospace;font-size:0.74rem;color:#94a3b8;margin-top:2px;}}
.flip-when{{font-size:0.72rem;color:rgba(148,163,184,0.55);margin-top:2px;}}
.flip-when-label{{color:rgba(148,163,184,0.4);}}
.flip-side{{display:flex;flex-direction:column;align-items:flex-end;flex-shrink:0;text-align:right;}}
.flip-gap{{font-family:'JetBrains Mono',monospace;font-size:1.05rem;font-weight:500;color:#f97316;line-height:1.1;}}
.flip-gap-label{{font-size:0.62rem;color:rgba(148,163,184,0.5);text-transform:uppercase;letter-spacing:0.06em;margin-top:1px;}}
.flip-amount{{font-family:'JetBrains Mono',monospace;font-size:0.78rem;color:#cbd5e1;margin-top:8px;}}
.flip-empty{{padding:24px 0;font-size:0.82rem;color:#94a3b8;}}
</style>
</head>
<body>
<nav>
  <div class="nav-inner">
    <a href="/" style="display:flex;align-items:center;gap:8px;color:#f1f5f9;">
      <svg width="22" height="22" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <span style="font-size:0.85rem;color:rgba(148,163,184,0.6);">PulseCities</span>
    </a>
    <div style="display:flex;align-items:center;gap:16px;">
      <a href="/map" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Map</a>
      <a href="/operators" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Operators</a>
      <a href="/radar" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Radar</a>
      <a href="/methodology" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Methodology</a>
      <button id="lang-toggle" style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.5);background:none;border:none;cursor:pointer;padding:4px 2px;min-height:32px;">EN / ES</button>
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; Home</a>
  </div>
  <h1 id="fw-heading" style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">Flip Watch</h1>
  <p id="fw-desc" style="font-size:0.82rem;color:#94a3b8;margin-bottom:8px;line-height:1.6;">
    Buildings where an LLC took the deed and filed a renovation permit within {FLIP_WINDOW_DAYS} days. That fast turn is one of the clearest early signals of a building being repositioned. Public records only.
  </p>
  <p id="fw-sub" style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.55);margin-bottom:28px;">{n} flips detected across NYC in the past 12 months.</p>
  <ul class="flip-list">
{rows_html}  </ul>
  <p id="fw-note" style="font-size:0.72rem;color:rgba(148,163,184,0.45);margin-top:24px;line-height:1.6;">
    A renovation permit alone is not wrongdoing. This page reports the public-record pattern, not a conclusion about any owner. <a href="/methodology" style="color:rgba(249,115,22,0.75);">How this is measured &rarr;</a>
  </p>
</div>
<footer>
  <div style="font-size:11px;color:#64748b;margin-bottom:8px;text-align:center;">Built by Michael Espin</div>
  <div class="footer-links">
    <a href="/" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Home</a>
    <a href="/methodology" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Methodology</a>
    <a href="/about" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">About</a>
    <a href="/status" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Status</a>
    <a href="mailto:nycdisplacement@gmail.com" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Contact</a>
    <a href="https://x.com/PulseCities" target="_blank" rel="noopener noreferrer" aria-label="PulseCities on X" style="color:#64748b;text-decoration:none;display:inline-flex;align-items:center;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" focusable="false"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg></a>
  </div>
</footer>
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  var i18n = {{
    en: {{
      heading: 'Flip Watch',
      desc: 'Buildings where an LLC took the deed and filed a renovation permit within {FLIP_WINDOW_DAYS} days. That fast turn is one of the clearest early signals of a building being repositioned. Public records only.',
      sub: '{n} flips detected across NYC in the past 12 months.',
      note: 'A renovation permit alone is not wrongdoing. This page reports the public-record pattern, not a conclusion about any owner.',
      bought: 'Bought',
      gap: 'buy \\u2192 permit',
      toggle: 'EN / ES'
    }},
    es: {{
      heading: 'Vigilancia de reventas',
      desc: 'Edificios donde una LLC tom\\u00f3 la escritura y solicit\\u00f3 un permiso de renovaci\\u00f3n en un plazo de {FLIP_WINDOW_DAYS} d\\u00edas. Ese giro r\\u00e1pido es una de las se\\u00f1ales tempranas m\\u00e1s claras de que un edificio est\\u00e1 siendo reposicionado. Solo registros p\\u00fablicos.',
      sub: '{n} reventas detectadas en NYC en los \\u00faltimos 12 meses.',
      note: 'Un permiso de renovaci\\u00f3n por s\\u00ed solo no es una infracci\\u00f3n. Esta p\\u00e1gina informa el patr\\u00f3n de registro p\\u00fablico, no una conclusi\\u00f3n sobre ning\\u00fan propietario.',
      bought: 'Comprado',
      gap: 'compra \\u2192 permiso',
      toggle: 'ES / EN'
    }}
  }};
  function applyLang(l) {{
    var s = i18n[l] || i18n.en;
    var set = function(id, val) {{ var el = document.getElementById(id); if (el) el.textContent = val; }};
    set('fw-heading', s.heading);
    set('fw-sub', s.sub);
    var d = document.getElementById('fw-desc'); if (d) d.textContent = s.desc;
    var note = document.getElementById('fw-note');
    if (note) note.innerHTML = s.note + ' <a href="/methodology" style="color:rgba(249,115,22,0.75);">' + (l === 'es' ? 'C\\u00f3mo se mide \\u2192' : 'How this is measured \\u2192') + '</a>';
    document.querySelectorAll('.flip-when-label').forEach(function(el) {{ el.textContent = s.bought; }});
    document.querySelectorAll('.flip-gap-label').forEach(function(el) {{ el.textContent = s.gap; }});
    var btn = document.getElementById('lang-toggle');
    if (btn) btn.textContent = s.toggle;
  }}
  applyLang(lang);
  var btn = document.getElementById('lang-toggle');
  if (btn) btn.addEventListener('click', function() {{
    lang = lang === 'en' ? 'es' : 'en';
    localStorage.setItem('pc-lang', lang);
    applyLang(lang);
  }});
}})();
</script>
</body>
</html>"""

    _flips_cache = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


_radar_cache: tuple[str, float] | None = None  # cleared on restart


@router.get("/radar", include_in_schema=False)
def speculation_radar_page(db: Session = Depends(get_db)):
    """Speculation Radar — concentrated LLC buying, server-rendered.

    Same content as /api/radar, rendered as a standing page so the pattern is
    indexable and shareable. Each cluster is one buyer assembling a position in
    one ZIP; the property list under it is the receipts.
    """
    global _radar_cache
    if _radar_cache and time.monotonic() < _radar_cache[1]:
        return HTMLResponse(_radar_cache[0])

    from api.routes.radar import query_radar, RADAR_WINDOW_DAYS, MIN_BUILDINGS
    clusters = query_radar(db)

    _MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _short_date(iso: str | None) -> str:
        if not iso:
            return ""
        try:
            y, m, d = iso.split("-")
            return f"{_MONTHS[int(m)]} {int(d)}, {y}"
        except (ValueError, IndexError):
            return iso

    rows_html = ""
    list_items = []
    for i, c in enumerate(clusters, 1):
        buyer = _html.escape(c["buyer"] or "")
        zip_code = _html.escape(str(c["zip_code"]))
        hood = _html.escape(c["neighborhood"] or zip_code)
        geo = f"{hood} &middot; {zip_code}" if c["neighborhood"] else zip_code
        first = _short_date(c["first_deed"])
        last = _short_date(c["last_deed"])
        when = first if c["first_deed"] == c["last_deed"] else f"{first} to {last}"
        amount = _fmt_amount(c["total_amount"])
        amount_html = f'<div class="radar-amount">{amount}</div>' if amount else ""
        props_html = ""
        for p in c["properties"]:
            bbl = _html.escape(str(p["bbl"]))
            addr = _html.escape(p["address"])
            p_amt = _fmt_amount(p["amount"])
            amt_span = f'<span class="radar-prop-amt">{p_amt}</span>' if p_amt else ""
            props_html += (
                f'<li><a href="/property/{bbl}" class="radar-prop">'
                f'<span class="radar-prop-addr">{addr}</span>{amt_span}</a></li>'
            )
        rows_html += (
            f'<li class="radar-row">'
            f'<div class="radar-head">'
            f'<div class="radar-main">'
            f'<div class="radar-buyer">{buyer}</div>'
            f'<div class="radar-geo">{geo}</div>'
            f'<div class="radar-when"><span class="radar-when-label">Deeds</span> {when}</div>'
            f'</div>'
            f'<div class="radar-side">'
            f'<div class="radar-count">{c["building_count"]}</div>'
            f'<div class="radar-count-label">buildings</div>'
            f'{amount_html}'
            f'</div>'
            f'</div>'
            f'<ul class="radar-props">{props_html}</ul>'
            f'</li>\n'
        )
        list_items.append({
            "@type": "ListItem",
            "position": i,
            "name": f"{c['buyer']} acquisitions in {c['neighborhood'] or c['zip_code']}",
        })

    n = len(clusters)
    if not rows_html:
        rows_html = (
            '<li class="radar-empty" id="radar-empty">No buying runs matched the pattern '
            'in the current window. Check back after the next nightly refresh.</li>\n'
        )

    title = "Speculation Radar | PulseCities"
    desc = (
        f"{n} NYC buying runs where one LLC took the deed on {MIN_BUILDINGS} or more "
        f"buildings in the same ZIP within {RADAR_WINDOW_DAYS} days, sourced from ACRIS "
        f"deeds. Updated nightly."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": "NYC Speculation Radar",
        "description": desc,
        "url": "https://pulsecities.com/radar",
        "numberOfItems": n,
        "itemListElement": list_items,
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="https://pulsecities.com/radar">
<meta property="og:title" content="Speculation Radar | PulseCities">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/radar">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="Speculation Radar | PulseCities">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.radar-list{{list-style:none;padding:0;margin:0}}
.radar-row{{border-bottom:1px solid rgba(148,163,184,0.07);padding:18px 0;}}
.radar-head{{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;}}
.radar-main{{display:flex;flex-direction:column;gap:3px;min-width:0;}}
.radar-buyer{{font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#e2e8f0;letter-spacing:0.03em;font-weight:500;}}
.radar-geo{{font-size:0.76rem;color:rgba(148,163,184,0.7);}}
.radar-when{{font-size:0.72rem;color:rgba(148,163,184,0.55);margin-top:2px;}}
.radar-when-label{{color:rgba(148,163,184,0.4);}}
.radar-side{{display:flex;flex-direction:column;align-items:flex-end;flex-shrink:0;text-align:right;}}
.radar-count{{font-family:'JetBrains Mono',monospace;font-size:1.35rem;font-weight:500;color:#f97316;line-height:1.1;}}
.radar-count-label{{font-size:0.62rem;color:rgba(148,163,184,0.5);text-transform:uppercase;letter-spacing:0.06em;margin-top:1px;}}
.radar-amount{{font-family:'JetBrains Mono',monospace;font-size:0.78rem;color:#cbd5e1;margin-top:8px;}}
.radar-props{{list-style:none;padding:0;margin:10px 0 0 0;border-left:2px solid rgba(249,115,22,0.25);}}
.radar-prop{{display:flex;align-items:baseline;justify-content:space-between;gap:12px;padding:4px 0 4px 12px;}}
.radar-prop:hover .radar-prop-addr{{color:#f97316;}}
.radar-prop-addr{{font-family:'JetBrains Mono',monospace;font-size:0.76rem;color:#94a3b8;}}
.radar-prop-amt{{font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.55);flex-shrink:0;}}
.radar-empty{{padding:24px 0;font-size:0.82rem;color:#94a3b8;}}
</style>
</head>
<body>
<nav>
  <div class="nav-inner">
    <a href="/" style="display:flex;align-items:center;gap:8px;color:#f1f5f9;">
      <svg width="22" height="22" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <span style="font-size:0.85rem;color:rgba(148,163,184,0.6);">PulseCities</span>
    </a>
    <div style="display:flex;align-items:center;gap:16px;">
      <a href="/map" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Map</a>
      <a href="/operators" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Operators</a>
      <a href="/flips" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Flips</a>
      <button id="lang-toggle" style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.5);background:none;border:none;cursor:pointer;padding:4px 2px;min-height:32px;">EN / ES</button>
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; Home</a>
  </div>
  <h1 id="sr-heading" style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">Speculation Radar</h1>
  <p id="sr-desc" style="font-size:0.82rem;color:#94a3b8;margin-bottom:8px;line-height:1.6;">
    One LLC taking the deed on {MIN_BUILDINGS} or more buildings in the same ZIP within {RADAR_WINDOW_DAYS} days. Concentrated buying like that is a position being assembled, not a one-off purchase, and it usually shows up months before anything changes on the block. Public records only.
  </p>
  <p id="sr-sub" style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.55);margin-bottom:28px;">{n} buying runs detected across NYC in the past {RADAR_WINDOW_DAYS} days.</p>
  <ul class="radar-list">
{rows_html}  </ul>
  <p id="sr-note" style="font-size:0.72rem;color:rgba(148,163,184,0.45);margin-top:24px;line-height:1.6;">
    Buying several buildings is not wrongdoing. This page reports the public-record pattern, not a conclusion about any buyer. <a href="/methodology" style="color:rgba(249,115,22,0.75);">How this is measured &rarr;</a>
  </p>
</div>
<footer>
  <div style="font-size:11px;color:#64748b;margin-bottom:8px;text-align:center;">Built by Michael Espin</div>
  <div class="footer-links">
    <a href="/" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Home</a>
    <a href="/methodology" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Methodology</a>
    <a href="/about" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">About</a>
    <a href="/status" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Status</a>
    <a href="mailto:nycdisplacement@gmail.com" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Contact</a>
    <a href="https://x.com/PulseCities" target="_blank" rel="noopener noreferrer" aria-label="PulseCities on X" style="color:#64748b;text-decoration:none;display:inline-flex;align-items:center;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" focusable="false"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg></a>
  </div>
</footer>
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  var i18n = {{
    en: {{
      heading: 'Speculation Radar',
      desc: 'One LLC taking the deed on {MIN_BUILDINGS} or more buildings in the same ZIP within {RADAR_WINDOW_DAYS} days. Concentrated buying like that is a position being assembled, not a one-off purchase, and it usually shows up months before anything changes on the block. Public records only.',
      sub: '{n} buying runs detected across NYC in the past {RADAR_WINDOW_DAYS} days.',
      note: 'Buying several buildings is not wrongdoing. This page reports the public-record pattern, not a conclusion about any buyer.',
      deeds: 'Deeds',
      buildings: 'buildings',
      toggle: 'EN / ES'
    }},
    es: {{
      heading: 'Radar de especulaci\\u00f3n',
      desc: 'Una LLC que toma la escritura de {MIN_BUILDINGS} o m\\u00e1s edificios en el mismo c\\u00f3digo postal en un plazo de {RADAR_WINDOW_DAYS} d\\u00edas. Una compra tan concentrada es una posici\\u00f3n en formaci\\u00f3n, no una compra aislada, y suele aparecer meses antes de que algo cambie en la cuadra. Solo registros p\\u00fablicos.',
      sub: '{n} rachas de compra detectadas en NYC en los \\u00faltimos {RADAR_WINDOW_DAYS} d\\u00edas.',
      note: 'Comprar varios edificios no es una infracci\\u00f3n. Esta p\\u00e1gina informa el patr\\u00f3n de registro p\\u00fablico, no una conclusi\\u00f3n sobre ning\\u00fan comprador.',
      deeds: 'Escrituras',
      buildings: 'edificios',
      toggle: 'ES / EN'
    }}
  }};
  function applyLang(l) {{
    var s = i18n[l] || i18n.en;
    var set = function(id, val) {{ var el = document.getElementById(id); if (el) el.textContent = val; }};
    set('sr-heading', s.heading);
    set('sr-sub', s.sub);
    var d = document.getElementById('sr-desc'); if (d) d.textContent = s.desc;
    var note = document.getElementById('sr-note');
    if (note) note.innerHTML = s.note + ' <a href="/methodology" style="color:rgba(249,115,22,0.75);">' + (l === 'es' ? 'C\\u00f3mo se mide \\u2192' : 'How this is measured \\u2192') + '</a>';
    document.querySelectorAll('.radar-when-label').forEach(function(el) {{ el.textContent = s.deeds; }});
    document.querySelectorAll('.radar-count-label').forEach(function(el) {{ el.textContent = s.buildings; }});
    var btn = document.getElementById('lang-toggle');
    if (btn) btn.textContent = s.toggle;
  }}
  applyLang(lang);
  var btn = document.getElementById('lang-toggle');
  if (btn) btn.addEventListener('click', function() {{
    lang = lang === 'en' ? 'es' : 'en';
    localStorage.setItem('pc-lang', lang);
    applyLang(lang);
  }});
}})();
</script>
</body>
</html>"""

    _radar_cache = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


_this_week_cache: tuple[str, float] | None = None  # cleared on restart


@router.get("/this-week", include_in_schema=False)
def this_week_page(db: Session = Depends(get_db)):
    """This week in NYC displacement — a standing weekly review.

    One canonical URL that always shows the current week: score movers,
    fresh public-record counts, and the newest flips. Computed from the
    same queries as the map and digest, cached for an hour.
    """
    global _this_week_cache
    if _this_week_cache and time.monotonic() < _this_week_cache[1]:
        return HTMLResponse(_this_week_cache[0])

    today = date.today()
    week_ago = today - timedelta(days=7)
    range_label = f"{week_ago.strftime('%b %-d')} to {today.strftime('%b %-d, %Y')}"

    movers = db.execute(text("""
        WITH now_s AS (
            SELECT DISTINCT ON (zip_code) zip_code, composite_score AS s
            FROM score_history ORDER BY zip_code, scored_at DESC
        ),
        then_s AS (
            SELECT DISTINCT ON (zip_code) zip_code, composite_score AS s
            FROM score_history
            WHERE scored_at <= :week_ago
            ORDER BY zip_code, scored_at DESC
        )
        SELECT n.zip_code, nb.name, nb.borough,
               ROUND(now_s.s::numeric, 1) AS score,
               ROUND((now_s.s - then_s.s)::numeric, 1) AS delta
        FROM now_s
        JOIN then_s ON then_s.zip_code = now_s.zip_code
        JOIN neighborhoods nb ON nb.zip_code = now_s.zip_code
        CROSS JOIN LATERAL (SELECT now_s.zip_code) n
        WHERE now_s.s - then_s.s >= 0.5
        ORDER BY (now_s.s - then_s.s) DESC
        LIMIT 5
    """), {"week_ago": week_ago}).fetchall()

    counts = db.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM evictions_raw
             WHERE executed_date >= CURRENT_DATE - INTERVAL '7 days')  AS evictions,
            (SELECT COUNT(*) FROM permits_raw
             WHERE filing_date >= CURRENT_DATE - INTERVAL '7 days')    AS permits,
            (SELECT COUNT(*) FROM complaints_raw
             WHERE created_date >= CURRENT_DATE - INTERVAL '7 days')   AS complaints,
            (SELECT COUNT(*) FROM violations_raw
             WHERE inspection_date >= CURRENT_DATE - INTERVAL '7 days') AS violations
    """)).fetchone()

    from api.routes.flips import query_flips
    flips = sorted(
        query_flips(db),
        key=lambda f: f.get("transfer_date") or "",
        reverse=True,
    )[:3]

    e = _html.escape

    movers_html = ""
    for m in movers:
        color = "#ef4444" if m.delta >= 5 else "#f97316"
        movers_html += (
            f'<li class="tw-row" onclick="location.href=\'/neighborhood/{e(m.zip_code)}\'">'
            f'<a href="/neighborhood/{e(m.zip_code)}">'
            f'<div class="tw-main"><div class="tw-name">{e(m.zip_code)} '
            f'<span class="tw-sub">{e(m.name or "")}{", " + e(m.borough) if m.borough else ""}</span></div></div>'
            f'<div class="tw-side"><span class="tw-delta" style="color:{color};">{float(m.delta):+.1f}</span>'
            f'<span class="tw-score">now {m.score}</span></div>'
            f'</a></li>\n'
        )
    if not movers_html:
        movers_html = '<li class="tw-empty">No neighborhood moved a half point or more this week.</li>'

    flips_html = ""
    for f in flips:
        addr = e(f["address"])
        flips_html += (
            f'<li class="tw-row" onclick="location.href=\'/property/{e(str(f["bbl"]))}\'">'
            f'<a href="/property/{e(str(f["bbl"]))}">'
            f'<div class="tw-main"><div class="tw-name">{addr} '
            f'<span class="tw-sub">{e(f["neighborhood"] or str(f["zip_code"]))}</span></div></div>'
            f'<div class="tw-side"><span class="tw-delta" style="color:#f97316;">+{f["days_between"]}d</span>'
            f'<span class="tw-score">buy &rarr; permit</span></div>'
            f'</a></li>\n'
        )
    if not flips_html:
        flips_html = '<li class="tw-empty">No new flips matched the pattern this week.</li>'

    stat_cells = "".join(
        f'<div class="tw-stat"><div class="tw-stat-n">{v:,}</div><div class="tw-stat-l" id="tw-stat-{key}">{label}</div></div>'
        for v, label, key in [
            (counts.evictions,  "eviction filings",       "evictions"),
            (counts.permits,    "construction permits",   "permits"),
            (counts.violations, "HPD violations",         "violations"),
            (counts.complaints, "311 housing complaints", "complaints"),
        ]
    )

    top_line = (
        f"{movers[0].name or movers[0].zip_code} rose {movers[0].delta} points"
        if movers else "No major score moves"
    )
    title = "This week in NYC displacement | PulseCities"
    desc = (
        f"NYC displacement week in review, {range_label}: {top_line}, "
        f"{counts.evictions:,} eviction filings, {counts.permits:,} construction permits. Public records only."
    )

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{e(title)}</title>
<meta name="description" content="{e(desc)}">
<link rel="canonical" href="https://pulsecities.com/this-week">
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="{e(desc)}">
<meta property="og:url" content="https://pulsecities.com/this-week">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{e(title)}">
<meta name="twitter:description" content="{e(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<link rel="icon" href="/favicon.ico" sizes="32x32">
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
h2{{font-size:0.78rem;font-weight:600;color:rgba(148,163,184,0.75);text-transform:uppercase;letter-spacing:0.1em;margin:32px 0 4px}}
.tw-list{{list-style:none;padding:0;margin:0}}
.tw-row{{border-bottom:1px solid rgba(148,163,184,0.07);cursor:pointer}}
.tw-row:hover{{background:rgba(148,163,184,0.04)}}
.tw-row a{{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:13px 0}}
.tw-name{{font-family:'JetBrains Mono',monospace;font-size:0.88rem;color:#e2e8f0;font-weight:500}}
.tw-row:hover .tw-name{{color:#f97316}}
.tw-sub{{font-family:'DM Sans',sans-serif;font-size:0.76rem;color:rgba(148,163,184,0.7);font-weight:400;margin-left:6px}}
.tw-side{{display:flex;flex-direction:column;align-items:flex-end;flex-shrink:0}}
.tw-delta{{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:500;line-height:1.1}}
.tw-score{{font-size:0.68rem;color:rgba(148,163,184,0.55)}}
.tw-empty{{padding:18px 0;font-size:0.8rem;color:#94a3b8}}
.tw-stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:12px}}
.tw-stat{{background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;padding:16px}}
.tw-stat-n{{font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:600;color:#f1f5f9}}
.tw-stat-l{{font-size:0.72rem;color:#94a3b8;margin-top:4px}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
</style>
</head>
<body>
<nav>
  <div class="nav-inner">
    <a href="/" style="display:flex;align-items:center;gap:8px;color:#f1f5f9;">
      <svg width="22" height="22" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <span style="font-size:0.85rem;color:rgba(148,163,184,0.6);">PulseCities</span>
    </a>
    <div style="display:flex;align-items:center;gap:16px;">
      <a href="/map" style="font-size:0.78rem;color:rgba(148,163,184,0.5);">Map</a>
      <a href="/flips" style="font-size:0.78rem;color:rgba(148,163,184,0.5);">Flip Watch</a>
      <a href="/operators" style="font-size:0.78rem;color:rgba(148,163,184,0.5);">Operators</a>
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; Home</a>
  </div>
  <h1 id="tw-heading" style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;font-weight:600;margin-bottom:6px;">This week in NYC displacement</h1>
  <p style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.55);margin-bottom:4px;">{e(range_label)}</p>
  <p id="tw-intro" style="font-size:0.82rem;color:#94a3b8;margin-bottom:8px;line-height:1.6;">
    The week's movement across all NYC neighborhoods, from the same public records that drive the map. This page always shows the current week.
  </p>

  <h2 id="tw-movers-h">Score movers</h2>
  <p id="tw-movers-sub" style="font-size:0.75rem;color:rgba(148,163,184,0.6);margin-bottom:4px;">Largest displacement-pressure increases over the past 7 days.</p>
  <ul class="tw-list">
{movers_html}  </ul>

  <h2 id="tw-records-h">New on the record</h2>
  <p id="tw-records-sub" style="font-size:0.75rem;color:rgba(148,163,184,0.6);">Citywide filings dated within the past 7 days.</p>
  <div class="tw-stats">{stat_cells}</div>

  <h2 id="tw-flips-h">Newest flips</h2>
  <p style="font-size:0.75rem;color:rgba(148,163,184,0.6);margin-bottom:4px;">LLC bought, then filed to renovate. <a href="/flips" style="color:rgba(249,115,22,0.75);">Full feed &rarr;</a></p>
  <ul class="tw-list">
{flips_html}  </ul>

  <p style="font-size:0.72rem;color:rgba(148,163,184,0.45);margin-top:28px;line-height:1.6;">
    <span id="tw-note">Counts reflect records published by NYC agencies, which can lag the events they describe. Scores are risk indicators, not claims of wrongdoing.</span> <a id="tw-meth-link" href="/methodology" style="color:rgba(249,115,22,0.75);">How scores work &rarr;</a>
  </p>
</div>
<footer>
  <div style="font-size:11px;color:#64748b;margin-bottom:8px;text-align:center;">Built by Michael Espin</div>
  <div class="footer-links">
    <a href="/" style="color:#64748b;">Home</a>
    <a href="/methodology" style="color:#64748b;">Methodology</a>
    <a href="/about" style="color:#64748b;">About</a>
    <a href="/status" style="color:#64748b;">Status</a>
    <a href="mailto:nycdisplacement@gmail.com" style="color:#64748b;">Contact</a>
  </div>
</footer>
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  if (lang !== 'es') return;
  var es = {{
    'tw-heading':     'Esta semana en el desplazamiento de NYC',
    'tw-intro':       'El movimiento de la semana en todos los vecindarios de NYC, con los mismos registros p\u00fablicos que alimentan el mapa. Esta p\u00e1gina siempre muestra la semana actual.',
    'tw-movers-h':    'Cambios de puntuaci\u00f3n',
    'tw-movers-sub':  'Mayores aumentos de presi\u00f3n de desplazamiento en los \u00faltimos 7 d\u00edas.',
    'tw-records-h':   'Nuevo en el registro',
    'tw-records-sub': 'Registros de toda la ciudad con fecha en los \u00faltimos 7 d\u00edas.',
    'tw-flips-h':     'Flips m\u00e1s recientes',
    'tw-note':        'Los conteos reflejan registros publicados por agencias de NYC, que pueden retrasarse respecto a los hechos. Las puntuaciones son indicadores de riesgo, no acusaciones.',
    'tw-meth-link':   'C\u00f3mo funcionan las puntuaciones \u2192',
    'tw-stat-evictions':  'desalojos presentados',
    'tw-stat-permits':    'permisos de construcci\u00f3n',
    'tw-stat-violations': 'violaciones HPD',
    'tw-stat-complaints': 'quejas de vivienda al 311'
  }};
  Object.keys(es).forEach(function(id) {{
    var el = document.getElementById(id);
    if (el) el.textContent = es[id];
  }});
  document.documentElement.lang = 'es';
}})();
</script>
</body>
</html>"""

    _this_week_cache = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)
