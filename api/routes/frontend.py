"""
Server-side rendered pages for neighborhood and operator deep links, plus the
operators directory.

GET /neighborhood/{zip_code}  — full SSR civic intelligence card (score, signals, FAQ, CTA)
GET /operator/{root}          — per-operator OG/meta injection into operator.html
GET /operators                — server-side rendered directory of all tracked operators
"""

import collections
import html as _html
import json
import logging
import math
import re
import time
from datetime import date, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.permit_kinds import (DECONVERSION_PARAMS, deconversion_sql,
                              renovation_sql)
from api.freshness import (ACRIS_THROUGH_SQL, FRESHNESS_SOURCES, db_through_sql,
                           feed_anchor, real_date, window_sql)
from config.nyc import DISPLACEMENT_COMPLAINT_TYPES
from models.database import get_db
from scoring.tiers import tier

logger = logging.getLogger(__name__)
router = APIRouter(tags=["frontend"])

_FRONTEND = Path(__file__).parent.parent.parent / "frontend"
_app_html: str | None = None
_operator_html: str | None = None
_page_cache: dict[str, tuple[str, float]] = {}   # zip -> (html, expires_at)
_op_page_cache: dict[str, tuple[str, float]] = {}  # root -> (html, expires_at)
_prop_page_cache: dict[str, tuple[str, float]] = {}  # bbl -> (html, expires_at)
_PAGE_TTL = 3600

# Plausible analytics, injected into every SSR page head right after the JSON-LD
# block. The server-rendered pages build their own <head> and were previously
# untracked, so the highest-intent pages (neighborhood, flips, radar, operators)
# reported nothing. Interpolated as {_PLAUSIBLE}; the braces are literal here
# because this is a plain string, not an f-string.
_PLAUSIBLE = (
    '\n<script async src="https://plausible.io/js/pa-U5kR6cdEChGa28HrQF_3J.js"></script>'
    '\n<script>window.plausible=window.plausible||function(){(plausible.q=plausible.q||[])'
    '.push(arguments)},plausible.init=plausible.init||function(i){plausible.o=i||{}};'
    'plausible.init()</script>'
)


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


# Corporate-form tokens have to stand alone: "INC" as a substring matched
# "PRINCE", "TRUST" matched "AS TRUSTEE". Servicers, nominees, and referees
# take title in foreclosure rather than buying, so they are not linked or
# indexed as buyers.
_ENTITY_FORM_RE = re.compile(r"\b(LLC|PLLC|LLP|CORP|INC|LTD|LP)\b")
_NOT_A_BUYER_RE = re.compile(
    r"\b(TRUSTEE|NOMINEE|REFEREE|SERVICING)\b|NOT IN ITS INDIVIDUAL|AS NOMINEE FOR"
)

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
    "are elevated at the ZIP level. Each signal is normalized across all 177 NYC ZIP codes so "
    "dense areas are not scored by raw counts alone."
)
_FAQ_Q2 = "What public records are included?"
_FAQ_A2 = (
    "PulseCities uses NYC public records: DOB building permits, HPD housing violations, "
    "311 housing complaints, executed evictions, ACRIS property deed transfers, HPD "
    "building registrations, and MapPLUTO residential unit counts."
)
_FAQ_Q3 = "Is this a prediction of eviction?"
_FAQ_A3 = (
    "No. PulseCities does not predict individual evictions and is not legal advice. "
    "The score shows neighborhood-level public-record indicators that may be worth reviewing."
)

# Spanish FAQ, terminology consistent with the client-side dicts in app.html
# and index.html (vecindario, desalojo, puntuación, registros públicos).
_FAQS = {
    "en": [(_FAQ_Q1, _FAQ_A1), (_FAQ_Q2, _FAQ_A2), (_FAQ_Q3, _FAQ_A3)],
    "es": [
        ("¿Qué significa esta puntuación de desplazamiento?",
         "La puntuación es un índice de 0 a 100 que muestra dónde se elevan varias señales "
         "de desplazamiento en registros públicos a nivel de código postal. Cada señal se "
         "normaliza entre los 177 códigos postales de NYC, así que las zonas densas no se "
         "puntúan solo por conteos brutos."),
        ("¿Qué registros públicos se incluyen?",
         "PulseCities usa registros públicos de NYC: permisos de construcción de DOB, "
         "violaciones de vivienda de HPD, quejas de vivienda al 311, casos de desalojo, "
         "transferencias de escrituras de ACRIS, registros de edificios de HPD y "
         "conteos de unidades residenciales de MapPLUTO."),
        ("¿Es esto una predicción de desalojo?",
         "No. PulseCities no predice desalojos individuales y no es asesoría legal. La "
         "puntuación muestra indicadores de registros públicos a nivel de vecindario que "
         "pueden merecer revisión."),
    ],
}

_ES_MONTHS = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
              "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]


def _long_date(d: date, lang: str) -> str:
    if lang == "es":
        return f"{d.day} de {_ES_MONTHS[d.month - 1]} de {d.year}"
    return d.strftime("%B %-d, %Y")


def _month_year(d, lang: str) -> str:
    if lang == "es":
        return f"{_ES_MONTHS[d.month - 1]} de {d.year}"
    return d.strftime("%B %Y")


# Display words for the score tier. _tier_info stays the internal canonical
# (English) label; these are presentation only. Spanish adjectives agree with
# "presión" (feminine).
_TIER_WORDS = {
    "en": {"Critical": "CRITICAL", "High": "HIGH", "Moderate": "MODERATE",
           "Low": "LOW", "Unknown": "UNKNOWN"},
    "es": {"Critical": "CRÍTICA", "High": "ALTA", "Moderate": "MODERADA",
           "Low": "BAJA", "Unknown": "DESCONOCIDA"},
}



def _jsonld(obj) -> str:
    """JSON for a <script type="application/ld+json"> block. Escapes < so a
    public-record string containing </script> can't break out of the element."""
    return json.dumps(obj, indent=2).replace("<", "\\u003c")


def _crumbs(*pairs) -> dict:
    """A schema.org BreadcrumbList from (name, path) pairs, for embedding in a
    page's JSON-LD @graph."""
    return {
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": i, "name": n, "item": f"https://pulsecities.com{p}"}
            for i, (n, p) in enumerate(pairs, 1)
        ],
    }


_MONTHS_SHORT = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _acris_through(db) -> date | None:
    """Newest deed date actually in hand, via the canonical freshness query."""
    try:
        return db.execute(text(ACRIS_THROUGH_SQL)).scalar()
    except Exception:
        logger.warning("acris through-date query failed", exc_info=True)
        return None


# Source and as-of date for every feed, cached because the answers move once a
# night and the alternative is five extra queries on the page that takes 88% of
# organic traffic.
_FEEDS_TTL = 900
_feeds_cache: tuple[dict, float] | None = None

# The name to print for each feed. The slug is api.freshness's, so a feed added
# there and not named here is caught by tests/test_source_attribution.py rather
# than silently losing its citation.
_FEED_SOURCE = {
    "acris":      "NYC ACRIS",
    "evictions":  "NYC marshal eviction records",
    "permits":    "NYC DOB job filings",
    "complaints": "NYC 311",
    "violations": "NYC HPD violations",
}


def _feeds_through(db) -> dict:
    """As-of date per feed, so any block of facts can say when it was current."""
    global _feeds_cache
    now = time.monotonic()
    if _feeds_cache and now < _feeds_cache[1]:
        return _feeds_cache[0]
    out = {}
    for slug, _scraper, _table, _col, _days in FRESHNESS_SOURCES:
        try:
            v = db.execute(text(db_through_sql(slug))).scalar()
        except Exception:
            logger.warning("through-date query failed for %s", slug, exc_info=True)
            continue
        if v is not None:
            out[slug] = v.date() if hasattr(v, "date") else v
    _feeds_cache = (out, now + _FEEDS_TTL)
    return out


def _cite(feeds: dict, slug: str) -> str:
    """The clause that turns a number on this page into a quotable fact.

    Every block already named its source. None of them said when the source was
    last current, so a figure lifted off the page travelled as a bare claim.
    That matters more than it used to: over the fifteen days to 2026-08-27 the
    AI crawlers fetched 82,631 pages here against Googlebot's 68,881, and an
    assistant repeating "42 violations" with no date attached is how a number
    outlives the record it came from. Reads the same date /api/status publishes,
    so the page and the status endpoint cannot disagree.
    """
    d = feeds.get(slug)
    src = _FEED_SOURCE.get(slug)
    if not d or not src:
        return ""
    return f' <span class="cite">Source: {src}, current through {_en_date(d)}.</span>'


def _deeds_through_line(db, lang: str = "en") -> str:
    """One sentence naming where the deed record actually stops.

    /flips and /radar are built entirely on ACRIS, whose data ends whenever the
    city last published. On 2026-08-18 that gap was eighteen days: /radar said
    "detected across NYC in the past 90 days" over a window whose final eighteen
    days held no deeds at all, and nothing on the page said so.

    This line was the first half of the fix and disclosure was never the whole
    of it. As of 2026-08-28 both windows also *end* at the last published deed
    rather than at CURRENT_DATE, so the ninety days are ninety days of data.
    Radar went from 4 clusters to 11 on that change alone. The line stays
    because a reader still needs to know which ninety days.

    Same shape as the /evictions through-line, and the same rule as the
    homepage LLC chip: never let a page imply coverage the query does not have.
    Returns "" when the date is unavailable rather than inventing one.
    """
    through = _acris_through(db)
    if not through:
        return ""
    if lang == "es":
        return (f"Escrituras registradas hasta el {through.day} de "
                f"{_ES_MONTHS_LONG[through.month]} de {through.year}, el "
                f"último día publicado por la ciudad.")
    return (f"Deeds recorded through {_en_date(through)}, the most recent "
            f"day the city has published.")


_ES_MONTHS_LONG = ["", "enero", "febrero", "marzo", "abril", "mayo", "junio",
                   "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]


# Tier colours for the dark page. The bands themselves live in scoring.tiers;
# only the palette is local, because each surface renders the same tier
# differently on purpose (see that module).
_TIER_COLORS = {
    "Critical": "#e4483b",
    "High":     "#ed6317",
    "Moderate": "#C08B2D",
    "Low":      "#93a1ad",
}


def _tier_info(score: float) -> tuple[str, str]:
    """Returns (display_label, hex_color) for the score tier."""
    label = tier(score)
    return label, _TIER_COLORS[label]


def _idx_color(v: float) -> str:
    if v >= 70: return "#ed6317"
    if v >= 45: return "#C08B2D"
    return "#93a1ad"


# One footer for every SSR page, same link set as the static pages.
# test_footer_consistency.py fails the suite if the two drift apart.
# Interpolate as {_FOOTER_HTML} inside the page f-strings.
_FOOTER_HTML = """<footer>
  <div style="font-size:12px;color:#93a1ad;margin-bottom:8px;text-align:center;">Built by Michael Espin</div>
  <div class="footer-links">
    <a href="/" style="color:#93a1ad;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'">Home</a>
    <a href="/neighborhoods" style="color:#93a1ad;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'">Neighborhoods</a>
    <a href="/displacement" style="color:#93a1ad;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'">Displacement</a>
    <a href="/methodology" style="color:#93a1ad;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'">Methodology</a>
    <a href="/about" style="color:#93a1ad;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'">About</a>
    <a href="/press" style="color:#93a1ad;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'">Press</a>
    <a href="/status" style="color:#93a1ad;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'">Status</a>
    <a href="mailto:nycdisplacement@gmail.com" style="color:#93a1ad;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'">Contact</a>
    <a href="https://www.linkedin.com/in/michaelespin/" target="_blank" rel="noopener noreferrer" style="color:#93a1ad;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'">LinkedIn</a>
    <a href="https://x.com/PulseCities" target="_blank" rel="noopener noreferrer" aria-label="PulseCities on X" style="color:#93a1ad;text-decoration:none;display:inline-flex;align-items:center;" onmouseover="this.style.color='#e4e8ec'" onmouseout="this.style.color='#93a1ad'"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" focusable="false"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg></a>
  </div>
</footer>"""

_FOOTERS = {
    "en": _FOOTER_HTML,
    "es": (_FOOTER_HTML
           .replace('>Home<', '>Inicio<')
           .replace('>Neighborhoods<', '>Vecindarios<')
           .replace('>Displacement<', '>Desplazamiento<')
           .replace('>Methodology<', '>Metodología<')
           .replace('>About<', '>Acerca de<')
           .replace('>Press<', '>Prensa<')
           .replace('>Status<', '>Estado<')
           .replace('>Contact<', '>Contacto<')
           .replace('Built by Michael Espin', 'Creado por Michael Espin')),
}

# One top nav for every SSR page. Before this each page hand-rolled its own
# <nav> and they disagreed on which hub links to show; none but the homepage
# surfaced /displacement or /this-week. test_ssr_nav.py fails the suite if a
# page's top nav drops a hub link. Bilingual pages pass lang + a server-toggle
# anchor as toggle_html; English pages pass the JS EN/ES button
# (_LANG_TOGGLE_BTN) or nothing. The links wrap on narrow widths.
_SSR_NAV_ITEMS = [
    ("/map", "map"),
    ("/displacement", "displacement"),
    ("/neighborhoods", "neighborhoods"),
    ("/operators", "operators"),
    ("/evictions", "evictions"),
    ("/flips", "flips"),
    ("/radar", "radar"),
    ("/this-week", "this_week"),
    ("/methodology", "methodology"),
]

_SSR_NAV_LABELS = {
    "en": {"map": "Map", "displacement": "Displacement", "neighborhoods": "Neighborhoods",
           "operators": "Operators", "evictions": "Evictions", "flips": "Flips",
           "radar": "Radar", "this_week": "This week", "methodology": "Methodology"},
    "es": {"map": "Mapa", "displacement": "Desplazamiento", "neighborhoods": "Vecindarios",
           "operators": "Operadores", "evictions": "Desalojos", "flips": "Flips",
           "radar": "Radar", "this_week": "Esta semana", "methodology": "Metodología"},
}

_LANG_TOGGLE_BTN = (
    '<button id="lang-toggle" style="font-family:\'JetBrains Mono\',monospace;'
    'font-size:0.75rem;color:var(--faint);background:none;border:none;'
    'cursor:pointer;padding:4px 2px;min-height:32px;">EN / ES</button>'
)

_SSR_NAV_BRAND = (
    '<a href="/" style="display:flex;align-items:center;gap:8px;color:#eef2f5;flex-shrink:0;">'
    '<svg width="22" height="22" viewBox="0 0 32 32" fill="none" aria-hidden="true">'
    '<rect width="32" height="32" rx="6" fill="#1a1a2e"/>'
    '<polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" '
    'stroke="#ed6317" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>'
    '<span style="font-size:0.85rem;color:var(--dim);">PulseCities</span></a>'
)


def _ssr_nav(active: str = "", lang: str = "en", toggle_html: str = "", track: bool = False) -> str:
    """Shared SSR top nav with the full hub set. `active` is the current page's
    path (its link is brightened + marked aria-current). `toggle_html` is
    appended after the links (JS button for EN pages, server anchor for the
    bilingual pages). `track` adds the /displacement Showcase Nav plausible
    events on each outbound link."""
    labels = _SSR_NAV_LABELS.get(lang, _SSR_NAV_LABELS["en"])
    parts = []
    for path, key in _SSR_NAV_ITEMS:
        label = labels[key]
        if path == active:
            parts.append(
                f'<a href="{path}" aria-current="page" '
                f'style="font-size:0.78rem;color:#c9d2da;">{label}</a>'
            )
            continue
        onclick = (f" onclick=\"plausible('Showcase Nav',{{props:{{to:'{key.replace('_', '-')}'}}}})\""
                   if track else "")
        parts.append(
            f'<a href="{path}"{onclick} style="font-size:0.78rem;color:var(--muted);" '
            f'onmouseover="this.style.color=\'#e4e8ec\'" '
            f'onmouseout="this.style.color=\'#93a1ad\'">{label}</a>'
        )
    links = "".join(parts)
    # One row at every width: the links scroll horizontally instead of
    # wrapping (nine hub links stack into a wall on phones and zoomed
    # desktops otherwise). Brand and language toggle stay pinned outside the
    # scroll area. Pages hide the scrollbar via .nav-inner>div::-webkit-scrollbar.
    return (
        '<nav>\n  <div class="nav-inner">\n    '
        + _SSR_NAV_BRAND
        + '\n    <div style="display:flex;align-items:center;gap:14px;min-width:0;'
        'overflow-x:auto;flex-wrap:nowrap;white-space:nowrap;scrollbar-width:none;'
        '-webkit-overflow-scrolling:touch;">'
        + links
        + '</div>\n    '
        + (f'<div style="flex-shrink:0;">{toggle_html}</div>' if toggle_html else '')
        + '\n  </div>\n</nav>\n'
        + '<script>(function(){var a=document.querySelector(\'nav [aria-current="page"]\');'
          'if(!a)return;var r=a.parentElement;if(r.scrollWidth<=r.clientWidth)return;'
          'r.scrollLeft=Math.max(0,a.offsetLeft-16);})();</script>'
    )


# Neighborhood-page copy, both languages. Data (names, numbers, dates) flows
# in via format slots; everything a reader sees as prose lives here.
_NB_L = {
    "en": {
        "title_scored": "{name} ({zip}) Displacement Score {s}/100 | PulseCities",
        "social_scored": "{name} ({zip}) | Displacement Score {s}/100 | PulseCities",
        "title_unscored": "{name} ({zip}) NYC Displacement Signals | PulseCities",
        "desc_scored": ("{name} shows {tier} displacement-pressure signals based on NYC public records, "
                        "including LLC acquisitions, executed evictions, 311 complaints, HPD violations, "
                        "permits, and rent-stabilized housing data."),
        "desc_unscored": ("Track displacement-pressure signals in {name} ({zip}) from NYC public records: "
                          "LLC acquisitions, executed evictions, 311 complaints, HPD violations, permits, "
                          "and rent-stabilized housing data."),
        "nav": [("/map", "Map"), ("/methodology", "Methodology"), ("/about", "About"), ("/press", "Press")],
        "back_map": "&#8592; Back to map",
        "all_borough": "All {borough} ZIPs",
        "kicker": "Displacement signals",
        "h1": "{name} {zip}",
        "updated": "{borough}. Updated {date}.",
        "updated_recently": "recently",
        "tier_line": "{tier} DISPLACEMENT PRESSURE",
        "no_score": "Score data not yet available.",
        "trend_h": "Score trend",
        "trend_sub": "Daily composite score, past {n} days. Change over the window: ",
        "trend_flat": "flat",
        "signals_h": "Signal breakdown",
        "signals_sub": "Public-record signals used in the neighborhood score.",
        "th": ("Signal", "Count", "Index"),
        "sig_labels": {
            "llc_acquisitions": "LLC property acquisitions",
            "permits": "Building permits (residential, 3+ units)",
            "evictions": "Executed residential evictions",
            "hpd_violations": "HPD violations (Class B+C)",
            "complaint_rate": "311 housing complaints",
            "rs_unit_loss": "Rent-stabilized unit loss",
        },
        "win_365": "past 365 days", "win_90": "past 90 days", "win_annual": "annual comparison",
        "rs_none": "No annual loss recorded in current data",
        "signals_note": ("All counts from NYC public records. Index values are normalized across "
                         "177 NYC ZIP codes. Data is refreshed nightly."),
        "pet_h": "Early warning: housing-court petitions",
        "pet_sub": ("Residential eviction cases filed in housing court for {zip}. Filings lead "
                    "executed evictions by months, so rising volume is the earliest public signal available."),
        "pet_stat": "petitions filed {window}",
        "pet_vs": "vs the prior three months ({n} filed)",
        "pet_note": ("Source: NYS Office of Court Administration via the OCA Data Collective "
                     "(Housing Data Coalition), CC BY-NC-SA. The extract is ZIP-level by design and "
                     "does not identify tenants or buildings. Shown for context only; not part of "
                     "the composite score."),
        "vac_h": "Vacated by city order",
        "vac_sub": ("Buildings in {zip} that HPD has ordered vacated in the past 12 months. A vacate "
                    "order is displacement already carried out: the city has ordered residents out of "
                    "the building."),
        "vac_one": "building", "vac_many": "buildings",
        "vac_orders": " across {n} orders", "vac_latest": ", most recent {date}",
        "vac_note": ("Source: HPD housing maintenance code violations, class I informational orders, "
                     "via NYC Open Data. Shown for context only; not part of the composite score."),
        "faq_h": "About this data",
        "embed_h": "Embed this score",
        "embed_sub": ("A live badge for articles and community pages. It stays current as the score "
                      "changes and links back to this page."),
        "embed_alt": "PulseCities displacement score badge for {name} ({zip})",
        "embed_aria": "Embed code",
        "embed_btn": "Copy embed code",
        "meth_link": "Read the methodology &#8594;",
        "cta_map": "Open {zip} on the map &#8594;",
        "cta_copy": "Copy link", "cta_brief": "Evidence brief",
        "copied": "Copied!",
        "watch_h": "Watch this block",
        "watch_sub": ("Get a one-page email whenever the public record for this neighborhood moves: "
                      "deeds, evictions, permits, violations. Quiet weeks send nothing."),
        "watch_placeholder": "you@email.com",
        "watch_btn": "Watch",
        "watch_ok": "You're watching this neighborhood. We'll email you when the record moves.",
        "watch_dupe": "You're already watching this neighborhood.",
        "watch_invalid": "Enter a valid email address.",
        "watch_err": "Something went wrong. Please try again.",
        "flip_h": "Recent renovation flips",
        "flip_sub": ("Buildings in {zip} where an LLC took the deed and filed a major renovation "
                     "permit within 60 days. A fast buy-to-permit turn is an early sign of repositioning."),
        "flip_th": ("Building", "Bought", "To permit"),
        "flip_days": "{n}d",
        "flip_note": ("Source: ACRIS deeds and DOB permits via NYC Open Data, past 365 days. "
                      "Shown for context; not part of the composite score."),
        "ops_h": "Operators active in {name}",
        "ops_sub": "Ownership networks with public-record deed activity in {zip}, most active here first.",
        "ops_meta": "{n} in {zip}, {t} citywide",
        "nearby_h": "More {borough} neighborhoods",
        "nearby_sub": "Other {borough} ZIP codes ranked by displacement-pressure score.",
        "disp_cta": "See the citywide displacement picture →",
        "lang_toggle_label": "ES", "lang_toggle_aria": "Ver esta página en español",
    },
    "es": {
        "title_scored": "{name} ({zip}) Puntuación de desplazamiento {s}/100 | PulseCities",
        "social_scored": "{name} ({zip}) | Puntuación de desplazamiento {s}/100 | PulseCities",
        "title_unscored": "{name} ({zip}) Señales de desplazamiento en NYC | PulseCities",
        "desc_scored": ("{name} muestra señales de presión de desplazamiento {tier} según registros "
                        "públicos de NYC, incluyendo adquisiciones LLC, casos de desalojo, quejas al "
                        "311, violaciones HPD, permisos y datos de renta estabilizada."),
        "desc_unscored": ("Sigue las señales de presión de desplazamiento en {name} ({zip}) desde "
                          "registros públicos de NYC: adquisiciones LLC, casos de desalojo, quejas al "
                          "311, violaciones HPD, permisos y datos de renta estabilizada."),
        "nav": [("/map", "Mapa"), ("/methodology", "Metodología"), ("/about", "Acerca de"), ("/press", "Prensa")],
        "back_map": "&#8592; Volver al mapa",
        "all_borough": "Todos los ZIP de {borough}",
        "kicker": "Señales de desplazamiento",
        "h1": "{name} {zip}",
        "updated": "{borough}. Actualizado el {date}.",
        "updated_recently": "recientemente",
        "tier_line": "PRESIÓN DE DESPLAZAMIENTO {tier}",
        "no_score": "La puntuación aún no está disponible.",
        "trend_h": "Tendencia de la puntuación",
        "trend_sub": "Puntuación compuesta diaria, últimos {n} días. Cambio en el período: ",
        "trend_flat": "sin cambio",
        "signals_h": "Desglose de señales",
        "signals_sub": "Señales de registros públicos usadas en la puntuación del vecindario.",
        "th": ("Señal", "Conteo", "Índice"),
        "sig_labels": {
            "llc_acquisitions": "Adquisiciones de propiedades por LLC",
            "permits": "Permisos de construcción (residencial, 3+ unidades)",
            "evictions": "Desalojos residenciales",
            "hpd_violations": "Violaciones HPD (Clase B+C)",
            "complaint_rate": "Quejas de vivienda al 311",
            "rs_unit_loss": "Pérdida de unidades de renta estabilizada",
        },
        "win_365": "últimos 365 días", "win_90": "últimos 90 días", "win_annual": "comparación anual",
        "rs_none": "Sin pérdida anual registrada en los datos actuales",
        "signals_note": ("Todos los conteos provienen de registros públicos de NYC. Los valores del "
                         "índice se normalizan entre 177 códigos postales de NYC. Los datos se "
                         "actualizan cada noche."),
        "pet_h": "Alerta temprana: peticiones en la corte de vivienda",
        "pet_sub": ("Casos de desalojo residencial presentados en la corte de vivienda para {zip}. "
                    "Las presentaciones anticipan por meses a los desalojos ejecutados, así que un "
                    "volumen creciente es la señal pública más temprana disponible."),
        "pet_stat": "peticiones presentadas {window}",
        "pet_vs": "vs los tres meses anteriores ({n} presentadas)",
        "pet_note": ("Fuente: NYS Office of Court Administration vía el OCA Data Collective (Housing "
                     "Data Coalition), CC BY-NC-SA. El extracto es a nivel de código postal por diseño "
                     "y no identifica inquilinos ni edificios. Solo para contexto; no forma parte de "
                     "la puntuación compuesta."),
        "vac_h": "Desalojados por orden de la ciudad",
        "vac_sub": ("Edificios en {zip} que HPD ordenó desalojar en los últimos 12 meses. Una orden "
                    "de desalojo del edificio es desplazamiento ya ejecutado: la ciudad ordenó la "
                    "salida de los residentes."),
        "vac_one": "edificio", "vac_many": "edificios",
        "vac_orders": " en {n} órdenes", "vac_latest": ", la más reciente en {date}",
        "vac_note": ("Fuente: violaciones del código de mantenimiento de vivienda de HPD, órdenes "
                     "informativas clase I, vía NYC Open Data. Solo para contexto; no forma parte de "
                     "la puntuación compuesta."),
        "faq_h": "Sobre estos datos",
        "embed_h": "Inserta esta puntuación",
        "embed_sub": ("Una insignia en vivo para artículos y páginas comunitarias. Se mantiene al día "
                      "cuando cambia la puntuación y enlaza de vuelta a esta página."),
        "embed_alt": "Insignia de puntuación de desplazamiento de PulseCities para {name} ({zip})",
        "embed_aria": "Código para insertar",
        "embed_btn": "Copiar código",
        "meth_link": "Lee la metodología &#8594;",
        "cta_map": "Abrir {zip} en el mapa &#8594;",
        "cta_copy": "Copiar enlace", "cta_brief": "Expediente de evidencia",
        "copied": "¡Copiado!",
        "watch_h": "Observa esta zona",
        "watch_sub": ("Recibe un correo de una página cuando el registro público de este vecindario "
                      "cambie: escrituras, desalojos, permisos, violaciones. Las semanas tranquilas "
                      "no envían nada."),
        "watch_placeholder": "tu@correo.com",
        "watch_btn": "Observar",
        "watch_ok": "Estás observando este vecindario. Te avisaremos cuando el registro cambie.",
        "watch_dupe": "Ya estás observando este vecindario.",
        "watch_invalid": "Ingresa un correo electrónico válido.",
        "watch_err": "Algo salió mal. Inténtalo de nuevo.",
        "flip_h": "Reformas y reventas recientes",
        "flip_sub": ("Edificios en {zip} donde una LLC tomó la escritura y presentó un permiso de "
                     "renovación mayor dentro de 60 días. Un giro rápido de compra a permiso es una "
                     "señal temprana de reposicionamiento."),
        "flip_th": ("Edificio", "Comprado", "Al permiso"),
        "flip_days": "{n}d",
        "flip_note": ("Fuente: escrituras ACRIS y permisos DOB vía NYC Open Data, últimos 365 días. "
                      "Mostrado como contexto; no forma parte de la puntuación compuesta."),
        "ops_h": "Operadores activos en {name}",
        "ops_sub": "Redes de propiedad con actividad de escrituras en {zip} en registros públicos, primero las más activas aquí.",
        "ops_meta": "{n} en {zip}, {t} en total",
        "nearby_h": "Más vecindarios de {borough}",
        "nearby_sub": "Otros códigos postales de {borough} por puntuación de presión de desplazamiento.",
        "disp_cta": "Ver el panorama de desplazamiento de toda la ciudad →",
        "lang_toggle_label": "EN", "lang_toggle_aria": "View this page in English",
    },
}


def _trend_svg(history: list[tuple[str, float]]) -> str:
    """
    Inline SVG of the daily composite score. Server-rendered so the page
    stays static and the trace shows up in reader modes and print.
    Returns "" when there is not enough history to draw an honest line.
    """
    if not history or len(history) < 7:
        return ""

    scores = [s for _, s in history]
    raw_lo = max(0.0, min(scores) - 2.0)
    raw_hi = min(100.0, max(scores) + 2.0)
    # Snap the axis to a human step so gridlines read 65/70/75, never 67/71/75.
    span = raw_hi - raw_lo
    step = next((s for s in (1, 2, 5, 10, 20) if span <= s * 4), 25)
    lo = max(0.0, math.floor(raw_lo / step) * step)
    hi = min(100.0, math.ceil(raw_hi / step) * step)
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
    val = lo
    while val <= hi + 1e-9:
        gy = py_t + (1 - (val - lo) / rng) * plot_h
        grid += (
            f'<line x1="{px_l}" y1="{gy:.1f}" x2="{px_l + plot_w}" y2="{gy:.1f}" '
            f'stroke="rgba(147,161,173,.12)" stroke-width="1"/>'
            f'<text x="{px_l + 2}" y="{gy - 4:.1f}" font-size="10" '
            f'font-family="JetBrains Mono,monospace" fill="rgba(147,161,173,.45)">{val:.0f}</text>'
        )
        val += step

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
        f'<polygon points="{area}" fill="rgba(237,99,23,.07)"/>'
        f'<polyline points="{line}" fill="none" stroke="#ed6317" stroke-width="1.6" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'<circle cx="{last_x}" cy="{last_y}" r="3" fill="#ed6317"/>'
        f'<text x="{px_l}" y="{h - 6:.0f}" font-size="10" font-family="JetBrains Mono,monospace" '
        f'fill="rgba(147,161,173,.45)">{first_lbl}</text>'
        f'<text x="{px_l + plot_w:.1f}" y="{h - 6:.0f}" font-size="10" text-anchor="end" '
        f'font-family="JetBrains Mono,monospace" fill="rgba(147,161,173,.45)">{last_lbl}</text>'
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
    petitions: dict | None = None,
    vacates: dict | None = None,
    flips: list | None = None,
    operators_here: list | None = None,
    nearby: list | None = None,
    ev_count: int = 0,
    lang: str = "en",
) -> str:
    e = _html.escape
    L = _NB_L.get(lang, _NB_L["en"])

    borough_disp = borough or "New York City"
    base_url     = f"https://pulsecities.com/neighborhood/{zip_code}"
    canonical    = base_url if lang == "en" else f"{base_url}?lang=es"
    og_image     = f"https://pulsecities.com/og/{zip_code}.png"

    if last_updated:
        try:
            updated_disp = _long_date(date.fromisoformat(last_updated), lang)
        except ValueError:
            updated_disp = last_updated
    else:
        updated_disp = L["updated_recently"]

    if score is not None:
        tier_label, tier_color = _tier_info(score)
        score_str    = f"{score:.1f}"
        page_title   = L["title_scored"].format(name=name, zip=zip_code, borough=borough_disp, s=score_str)
        social_title = L["social_scored"].format(name=name, zip=zip_code, borough=borough_disp, s=score_str)
        meta_desc    = L["desc_scored"].format(name=name, zip=zip_code,
                                               tier=_TIER_WORDS[lang][tier_label].lower())
    else:
        tier_label, tier_color = "Unknown", "#93a1ad"
        score_str    = "N/A"
        page_title   = L["title_unscored"].format(name=name, zip=zip_code, borough=borough_disp)
        social_title = page_title
        meta_desc    = L["desc_unscored"].format(name=name, zip=zip_code)

    # (breakdown_key, window_label, raw_count or None for dormant signals)
    _signals = [
        ("llc_acquisitions", L["win_365"],    raw_counts.get("llc_acquisitions", 0)),
        ("permits",          L["win_365"],    raw_counts.get("permits", 0)),
        ("evictions",        L["win_365"],    raw_counts.get("evictions", 0)),
        ("hpd_violations",   L["win_90"],     raw_hpd),
        ("complaint_rate",   L["win_365"],    raw_counts.get("complaint_rate", 0)),
        ("rs_unit_loss",     L["win_annual"], None),
    ]

    rows_html = ""
    for key, window, count in _signals:
        label = L["sig_labels"][key]
        idx   = breakdown.get(key)
        idx_s = f"{idx:.1f}" if idx is not None else "&mdash;"
        i_col = _idx_color(float(idx) if idx is not None else 0.0)
        if key == "rs_unit_loss":
            cnt_s = L["rs_none"]
            c_col = "#818c97"
        elif count == 0:
            cnt_s = "0"
            c_col = "#818c97"
        else:
            cnt_s = f"{count:,}"
            c_col = "#eef2f5"
        rows_html += (
            f'<tr>'
            f'<td class="sc">{e(label)}<span class="sw">{e(window)}</span></td>'
            f'<td class="sr" style="color:{c_col};">{cnt_s}</td>'
            f'<td class="si" style="color:{i_col};">{idx_s}</td>'
            f'</tr>'
        )

    tier_line = L["tier_line"].format(tier=_TIER_WORDS[lang][tier_label])
    score_block = (
        f'<div class="score-block">'
        f'<span class="score-num" style="color:{tier_color};">{score_str}</span>'
        f'<span class="score-denom">/100</span>'
        f'<span class="score-tier" style="color:{tier_color};">{tier_line}</span>'
        f'</div>'
        if score is not None
        else f'<div class="score-block"><p style="color:var(--faint);font-size:0.9rem;">{L["no_score"]}</p></div>'
    )
    summary_html = f'<p class="summary">{e(summary)}</p>' if summary else ""

    trend_section = ""
    svg = _trend_svg(history or [])
    if svg:
        n_days = len(history)
        delta = history[-1][1] - history[0][1]
        delta_s = f"{delta:+.1f}" if abs(delta) >= 0.05 else L["trend_flat"]
        trend_section = (
            f'<section style="margin-bottom:32px;">'
            f'<h2>{L["trend_h"]}</h2>'
            f'<p class="section-sub">{L["trend_sub"].format(n=n_days)}'
            f'<span style="font-family:\'JetBrains Mono\',monospace;color:#eef2f5;">{delta_s}</span></p>'
            f'<div style="border:1px solid var(--border);border-radius:8px;padding:14px 12px 8px;background:rgba(255,255,255,.02);">{svg}</div>'
            f'</section>'
        )

    dataset_ld = _jsonld({
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"Displacement Signals: {name} ({zip_code}), {borough_disp}, NYC",
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

    _bslug = (borough_disp or "").lower().replace(" ", "-")
    breadcrumb_ld = _jsonld({
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://pulsecities.com/"},
            *([{"@type": "ListItem", "position": 2, "name": borough_disp,
                "item": f"https://pulsecities.com/{_bslug}"}]
              if _bslug in ("brooklyn", "manhattan", "queens", "bronx", "staten-island") else []),
            {"@type": "ListItem", "position": 3, "name": f"{name} ({zip_code})", "item": canonical},
        ],
    })

    faqs = _FAQS.get(lang, _FAQS["en"])
    faq_ld = _jsonld({
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "inLanguage": lang,
        "mainEntity": [
            {"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": a}}
            for q, a in faqs
        ],
    })
    faq_html = "".join(
        f'<div class="faq-item"><p class="faq-q">{e(q)}</p><p class="faq-a">{e(a)}</p></div>'
        for q, a in faqs
    )

    breadcrumb_borough = (
        f' &middot; <a href="/{_bslug}">{L["all_borough"].format(borough=e(borough_disp))}</a>'
        if _bslug in ("brooklyn", "manhattan", "queens", "bronx", "staten-island") else ""
    )

    embed_code = (
        f'<a href="https://pulsecities.com/neighborhood/{zip_code}">'
        f'<img src="https://pulsecities.com/badge/{zip_code}.svg" '
        f'alt="PulseCities displacement score for {name} ({zip_code})" '
        f'width="320" height="64"></a>'
    )

    # Housing-court petition volumes, ZIP-level by design (the OCA extract
    # is de-identified). Display-only: the CC BY-NC-SA license keeps this
    # out of the composite score and off every API surface.
    petitions_section = ""
    if petitions and petitions.get("recent"):
        pct = ""
        if petitions.get("prior"):
            change = (petitions["recent"] - petitions["prior"]) / petitions["prior"] * 100
            arrow_color = "#e4483b" if change >= 10 else ("#6fa287" if change <= -10 else "var(--muted)")
            prior_s = f"{petitions['prior']:,}"
            pct = (f' <span style="font-family:\'JetBrains Mono\',monospace;color:{arrow_color};">'
                   f'{change:+.0f}%</span> <span style="color:var(--faint);">'
                   f'{L["pet_vs"].format(n=prior_s)}</span>')
        petitions_section = f"""  <section style="margin-bottom:32px;">
    <h2>{L["pet_h"]}</h2>
    <p class="section-sub">{L["pet_sub"].format(zip=zip_code)}</p>
    <p style="font-size:.95rem;margin-bottom:8px;"><span style="font-family:'JetBrains Mono',monospace;font-size:1.3rem;font-weight:600;">{petitions["recent"]:,}</span> <span style="color:var(--muted);">{L["pet_stat"].format(window=e(petitions["window"]))}</span>{pct}</p>
    <p class="data-note">{L["pet_note"]}</p>
  </section>
"""

    # Buildings vacated by HPD order: not a leading indicator like petitions,
    # but displacement already executed and on the record. Rendered only when
    # the past year has any; most ZIPs stay quiet.
    vacates_section = ""
    if vacates and vacates.get("buildings"):
        n_b, n_o = vacates["buildings"], vacates["orders"]
        latest = vacates.get("latest")
        latest_txt = L["vac_latest"].format(date=_month_year(latest, lang)) if latest else ""
        orders_txt = L["vac_orders"].format(n=n_o) if n_o > n_b else ""
        noun = L["vac_many"] if n_b != 1 else L["vac_one"]
        vacates_section = f"""  <section style="margin-bottom:32px;">
    <h2>{L["vac_h"]}</h2>
    <p class="section-sub">{L["vac_sub"].format(zip=zip_code)}</p>
    <p style="font-size:.95rem;margin-bottom:8px;"><span style="font-family:'JetBrains Mono',monospace;font-size:1.3rem;font-weight:600;">{n_b:,}</span> <span style="color:var(--muted);">{noun}{orders_txt}{latest_txt}</span></p>
    <p class="data-note">{L["vac_note"]}</p>
  </section>
"""

    # Recent renovation flips in this ZIP. Renders only when there are matches,
    # so quiet neighborhoods do not get a thin, near-empty section.
    flips_section = ""
    if flips:
        flip_items = ""
        for f in flips:
            bought = _month_year(date.fromisoformat(f["transfer_date"]), lang) if f.get("transfer_date") else ""
            days = f.get("days_between")
            gap = L["flip_days"].format(n=days) if days is not None else ""
            flip_items += (
                # The address is an anchor, not just an onclick target. This was
                # the one row on the site navigating by JavaScript alone, and it
                # sits on all 177 neighborhood hubs: a crawler following links
                # from a hub to its buildings found none here, and neither did
                # anyone using a keyboard.
                f'<tr onclick="location.href=\'/property/{e(str(f["bbl"]))}\'" style="cursor:pointer;">'
                f'<td class="sc"><a href="/property/{e(str(f["bbl"]))}">{e(f["address"])}</a>'
                f'<span class="sw">{e(f.get("buyer") or "")}</span></td>'
                f'<td class="sr">{bought}</td>'
                f'<td class="si">{gap}</td></tr>'
            )
        flips_section = f"""  <section style="margin-bottom:32px;">
    <h2>{L["flip_h"]}</h2>
    <p class="section-sub">{L["flip_sub"].format(zip=zip_code)}</p>
    <div class="table-wrap"><table>
      <thead><tr><th>{L["flip_th"][0]}</th><th>{L["flip_th"][1]}</th><th>{L["flip_th"][2]}</th></tr></thead>
      <tbody>{flip_items}</tbody>
    </table></div>
    <p class="data-note">{L["flip_note"]}</p>
  </section>
"""

    # Lateral internal links. Operators with deed activity in this ZIP and the
    # borough's other neighborhoods deepen the crawl graph and give a reader
    # somewhere to go next. Each renders only when it has rows.
    operators_section = ""
    if operators_here:
        op_items = ""
        for op in operators_here:
            meta = L["ops_meta"].format(n=op["local"], zip=zip_code, t=op["total"])
            op_items += (
                '<li class="lat-row">'
                f'<a href="/operator/{e(op["slug"])}">{e(op["name"])}</a>'
                f'<span class="lat-meta">{e(meta)}</span></li>'
            )
        operators_section = f"""  <section style="margin-bottom:32px;">
    <h2>{L["ops_h"].format(name=e(name))}</h2>
    <p class="section-sub">{L["ops_sub"].format(zip=zip_code)}</p>
    <ul class="lat-list">{op_items}</ul>
  </section>
"""

    nearby_section = ""
    if nearby:
        nb_items = ""
        for nb in nearby:
            _tl, tier_color = _tier_info(nb["score"])
            nb_items += (
                '<li class="lat-row">'
                # Three ZIPs are called Bushwick; the name alone rendered the
                # same neighborhood twice with two scores.
                f'<a href="/neighborhood/{e(nb["zip"])}">{e(nb["name"])} {e(nb["zip"])}</a>'
                f'<span class="lat-score" style="color:{tier_color};">{nb["score"]:.0f}</span></li>'
            )
        nearby_section = f"""  <section style="margin-bottom:32px;">
    <h2>{L["nearby_h"].format(borough=e(borough_disp))}</h2>
    <p class="section-sub">{L["nearby_sub"].format(borough=e(borough_disp))}</p>
    <ul class="lat-list">{nb_items}</ul>
  </section>
"""

    # The eviction page for this place. The score summarises five signals; a
    # reader who came for the eviction number wants the addresses, and this is
    # the only path to them from here. Bilingual, and rendered only when that
    # page clears its own index floor so the link never points at a noindex.
    ev_area_section = ""
    if name:
        ev_slug = _ev_area_slug(name)
        ev_n = ev_count
        if ev_slug and ev_n >= _EV_AREA_MIN:
            ev_h = ("Evictions in " + name if lang != "es"
                    else "Desalojos en " + name)
            ev_body = (
                f"City marshals have executed {ev_n:,} residential evictions in "
                f"{e(name)}. The eviction page lists them by address, with who "
                f"holds the deed on the buildings where they repeat."
                if lang != "es" else
                f"Los alguaciles han ejecutado {ev_n:,} desalojos residenciales en "
                f"{e(name)}. La p&aacute;gina de desalojos los enumera por "
                f"direcci&oacute;n."
            )
            ev_cta = ("See the address-level record &rarr;" if lang != "es"
                      else "Ver el registro por direcci&oacute;n &rarr;")
            ev_area_section = f"""  <section style="margin-bottom:32px;">
    <h2>{e(ev_h)}</h2>
    <p class="section-sub">{ev_body}</p>
    <p class="disp-cta"><a href="/evictions/{e(ev_slug)}">{ev_cta}</a></p>
  </section>
"""

    # Always-valid link to the flagship citywide page, from every neighborhood.
    disp_cta_html = (
        f'  <p class="disp-cta"><a href="/displacement">{L["disp_cta"]}</a></p>\n'
    )

    # The alternate-language URL for the toggle and hreflang pair. English is
    # the parameterless canonical form; Spanish lives at ?lang=es.
    alt_url = f"{base_url}?lang=es" if lang == "en" else base_url
    nav_toggle = (
        f'<a href="{e(alt_url)}" id="lang-toggle" aria-label="{L["lang_toggle_aria"]}" '
        f'style="font-size:0.78rem;color:var(--faint);" '
        f'onmouseover="this.style.color=\'#e4e8ec\'" '
        f'onmouseout="this.style.color=\'#93a1ad\'">{L["lang_toggle_label"]}</a>'
    )

    # Watch-this-block CTA: subscribes the reader's email to this ZIP straight
    # from the organic landing page, closing the search -> view -> watch funnel.
    # Copy is bilingual (L[...]); JS strings go through json.dumps so quotes and
    # the ZIP interpolate safely into the inline script.
    watch_card = (
        '<section class="watch-card">'
        f'<h2 class="watch-h">{L["watch_h"]}</h2>'
        f'<p class="section-sub">{L["watch_sub"]}</p>'
        '<div class="watch-row">'
        f'<input id="watch-email" type="email" inputmode="email" autocomplete="email" '
        f'placeholder="{e(L["watch_placeholder"])}" aria-label="{e(L["watch_h"])}">'
        f'<button id="watch-btn" class="btn-map" type="button">{L["watch_btn"]}</button>'
        '</div>'
        '<p id="watch-msg" class="watch-msg" aria-live="polite" style="display:none;"></p>'
        '</section>'
    )
    _j = json.dumps
    watch_js = (
        "<script>(function(){"
        "var b=document.getElementById('watch-btn'),m=document.getElementById('watch-msg'),"
        "el=document.getElementById('watch-email');"
        "if(!b)return;"
        "function show(t,ok){m.textContent=t;m.style.color=ok?'#6fa287':'#e4483b';m.style.display='block';}"
        "async function go(){var v=(el.value||'').trim();"
        "if(!/^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$/.test(v)){show(" + _j(L["watch_invalid"]) + ",false);return;}"
        "b.disabled=true;b.textContent='\\u2026';"
        "try{var r=await fetch('/api/subscribe',{method:'POST',"
        "headers:{'Content-Type':'application/json'},"
        "body:JSON.stringify({email:v,zip_code:" + _j(zip_code) + "})});"
        "if(r.ok){plausible('Subscribe',{props:{zip_code:" + _j(zip_code) + "}});"
        "plausible('Neighborhood Watch Submit');"
        "document.querySelector('.watch-row').style.display='none';"
        "show(" + _j(L["watch_ok"]) + ",true);}"
        "else if(r.status===409){show(" + _j(L["watch_dupe"]) + ",true);"
        "b.disabled=false;b.textContent=" + _j(L["watch_btn"]) + ";}"
        "else{throw new Error();}}"
        "catch(err){show(" + _j(L["watch_err"]) + ",false);"
        "b.disabled=false;b.textContent=" + _j(L["watch_btn"]) + ";}}"
        "b.addEventListener('click',go);"
        "el.addEventListener('keydown',function(ev){if(ev.key==='Enter')go();});"
        "})();</script>"
    )

    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<meta name="robots" content="index, follow">
<title>{e(page_title)}</title>
<meta name="description" content="{e(meta_desc)}">
<link rel="canonical" href="{e(canonical)}">
<link rel="alternate" hreflang="en" href="{e(base_url)}">
<link rel="alternate" hreflang="es" href="{e(base_url)}?lang=es">
<link rel="alternate" hreflang="x-default" href="{e(base_url)}">
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
<script type="application/ld+json">{breadcrumb_ld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
:root{{--bg:#111823;--border:rgba(147,161,173,.1);--text:#eef2f5;--muted:#93a1ad;--dim:#8a97a2;--faint:#818c97;--accent:#ed6317}}
body{{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;line-height:1.6;overflow-x:hidden}}
a{{color:inherit;text-decoration:none}}
nav{{border-bottom:1px solid var(--border);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.nav-links a{{font-size:.78rem;color:var(--muted);margin-left:16px;transition:color .15s}}
.nav-links a:hover{{color:var(--text)}}
.container{{max-width:720px;margin:0 auto;padding:32px 20px 80px}}
.breadcrumb{{font-size:.78rem;color:var(--muted);margin-bottom:20px}}
.breadcrumb a{{color:var(--muted)}}
.breadcrumb a:hover{{color:var(--text)}}
h1{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.45rem;font-weight:600;line-height:1.3;margin-bottom:6px}}
.kicker{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;letter-spacing:0.18em;color:#ed6317;text-transform:uppercase;margin-bottom:8px}}
.subline{{font-size:.82rem;color:var(--muted);margin-bottom:28px}}
.score-block{{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap;padding:20px 24px;background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:8px;margin-bottom:20px}}
.score-num{{font-size:2.8rem;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1}}
.score-denom{{font-size:1rem;color:var(--muted);font-family:'JetBrains Mono',monospace;align-self:flex-end;padding-bottom:4px}}
.score-tier{{font-size:0.75rem;font-weight:600;letter-spacing:.08em;align-self:flex-end;padding-bottom:6px;margin-left:8px}}
.summary{{font-size:.92rem;color:var(--muted);line-height:1.7;margin-bottom:32px}}
h2{{font-size:0.75rem;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--faint);margin-bottom:8px}}
.section-sub{{font-size:.82rem;color:var(--muted);margin-bottom:14px}}
.table-wrap{{overflow-x:auto;margin-bottom:12px}}
table{{width:100%;border-collapse:collapse}}
th{{font-size:0.75rem;font-weight:500;text-transform:uppercase;letter-spacing:.06em;color:var(--faint);padding:6px 0;border-bottom:1px solid var(--border)}}
th:not(:first-child){{text-align:right}}
td{{padding:12px 0;border-bottom:1px solid rgba(147,161,173,.06);vertical-align:top}}
.sc{{font-size:.87rem}}
.sw{{display:block;font-size:0.75rem;color:var(--faint);margin-top:2px}}
.sr,.si{{font-size:.87rem;font-family:'JetBrains Mono',monospace;text-align:right;white-space:nowrap}}
.data-note{{font-size:0.75rem;color:var(--faint);margin-top:10px;margin-bottom:36px;line-height:1.55}}
.faq-list{{margin-bottom:36px}}
.faq-item{{padding:16px 0;border-bottom:1px solid var(--border)}}
.faq-item:first-child{{border-top:1px solid var(--border)}}
.faq-q{{font-size:.88rem;font-weight:600;margin-bottom:6px}}
.faq-a{{font-size:.83rem;color:var(--muted);line-height:1.65}}
.lat-list{{list-style:none;padding:0;margin:0}}
.lat-row{{display:flex;justify-content:space-between;gap:12px;align-items:baseline;padding:10px 0;border-bottom:1px solid var(--border)}}
.lat-row a{{color:var(--text);font-weight:500;font-size:.9rem}}
.lat-row a:hover{{color:var(--accent)}}
.lat-meta{{color:var(--muted);font-size:.78rem;white-space:nowrap}}
.lat-score{{font-family:'JetBrains Mono',monospace;font-size:.82rem;font-weight:600;white-space:nowrap}}
.disp-cta{{text-align:center;margin:8px 0 28px}}
.disp-cta a{{color:var(--accent);font-size:.9rem}}
.disp-cta a:hover{{text-decoration:underline}}
.meth-link{{font-size:.82rem;margin-bottom:28px}}
.meth-link a{{color:var(--accent)}}
.meth-link a:hover{{text-decoration:underline}}
.cta-row{{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-top:4px}}
.btn-map{{display:inline-flex;align-items:center;gap:6px;padding:10px 20px;background:var(--accent);color:#fff;border-radius:6px;font-size:.85rem;font-weight:500;transition:opacity .15s}}
.btn-map:hover{{opacity:.88}}
.btn-copy{{display:inline-flex;align-items:center;gap:6px;padding:10px 20px;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:.85rem;cursor:pointer;font-family:inherit;transition:color .15s,border-color .15s}}
.btn-copy:hover{{color:var(--text);border-color:rgba(147,161,173,.3)}}
.watch-card{{background:rgba(237,99,23,.05);border:1px solid rgba(237,99,23,.22);border-radius:10px;padding:20px 22px;margin-bottom:32px}}
.watch-h{{color:var(--accent);font-size:.95rem;font-weight:600;text-transform:none;letter-spacing:0;margin-bottom:6px}}
.watch-row{{display:flex;gap:10px;margin-top:12px;flex-wrap:wrap}}
.watch-row input{{flex:1;min-width:180px;background:rgba(255,255,255,.03);border:1px solid var(--border);border-radius:6px;padding:10px 12px;color:var(--text);font-family:inherit;font-size:.85rem}}
.watch-row input:focus{{outline:none;border-color:var(--accent)}}
.watch-row .btn-map{{border:none;cursor:pointer;font-family:inherit}}
.watch-msg{{font-size:.8rem;margin-top:10px;line-height:1.5}}
footer{{border-top:1px solid var(--border);padding:24px 20px calc(env(safe-area-inset-bottom) + 24px);text-align:center}}
.footer-links{{max-width:720px;margin:0 auto;display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
.footer-links a{{font-size:.75rem;color:var(--faint)}}
.footer-links a:hover{{color:var(--muted)}}
@media(max-width:600px){{h1{{font-size:1.2rem}}.score-num{{font-size:2.2rem}}.container{{padding:24px 16px 60px}}.cta-row{{flex-direction:column;align-items:flex-start}}}}
</style>
</head>
<body>
{_ssr_nav("/neighborhoods", lang=lang, toggle_html=nav_toggle)}
<main><div class="container">
  <p class="breadcrumb"><a href="/map">{L['back_map']}</a>{breadcrumb_borough}</p>
  <div class="kicker">{L['kicker']}</div>
  <h1>{L['h1'].format(name=e(name), zip=zip_code)}</h1>
  <p class="subline">{L['updated'].format(borough=e(borough_disp), date=e(updated_disp))}</p>
  {score_block}
  {summary_html}
  {watch_card}
  {trend_section}
  <section style="margin-bottom:32px;">
    <h2>{L['signals_h']}</h2>
    <p class="section-sub">{L['signals_sub']}</p>
    <div class="table-wrap">
    <table>
      <thead><tr><th>{L['th'][0]}</th><th>{L['th'][1]}</th><th>{L['th'][2]}</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    </div>
    <p class="data-note">{L['signals_note']}</p>
  </section>
{petitions_section}{vacates_section}{flips_section}{ev_area_section}{operators_section}{nearby_section}  <section style="margin-bottom:32px;">
    <h2>{L['faq_h']}</h2>
    <div class="faq-list">
      {faq_html}
    </div>
  </section>
  <section style="margin-bottom:32px;">
    <h2>{L['embed_h']}</h2>
    <p class="section-sub">{L['embed_sub']}</p>
    <p style="margin-bottom:12px;"><img src="/badge/{zip_code}.svg" alt="{L['embed_alt'].format(name=e(name), zip=zip_code)}" width="320" height="64" style="display:block;"></p>
    <textarea id="embed-code" readonly rows="3" aria-label="{L['embed_aria']}" style="width:100%;max-width:560px;background:var(--surface);color:var(--muted);border:1px solid var(--border);border-radius:8px;padding:10px 12px;font-size:12px;font-family:SFMono-Regular,Menlo,Consolas,monospace;line-height:1.5;resize:none;">{e(embed_code)}</textarea>
    <p style="margin-top:8px;"><button class="btn-copy" id="copy-embed-btn" onclick="copyEmbed()">{L['embed_btn']}</button></p>
  </section>
{disp_cta_html}  <p class="meth-link"><a href="/methodology">{L['meth_link']}</a></p>
  <div class="cta-row">
    <a href="/map?q={zip_code}" class="btn-map">{L['cta_map'].format(zip=zip_code)}</a>
    <button class="btn-copy" id="copy-btn" onclick="copyLink()">{L['cta_copy']}</button>
    <a href="/brief/zip/{zip_code}" class="btn-copy">{L['cta_brief']}</a>
  </div>
</div></main>
{_FOOTERS.get(lang, _FOOTER_HTML)}
{watch_js}
<script>
// The toggle remembers the choice; pages honor a stored 'es' on arrival so a
// Spanish reader stays in Spanish while sharing links that default to English.
(function() {{
  var toggle = document.getElementById('lang-toggle');
  if (toggle) toggle.addEventListener('click', function() {{
    try {{ localStorage.setItem('pc-lang', '{lang}' === 'en' ? 'es' : 'en'); }} catch (err) {{}}
  }});
  if ('{lang}' === 'en' && location.search.indexOf('lang=') === -1) {{
    try {{
      if (localStorage.getItem('pc-lang') === 'es') location.replace('{base_url}?lang=es');
    }} catch (err) {{}}
  }}
}})();
function copyLink() {{
  var url = 'https://pulsecities.com/neighborhood/{zip_code}';
  var btn = document.getElementById('copy-btn');
  function onDone() {{
    btn.textContent = '{L['copied']}';
    setTimeout(function() {{ btn.textContent = '{L['cta_copy']}'; }}, 2000);
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
    btn.textContent = '{L['copied']}';
    setTimeout(function() {{ btn.textContent = '{L['embed_btn']}'; }}, 2000);
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
def neighborhood_page(zip_code: str, lang: str = "en", db: Session = Depends(get_db)):
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        return _not_found()

    # English is the parameterless canonical; anything that isn't exactly
    # ?lang=es renders English.
    lang = "es" if lang == "es" else "en"

    cache_key = f"{zip_code}:{lang}"
    cached = _page_cache.get(cache_key)
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

    summary = _build_summary(score, breakdown, raw_counts, lang=lang)

    history_rows = db.execute(text("""
        SELECT scored_at, composite_score
        FROM score_history
        WHERE zip_code = :zip
          AND scored_at >= CURRENT_DATE - INTERVAL '180 days'
        ORDER BY scored_at ASC
    """), {"zip": zip_code}).fetchall()
    history = [(r.scored_at.isoformat(), round(float(r.composite_score), 1)) for r in history_rows]

    # OCA petition volumes: newest three complete months in the extract vs
    # the three calendar months before them. Months with zero filings have
    # no table row, so the six-month window is generated over the calendar
    # (anchored on the ZIP's newest complete month) and missing months count
    # as zero; otherwise a gap month would silently stretch the comparison span.
    petitions = None
    pet_rows = db.execute(text("""
        WITH anchor AS (
            SELECT max(month) AS m FROM oca_petitions_monthly
            WHERE zip_code = :zip AND month < date_trunc('month', CURRENT_DATE)
        )
        SELECT gs.month::date AS month, COALESCE(SUM(o.filings), 0) AS n
        FROM anchor,
             generate_series(anchor.m - interval '5 months', anchor.m,
                             interval '1 month') AS gs(month)
        LEFT JOIN oca_petitions_monthly o
               ON o.zip_code = :zip AND o.month = gs.month::date
        WHERE anchor.m IS NOT NULL
        GROUP BY gs.month ORDER BY gs.month DESC
    """), {"zip": zip_code}).fetchall()
    if pet_rows:
        recent = sum(int(r.n) for r in pet_rows[:3])
        prior = sum(int(r.n) for r in pet_rows[3:6]) if len(pet_rows) > 3 else None
        newest, oldest_recent = pet_rows[0].month, pet_rows[min(2, len(pet_rows) - 1)].month
        # The window string lands inside translated copy, so it carries its
        # own language rather than shipping "May to Jul" into a Spanish page.
        if lang == "es":
            window = (
                f"de {_ES_MONTHS_LONG[oldest_recent.month]} a "
                f"{_ES_MONTHS_LONG[newest.month]} de {newest.year}"
                if oldest_recent != newest else
                f"en {_ES_MONTHS_LONG[newest.month]} de {newest.year}"
            )
        else:
            window = (
                f"{oldest_recent.strftime('%b')} to {newest.strftime('%b %Y')}"
                if oldest_recent != newest else newest.strftime("%b %Y")
            )
        petitions = {"recent": recent, "prior": prior, "window": window}

    # Buildings vacated by HPD order in the past year. Class-I violations
    # ingest as of 2026-07-11; display-only, never part of the composite.
    vacates = None
    vac_row = db.execute(text("""
        SELECT COUNT(DISTINCT bbl) AS buildings, COUNT(*) AS orders,
               MAX(COALESCE(nov_issued_date, inspection_date)) AS latest
        FROM violations_raw
        WHERE zip_code = :zip AND violation_class = 'I'
          AND description ILIKE '%VACATE%'
          AND COALESCE(nov_issued_date, inspection_date) >= CURRENT_DATE - INTERVAL '365 days'
    """), {"zip": zip_code}).fetchone()
    if vac_row and vac_row.buildings:
        vacates = {"buildings": int(vac_row.buildings), "orders": int(vac_row.orders),
                   "latest": vac_row.latest}

    # Recent renovation flips in this ZIP: LLC deed transfer followed by an A1/A2
    # permit on the same lot within 60 days, past 365 days. Same pattern as the
    # citywide /flips feed, scoped to the neighborhood. Unique, indexable content
    # that also seeds internal links to /property; renders only when non-empty, so
    # quiet ZIPs stay lean rather than becoming thin pages.
    flip_rows = db.execute(text(f"""
        WITH llc_transfers AS (
            SELECT o.bbl, o.doc_date AS transfer_date,
                   o.party_name_normalized AS buyer, o.doc_amount, p.address
            FROM ownership_raw o
            JOIN parcels p ON p.bbl = o.bbl
            WHERE o.party_name_normalized LIKE '%LLC%'
              AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
              AND o.party_type = '2'
              -- Window ends at the last published deed, not at the calendar.
              -- ACRIS lags weeks; see api.freshness.window_sql.
              AND {window_sql('o.doc_date', 365)}
              AND p.zip_code = :zip
        ),
        reno_permits AS (
            SELECT bbl, MIN(filing_date) AS first_permit_date
            FROM permits_raw
            WHERE """ + renovation_sql() + """
              AND filing_date >= CURRENT_DATE - INTERVAL '365 days'
              AND zip_code = :zip
            GROUP BY bbl
        )
        SELECT DISTINCT ON (l.bbl) l.bbl, l.address, l.buyer, l.doc_amount,
               l.transfer_date, r.first_permit_date,
               (r.first_permit_date - l.transfer_date) AS days_between
        FROM llc_transfers l
        JOIN reno_permits r ON r.bbl = l.bbl
        WHERE r.first_permit_date > l.transfer_date
          AND (r.first_permit_date - l.transfer_date) <= 60
        ORDER BY l.bbl, l.transfer_date DESC
    """), {"zip": zip_code, "anchor": feed_anchor(db)}).fetchall()
    flips = [
        {
            "bbl": r.bbl,
            "address": r.address or f"BBL {r.bbl}",
            "buyer": r.buyer,
            "transfer_date": r.transfer_date.isoformat() if r.transfer_date else None,
            "days_between": (r.days_between.days if hasattr(r.days_between, "days")
                             else int(r.days_between)) if r.days_between is not None else None,
        }
        for r in flip_rows
    ]
    flips.sort(key=lambda f: f["transfer_date"] or "", reverse=True)
    flips = flips[:6]

    # Lateral internal links: operators with deed activity in this ZIP, and the
    # other neighborhoods in this borough by score. Both deepen the crawl graph
    # and give a reader somewhere to go; each renders only when non-empty.
    from api.routes.operators import OPERATOR_NOISE_ROOTS
    op_rows = db.execute(text("""
        SELECT o.operator_root, o.slug,
               COALESCE(o.total_acquisitions, 0) AS total,
               count(DISTINCT p.bbl) AS local
        FROM operators o
        JOIN operator_parcels op ON op.operator_id = o.id
        JOIN parcels p ON p.bbl = op.bbl
        WHERE o.operator_class = 'operator'
          AND COALESCE(jsonb_array_length(o.llc_entities), 0) > 0
          AND p.zip_code = :zip
        GROUP BY o.id
        ORDER BY local DESC, total DESC, o.operator_root
        LIMIT 8
    """), {"zip": zip_code}).fetchall()
    operators_here = [
        {"name": r.operator_root, "slug": r.slug, "total": int(r.total), "local": int(r.local)}
        for r in op_rows
        if r.slug and r.operator_root not in OPERATOR_NOISE_ROOTS
    ][:6]

    nearby = []
    if borough:
        nb_rows = db.execute(text("""
            SELECT n.zip_code, n.name, ds.score
            FROM neighborhoods n
            JOIN displacement_scores ds ON ds.zip_code = n.zip_code
            WHERE ds.score IS NOT NULL AND n.name IS NOT NULL AND n.zip_code <> :zip
        """), {"zip": zip_code}).fetchall()
        nearby = sorted(
            ({"zip": r.zip_code, "name": r.name, "score": float(r.score)}
             for r in nb_rows if _borough_from_zip(r.zip_code) == borough),
            key=lambda x: x["score"], reverse=True,
        )[:6]

    page_html = _build_neighborhood_page(
        zip_code, name, borough, score, breakdown, raw_counts, raw_hpd, summary, last_updated, history,
        petitions=petitions, vacates=vacates, flips=flips,
        operators_here=operators_here, nearby=nearby, lang=lang,
        ev_count=_ev_area_counts(name, db) if name else 0,
    )
    _page_cache[cache_key] = (page_html, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page_html)


_BOROUGH_SLUGS = {
    "Manhattan": "manhattan", "Brooklyn": "brooklyn", "Queens": "queens",
    "Bronx": "bronx", "Staten Island": "staten-island",
}

# PLUTO land-use codes. Only the residential and mixed classes get a building
# noun; the rest stay generic, because calling a parking lot a "building" in
# the lede is the kind of error a reader notices immediately.
_LAND_USE = {
    "01": "one and two family building",
    "02": "multi-family walk-up",
    "03": "multi-family elevator building",
    "04": "mixed residential and commercial building",
    "05": "commercial or office building",
    "06": "industrial or manufacturing property",
    "07": "transportation or utility property",
    "08": "public facility or institution",
    "09": "open space or recreation lot",
    "10": "parking facility",
    "11": "vacant lot",
}

# HPD grades a violation by hazard; DOB class I is the immediately hazardous
# one that carries a vacate order.
_VIOLATION_CLASS = {
    "A": "non-hazardous", "B": "hazardous",
    "C": "immediately hazardous", "I": "class I, immediately hazardous",
}

# A violation stops mattering when it is closed or thrown out. Everything else
# in the status vocabulary still describes live enforcement.
_VIOLATION_RESOLVED = ("VIOLATION CLOSED", "VIOLATION DISMISSED")


# str.title() turns LLC into Llc, which is how a records page announces that
# nobody read it. Acronyms and the ordinals in numbered entities stay put.
_ENTITY_ACRONYMS = {
    "LLC", "PLLC", "LLP", "LP", "INC", "CORP", "CO", "LTD", "HDFC", "NYC", "NY",
    "USA", "US", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "XI", "XII",
    "LC", "PC", "REIT", "TRS", "JV", "DBA", "MTA", "HPD", "NYCHA", "SPE",
    "MTEK",
}

# A short token with no vowel is an initialism, not a word: GS, MGMT, BK. This
# catches the long tail the list above cannot enumerate, and `bronx gs
# properties llc` is a query the site already ranks for.
_VOWELLESS = re.compile(r"^[BCDFGHJKLMNPQRSTVWXZ]{2,6}$")

# Street types come through the vowel-less rule as initialisms and print
# shouting: "FLGSP 1023 CARROLL ST LLC" reads better as "Carroll St" than
# "Carroll ST". These are the abbreviations that appear in address-named LLCs.
_ADDRESS_WORDS = {"ST", "RD", "DR", "PL", "CT", "LN", "SQ", "TR", "BLVD",
                  "PKWY", "HWY", "TPKE", "PLZ", "CRES"}


def _entity_title(name: str) -> str:
    if not name:
        return ""
    out = []
    for token in name.split():
        bare = token.strip(".,()").upper()
        if not bare:
            # PLUTO owner names carry stray separators ("ASSOCIATES    .") on
            # 27,900 parcels. A detached period is not part of the name, and
            # left in it becomes a second full stop mid-sentence.
            continue
        keep_caps = (bare in _ENTITY_ACRONYMS
                     or (bare not in _ADDRESS_WORDS and _VOWELLESS.match(bare)))
        if keep_caps:
            out.append(token.upper())
        elif bare in ("AND", "OF", "THE") and out:
            out.append(token.lower())
        else:
            out.append(token.title())
    joined = " ".join(out).strip(" ,&-")
    return re.sub(r"(\d)(St|Nd|Rd|Th)\b", lambda m: m.group(1) + m.group(2).lower(),
                  joined)


def _sentence(body: str) -> str:
    """One terminal period, however the record punctuated the name that ends it."""
    body = body.rstrip()
    return body if body.endswith((".", "?", "!")) else body + "."


_SPELLED = ["zero", "one", "two", "three", "four", "five", "six", "seven",
            "eight", "nine", "ten"]


def _plural(n: int, one: str, many: str = "") -> str:
    """Pluralises the head noun, so 'of the building' becomes 'of the
    buildings' rather than 'of the buildingss'."""
    if n == 1:
        return one
    if many:
        return many
    head, sep, tail = one.partition(" of ")
    if sep:
        return f"{head}s of {tail}"
    words = one.split()
    head = words[-1]
    if head.endswith("y") and len(head) > 1 and head[-2] not in "aeiou":
        words[-1] = head[:-1] + "ies"          # entity -> entities
    elif head.endswith(("s", "x", "z", "ch", "sh")):
        words[-1] = head + "es"                # address -> addresses
    else:
        words[-1] = head + "s"
    return " ".join(words)


def _count(n: int, one: str, many: str = "") -> str:
    return f"{n:,} {_plural(n, one, many)}"


def _count_open(n: int, one: str, many: str = "") -> str:
    """Same, for the start of a sentence, where a bare numeral reads as a typo."""
    word = _SPELLED[n].capitalize() if 0 <= n <= 10 else f"{n:,}"
    return f"{word} {_plural(n, one, many)}"


def _en_date(d) -> str:
    """Dates in prose read as dates, not as ISO stamps. Thin wrapper so the
    English-only callers do not each repeat the lang argument."""
    return _long_date(d, "en") if d else ""


def _hold_length(start, end) -> str:
    """How long an owner held, in the units a reader thinks in."""
    months = (end.year - start.year) * 12 + (end.month - start.month)
    if months < 1:
        days = (end - start).days
        return _count(max(days, 0), "day")
    if months < 24:
        return _count(months, "month")
    years, rem = divmod(months, 12)
    if rem == 0:
        return _count(years, "year")
    return f"{years} {_plural(years, 'year')} {_count(rem, 'month')}"


_zip_ctx_cache: dict[str, tuple[dict, float]] = {}
_ZIP_CTX_TTL = 21600


def _zip_context(zip_code: str, db) -> dict:
    """ZIP-level comparison figures for a property page.

    Identical for every building in the ZIP, so computing it per property was
    paying a full-ZIP scan 918,338 times over. The peer count alone measured
    402ms on 11207 and there are ZIPs half again that size. There are ~180
    ZIPs, so the whole table fits in a dict.
    """
    hit = _zip_ctx_cache.get(zip_code)
    if hit and time.monotonic() < hit[1]:
        return hit[0]

    ctx: dict = {}
    peer = db.execute(text("""
        SELECT count(*) AS tracked,
               count(*) FILTER (
                   WHERE EXISTS (SELECT 1 FROM evictions_raw e WHERE e.bbl = p.bbl)
               ) AS with_eviction,
               count(*) FILTER (
                   WHERE EXISTS (SELECT 1 FROM ownership_raw o
                                 WHERE o.bbl = p.bbl AND o.doc_type = 'DEED')
               ) AS with_deed
        FROM parcels p WHERE p.zip_code = :zip
    """), {"zip": zip_code}).first()
    ctx["peers"] = {
        "tracked": int(peer.tracked or 0),
        "with_eviction": int(peer.with_eviction or 0),
        "with_deed": int(peer.with_deed or 0),
    } if peer else {}

    hood = db.execute(text(
        "SELECT name FROM neighborhoods WHERE zip_code = :zip LIMIT 1"
    ), {"zip": zip_code}).first()
    ctx["hood"] = hood.name if hood and hood.name else ""

    rank = db.execute(text("""
        SELECT count(*) FILTER (WHERE score >= (
                   SELECT score FROM displacement_scores WHERE zip_code = :zip
               )) AS rank,
               count(*) AS total
        FROM displacement_scores WHERE score IS NOT NULL
    """), {"zip": zip_code}).first()
    if rank and rank.rank:
        ctx["zip_rank"] = (int(rank.rank), int(rank.total))

    _zip_ctx_cache[zip_code] = (ctx, time.monotonic() + _ZIP_CTX_TTL)
    return ctx


def _property_facts(bbl: str, zip_code: str, db) -> dict:
    """Everything the property body says in sentences rather than table rows.

    The page had the records and none of the reading of them: 100 visible
    words across four tables, near-identical to every other property page. The
    queries here are the ones that turn a row into a claim, and they run behind
    the same page cache as the rest of the body.
    """
    facts: dict = {}

    # The tax year the assessed value belongs to. parcels carries the figure and
    # not the year, and a tax number with no year on it is exactly the kind of
    # fact that outlives the record it came from.
    facts["tax_year"] = db.execute(text("""
        SELECT max(tax_year) FROM assessment_history
        WHERE bbl = :bbl AND assessed_total > 0
    """), {"bbl": bbl}).scalar()

    # Filings that propose fewer homes than the building has, corroborated by
    # the filer's own description. The counts alone are unusable: read raw,
    # 2,164 jobs a year "remove" 44,406 homes and the biggest is a $1,500
    # gas-valve permit on a 792-unit building. api.permit_kinds.deconversion_sql
    # carries the reasoning for every condition.
    #
    # The parcels join is not incidental: without it the biggest hits are a
    # hotel and a dormitory, both reducing a room count without removing a home.
    #
    # DOB NOW only, because legacy BIS records no dwelling counts, so this is
    # silent about anything filed before roughly 2021. The rendered copy says so.
    facts["deconversions"] = [
        {"date": d.filing_date, "existing": d.units_existing,
         "proposed": d.units_proposed,
         "cost": float(d.job_cost) if d.job_cost else 0.0,
         "description": (d.job_description or "").strip()}
        for d in db.execute(text(f"""
            SELECT pr.filing_date, pr.units_existing, pr.units_proposed,
                   pr.job_cost, pr.job_description
            FROM permits_raw pr
            JOIN parcels p ON p.bbl = pr.bbl
            WHERE pr.bbl = :bbl AND {deconversion_sql("pr", "p")}
            ORDER BY (pr.units_existing - pr.units_proposed) DESC,
                     pr.filing_date DESC
            LIMIT 3
        """), {"bbl": bbl, **DECONVERSION_PARAMS}).fetchall()
        if d.filing_date
    ]

    # Deed chain. ownership_raw carries assignments too, and an assignment is a
    # lender moving paper, not a sale, so the chain reads DEED rows only.
    deeds = db.execute(text("""
        SELECT o.document_id, o.doc_date, max(o.doc_amount) AS amount,
               max(o.party_name_normalized) FILTER (WHERE o.party_type = '2') AS buyer,
               max(o.party_name_normalized) FILTER (WHERE o.party_type = '1') AS seller
        FROM ownership_raw o
        WHERE o.bbl = :bbl AND o.doc_type = 'DEED'
        GROUP BY o.document_id, o.doc_date
        ORDER BY o.doc_date DESC NULLS LAST
        LIMIT 20
    """), {"bbl": bbl}).fetchall()
    facts["deeds"] = [
        {"date": d.doc_date, "amount": float(d.amount) if d.amount else 0.0,
         "buyer": d.buyer or "", "seller": d.seller or ""}
        for d in deeds if d.doc_date
    ]

    # Same grouping for the visible table. It used to render one row per party
    # row, so a single deed appeared twice and the seller was printed under a
    # column headed "Buyer". 82,756 parcels carry a seller row.
    docs = db.execute(text("""
        SELECT o.document_id, o.doc_type, o.doc_date, max(o.doc_amount) AS amount,
               max(o.party_name_normalized) FILTER (WHERE o.party_type = '2') AS buyer,
               max(o.party_name_normalized) FILTER (WHERE o.party_type = '1') AS seller
        FROM ownership_raw o
        WHERE o.bbl = :bbl
        GROUP BY o.document_id, o.doc_type, o.doc_date
        ORDER BY o.doc_date DESC NULLS LAST
        LIMIT 20
    """), {"bbl": bbl}).fetchall()
    facts["documents"] = [
        {"date": d.doc_date, "doc_type": d.doc_type or "",
         "amount": float(d.amount) if d.amount else 0.0,
         "buyer": d.buyer or "", "seller": d.seller or ""}
        for d in docs
    ]

    # Evictions run past the 12-month signal window the panel uses; the record
    # itself starts 2024-04-12 and the page should say what it really holds.
    ev = db.execute(text("""
        SELECT count(*) AS n, min(executed_date) AS first, max(executed_date) AS last,
               count(*) FILTER (WHERE eviction_type = 'Residential') AS residential
        FROM evictions_raw WHERE bbl = :bbl
    """), {"bbl": bbl}).first()
    facts["evictions"] = {
        "n": int(ev.n or 0), "first": ev.first, "last": ev.last,
        "residential": int(ev.residential or 0),
    } if ev else {"n": 0}

    viol = db.execute(text("""
        SELECT violation_class, count(*) AS n,
               count(*) FILTER (WHERE current_status NOT IN :resolved) AS open
        FROM violations_raw WHERE bbl = :bbl
        GROUP BY violation_class
    """), {"bbl": bbl, "resolved": _VIOLATION_RESOLVED}).fetchall()
    facts["violations"] = {r.violation_class: {"n": int(r.n), "open": int(r.open or 0)}
                           for r in viol if r.violation_class}

    # The violations themselves, which were the one record class the page counted
    # and never showed. 27,825 buildings are in the sitemap precisely because they
    # carry five or more, and every one of those pages said "written 9 violations"
    # and stopped. Newest first; the date is the notice, falling back to the
    # inspection that produced it.
    facts["violation_rows"] = [
        {"class": r.violation_class, "status": r.current_status,
         "date": r.dated.isoformat() if r.dated else None,
         "description": r.description}
        for r in db.execute(text("""
            SELECT violation_class, current_status, description,
                   COALESCE(nov_issued_date, inspection_date) AS dated
            FROM violations_raw
            WHERE bbl = :bbl
            ORDER BY COALESCE(nov_issued_date, inspection_date) DESC NULLS LAST,
                     violation_id DESC
            LIMIT 8
        """), {"bbl": bbl}).fetchall()
    ]

    # Registration history is the displacement signal, so both endpoints of the
    # series matter, not just the latest count.
    rs = db.execute(text("""
        SELECT year, rs_unit_count, source FROM rs_buildings
        WHERE bbl = :bbl AND source = 'dhcr' AND rs_unit_count > 0
        ORDER BY year
    """), {"bbl": bbl}).fetchall()
    facts["rs"] = [{"year": int(r.year), "units": int(r.rs_unit_count)} for r in rs]

    if zip_code:
        facts.update(_zip_context(zip_code, db))

    # Same rule as /flips and /radar: a page that shows deeds says where the
    # deed record stops. ACRIS publishes on its own schedule and has frozen
    # for weeks at a time.
    facts["deeds_through"] = _deeds_through_line(db)
    return facts


def _sibling_buildings(bbl: str, op, db) -> dict:
    """Other buildings the same owner holds, by the most precise route available.

    Two things asked for this and neither could be answered. The 2026-08-27
    console export carries assistant follow-up turns as queries: "do they own any
    other properties?" at position 2.0 and "what else do they own" at 8.0, asked
    against a page that carried one link to the owning company and no list. And
    73% of organic visitors read one page and leave, while the ones who stay go
    to another property page 720 times in fifteen days, which they were doing by
    going back to Google.

    Curated operator networks cover 566 parcels. Falling through to the deed
    buyer covers 6,669, which is twelve times as many and the whole of what can
    be done without a new index: `parcels.owner_name` reaches 167,637 and has no
    index, so it would seq-scan 918k rows on the page taking 88% of our traffic.
    That source and the entity families both belong to the maintenance window.

    Returns the rows plus which route found them, because the two mean different
    things to a reader and the page has to say which one it is showing.
    """
    if op is not None:
        rows = db.execute(text("""
            SELECT p.bbl, p.address, p.zip_code, n.name AS hood
            FROM operator_parcels op
            JOIN parcels p ON p.bbl = op.bbl
            LEFT JOIN neighborhoods n ON n.zip_code = p.zip_code
            WHERE op.operator_id = (SELECT id FROM operators WHERE slug = :slug)
              AND op.bbl <> :bbl AND p.address IS NOT NULL
            ORDER BY p.zip_code, p.address
            LIMIT 8
        """), {"slug": op.slug, "bbl": bbl}).fetchall()
        if rows:
            return {"rows": _sib_rows(rows), "source": "operator", "entity": None}

    # The buyer on the newest deed, and only if it is a company. A natural
    # person who bought two houses is not a portfolio, and a page listing "every
    # building this named individual owns" is a people-search directory wearing
    # a displacement-research mission statement. _is_buyer_entity is the same
    # test /llc indexes on, so the two cannot disagree about who is a company.
    rows = db.execute(text(f"""
        WITH latest AS (
            SELECT o.party_name_normalized AS name
            FROM ownership_raw o
            WHERE o.bbl = :bbl AND o.doc_type = 'DEED' AND o.party_type = '2'
              AND {real_date('o.doc_date', 'o.created_at')}
            ORDER BY o.doc_date DESC NULLS LAST
            LIMIT 1
        )
        SELECT p.bbl, p.address, p.zip_code, n.name AS hood, l.name AS entity
        FROM latest l
        JOIN ownership_raw o ON o.party_name_normalized = l.name
             AND o.doc_type = 'DEED' AND o.party_type = '2'
        JOIN parcels p ON p.bbl = o.bbl
        LEFT JOIN neighborhoods n ON n.zip_code = p.zip_code
        WHERE o.bbl <> :bbl AND p.address IS NOT NULL
        GROUP BY p.bbl, p.address, p.zip_code, n.name, l.name
        ORDER BY p.zip_code, p.address
        LIMIT 8
    """), {"bbl": bbl}).fetchall()
    if rows and _is_buyer_entity(rows[0].entity):
        return {"rows": _sib_rows(rows), "source": "entity",
                "entity": _entity_title(rows[0].entity)}
    return {"rows": [], "source": None, "entity": None}


def _sib_rows(rows) -> list[dict]:
    return [{"bbl": r.bbl, "address": _addr_title(r.address),
             "zip": r.zip_code or "", "hood": r.hood or ""} for r in rows]


def _build_property_page(bbl, address, zip_code, borough, score, sig, op,
                         parcel=None, facts=None, siblings=None,
                         unit_lot=None, feeds=None) -> str:
    """Server-rendered content body for a single building: its public-record
    history (deeds, evictions, permits, complaints) plus links up to the ZIP,
    borough, and owning operator. Replaces the old map-shell body so the page is
    real content, not a near-duplicate JS app. Thin buildings (no records, no
    score) are rendered noindex so they don't dilute the index."""
    e = _html.escape
    _MO = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _d(iso):
        if not iso:
            return ""
        try:
            y, m, d = iso[:10].split("-")
            return f"{_MO[int(m)]} {int(d)}, {y}"
        except (ValueError, IndexError):
            return ""

    def _money(v):
        try:
            v = float(v)
        except (TypeError, ValueError):
            return ""
        if v <= 0:
            return ""
        if v >= 1_000_000:
            return f"${v / 1e6:.1f}M"
        if v >= 1000:
            return f"${v / 1000:.0f}K"
        return f"${v:.0f}"

    owners = sig.get("ownership_transfers") or []
    evicts = sig.get("evictions_last_12mo") or []
    permits = sig.get("permits_last_12mo") or []
    complaints = sig.get("complaints_last_12mo") or []

    # Indexability turns on this lot having a record of its own.
    #
    # It used to also pass on `score is not None`, and the score is ZIP-level,
    # so every parcel in a scored ZIP was "index, follow". Measured: 596,432
    # parcels with no deed, no eviction, no violation and no permit were
    # telling Google to index 429 words of boilerplate that runs **81%**
    # identical page to page. That is the doorway-page failure the SEO plan
    # warns about, arriving through the robots tag rather than the sitemap,
    # which is why a sitemap-only reading of the problem missed it.
    #
    # The windowed lists above cover twelve months; robots policy should not
    # flip because a building's only eviction aged out, so the all-time facts
    # decide it.
    has_signals = bool(
        (facts or {}).get("documents")
        or ((facts or {}).get("evictions") or {}).get("n")
        or (facts or {}).get("violations")
        or (facts or {}).get("rs")
        or owners or evicts or permits
    )

    def _section(h2, note, heads, rows):
        if not rows:
            return ""
        th = "".join(f"<th>{c}</th>" for c in heads)
        return (f'<section style="margin-bottom:30px;"><h2>{h2}</h2>'
                f'<div class="table-wrap"><table><thead><tr>{th}</tr></thead>'
                f'<tbody>{rows}</tbody></table></div>'
                f'<p class="data-note">{note}</p></section>')

    def _buyer_cell(o) -> str:
        """Link company buyers to their deed ledger. Only parties on a DEED
        resolve at /llc/{slug}; assignment parties would 404. Substring
        matching also caught people ("... AS TRUSTEE"), so the form token has
        to stand on its own."""
        name = o.get("buyer") or ""
        shown = _entity_title(name)
        if ((o.get("doc_type") or "").upper() == "DEED"
                and _ENTITY_FORM_RE.search(name)
                and not _NOT_A_BUYER_RE.search(name)):
            slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
            if _LLC_SLUG_RE.match(slug):
                return f'<a href="/llc/{e(slug)}" style="color:#6fb1d8;">{e(shown)}</a>'
        return e(shown)

    # One row per document, not per party row. An ACRIS document names both
    # sides; printing each side as its own transfer doubled every sale and put
    # the seller under a column headed "Buyer".
    documents = (facts or {}).get("documents") or []
    doc_rows = "".join(
        f'<tr><td class="sc">{_buyer_cell({"buyer": d["buyer"], "doc_type": d["doc_type"]})}'
        f'<span class="sw">{e(d["doc_type"])}'
        + (f' from {e(_entity_title(d["seller"]))}' if d["seller"] else "")
        + '</span></td>'
        f'<td class="sr">{_d(d["date"].isoformat()) if d["date"] else ""}</td>'
        f'<td class="si">{_money(d["amount"])}</td></tr>'
        for d in documents
    )
    # Passed in rather than queried here: this function renders, the route
    # reads. A feed with no date prints no citation instead of a wrong one.
    feeds = feeds or {}

    own_sec = _section(
        "Sales and ownership history",
        "Deeds and assignments recorded in ACRIS, one row per document. Amount is the "
        "stated consideration; $0 often marks a non-arms-length transfer. An assignment "
        "moves a lender's paper and is not a sale." + _cite(feeds, "acris"),
        ("Party taking title", "Recorded", "Amount"), doc_rows,
    )

    ev_rows = "".join(
        f'<tr><td class="sc">{e(ev.get("type") or "Residential")}'
        f'<span class="sw">{e("docket " + ev["docket"] if ev.get("docket") else "")}</span></td>'
        f'<td class="sr">{_d(ev.get("date"))}</td><td class="si"></td></tr>'
        for ev in evicts
    )
    ev_sec = _section(
        "Executed evictions",
        "Marshal-executed residential evictions from the NYC evictions dataset, past 12 months."
        + _cite(feeds, "evictions"),
        ("Type", "Executed", ""), ev_rows,
    )

    pm_rows = "".join(
        f'<tr><td class="sc">{e(p.get("work_type") or p.get("type") or "Permit")}'
        f'<span class="sw">{e((p.get("description") or "")[:80])}</span></td>'
        f'<td class="sr">{_d(p.get("filed"))}</td><td class="si">{e(p.get("type") or "")}</td></tr>'
        for p in permits
    )
    pm_sec = _section(
        "Building permits",
        "DOB job filings on this lot, past 12 months." + _cite(feeds, "permits"),
        ("Work", "Filed", "Type"), pm_rows,
    )

    comp_sec = ""
    if complaints:
        comp_sec = (
            '<section style="margin-bottom:30px;"><h2>311 complaint history</h2>'
            f'<p style="font-size:.95rem;margin-bottom:8px;"><span style="font-family:\'JetBrains Mono\','
            f'monospace;font-size:1.3rem;font-weight:600;">{len(complaints)}</span> '
            '<span style="color:var(--muted);">complaints in the past 12 months</span></p>'
            '<p class="data-note">NYC 311 housing and building complaints logged for this address.'
            + _cite(feeds, "complaints") + '</p></section>'
        )

    score_block = ""
    if score is not None:
        tier, color = _tier_info(score)
        score_block = (
            f'<div class="score-block"><span class="score-num" style="color:{color}">{score:.1f}</span>'
            f'<span class="score-denom">/100</span>'
            f'<span class="score-tier" style="color:{color}">{tier.upper()} AREA PRESSURE</span></div>'
        )

    # --- The reading of the record ------------------------------------------
    # Everything below turns rows into sentences. It is what the page was
    # missing: the tables were already here, and nothing said what they meant.
    facts = facts or {}
    parcel = parcel or {}
    siblings = siblings or []
    hood = facts.get("hood") or ""
    place = f"{hood}, {borough}" if hood else borough
    deed_chain = facts.get("deeds") or []
    ev_facts = facts.get("evictions") or {}
    rs_series = facts.get("rs") or []
    peers = facts.get("peers") or {}

    def _para(*sentences) -> str:
        text_ = " ".join(s for s in sentences if s)
        return f'<p class="prose">{text_}</p>' if text_ else ""

    def _prose_section(h2, *paragraphs) -> str:
        inner = "".join(p for p in paragraphs if p)
        if not inner:
            return ""
        return f'<section style="margin-bottom:30px;"><h2>{h2}</h2>{inner}</section>'

    # Homes proposed for removal. Placed above the permit table because it is
    # the one permit fact that changes how many people can live here, and below
    # everything about who owns it, because that is the question the page is
    # usually opened with.
    dec_sec = ""
    decs = facts.get("deconversions") or []
    if decs:
        paras = []
        for d in decs:
            lost = int(d["existing"]) - int(d["proposed"])
            cost = (f" The job is valued at ${d['cost']:,.0f}."
                    if d["cost"] and d["cost"] >= 1000 else "")
            desc = d["description"][:200].rstrip()
            paras.append(_para(
                f"A filing dated {e(_en_date(d['date']))} proposes taking this "
                f"building from {_count(int(d['existing']), 'home')} to "
                f"{_count(int(d['proposed']), 'home')}, "
                f"{_count(lost, 'home')} fewer.{cost}",
                f"The filed description reads: {e(desc)}." if desc else "",
            ))
        paras.append(_para(
            "Dwelling counts come from the applicant, not from an inspection, "
            "and they are only recorded in DOB NOW filings, so nothing here "
            "covers work filed before 2021."
            + _cite(feeds, "permits").replace('<span class="cite">', " ")
              .replace("</span>", "")))
        dec_sec = _prose_section("Homes proposed for removal", *paras)


    # Lede. Every clause is drawn from this lot's own record, which is also
    # what keeps 1,792 pages from reading as one page.
    kind = _LAND_USE.get((parcel.get("land_use") or "").zfill(2), "building")
    built = parcel.get("year_built") or 0
    units_res = int(parcel.get("units_res") or 0)
    units_total = int(parcel.get("units_total") or 0)

    lede_bits = [f"{e(address)} is a {e(kind)} in {e(place)}"]
    if zip_code:
        lede_bits.append(f"ZIP {e(zip_code)}")
    lede_open = ", ".join(lede_bits) + "."
    build_line = ""
    if built and built > 1700:
        build_line = f"City records date it to {built}"
        if units_res:
            build_line += f" and count {_count(units_res, 'residential unit')}"
            if units_total > units_res:
                build_line += f" of {units_total:,} total"
        build_line += "."
    elif units_res:
        build_line = f"City records count {_count(units_res, 'residential unit')} here."

    held_line = ""
    owner_name = (parcel.get("owner_name") or "").strip()
    if owner_name:
        held_line = _sentence(f"The city's property file lists the owner of record "
                              f"as {e(_entity_title(owner_name))}")

    assessed = parcel.get("assessed_total") or 0
    assess_line = ""
    if assessed:
        assess_line = (f"Its most recent total assessed value is "
                       f"{_fmt_amount(assessed)}.")

    lede = _para(lede_open, build_line, held_line, assess_line)

    # Ownership chain. A resale inside a short hold with a price jump is the
    # site's whole thesis, so it gets said outright rather than left to the
    # reader to compute from two table rows.
    chain_paras = []
    if deed_chain:
        latest = deed_chain[0]
        first_sentence = f"The most recent recorded deed is dated {_d(latest['date'].isoformat())}"
        if latest["buyer"]:
            first_sentence += f", transferring the lot to {e(_entity_title(latest['buyer']))}"
        if latest["amount"] > 0:
            first_sentence += f" for a stated {_fmt_amount(latest['amount'])}"
        chain_paras.append(_sentence(first_sentence))

        if len(deed_chain) > 1:
            prior = deed_chain[1]
            hold = _hold_length(prior["date"], latest["date"])
            resale = f"The deed before it was recorded {_d(prior['date'].isoformat())}"
            if prior["amount"] > 0:
                resale += f" at {_fmt_amount(prior['amount'])}"
            resale += f", so the lot changed hands twice inside {hold}."
            chain_paras.append(resale)
            if prior["amount"] > 0 and latest["amount"] > 0:
                delta = (latest["amount"] - prior["amount"]) / prior["amount"] * 100
                if abs(delta) >= 5:
                    verb = "rose" if delta > 0 else "fell"
                    chain_paras.append(
                        f"The stated consideration {verb} {abs(delta):.0f}% between "
                        f"the two deeds."
                    )
        chain_paras.append(
            f"PulseCities holds {_count(len(deed_chain), 'recorded deed')} for this lot."
        )
        if facts.get("deeds_through"):
            chain_paras.append(facts["deeds_through"])
    chain_note = ('<p class="data-note">A deed names a party of record. Stated '
                  'consideration of $0 usually marks a transfer between related '
                  'parties rather than a sale.</p>')
    chain_sec = _prose_section(f"Who owns {e(address)}", _para(*chain_paras),
                               chain_note if deed_chain else "")

    # What the city taxes. The figure was already on the page as one clause of
    # the lede, which is not a thing a reader searching "205 dean street taxes"
    # can find. It is a snapshot: only the current tax year carries an assessed
    # value, so there is no trend here to report and none is implied.
    tax_sec = ""
    if assessed:
        tax_year = facts.get("tax_year")
        tax_paras = [_sentence(
            f"The city's Department of Finance assessed this lot at "
            f"{_fmt_amount(assessed)}"
            + (f" for the {int(tax_year)} tax year" if tax_year else "")
        )]
        # Only where the division says something. On a one-family lot the per-unit
        # figure is the same number twice.
        if units_res and units_res > 1:
            tax_paras.append(
                f"That works out to {_fmt_amount(assessed / units_res)} per "
                f"residential unit.")
        tax_paras.append(
            "Assessed value is what the city taxes, not what the building would sell "
            "for, and the two routinely differ by a wide margin. The bill itself turns "
            "on the tax class rate and on any exemptions or abatements attached to the "
            "lot, neither of which is published here."
        )
        tax_sec = _prose_section("Taxes and assessed value", _para(*tax_paras))

    # Eviction record, in sentences and over the full window rather than the
    # rolling twelve months the table shows.
    ev_paras = []
    n_ev = int(ev_facts.get("n") or 0)
    if n_ev:
        line = (f"City marshals executed {_count(n_ev, 'eviction')} at {e(address)} "
                f"in the record PulseCities holds")
        if ev_facts.get("last"):
            line += f", the most recent on {_d(ev_facts['last'].isoformat())}"
        line += "."
        ev_paras.append(line)
        if n_ev > 1 and ev_facts.get("first") and ev_facts.get("last"):
            ev_paras.append(
                f"The first fell on {_d(ev_facts['first'].isoformat())}, so the "
                f"filings span {_hold_length(ev_facts['first'], ev_facts['last'])}."
            )
        res = int(ev_facts.get("residential") or 0)
        if res and res < n_ev:
            ev_paras.append(f"{_count_open(res, 'of them was', 'of them were')} residential.")
        # An eviction shortly before a deed is the pattern the site was built
        # to surface, so say it on the building's own page.
        if deed_chain and ev_facts.get("last"):
            gap_deeds = [dd for dd in deed_chain
                         if dd["date"] and 0 <= (dd["date"] - ev_facts["last"]).days <= 365]
            if gap_deeds:
                ev_paras.append(
                    f"A deed was recorded {_count((gap_deeds[-1]['date'] - ev_facts['last']).days, 'day')} "
                    f"after that eviction, the sequence PulseCities tracks citywide."
                )
    elif peers:
        ev_paras.append(
            f"No executed eviction is on record at {e(address)} in the citywide "
            f"marshal file, which runs from April 2024."
        )
    # A visitor reading an eviction count here often has the paperwork for one
    # of them. Only offered where there is an eviction to look up.
    if n_ev:
        ev_paras.append(
            'Holding the marshal docket number or the court index number from '
            'one of these? <a href="/eviction-case">Look up that case &rarr;</a>'
        )
    ev_prose = _prose_section("What the eviction record shows", _para(*ev_paras))

    # Rent stabilization. The registration series is the signal, and a building
    # that stops registering is the thing worth naming.
    rs_paras = []
    if rs_series:
        newest, oldest = rs_series[-1], rs_series[0]
        rs_paras.append(
            f"DHCR registration records list {_count(newest['units'], 'rent-stabilized unit')} "
            f"at this building in {newest['year']}, the most recent year PulseCities holds."
        )
        if len(rs_series) > 1 and oldest["units"] != newest["units"]:
            direction = "down from" if newest["units"] < oldest["units"] else "up from"
            rs_paras.append(
                f"That is {direction} {_count(oldest['units'], 'unit')} in {oldest['year']}."
            )
        rs_paras.append(
            "Registration is building-level and does not settle the status of any "
            "single apartment; the rent history does."
        )
    else:
        rs_paras.append(
            f"No DHCR rent-stabilization registration is on file for {e(address)} in "
            f"the years PulseCities holds. Absence is not proof: an owner who "
            f"stops registering leaves the same gap as a building that never had "
            f"stabilized units."
        )
    rs_paras.append('Checking your own apartment starts with the free rent history. '
                    '<a href="/is-my-building-rent-stabilized">How to check '
                    'rent-stabilized status &rarr;</a>')
    rs_sec = _prose_section("Rent stabilization at this address", _para(*rs_paras))

    # Violations. New surface: the data was in the DB and on no page.
    viols = facts.get("violations") or {}
    viol_sec = ""
    if viols:
        total = sum(v["n"] for v in viols.values())
        open_n = sum(v["open"] for v in viols.values())
        vp = [f"HPD and DOB inspectors have written {_count(total, 'violation')} "
              f"against this building, of which {open_n:,} "
              f"{'remains' if open_n == 1 else 'remain'} unresolved."]
        worst = [c for c in ("I", "C", "B", "A") if viols.get(c, {}).get("open")]
        if worst:
            label = _VIOLATION_CLASS.get(worst[0], worst[0])
            vp.append(f"The most serious open grade here is {label}, with "
                      f"{_count(viols[worst[0]]['open'], 'open violation')}.")
        viol_sec = _prose_section("Open code violations", _para(*vp),
                                  '<p class="data-note">Classes run A (non-hazardous) '
                                  'to C (immediately hazardous); DOB class I carries a '
                                  'vacate order.' + _cite(feeds, "violations") + '</p>')

    viol_table = ""
    vrows = facts.get("violation_rows") or []
    if vrows:
        open_words = ("open", "not complied", "nov sent", "notice of issuance")
        rows_html = "".join(
            f'<tr><td class="sc">{e(_violation_text(v.get("description")))}'
            f'<span class="sw">{e((v.get("status") or "").title())}</span></td>'
            f'<td class="sr">{_d(v.get("date"))}</td>'
            f'<td class="si">{e(v.get("class") or "")}</td></tr>'
            for v in vrows
        )
        still_open = sum(1 for v in vrows
                         if any(w in (v.get("status") or "").lower() for w in open_words))
        viol_table = _section(
            "Violation history",
            f"The {len(vrows)} most recent violations written against this building, "
            f"newest first"
            + (f", {still_open} of them still open" if still_open else "")
            + ". Each row is the inspector's own wording with the statute citation "
              "removed." + _cite(feeds, "violations"),
            ("Violation", "Issued", "Class"), rows_html,
        )

    # The building against its ZIP. Turns a lone score into a comparison, and
    # is the paragraph that earns the link up to the neighbourhood page.
    cmp_paras = []
    if zip_code and peers.get("tracked"):
        tracked = peers["tracked"]
        cmp_paras.append(
            f"PulseCities tracks {_count(tracked, 'lot')} in {e(zip_code)}, of which "
            f"{peers.get('with_eviction', 0):,} carry an executed eviction from the "
            f"marshal record, which starts in April 2024, and "
            f"{peers.get('with_deed', 0):,} carry a deed from the shorter ACRIS window."
        )
        if n_ev:
            share = peers.get("with_eviction", 0) / tracked * 100 if tracked else 0
            cmp_paras.append(
                f"This building is in that second group, which is {share:.1f}% of "
                f"the ZIP."
            )
        if score is not None and facts.get("zip_rank"):
            rank, total_z = facts["zip_rank"]
            cmp_paras.append(
                f"{e(zip_code)} scores {score:.1f} out of 100 for displacement "
                f"pressure, {rank} of {total_z} scored NYC ZIP codes."
            )
        cmp_paras.append(
            f'<a href="/neighborhood/{e(zip_code)}">The full signal breakdown for '
            f'{e(hood) if hood else e(zip_code)} &rarr;</a>'
        )
    cmp_sec = _prose_section(f"How {e(address)} compares in {e(zip_code or borough)}",
                             _para(*cmp_paras))

    # Sideways links. Property to operator existed; operator to sibling
    # buildings did not, so a portfolio was 30 unconnected pages.
    sib_sec = ""
    sib = siblings if isinstance(siblings, dict) else {"rows": siblings or [], "source": "operator", "entity": None}
    siblings = sib["rows"]
    if siblings:
        owner_label = e(op.display_name or op.operator_root) if op is not None else "the same owner"
        rows_html = "".join(
            f'<li class="rec-row"><a href="/property/{e(s["bbl"])}">'
            f'<div><div class="rec-addr">{e(s["address"])}</div>'
            f'<div class="rec-geo">{e(s["hood"] + ", " if s["hood"] else "")}{e(s["zip"])}</div></div>'
            f'</a></li>' for s in siblings
        )
        if sib["source"] == "entity":
            # Say which record drew the line. A shared deed buyer is a stronger
            # statement than a shared operator classification and a weaker one
            # than common control, and the page should not let a reader round it
            # up to the latter.
            ent = e(sib["entity"] or "")
            slug = re.sub(r"[^a-z0-9]+", "-", (sib["entity"] or "").lower()).strip("-")
            more = (_para(f'<a href="/llc/{e(slug)}">Every deed recorded for {ent} '
                          f'&rarr;</a>') if _LLC_SLUG_RE.match(slug) else "")
            sib_sec = _prose_section(
                "What else this owner holds",
                _para(f"{ent} took title to {_count(len(siblings), 'other building')} "
                      f"in the deed record. A shared buyer on a deed is a fact about "
                      f"the filing, not proof that one operation runs them all:"),
                f'<ul class="sib-list">{rows_html}</ul>',
                more,
            )
        else:
            sib_sec = _prose_section(
                "Other buildings in this owner network",
                _para(f"The {owner_label} network holds other NYC buildings PulseCities "
                      f"tracks. {_count(len(siblings), 'address')} from that portfolio:"),
                f'<ul class="sib-list">{rows_html}</ul>',
                _para(f'<a href="/operator/{e(op.slug)}">The full {owner_label} portfolio '
                      f'&rarr;</a>') if op is not None else "",
            )

    # FAQ. The queries that reach these pages arrive phrased as questions, and
    # the answers are already in the record above.
    faq: list[tuple[str, str]] = []
    who = ""
    if deed_chain and deed_chain[0]["buyer"]:
        who = _entity_title(deed_chain[0]["buyer"])
    elif owner_name:
        who = _entity_title(owner_name)
    if who:
        ans = (f"The most recent deed on file for {address} names {who}. "
               f"Deeds record the party that took title, which is often a holding "
               f"company rather than the operator managing the building.")
        if deed_chain and deed_chain[0]["date"]:
            ans += f" That deed was recorded {_d(deed_chain[0]['date'].isoformat())}."
        faq.append((f"Who owns {address}?", ans))

    if deed_chain and deed_chain[0]["amount"] > 0:
        faq.append((
            f"How much did {address} sell for?",
            f"The most recent deed states a consideration of "
            f"{_fmt_amount(deed_chain[0]['amount'])}, recorded "
            f"{_d(deed_chain[0]['date'].isoformat())} in ACRIS. Stated consideration "
            f"is what the parties filed, and it can differ from the economics of "
            f"the deal.",
        ))
    else:
        faq.append((
            f"Has {address} changed hands recently?",
            f"PulseCities holds {_count(len(deed_chain), 'recorded deed')} for this "
            f"lot. Deeds appear here once the city publishes them to ACRIS, which "
            f"runs behind the closing date." if deed_chain else
            f"No deed for {address} appears in the ACRIS records PulseCities holds. "
            f"That means no transfer has been published for this lot in the current "
            f"window, not that the building has never been sold.",
        ))

    ev_answer = (
        f"Yes. {_count_open(n_ev, 'marshal-executed eviction')} at {address} "
        f"{'appears' if n_ev == 1 else 'appear'} in the NYC evictions dataset"
        + (f", the most recent on {_d(ev_facts['last'].isoformat())}." if ev_facts.get("last") else ".")
        + " The dataset covers executed evictions only, so it undercounts housing "
          "court activity: cases that settle or end in a move-out never reach a marshal."
    ) if n_ev else (
        f"No executed eviction at {address} appears in the NYC evictions dataset, "
        f"which PulseCities holds from April 2024 forward. Executed evictions are "
        f"the end of the process, so a building with none may still have active "
        f"housing court cases."
    )
    faq.append((f"Have there been evictions at {address}?", ev_answer))

    if rs_series:
        newest = rs_series[-1]
        faq.append((
            f"Is {address} rent stabilized?",
            f"DHCR registrations list {_count(newest['units'], 'stabilized unit')} at "
            f"this building in {newest['year']}. Registration is building-level, so it "
            f"does not settle whether one apartment is stabilized. The free rent "
            f"history from NYS Homes and Community Renewal does.",
        ))
    else:
        faq.append((
            f"Is {address} rent stabilized?",
            f"No DHCR registration for {address} appears in the years PulseCities "
            f"holds. That is a gap in the registration record rather than an answer "
            f"about any apartment, and the free rent history from NYS Homes and "
            f"Community Renewal is what settles it.",
        ))

    if assessed:
        faq.append((
            f"What is {address} assessed at for property taxes?",
            f"The Department of Finance assessed the lot at {_fmt_amount(assessed)}"
            + (f" for the {int(facts['tax_year'])} tax year" if facts.get("tax_year") else "")
            + ". That is the figure the city taxes rather than a sale price, and the "
              "bill depends on the tax class rate and on any exemptions on the lot.",
        ))

    if zip_code and peers.get("tracked"):
        faq.append((
            f"What is the displacement risk around {address}?",
            f"{zip_code}"
            + (f" ({hood})" if hood else "")
            + (f" scores {score:.1f} out of 100 on the PulseCities displacement index."
               if score is not None else " has no current displacement score.")
            + f" The score reads five ZIP-level signals: LLC acquisition rate, permit "
              f"intensity, 311 complaint volume, executed evictions, and serious HPD "
              f"violations. It describes the area around the building, not the "
              f"building itself.",
        ))

    faq_html = "".join(
        f'<div class="faq-item"><h3>{e(q)}</h3><p>{e(a)}</p></div>' for q, a in faq
    )
    faq_sec = (f'<section style="margin-bottom:30px;"><h2>Questions about {e(address)}</h2>'
               f'{faq_html}</section>') if faq else ""

    # Up-links: ZIP, owning operator, borough. These turn the property page from
    # a dead-end into a hub node and give crawlers a path back to the money pages.
    links = []
    if zip_code:
        links.append(f'<a href="/neighborhood/{e(zip_code)}" class="btn-map">Displacement signals for {e(zip_code)} &rarr;</a>')
    if op is not None:
        links.append(f'<a href="/operator/{e(op.slug)}" class="btn-copy">Owner network: {e(op.display_name or op.operator_root)} &rarr;</a>')
    links.append('<a href="/map" class="btn-copy">Open the map &rarr;</a>')
    links_html = "".join(links)

    # Watch-this-building card. This page takes ~88% of organic landings and
    # until now offered no way to come back: a reader searched their own
    # address, read the record, and left no trace. The alert side has existed
    # since July (scripts/building_alerts.py, 03:25 daily, deeds + evictions +
    # permits + violations on one BBL) and /api/subscribe already accepted a
    # bbl target; only the form was missing. Single opt-in, so the row is live
    # the moment it is written.
    watch_card = (
        '<section class="watch-card">'
        f'<h2 class="watch-h">Watch {e(address)}</h2>'
        '<p class="section-sub">Get an email when a deed, eviction, permit or '
        'violation is filed on this building. Only when something lands, and '
        'never more than once a day</p>'
        '<div class="watch-row" id="pw-row">'
        '<input id="pw-email" type="email" inputmode="email" autocomplete="email" '
        f'placeholder="you@example.com" aria-label="Email address to watch {e(address)}">'
        '<button id="pw-btn" class="btn-map" type="button">Start watching</button>'
        '</div>'
        '<p id="pw-msg" class="watch-msg" aria-live="polite" style="display:none;"></p>'
        '</section>'
    )
    _j = json.dumps
    watch_js = (
        "<script>(function(){"
        "var b=document.getElementById('pw-btn'),m=document.getElementById('pw-msg'),"
        "el=document.getElementById('pw-email'),row=document.getElementById('pw-row');"
        "if(!b)return;"
        "function show(t,ok){m.textContent=t;m.style.color=ok?'#6fa287':'#e4483b';"
        "m.style.display='block';}"
        "async function go(){var v=(el.value||'').trim();"
        "if(!/^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$/.test(v)){"
        "show('That does not look like an email address.',false);return;}"
        "b.disabled=true;b.textContent='\\u2026';"
        "try{var r=await fetch('/api/subscribe',{method:'POST',"
        "headers:{'Content-Type':'application/json'},"
        "body:JSON.stringify({email:v,bbl:" + _j(bbl) + "})});"
        "if(r.ok){if(window.plausible){plausible('Building Watch Submit',"
        "{props:{bbl:" + _j(bbl) + "}});}"
        "row.style.display='none';"
        "show('Watching. New filings here will come to your inbox.',true);}"
        "else if(r.status===409){show('You are already watching this building.',true);"
        "b.disabled=false;b.textContent='Start watching';}"
        "else{throw new Error();}}"
        "catch(err){show('Something went wrong. Try again in a moment.',false);"
        "b.disabled=false;b.textContent='Start watching';}}"
        "b.addEventListener('click',go);"
        "el.addEventListener('keydown',function(ev){if(ev.key==='Enter')go();});"
        "})();</script>"
    )

    # Breadcrumb (visible + schema): Home > Borough > ZIP > Address.
    crumb_items = [{"@type": "ListItem", "position": 1, "name": "Home", "item": "https://pulsecities.com/"}]
    crumb_html = '<a href="/">Home</a>'
    pos = 2
    bslug = _BOROUGH_SLUGS.get(borough)
    if bslug:
        crumb_items.append({"@type": "ListItem", "position": pos, "name": borough, "item": f"https://pulsecities.com/borough/{bslug}"})
        crumb_html += f' &middot; <a href="/borough/{bslug}">{e(borough)}</a>'
        pos += 1
    if zip_code:
        crumb_items.append({"@type": "ListItem", "position": pos, "name": zip_code, "item": f"https://pulsecities.com/neighborhood/{zip_code}"})
        crumb_html += f' &middot; <a href="/neighborhood/{e(zip_code)}">{e(zip_code)}</a>'
        pos += 1
    crumb_items.append({"@type": "ListItem", "position": pos, "name": address, "item": f"https://pulsecities.com/property/{bbl}"})

    url = f"https://pulsecities.com/property/{bbl}"
    # Searchers type the address plus "bronx ny" or a ZIP, and they want the
    # records, not our score jargon. Title matches the query and promises the
    # record; the score stays in the description and on the page.
    zip_part = f" {zip_code}" if zip_code else ""
    # Unit lots on one block all inherit the building's address, so without
    # the lot in the title, dozens of pages would share one title with each
    # other and with the building's own page.
    title_name = f"{address} unit lot {bbl[6:]}" if unit_lot else address
    # No " NY" and no brand suffix. Google renders ~580px, about 60 characters,
    # and the old tail ", {borough} NY {zip}: deeds, evictions, permits |
    # PulseCities" was 41 characters before the address, so every one of ~97,790
    # titles ran 65 to 78 and 100% of them were truncated. "| PulseCities" was
    # therefore never displayed on this template, which makes dropping it free,
    # and the 14 characters it returns are what carry "deeds, evictions,
    # permits" -- the only part of the title that says why to click this rather
    # than Zillow. Median address is 17 characters, so the median title is now
    # 60 and p90 is 64. Guarded by tests/test_title_budget.py.
    title = f"{title_name}, {borough}{zip_part}: deeds, evictions, permits"
    zloc = f" ({zip_code})" if zip_code else ""
    # The BBL belongs in the description because people search it: 3009970039
    # took 37 impressions and one of the site's five clicks, and two more BBLs
    # show up in the Bing export. Nothing else on the web answers "which
    # building is this number" for a lay searcher.
    desc = (f"{address}, {borough}{zloc}, BBL {bbl}: deed transfers, executed "
            f"evictions, and renovation permits from NYC public records"
            + (f", displacement score {score:.1f}/100." if score is not None else "."))
    if len(desc) > 165:
        desc = desc[:162].rsplit(" ", 1)[0] + "."

    og_image = f"https://pulsecities.com/og/{zip_code}.png" if zip_code else "https://pulsecities.com/og-image.png"
    robots = "index, follow" if has_signals else "noindex, follow"

    place_ld = _jsonld({
        "@context": "https://schema.org",
        "@type": "Place",
        "name": address,
        "url": url,
        "address": {
            "@type": "PostalAddress",
            "streetAddress": address,
            "addressLocality": borough,
            "addressRegion": "NY",
            "postalCode": zip_code,
            "addressCountry": "US",
        },
    })
    bc_ld = _jsonld({"@context": "https://schema.org", "@type": "BreadcrumbList", "itemListElement": crumb_items})
    faq_ld = _jsonld({
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {"@type": "Question", "name": q,
             "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faq
        ],
    }) if faq else ""

    body_note = ("Sourced from NYC public records: ACRIS deeds, DOB permits, the NYC evictions dataset, "
                 "and 311. Records reflect what agencies have published and can lag events.")
    empty = "" if has_signals else (
        '<p class="section-sub" style="margin-top:8px;">No deed transfers, evictions, or permits are on '
        'record for this building in the current window. It is shown for reference.</p>'
    )
    # PLUTO doesn't carry unit lots; the address is inherited from the condo's
    # billing lot, and the page says so rather than passing the building's
    # identity off as the lot's.
    unit_note = ""
    if unit_lot:
        unit_note = (
            '<p class="section-sub" style="margin-top:2px;margin-bottom:14px;">'
            "This BBL is a condominium unit lot. Records below are the ones filed against "
            f'this lot; the building&#39;s own page is <a href="/property/{e(unit_lot["billing_bbl"])}">'
            f'BBL {e(unit_lot["billing_bbl"])}</a>.</p>'
        )

    head = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<meta name="robots" content="{robots}">
<title>{e(title)}</title>
<meta name="description" content="{e(desc)}">
<link rel="canonical" href="{url}">
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="{e(desc)}">
<meta property="og:url" content="{url}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="{og_image}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{e(title)}">
<meta name="twitter:description" content="{e(desc)}">
<meta name="twitter:image" content="{og_image}">
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%231a1a2e'/%3E%3Cpolyline points='2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16' fill='none' stroke='%23ed6317' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<script type="application/ld+json">{place_ld}</script>
<script type="application/ld+json">{bc_ld}</script>
{f'<script type="application/ld+json">{faq_ld}</script>' if faq_ld else ""}{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
"""

    css = """<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
h1,h2,h3{text-wrap:balance}
:root{--bg:#111823;--border:rgba(147,161,173,.1);--text:#eef2f5;--muted:#93a1ad;--dim:#8a97a2;--faint:#818c97;--accent:#ed6317}
body{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;line-height:1.6;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
nav{border-bottom:1px solid var(--border);padding:12px 0}
.nav-inner{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}
.nav-inner>div::-webkit-scrollbar{display:none}
.brand{font-size:.85rem;color:var(--faint)}
.container{max-width:720px;margin:0 auto;padding:28px 20px 72px}
.breadcrumb{font-size:.78rem;color:var(--muted);margin-bottom:18px}
.breadcrumb a:hover{color:var(--text)}
h1{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;font-weight:600;line-height:1.25;margin-bottom:6px}
.subline{font-size:.82rem;color:var(--muted);margin-bottom:22px;font-family:'JetBrains Mono',monospace}
.score-block{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap;padding:16px 20px;background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:8px;margin-bottom:26px}
.score-num{font-size:2.2rem;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1}
.score-denom{font-size:.9rem;color:var(--muted);font-family:'JetBrains Mono',monospace;align-self:flex-end;padding-bottom:3px}
.score-tier{font-size:0.75rem;font-weight:600;letter-spacing:.08em;align-self:flex-end;padding-bottom:5px;margin-left:8px}
h2{font-size:0.75rem;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--faint);margin-bottom:10px}
.section-sub{font-size:.82rem;color:var(--muted)}
.table-wrap{overflow-x:auto;margin-bottom:10px}
table{width:100%;border-collapse:collapse}
th{font-size:0.75rem;font-weight:500;text-transform:uppercase;letter-spacing:.06em;color:var(--faint);padding:6px 0;border-bottom:1px solid var(--border);text-align:left}
th:not(:first-child){text-align:right}
td{padding:11px 0;border-bottom:1px solid rgba(147,161,173,.06);vertical-align:top}
.sc{font-size:.86rem}
.sw{display:block;font-size:0.75rem;color:var(--faint);margin-top:2px}
.sr,.si{font-size:.85rem;font-family:'JetBrains Mono',monospace;text-align:right;white-space:nowrap}
.data-note{font-size:0.75rem;color:var(--faint);margin-top:8px;line-height:1.5}
.cite{color:var(--dim);white-space:normal}
.prose{font-size:.9rem;color:var(--muted);line-height:1.7;margin-bottom:10px;max-width:64ch}
.prose a{color:var(--accent)}
.prose a:hover{text-decoration:underline}
.faq-item h3{font-size:.9rem;font-weight:600;color:var(--text);margin:18px 0 4px}
.faq-item p{font-size:.86rem;color:var(--muted);line-height:1.7;max-width:64ch}
.sib-list{list-style:none;padding:0;margin:6px 0 0}
.sib-list .rec-row{border-bottom:1px solid rgba(147,161,173,.06)}
.sib-list a{display:block;padding:10px 0}
.sib-list a:hover .rec-addr{color:var(--accent)}
.rec-addr{font-family:'JetBrains Mono',monospace;font-size:.84rem;color:var(--text);overflow-wrap:anywhere}
.rec-geo{font-size:0.75rem;color:var(--faint);margin-top:2px}
.cta-row{display:flex;gap:10px;flex-wrap:wrap;margin:28px 0 4px}
.btn-map{display:inline-flex;align-items:center;padding:10px 18px;background:var(--accent);color:#fff;border-radius:6px;font-size:.84rem;font-weight:500}
.btn-map:hover{opacity:.9}
.btn-copy{display:inline-flex;align-items:center;padding:10px 18px;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:.84rem}
.btn-copy:hover{color:var(--text);border-color:rgba(147,161,173,.3)}
.foot-note{font-size:0.75rem;color:var(--faint);margin-top:20px;line-height:1.5}
.watch-card{background:rgba(237,99,23,.05);border:1px solid rgba(237,99,23,.22);border-radius:10px;padding:20px 22px;margin-bottom:30px}
.watch-h{color:var(--accent);font-size:.95rem;font-weight:600;margin-bottom:6px}
.watch-row{display:flex;gap:10px;margin-top:12px;flex-wrap:wrap}
.watch-row input{flex:1;min-width:180px;background:rgba(255,255,255,.03);border:1px solid var(--border);border-radius:6px;padding:10px 12px;color:var(--text);font-family:inherit;font-size:.85rem}
.watch-row input::placeholder{color:var(--faint)}
.watch-row input:focus{outline:none;border-color:var(--accent)}
.watch-row .btn-map{border:none;cursor:pointer;font-family:inherit}
.watch-msg{font-size:.8rem;margin-top:10px;line-height:1.5}
footer{border-top:1px solid var(--border);padding:24px 20px calc(env(safe-area-inset-bottom,0px) + 24px);text-align:center;margin-top:20px;font-size:12px;color:var(--muted)}
.footer-links{display:flex;justify-content:center;gap:20px;flex-wrap:wrap}
</style>
"""

    body = f"""</head>
<body>
{_ssr_nav()}
<main><div class="container">
<p class="breadcrumb">{crumb_html}</p>
<h1>{e(address)}</h1>
<p class="subline">{e(borough)}{(" &middot; " + e(zip_code)) if zip_code else ""} &middot; BBL {e(bbl)}</p>
{unit_note}
{score_block}
{lede}
{empty}{chain_sec}{own_sec}{tax_sec}{ev_prose}{ev_sec}{rs_sec}{viol_sec}{viol_table}{dec_sec}{pm_sec}{comp_sec}{cmp_sec}{sib_sec}
{watch_card}
{faq_sec}
<div class="cta-row">{links_html}</div>
<p class="foot-note">{body_note}</p>
</div></main>
{_FOOTER_HTML}
{watch_js}
</body>
</html>"""

    return head + css + body


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
               p.year_built, p.units_res, p.units_total, p.land_use,
               p.owner_name, p.assessed_total,
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

    unit_lot = None
    if not row:
        # Condo unit lots (17k deed BBLs) have no parcels row; the nightly
        # refresh recovers the building address from the block's single
        # billing lot. Everything else is a genuine 404.
        condo = db.execute(text("""
            SELECT c.address, c.zip_code, c.billing_bbl, ds.score
            FROM condo_unit_addresses c
            LEFT JOIN displacement_scores ds ON ds.zip_code = c.zip_code
            WHERE c.bbl = :bbl
        """), {"bbl": clean}).fetchone()
        if not condo:
            return _not_found()
        unit_lot = {"billing_bbl": condo.billing_bbl}
        address  = _addr_title(condo.address)
        zip_code = condo.zip_code or ""
        borough  = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn",
                    "4": "Queens", "5": "Staten Island"}.get(clean[0], "NYC")
        score    = float(condo.score) if condo.score is not None else None
    else:
        address  = row.address.title() if row.address else clean
        zip_code = row.zip_code or ""
        borough  = row.borough or "NYC"
        score    = float(row.score) if row.score is not None else None

    from api.routes.properties import _get_property_data
    sig = _get_property_data(clean, db).get("signals", {})
    op = db.execute(text(
        "SELECT o.slug, o.display_name, o.operator_root "
        "FROM operators o JOIN operator_parcels op ON op.operator_id = o.id "
        "WHERE op.bbl = :bbl AND o.operator_class = 'operator' LIMIT 1"
    ), {"bbl": clean}).fetchone()
    parcel = {
        "year_built": row.year_built, "units_res": row.units_res,
        "units_total": row.units_total, "land_use": row.land_use,
        "owner_name": row.owner_name, "assessed_total": row.assessed_total,
    } if row else None
    facts = _property_facts(clean, zip_code, db)
    siblings = _sibling_buildings(clean, op, db)
    html = _build_property_page(clean, address, zip_code, borough, score, sig, op,
                                parcel=parcel, facts=facts, siblings=siblings,
                                unit_lot=unit_lot, feeds=_feeds_through(db))

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
<meta name="theme-color" content="#111823">
<title>{name} | PulseCities</title>
<meta name="robots" content="noindex">
<link rel="canonical" href="https://pulsecities.com/operators">
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  h1,h2,h3{{text-wrap:balance}}
  body{{font-family:'DM Sans',sans-serif;background:#111823;color:#e4e8ec;line-height:1.7;
       min-height:100vh;display:flex;flex-direction:column}}
  a{{color:#6fb1d8;text-decoration:none}}
  a:hover{{text-decoration:underline}}
  nav{{border-bottom:1px solid rgba(147,161,173,0.12);padding:0 24px;height:52px;display:flex;align-items:center;gap:16px}}
  .brand{{font-size:14px;font-weight:600;color:#ed6317}}
  .wrap{{flex:1;max-width:620px;margin:0 auto;padding:72px 24px;width:100%}}
  .label{{font-family:'JetBrains Mono',monospace;font-size:12px;letter-spacing:0.1em;text-transform:uppercase;color:#93a1ad;margin-bottom:12px}}
  h1{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:clamp(22px,4vw,28px);font-weight:600;margin-bottom:10px}}
  .klass{{display:inline-block;font-family:'JetBrains Mono',monospace;font-size:12px;color:#c9d2da;
         border:1px solid rgba(147,161,173,0.25);border-radius:6px;padding:4px 10px;margin-bottom:20px}}
  p{{color:#93a1ad;font-size:15px}}
  .back{{display:inline-block;margin-top:28px;font-size:13px}}
  footer{{border-top:1px solid rgba(147,161,173,0.12);padding:24px;text-align:center;font-size:13px;color:#93a1ad}}
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
<meta name="theme-color" content="#111823">
<title>Operator not found | PulseCities</title>
<meta name="robots" content="noindex">
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  h1,h2,h3{{text-wrap:balance}}
  body{{font-family:'DM Sans',sans-serif;background:#111823;color:#e4e8ec;line-height:1.7;
       min-height:100vh;display:flex;flex-direction:column}}
  a{{color:#6fb1d8;text-decoration:none}}
  a:hover{{text-decoration:underline}}
  nav{{border-bottom:1px solid rgba(147,161,173,0.12);padding:0 24px;height:52px;display:flex;align-items:center}}
  .brand{{font-size:14px;font-weight:600;color:#ed6317}}
  .wrap{{flex:1;max-width:620px;margin:0 auto;padding:72px 24px;width:100%}}
  h1{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:clamp(22px,4vw,28px);font-weight:600;margin-bottom:10px}}
  p{{color:#93a1ad;font-size:15px}}
  .back{{display:inline-block;margin-top:28px;font-size:13px}}
  footer{{border-top:1px solid rgba(147,161,173,0.12);padding:24px;text-align:center;font-size:13px;color:#93a1ad}}
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
def operator_page_head(root: str, db: Session = Depends(get_db)):
    # Mirror the GET status so a HEAD probe sees dead and noise slugs as 404,
    # not a live 200. Body is discarded; the page is cached for the GET anyway.
    resp = operator_page(root, db)
    return Response(status_code=resp.status_code)


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
        # Link each acquisition out to its property page and ZIP page: turns the
        # operator table into internal links crawlers can follow into the money pages.
        addr_cell = f'<a href="/property/{_e(r.bbl)}" style="color:inherit;text-decoration:none;">{addr}</a>'
        zip_cell = (
            f'<a href="/neighborhood/{_e(r.zip_code)}" style="color:inherit;text-decoration:none;">{_e(r.zip_code)}</a>'
            if r.zip_code else ""
        )
        doc_date = r.doc_date.isoformat() if r.doc_date else ""
        amount = f"${int(r.doc_amount):,}" if r.doc_amount and float(r.doc_amount) > 0 else "N/A"
        acq_body += (
            "<tr>"
            f'<td style="padding:8px 16px;color:rgba(238,242,245,0.85);">{addr_cell}</td>'
            f'<td class="mono" style="padding:8px 16px;color:var(--dim);font-size:0.75rem;">{zip_cell}</td>'
            f'<td class="mono" style="padding:8px 16px;color:#93a1ad;font-size:0.75rem;">{_e(r.buyer or "")}</td>'
            f'<td class="mono" style="padding:8px 16px;color:var(--dim);font-size:0.75rem;">{_e(doc_date)}</td>'
            f'<td class="mono" style="padding:8px 8px 8px 16px;text-align:right;color:#93a1ad;font-size:0.75rem;">{_e(amount)}</td>'
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
    # Structured data: the operator page previously emitted none. A Dataset (the
    # acquisition record set) plus a BreadcrumbList make it eligible for rich
    # results and give the page an explicit place in the site hierarchy.
    op_ld = _jsonld({
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"{root_upper} NYC property acquisitions",
        "description": desc,
        "url": url,
        "creator": {"@type": "Person", "name": "Michael Espin", "url": "https://pulsecities.com"},
        "isBasedOn": "https://data.cityofnewyork.us/City-Government/ACRIS-Real-Property-Master/bnx9-e6tj",
    })
    bc_ld = _jsonld({
        "@context": "https://schema.org", "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://pulsecities.com/"},
            {"@type": "ListItem", "position": 2, "name": "Operators", "item": "https://pulsecities.com/operators"},
            {"@type": "ListItem", "position": 3, "name": root_upper, "item": url},
        ],
    })
    op_schema = (f'    <script type="application/ld+json">{op_ld}</script>\n'
                 f'    <script type="application/ld+json">{bc_ld}</script>')
    html = html.replace('</head>', f'{og_block}\n{op_schema}\n</head>', 1)

    # Inject the real operator data into the body so the served HTML is
    # substantive on first byte. The client JS overwrites these on hydration.
    html = html.replace(
        '<h1 id="op-root" class="mono accent" style="font-size: 1.8rem; font-weight: 400; letter-spacing: 0.04em;"></h1>',
        f'<h1 id="op-root" class="mono accent" style="font-size: 1.8rem; font-weight: 400; letter-spacing: 0.04em;">{_e(root_upper)}</h1>',
        1,
    )
    html = html.replace(
        '<div id="op-summary" style="font-size:0.82rem; color: #93a1ad; margin-top: 5px;"></div>',
        f'<div id="op-summary" style="font-size:0.82rem; color: #93a1ad; margin-top: 5px;">{_e(summary_html)}</div>',
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
    tot_acqs = sum(int(o.acqs or 0) for o in operators)
    tot_entities = sum(int(o.entities or 0) for o in operators)
    tot_zips = len({z for o in operators for z in (o.zips or [])})
    biggest = operators[0] if operators else None

    # This page ranked for "biggest landlords in nyc" on 72 visible words: a
    # list with nothing said about it. The questions below are the ones that
    # query is actually asking.
    dir_faq = [
        ("Who are the biggest landlords in NYC?",
         f"By measurable buying activity in the ACRIS deed record, PulseCities "
         f"tracks {n_visible} ownership networks accounting for {tot_acqs:,} "
         f"acquisitions across {tot_zips:,} ZIP codes. "
         + (f"{biggest.operator_root} leads the list with {int(biggest.acqs):,} "
            f"acquisitions held through {int(biggest.entities):,} separate LLCs. "
            if biggest else "")
         + "This ranks by recorded purchasing, not by total units owned: an "
           "owner who bought decades ago and has not transacted since will not "
           "appear."),
        ("Why is NYC property held through so many LLCs?",
         f"Buying each building through its own limited liability company is "
         f"ordinary practice: it separates liability, simplifies financing and "
         f"eases resale. The side effect is that a portfolio of "
         f"{tot_entities:,} entities can read as {tot_entities:,} unrelated "
         f"owners in the public record. Grouping them back together is the "
         f"point of this directory."),
        ("How does PulseCities group LLCs into one network?",
         "Entities are clustered where the public record supports it: shared "
         "naming stems across numbered siblings, shared filing addresses on the "
         "deeds, and overlapping acquisition activity. Financial institutions, "
         "government bodies, servicers taking title in foreclosure and "
         "nonprofit HDFCs are classified separately and excluded, so a bank "
         "that forecloses is never listed as a landlord."),
        ("What does an acquisition count actually measure?",
         "One recorded deed naming that network as the buyer. It counts tax "
         "lots, so a condominium purchased whole records one deed per unit and "
         "can inflate a raw count. Each profile page shows the buildings behind "
         "the number so the figure can be checked rather than taken."),
    ]
    dir_faq_html = "".join(
        f'<div class="dir-faq"><h3>{_html.escape(q)}</h3><p>{_html.escape(ans)}</p></div>'
        for q, ans in dir_faq
    )

    title = "Biggest NYC Landlords by Acquisition Volume | PulseCities"
    desc = (
        f"The biggest NYC landlords by acquisition volume: {n_visible} ownership networks "
        "grouped from ACRIS deed records, ranked by measurable buying activity."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [{
            "@type": "FAQPage",
            "mainEntity": [
                {"@type": "Question", "name": q,
                 "acceptedAnswer": {"@type": "Answer", "text": ans}}
                for q, ans in dir_faq
            ],
        }, {
            "@type": "ItemList",
            "name": "Biggest NYC landlords by acquisition volume",
            "description": desc,
            "url": "https://pulsecities.com/operators",
            "numberOfItems": n_visible,
            "itemListElement": list_items,
        }, _crumbs(("Home", "/"), ("Operators", "/operators"))],
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="https://pulsecities.com/operators">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/operators">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
body{{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}}
nav{{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.dir-h2{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.05rem;font-weight:600;margin-bottom:8px}}
.dir-p{{font-size:0.86rem;color:#93a1ad;line-height:1.7;margin-bottom:12px;max-width:64ch}}
.dir-p a{{color:#ed6317}}
.dir-p a:hover{{text-decoration:underline}}
.dir-faq h3{{font-size:0.9rem;font-weight:600;color:#e4e8ec;margin:18px 0 4px}}
.dir-faq p{{font-size:0.84rem;color:#93a1ad;line-height:1.7;max-width:64ch}}
.op-list{{list-style:none;padding:0;margin:0}}
.op-row{{border-bottom:1px solid rgba(147,161,173,0.07);cursor:pointer;}}
.op-row:hover{{background:rgba(147,161,173,0.04)}}
.op-row a{{display:flex;align-items:flex-start;gap:12px;padding:14px 0;text-decoration:none;color:inherit;}}
.op-rank{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);min-width:24px;padding-top:3px;flex-shrink:0;}}
.op-body{{display:flex;flex-direction:column;gap:3px;}}
.op-name{{font-family:'JetBrains Mono',monospace;font-size:0.88rem;color:#e4e8ec;letter-spacing:0.04em;font-weight:500;}}
.op-row:hover .op-name{{color:#ed6317;}}
.op-meta{{font-size:0.78rem;color:#93a1ad;}}
.op-geo{{font-size:0.75rem;color:var(--dim);}}
.op-cta{{font-size:0.75rem;color:var(--accent);font-family:'JetBrains Mono',monospace;margin-top:2px;}}
.op-row:hover .op-cta{{color:var(--accent);}}
</style>
</head>
<body>
{_ssr_nav("/operators", toggle_html=_LANG_TOGGLE_BTN)}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:var(--faint);">&#8592; Home</a>
  </div>
  <h1 id="dir-heading" style="font-size:1.4rem;font-weight:600;margin-bottom:6px;">The biggest NYC landlords</h1>
  <p id="dir-desc" style="font-size:0.82rem;color:#93a1ad;margin-bottom:8px;line-height:1.6;">
    Ownership clusters identified in NYC deed records. Each groups LLCs by naming patterns and acquisition activity. Public records only.
  </p>
  <p id="dir-sub" style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-bottom:8px;">{n_visible} clusters tracked across an 18-month public records window.</p>
  <p id="dir-ledger-link" style="font-size:0.78rem;margin-bottom:28px;">Looking for a specific company instead? <a href="/llc" style="color:#6fb1d8;">Search the full ledger of LLC buyers &rarr;</a></p>
  <ul class="op-list">
{rows_html}  </ul>

  <section style="margin-top:36px;">
    <h2 class="dir-h2">What this list is</h2>
    <p class="dir-p">These {n_visible} networks account for {tot_acqs:,} recorded
    acquisitions held through {tot_entities:,} separate limited liability
    companies across {tot_zips:,} NYC ZIP codes. Every one of those numbers comes
    from deeds filed with the city, so the list ranks measurable buying rather
    than total holdings: an owner who bought in 1988 and has not transacted
    since does not appear here at all.</p>
    <p class="dir-p">The reason a directory like this needs to exist is that NYC
    property is almost never held in the owner's own name. It is held one
    building at a time, each in its own LLC, which is ordinary practice and also
    why a single operation can appear in the record as thirty unrelated
    strangers. Reassembling those entities is what turns a deed filing into an
    answer about who owns the block.</p>
    <p class="dir-p">Financial institutions, government bodies, loan servicers
    taking title in foreclosure and nonprofit HDFCs are classified separately
    and kept off this list, because a bank completing a foreclosure is not a
    landlord acquiring a building. Looking for one specific company rather than
    a network? The <a href="/llc">full ledger of LLC buyers</a> is searchable by
    name, and every building has <a href="/who-owns-my-building">its own
    ownership page</a>.</p>
  </section>

  <section style="margin-top:32px;">
    <h2 class="dir-h2">Common questions</h2>
    {dir_faq_html}
  </section>
</div>
{_FOOTER_HTML}
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  var i18n = {{
    en: {{
      heading: 'The biggest NYC landlords',
      desc: 'Ownership clusters identified in NYC deed records. Each groups LLCs by naming patterns and acquisition activity. Public records only.',
      sub: '{n_visible} clusters tracked across an 18-month public records window.',
      acq: 'acquisitions',
      cta: 'View profile \\u2192',
      toggle: 'EN / ES'
    }},
    es: {{
      heading: 'Los mayores propietarios de NYC',
      desc: 'Grupos de propiedad identificados en registros de escrituras de NYC. Cada uno agrupa LLC por patrones de nombres y actividad de adquisición. Solo registros públicos.',
      sub: '{n_visible} grupos rastreados en una ventana de registros públicos de 18 meses.',
      acq: 'adquisiciones',
      cta: 'Ver perfil \\u2192',
      toggle: 'ES / EN'
    }}
  }};
  function applyLang(l) {{
    var s = i18n[l] || i18n.en;
    var h = document.getElementById('dir-heading');
    if (h) h.textContent = s.heading;
    var d = document.getElementById('dir-desc');
    if (d) d.textContent = s.desc;
    var sub = document.getElementById('dir-sub');
    if (sub) sub.textContent = s.sub;
    document.querySelectorAll('.op-label-acq').forEach(function(el) {{
      el.textContent = s.acq;
    }});
    document.querySelectorAll('.op-cta').forEach(function(el) {{
      el.textContent = s.cta;
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


_nbhd_index_cache: dict[str, tuple[str, float]] = {}  # lang -> (html, expires); cleared on restart


@router.get("/neighborhoods", include_in_schema=False)
def neighborhoods_directory(lang: str = "en", db: Session = Depends(get_db)):
    """Every scored ZIP page, grouped by borough, ranked by score.

    One crawlable hop from the homepage to all 177 neighborhood pages, and a
    scannable answer to "how does my area compare" without opening the map.
    """
    lang = "es" if lang == "es" else "en"
    LL = _LIST_L[lang]
    cached = _nbhd_index_cache.get(lang)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

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

    # ES readers keep their language while drilling into ZIP and borough pages.
    lsuf = "" if lang == "en" else "?lang=es"

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
                f'<li class="nb-row"><a href="/neighborhood/{r.zip_code}{lsuf}">'
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
        slug = borough.lower().replace(" ", "-")
        heading = (
            f'<a href="/{slug}{lsuf}" style="color:inherit;">{borough}</a>'
            if borough != "Other" else borough
        )
        sections_html += (
            f'<section class="nb-borough">'
            f'<h2>{heading}</h2>'
            f'<ul class="nb-list">\n{rows_html}</ul>'
            f'</section>\n'
        )

    n = len(rows)
    title = LL["dir_title"]
    desc = LL["dir_desc"].format(n=n)
    base_url = "https://pulsecities.com/neighborhoods"
    canonical = base_url if lang == "en" else f"{base_url}?lang=es"
    alt_url = f"{base_url}?lang=es" if lang == "en" else base_url
    nav_toggle = (
        f'<a href="{alt_url}" id="lang-toggle" aria-label="{LL["toggle_aria"]}" '
        f'style="font-size:0.78rem;color:var(--faint);" '
        f'onmouseover="this.style.color=\'#e4e8ec\'" '
        f'onmouseout="this.style.color=\'#93a1ad\'">{LL["toggle"]}</a>'
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [{
            "@type": "ItemList",
            "name": LL["dir_h1"],
            "description": desc,
            "url": canonical,
            "numberOfItems": n,
            "itemListElement": list_items,
        }, _crumbs(("Home", "/"), ("Neighborhoods", "/neighborhoods"))],
    })

    page = f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="{canonical}">
<link rel="alternate" hreflang="en" href="{base_url}">
<link rel="alternate" hreflang="es" href="{base_url}?lang=es">
<link rel="alternate" hreflang="x-default" href="{base_url}">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="{canonical}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
body{{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}}
nav{{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.nb-borough{{margin-bottom:36px}}
.nb-borough h2{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.05rem;font-weight:600;margin-bottom:10px;color:#e4e8ec}}
.nb-list{{list-style:none;padding:0;margin:0}}
.nb-row{{border-bottom:1px solid rgba(147,161,173,0.07)}}
.nb-row:hover{{background:rgba(147,161,173,0.04)}}
.nb-row a{{display:grid;grid-template-columns:56px 1fr 52px;grid-template-rows:auto auto;column-gap:14px;row-gap:5px;align-items:baseline;padding:10px 0}}
.nb-zip{{font-family:'JetBrains Mono',monospace;font-size:0.85rem;font-weight:500;color:#e4e8ec}}
.nb-row:hover .nb-zip{{color:#ed6317}}
.nb-name{{font-size:0.82rem;color:#93a1ad;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.nb-score{{font-family:'JetBrains Mono',monospace;font-size:0.85rem;font-weight:600;text-align:right}}
.nb-track{{grid-column:1 / -1;display:block;height:3px;border-radius:2px;background:rgba(147,161,173,0.1);overflow:hidden}}
.nb-fill{{display:block;height:100%;border-radius:2px}}
</style>
</head>
<body>
{_ssr_nav("/neighborhoods", lang=lang, toggle_html=nav_toggle)}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:var(--faint);">{LL['back_home']}</a>
  </div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">{LL['dir_h1']}</h1>
  <p style="font-size:0.82rem;color:#93a1ad;margin-bottom:8px;line-height:1.6;">
    {LL['dir_intro']}
  </p>
  <p style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-bottom:28px;">{LL['dir_count'].format(n=n)}</p>
  {sections_html}
</div>
{_FOOTERS.get(lang, _FOOTER_HTML)}
<script>
(function() {{
  var toggle = document.getElementById('lang-toggle');
  if (toggle) toggle.addEventListener('click', function() {{
    try {{ localStorage.setItem('pc-lang', '{lang}' === 'en' ? 'es' : 'en'); }} catch (err) {{}}
  }});
  if ('{lang}' === 'en' && location.search.indexOf('lang=') === -1) {{
    try {{
      if (localStorage.getItem('pc-lang') === 'es') location.replace('{base_url}?lang=es');
    }} catch (err) {{}}
  }}
}})();
</script>
</body>
</html>"""

    _nbhd_index_cache[lang] = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


_BOROUGH_SLUGS = {
    "brooklyn":      "Brooklyn",
    "manhattan":     "Manhattan",
    "queens":        "Queens",
    "bronx":         "Bronx",
    "staten-island": "Staten Island",
}

# Copy for the two ranking-list pages (/neighborhoods and the borough pages).
# NYC Spanish usage says "condado" for borough (nyc.gov's own convention).
_LIST_L = {
    "en": {
        "dir_title": "NYC Neighborhoods by Displacement Score | PulseCities",
        "dir_desc": ("Displacement-pressure scores for all {n} scored NYC ZIP codes, grouped by "
                     "borough and ranked by current score. Built from public records, refreshed nightly."),
        "dir_h1": "NYC neighborhoods by displacement score",
        "dir_intro": ("Every scored ZIP in the city, grouped by borough and ranked by current "
                      "displacement pressure. Each page shows the signal breakdown, the six-month "
                      "trend, and an embeddable score badge."),
        "dir_count": "{n} ZIP codes scored nightly from public records.",
        "back_home": "&#8592; Home",
        "back_all": "&#8592; All neighborhoods",
        "nav_map": "Map", "nav_operators": "Operators", "nav_flips": "Flips",
        "nav_radar": "Radar", "nav_meth": "Methodology", "nav_nbhds": "Neighborhoods",
        "b_title": "{borough} Displacement Risk by ZIP Code | PulseCities",
        "b_desc": ("Displacement-pressure scores for all {n} scored ZIP codes in {borough}, "
                   "ranked by current score. Highest right now: {top} ({zip}) at {s}/100. "
                   "Public records, refreshed nightly."),
        "b_h1": "{borough} displacement risk by ZIP",
        "b_intro": ("Every scored ZIP in {borough}, ranked by current displacement pressure. "
                    "Scores come from six public-record signals and refresh nightly. Open any "
                    "ZIP for its signal breakdown and six-month trend."),
        "b_stat_zips": "ZIPs scored", "b_stat_avg": "Borough average", "b_stat_top": "Highest: {name}",
        "b_others": "Other boroughs:",
        "toggle": "ES", "toggle_aria": "Ver esta página en español",
    },
    "es": {
        "dir_title": "Vecindarios de NYC por puntuación de desplazamiento | PulseCities",
        "dir_desc": ("Puntuaciones de presión de desplazamiento para los {n} códigos postales "
                     "puntuados de NYC, agrupados por condado y ordenados por puntuación actual. "
                     "Construido con registros públicos, actualizado cada noche."),
        "dir_h1": "Vecindarios de NYC por puntuación de desplazamiento",
        "dir_intro": ("Cada ZIP puntuado de la ciudad, agrupado por condado y ordenado por la "
                      "presión de desplazamiento actual. Cada página muestra el desglose de "
                      "señales, la tendencia de seis meses y una insignia insertable."),
        "dir_count": "{n} códigos postales puntuados cada noche con registros públicos.",
        "back_home": "&#8592; Inicio",
        "back_all": "&#8592; Todos los vecindarios",
        "nav_map": "Mapa", "nav_operators": "Operadores", "nav_flips": "Flips",
        "nav_radar": "Radar", "nav_meth": "Metodología", "nav_nbhds": "Vecindarios",
        "b_title": "Riesgo de desplazamiento en {borough} por código postal | PulseCities",
        "b_desc": ("Puntuaciones de presión de desplazamiento para los {n} códigos postales "
                   "puntuados de {borough}, ordenados por puntuación actual. El más alto ahora: "
                   "{top} ({zip}) con {s}/100. Registros públicos, actualizados cada noche."),
        "b_h1": "Riesgo de desplazamiento en {borough} por ZIP",
        "b_intro": ("Cada ZIP puntuado en {borough}, ordenado por la presión de desplazamiento "
                    "actual. Las puntuaciones provienen de seis señales de registros públicos y "
                    "se actualizan cada noche. Abre cualquier ZIP para ver su desglose de señales "
                    "y su tendencia de seis meses."),
        "b_stat_zips": "ZIPs puntuados", "b_stat_avg": "Promedio del condado", "b_stat_top": "Más alto: {name}",
        "b_others": "Otros condados:",
        "toggle": "EN", "toggle_aria": "View this page in English",
    },
}

_borough_page_cache: dict[str, tuple[str, float]] = {}  # slug -> (html, expires)


@router.get("/borough/{slug}", include_in_schema=False)
def borough_page(slug: str, lang: str = "en", db: Session = Depends(get_db)):
    """Borough-level ranking page, served at /brooklyn etc. via nginx.

    Matches how people actually search ("brooklyn displacement data") and
    gives every neighborhood page a mid-tier parent in the link graph.
    """
    borough = _BOROUGH_SLUGS.get(slug)
    if not borough:
        return _not_found()

    lang = "es" if lang == "es" else "en"
    LL = _LIST_L[lang]
    lsuf = "" if lang == "en" else "?lang=es"
    cache_key = f"{slug}:{lang}"
    cached = _borough_page_cache.get(cache_key)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    from api.routes.neighborhoods import _borough_from_zip

    rows = [
        r for r in db.execute(text("""
            SELECT n.zip_code, n.name, ds.score
            FROM neighborhoods n
            JOIN displacement_scores ds ON ds.zip_code = n.zip_code
            WHERE ds.score IS NOT NULL
            ORDER BY ds.score DESC
        """)).fetchall()
        if _borough_from_zip(r.zip_code) == borough
    ]
    if not rows:
        return _not_found()

    n = len(rows)
    avg = sum(float(r.score) for r in rows) / n
    top = rows[0]
    canonical = f"https://pulsecities.com/{slug}"

    rows_html = ""
    list_items = []
    for i, r in enumerate(rows, 1):
        score = float(r.score)
        _, color = _tier_info(score)
        name = _html.escape(r.name or r.zip_code)
        width = max(2, min(100, score))
        rows_html += (
            f'<li class="nb-row"><a href="/neighborhood/{r.zip_code}{lsuf}">'
            f'<span class="nb-zip">{r.zip_code}</span>'
            f'<span class="nb-name">{name}</span>'
            f'<span class="nb-score" style="color:{color};">{score:.1f}</span>'
            f'<span class="nb-track"><span class="nb-fill" style="width:{width}%;background:{color};"></span></span>'
            f'</a></li>\n'
        )
        list_items.append({
            "@type": "ListItem",
            "position": i,
            "name": f"{r.name or r.zip_code} ({r.zip_code}) displacement score",
            "url": f"https://pulsecities.com/neighborhood/{r.zip_code}",
        })

    title = LL["b_title"].format(borough=borough)
    desc = LL["b_desc"].format(n=n, borough=borough, top=top.name or top.zip_code,
                               zip=top.zip_code, s=f"{float(top.score):.1f}")
    base_url = canonical
    page_url = base_url if lang == "en" else f"{base_url}?lang=es"
    alt_url = f"{base_url}?lang=es" if lang == "en" else base_url
    nav_toggle = (
        f'<a href="{alt_url}" id="lang-toggle" aria-label="{LL["toggle_aria"]}" '
        f'style="font-size:0.78rem;color:var(--faint);" '
        f'onmouseover="this.style.color=\'#e4e8ec\'" '
        f'onmouseout="this.style.color=\'#93a1ad\'">{LL["toggle"]}</a>'
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "ItemList",
                "name": f"{borough} neighborhoods by displacement score",
                "description": desc,
                "url": canonical,
                "numberOfItems": n,
                "itemListElement": list_items,
            },
            {
                "@type": "BreadcrumbList",
                "itemListElement": [
                    {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://pulsecities.com/"},
                    {"@type": "ListItem", "position": 2, "name": "Neighborhoods", "item": "https://pulsecities.com/neighborhoods"},
                    {"@type": "ListItem", "position": 3, "name": borough, "item": canonical},
                ],
            },
        ],
    })

    others = " · ".join(
        f'<a href="/{s}{lsuf}" style="color:var(--dim);">{b}</a>'
        for s, b in _BOROUGH_SLUGS.items() if s != slug
    )

    page = f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="{page_url}">
<link rel="alternate" hreflang="en" href="{base_url}">
<link rel="alternate" hreflang="es" href="{base_url}?lang=es">
<link rel="alternate" hreflang="x-default" href="{base_url}">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="{page_url}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og/borough/{slug}.png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og/borough/{slug}.png">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
body{{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}}
nav{{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.stat-row{{display:flex;gap:28px;flex-wrap:wrap;margin-bottom:28px}}
.stat{{display:flex;flex-direction:column;gap:2px}}
.stat-num{{font-family:'JetBrains Mono',monospace;font-size:1.15rem;font-weight:600;color:#e4e8ec}}
.stat-label{{font-size:0.75rem;color:var(--faint);text-transform:uppercase;letter-spacing:0.06em}}
.nb-list{{list-style:none;padding:0;margin:0}}
.nb-row{{border-bottom:1px solid rgba(147,161,173,0.07)}}
.nb-row:hover{{background:rgba(147,161,173,0.04)}}
.nb-row a{{display:grid;grid-template-columns:56px 1fr 52px;grid-template-rows:auto auto;column-gap:14px;row-gap:5px;align-items:baseline;padding:10px 0}}
.nb-zip{{font-family:'JetBrains Mono',monospace;font-size:0.85rem;font-weight:500;color:#e4e8ec}}
.nb-row:hover .nb-zip{{color:#ed6317}}
.nb-name{{font-size:0.82rem;color:#93a1ad;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.nb-score{{font-family:'JetBrains Mono',monospace;font-size:0.85rem;font-weight:600;text-align:right}}
.nb-track{{grid-column:1 / -1;display:block;height:3px;border-radius:2px;background:rgba(147,161,173,0.1);overflow:hidden}}
.nb-fill{{display:block;height:100%;border-radius:2px}}
</style>
</head>
<body>
{_ssr_nav("/neighborhoods", lang=lang, toggle_html=nav_toggle)}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/neighborhoods{lsuf}" style="font-size:0.75rem;color:var(--faint);">{LL['back_all']}</a>
  </div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">{LL['b_h1'].format(borough=borough)}</h1>
  <p style="font-size:0.82rem;color:#93a1ad;margin-bottom:20px;line-height:1.6;">
    {LL['b_intro'].format(borough=borough)}
  </p>
  <div class="stat-row">
    <div class="stat"><span class="stat-num">{n}</span><span class="stat-label">{LL['b_stat_zips']}</span></div>
    <div class="stat"><span class="stat-num">{avg:.1f}</span><span class="stat-label">{LL['b_stat_avg']}</span></div>
    <div class="stat"><span class="stat-num">{float(top.score):.1f}</span><span class="stat-label">{LL['b_stat_top'].format(name=_html.escape(top.name or top.zip_code))}</span></div>
  </div>
  <ul class="nb-list">
{rows_html}  </ul>
  <p style="font-size:0.75rem;color:var(--faint);margin-top:24px;">{LL['b_others']} {others}</p>
</div>
{_FOOTERS.get(lang, _FOOTER_HTML)}
<script>
(function() {{
  var toggle = document.getElementById('lang-toggle');
  if (toggle) toggle.addEventListener('click', function() {{
    try {{ localStorage.setItem('pc-lang', '{lang}' === 'en' ? 'es' : 'en'); }} catch (err) {{}}
  }});
  if ('{lang}' === 'en' && location.search.indexOf('lang=') === -1) {{
    try {{
      if (localStorage.getItem('pc-lang') === 'es') location.replace('{base_url}?lang=es');
    }} catch (err) {{}}
  }}
}})();
</script>
</body>
</html>"""

    _borough_page_cache[cache_key] = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


_flips_cache: tuple[str, float] | None = None  # cleared on restart


# Two shapes of citation prefix, because not every description uses a colon:
# "§ 27-2005, 27-2007 HMC ... § 25-171: REPLACE OR REPAIR" and
# "§ 27-2013 ADM CODE PAINT WITH LIGHT COLORED PAINT".
_VIOL_COLON = re.compile(r"^[^:]{0,140}:\s*")
# Consumes citation tokens one at a time until it reaches a word, which is more
# forgiving than one pattern for the whole prefix: a citation can read
# "§ 27-2045(B)(1)(B) HMC, § 12-06, § 12-07, § 12-09 RCNY" and a single greedy
# character class either stops at the first letter inside a paren or eats the
# sentence.
_VIOL_CITE = re.compile(
    r"^(?:[&§]"
    r"|\d+[\d\-.]*"
    r"|\(?[A-Za-z0-9]{1,4}\)"
    r"|(?:HMC|ADM\s+CODE|M/D\s+LAW|MDL|RCNY|LAW)\b"
    r"|[,;:.\s]+)+",
    re.IGNORECASE)


def _violation_text(desc: str) -> str:
    """The half of an HPD violation a tenant can read.

    Every description opens with the statute it was written under, so the rows
    would otherwise read "§ 27-2005, 27-2007, 27-2041.1 HMC, §238" eight times
    down the page and say nothing about any of them. The inspector's wording is
    kept verbatim after that; only the case is normalised, and only where the
    record is shouting.
    """
    body = (desc or "").strip()
    if ":" in body[:140]:
        body = _VIOL_COLON.sub("", body)
    # The citation run survives a colon strip as often as it replaces one:
    # "(a) § HMC:FILE ANNUAL BEDBUG REPORT" and "B)(5) HMC, § 12-06 RCNY POST".
    body = _VIOL_CITE.sub("", body).lstrip(" ()[],.;:-")
    body = body.strip(" .,;")
    if not body:
        return "Violation issued"
    letters = [ch for ch in body if ch.isalpha()]
    if letters and sum(ch.isupper() for ch in letters) / len(letters) > 0.8:
        body = body[:1].upper() + body[1:].lower()
    return body[:110].rstrip(" ,;") + "\u2026" if len(body) > 110 else body


def _fmt_amount(v) -> str:
    """Compact money label: $2.4M, $815K. Empty string when the deed had no price."""
    if not v:
        return ""
    v = float(v)
    if v >= 1_000_000_000:
        return f"${v / 1_000_000_000:.2f}B".replace(".00B", "B")
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

    # The 12-month lookback runs to today; the deeds in it stop wherever the
    # city last published. "Updated nightly" is true of the job, not the record.
    through_en = _deeds_through_line(db)
    through_es = _deeds_through_line(db, "es")
    through_en_js = through_en.replace("'", "\\'")
    through_es_js = through_es.replace("'", "\\'").encode("ascii", "backslashreplace").decode()

    _MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _short_date(iso: str | None) -> str:
        if not iso:
            return ""
        try:
            y, m, d = iso.split("-")
            return f"{_MONTHS[int(m)]} {int(d)}, {y}"
        except (ValueError, IndexError):
            return _html.escape(iso)

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

    # "Flip Watch" is a name we invented; nobody types it. The H1 and nav
    # keep the brand, the title says what the page is.
    title = "NYC renovation flips: LLC bought, permit within 60 days | PulseCities"
    desc = (
        f"{n} NYC buildings where an LLC bought and filed a renovation permit within "
        f"{FLIP_WINDOW_DAYS} days, sourced from ACRIS deeds and DOB permits. Updated nightly."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [{
            "@type": "ItemList",
            "name": "NYC Flip Watch",
            "description": desc,
            "url": "https://pulsecities.com/flips",
            "numberOfItems": n,
            "itemListElement": list_items,
        }, _crumbs(("Home", "/"), ("Flip Watch", "/flips"))],
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="https://pulsecities.com/flips">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/flips">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
body{{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}}
nav{{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.flip-list{{list-style:none;padding:0;margin:0}}
.flip-row{{border-bottom:1px solid rgba(147,161,173,0.07);cursor:pointer;}}
.flip-row:hover{{background:rgba(147,161,173,0.04)}}
.flip-row a{{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;padding:16px 0;text-decoration:none;color:inherit;}}
.flip-main{{display:flex;flex-direction:column;gap:3px;min-width:0;}}
.flip-addr{{font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#e4e8ec;letter-spacing:0.03em;font-weight:500;}}
.flip-row:hover .flip-addr{{color:#ed6317;}}
.flip-geo{{font-size:0.76rem;color:var(--dim);}}
.flip-buyer{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:#93a1ad;margin-top:2px;}}
.flip-when{{font-size:0.75rem;color:var(--faint);margin-top:2px;}}
.flip-when-label{{color:var(--faint);}}
.flip-side{{display:flex;flex-direction:column;align-items:flex-end;flex-shrink:0;text-align:right;}}
.flip-gap{{font-family:'JetBrains Mono',monospace;font-size:1.05rem;font-weight:500;color:#ed6317;line-height:1.1;}}
.flip-gap-label{{font-size:0.75rem;color:var(--faint);text-transform:uppercase;letter-spacing:0.06em;margin-top:1px;}}
.flip-amount{{font-family:'JetBrains Mono',monospace;font-size:0.78rem;color:#c9d2da;margin-top:8px;}}
.flip-empty{{padding:24px 0;font-size:0.82rem;color:#93a1ad;}}
</style>
</head>
<body>
{_ssr_nav("/flips", toggle_html=_LANG_TOGGLE_BTN)}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:var(--faint);">&#8592; Home</a>
  </div>
  <h1 id="fw-heading" style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">Flip Watch</h1>
  <p id="fw-desc" style="font-size:0.82rem;color:#93a1ad;margin-bottom:8px;line-height:1.6;">
    Buildings where an LLC took the deed and filed a renovation permit within {FLIP_WINDOW_DAYS} days. That fast turn is one of the clearest early signals of a building being repositioned. Public records only.
  </p>
  <p id="fw-sub" style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-bottom:6px;">{n} flips detected across NYC in the past 12 months.</p>
  <p id="fw-through" style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-bottom:6px;">{through_en}</p>
  <p style="font-size:0.75rem;margin-bottom:28px;"><a href="/flips/editions" id="fw-editions-link" style="color:var(--accent);">Weekly reviewed editions &rarr;</a></p>
  <ul class="flip-list">
{rows_html}  </ul>
  <p id="fw-note" style="font-size:0.75rem;color:var(--faint);margin-top:24px;line-height:1.6;">
    A renovation permit alone is not wrongdoing. This page reports the public-record pattern, not a conclusion about any owner. <a href="/methodology" style="color:var(--accent);">How this is measured &rarr;</a>
  </p>
</div>
{_FOOTER_HTML}
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  var i18n = {{
    en: {{
      heading: 'Flip Watch',
      desc: 'Buildings where an LLC took the deed and filed a renovation permit within {FLIP_WINDOW_DAYS} days. That fast turn is one of the clearest early signals of a building being repositioned. Public records only.',
      sub: '{n} flips detected across NYC in the past 12 months.',
      through: '{through_en_js}',
      note: 'A renovation permit alone is not wrongdoing. This page reports the public-record pattern, not a conclusion about any owner.',
      bought: 'Bought',
      gap: 'buy \\u2192 permit',
      toggle: 'EN / ES'
    }},
    es: {{
      heading: 'Vigilancia de reventas',
      desc: 'Edificios donde una LLC tom\\u00f3 la escritura y solicit\\u00f3 un permiso de renovaci\\u00f3n en un plazo de {FLIP_WINDOW_DAYS} d\\u00edas. Ese giro r\\u00e1pido es una de las se\\u00f1ales tempranas m\\u00e1s claras de que un edificio est\\u00e1 siendo reposicionado. Solo registros p\\u00fablicos.',
      sub: '{n} reventas detectadas en NYC en los \\u00faltimos 12 meses.',
      through: '{through_es_js}',
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
    set('fw-through', s.through);
    var d = document.getElementById('fw-desc'); if (d) d.textContent = s.desc;
    var note = document.getElementById('fw-note');
    if (note) note.innerHTML = s.note + ' <a href="/methodology" style="color:var(--accent);">' + (l === 'es' ? 'C\\u00f3mo se mide \\u2192' : 'How this is measured \\u2192') + '</a>';
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


# Editions cache is short: an approval should reach the page within minutes.
_editions_page_cache: tuple[str, float] | None = None
_EDITIONS_TTL = 600


@router.get("/flips/editions", include_in_schema=False)
def flips_editions_page(db: Session = Depends(get_db)):
    """Eviction Flips editions — the human-reviewed weekly archive.

    Renders approved editions only, newest first. Each arc is the full
    paper trail: eviction, LLC purchase, resale, with ACRIS document IDs.
    The weekly scan writes editions with approved: false; nothing shows
    here until a human has reviewed it.
    """
    global _editions_page_cache
    if _editions_page_cache and time.monotonic() < _editions_page_cache[1]:
        return HTMLResponse(_editions_page_cache[0])

    from api.routes.flips import _EDITIONS_PATH, _BOROUGHS
    try:
        editions = json.loads(_EDITIONS_PATH.read_text()).get("editions", [])
    except (OSError, ValueError):
        editions = []
    approved = [e for e in editions if e.get("approved") and e.get("arcs")]
    approved.reverse()

    zips = {a.get("zip_code") for e in approved for a in e["arcs"] if a.get("zip_code")}
    hood_by_zip = {}
    if zips:
        rows = db.execute(
            text("SELECT zip_code, name FROM neighborhoods WHERE zip_code = ANY(:zips)"),
            {"zips": list(zips)},
        ).fetchall()
        hood_by_zip = {r.zip_code: r.name for r in rows}

    _MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _short_date(iso: str | None) -> str:
        if not iso:
            return ""
        try:
            y, m, d = iso.split("-")
            return f"{_MONTHS[int(m)]} {int(d)}, {y}"
        except (ValueError, IndexError):
            return _html.escape(iso)

    def _days_between(a: str, b: str) -> int | None:
        try:
            return (date.fromisoformat(b) - date.fromisoformat(a)).days
        except (ValueError, TypeError):
            return None

    sections_html = ""
    list_items = []
    pos = 0
    total_arcs = 0
    for ed in approved:
        week = _html.escape(ed.get("week", ""))
        generated = _short_date(ed.get("generated"))
        arcs = sorted(ed["arcs"], key=lambda a: a.get("gain_pct") or 0, reverse=True)
        total_arcs += len(arcs)
        cards = ""
        for a in arcs:
            pos += 1
            bbl = _html.escape(str(a.get("bbl", "")))
            addr = _html.escape((a.get("address") or f"BBL {a.get('bbl')}").title())
            zip_code = a.get("zip_code") or ""
            hood = hood_by_zip.get(zip_code)
            borough = _BOROUGHS.get(str(a.get("bbl", ""))[:1], "")
            place_bits = [b for b in (hood, borough) if b]
            geo = _html.escape(" · ".join(place_bits) + (f" · {zip_code}" if zip_code else ""))
            days = _days_between(a.get("buy_date"), a.get("sell_date"))
            gain = f"+{int(a.get('gain_pct') or 0)}%"
            gain_days = f"{days}" if days is not None else ""
            ev_n = int(a.get("eviction_count") or 1)
            buyer = _html.escape(a.get("buyer") or "an LLC")
            buy_amt = _fmt_amount(a.get("buy_amt"))
            sell_amt = _fmt_amount(a.get("sell_amt"))
            ev_line = (
                f"The latest of {ev_n} residential evictions on record is executed."
                if ev_n > 1 else "A city marshal executes a residential eviction."
            )
            cards += f"""
<article class="arc-card">
  <div class="arc-head">
    <a class="arc-addr" href="/property/{bbl}">{addr}</a>
    <span class="arc-gain" data-gain="{gain}" data-days="{gain_days}">{gain}{f' in {days} days' if days is not None else ''}</span>
  </div>
  <div class="arc-geo">{geo} &middot; BBL {bbl}</div>
  <ol class="arc-steps">
    <li><span class="arc-date" data-date="{_html.escape(a.get('eviction_date') or '')}">{_short_date(a.get('eviction_date'))}</span>
        <span class="arc-line" data-t="{'ev_many' if ev_n > 1 else 'ev_one'}" data-n="{ev_n}">{ev_line}</span></li>
    <li><span class="arc-date" data-date="{_html.escape(a.get('buy_date') or '')}">{_short_date(a.get('buy_date'))}</span>
        <span class="arc-line" data-t="buy" data-buyer="{buyer}" data-amt="{buy_amt}">{buyer} buys the property for {buy_amt}.</span></li>
    <li><span class="arc-date" data-date="{_html.escape(a.get('sell_date') or '')}">{_short_date(a.get('sell_date'))}</span>
        <span class="arc-line" data-t="sell" data-amt="{sell_amt}">The LLC resells for {sell_amt}.</span></li>
  </ol>
  <div class="arc-ids">ACRIS {_html.escape(a.get('buy_doc') or '')} &middot; {_html.escape(a.get('sell_doc') or '')}</div>
</article>"""
            list_items.append({
                "@type": "ListItem",
                "position": pos,
                "name": f"{(a.get('address') or '').title()} eviction flip",
                "url": f"https://pulsecities.com/property/{a.get('bbl')}",
            })
        sections_html += f"""
<section class="edition">
  <h2 class="edition-week">{week}<span class="edition-date">, published {generated}</span></h2>
{cards}
</section>"""

    if not sections_html:
        sections_html = ('<p class="ed-empty" id="ed-empty">No reviewed editions yet. '
                         'The first one publishes after human review of the weekly scan.</p>')

    title = "Eviction Flips: weekly editions | PulseCities"
    desc = (
        f"{total_arcs} verified eviction-to-resale arcs across NYC: a residential eviction, "
        "an LLC purchase, and a markup resale, each backed by ACRIS document IDs. "
        "Human-reviewed weekly."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [{
            "@type": "ItemList",
            "name": "NYC Eviction Flips, weekly editions",
            "description": desc,
            "url": "https://pulsecities.com/flips/editions",
            "numberOfItems": total_arcs,
            "itemListElement": list_items,
        }, _crumbs(("Home", "/"), ("Flip Watch", "/flips"), ("Weekly editions", "/flips/editions"))],
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="https://pulsecities.com/flips/editions">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/flips/editions">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
body{{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh;line-height:1.65}}
nav{{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.edition{{margin-bottom:36px}}
.edition-week{{font-family:'JetBrains Mono',monospace;font-size:0.85rem;font-weight:600;color:#ed6317;letter-spacing:0.06em;margin-bottom:14px;text-transform:uppercase}}
.edition-date{{color:var(--faint);font-weight:400;text-transform:none;letter-spacing:0}}
.arc-card{{background:#16202d;border:1px solid rgba(147,161,173,0.12);border-radius:12px;padding:16px 18px 14px;margin-bottom:12px}}
.arc-head{{display:flex;align-items:baseline;justify-content:space-between;gap:12px;flex-wrap:wrap}}
.arc-addr{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.02rem;font-weight:600;color:#e4e8ec}}
.arc-addr:hover{{color:#ed6317}}
.arc-gain{{font-family:'JetBrains Mono',monospace;font-size:0.8rem;font-weight:600;color:#e4483b;border:1.5px solid rgba(228,72,59,0.5);border-radius:4px;padding:1px 8px;white-space:nowrap}}
.arc-geo{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin:2px 0 12px}}
.arc-steps{{list-style:none;margin:0 0 12px;display:flex;flex-direction:column;gap:7px}}
.arc-steps li{{font-size:0.85rem;color:#c9d2da}}
.arc-date{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:#ed6317;display:block}}
.arc-ids{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);border-top:1px solid rgba(147,161,173,0.08);padding-top:10px;word-break:break-word}}
.ed-empty{{font-size:0.85rem;color:#93a1ad;padding:24px 0}}
</style>
</head>
<body>
{_ssr_nav("/flips", toggle_html=_LANG_TOGGLE_BTN)}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/flips" style="font-size:0.75rem;color:var(--faint);">&#8592; Flip Watch</a>
  </div>
  <h1 id="ed-heading" style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">Eviction Flips: weekly editions</h1>
  <p id="ed-desc" style="font-size:0.82rem;color:#93a1ad;margin-bottom:28px;line-height:1.6;">
    The arc this site exists to document: a residential eviction, an LLC purchase, and a markup resale on the same lot. Every step is a public record with its ACRIS document ID. A new edition publishes each week after human review; nothing appears here unreviewed.
  </p>
{sections_html}
  <p id="ed-note" style="font-size:0.75rem;color:var(--faint);margin-top:24px;line-height:1.6;">
    An eviction followed by a sale is not by itself wrongdoing. This page reports the public-record pattern, not a conclusion about any owner. <a href="/methodology" style="color:var(--accent);">How this is measured &rarr;</a>
  </p>
</div>
{_FOOTER_HTML}
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  var i18n = {{
    en: {{
      heading: 'Eviction Flips: weekly editions',
      desc: 'The arc this site exists to document: a residential eviction, an LLC purchase, and a markup resale on the same lot. Every step is a public record with its ACRIS document ID. A new edition publishes each week after human review; nothing appears here unreviewed.',
      note: 'An eviction followed by a sale is not by itself wrongdoing. This page reports the public-record pattern, not a conclusion about any owner.',
      how: 'How this is measured \\u2192',
      gain_in: '{{gain}} in {{days}} days',
      ev_one: 'A city marshal executes a residential eviction.',
      ev_many: 'The latest of {{n}} residential evictions on record is executed.',
      buy: '{{buyer}} buys the property for {{amt}}.',
      sell: 'The LLC resells for {{amt}}.',
      toggle: 'EN / ES'
    }},
    es: {{
      heading: 'Reventas tras desalojo: ediciones semanales',
      desc: 'El arco que este sitio existe para documentar: un desalojo residencial, una compra por una LLC y una reventa con sobreprecio en el mismo lote. Cada paso es un registro p\\u00fablico con su ID de documento ACRIS. Cada semana se publica una edici\\u00f3n tras revisi\\u00f3n humana; nada aparece aqu\\u00ed sin revisar.',
      note: 'Un desalojo seguido de una venta no es en s\\u00ed una irregularidad. Esta p\\u00e1gina reporta el patr\\u00f3n del registro p\\u00fablico, no una conclusi\\u00f3n sobre ning\\u00fan propietario.',
      how: 'C\\u00f3mo se mide \\u2192',
      gain_in: '{{gain}} en {{days}} d\\u00edas',
      ev_one: 'Un alguacil de la ciudad ejecuta un desalojo residencial.',
      ev_many: 'Se ejecuta el \\u00faltimo de {{n}} desalojos residenciales registrados.',
      buy: '{{buyer}} compra la propiedad por {{amt}}.',
      sell: 'La LLC la revende por {{amt}}.',
      toggle: 'EN / ES'
    }}
  }};
  function fill(t, params) {{
    return t.replace(/\\{{(\\w+)\\}}/g, function(_, k) {{ return params[k] != null ? params[k] : ''; }});
  }}
  function applyLang(l) {{
    var s = i18n[l] || i18n.en;
    var set = function(id, val) {{ var el = document.getElementById(id); if (el) el.textContent = val; }};
    set('ed-heading', s.heading);
    set('ed-desc', s.desc);
    var note = document.getElementById('ed-note');
    if (note) note.innerHTML = s.note + ' <a href="/methodology" style="color:var(--accent);">' + s.how + '</a>';
    document.querySelectorAll('.arc-line').forEach(function(el) {{
      var t = s[el.dataset.t];
      if (t) el.textContent = fill(t, el.dataset);
    }});
    document.querySelectorAll('.arc-gain').forEach(function(el) {{
      if (el.dataset.days) el.textContent = fill(s.gain_in, {{ gain: el.dataset.gain, days: el.dataset.days }});
    }});
    document.querySelectorAll('.arc-date').forEach(function(el) {{
      if (!el.dataset.date) return;
      el.textContent = new Date(el.dataset.date + 'T00:00:00Z').toLocaleDateString(
        l === 'es' ? 'es' : 'en-US', {{ month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }});
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

    _editions_page_cache = (page, time.monotonic() + _EDITIONS_TTL)
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

    # The radar window is anchored to CURRENT_DATE but the deeds in it end
    # whenever the city last published, so the page has to say where they stop.
    through_en = _deeds_through_line(db)
    through_es = _deeds_through_line(db, "es")
    through_en_js = through_en.replace("'", "\\'")
    through_es_js = through_es.replace("'", "\\'").encode("ascii", "backslashreplace").decode()

    _MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _short_date(iso: str | None) -> str:
        if not iso:
            return ""
        try:
            y, m, d = iso.split("-")
            return f"{_MONTHS[int(m)]} {int(d)}, {y}"
        except (ValueError, IndexError):
            return _html.escape(iso)

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

    # Same as /flips: the brand is not the query.
    title = "NYC LLC buying clusters: one buyer, one ZIP, one window | PulseCities"
    desc = (
        f"{n} NYC buying runs where one LLC took the deed on {MIN_BUILDINGS} or more "
        f"buildings in the same ZIP within {RADAR_WINDOW_DAYS} days, sourced from ACRIS "
        f"deeds. Updated nightly."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [{
            "@type": "ItemList",
            "name": "NYC Speculation Radar",
            "description": desc,
            "url": "https://pulsecities.com/radar",
            "numberOfItems": n,
            "itemListElement": list_items,
        }, _crumbs(("Home", "/"), ("Speculation Radar", "/radar"))],
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="https://pulsecities.com/radar">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/radar">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
body{{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}}
nav{{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.radar-list{{list-style:none;padding:0;margin:0}}
.radar-row{{border-bottom:1px solid rgba(147,161,173,0.07);padding:18px 0;}}
.radar-head{{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;}}
.radar-main{{display:flex;flex-direction:column;gap:3px;min-width:0;}}
.radar-buyer{{font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#e4e8ec;letter-spacing:0.03em;font-weight:500;}}
.radar-geo{{font-size:0.76rem;color:var(--dim);}}
.radar-when{{font-size:0.75rem;color:var(--faint);margin-top:2px;}}
.radar-when-label{{color:var(--faint);}}
.radar-side{{display:flex;flex-direction:column;align-items:flex-end;flex-shrink:0;text-align:right;}}
.radar-count{{font-family:'JetBrains Mono',monospace;font-size:1.35rem;font-weight:500;color:#ed6317;line-height:1.1;}}
.radar-count-label{{font-size:0.75rem;color:var(--faint);text-transform:uppercase;letter-spacing:0.06em;margin-top:1px;}}
.radar-amount{{font-family:'JetBrains Mono',monospace;font-size:0.78rem;color:#c9d2da;margin-top:8px;}}
.radar-props{{list-style:none;padding:0;margin:10px 0 0 0;border-left:2px solid rgba(237,99,23,0.25);}}
.radar-prop{{display:flex;align-items:baseline;justify-content:space-between;gap:12px;padding:4px 0 4px 12px;}}
.radar-prop:hover .radar-prop-addr{{color:#ed6317;}}
.radar-prop-addr{{font-family:'JetBrains Mono',monospace;font-size:0.76rem;color:#93a1ad;}}
.radar-prop-amt{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);flex-shrink:0;}}
.radar-empty{{padding:24px 0;font-size:0.82rem;color:#93a1ad;}}
</style>
</head>
<body>
{_ssr_nav("/radar", toggle_html=_LANG_TOGGLE_BTN)}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:var(--faint);">&#8592; Home</a>
  </div>
  <h1 id="sr-heading" style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">Speculation Radar</h1>
  <p id="sr-desc" style="font-size:0.82rem;color:#93a1ad;margin-bottom:8px;line-height:1.6;">
    One LLC taking the deed on {MIN_BUILDINGS} or more buildings in the same ZIP within {RADAR_WINDOW_DAYS} days. Concentrated buying like that is a position being assembled, not a one-off purchase, and it usually shows up months before anything changes on the block. Public records only.
  </p>
  <p id="sr-sub" style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-bottom:4px;">{n} buying runs detected across NYC in the past {RADAR_WINDOW_DAYS} days.</p>
  <p id="sr-through" style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-bottom:28px;">{through_en}</p>
  <ul class="radar-list">
{rows_html}  </ul>
  <p id="sr-note" style="font-size:0.75rem;color:var(--faint);margin-top:24px;line-height:1.6;">
    Buying several buildings is not wrongdoing. This page reports the public-record pattern, not a conclusion about any buyer. <a href="/methodology" style="color:var(--accent);">How this is measured &rarr;</a>
  </p>
</div>
{_FOOTER_HTML}
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  var i18n = {{
    en: {{
      heading: 'Speculation Radar',
      desc: 'One LLC taking the deed on {MIN_BUILDINGS} or more buildings in the same ZIP within {RADAR_WINDOW_DAYS} days. Concentrated buying like that is a position being assembled, not a one-off purchase, and it usually shows up months before anything changes on the block. Public records only.',
      sub: '{n} buying runs detected across NYC in the past {RADAR_WINDOW_DAYS} days.',
      through: '{through_en_js}',
      note: 'Buying several buildings is not wrongdoing. This page reports the public-record pattern, not a conclusion about any buyer.',
      deeds: 'Deeds',
      buildings: 'buildings',
      toggle: 'EN / ES'
    }},
    es: {{
      heading: 'Radar de especulaci\\u00f3n',
      desc: 'Una LLC que toma la escritura de {MIN_BUILDINGS} o m\\u00e1s edificios en el mismo c\\u00f3digo postal en un plazo de {RADAR_WINDOW_DAYS} d\\u00edas. Una compra tan concentrada es una posici\\u00f3n en formaci\\u00f3n, no una compra aislada, y suele aparecer meses antes de que algo cambie en la cuadra. Solo registros p\\u00fablicos.',
      sub: '{n} rachas de compra detectadas en NYC en los \\u00faltimos {RADAR_WINDOW_DAYS} d\\u00edas.',
      through: '{through_es_js}',
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
    set('sr-through', s.through);
    var d = document.getElementById('sr-desc'); if (d) d.textContent = s.desc;
    var note = document.getElementById('sr-note');
    if (note) note.innerHTML = s.note + ' <a href="/methodology" style="color:var(--accent);">' + (l === 'es' ? 'C\\u00f3mo se mide \\u2192' : 'How this is measured \\u2192') + '</a>';
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

# --- Weekly review: shared history queries + the completed-week archive -------

_WEEK_SLUG_RE = re.compile(r"^(\d{4})-W(\d{2})$")
_week_page_cache: dict[str, tuple[str, float]] = {}   # slug -> (html, expires)
_week_index_cache: tuple[str, float] | None = None


# How far back a snapshot lookup may walk. Scoring snapshots daily, so the
# nearest earlier row is normally the same day; 30 days covers any realistic
# run of missed nights. Without a floor each lookup scanned the entire history
# to find one row per ZIP, which the archive page then paid 84 times over.
_SNAPSHOT_LOOKBACK_DAYS = 30


def _movers_between(db, as_of: date, prior: date, limit: int = 8):
    """Top risers comparing the latest score on/before `as_of` to the latest on
    or before `prior`. DISTINCT ON walks back to the nearest earlier snapshot, so
    a missing exact date still resolves. Powers both /this-week and the archive."""
    return db.execute(text("""
        WITH now_s AS (
            SELECT DISTINCT ON (zip_code) zip_code, composite_score AS s
            FROM score_history
            WHERE scored_at <= :as_of AND scored_at > :as_of_floor
            ORDER BY zip_code, scored_at DESC
        ),
        then_s AS (
            SELECT DISTINCT ON (zip_code) zip_code, composite_score AS s
            FROM score_history
            WHERE scored_at <= :prior AND scored_at > :prior_floor
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
        LIMIT :limit
    """), {
        "as_of": as_of,
        "as_of_floor": as_of - timedelta(days=_SNAPSHOT_LOOKBACK_DAYS),
        "prior": prior,
        "prior_floor": prior - timedelta(days=_SNAPSHOT_LOOKBACK_DAYS),
        "limit": limit,
    }).fetchall()


def _counts_between(db, start: date, end_exclusive: date):
    """Public-record filings dated within [start, end_exclusive). Event-dated, so
    a past week reconstructs exactly from the retained raw tables."""
    return db.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM evictions_raw  WHERE executed_date   >= :s AND executed_date   < :e
                AND eviction_type = 'Residential') AS evictions,
            (SELECT COUNT(*) FROM permits_raw    WHERE filing_date     >= :s AND filing_date     < :e) AS permits,
            (SELECT COUNT(*) FROM complaints_raw WHERE created_date    >= :s AND created_date    < :e
                AND complaint_type = ANY(:ctypes)) AS complaints,
            (SELECT COUNT(*) FROM violations_raw WHERE inspection_date >= :s AND inspection_date < :e) AS violations
    """), {"s": start, "e": end_exclusive,
           "ctypes": list(DISPLACEMENT_COMPLAINT_TYPES)}).fetchone()


def _completed_weeks(db) -> list[tuple[date, date]]:
    """(monday, sunday) for every fully-elapsed ISO week we can score week-over-
    week, newest first. Starts one week after history begins so a prior-week
    baseline exists; ends at the last week whose Sunday is already past."""
    row = db.execute(text("SELECT MIN(scored_at), MAX(scored_at) FROM score_history")).fetchone()
    if not row or not row[0]:
        return []
    hist_min = row[0]
    today = date.today()

    anchor = hist_min + timedelta(days=7)
    y, w, _ = anchor.isocalendar()
    monday = date.fromisocalendar(y, w, 1)

    weeks: list[tuple[date, date]] = []
    while True:
        sunday = monday + timedelta(days=6)
        if sunday >= today:
            break
        weeks.append((monday, sunday))
        monday += timedelta(days=7)
    weeks.reverse()
    return weeks


def _week_slug(monday: date) -> str:
    y, w, _ = monday.isocalendar()
    return f"{y}-W{w:02d}"


def _week_range_label(monday: date, sunday: date) -> str:
    # House style uses "to" for ranges, matching /this-week (no dash connectors).
    if monday.year == sunday.year and monday.month == sunday.month:
        return f"{monday.strftime('%b %-d')} to {sunday.strftime('%-d, %Y')}"
    if monday.year == sunday.year:
        return f"{monday.strftime('%b %-d')} to {sunday.strftime('%b %-d, %Y')}"
    return f"{monday.strftime('%b %-d, %Y')} to {sunday.strftime('%b %-d, %Y')}"


def _movers_rows_html(movers, e) -> str:
    out = ""
    for m in movers:
        color = "#e4483b" if m.delta >= 5 else "#ed6317"
        out += (
            f'<li class="tw-row" onclick="location.href=\'/neighborhood/{e(m.zip_code)}\'">'
            f'<a href="/neighborhood/{e(m.zip_code)}">'
            f'<div class="tw-main"><div class="tw-name">{e(m.zip_code)} '
            f'<span class="tw-sub">{e(m.name or "")}{", " + e(m.borough) if m.borough else ""}</span></div></div>'
            f'<div class="tw-side"><span class="tw-delta" style="color:{color};">{float(m.delta):+.1f}</span>'
            f'<span class="tw-score">to {m.score}</span></div>'
            f'</a></li>\n'
        )
    return out or '<li class="tw-empty">No neighborhood moved a half point or more this week.</li>'


def _stat_cells_html(counts) -> str:
    return "".join(
        f'<div class="tw-stat"><div class="tw-stat-n">{v:,}</div><div class="tw-stat-l">{label}</div></div>'
        for v, label in [
            (counts.evictions,  "executed evictions"),
            (counts.permits,    "construction permits"),
            (counts.violations, "HPD violations"),
            (counts.complaints, "311 housing complaints"),
        ]
    )


_WEEK_CSS = """*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
h1,h2,h3{text-wrap:balance}
body{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}
nav{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}
.nav-inner{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}
.nav-inner>div::-webkit-scrollbar{display:none}
.container{max-width:860px;margin:0 auto;padding:32px 20px 80px}
a{color:inherit;text-decoration:none}
h2{font-size:0.78rem;font-weight:600;color:var(--dim);text-transform:uppercase;letter-spacing:0.1em;margin:32px 0 4px}
.tw-list{list-style:none;padding:0;margin:0}
.tw-row{border-bottom:1px solid rgba(147,161,173,0.07);cursor:pointer}
.tw-row:hover{background:rgba(147,161,173,0.04)}
.tw-row a{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:13px 0}
.tw-name{font-family:'JetBrains Mono',monospace;font-size:0.88rem;color:#e4e8ec;font-weight:500}
.tw-row:hover .tw-name{color:#ed6317}
.tw-sub{font-family:'DM Sans',sans-serif;font-size:0.76rem;color:var(--dim);font-weight:400;margin-left:6px}
.tw-side{display:flex;flex-direction:column;align-items:flex-end;flex-shrink:0}
.tw-delta{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:500;line-height:1.1}
.tw-score{font-size:0.75rem;color:var(--faint)}
.tw-empty{padding:18px 0;font-size:0.8rem;color:#93a1ad}
.tw-stats{display:flex;flex-wrap:wrap;gap:10px 36px;margin-top:12px;padding:12px 2px;border-top:1px solid rgba(147,161,173,0.22);border-bottom:1px solid rgba(147,161,173,0.1)}
.tw-stat{display:flex;align-items:baseline;gap:8px}
.tw-stat-n{font-family:'JetBrains Mono',monospace;font-size:1.25rem;font-weight:600;color:#eef2f5;line-height:1}
.tw-stat-l{font-size:0.75rem;color:#93a1ad;text-transform:uppercase;letter-spacing:0.06em}
.wk-nav{display:flex;justify-content:space-between;gap:12px;margin-top:36px;font-family:'JetBrains Mono',monospace;font-size:0.75rem}
.wk-nav a{color:var(--accent)}
.wk-idx{list-style:none;padding:0;margin:0}
.wk-idx-row{border-bottom:1px solid rgba(147,161,173,0.07)}
.wk-idx-row a{display:flex;align-items:baseline;justify-content:space-between;gap:16px;padding:14px 0}
.wk-idx-row:hover{background:rgba(147,161,173,0.04)}
.wk-idx-range{font-family:'JetBrains Mono',monospace;font-size:0.85rem;color:#e4e8ec}
.wk-idx-row:hover .wk-idx-range{color:#ed6317}
.wk-idx-top{font-size:0.76rem;color:var(--dim);text-align:right}
footer{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}
.footer-links{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}
@media(max-width:767px){.container{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}"""


def _week_nav_html() -> str:
    # Weekly editions are children of /this-week; mark it active.
    return _ssr_nav("/this-week")




@router.get("/week/{slug}", include_in_schema=False)
def week_edition_page(slug: str, db: Session = Depends(get_db)):
    """A single completed week, reconstructed from history and event-dated
    records. Stable URL so each edition accumulates as indexable content."""
    m = _WEEK_SLUG_RE.match(slug)
    if not m:
        return _not_found()
    iso_year, iso_week = int(m.group(1)), int(m.group(2))
    try:
        monday = date.fromisocalendar(iso_year, iso_week, 1)
    except ValueError:
        return _not_found()
    sunday = monday + timedelta(days=6)

    weeks = _completed_weeks(db)
    if (monday, sunday) not in weeks:
        # The in-progress week lives at /this-week; hand-edited URLs for it
        # should land there instead of a 404.
        today = date.today()
        if monday <= today <= sunday:
            return RedirectResponse("/this-week", status_code=302)
        return _not_found()

    cached = _week_page_cache.get(slug)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    e = _html.escape
    movers = _movers_between(db, sunday, sunday - timedelta(days=7))
    counts = _counts_between(db, monday, sunday + timedelta(days=1))
    range_label = _week_range_label(monday, sunday)
    canonical = f"https://pulsecities.com/week/{slug}"

    # prev/next completed weeks for on-page navigation
    idx = weeks.index((monday, sunday))
    newer = weeks[idx - 1] if idx > 0 else None          # weeks is newest-first
    older = weeks[idx + 1] if idx + 1 < len(weeks) else None
    nav_bits = []
    if older:
        nav_bits.append(f'<a href="/week/{_week_slug(older[0])}">&#8592; {_week_range_label(*older)}</a>')
    else:
        nav_bits.append("<span></span>")
    if newer:
        nav_bits.append(f'<a href="/week/{_week_slug(newer[0])}">{_week_range_label(*newer)} &#8594;</a>')
    else:
        nav_bits.append('<a href="/this-week">This week &#8594;</a>')
    wk_nav = f'<div class="wk-nav">{nav_bits[0]}{nav_bits[1]}</div>'

    top_line = (
        f"{movers[0].name or movers[0].zip_code} rose {float(movers[0].delta):+.1f} points"
        if movers else "no neighborhood moved a half point or more"
    )
    title = f"NYC displacement, week of {range_label} | PulseCities"
    desc = (
        f"NYC displacement week in review, {range_label}: {top_line}, "
        f"{counts.evictions:,} executed evictions, {counts.permits:,} construction permits. Public records only."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "ItemList",
                "name": f"NYC displacement score movers, {range_label}",
                "description": desc,
                "url": canonical,
                "numberOfItems": len(movers),
                "itemListElement": [
                    {"@type": "ListItem", "position": i,
                     "name": f"{mv.name or mv.zip_code} ({mv.zip_code}) rose {float(mv.delta):+.1f}",
                     "url": f"https://pulsecities.com/neighborhood/{mv.zip_code}"}
                    for i, mv in enumerate(movers, 1)
                ],
            },
            {
                "@type": "BreadcrumbList",
                "itemListElement": [
                    {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://pulsecities.com/"},
                    {"@type": "ListItem", "position": 2, "name": "Weekly review", "item": "https://pulsecities.com/this-week/archive"},
                    {"@type": "ListItem", "position": 3, "name": range_label, "item": canonical},
                ],
            },
        ],
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{e(title)}</title>
<meta name="description" content="{e(desc)}">
<link rel="canonical" href="{canonical}">
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="{e(desc)}">
<meta property="og:url" content="{canonical}">
<meta property="og:type" content="article">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{e(title)}">
<meta name="twitter:description" content="{e(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<link rel="icon" href="/favicon.ico" sizes="32x32">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>{_WEEK_CSS}</style>
</head>
<body>
{_week_nav_html()}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/this-week/archive" style="font-size:0.75rem;color:var(--faint);">&#8592; Weekly review archive</a>
  </div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;font-weight:600;margin-bottom:6px;">NYC displacement, week of {e(range_label)}</h1>
  <p style="font-size:0.82rem;color:#93a1ad;margin-bottom:8px;line-height:1.6;">
    Where displacement pressure moved that week, from the public records behind the map. Reconstructed from the score history and agency filings dated in this window.
  </p>

  <h2>Score movers</h2>
  <p style="font-size:0.75rem;color:var(--dim);margin-bottom:4px;">Largest displacement-pressure increases over the week.</p>
  <ul class="tw-list">
{_movers_rows_html(movers, e)}  </ul>

  <h2>New on the record</h2>
  <p style="font-size:0.75rem;color:var(--dim);">Citywide filings dated within this week.</p>
  <div class="tw-stats">{_stat_cells_html(counts)}</div>

  {wk_nav}

  <p style="font-size:0.75rem;color:var(--faint);margin-top:28px;line-height:1.6;">
    Counts reflect records published by NYC agencies, which can lag the events they describe. Scores are risk indicators, not claims of wrongdoing. <a href="/methodology" style="color:var(--accent);">How scores work &rarr;</a>
  </p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    _week_page_cache[slug] = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


@router.get("/this-week/archive", include_in_schema=False)
def week_archive_index(db: Session = Depends(get_db)):
    """Index of every completed weekly edition, newest first."""
    global _week_index_cache
    if _week_index_cache and time.monotonic() < _week_index_cache[1]:
        return HTMLResponse(_week_index_cache[0])

    e = _html.escape
    weeks = _completed_weeks(db)[:52]  # cap the visible index at a year

    rows_html = ""
    for monday, sunday in weeks:
        slug = _week_slug(monday)
        movers = _movers_between(db, sunday, sunday - timedelta(days=7), limit=1)
        top = (
            f"{e(movers[0].name or movers[0].zip_code)} {float(movers[0].delta):+.1f}"
            if movers else "quiet week"
        )
        rows_html += (
            f'<li class="wk-idx-row"><a href="/week/{slug}">'
            f'<span class="wk-idx-range">{e(_week_range_label(monday, sunday))}</span>'
            f'<span class="wk-idx-top">{top}</span>'
            f'</a></li>\n'
        )
    if not rows_html:
        rows_html = '<li class="tw-empty">The first weekly edition publishes once a full week of history is on record.</li>'

    canonical = "https://pulsecities.com/this-week/archive"
    title = "Weekly review archive | PulseCities"
    desc = (
        "Every week of NYC displacement pressure since PulseCities began tracking: "
        "which neighborhoods rose, and what the public record showed. One page per week."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@type": "CollectionPage",
        "name": "PulseCities weekly review archive",
        "description": desc,
        "url": canonical,
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{e(title)}</title>
<meta name="description" content="{e(desc)}">
<link rel="canonical" href="{canonical}">
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="{e(desc)}">
<meta property="og:url" content="{canonical}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{e(title)}">
<meta name="twitter:description" content="{e(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<link rel="icon" href="/favicon.ico" sizes="32x32">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>{_WEEK_CSS}</style>
</head>
<body>
{_week_nav_html()}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/this-week" style="font-size:0.75rem;color:var(--faint);">&#8592; This week</a>
  </div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;font-weight:600;margin-bottom:6px;">Weekly review archive</h1>
  <p style="font-size:0.82rem;color:#93a1ad;margin-bottom:20px;line-height:1.6;">
    Every week since tracking began. Each edition captures where displacement pressure moved and what the public record showed, reconstructed from the score history.
  </p>
  <ul class="wk-idx">
{rows_html}  </ul>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    _week_index_cache = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


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
             WHERE executed_date >= CURRENT_DATE - INTERVAL '7 days'
               AND eviction_type = 'Residential')  AS evictions,
            (SELECT COUNT(*) FROM permits_raw
             WHERE filing_date >= CURRENT_DATE - INTERVAL '7 days')    AS permits,
            (SELECT COUNT(*) FROM complaints_raw
             WHERE created_date >= CURRENT_DATE - INTERVAL '7 days'
               AND complaint_type = ANY(:ctypes))   AS complaints,
            (SELECT COUNT(*) FROM violations_raw
             WHERE inspection_date >= CURRENT_DATE - INTERVAL '7 days') AS violations
    """), {"ctypes": list(DISPLACEMENT_COMPLAINT_TYPES)}).fetchone()

    from api.routes.flips import query_flips
    flips = sorted(
        query_flips(db),
        key=lambda f: f.get("transfer_date") or "",
        reverse=True,
    )[:3]

    e = _html.escape

    movers_html = ""
    for m in movers:
        color = "#e4483b" if m.delta >= 5 else "#ed6317"
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
            f'<div class="tw-side"><span class="tw-delta" style="color:#ed6317;">+{f["days_between"]}d</span>'
            f'<span class="tw-score">buy &rarr; permit</span></div>'
            f'</a></li>\n'
        )
    if not flips_html:
        flips_html = '<li class="tw-empty">No new flips matched the pattern this week.</li>'

    stat_cells = "".join(
        f'<div class="tw-stat"><div class="tw-stat-n">{v:,}</div><div class="tw-stat-l" id="tw-stat-{key}">{label}</div></div>'
        for v, label, key in [
            (counts.evictions,  "executed evictions",    "evictions"),
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
        f"{counts.evictions:,} executed evictions, {counts.permits:,} construction permits. Public records only."
    )

    # /this-week emitted no structured data (regression vs /week/{slug}). It is a
    # dated editorial review, so it earns a NewsArticle plus the movers ItemList
    # and a breadcrumb.
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "NewsArticle",
                "headline": title,
                "description": desc,
                "datePublished": date.today().isoformat(),
                "author": {"@type": "Person", "name": "Michael Espin"},
                "publisher": {"@type": "Organization", "name": "PulseCities", "url": "https://pulsecities.com"},
                "url": "https://pulsecities.com/this-week",
            },
            {
                "@type": "ItemList",
                "name": f"NYC displacement score movers, {range_label}",
                "numberOfItems": len(movers),
                "itemListElement": [
                    {"@type": "ListItem", "position": i,
                     "name": (mv.name or mv.zip_code),
                     "url": f"https://pulsecities.com/neighborhood/{mv.zip_code}"}
                    for i, mv in enumerate(movers, 1)
                ],
            },
            {
                "@type": "BreadcrumbList",
                "itemListElement": [
                    {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://pulsecities.com/"},
                    {"@type": "ListItem", "position": 2, "name": "This week", "item": "https://pulsecities.com/this-week"},
                ],
            },
        ],
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{e(title)}</title>
<meta name="description" content="{e(desc)}">
<link rel="canonical" href="https://pulsecities.com/this-week">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="{e(desc)}">
<meta property="og:url" content="https://pulsecities.com/this-week">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og/this-week/card.png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{e(title)}">
<meta name="twitter:description" content="{e(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og/this-week/card.png">
<link rel="icon" href="/favicon.ico" sizes="32x32">
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
body{{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}}
nav{{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
h2{{font-size:0.78rem;font-weight:600;color:var(--dim);text-transform:uppercase;letter-spacing:0.1em;margin:32px 0 4px}}
.tw-list{{list-style:none;padding:0;margin:0}}
.tw-row{{border-bottom:1px solid rgba(147,161,173,0.07);cursor:pointer}}
.tw-row:hover{{background:rgba(147,161,173,0.04)}}
.tw-row a{{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:13px 0}}
.tw-name{{font-family:'JetBrains Mono',monospace;font-size:0.88rem;color:#e4e8ec;font-weight:500}}
.tw-row:hover .tw-name{{color:#ed6317}}
.tw-sub{{font-family:'DM Sans',sans-serif;font-size:0.76rem;color:var(--dim);font-weight:400;margin-left:6px}}
.tw-side{{display:flex;flex-direction:column;align-items:flex-end;flex-shrink:0}}
.tw-delta{{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:500;line-height:1.1}}
.tw-score{{font-size:0.75rem;color:var(--faint)}}
.tw-empty{{padding:18px 0;font-size:0.8rem;color:#93a1ad}}
.tw-stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:12px}}
.tw-stat{{background:rgba(27,37,52,0.5);border:1px solid rgba(147,161,173,0.1);border-radius:10px;padding:16px}}
.tw-stat-n{{font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:600;color:#eef2f5}}
.tw-stat-l{{font-size:0.75rem;color:#93a1ad;margin-top:4px}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
</style>
</head>
<body>
{_ssr_nav("/this-week")}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:var(--faint);">&#8592; Home</a>
  </div>
  <h1 id="tw-heading" style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;font-weight:600;margin-bottom:6px;">This week in NYC displacement</h1>
  <p style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-bottom:4px;">{e(range_label)}</p>
  <p id="tw-intro" style="font-size:0.82rem;color:#93a1ad;margin-bottom:8px;line-height:1.6;">
    <span id="tw-intro-text">The week's movement across all NYC neighborhoods, from the same public records that drive the map. This page always shows the current week.</span> <a id="tw-archive-link" href="/this-week/archive" style="color:var(--accent);">Past weeks &rarr;</a>
  </p>

  <h2 id="tw-movers-h">Score movers</h2>
  <p id="tw-movers-sub" style="font-size:0.75rem;color:var(--dim);margin-bottom:4px;">Largest displacement-pressure increases over the past 7 days.</p>
  <ul class="tw-list">
{movers_html}  </ul>

  <h2 id="tw-records-h">New on the record</h2>
  <p id="tw-records-sub" style="font-size:0.75rem;color:var(--dim);">Citywide filings dated within the past 7 days.</p>
  <div class="tw-stats">{stat_cells}</div>

  <h2 id="tw-flips-h">Newest flips</h2>
  <p style="font-size:0.75rem;color:var(--dim);margin-bottom:4px;">LLC bought, then filed to renovate. <a href="/flips" style="color:var(--accent);">Full feed &rarr;</a></p>
  <ul class="tw-list">
{flips_html}  </ul>

  <p style="font-size:0.75rem;color:var(--faint);margin-top:28px;line-height:1.6;">
    <span id="tw-note">Counts reflect records published by NYC agencies, which can lag the events they describe. Scores are risk indicators, not claims of wrongdoing.</span> <a id="tw-meth-link" href="/methodology" style="color:var(--accent);">How scores work &rarr;</a>
  </p>
</div>
{_FOOTER_HTML}
<script>
(function() {{
  var lang = 'en';
  try {{ lang = localStorage.getItem('pc-lang') || 'en'; }} catch (e) {{ return; }}
  if (lang !== 'es') return;
  var es = {{
    'tw-heading':     'Esta semana en el desplazamiento de NYC',
    'tw-intro-text':  'El movimiento de la semana en todos los vecindarios de NYC, con los mismos registros p\u00fablicos que alimentan el mapa. Esta p\u00e1gina siempre muestra la semana actual.',
    'tw-archive-link': 'Semanas anteriores \u2192',
    'tw-movers-h':    'Cambios de puntuaci\u00f3n',
    'tw-movers-sub':  'Mayores aumentos de presi\u00f3n de desplazamiento en los \u00faltimos 7 d\u00edas.',
    'tw-records-h':   'Nuevo en el registro',
    'tw-records-sub': 'Registros de toda la ciudad con fecha en los \u00faltimos 7 d\u00edas.',
    'tw-flips-h':     'Flips m\u00e1s recientes',
    'tw-note':        'Los conteos reflejan registros publicados por agencias de NYC, que pueden retrasarse respecto a los hechos. Las puntuaciones son indicadores de riesgo, no acusaciones.',
    'tw-meth-link':   'C\u00f3mo funcionan las puntuaciones \u2192',
    # "presentados" means filed, and these are warrants a marshal executed.
    # English fixed this label on 2026-08-19 and the Spanish string kept the
    # wrong one, so the two languages have been describing different events
    # from the same number since. Matches the English "executed evictions".
    'tw-stat-evictions':  'desalojos ejecutados',
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


# ---------------------------------------------------------------------------
# /displacement — the citywide findings showcase. One page that pulls the
# strongest signals into a single narrative destination: hottest neighborhoods,
# eviction-to-resale flips (approved editions only, so the human review gate
# still holds), the largest landlords, and speculative buying clusters. Every
# section links out to its deep page. Rebuilt nightly, cached like the others.
# ---------------------------------------------------------------------------
_displacement_cache: tuple[str, float] | None = None


def _approved_flip_arcs() -> list[dict]:
    """Eviction-to-resale arcs cleared for publication (approved editions only).

    The raw weekly scan stays behind a human gate; naming a building as an
    eviction-flip is a review decision, not an automatic one. Only arcs a person
    has approved (the same set /flips/editions publishes) surface here.
    """
    path = _FRONTEND.parent / "scripts" / "eviction_flips_editions.json"
    try:
        editions = json.loads(path.read_text()).get("editions", [])
    except (json.JSONDecodeError, OSError):
        return []
    arcs: list[dict] = []
    for ed in editions:
        if ed.get("approved"):
            arcs.extend(ed.get("arcs", []))
    # De-dupe by arc key in case a building appears across editions; keep the first.
    seen: set = set()
    unique = []
    for a in arcs:
        k = a.get("key") or a.get("bbl")
        if k in seen:
            continue
        seen.add(k)
        unique.append(a)
    return unique


@router.get("/displacement", include_in_schema=False)
def displacement_page(db: Session = Depends(get_db)):
    global _displacement_cache
    if _displacement_cache and time.monotonic() < _displacement_cache[1]:
        return HTMLResponse(_displacement_cache[0])

    from api.routes.flips import query_flips
    from api.routes.radar import query_radar
    from api.routes.operators import OPERATOR_NOISE_ROOTS

    _MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _my(iso: str | None) -> str:
        if not iso:
            return ""
        try:
            y, m, _ = iso.split("-")
            return f"{_MONTHS[int(m)]} {y}"
        except (ValueError, IndexError):
            return ""

    def _m(v) -> str:
        v = float(v or 0)
        if v >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        if v >= 1_000:
            return f"${v / 1_000:.0f}K"
        return f"${v:.0f}"

    # ---- data ----
    agg = db.execute(text(
        "SELECT AVG(score) AS avg, MAX(score) AS max, COUNT(*) AS n "
        "FROM displacement_scores WHERE score IS NOT NULL"
    )).first()
    avg_score = float(agg.avg) if agg and agg.avg is not None else 0.0
    max_score = float(agg.max) if agg and agg.max is not None else 0.0
    n_hoods = int(agg.n) if agg and agg.n else 0

    arcs = sorted(_approved_flip_arcs(), key=lambda a: a.get("gain_pct") or 0, reverse=True)
    flips = query_flips(db)
    clusters = query_radar(db)

    hot = db.execute(text("""
        SELECT n.zip_code, n.name, ds.score
        FROM neighborhoods n
        JOIN displacement_scores ds ON ds.zip_code = n.zip_code
        WHERE ds.score IS NOT NULL
        ORDER BY ds.score DESC
        LIMIT 8
    """)).fetchall()

    op_rows = db.execute(text(
        "SELECT operator_root, slug, display_name, "
        "COALESCE(total_acquisitions, 0) AS acqs, "
        "COALESCE(jsonb_array_length(llc_entities), 0) AS entities "
        "FROM operators WHERE operator_class = 'operator' "
        "AND COALESCE(jsonb_array_length(llc_entities), 0) > 0 "
        "ORDER BY COALESCE(total_acquisitions, 0) DESC, operator_root LIMIT 12"
    )).fetchall()
    ops = [o for o in op_rows if o.operator_root not in OPERATOR_NOISE_ROOTS][:5]

    esc = _html.escape

    # ---- sections ----
    def _stat(num: str, label: str) -> str:
        return f'<div class="stat"><div class="stat-num">{num}</div><div class="stat-label">{label}</div></div>'

    stats_html = (
        _stat(f"{avg_score:.0f}<span class=\"stat-unit\">/100</span>", f"avg pressure across {n_hoods} neighborhoods")
        + _stat(str(len(arcs)), "eviction-to-resale arcs documented")
        + _stat(str(len(flips)), "renovation flips flagged")
        + _stat(str(len(clusters)), "active buying clusters")
    )

    # Eviction -> flip arcs
    arc_items = ""
    for a in arcs[:6]:
        bbl = esc(str(a.get("bbl", "")))
        addr = esc(_addr_title(a.get("address") or f"BBL {a.get('bbl')}"))
        zc = esc(str(a.get("zip_code") or ""))
        line = (
            f"Evicted {_my(a.get('eviction_date'))}. Bought {_my(a.get('buy_date'))} "
            f"for {_m(a.get('buy_amt'))}, sold {_my(a.get('sell_date'))} for {_m(a.get('sell_amt'))}"
        )
        gain = int(a.get("gain_pct") or 0)
        arc_items += (
            f'<li class="arc" onclick="location.href=\'/property/{bbl}\'">'
            f'<a href="/property/{bbl}">'
            f'<div class="arc-main"><div class="arc-addr">{addr}</div>'
            f'<div class="arc-sub">{zc}</div>'
            f'<div class="arc-line">{line}</div></div>'
            f'<div class="arc-gain">+{gain}%</div>'
            f'</a></li>'
        )
    if not arc_items:
        arc_items = '<li class="empty">The latest arcs are under review. Check back after the next edition.</li>'

    # Hottest ZIPs
    hot_items = ""
    for i, r in enumerate(hot, 1):
        label, color = _tier_info(float(r.score))
        name = esc(r.name or r.zip_code)
        hot_items += (
            f'<li class="row" onclick="location.href=\'/neighborhood/{esc(r.zip_code)}\'">'
            f'<a href="/neighborhood/{esc(r.zip_code)}">'
            f'<span class="rank">#{i}</span>'
            f'<span class="row-name">{name}<span class="row-sub">{esc(r.zip_code)}</span></span>'
            f'<span class="row-val" style="color:{color}">{float(r.score):.0f}'
            f'<span class="row-tier">{label}</span></span>'
            f'</a></li>'
        )

    # Top operators
    op_items = ""
    for i, o in enumerate(ops, 1):
        name = esc(o.display_name or o.operator_root)
        meta = []
        if o.acqs:
            meta.append(f"{o.acqs} acquisitions")
        if o.entities:
            meta.append(f'{o.entities} LLC{"s" if o.entities != 1 else ""}')
        op_items += (
            f'<li class="row" onclick="location.href=\'/operator/{esc(o.slug)}\'">'
            f'<a href="/operator/{esc(o.slug)}">'
            f'<span class="rank">#{i}</span>'
            f'<span class="row-name">{name}<span class="row-sub">{esc(", ".join(meta))}</span></span>'
            f'<span class="row-arrow">&rarr;</span>'
            f'</a></li>'
        )

    # Speculation clusters
    cl_items = ""
    for c in clusters[:5]:
        buyer = esc(c["buyer"] or "")
        zc = esc(str(c["zip_code"]))
        hood = esc(c["neighborhood"] or zc)
        span = c.get("span_days")
        if span is None:
            span_txt = "recently"
        elif span == 0:
            span_txt = "on a single day"
        else:
            span_txt = f"over {span} day" + ("s" if span != 1 else "")
        amt = f", {_m(c['total_amount'])}" if c.get("total_amount") else ""
        cl_items += (
            f'<li class="row" onclick="location.href=\'/radar\'">'
            f'<a href="/radar">'
            f'<span class="row-name">{buyer}'
            f'<span class="row-sub">{c["building_count"]} buildings in {hood} ({zc}) {span_txt}{amt}</span>'
            f'</span><span class="row-arrow">&rarr;</span>'
            f'</a></li>'
        )
    if not cl_items:
        cl_items = '<li class="empty">No active clusters in the current window.</li>'

    title = "The state of NYC displacement | PulseCities"
    desc = (
        "A live read of NYC displacement pressure, rebuilt nightly from public records: "
        "hottest neighborhoods, largest landlords, eviction-to-resale flips, and buying clusters."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "CollectionPage",
                "name": "The state of NYC displacement",
                "description": desc,
                "url": "https://pulsecities.com/displacement",
                "isPartOf": {"@type": "WebSite", "name": "PulseCities", "url": "https://pulsecities.com"},
                "dateModified": date.today().isoformat(),
            },
            {
                "@type": "BreadcrumbList",
                "itemListElement": [
                    {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://pulsecities.com/"},
                    {"@type": "ListItem", "position": 2, "name": "Displacement", "item": "https://pulsecities.com/displacement"},
                ],
            },
        ],
    })

    head = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{esc(title)}</title>
<meta name="description" content="{esc(desc)}">
<link rel="canonical" href="https://pulsecities.com/displacement">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{esc(desc)}">
<meta property="og:url" content="https://pulsecities.com/displacement">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{esc(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%231a1a2e'/%3E%3Cpolyline points='2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16' fill='none' stroke='%23ed6317' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
"""

    css = """<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
h1,h2,h3{text-wrap:balance}
body{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
nav{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0;position:sticky;top:0;background:rgba(17,24,35,0.92);backdrop-filter:blur(8px);z-index:5}
.nav-inner{max-width:900px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}
.brand{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;letter-spacing:-0.01em;color:#eef2f5}
.nav-links{display:flex;gap:18px;font-size:0.82rem;color:#93a1ad;flex-wrap:wrap}
.nav-links a:hover{color:#ed6317}
.wrap{max-width:900px;margin:0 auto;padding:40px 20px 72px}
.eyebrow{font-family:'JetBrains Mono',monospace;font-size:0.75rem;letter-spacing:0.18em;color:#ed6317;text-transform:uppercase;margin-bottom:14px}
h1{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:clamp(2rem,5.4vw,3.1rem);line-height:1.04;letter-spacing:-0.02em;margin-bottom:14px}
.lede{font-size:1.05rem;color:#93a1ad;max-width:620px;line-height:1.5}
.stats{display:flex;flex-wrap:wrap;gap:10px 40px;margin:34px 0 8px;padding:14px 2px;border-top:1px solid rgba(147,161,173,0.22);border-bottom:1px solid rgba(147,161,173,0.1)}
.stat{display:flex;align-items:baseline;gap:8px}
.stat-num{font-family:'JetBrains Mono',monospace;font-weight:600;font-size:1.35rem;line-height:1;color:#eef2f5}
.stat-unit{font-size:0.8rem;color:var(--muted);font-weight:600}
.stat-label{font-size:0.75rem;color:#93a1ad;text-transform:uppercase;letter-spacing:0.06em;line-height:1.3}
@media(max-width:640px){.stats{gap:10px 24px}}
.section{margin-top:44px}
.sec-h{font-family:'Bricolage Grotesque',sans-serif;font-weight:600;font-size:1.3rem;letter-spacing:-0.01em;display:flex;align-items:baseline;justify-content:space-between;gap:12px}
.sec-more{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:#ed6317;white-space:nowrap}
.sec-more:hover{text-decoration:underline}
.sec-sub{font-size:0.86rem;color:var(--muted);margin-top:5px;margin-bottom:14px;max-width:640px;line-height:1.45}
ul{list-style:none}
.arc{border-bottom:1px solid rgba(147,161,173,0.08);cursor:pointer}
.arc:hover{background:rgba(147,161,173,0.04)}
.arc a{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:15px 0}
.arc-addr{font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#e4e8ec;font-weight:600;letter-spacing:0.02em}
.arc:hover .arc-addr{color:#ed6317}
.arc-sub{font-size:0.75rem;color:var(--muted);margin-top:2px}
.arc-line{font-size:0.78rem;color:#93a1ad;margin-top:5px;line-height:1.4}
/* .arc-gain and .row-val are the only number columns on the site not set in
   JetBrains Mono, so they are the only two that need tabular figures asked for. */
.arc-gain{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:1.35rem;color:#e4483b;white-space:nowrap;font-variant-numeric:tabular-nums}
.row{border-bottom:1px solid rgba(147,161,173,0.08);cursor:pointer}
.row:hover{background:rgba(147,161,173,0.04)}
.row a{display:flex;align-items:center;gap:14px;padding:13px 0}
.rank{font-family:'JetBrains Mono',monospace;font-size:0.8rem;color:var(--muted);min-width:26px}
.row-name{flex:1;min-width:0;font-size:0.92rem;color:#e4e8ec;font-weight:500}
.row:hover .row-name{color:#ed6317}
.row-sub{display:block;font-size:0.75rem;color:var(--muted);font-weight:400;margin-top:2px}
.row-val{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:1.15rem;display:flex;flex-direction:column;align-items:flex-end;line-height:1;font-variant-numeric:tabular-nums}
.row-tier{font-family:'DM Sans',sans-serif;font-size:0.75rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;margin-top:4px;opacity:0.85}
.row-arrow{color:var(--dim);font-size:1.1rem}
.row:hover .row-arrow{color:#ed6317}
.empty{padding:16px 0;color:var(--muted);font-size:0.86rem}
.cta{margin-top:52px;border:1px solid rgba(237,99,23,0.25);border-radius:14px;padding:26px 22px;background:rgba(237,99,23,0.04);text-align:center}
.cta-h{font-family:'Bricolage Grotesque',sans-serif;font-weight:600;font-size:1.2rem;margin-bottom:6px}
.cta-sub{font-size:0.88rem;color:#93a1ad;margin-bottom:16px}
.cta-btns{display:flex;gap:12px;justify-content:center;flex-wrap:wrap}
.btn{padding:10px 20px;border-radius:8px;font-size:0.88rem;font-weight:600}
.btn-primary{background:#ed6317;color:#111823}
.btn-primary:hover{background:#fb8c3a}
.btn-secondary{border:1px solid rgba(147,161,173,0.25);color:#e4e8ec}
.btn-secondary:hover{border-color:#ed6317;color:#ed6317}
.note{margin-top:26px;font-size:0.78rem;color:var(--faint);line-height:1.5;text-align:center}
.note a{color:var(--accent)}
footer{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:40px;font-size:12px;color:var(--muted)}
.footer-links{display:flex;justify-content:center;gap:20px;flex-wrap:wrap}
</style>
"""

    body = f"""</head>
<body>
{_ssr_nav("/displacement", track=True)}
<div class="wrap">
<div class="eyebrow">PulseCities &middot; Citywide &middot; NYC public records</div>
<h1>The state of NYC displacement</h1>
<p class="lede">What the public record shows right now. Every number below is rebuilt nightly from NYC open data: deeds, evictions, permits, violations, and complaints.</p>

<div class="stats">{stats_html}</div>

<div class="section">
<h2 class="sec-h">Evicted, then flipped <a class="sec-more" href="/flips/editions" onclick="plausible('Showcase Section',{{props:{{sec:'arcs'}}}})">All editions &rarr;</a></h2>
<div class="sec-sub">Buildings where tenants were evicted, an LLC bought in, and the building resold at a markup within a year. Reviewed before listing. Every step is a public deed.</div>
<ul>{arc_items}</ul>
</div>

<div class="section">
<h2 class="sec-h">Highest pressure this week <a class="sec-more" href="/neighborhoods" onclick="plausible('Showcase Section',{{props:{{sec:'hot'}}}})">All neighborhoods &rarr;</a></h2>
<div class="sec-sub">The neighborhoods with the strongest combined displacement signals across {n_hoods} scored ZIP codes.</div>
<ul>{hot_items}</ul>
</div>

<div class="section">
<h2 class="sec-h">The largest landlords <a class="sec-more" href="/operators" onclick="plausible('Showcase Section',{{props:{{sec:'operators'}}}})">All landlords &rarr;</a></h2>
<div class="sec-sub">Owner networks with the most acquisitions across the city, resolved from ACRIS deeds through their LLC shells.</div>
<ul>{op_items}</ul>
</div>

<div class="section">
<h2 class="sec-h">Buying clusters <a class="sec-more" href="/radar" onclick="plausible('Showcase Section',{{props:{{sec:'radar'}}}})">Speculation radar &rarr;</a></h2>
<div class="sec-sub">A single LLC taking the deed on several buildings in one ZIP within 90 days. Concentrated buying often precedes turnover.</div>
<ul>{cl_items}</ul>
</div>

<div class="cta">
<div class="cta-h">Watch your own block</div>
<div class="cta-sub">Get a weekly read on any NYC neighborhood or building, straight from the record.</div>
<div class="cta-btns">
<a href="/neighborhoods" class="btn btn-primary" onclick="plausible('Showcase CTA',{{props:{{act:'browse'}}}})">Find your neighborhood</a>
<a href="/map" class="btn btn-secondary" onclick="plausible('Showcase CTA',{{props:{{act:'map'}}}})">Open the map</a>
</div>
</div>

<div class="note">Counts reflect records published by NYC agencies, which can lag the events they describe. Scores are risk indicators, not claims of wrongdoing. <a href="/methodology">How this works &rarr;</a></div>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    page = head + css + body
    _displacement_cache = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


# --- Evictions tracker: citywide marshal-eviction record ----------------------

_EVICTIONS_STRINGS = {
    "en": {
        "heading": "NYC marshal evictions",
        "desc": ("A marshal eviction is the end of the court process: a warrant executed "
                 "and a household removed. Every entry below comes from the city's public "
                 "eviction record and refreshes nightly."),
        "l7": "7 days", "l30": "30 days", "l365": "12 months",
        "trend_h": "Twelve months of executions",
        "trend_sub": "Residential warrants executed by month, complete months only",
        "recent_h": "Most recent executions",
        "recent_sub": "Residential warrants executed in the past 30 days, newest first",
        "case_cta": "Holding a marshal docket number or a court index number?",
        "case_cta_link": "Look up that case",
        "where_h": "Where evictions concentrate",
        "where_sub": "Ranked by executions in the past 30 days",
        "after_h": "After the eviction",
        "faq_h": "About this record",
        "th": ("Neighborhood", "ZIP", "30 days", "12 months"),
        "borough_prefix": "Past 30 days by borough",
        "toggle": "EN / ES", "toggle_aria": "Cambiar a espanol",
        "home": "&#8592; Home", "kicker": "NYC public records",
    },
    "es": {
        "heading": "Desalojos por alguacil en NYC",
        "desc": ("Un desalojo por alguacil es el final del proceso judicial: una orden "
                 "ejecutada y un hogar desalojado. Cada entrada proviene del registro "
                 "p\u00fablico de desalojos de la ciudad y se actualiza cada noche."),
        "l7": "7 d\u00edas", "l30": "30 d\u00edas", "l365": "12 meses",
        "trend_h": "Doce meses de ejecuciones",
        "trend_sub": "Ejecuciones residenciales por mes, solo meses completos",
        "recent_h": "Ejecuciones m\u00e1s recientes",
        "recent_sub": "\u00d3rdenes residenciales ejecutadas en los \u00faltimos 30 d\u00edas, primero las m\u00e1s nuevas",
        "case_cta": "\u00bfTiene un n\u00famero de expediente del alguacil o de \u00edndice judicial?",
        "case_cta_link": "Busque ese caso",
        "where_h": "D\u00f3nde se concentran los desalojos",
        "where_sub": "Ordenado por ejecuciones en los \u00faltimos 30 d\u00edas",
        "after_h": "Despu\u00e9s del desalojo",
        "faq_h": "Sobre este registro",
        "th": ("Vecindario", "ZIP", "30 d\u00edas", "12 meses"),
        "borough_prefix": "\u00daltimos 30 d\u00edas por condado",
        "toggle": "ES / EN", "toggle_aria": "Switch to English",
        "home": "&#8592; Inicio", "kicker": "Registros p\u00fablicos de NYC",
    },
}

_evictions_cache: dict[str, tuple[str, float]] = {}  # cleared on restart


def _addr_title(a: str) -> str:
    # str.title() capitalizes after digits ("233Rd St"); put ordinals back
    return re.sub(r"(\d)(St|Nd|Rd|Th)\b", lambda m: m.group(1) + m.group(2).lower(),
                  a.title())


@router.get("/evictions", include_in_schema=False)
def evictions_page(lang: str = "en", db: Session = Depends(get_db)):
    """Citywide tracker for executed residential marshal evictions.

    Search demand already exists for "{neighborhood} evictions" and nothing on
    the site answered it directly; this page does, and it is the press-facing
    home for the eviction record. Nightly data, Residential type only.
    """
    lang = "es" if lang == "es" else "en"
    L = _EVICTIONS_STRINGS[lang]
    cached = _evictions_cache.get(lang)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    esc = _html.escape

    # Every window ends at the latest published record, not today. The city
    # publishes on a lag of a week or more; anchoring on CURRENT_DATE counted
    # unpublished days as zero, which read as "0 evictions this week" and
    # made the year-over-year line compare a short window against a full one.
    counts = db.execute(text("""
        WITH bound AS (
            SELECT max(executed_date) AS latest
            FROM evictions_raw WHERE eviction_type = 'Residential'
        )
        SELECT b.latest,
               count(*) FILTER (WHERE e.executed_date >  b.latest - 7)   AS d7,
               count(*) FILTER (WHERE e.executed_date >  b.latest - 30)  AS d30,
               count(*) FILTER (WHERE e.executed_date >  b.latest - 365) AS d365,
               count(*) FILTER (WHERE e.executed_date >  b.latest - 395
                            AND e.executed_date <= b.latest - 365)       AS prev30
        FROM evictions_raw e CROSS JOIN bound b
        WHERE e.eviction_type = 'Residential'
        GROUP BY b.latest
    """)).first()
    d7, d30, d365 = int(counts.d7 or 0), int(counts.d30 or 0), int(counts.d365 or 0)
    latest = counts.latest.isoformat() if counts.latest else None

    # Anchored on the newest published day, exactly like the counts above.
    # CURRENT_DATE - 30 silently walked a shorter window every day the city
    # went quiet, because executions publish well behind the calendar.
    _RECENT_WINDOW = """
        FROM evictions_raw e
        CROSS JOIN (SELECT max(executed_date) AS latest FROM evictions_raw
                    WHERE eviction_type = 'Residential') b
        WHERE e.eviction_type = 'Residential'
          AND e.executed_date IS NOT NULL
          AND e.executed_date > b.latest - 30
          AND e.address NOT ILIKE '%store located%'
    """

    recent = db.execute(text(f"""
        SELECT * FROM (
            SELECT DISTINCT ON (e.address, e.executed_date)
                   e.bbl, e.address, e.zip_code, e.borough, e.executed_date,
                   (SELECT name FROM neighborhoods n WHERE n.zip_code = e.zip_code) AS name
            {_RECENT_WINDOW}
            ORDER BY e.address, e.executed_date
        ) t ORDER BY executed_date DESC LIMIT 150
    """)).fetchall()

    # The list is deduplicated per address and day and drops storefronts, so it
    # needs its own total. Measuring it against the raw 30-day count would
    # compare two different populations and overstate what is being withheld.
    recent_total = int(db.execute(text(f"""
        SELECT count(*) FROM (
            SELECT DISTINCT ON (e.address, e.executed_date) e.address
            {_RECENT_WINDOW}
            ORDER BY e.address, e.executed_date
        ) t
    """)).scalar() or 0)

    # Same anchor as the headline: the latest published record, not today.
    # These two were still on CURRENT_DATE, so the ZIP table silently lost a
    # day of coverage for every day the DOI feed slipped.
    top_zips = db.execute(text("""
        WITH bound AS (
            SELECT max(executed_date) AS latest
            FROM evictions_raw WHERE eviction_type = 'Residential'
        )
        SELECT e.zip_code, coalesce(n.name, e.zip_code) AS name,
               count(*) FILTER (WHERE e.executed_date > b.latest - 30) AS d30,
               count(*) AS d365
        FROM evictions_raw e
        CROSS JOIN bound b
        LEFT JOIN neighborhoods n ON n.zip_code = e.zip_code
        WHERE e.eviction_type = 'Residential'
          AND e.executed_date > b.latest - 365
          AND e.zip_code IS NOT NULL
        GROUP BY 1, 2 ORDER BY d30 DESC, d365 DESC LIMIT 15
    """)).fetchall()

    boroughs = db.execute(text("""
        WITH bound AS (
            SELECT max(executed_date) AS latest
            FROM evictions_raw WHERE eviction_type = 'Residential'
        )
        SELECT initcap(e.borough) AS b, count(*) AS c
        FROM evictions_raw e CROSS JOIN bound bd
        WHERE e.eviction_type = 'Residential'
          AND e.executed_date > bd.latest - 30 AND e.borough IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """)).fetchall()

    monthly = db.execute(text("""
        SELECT date_trunc('month', executed_date)::date AS m, count(*) AS c
        FROM evictions_raw
        WHERE eviction_type = 'Residential'
          AND executed_date >= (date_trunc('month', CURRENT_DATE) - interval '12 months')
          AND executed_date < date_trunc('month', CURRENT_DATE)
        GROUP BY 1 ORDER BY 1
    """)).fetchall()

    yoy = counts  # same 30-day widths, both anchored to the latest record

    n_arcs = len(_approved_flip_arcs())

    _MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _short_date(d) -> str:
        if not d:
            return ""
        return f"{_MONTHS[d.month]} {d.day}, {d.year}"

    recent_html = ""
    for r in recent:
        addr = esc(_addr_title(r.address or ""))
        hood = esc(r.name or (r.borough or "").title())
        when = _short_date(r.executed_date)
        inner = (
            f'<div class="ev-main"><div class="ev-addr">{addr}</div>'
            f'<div class="ev-geo">{hood}, {esc(r.zip_code or "")}</div></div>'
            f'<div class="ev-when">{when}</div>'
        )
        if r.bbl:
            recent_html += (f'<li class="ev-row"><a href="/property/{esc(str(r.bbl))}">'
                            f'{inner}</a></li>\n')
        else:
            recent_html += f'<li class="ev-row"><div class="ev-static">{inner}</div></li>\n'
    if not recent_html:
        recent_html = ('<li class="ev-row"><div class="ev-static">No executions published '
                       'in the current window. Check back after the next nightly refresh</div></li>\n')

    # The subtitle described the whole 30-day window while the list showed a
    # slice of it. Say which slice, or the page reads as though the city
    # executed fifteen warrants last month.
    recent_sub = L["recent_sub"]
    if len(recent) < recent_total:
        shown = f"{len(recent):,}"
        total = f"{recent_total:,}"
        # The list is deduplicated by address and day; the stat card above
        # counts warrants. Say so, or the two numbers read as a contradiction.
        warrants = f"{int(counts.d30):,}" if counts else total
        recent_sub += (f". Showing the {shown} newest of {total} distinct "
                       f"address-days ({warrants} warrants in all)"
                       if lang != "es" else
                       f". Mostrando las {shown} más recientes de {total} "
                       f"combinaciones de dirección y día ({warrants} órdenes "
                       f"en total)")

    zip_rows = ""
    for z in top_zips:
        area = _ev_area_slug(z.name)
        label = (f'<a href="/evictions/{esc(area)}">{esc(z.name)}</a>'
                 if area and z.name != z.zip_code else esc(z.name))
        zip_rows += (
            f'<tr><td>{label}</td>'
            f'<td class="num"><a href="/neighborhood/{esc(z.zip_code)}">{esc(z.zip_code)}</a></td>'
            f'<td class="num">{int(z.d30)}</td><td class="num">{int(z.d365):,}</td></tr>\n'
        )

    # Full index of the per-neighbourhood pages. The table above shows fifteen;
    # without this block the other ~112 pages are reachable only from the
    # sitemap, and a sitemap is a hint rather than a path.
    area_rows = db.execute(text("""
        SELECT n.name, count(*) AS n
        FROM evictions_raw e
        JOIN neighborhoods n ON n.zip_code = e.zip_code
        WHERE e.eviction_type = 'Residential' AND n.name IS NOT NULL
        GROUP BY 1 HAVING count(*) >= :floor
        ORDER BY 1
    """), {"floor": _EV_AREA_MIN}).fetchall()
    area_links = " &middot; ".join(
        f'<a href="/evictions/{esc(_ev_area_slug(a.name))}">{esc(a.name)}</a>'
        for a in area_rows if _ev_area_slug(a.name)
    )
    area_h = ("Evictions by neighborhood" if lang != "es"
              else "Desalojos por vecindario")
    area_sub = (f"Every NYC neighborhood with {_EV_AREA_MIN} or more executed "
                f"evictions on record, each with its own address-level page"
                if lang != "es" else
                f"Cada vecindario de NYC con {_EV_AREA_MIN} o m\u00e1s desalojos "
                f"ejecutados, cada uno con su propia p\u00e1gina")
    boro_index = " &middot; ".join(
        f'<a href="/evictions/{bslug}">{esc(bname)}</a>'
        for bslug, bname in _EV_BOROUGHS.items()
    )
    boro_h = ("Evictions by borough" if lang != "es" else "Desalojos por distrito")
    boro_sub = ("Each borough page carries its own neighborhood list, the buildings "
                "with the most executions, and the rate per 1,000 apartments"
                if lang != "es" else
                "Cada p\u00e1gina de distrito incluye su lista de vecindarios, los "
                "edificios con m\u00e1s ejecuciones y la tasa por cada 1,000 apartamentos")
    area_section = (
        f'  <h2 id="ev-boro-h">{esc(boro_h)}</h2>\n'
        f'  <p class="section-sub">{esc(boro_sub)}</p>\n'
        f'  <p class="cross" style="line-height:2;">{boro_index}</p>\n'
        f'  <h2 id="ev-area-h">{esc(area_h)}</h2>\n'
        f'  <p class="section-sub">{esc(area_sub)}</p>\n'
        f'  <p class="cross" style="line-height:2;">{area_links}</p>\n'
    ) if area_links else ""

    # Each borough name is the path into its own page. initcap() of a dirty
    # column also yields "New York" and "Kings" on a handful of rows, and those
    # stay as plain text rather than linking somewhere that does not exist.
    def _boro_link(name: str) -> str:
        for bslug, bname in _EV_BOROUGHS.items():
            if name == bname:
                return f'<a href="/evictions/{bslug}">{esc(bname)}</a>'
        return esc(name)

    borough_line = ", ".join(f"{_boro_link(b.b)} {int(b.c)}" for b in boroughs)
    latest_disp = _short_date(counts.latest) if latest else ""
    through_line = (f"Every window above ends {latest_disp}, the most recent day the city "
                    f"has published") if latest else "Refreshed nightly"

    yoy_line = ""
    if yoy and yoy.prev30:
        cur, prev = d30, int(yoy.prev30)
        delta = (cur - prev) / prev * 100.0
        if abs(delta) < 0.5:
            yoy_line = (f"The 30 days through {latest_disp} are level with the same 30 days "
                        f"a year earlier, {cur:,} against {prev:,}")
        else:
            direction = "above" if delta > 0 else "below"
            yoy_line = (f"The 30 days through {latest_disp} run {abs(delta):.0f}% {direction} "
                        f"the same 30 days a year earlier, {cur:,} executions against {prev:,}")

    def _month_bars(rows) -> str:
        if len(rows) < 6:
            return ""
        w, h = 640.0, 170.0
        pad_l, pad_r, pad_t, pad_b = 6.0, 6.0, 22.0, 24.0
        plot_w, plot_h = w - pad_l - pad_r, h - pad_t - pad_b
        peak = max(int(r.c) for r in rows) or 1
        slot = plot_w / len(rows)
        bar_w = slot * 0.58
        parts = []
        for i, r in enumerate(rows):
            c = int(r.c)
            bh = (c / peak) * plot_h
            x = pad_l + i * slot + (slot - bar_w) / 2
            y = pad_t + plot_h - bh
            fill = "#ed6317" if i == len(rows) - 1 else "rgba(237,99,23,0.45)"
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
                         f'height="{bh:.1f}" fill="{fill}"/>')
            parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{y - 5:.1f}" font-size="9.5" '
                         f'text-anchor="middle" font-family="JetBrains Mono,monospace" '
                         f'fill="#93a1ad">{c:,}</text>')
            parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{h - 8:.1f}" font-size="9.5" '
                         f'text-anchor="middle" font-family="JetBrains Mono,monospace" '
                         f'fill="#93a1ad">{_MONTHS[r.m.month]}</text>')
        base_y = pad_t + plot_h
        parts.append(f'<line x1="{pad_l}" y1="{base_y:.1f}" x2="{pad_l + plot_w:.1f}" '
                     f'y2="{base_y:.1f}" stroke="rgba(147,161,173,0.25)" stroke-width="1"/>')
        label = (f"{_MONTHS[rows[0].m.month]} {rows[0].m.year} to "
                 f"{_MONTHS[rows[-1].m.month]} {rows[-1].m.year}")
        return (f'<svg viewBox="0 0 {w:.0f} {h:.0f}" role="img" '
                f'aria-label="Residential marshal evictions by month, {label}" '
                f'style="width:100%;height:auto;display:block;">' + "".join(parts) + "</svg>")

    bars_svg = _month_bars(monthly)
    trend_section = ""
    if bars_svg:
        yoy_html = f'<p class="cross" id="ev-yoy" style="margin-top:8px;">{yoy_line}</p>' if yoy_line else ""
        trend_section = f"""
  <h2 id="ev-trend-h">{esc(L["trend_h"])}</h2>
  <p class="section-sub" id="ev-trend-sub">{esc(L["trend_sub"])}</p>
  <div style="border:1px solid rgba(147,161,173,0.12);border-radius:8px;padding:14px 12px 8px;background:rgba(255,255,255,.02);">{bars_svg}</div>
  {yoy_html}
"""

    canonical = ("https://pulsecities.com/evictions?lang=es" if lang == "es"
                 else "https://pulsecities.com/evictions")
    alt_url = ("https://pulsecities.com/evictions" if lang == "es"
               else "https://pulsecities.com/evictions?lang=es")
    nav_toggle = (
        f'<a href="{alt_url}" id="lang-toggle" aria-label="{L["toggle_aria"]}" '
        f'style="font-size:0.75rem;color:var(--faint);">{L["toggle"]}</a>'
    )

    # "nyc marshal eviction list" is the single largest query this site takes
    # impressions on, 52 in 28 days, and the page has answered it since the
    # eviction build: 150 executions with address, neighborhood and date. The
    # title said "tracker", which is the word we use rather than the word they
    # type.
    title = "NYC marshal eviction list: recent executions by address | PulseCities"
    desc = (f"{d30:,} residential marshal evictions executed across NYC in the past 30 days, "
            f"tracked nightly from public records, with the neighborhoods where they concentrate.")

    faq = [
        ("What is a marshal eviction?",
         "A court-ordered warrant of eviction carried out by a city marshal or sheriff. "
         "The record marks the day possession of the home was returned to the landlord, "
         "which is the end of the court process, not the start."),
        ("Is this list public record?",
         "Yes. Every entry comes from NYC Open Data's evictions dataset, published by the "
         "Department of Investigation from marshal and sheriff filings. PulseCities adds "
         "the neighborhood context and refreshes nightly."),
        ("Why is a recent eviction missing?",
         "Agencies publish on a lag. Counts here reflect published records, which can "
         "trail the event itself by days or weeks."),
    ]
    faq_html = "".join(
        f'<div class="faq-item"><h3>{esc(q)}</h3><p>{esc(a)}</p></div>' for q, a in faq
    )

    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "Dataset",
                "name": "NYC residential marshal evictions",
                "description": desc,
                "url": "https://pulsecities.com/evictions",
                "license": "https://opendata.cityofnewyork.us/overview/",
                "creator": {"@type": "Organization", "name": "PulseCities"},
            },
            {
                "@type": "FAQPage",
                "mainEntity": [
                    {"@type": "Question", "name": q,
                     "acceptedAnswer": {"@type": "Answer", "text": a}}
                    for q, a in faq
                ],
            },
            _crumbs(("Home", "/"), ("Evictions", "/evictions")),
        ],
    })

    page = f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{esc(title)}</title>
<meta name="description" content="{esc(desc)}">
<link rel="canonical" href="{canonical}">
<link rel="alternate" hreflang="en" href="https://pulsecities.com/evictions">
<link rel="alternate" hreflang="es" href="https://pulsecities.com/evictions?lang=es">
<link rel="alternate" hreflang="x-default" href="https://pulsecities.com/evictions">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{esc(desc)}">
<meta property="og:url" content="https://pulsecities.com/evictions">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og/site/evictions.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{esc(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og/site/evictions.png">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
body{{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}}
nav{{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.eyebrow{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;letter-spacing:0.18em;color:#ed6317;text-transform:uppercase;margin-bottom:10px}}
h1{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;font-weight:600;margin-bottom:6px}}
h2{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.05rem;font-weight:600;margin:34px 0 4px}}
.sub{{font-size:0.82rem;color:#93a1ad;line-height:1.6;max-width:640px}}
.stats{{display:flex;flex-wrap:wrap;gap:10px 40px;margin:26px 0 4px;padding:14px 2px;border-top:1px solid rgba(147,161,173,0.22);border-bottom:1px solid rgba(147,161,173,0.1)}}
.stat{{display:flex;align-items:baseline;gap:8px}}
.stat-num{{font-family:'JetBrains Mono',monospace;font-weight:600;font-size:1.35rem;line-height:1;color:#eef2f5}}
.stat-label{{font-size:0.75rem;color:#93a1ad;text-transform:uppercase;letter-spacing:0.06em;line-height:1.3}}
.mono-note{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-top:6px}}
.section-sub{{font-size:0.76rem;color:var(--dim);margin-bottom:10px}}
.ev-list{{list-style:none;padding:0;margin:0}}
.ev-row{{border-bottom:1px solid rgba(147,161,173,0.07)}}
.ev-row a,.ev-static{{display:flex;align-items:baseline;justify-content:space-between;gap:16px;padding:12px 0}}
.ev-row a:hover .ev-addr{{color:#ed6317}}
.ev-addr{{font-family:'JetBrains Mono',monospace;font-size:0.86rem;font-weight:500;color:#e4e8ec;letter-spacing:0.02em}}
.ev-geo{{font-size:0.75rem;color:var(--dim);margin-top:2px}}
.ev-when{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:#93a1ad;flex-shrink:0}}
.table-wrap{{overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:0.82rem}}
th{{text-align:left;font-family:'JetBrains Mono',monospace;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.08em;color:var(--dim);font-weight:500;padding:8px 12px 8px 0;border-bottom:1px solid rgba(147,161,173,0.22)}}
td{{padding:9px 12px 9px 0;border-bottom:1px solid rgba(147,161,173,0.07)}}
td a{{color:#6fb1d8}}
td a:hover{{text-decoration:underline}}
th.num,td.num{{text-align:right;font-family:'JetBrains Mono',monospace}}
.faq-item h3{{font-size:0.88rem;font-weight:600;margin:18px 0 4px}}
.faq-item p{{font-size:0.8rem;color:#93a1ad;line-height:1.6;max-width:640px}}
.note{{font-size:0.75rem;color:var(--faint);margin-top:26px;line-height:1.6}}
.note a,.cross a{{color:var(--accent)}}
.cross{{font-size:0.82rem;color:#93a1ad;line-height:1.6;max-width:640px}}
</style>
</head>
<body>
{_ssr_nav("/evictions", lang=lang, toggle_html=nav_toggle)}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:var(--faint);">{L["home"]}</a>
  </div>
  <div class="eyebrow">{L["kicker"]}</div>
  <h1 id="ev-heading">{esc(L["heading"])}</h1>
  <p class="sub" id="ev-desc">{esc(L["desc"])}</p>
  <div class="stats">
    <div class="stat"><div class="stat-num">{d7}</div><div class="stat-label" id="ev-l7">{esc(L["l7"])}</div></div>
    <div class="stat"><div class="stat-num">{d30:,}</div><div class="stat-label" id="ev-l30">{esc(L["l30"])}</div></div>
    <div class="stat"><div class="stat-num">{d365:,}</div><div class="stat-label" id="ev-l365">{esc(L["l365"])}</div></div>
  </div>
  <p class="mono-note">{through_line}</p>
{trend_section}
  <h2 id="ev-recent-h">{esc(L["recent_h"])}</h2>
  <p class="section-sub" id="ev-recent-sub">{esc(recent_sub)}</p>
  <ul class="ev-list">
{recent_html}  </ul>
  <p class="section-sub" style="margin-top:10px;" id="ev-case-cta">{esc(L["case_cta"])}
    <a href="/eviction-case">{esc(L["case_cta_link"])} &rarr;</a></p>

  <h2 id="ev-where-h">{esc(L["where_h"])}</h2>
  <p class="section-sub" id="ev-where-sub">{esc(L["where_sub"])}</p>
  <div class="table-wrap">
  <table>
    <thead><tr><th>{esc(L["th"][0])}</th><th class="num">{esc(L["th"][1])}</th><th class="num">{esc(L["th"][2])}</th><th class="num">{esc(L["th"][3])}</th></tr></thead>
    <tbody>
{zip_rows}    </tbody>
  </table>
  </div>
  <p class="mono-note">{esc(L["borough_prefix"])}: {borough_line}</p>

{area_section}
  <h2 id="ev-after-h">{esc(L["after_h"])}</h2>
  <p class="cross">{n_arcs} buildings on the record had a residential eviction executed, were bought by an LLC, and resold at a markup within a year. <a href="/displacement">See the documented arcs &rarr;</a></p>
  <p class="cross" style="margin-top:6px;">Checking a specific building? <a href="/who-owns-my-building">Who owns my building &rarr;</a></p>

  <h2 id="ev-faq-h">{esc(L["faq_h"])}</h2>
  {faq_html}

  <p class="note" id="ev-note">An eviction record describes a court outcome, not wrongdoing by any party. Eviction cases begin as court filings months earlier; neighborhood pages show ZIP-level filing trends. <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    _evictions_cache[lang] = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


# --- Evictions by neighbourhood ----------------------------------------------
#
# The largest block of unserved demand in the search exports: roughly 200
# impressions and zero clicks across ~35 phrasings of "eviction marshal
# {place}" (wakefield 15, mott haven 11, williamsbridge 10, bushwick 8,
# washington heights 7, ozone park 7, and on down), with nothing on the site
# targeting any of them. /evictions is citywide and /neighborhood/{zip} is
# about the displacement score.
#
# Keyed on the neighbourhood NAME rather than the ZIP, because that is how the
# queries arrive and because three separate ZIPs are called Bushwick. Three
# pages titled "Bushwick evictions" would compete with each other for the one
# query they all want.

_ev_area_cache: dict[str, tuple[str, float]] = {}
_EV_AREA_TTL = 21600

# Below this the page has too little record to be worth an index slot, and the
# plan's own warning applies: do not ship these at property-page depth or they
# become the next thin-content problem.
_EV_AREA_MIN = 20


def _ev_area_slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")


_ev_area_index: dict[str, str] | None = None   # slug -> canonical name
_ev_counts: dict[str, tuple[int, float]] = {}


def _borough_zips(borough: str | None, db) -> list[str]:
    """ZIPs in a borough, derived from the ZIP ranges rather than from
    neighborhoods.borough, which is NULL on every row."""
    if not borough:
        return []
    from api.routes.neighborhoods import _borough_from_zip
    rows = db.execute(text(
        "SELECT zip_code FROM neighborhoods WHERE zip_code IS NOT NULL"
    )).fetchall()
    return [r.zip_code for r in rows if _borough_from_zip(r.zip_code) == borough]


def _ev_area_counts(name: str, db) -> int:
    """Executed residential evictions for a neighbourhood name. Memoised: this
    is asked once per neighbourhood page render and the answer moves nightly."""
    hit = _ev_counts.get(name)
    if hit and time.monotonic() < hit[1]:
        return hit[0]
    n = db.execute(text("""
        SELECT count(*) FROM evictions_raw e
        JOIN neighborhoods n ON n.zip_code = e.zip_code
        WHERE n.name = :name AND e.eviction_type = 'Residential'
    """), {"name": name}).scalar() or 0
    _ev_counts[name] = (int(n), time.monotonic() + _EV_AREA_TTL)
    return int(n)


def _ev_areas(db) -> dict[str, str]:
    global _ev_area_index
    if _ev_area_index is None:
        rows = db.execute(text("""
            SELECT DISTINCT n.name FROM neighborhoods n
            WHERE n.name IS NOT NULL
        """)).fetchall()
        _ev_area_index = {_ev_area_slug(r.name): r.name for r in rows if _ev_area_slug(r.name)}
    return _ev_area_index


# --- Borough tier: the parent the 127 neighbourhood pages never had ----------
#
# The search exports carry 60+ place variants of "eviction marshal {place}" and
# "nyc eviction {place}" ranking anywhere from 2 to 43 with almost no clicks. The
# citywide page and the 127 neighbourhood leaves both existed; nothing in between
# was *about* a borough, so a query naming one matched a page that did not answer
# it. These five pages are that tier, and they carry the per-neighbourhood index
# for their own borough rather than repeating the citywide list.
_EV_BOROUGHS = {
    "brooklyn": "Brooklyn",
    "queens": "Queens",
    "bronx": "Bronx",
    "manhattan": "Manhattan",
    "staten-island": "Staten Island",
}


def _eviction_borough_page(slug: str, db: Session):
    """Executed marshal evictions across one borough, with every neighbourhood."""
    borough = _EV_BOROUGHS[slug]
    # One borough takes an article: "evictions in Bronx" reads wrong in every
    # sentence it appears in. The bare name still does the adjective work
    # ("Every Bronx neighborhood") and the title.
    where = "the Bronx" if borough == "Bronx" else borough
    e = _html.escape

    zips = _borough_zips(borough, db)
    if not zips:
        return _not_found()
    params = {"zips": zips, "floor": _EV_AREA_MIN}

    # Every window ends at the latest published record, never at CURRENT_DATE.
    # The city publishes on a lag; anchoring on today counts unpublished days as
    # zero and reports a fall that did not happen.
    head = db.execute(text(f"""
        WITH bound AS (
            SELECT max(executed_date) AS latest FROM evictions_raw
            WHERE eviction_type = 'Residential' AND {real_date('executed_date')}
        ),
        area AS (
            SELECT e.* FROM evictions_raw e
            WHERE e.zip_code = ANY(:zips) AND e.eviction_type = 'Residential'
              AND {real_date('e.executed_date', 'e.created_at')}
        )
        SELECT b.latest,
               count(*) AS total,
               count(DISTINCT a.bbl) AS buildings,
               min(a.executed_date) AS first,
               max(a.executed_date) AS last,
               count(*) FILTER (WHERE a.executed_date > b.latest - 365) AS y1,
               count(*) FILTER (WHERE a.executed_date > b.latest - 730
                            AND a.executed_date <= b.latest - 365) AS y2,
               count(*) FILTER (WHERE a.executed_date > b.latest - 30) AS d30
        FROM area a CROSS JOIN bound b
        GROUP BY b.latest
    """), params).first()
    if not head or not head.total:
        return _not_found()

    total, buildings = int(head.total), int(head.buildings or 0)
    y1, y2 = int(head.y1 or 0), int(head.y2 or 0)

    citywide = int(db.execute(text(
        "SELECT count(*) FROM evictions_raw WHERE eviction_type = 'Residential'"
    )).scalar() or 0)

    # Rate per 1,000 apartments, which is the only honest way to line five
    # boroughs of very different size up against each other. A raw count says
    # Brooklyn and the Bronx are comparable; per apartment they are not.
    homes = int(db.execute(text(
        "SELECT COALESCE(SUM(units_res), 0) FROM parcels WHERE zip_code = ANY(:zips)"
    ), {"zips": zips}).scalar() or 0)
    rate = (total * 1000.0 / homes) if homes else None

    # The index this tier exists for: every neighbourhood page in this borough,
    # with the counts that say which ones to open.
    areas = db.execute(text("""
        SELECT n.name,
               count(*) AS n,
               count(*) FILTER (WHERE e.executed_date > :latest - 365) AS d365,
               count(*) FILTER (WHERE e.executed_date > :latest - 30) AS d30
        FROM evictions_raw e
        JOIN neighborhoods n ON n.zip_code = e.zip_code
        WHERE e.eviction_type = 'Residential' AND n.name IS NOT NULL
          AND n.zip_code = ANY(:zips)
        GROUP BY 1 HAVING count(*) >= :floor
        ORDER BY count(*) DESC, 1
    """), {**params, "latest": head.latest}).fetchall()

    top = db.execute(text(f"""
        SELECT e.bbl, count(*) AS n, max(e.executed_date) AS last,
               max(p.address) AS address, max(e.zip_code) AS zip_code
        FROM evictions_raw e
        LEFT JOIN parcels p ON p.bbl = e.bbl
        WHERE e.zip_code = ANY(:zips) AND e.eviction_type = 'Residential'
          AND e.bbl IS NOT NULL AND {real_date('e.executed_date', 'e.created_at')}
        GROUP BY e.bbl HAVING count(*) > 2
        ORDER BY count(*) DESC, max(e.executed_date) DESC
        LIMIT 10
    """), params).fetchall()

    repeat_total = int(db.execute(text("""
        SELECT count(*) FROM (
            SELECT 1 FROM evictions_raw e
            WHERE e.zip_code = ANY(:zips) AND e.eviction_type = 'Residential'
              AND e.bbl IS NOT NULL
            GROUP BY e.bbl HAVING count(*) > 1
        ) t
    """), params).scalar() or 0)

    recent = db.execute(text(f"""
        SELECT e.bbl, e.executed_date, e.zip_code, p.address
        FROM evictions_raw e
        LEFT JOIN parcels p ON p.bbl = e.bbl
        WHERE e.zip_code = ANY(:zips) AND e.eviction_type = 'Residential'
          AND e.bbl IS NOT NULL AND {real_date('e.executed_date', 'e.created_at')}
        ORDER BY e.executed_date DESC, e.bbl
        LIMIT 8
    """), params).fetchall()

    # The join the site exists to make: who took title on the buildings doing
    # the evicting.
    owners = db.execute(text("""
        WITH repeat_bldgs AS (
            SELECT e.bbl, count(*) AS evictions
            FROM evictions_raw e
            WHERE e.zip_code = ANY(:zips) AND e.eviction_type = 'Residential'
              AND e.bbl IS NOT NULL
            GROUP BY 1 HAVING count(*) > 2
        )
        SELECT o.party_name_normalized AS buyer,
               count(DISTINCT r.bbl) AS bldgs,
               sum(r.evictions) AS evictions
        FROM repeat_bldgs r
        JOIN LATERAL (
            SELECT party_name_normalized FROM ownership_raw o2
            WHERE o2.bbl = r.bbl AND o2.doc_type = 'DEED' AND o2.party_type = '2'
            ORDER BY o2.doc_date DESC LIMIT 1
        ) o ON true
        GROUP BY 1 ORDER BY evictions DESC, bldgs DESC LIMIT 5
    """), params).fetchall()

    def _para(*parts) -> str:
        body = " ".join(x for x in parts if x)
        return f'<p class="prose">{body}</p>' if body else ""

    share = (total / citywide * 100) if citywide else 0
    lede = _para(
        f"City marshals executed {_count(total, 'residential eviction')} in "
        f"{e(where)} between {_en_date(head.first)} and {_en_date(head.last)}, "
        f"across {_count(buildings, 'building')}.",
        f"That is {share:.0f}% of every execution the city has published since the "
        f"record began in April 2024." if share else "",
        f"Against {homes:,} apartments in the borough it works out at "
        f"{rate:.1f} per 1,000, which is the number to compare boroughs on rather "
        f"than the raw count." if rate else "",
    )

    trend = ""
    if y2:
        delta = (y1 - y2) / y2 * 100
        movement = ("essentially flat year over year." if abs(delta) < 1 else
                    f"{'up' if delta > 0 else 'down'} {abs(delta):.0f}%.")
        trend = _para(
            f"In the twelve months to {_en_date(head.last)}, marshals executed "
            f"{_count(y1, 'eviction')} in {e(where)}, against {y2:,} in the twelve "
            f"months before that. That is {movement}",
            f"{_count(int(head.d30 or 0), 'eviction')} fell in the most recent thirty "
            f"days of the published record."
        )
    elif y1:
        trend = _para(
            f"{_count_open(y1, 'eviction')} fell in the twelve months to "
            f"{_en_date(head.last)}. The citywide record starts in April 2024, so "
            f"there is no full prior year to compare against yet."
        )

    area_sec = ""
    if areas:
        rows_html = "".join(
            f'<li class="rec-row"><a href="/evictions/{e(_ev_area_slug(a.name))}">'
            f'<div><div class="rec-addr">{e(a.name)}</div>'
            f'<div class="rec-geo">{int(a.d365):,} in the last 12 months, '
            f'{int(a.d30):,} in the last 30 days</div></div>'
            f'<div class="rec-side"><div class="rec-amt">{int(a.n):,}</div>'
            f'<div class="rec-date">on record</div></div></a></li>'
            for a in areas if _ev_area_slug(a.name)
        )
        area_sec = (
            f"<h2>Every {e(borough)} neighborhood on the record</h2>"
            + _para(
                f"{_count_open(len(areas), 'neighborhood')} in {e(where)} "
                f"{'carries' if len(areas) == 1 else 'carry'} {_EV_AREA_MIN} or more "
                f"executed evictions, each with its own page listing the addresses. "
                f"Sorted by the total on record."
            )
            + f'<ul class="rec-list">{rows_html}</ul>'
        )

    repeat_sec = ""
    if top:
        rows_html = "".join(
            f'<li class="rec-row"><a href="/property/{e(str(t.bbl))}">'
            f'<div><div class="rec-addr">'
            f'{e(_addr_title(t.address)) if t.address else "BBL " + e(str(t.bbl))}</div>'
            f'<div class="rec-geo">{e(t.zip_code or "")}</div></div>'
            f'<div class="rec-side"><div class="rec-amt">{_count(int(t.n), "eviction")}</div>'
            f'<div class="rec-date">latest {_en_date(t.last)}</div></div></a></li>'
            for t in top
        )
        repeat_sec = (
            f"<h2>Where the evictions concentrate</h2>"
            + _para(
                f"{_count_open(repeat_total, 'building')} in {e(where)} "
                f"{'has' if repeat_total == 1 else 'have'} more than one executed "
                f"eviction on record. The ten with the most are listed here, and a "
                f"repeat execution at one address is what separates a building under "
                f"pressure from a borough with a high count spread thinly."
            )
            + f'<ul class="rec-list">{rows_html}</ul>'
        )

    recent_sec = ""
    if recent:
        rows_html = "".join(
            f'<li class="rec-row"><a href="/property/{e(str(r.bbl))}">'
            f'<div><div class="rec-addr">'
            f'{e(_addr_title(r.address)) if r.address else "BBL " + e(str(r.bbl))}</div>'
            f'<div class="rec-geo">{e(r.zip_code or "")}</div></div>'
            f'<div class="rec-side"><div class="rec-date">{_en_date(r.executed_date)}</div>'
            f'</div></a></li>'
            for r in recent
        )
        recent_sec = f"<h2>The most recent executions in {e(where)}</h2>" + _para(
            f"The last {_count(len(recent), 'residential warrant')} a marshal executed "
            f"here, newest first. Each address links to that building's own record.",
            'Holding a marshal docket number or a court index number? '
            '<a href="/eviction-case">Look up that case &rarr;</a>'
        ) + f'<ul class="rec-list">{rows_html}</ul>'

    owner_sec = ""
    if owners:
        def _owner_link(nm: str) -> str:
            slug_o = re.sub(r"[^a-z0-9]+", "-", (nm or "").lower()).strip("-")
            label = e(_entity_title(nm))
            if _is_buyer_entity(nm) and _LLC_SLUG_RE.match(slug_o):
                return f'<a href="/llc/{e(slug_o)}">{label}</a>'
            return label
        listed = "; ".join(
            f"{_owner_link(o.buyer)} ({_count(int(o.bldgs), 'building')}, "
            f"{int(o.evictions):,} evictions)" for o in owners
        )
        owner_sec = "<h2>Who holds the deeds</h2>" + _para(
            f"On the {e(borough)} buildings with three or more executed evictions, the "
            f"most recent deed names these buyers: {listed}.",
            "A deed names the party that took title, which is often a holding company "
            "rather than whoever manages the building day to day."
        )

    limits_sec = "<h2>What this record does and does not cover</h2>" + _para(
        f"These are evictions a city marshal actually executed in {e(where)}, "
        f"published by the city from April 2024 forward. They are the end of the "
        f"process, so the count understates housing court activity: a case that "
        f"settles, or a tenant who leaves before the marshal arrives, never appears "
        f"here.",
        "Nothing on this page names a tenant. The city publishes the address, the date "
        "and the docket, and that is what is shown."
    )

    faq = [
        (f"How many evictions have there been in {where}?",
         f"City marshals executed {total:,} residential evictions in {where} between "
         f"{_en_date(head.first)} and {_en_date(head.last)}, across {buildings:,} "
         f"buildings. The most recent twelve months account for {y1:,} of them."),
        (f"Which {borough} neighborhoods have the most evictions?",
         (f"{areas[0].name} leads with {int(areas[0].n):,} on record, followed by "
          + ", ".join(f"{a.name} ({int(a.n):,})" for a in areas[1:4])
          + f". Every {borough} neighborhood with {_EV_AREA_MIN} or more is listed "
            f"above with its own page."
          ) if len(areas) > 4 else
         (f"{areas[0].name} leads with {int(areas[0].n):,} on record, and every "
          f"{borough} neighborhood on the list is shown above with its own page."
          if areas else
          f"No {borough} neighborhood carries {_EV_AREA_MIN} or more executed "
          f"evictions, so the borough's count is spread thinly rather than "
          f"concentrated.")),
        ("What is a marshal eviction?",
         "A city marshal is the officer who carries out a warrant of eviction after a "
         "housing court case ends in the landlord's favor. The city publishes each "
         "execution with the address, the date and the docket number. An eviction that "
         "is filed, settled or abandoned before that point never reaches a marshal and "
         "is not in this dataset."),
        (f"How do I find the marshal eviction record for a {borough} address?",
         f"Search the address on PulseCities. Every building page carries its own "
         f"eviction record alongside the deed history, open violations and "
         f"rent-stabilization registrations, so the eviction can be read next to who "
         f"owned the building at the time."),
    ]
    faq_html = "".join(
        f'<div class="faq-item"><h3>{e(q)}</h3><p>{e(a)}</p></div>' for q, a in faq
    )

    title = f"{borough} marshal evictions: the record by address | PulseCities"
    desc = (f"{total:,} residential evictions executed by city marshals in {where} "
            f"across {buildings:,} buildings, with the count for every neighborhood.")
    url = f"https://pulsecities.com/evictions/{slug}"
    jsonld = _jsonld({"@context": "https://schema.org", "@graph": [
        {"@type": "Dataset", "name": f"Executed marshal evictions in {borough}, NYC",
         "description": desc, "url": url,
         "temporalCoverage": f"{head.first.isoformat()}/{head.last.isoformat()}",
         "creator": {"@type": "Organization", "name": "PulseCities"},
         "isBasedOn": "https://data.cityofnewyork.us/City-Government/Evictions/6z8x-wfk4"},
        {"@type": "FAQPage", "mainEntity": [
            {"@type": "Question", "name": q,
             "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faq]},
        _crumbs(("Home", "/"), ("Evictions", "/evictions"), (borough, url)),
    ]})

    stat_cells = (
        f'<div class="stat"><div class="stat-num">{total:,}</div>'
        f'<div class="stat-label">evictions executed</div></div>'
        f'<div class="stat"><div class="stat-num">{buildings:,}</div>'
        f'<div class="stat-label">buildings</div></div>'
        f'<div class="stat"><div class="stat-num">{y1:,}</div>'
        f'<div class="stat-label">in the last 12 months</div></div>'
    )

    others = " &middot; ".join(
        f'<a href="/evictions/{s}">{e(n)}</a>'
        for s, n in _EV_BOROUGHS.items() if s != slug
    )

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
{_llc_head(title, desc, url, "index, follow", jsonld)}
</head>
<body>
{_ssr_nav("", toggle_html="")}
<div class="container" style="max-width:760px;">
  <p style="margin-bottom:8px;font-size:0.75rem;color:var(--faint);">
    <a href="/">Home</a> &middot; <a href="/evictions">Evictions</a>
  </p>
  <div class="eyebrow">NYC marshal evictions</div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.6rem;letter-spacing:0;font-weight:600;">Marshal evictions in {e(where)}</h1>
  <div class="stats">{stat_cells}</div>
  {lede}
  {trend}
  {area_sec}
  {repeat_sec}
  {recent_sec}
  {owner_sec}
  {limits_sec}
  <h2>Common questions</h2>
  {faq_html}
  <h2>The other boroughs</h2>
  <p class="cross" style="line-height:2;">{others}</p>
  <p class="prose"><a href="/evictions">The citywide eviction tracker &rarr;</a></p>
  <p class="note">Executed residential evictions published by the City of New York.
  This page describes public records, not conduct, and makes no claim of wrongdoing.
  <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    _ev_area_cache[slug] = (page, time.monotonic() + _EV_AREA_TTL)
    return HTMLResponse(page)


@router.get("/evictions/{slug}", include_in_schema=False)
def eviction_area_page(slug: str, db: Session = Depends(get_db)):
    """Executed marshal evictions for one named neighbourhood."""
    slug = (slug or "").lower()
    if not re.match(r"^[a-z0-9][a-z0-9-]{1,60}$", slug):
        return _not_found()

    hit = _ev_area_cache.get(slug)
    if hit and time.monotonic() < hit[1]:
        return HTMLResponse(hit[0])

    # The borough tier shares this path and this cache. No neighbourhood is named
    # after a borough, so the two namespaces cannot collide.
    if slug in _EV_BOROUGHS:
        return _eviction_borough_page(slug, db)

    name = _ev_areas(db).get(slug)
    if not name:
        return _not_found()

    e = _html.escape

    # Every window ends at the latest published record, never at CURRENT_DATE.
    # The city publishes on a lag, so anchoring on today counts unpublished
    # days as zero and reports a fall that did not happen.
    head = db.execute(text("""
        WITH bound AS (
            SELECT max(executed_date) AS latest FROM evictions_raw
            WHERE eviction_type = 'Residential'
        ),
        area AS (
            SELECT e.* FROM evictions_raw e
            JOIN neighborhoods n ON n.zip_code = e.zip_code
            WHERE n.name = :name AND e.eviction_type = 'Residential'
        )
        SELECT b.latest,
               count(*) AS total,
               count(DISTINCT a.bbl) AS buildings,
               min(a.executed_date) AS first,
               max(a.executed_date) AS last,
               count(*) FILTER (WHERE a.executed_date > b.latest - 365) AS y1,
               count(*) FILTER (WHERE a.executed_date > b.latest - 730
                            AND a.executed_date <= b.latest - 365) AS y2,
               count(*) FILTER (WHERE a.executed_date > b.latest - 30) AS d30
        FROM area a CROSS JOIN bound b
        GROUP BY b.latest
    """), {"name": name}).first()

    if not head or not head.total:
        return _not_found()

    zips = db.execute(text("""
        SELECT n.zip_code, n.borough, count(e.*) AS n
        FROM neighborhoods n
        LEFT JOIN evictions_raw e ON e.zip_code = n.zip_code
             AND e.eviction_type = 'Residential'
        WHERE n.name = :name
        GROUP BY 1, 2 ORDER BY n DESC, 1
    """), {"name": name}).fetchall()

    # neighborhoods.borough is NULL on all 178 rows, so reading it here meant
    # borough was always None: the lede lost its borough and the "Elsewhere in"
    # section, which is the only path between these 127 pages, never rendered.
    # The ZIP ranges are the source that actually works.
    from api.routes.neighborhoods import _borough_from_zip
    borough = next((b for z in zips if (b := _borough_from_zip(z.zip_code))), None)

    # The investigative payload: which buildings the evictions concentrate in.
    top = db.execute(text("""
        SELECT e.bbl, count(*) AS n, max(e.executed_date) AS last,
               max(p.address) AS address, max(e.zip_code) AS zip_code
        FROM evictions_raw e
        JOIN neighborhoods n ON n.zip_code = e.zip_code
        LEFT JOIN parcels p ON p.bbl = e.bbl
        WHERE n.name = :name AND e.eviction_type = 'Residential' AND e.bbl IS NOT NULL
        GROUP BY e.bbl HAVING count(*) > 1
        ORDER BY count(*) DESC, max(e.executed_date) DESC
        LIMIT 10
    """), {"name": name}).fetchall()

    # The page shows ten; the claim must count them all. len(top) was printed
    # as the population, which said "Ten buildings have more than one" in a
    # neighborhood where 139 do.
    repeat_total = int(db.execute(text("""
        SELECT count(*) FROM (
            SELECT 1 FROM evictions_raw e
            JOIN neighborhoods n ON n.zip_code = e.zip_code
            WHERE n.name = :name AND e.eviction_type = 'Residential'
              AND e.bbl IS NOT NULL
            GROUP BY e.bbl HAVING count(*) > 1
        ) t
    """), {"name": name}).scalar() or 0)

    # The site's thesis, localised: an eviction, then the building changes hands.
    after = db.execute(text("""
        SELECT count(DISTINCT e.bbl) AS n
        FROM evictions_raw e
        JOIN neighborhoods n ON n.zip_code = e.zip_code
        JOIN ownership_raw o ON o.bbl = e.bbl AND o.doc_type = 'DEED'
             AND o.doc_date > e.executed_date
             AND o.doc_date <= e.executed_date + 365
        WHERE n.name = :name AND e.eviction_type = 'Residential'
    """), {"name": name}).scalar() or 0

    # The concrete record, newest first. On a large neighbourhood this sits
    # alongside the repeat-building list; on a small one it is the page, since
    # a place with 25 executions spread over 25 addresses has no repeats to
    # show and would otherwise carry no addresses at all.
    recent = db.execute(text("""
        SELECT DISTINCT ON (coalesce(p.address, e.bbl), e.executed_date)
               e.bbl, e.executed_date, e.zip_code, p.address
        FROM evictions_raw e
        JOIN neighborhoods n ON n.zip_code = e.zip_code
        LEFT JOIN parcels p ON p.bbl = e.bbl
        WHERE n.name = :name AND e.eviction_type = 'Residential'
          AND (p.address IS NOT NULL OR e.bbl IS NOT NULL)
        ORDER BY coalesce(p.address, e.bbl), e.executed_date DESC
    """), {"name": name}).fetchall()
    recent = sorted(recent, key=lambda r: (r.executed_date, str(r.bbl or "")),
                    reverse=True)[:8]

    # Who took title on the buildings doing the evicting. This is the join the
    # site exists to make, and it is also the only thing on the page that links
    # sideways into the entity ledger.
    owners = db.execute(text("""
        WITH repeat_bldgs AS (
            SELECT e.bbl, count(*) AS evictions
            FROM evictions_raw e
            JOIN neighborhoods n ON n.zip_code = e.zip_code
            WHERE n.name = :name AND e.eviction_type = 'Residential' AND e.bbl IS NOT NULL
            GROUP BY 1 HAVING count(*) > 1
        )
        SELECT o.party_name_normalized AS buyer,
               count(DISTINCT r.bbl) AS bldgs,
               sum(r.evictions) AS evictions
        FROM repeat_bldgs r
        JOIN LATERAL (
            SELECT party_name_normalized FROM ownership_raw o2
            WHERE o2.bbl = r.bbl AND o2.doc_type = 'DEED' AND o2.party_type = '2'
            ORDER BY o2.doc_date DESC LIMIT 1
        ) o ON true
        GROUP BY 1 ORDER BY evictions DESC, bldgs DESC LIMIT 5
    """), {"name": name}).fetchall()

    rank = db.execute(text("""
        WITH per_area AS (
            SELECT n.name, count(*) AS n
            FROM evictions_raw e
            JOIN neighborhoods n ON n.zip_code = e.zip_code
            WHERE e.eviction_type = 'Residential' AND n.name IS NOT NULL
            GROUP BY 1
        )
        SELECT count(*) FILTER (WHERE n >= (SELECT n FROM per_area WHERE name = :name)) AS rank,
               count(*) AS total
        FROM per_area
    """), {"name": name}).first()

    total, buildings = int(head.total), int(head.buildings or 0)
    y1, y2 = int(head.y1 or 0), int(head.y2 or 0)
    indexable = total >= _EV_AREA_MIN

    def _para(*parts) -> str:
        body = " ".join(x for x in parts if x)
        return f'<p class="prose">{body}</p>' if body else ""

    zip_links = ", ".join(
        f'<a href="/neighborhood/{e(z.zip_code)}">{e(z.zip_code)}</a>' for z in zips
    )
    where = f"{e(name)}, {e(borough)}" if borough else e(name)

    lede = _para(
        f"City marshals executed {_count(total, 'residential eviction')} in "
        f"{where} between {_en_date(head.first)} and {_en_date(head.last)}, "
        f"across {_count(buildings, 'building')}.",
        f"{e(name)} covers {_plural(len(zips), 'ZIP code')} {zip_links}."
        if zips else "",
        f"That ranks {rank.rank} of {rank.total} named NYC neighborhoods by "
        f"executed-eviction count, where 1 is the highest."
        if rank and rank.rank else "",
    )

    # Direction, stated only when there is a full prior year to compare against.
    trend = ""
    if y2:
        delta = (y1 - y2) / y2 * 100
        if abs(delta) < 1:
            movement = "essentially flat year over year."
        else:
            movement = f"{'up' if delta > 0 else 'down'} {abs(delta):.0f}%."
        trend = _para(
            f"In the twelve months to {_en_date(head.last)}, marshals executed "
            f"{_count(y1, 'eviction')} here, against {y2:,} in the twelve months "
            f"before that. That is {movement}",
            f"{_count(int(head.d30 or 0), 'eviction')} fell in the most recent "
            f"thirty days of the published record."
        )
    elif y1:
        trend = _para(
            f"{_count_open(y1, 'eviction')} fell in the twelve months to "
            f"{_en_date(head.last)}. The citywide record starts in April 2024, "
            f"so there is no full prior year here to compare against yet."
        )

    repeat_sec = ""
    if top:
        rows_html = "".join(
            f'<li class="rec-row"><a href="/property/{e(str(t.bbl))}">'
            f'<div><div class="rec-addr">'
            f'{e(_addr_title(t.address)) if t.address else "BBL " + e(str(t.bbl))}</div>'
            f'<div class="rec-geo">{e(t.zip_code or "")}</div></div>'
            f'<div class="rec-side"><div class="rec-amt">'
            f'{_count(int(t.n), "eviction")}</div>'
            f'<div class="rec-date">latest {_en_date(t.last)}</div></div></a></li>'
            for t in top
        )
        repeat_sec = (
            f"<h2>Where the evictions concentrate</h2>"
            + _para(
                f"{_count_open(repeat_total, 'building')} in {e(name)} "
                f"{'has' if repeat_total == 1 else 'have'} more than one executed "
                f"eviction on record"
                + (f"; the {len(top)} with the most are listed here."
                   if repeat_total > len(top) else ".")
                + " Repeat executions at one address are what "
                f"separates a building under pressure from a neighborhood with a "
                f"high count spread thinly."
            )
            + f'<ul class="rec-list">{rows_html}</ul>'
        )

    recent_sec = ""
    if recent:
        def _recent_row(r) -> str:
            label = (e(_addr_title(r.address)) if r.address
                     else "BBL " + e(str(r.bbl)))
            inner = (f'<div><div class="rec-addr">{label}</div>'
                     f'<div class="rec-geo">{e(r.zip_code or "")}</div></div>'
                     f'<div class="rec-side"><div class="rec-date">'
                     f'{_en_date(r.executed_date)}</div></div>')
            if r.bbl:
                return (f'<li class="rec-row"><a href="/property/{e(str(r.bbl))}">'
                        f'{inner}</a></li>')
            return f'<li class="rec-row"><div class="rec-static">{inner}</div></li>'

        recent_sec = "<h2>The most recent executions</h2>" + _para(
            f"The last {_count(len(recent), 'residential warrant')} a marshal "
            f"executed in {e(name)}, newest first. Each address links to that "
            f"building's own record: who holds the deed, what it sold for, and "
            f"what else the city has logged there.",
            f'Holding a marshal docket number or a court index number? '
            f'<a href="/eviction-case">Look up that case &rarr;</a>'
        ) + f'<ul class="rec-list">{"".join(_recent_row(r) for r in recent)}</ul>'

    owner_sec = ""
    if owners:
        def _owner_link(nm: str) -> str:
            slug_o = re.sub(r"[^a-z0-9]+", "-", (nm or "").lower()).strip("-")
            label = e(_entity_title(nm))
            if _is_buyer_entity(nm) and _LLC_SLUG_RE.match(slug_o):
                return f'<a href="/llc/{e(slug_o)}">{label}</a>'
            return label
        listed = "; ".join(
            f"{_owner_link(o.buyer)} ({_count(int(o.bldgs), 'building')}, "
            f"{int(o.evictions):,} evictions)"
            for o in owners
        )
        owner_sec = "<h2>Who holds the deeds</h2>" + _para(
            f"The most recent deed on each of those repeat-eviction buildings names "
            f"these buyers: {listed}.",
            "A deed names the party that took title, which is often a holding "
            "company rather than whoever manages the building day to day. The "
            "entity pages carry the rest of each buyer's NYC purchases."
        )

    after_sec = ""
    if after:
        after_sec = "<h2>What happened next</h2>" + _para(
            f"{_count_open(after, 'building')} in {e(name)} with an executed "
            f"eviction had a deed recorded within the following year. That "
            f"sequence, an eviction and then a transfer, is the pattern "
            f"PulseCities tracks citywide.",
            f'It is a sequence in the public record and not a finding about any '
            f'owner\'s conduct. <a href="/flips">How the flip scan reads it '
            f'&rarr;</a>'
        )

    # The eviction count answers "how many"; the score answers "and is this
    # area under pressure", which is the next question and the reason the rest
    # of the site exists.
    scores = db.execute(text("""
        SELECT ds.zip_code, ds.score FROM displacement_scores ds
        JOIN neighborhoods n ON n.zip_code = ds.zip_code
        WHERE n.name = :name AND ds.score IS NOT NULL
        ORDER BY ds.score DESC
    """), {"name": name}).fetchall()
    score_sec = ""
    if scores:
        top_z = scores[0]
        tier_label, _ = _tier_info(float(top_z.score))
        score_sec = f"<h2>{e(name)} beyond the eviction count</h2>" + _para(
            f"Evictions are one of five signals in the PulseCities displacement "
            f"index. {e(top_z.zip_code)} scores {float(top_z.score):.1f} out of 100, "
            f"which is {tier_label.lower()} pressure, read alongside LLC "
            f"acquisition rate, renovation permits, assessment spikes and 311 "
            f"complaint volume.",
            " ".join(
                f'<a href="/neighborhood/{e(z.zip_code)}">Full signal breakdown for '
                f'{e(z.zip_code)} &rarr;</a>' for z in scores[:2]
            )
        )

    limits_sec = "<h2>What this record does and does not cover</h2>" + _para(
        "These are evictions a city marshal actually executed, published by the "
        "city from April 2024 forward. They are the end of the process, so the "
        "count understates housing court activity in "
        f"{e(name)}: a case that settles, or a tenant who leaves before the "
        "marshal arrives, never appears here.",
        "Nothing on this page names a tenant. The city publishes the address, "
        "the date and the docket, and that is what is shown."
    )

    faq = [
        (f"How many evictions have there been in {name}?",
         f"City marshals executed {total:,} residential evictions in {name} "
         f"between {_en_date(head.first)} and {_en_date(head.last)}, across "
         f"{buildings:,} buildings. The most recent twelve months account for "
         f"{y1:,} of them."),
        (f"Which buildings in {name} have the most evictions?",
         (f"{repeat_total} building{'s' if repeat_total != 1 else ''} "
          f"{'have' if repeat_total != 1 else 'has'} more than one executed "
          f"eviction on record"
          + (f", and the {len(top)} with the most are listed above"
             if repeat_total > len(top) else ", and each is listed above")
          + " with the address and count. Repeat "
          f"executions at a single address are the signal worth following."
          if top else
          f"No building in {name} has more than one executed eviction in the "
          f"published record, so the count is spread across {buildings:,} "
          f"separate addresses rather than concentrated.")),
        ("What is a marshal eviction?",
         "A city marshal is the officer who carries out a warrant of eviction "
         "after a housing court case ends in the landlord's favor. The city "
         "publishes each execution with the address, the date and the docket "
         "number. An eviction that is filed, settled or abandoned before that "
         "point never reaches a marshal and is not in this dataset."),
        (f"How do I look up evictions for a specific address in {name}?",
         f"Search the address on PulseCities. Every building page carries its "
         f"own eviction record alongside the deed history, open violations and "
         f"rent-stabilization registrations, so you can see the eviction in the "
         f"context of who owned the building at the time."),
    ]
    if after:
        faq.append((
            f"Are landlords in {name} selling after they evict?",
            f"{after} buildings with an executed eviction had a deed recorded "
            f"within the next year. PulseCities reports the sequence in the "
            f"public record and makes no claim about why either event happened.",
        ))

    faq_html = "".join(
        f'<div class="faq-item"><h3>{e(q)}</h3><p>{e(a)}</p></div>' for q, a in faq
    )

    title = f"Evictions in {name}, NYC: the marshal record by address | PulseCities"
    # Built to fit: the old tail clause pushed long names past 165 chars and
    # the hard truncation shipped "eviction and." on every page.
    desc = (f"{total:,} residential evictions executed by city marshals in {name} "
            f"across {buildings:,} buildings, from NYC public records.")
    if len(desc) > 165:
        desc = (f"{total:,} marshal evictions in {name} across {buildings:,} "
                f"buildings, from NYC public records.")
    url = f"https://pulsecities.com/evictions/{slug}"

    jsonld = _jsonld({"@context": "https://schema.org", "@graph": [
        {"@type": "Dataset", "name": f"Executed marshal evictions in {name}, NYC",
         "description": desc, "url": url,
         "temporalCoverage": f"{head.first.isoformat()}/{head.last.isoformat()}",
         "creator": {"@type": "Organization", "name": "PulseCities"},
         "isBasedOn": "https://data.cityofnewyork.us/City-Government/Evictions/6z8x-wfk4"},
        {"@type": "FAQPage", "mainEntity": [
            {"@type": "Question", "name": q,
             "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faq]},
        _crumbs(("Home", "/"), ("Evictions", "/evictions"), (name, f"/evictions/{slug}")),
    ]})

    stat_cells = (
        f'<div class="stat"><div class="stat-num">{total:,}</div>'
        f'<div class="stat-label">evictions executed</div></div>'
        f'<div class="stat"><div class="stat-num">{buildings:,}</div>'
        f'<div class="stat-label">buildings</div></div>'
        f'<div class="stat"><div class="stat-num">{y1:,}</div>'
        f'<div class="stat-label">in the last 12 months</div></div>'
    )

    nearby = ""
    if borough:
        peers = db.execute(text("""
            SELECT n.name, count(*) AS c
            FROM evictions_raw e
            JOIN neighborhoods n ON n.zip_code = e.zip_code
            WHERE e.eviction_type = 'Residential' AND n.name IS NOT NULL
              AND n.name <> :name AND n.zip_code = ANY(:zips)
            GROUP BY 1 HAVING count(*) >= :floor
            ORDER BY count(*) DESC LIMIT 8
        """), {"name": name, "floor": _EV_AREA_MIN,
               "zips": [z for z in _borough_zips(borough, db)]}).fetchall()
        if peers:
            links = ", ".join(
                f'<a href="/evictions/{_ev_area_slug(pr.name)}">{e(pr.name)}</a>'
                for pr in peers
            )
            boro_slug = next((bs for bs, bn in _EV_BOROUGHS.items() if bn == borough), None)
            parent = (f'<a href="/evictions/{boro_slug}">Every {e(borough)} neighborhood '
                      f'on the record &rarr;</a>' if boro_slug else "")
            nearby = f"<h2>Elsewhere in {e(borough)}</h2>" + _para(
                f"{links}.", parent,
                f'<a href="/evictions">The citywide eviction tracker &rarr;</a>'
            )

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
{_llc_head(title, desc, url, "index, follow" if indexable else "noindex, follow", jsonld)}
</head>
<body>
{_ssr_nav("", toggle_html="")}
<div class="container" style="max-width:760px;">
  <p style="margin-bottom:8px;font-size:0.75rem;color:var(--faint);">
    <a href="/">Home</a> &middot; <a href="/evictions">Evictions</a>
  </p>
  <div class="eyebrow">NYC marshal evictions</div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.6rem;letter-spacing:0;font-weight:600;">Evictions in {e(name)}</h1>
  <div class="stats">{stat_cells}</div>
  {lede}
  {trend}
  {recent_sec}
  {repeat_sec}
  {owner_sec}
  {after_sec}
  {score_sec}
  {nearby}
  {limits_sec}
  <h2>Common questions</h2>
  {faq_html}
  <p class="note">Executed residential evictions published by the City of New York.
  This page describes public records, not conduct, and makes no claim of wrongdoing.
  <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    if len(_ev_area_cache) >= 256:
        now = time.monotonic()
        for k in [k for k, v in _ev_area_cache.items() if now >= v[1]]:
            _ev_area_cache.pop(k, None)
        if len(_ev_area_cache) >= 256:
            _ev_area_cache.clear()
    _ev_area_cache[slug] = (page, time.monotonic() + _EV_AREA_TTL)
    return HTMLResponse(page)


# --- Who owns my building: the search intent every tenant starts with ---------

_who_owns_cache: tuple[str, float] | None = None
_WHO_OWNS_TTL = 21600  # explainer copy plus a top-buyers list; 6h is plenty


@router.get("/who-owns-my-building", include_in_schema=False)
def who_owns_page(db: Session = Depends(get_db)):
    """Landing page for the "who owns my building" search intent.

    The GSC query log shows people arriving on bare addresses and LLC names;
    this page meets the question directly and routes it into address search,
    property pages, and operator profiles.
    """
    global _who_owns_cache
    if _who_owns_cache and time.monotonic() < _who_owns_cache[1]:
        return HTMLResponse(_who_owns_cache[0])

    esc = _html.escape

    buyers = db.execute(text("""
        SELECT slug, display_name, total_acquisitions
        FROM operators WHERE operator_class = 'operator'
        ORDER BY total_acquisitions DESC NULLS LAST LIMIT 5
    """)).fetchall()

    buyers_html = "".join(
        f'<li class="buyer-row"><a href="/operator/{esc(b.slug)}">'
        f'<span class="buyer-name">{esc(b.display_name)}</span>'
        f'<span class="buyer-count">{int(b.total_acquisitions or 0):,} acquisitions</span>'
        f'</a></li>\n'
        for b in buyers
    )

    title = "Who owns my building? Look up any NYC address | PulseCities"
    desc = ("Look up any NYC address and see who took the deed, what they paid, and what "
            "happened next: evictions, permits, and violations, all from public records.")

    faq = [
        ("How do I find out who owns a building in NYC?",
         "Start with the deed. Every property sale in NYC is recorded in ACRIS, the city "
         "register, and names the buyer. Search your address on PulseCities and the "
         "property page shows the most recent deed holder along with the eviction, "
         "permit, and violation record for the building."),
        ("Why is the owner an LLC instead of a person?",
         "Most investor purchases close under a limited liability company, often one LLC "
         "per building, which keeps the people behind it off the deed. PulseCities "
         "groups related LLCs into operator networks using shared names and filing "
         "patterns, so one name leads to the wider portfolio."),
        ("Is ownership information public?",
         "Yes. Deeds, mortgages, permits, violations, and eviction records are all "
         "public record. PulseCities reads the same city sources anyone can, and links "
         "each claim to its document ID where one exists."),
        ("Can I find out if my building is rent-stabilized?",
         "DHCR publishes building-level counts of rent-stabilized units once a year. "
         "Neighborhood pages track stabilized-unit loss as a signal; for a binding "
         "answer on your own lease, request your rent history from DHCR directly."),
    ]
    faq_html = "".join(
        f'<div class="faq-item"><h3>{esc(q)}</h3><p>{esc(a)}</p></div>' for q, a in faq
    )

    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "FAQPage",
                "mainEntity": [
                    {"@type": "Question", "name": q,
                     "acceptedAnswer": {"@type": "Answer", "text": a}}
                    for q, a in faq
                ],
            },
            _crumbs(("Home", "/"), ("Who owns my building", "/who-owns-my-building")),
        ],
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{esc(title)}</title>
<meta name="description" content="{esc(desc)}">
<link rel="canonical" href="https://pulsecities.com/who-owns-my-building">
<meta property="og:title" content="{_html.escape(title)}">
<meta property="og:description" content="{esc(desc)}">
<meta property="og:url" content="https://pulsecities.com/who-owns-my-building">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og/site/who-owns.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{esc(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og/site/who-owns.png">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
h1,h2,h3{{text-wrap:balance}}
body{{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}}
nav{{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}}
.nav-inner{{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
.nav-inner>div::-webkit-scrollbar{{display:none}}
.container{{max-width:720px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.eyebrow{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;letter-spacing:0.18em;color:#ed6317;text-transform:uppercase;margin-bottom:10px}}
h1{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.7rem;font-weight:600;margin-bottom:8px}}
h2{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.05rem;font-weight:600;margin:34px 0 6px}}
p{{font-size:0.86rem;color:#93a1ad;line-height:1.65;max-width:640px}}
p a,.body-link{{color:#6fb1d8}}
p a:hover{{text-decoration:underline}}
.search-row{{display:flex;gap:10px;margin:22px 0 6px;max-width:560px}}
.search-row input{{flex:1;font-family:'JetBrains Mono',monospace;font-size:0.85rem;color:#e4e8ec;background:#16202d;border:1px solid rgba(147,161,173,0.2);border-radius:8px;padding:12px 14px;min-width:0}}
.search-row input::placeholder{{color:var(--faint)}}
.search-row button{{font-family:'DM Sans',sans-serif;font-size:0.9rem;font-weight:600;color:#111823;background:#ed6317;border:none;border-radius:8px;padding:12px 22px;cursor:pointer}}
.search-row button:hover{{background:#f0854b}}
.search-hint{{font-size:0.75rem;color:var(--faint);margin-bottom:4px}}
.buyer-list{{list-style:none;padding:0;margin:8px 0 0}}
.buyer-row{{border-bottom:1px solid rgba(147,161,173,0.07)}}
.buyer-row a{{display:flex;align-items:baseline;justify-content:space-between;gap:16px;padding:11px 0}}
.buyer-name{{font-family:'JetBrains Mono',monospace;font-size:0.85rem;font-weight:500;color:#e4e8ec}}
.buyer-row a:hover .buyer-name{{color:#ed6317}}
.buyer-count{{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:#93a1ad;flex-shrink:0}}
.faq-item h3{{font-size:0.9rem;font-weight:600;margin:20px 0 4px}}
.faq-item p{{font-size:0.82rem}}
.note{{font-size:0.75rem;color:var(--faint);margin-top:26px;line-height:1.6}}
.note a{{color:var(--accent)}}
</style>
</head>
<body>
{_ssr_nav("", toggle_html="")}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:var(--faint);">&#8592; Home</a>
  </div>
  <div class="eyebrow">NYC ownership records</div>
  <h1>Who owns my building?</h1>
  <p>Every NYC property sale is a public record. Search your address and see who took the deed, what they paid, and what happened next in the building: evictions, permits, and violations.</p>
  <form class="search-row" action="/map" method="get">
    <input type="text" name="q" placeholder="Enter an address, ZIP, or neighborhood" aria-label="Search an address, ZIP, or neighborhood">
    <button type="submit">Search</button>
  </form>
  <p class="search-hint">Free, no signup. Public records only</p>

  <h2>Start with the deed</h2>
  <p>Deeds are recorded in ACRIS, the city register run by the Department of Finance. The most recent deed names the current owner of record and the price paid. A PulseCities property page puts that deed next to the building's eviction, permit, and violation history, with ACRIS document IDs you can verify yourself. <a href="/property/4109220015">See a worked example &rarr;</a></p>

  <h2>When the owner is an LLC</h2>
  <p>Most investor purchases close under a limited liability company, often a fresh LLC per building, which keeps the people behind the purchase off the deed. PulseCities groups related LLCs into operator networks using shared naming and filing patterns, so one LLC on your deed can lead to the whole portfolio. <a href="/operators">Browse the operator directory &rarr;</a></p>

  <h2>Most active buyers on record</h2>
  <p class="search-hint" style="margin-top:2px;">Operator networks ranked by recorded LLC acquisitions</p>
  <ul class="buyer-list">
{buyers_html}  </ul>

  <p style="margin-top:10px;font-size:0.8rem;"><a href="/llc" class="body-link">The full ledger of LLC buyers &rarr;</a></p>

  <h2>What to check after the owner</h2>
  <p>The name on the deed matters less than what the record shows around it. On any property or neighborhood page, look at executed evictions, renovation permits filed soon after a purchase, open HPD violations, and rent-stabilized unit counts over time. Every neighborhood page has a watch card that emails you when the record moves. <a href="/evictions">Citywide eviction tracker &rarr;</a> <a href="/is-my-building-rent-stabilized" style="margin-left:10px;">Is my building rent stabilized &rarr;</a> <a href="/eviction-case" style="margin-left:10px;">Look up an eviction case &rarr;</a></p>

  <h2>Other official registries</h2>
  <p>For the deed itself, <a href="https://a836-acris.nyc.gov/DS/DocumentSearch/BBL" target="_blank" rel="noopener noreferrer">search ACRIS directly</a>. Multiple-dwelling landlords must also register a managing agent with HPD, searchable on <a href="https://hpdonline.nyc.gov/hpdonline/" target="_blank" rel="noopener noreferrer">HPD Online</a>. PulseCities indexes the deed record and the enforcement record; HPD registration can name a person where the deed names a shell.</p>

  <h2>Common questions</h2>
  {faq_html}

  <p class="note">Ownership records describe documents, not conduct. Nothing here is a claim of wrongdoing by any owner. <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    _who_owns_cache = (page, time.monotonic() + _WHO_OWNS_TTL)
    return HTMLResponse(page)


# --- LLC entity pages: one page per deed-record buyer -------------------------
#
# Tenants google the LLC on their deed or eviction notice. Any entity in the
# deed record renders on demand so those searches always land; only LLC-named
# entities with 2+ properties are indexed and sitemapped, mirroring the
# property-page noindex policy. Slugs resolve through _SLUG_SQL, which must
# stay byte-identical to the expression in scripts/add_entity_slug_index.sql.

def _dos_agent_kind(entity_name: str, agent_name: str | None) -> str:
    """Whether a designated process agent names anyone. Imported from the
    refresh script so the page and the ingest cannot disagree about what counts
    as a real third party, which is the difference between naming a controlling
    party and printing "The Limited Liability Company" as a finding."""
    from scripts.refresh_dos_entities import agent_kind
    return agent_kind(entity_name, agent_name)


def _is_buyer_entity(name: str) -> bool:
    """A company that actually buys, as opposed to a servicer or trustee
    taking title in foreclosure, and not a name the source truncated
    mid-token (party_name_normalized is cut at 48 characters)."""
    if not name or len(name) >= 48:
        return False
    return bool(_ENTITY_FORM_RE.search(name)) and not _NOT_A_BUYER_RE.search(name)


# How many actual buildings sit behind an entity's deeds.
#
# A condominium records one deed per unit, so a raw lot count reads as
# portfolio breadth it does not have. The old rule proxied for this with
# "2+ tax blocks", which does exclude a whole-condo buy but also excludes a
# genuine three-building portfolio that happens to sit on one block. That cost
# real traffic: NORWORTH HOLDINGS LLC is three buildings on one block, it was
# noindex and unsitemapped, and it earned 3 of the site's 5 total clicks.
#
# Collapsing unit lots (1001 and up) to their block counts what a reader would
# count. Verified against both failure modes: NORWORTH scores 3, while
# JOBER EXECUTIVE HOUSE LLC's 53 unit deeds score 1.
_BUILDING_KEY_SQL = (
    "substring(bbl, 1, 6) || CASE WHEN substring(bbl, 7, 4) >= '1001' "
    "THEN '0000' ELSE substring(bbl, 7, 4) END"
)
# Two, not three, since 2026-08-18. The floor exists to keep near-duplicate
# pages out of the index, so it should be set by measuring duplication rather
# than by picking a round number. Measured with the method the rest of this
# repo uses, 5-gram containment over digit-bearing tokens, 14 pages sampled per
# group:
#
#     1 building     15,020 entities   584 words   67% mean overlap, 78% max
#     2 buildings       499 entities   677 words   55% mean overlap, 72% max
#     3+ buildings      156 entities   702 words   55% mean overlap, 72% max
#
# The two-building pages are indistinguishable from the three-building pages
# already indexed, and both sit under /neighborhood's 68-69%, the benchmark for
# a template that indexes cleanly. One-building pages are visibly worse and
# stay out; there are 15,020 of them and they would be a doorway flood.
#
# The cost of the old floor was measurable: TERRA DEVELOPERS LLC (two
# buildings) took 17 impressions and one of the site's five clicks while
# telling Google not to index it.
_LLC_MIN_BUILDINGS = 2
_LLC_MIN_LOTS = 2


def _building_count(bbls) -> int:
    """Python twin of _BUILDING_KEY_SQL, for the route's own robots decision."""
    keys = set()
    for bbl in bbls:
        if not bbl or len(bbl) < 10:
            continue
        block, lot = bbl[:6], bbl[6:10]
        keys.add(block + ("0000" if lot >= "1001" else lot))
    return len(keys)


def _lot_label(bbls: int, blocks: int) -> str:
    """Whole-condo purchases record one deed per unit, so a raw lot count
    reads as portfolio breadth it does not have."""
    if blocks and blocks < bbls:
        return (f"{bbls} lots in {blocks} building" + ("s" if blocks != 1 else ""))
    return f"{bbls} propert" + ("ies" if bbls != 1 else "y")


_llc_page_cache: dict[str, tuple[str, float]] = {}
_llc_dir_cache: tuple[str, float] | None = None
_LLC_DIR_TTL = 21600
_LLC_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,78}$")
_SLUG_SQL = "btrim(regexp_replace(lower(party_name_normalized), '[^a-z0-9]+', '-', 'g'), '-')"

_LLC_PAGE_CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
h1,h2,h3{text-wrap:balance}
body{font-family:'DM Sans',sans-serif;background:#111823;color:#eef2f5;min-height:100vh}
nav{border-bottom:1px solid rgba(147,161,173,0.08);padding:12px 0}
.nav-inner{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}
.nav-inner>div::-webkit-scrollbar{display:none}
.container{max-width:860px;margin:0 auto;padding:32px 20px 80px}
a{color:inherit;text-decoration:none}
footer{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(147,161,173,0.08);margin-top:32px;font-size:12px;color:var(--muted)}
.footer-links{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}
@media(max-width:767px){.container{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}
.eyebrow{font-family:'JetBrains Mono',monospace;font-size:0.75rem;letter-spacing:0.18em;color:#ed6317;text-transform:uppercase;margin-bottom:10px}
h1{font-family:'JetBrains Mono',monospace;font-size:1.35rem;font-weight:500;letter-spacing:0.03em;margin-bottom:6px}
h2{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.05rem;font-weight:600;margin:34px 0 4px}
.sub{font-size:0.82rem;color:#93a1ad;line-height:1.6;max-width:640px}
.stats{display:flex;flex-wrap:wrap;gap:10px 40px;margin:26px 0 4px;padding:14px 2px;border-top:1px solid rgba(147,161,173,0.22);border-bottom:1px solid rgba(147,161,173,0.1)}
.stat{display:flex;align-items:baseline;gap:8px}
.stat-num{font-family:'JetBrains Mono',monospace;font-weight:600;font-size:1.35rem;line-height:1;color:#eef2f5}
.stat-label{font-size:0.75rem;color:#93a1ad;text-transform:uppercase;letter-spacing:0.06em;line-height:1.3}
.section-sub{font-size:0.76rem;color:var(--dim);margin-bottom:10px}
.rec-list{list-style:none;padding:0;margin:0}
.rec-row{border-bottom:1px solid rgba(147,161,173,0.07)}
.rec-row a,.rec-static{display:flex;align-items:baseline;justify-content:space-between;gap:16px;padding:12px 0}
.rec-row a:hover .rec-addr{color:#ed6317}
.rec-addr{font-family:'JetBrains Mono',monospace;font-size:0.86rem;font-weight:500;color:#e4e8ec;letter-spacing:0.02em;overflow-wrap:anywhere}
.rec-geo{font-size:0.75rem;color:var(--dim);margin-top:2px}
.rec-doc{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-top:3px;overflow-wrap:anywhere}
.rec-side{display:flex;flex-direction:column;align-items:flex-end;flex-shrink:0;text-align:right}
.rec-amt{font-family:'JetBrains Mono',monospace;font-size:0.8rem;color:#c9d2da}
.rec-date{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-top:2px}
.cross{font-size:0.82rem;color:#93a1ad;line-height:1.6;max-width:640px}
.prose{font-size:0.86rem;color:#93a1ad;line-height:1.7;margin-bottom:10px;max-width:64ch}
.prose a{color:var(--accent)}
.prose a:hover{text-decoration:underline}
.faq-item h3{font-size:0.9rem;font-weight:600;margin:20px 0 4px;color:#e4e8ec}
.faq-item p{font-size:0.82rem;color:#93a1ad;line-height:1.6;max-width:640px}
.cross a{color:var(--accent)}
.note{font-size:0.75rem;color:var(--faint);margin-top:26px;line-height:1.6}
.note a{color:var(--accent)}
.mono-note{font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:var(--faint);margin-top:6px}
"""


def _llc_head(title: str, desc: str, url: str, robots: str, jsonld: str,
              image: str = "https://pulsecities.com/og-image.png") -> str:
    e = _html.escape
    return f"""<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#111823">
<title>{e(title)}</title>
<meta name="description" content="{e(desc)}">
<meta name="robots" content="{robots}">
<link rel="canonical" href="{url}">
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="{e(desc)}">
<meta property="og:url" content="{url}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="{image}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{e(title)}">
<meta name="twitter:description" content="{e(desc)}">
<meta name="twitter:image" content="{image}">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="/fonts/dm-sans-latin.woff2" as="font" type="font/woff2" crossorigin><style>@font-face{{font-family:'Bricolage Grotesque';font-style:normal;font-weight:600 700;font-display:swap;src:url(/fonts/bricolage-grotesque-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'DM Sans';font-style:normal;font-weight:400 600;font-display:swap;src:url(/fonts/dm-sans-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}@font-face{{font-family:'JetBrains Mono';font-style:normal;font-weight:400 700;font-display:swap;src:url(/fonts/jetbrains-mono-latin.woff2) format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}</style>
<style>{_LLC_PAGE_CSS}</style>"""


@router.get("/llc", include_in_schema=False)
def llc_directory(db: Session = Depends(get_db)):
    """Directory of the most active LLC buyers in the deed record."""
    global _llc_dir_cache
    if _llc_dir_cache and time.monotonic() < _llc_dir_cache[1]:
        return HTMLResponse(_llc_dir_cache[0])

    esc = _html.escape
    rows = db.execute(text(f"""
        SELECT party_name_normalized AS name, {_SLUG_SQL} AS slug,
               count(DISTINCT bbl) AS bbls,
               count(DISTINCT substring(bbl, 1, 6)) AS blocks,
               max(doc_date) AS last_seen
        FROM ownership_raw
        WHERE doc_type = 'DEED' AND party_type = '2'
          AND party_name_normalized LIKE '%LLC%'
          AND {real_date('doc_date')}
        GROUP BY 1, 2
        HAVING count(DISTINCT bbl) >= {_LLC_MIN_LOTS}
           AND count(DISTINCT ({_BUILDING_KEY_SQL})) >= {_LLC_MIN_BUILDINGS}
        ORDER BY blocks DESC, bbls DESC, last_seen DESC LIMIT 100
    """)).fetchall()

    items = ""
    listed: list[dict] = []
    for r in rows:
        if not _is_buyer_entity(r.name):
            continue
        if not _LLC_SLUG_RE.match(r.slug or ""):
            continue
        items += (
            f'<li class="rec-row"><a href="/llc/{esc(r.slug)}">'
            f'<div><div class="rec-addr">{esc(r.name)}</div></div>'
            f'<div class="rec-side"><div class="rec-amt">{_lot_label(int(r.bbls), int(r.blocks))}</div>'
            f'<div class="rec-date">latest {esc(r.last_seen.isoformat()) if r.last_seen else ""}</div></div>'
            f'</a></li>\n'
        )
        listed.append({
            "@type": "ListItem", "position": len(listed) + 1, "name": r.name,
            "url": f"https://pulsecities.com/llc/{r.slug}",
        })

    # Families are the answer to "these forty names are one landlord", so the
    # directory that lists the names should say so.
    fams = sorted(_families(db).values(),
                  key=lambda f: -max(f["buildings"], f.get("sold", 0)))
    # A family that sold everything reads "(0 buildings)" as broken; say what
    # actually happened to the portfolio instead.
    def _fam_size(f) -> str:
        if f["buildings"]:
            return f'({f["buildings"]} buildings)'
        return f'(all {f.get("sold", 0)} sold)'
    fam_links = " &middot; ".join(
        f'<a href="/network/{esc(f["slug"])}">{esc(f["label"])}</a> '
        f'{_fam_size(f)}'
        for f in fams
    )
    family_section = (
        '  <h2 style="font-family:\'Bricolage Grotesque\',\'DM Sans\',sans-serif;'
        'font-size:1.05rem;font-weight:600;margin:34px 0 4px;">Entities that belong together</h2>\n'
        '  <p class="sub">Some of the names above are one operation filing under many '
        'companies. Where a shared naming pattern and a shared filing address both '
        'agree, PulseCities groups them. '
        '<a href="/network">All owner networks &rarr;</a></p>\n'
        f'  <p class="cross" style="line-height:2;margin-top:8px;">{fam_links}</p>\n'
    ) if fam_links else ""

    title = "NYC LLC property buyers: the deed record | PulseCities"
    desc = ("The most active LLC buyers in NYC's deed record, ranked by properties "
            "acquired, each with its full purchase history from ACRIS public records.")
    # Every other directory on the site declares its list; this one listed 100
    # entities and declared a breadcrumb.
    jsonld = _jsonld({"@context": "https://schema.org", "@graph": [
        {"@type": "ItemList", "name": "NYC LLC property buyers",
         "numberOfItems": len(listed), "itemListElement": listed},
        _crumbs(("Home", "/"), ("LLC buyers", "/llc")),
    ]})

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
{_llc_head(title, desc, "https://pulsecities.com/llc", "index, follow", jsonld, image="https://pulsecities.com/og/site/llc.png")}
</head>
<body>
{_ssr_nav("", toggle_html="")}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:var(--faint);">&#8592; Home</a>
  </div>
  <div class="eyebrow">NYC deed record</div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;letter-spacing:0;font-weight:600;">The LLC buyers</h1>
  <p class="sub">Every entity below appears as the buyer on three or more NYC deeds, across more than one building. The deed record here begins in 2025. Each page lists that entity's recorded deeds, with links to every building. Curated operator networks live in the <a href="/operators" style="color:#6fb1d8;">operator directory</a>; this is the raw ledger.</p>
  <ul class="rec-list" style="margin-top:18px;">
{items}  </ul>
{family_section}
  <p class="note">A deed names a buyer of record. Appearing here describes documents, not conduct. <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    _llc_dir_cache = (page, time.monotonic() + _LLC_DIR_TTL)
    return HTMLResponse(page)


@router.get("/llc/{slug}", include_in_schema=False)
def llc_entity_page(slug: str, db: Session = Depends(get_db)):
    """Deed-record profile for one exact buyer entity."""
    slug = slug.lower()
    if not _LLC_SLUG_RE.match(slug):
        return _not_found()

    cached = _llc_page_cache.get(slug)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    esc = _html.escape

    ent = db.execute(text(f"""
        SELECT party_name_normalized AS name,
               count(DISTINCT bbl) FILTER (WHERE party_type = '2') AS buys
        FROM ownership_raw
        WHERE doc_type = 'DEED' AND {_SLUG_SQL} = :slug
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """), {"slug": slug}).first()
    if not ent:
        return _not_found()
    name = ent.name

    def _side(party_type: str):
        # 17,114 of 64,849 deed BBLs are condo unit lots (1001 and up) that
        # PLUTO does not carry, so a quarter of this table joined to nothing:
        # no address, no ZIP, no neighbourhood link. condo_unit_addresses
        # covers the unambiguous subset with a full address (single billing
        # lot on the block, refreshed nightly). For the rest, the tax block is
        # shared with the parcels PLUTO does carry, and 92% of blocks sit in
        # exactly one ZIP. Where the block is unambiguous, take the ZIP from
        # it; where it is not, take nothing. No address is ever guessed.
        return db.execute(text("""
            SELECT DISTINCT ON (o.document_id)
                   o.bbl, o.doc_date, o.doc_amount, o.document_id,
                   coalesce(p.address, c.address) AS address,
                   coalesce(p.zip_code, c.zip_code, blk.zip_code) AS zip_code,
                   coalesce(n.name, cn.name, bn.name) AS hood
            FROM ownership_raw o
            LEFT JOIN parcels p ON p.bbl = o.bbl
            LEFT JOIN neighborhoods n ON n.zip_code = p.zip_code
            LEFT JOIN condo_unit_addresses c ON c.bbl = o.bbl AND p.bbl IS NULL
            LEFT JOIN neighborhoods cn ON cn.zip_code = c.zip_code
            LEFT JOIN LATERAL (
                SELECT max(q.zip_code) AS zip_code
                FROM parcels q
                WHERE p.bbl IS NULL AND c.bbl IS NULL
                  AND q.bbl >= substring(o.bbl, 1, 6) || '0000'
                  AND q.bbl <= substring(o.bbl, 1, 6) || '9999'
                  AND q.zip_code IS NOT NULL
                HAVING count(DISTINCT q.zip_code) = 1
            ) blk ON true
            LEFT JOIN neighborhoods bn ON bn.zip_code = blk.zip_code
            WHERE o.doc_type = 'DEED' AND o.party_type = :pt
              AND o.party_name_normalized = :name
            ORDER BY o.document_id, o.doc_date DESC
        """), {"pt": party_type, "name": name}).fetchall()

    buys = sorted(_side("2"), key=lambda r: r.doc_date or date.min, reverse=True)
    sells = sorted(_side("1"), key=lambda r: r.doc_date or date.min, reverse=True)

    post_ev = db.execute(text("""
        SELECT count(DISTINCT o.bbl) FROM ownership_raw o
        JOIN evictions_raw e ON e.bbl = o.bbl
         AND e.eviction_type = 'Residential'
         AND e.executed_date < o.doc_date
         AND e.executed_date >= o.doc_date - 365
        WHERE o.doc_type = 'DEED' AND o.party_type = '2'
          AND o.party_name_normalized = :name AND o.doc_amount > 0
    """), {"name": name}).scalar() or 0

    network = db.execute(text("""
        SELECT slug, display_name FROM operators
        WHERE operator_class = 'operator' AND jsonb_exists(llc_entities, :name) LIMIT 1
    """), {"name": name}).first()

    # What the portfolio itself carries. The page described the deeds and never
    # the buildings, which is where an entity page stops being a receipt and
    # starts being worth reading.
    portfolio = db.execute(text("""
        WITH held AS (
            SELECT DISTINCT o.bbl FROM ownership_raw o
            WHERE o.doc_type = 'DEED' AND o.party_type = '2'
              AND o.party_name_normalized = :name
        )
        SELECT
            count(*) FILTER (WHERE p.units_res > 0) AS residential,
            sum(p.units_res) AS units,
            min(p.year_built) FILTER (WHERE p.year_built > 1700) AS oldest,
            max(p.year_built) FILTER (WHERE p.year_built > 1700) AS newest,
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM evictions_raw e WHERE e.bbl = h.bbl)) AS with_eviction,
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM violations_raw v WHERE v.bbl = h.bbl
                  AND v.current_status NOT IN :resolved)) AS with_violation,
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM rs_buildings rs WHERE rs.bbl = h.bbl
                  AND rs.source = 'dhcr' AND rs.rs_unit_count > 0)) AS with_rs
        FROM held h LEFT JOIN parcels p ON p.bbl = h.bbl
    """), {"name": name, "resolved": _VIOLATION_RESOLVED}).first()

    # Where the entity files from. ACRIS party addresses were recorded as
    # unavailable in an earlier pass; they are populated on ~28% of buyer rows
    # now, and they cluster: dozens of separate LLCs file from one suite.
    #
    # The street line is printed only when two or more buying entities share
    # it, which is what makes it a registered-agent or management address
    # rather than somebody's house. A single-entity filing gets its locality
    # and nothing narrower, because out-of-borough ownership is the signal
    # worth reporting and the street number adds nothing to it.
    # The state's registration for this company, when we hold it. The deed says
    # who took title; this says who filed the company and where the state sends
    # process, which is the question the search data shows people actually ask.
    dos = db.execute(text("""
        SELECT d.dos_id, d.entity_type, d.jurisdiction, d.initial_filing_date,
               d.agent_name, d.agent_address, d.agent_city, d.agent_state,
               -- Normalized, not upper(). The same agent is spelled six ways in
               -- DOS ("C/O SUMMIT MALLS MANAGEMENT LLC", "summit malls
               -- management llc", with and without the suite line), and an
               -- exact match undercounted this one at 63 against a real 76.
               (SELECT count(*) FROM dos_entities x
                WHERE x.agent_name IS NOT NULL
                  AND regexp_replace(upper(regexp_replace(x.agent_name, '^C ?/?O ', '', 'i')),
                                     '[^A-Z0-9]+', ' ', 'g')
                    = regexp_replace(upper(regexp_replace(d.agent_name, '^C ?/?O ', '', 'i')),
                                     '[^A-Z0-9]+', ' ', 'g')) AS agent_shared
        FROM dos_entities d
        WHERE d.entity_name_normalized =
              regexp_replace(upper(:name), '[^A-Z0-9]+', ' ', 'g')
        ORDER BY d.initial_filing_date DESC NULLS LAST
        LIMIT 1
    """), {"name": name}).first()

    filings = db.execute(text("""
        SELECT o.party_addr_1 AS addr, o.party_city AS city, o.party_state AS st,
               (SELECT count(DISTINCT x.party_name_normalized)
                FROM ownership_raw x
                WHERE x.doc_type = 'DEED' AND x.party_type = '2'
                  AND x.party_addr_1 = o.party_addr_1
                  AND x.party_zip IS NOT DISTINCT FROM o.party_zip) AS entities
        FROM ownership_raw o
        WHERE o.doc_type = 'DEED' AND o.party_type = '2'
          AND o.party_name_normalized = :name AND o.party_addr_1 IS NOT NULL
        GROUP BY 1, 2, 3, o.party_zip
        ORDER BY entities DESC
        LIMIT 1
    """), {"name": name}).first()

    # Named buildings behind the pre-purchase eviction count. The count alone
    # was a number with nothing under it; the addresses are the evidence.
    ev_before = db.execute(text("""
        SELECT DISTINCT ON (o.bbl) o.bbl, p.address, p.zip_code,
               o.doc_date, e.executed_date
        FROM ownership_raw o
        JOIN evictions_raw e ON e.bbl = o.bbl
         AND e.eviction_type = 'Residential'
         AND e.executed_date < o.doc_date
         AND e.executed_date >= o.doc_date - 365
        LEFT JOIN parcels p ON p.bbl = o.bbl
        WHERE o.doc_type = 'DEED' AND o.party_type = '2'
          AND o.party_name_normalized = :name AND o.doc_amount > 0
        ORDER BY o.bbl, e.executed_date DESC
        LIMIT 6
    """), {"name": name}).fetchall()

    n_bbls = len({r.bbl for r in buys if r.bbl})
    n_blocks = len({r.bbl[:6] for r in buys if r.bbl})
    volume = sum(float(r.doc_amount) for r in buys if r.doc_amount and float(r.doc_amount) > 0)
    dates = [r.doc_date for r in buys if r.doc_date]
    first_seen = min(dates).year if dates else None
    last_seen = max(dates).isoformat() if dates else None

    def _rows(records):
        # The ACRIS document ID and the BBL are printed on every row because the
        # people arriving here search for them: "acris deed search <llc> 2018",
        # "<address> deeds acris records block lot bbl". They are also what makes
        # a row checkable against the city's own record, which is the pitch.
        out = ""
        for r in records[:60]:
            addr = esc(_addr_title(r.address)) if r.address else f"BBL {esc(str(r.bbl))}"
            geo = esc(f"{r.hood}, {r.zip_code}" if r.hood else (r.zip_code or ""))
            amt = _fmt_amount(r.doc_amount) or "no amount recorded"
            when = esc(r.doc_date.isoformat()) if r.doc_date else ""
            ref = " / ".join(p for p in (
                f"ACRIS {esc(str(r.document_id))}" if r.document_id else "",
                f"BBL {esc(str(r.bbl))}" if r.bbl and r.address else "",
            ) if p)
            inner = (f'<div><div class="rec-addr">{addr}</div><div class="rec-geo">{geo}</div>'
                     + (f'<div class="rec-doc">{ref}</div>' if ref else "")
                     + f'</div><div class="rec-side"><div class="rec-amt">{esc(amt)}</div>'
                     f'<div class="rec-date">{when}</div></div>')
            if r.bbl:
                out += f'<li class="rec-row"><a href="/property/{esc(str(r.bbl))}">{inner}</a></li>\n'
            else:
                out += f'<li class="rec-row"><div class="rec-static">{inner}</div></li>\n'
        return out

    buys_html = _rows(buys) or '<li class="rec-row"><div class="rec-static"><div class="rec-geo">No purchase deeds in the current record</div></div></li>'
    sells_section = ""
    if sells:
        sells_section = f"""
  <h2>Deeds sold</h2>
  <p class="section-sub">Conveyances where this entity is the grantor of record</p>
  <ul class="rec-list">
{_rows(sells)}  </ul>"""

    network_line = ""
    if network:
        network_line = (f'<p class="cross" style="margin-top:10px;">This entity is part of the '
                        f'<a href="/operator/{esc(network.slug)}">{esc(network.display_name)} network &rarr;</a></p>')
    else:
        # Derived family. An LLC page used to link to its own buildings and
        # nothing else, so a portfolio held one company at a time read as a
        # portfolio of strangers.
        fam = next((f for f in _families(db).values() if name in f["entities"]), None)
        if fam:
            others = len(fam["entities"]) - 1
            network_line = (
                f'<p class="cross" style="margin-top:10px;">The deed record links this '
                f'entity to {_count(others, "other company")} filing under the '
                f'{esc(fam["label"])} name, holding {fam["buildings"]} buildings between '
                f'them. <a href="/network/{esc(fam["slug"])}">The {esc(fam["label"])} '
                f'entities &rarr;</a> '
                f'<a href="/network/{esc(fam["slug"])}#follow">Follow the '
                f'portfolio &rarr;</a></p>'
            )

    # --- The reading of the ledger -------------------------------------------
    # The page listed deeds and said almost nothing about them. Everything
    # below is derived from this entity's own rows, which is what stops 122
    # pages from being one page with the name swapped.
    def _para(*sentences) -> str:
        body = " ".join(s for s in sentences if s)
        return f'<p class="prose">{body}</p>' if body else ""

    def _prose_section(h2, *paragraphs) -> str:
        inner = "".join(p for p in paragraphs if p)
        return f"<h2>{h2}</h2>{inner}" if inner else ""

    priced = sorted((r for r in buys if r.doc_amount and float(r.doc_amount) > 0),
                    key=lambda r: float(r.doc_amount), reverse=True)
    span_start = min(dates) if dates else None
    span_end = max(dates) if dates else None

    lede_parts = [
        f"{esc(name)} appears as the buyer of record on "
        f"{_count(len(buys), 'NYC deed')} in the ACRIS record PulseCities holds."
    ]
    if span_start and span_end and span_start != span_end:
        lede_parts.append(
            f"Those deeds run from {_en_date(span_start)} to {_en_date(span_end)}, "
            f"a span of {_hold_length(span_start, span_end)}."
        )
    elif span_end:
        lede_parts.append(f"The single recorded date is {_en_date(span_end)}.")
    if n_blocks and n_blocks < n_bbls:
        lede_parts.append(
            f"They cover {_count(n_bbls, 'tax lot')} across {_count(n_blocks, 'building')}, "
            f"the gap being whole-building purchases that record one deed per unit."
        )
    else:
        lede_parts.append(f"They cover {_count(n_bbls, 'distinct tax lot')}.")
    # A deed filed at $10 is not a price. Where every deed is nominal, totalling
    # them and calling the biggest one "the largest purchase" reports a number
    # that means nothing, so those lines are suppressed rather than dressed up.
    nominal = [r for r in priced if float(r.doc_amount) < 1000]
    all_nominal = bool(priced) and len(nominal) == len(priced)

    if volume and not all_nominal:
        lede_parts.append(
            f"Stated consideration across the priced deeds totals {_fmt_amount(volume)}"
            + (f", an average of {_fmt_amount(volume / len(priced))} per deed."
               if priced else ".")
        )
    if priced and priced[0].address and not all_nominal:
        lede_parts.append(
            f"The largest single purchase is {esc(_addr_title(priced[0].address))} at "
            f"{_fmt_amount(priced[0].doc_amount)}."
        )
    if all_nominal:
        lede_parts.append(
            f"No deed here states a real price. Every priced deed records a nominal "
            f"consideration under $1,000, the filing pattern of a transfer between "
            f"related parties rather than an arm's length sale, so there is no "
            f"purchase total worth reporting."
        )
    elif nominal:
        lede_parts.append(
            f"{_count_open(len(nominal), 'of those deeds', 'of those deeds')} record" + ("s" if len(nominal) == 1 else "") + " a nominal "
            f"consideration under $1,000, which usually marks a related-party transfer."
        )
    unpriced = len(buys) - len(priced)
    if unpriced:
        lede_parts.append(
            f"{_count_open(unpriced, 'deed')} carr" + ("ies" if unpriced == 1 else "y") + " no stated amount at all."
        )
    # The page said where the record starts and never where it stops, on 122
    # URLs. Same rule as /flips and /radar: never imply coverage the query
    # does not have.
    through = _deeds_through_line(db)
    if through:
        lede_parts.append(
            through + " A deed signed after that date has not been published yet "
            "and cannot appear here."
        )
    lede = _para(*lede_parts)

    # Chain of title. People arrive here searching the phrase itself, and the
    # honest answer includes where our copy of the chain stops: ACRIS is the
    # authority, we hold it from 2025, and a title search goes back further
    # than we do. Saying so is cheaper than being caught not saying it.
    newest_deed = next((r for r in buys if r.document_id), None)
    chain_parts = [
        f"A chain of title is the ordered list of conveyances that moved a "
        f"property from one owner of record to the next. {esc(name)} is the "
        f"grantee on {_count(len(buys), 'link')} in that chain"
        + (f", recorded between {_en_date(span_start)} and {_en_date(span_end)}"
           if span_start and span_end and span_start != span_end else "")
        + (f", all on {_en_date(span_start)}" if span_start and span_start == span_end else "")
        + "."
    ]
    if newest_deed:
        chain_parts.append(
            f"Its most recent conveyance is ACRIS document "
            f"{esc(str(newest_deed.document_id))}"
            + (f" on {esc(_addr_title(newest_deed.address))}" if newest_deed.address else "")
            + (f", recorded {_en_date(newest_deed.doc_date)}"
               if newest_deed.doc_date and span_start != span_end else "")
            + ". Follow any address above for the whole chain on that building, "
              "including the deeds either side of this one and the party opposite."
        )
    acris_through = _acris_through(db)
    chain_parts.append(
        "PulseCities holds ACRIS deeds recorded from 2025 onward, so these are "
        "the recent end of a chain rather than the whole of it. For a full title "
        "search the city's own ACRIS system is the record of authority."
        + (f" Source: NYC ACRIS, current through {_en_date(acris_through)}."
           if acris_through else "")
    )
    chain_sec = _prose_section("The chain of title", _para(*chain_parts))

    # The buildings behind the deeds. A portfolio that carries none of these
    # records still gets the section: "no evictions, no open violations, no
    # registered stabilized units" is an answer, and on an entity page it is
    # often the answer a reader came for.
    resolved = [r for r in buys if r.address]
    holdings_sec = ""
    if portfolio and resolved:
        hp = []
        if not (portfolio.with_eviction or portfolio.with_violation
                or portfolio.with_rs):
            hp.append(
                f"None of the {_plural(len(resolved), 'lot')} on this page carries an "
                f"executed eviction, an unresolved HPD or DOB violation, or a DHCR "
                f"rent-stabilization registration in the records PulseCities holds."
            )
        if portfolio.residential:
            line = f"{_count_open(int(portfolio.residential), 'of the buildings', 'of the buildings')} "
            line += "is residential" if portfolio.residential == 1 else "are residential"
            if portfolio.units:
                line += f", carrying {_count(int(portfolio.units), 'residential unit')} between them"
            line += "."
            hp.append(line)
        if portfolio.oldest:
            if portfolio.newest and portfolio.newest != portfolio.oldest:
                hp.append(f"City records date them between {int(portfolio.oldest)} "
                          f"and {int(portfolio.newest)}.")
            else:
                hp.append(f"City records date them to {int(portfolio.oldest)}.")
        if portfolio.with_eviction:
            hp.append(
                f"{_count_open(int(portfolio.with_eviction), 'building')} in this "
                f"portfolio {'carries' if portfolio.with_eviction == 1 else 'carry'} "
                f"an executed marshal eviction in the citywide record."
            )
        if portfolio.with_violation:
            hp.append(
                f"{_count_open(int(portfolio.with_violation), 'building')} "
                f"{'has' if portfolio.with_violation == 1 else 'have'} unresolved "
                f"HPD or DOB violations on file."
            )
        if portfolio.with_rs:
            hp.append(
                f"{_count_open(int(portfolio.with_rs), 'building')} "
                f"{'appears' if portfolio.with_rs == 1 else 'appear'} in DHCR "
                f"rent-stabilization registrations, so stabilized tenants live in "
                f"what this entity bought."
            )
        else:
            hp.append(
                "None appear in the DHCR rent-stabilization registrations "
                "PulseCities holds."
            )
        holdings_sec = _prose_section("What the buildings carry", _para(*hp))
    else:
        # Condo unit lots (1001 and up) are not in PLUTO, so a quarter of the
        # deed record resolves to no building file at all. A reader looking at
        # a page of bare BBLs deserves to be told why, rather than left to
        # assume the site simply failed.
        unresolved = [r for r in buys if not r.address]
        if unresolved:
            holdings_sec = _prose_section(
                "What the buildings carry",
                _para(
                    f"{_count_open(len(unresolved), 'of the lots', 'of the lots')} on "
                    f"this page {'resolves' if len(unresolved) == 1 else 'resolve'} to "
                    f"no entry in the city's property file, which is why "
                    f"{'it appears' if len(unresolved) == 1 else 'they appear'} above "
                    f"as a tax lot number rather than an address.",
                    "That is the ordinary signature of a condominium: each unit gets "
                    "its own tax lot, and the citywide land-use file carries the "
                    "building rather than the units. Unit count, year built, "
                    "violations and evictions are all recorded against the building, "
                    "so none of them attach to these deeds.",
                ),
            )

    filing_sec = ""
    if filings and (filings.city or filings.entities):
        fl = []
        where = ", ".join(x for x in (filings.city, filings.st) if x)
        if filings.entities and filings.entities > 1:
            fl.append(
                f"The deeds list a mailing address for {esc(name)} at "
                f"{esc(_entity_title(filings.addr))}"
                + (f", {esc(_entity_title(where))}." if where else ".")
            )
            # This sentence used to say a shared filing address is "how the
            # same operation appears as many names". Checked against the data,
            # that is often false: 525 6th Avenue files for APPLEBAUM SPENCER,
            # CHEN DOROTHY and GOPSTEIN SHELDON, which is a lawyer's office and
            # not a landlord. State the fact, name both readings, assert
            # neither.
            fl.append(
                f"{_count(int(filings.entities), 'separate buying entity')} in the "
                f"deed record {'files' if filings.entities == 1 else 'file'} from that "
                f"same address. That can mean one operation holding buildings one "
                f"LLC at a time, or simply a managing agent, attorney or title "
                f"company filing on behalf of clients who have nothing to do with "
                f"each other. The deed does not say which."
            )
        elif where:
            fl.append(
                f"The deeds list a mailing address for {esc(name)} in "
                f"{esc(_entity_title(where))}, and no other buying entity in the record "
                f"files from it."
            )
        if fl:
            fl.append(
                "A mailing address on a deed is where the filing said to send "
                "paper. It is not proof of who controls the entity."
            )
            filing_sec = _prose_section("Where the filings come from", _para(*fl))

    # State registration. Kept as its own section because it answers a different
    # question from the deed: not what was bought, but what the company is.
    dos_sec = ""
    if dos:
        dp = []
        formed = (f"{_en_date(dos.initial_filing_date)}"
                  if dos.initial_filing_date else "an unrecorded date")
        kind = (dos.entity_type or "company").lower()
        where = (f"in {esc(dos.jurisdiction)}"
                 if dos.jurisdiction and dos.jurisdiction != "New York"
                 else "in New York")
        dp.append(
            f"New York registers {esc(name)} as a {esc(kind)}, DOS ID "
            f"{esc(str(dos.dos_id))}, formed {where} and first filed on {formed}."
            + (" A company formed in another state and registered to hold "
               "property here is a routine structure and is worth noticing "
               "anyway, because it is a choice."
               if dos.jurisdiction and dos.jurisdiction != "New York" else "")
        )
        kindof = _dos_agent_kind(name, dos.agent_name)
        if kindof == "third_party":
            agent_where = ", ".join(x for x in (
                _entity_title(dos.agent_address or ""),
                _entity_title(dos.agent_city or ""), dos.agent_state or "") if x)
            dp.append(
                f"Service of process is designated to "
                f"{esc(_entity_title(dos.agent_name))}"
                + (f" at {esc(agent_where)}" if agent_where else "")
                + ". That is the name the state sends legal papers to, which is "
                  "the closest the public record comes to naming who stands "
                  "behind the company. It is not the same as a member or an owner."
            )
            if dos.agent_shared and dos.agent_shared > 1:
                dp.append(
                    f"{_count(int(dos.agent_shared), 'buying entity')} in the deed "
                    f"record {'designates' if dos.agent_shared == 1 else 'designate'} "
                    f"that same name. Shared process agents cluster the way shared "
                    f"filing addresses do, and with the same caveat: a managing "
                    f"agent or an attorney serves clients who have nothing to do "
                    f"with each other."
                )
        elif kindof == "commercial":
            dp.append(
                f"Service of process is designated to "
                f"{esc(_entity_title(dos.agent_name))}, a commercial registered "
                f"agent. That is a service the company pays for, so it names no "
                f"one behind the entity and is itself only evidence that the "
                f"filing was done through a professional."
            )
        else:
            dp.append(
                "The company designates itself for service of process, which is "
                "the default and names nobody. Membership is not filed with the "
                "state, so the public record stops here."
            )
        dos_sec = _prose_section("What the state register says", _para(*dp))

    # Where it buys. The links are the point as much as the prose: an LLC page
    # pointed at no neighbourhood, so the deed record led nowhere.
    zips: dict[str, dict] = {}
    for r in buys:
        if not r.zip_code:
            continue
        z = zips.setdefault(r.zip_code, {"n": 0, "hood": r.hood or ""})
        z["n"] += 1
        if r.hood and not z["hood"]:
            z["hood"] = r.hood
    ranked_zips = sorted(zips.items(), key=lambda kv: (-kv[1]["n"], kv[0]))

    geo_sec = ""
    if ranked_zips:
        top = ranked_zips[0]
        geo_lines = [
            f"The purchases sit in {_count(len(ranked_zips), 'ZIP code')}. "
            f"The heaviest concentration is {esc(top[0])}"
            + (f" ({esc(top[1]['hood'])})" if top[1]["hood"] else "")
            + f", with {_count(top[1]['n'], 'recorded deed')}."
        ]
        if len(ranked_zips) > 1:
            rest = ", ".join(
                f'<a href="/neighborhood/{esc(z)}">{esc(v["hood"] + " " + z if v["hood"] else z)}</a>'
                f' ({v["n"]})'
                for z, v in ranked_zips[1:7]
            )
            geo_lines.append(f"The rest: {rest}.")

        # Which ZIPs an entity buys in is the question the whole site exists to
        # answer, and the entity page was the one place not answering it.
        scored = db.execute(text("""
            SELECT ds.zip_code, ds.score,
                   (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY score)
                    FROM displacement_scores WHERE score IS NOT NULL) AS median
            FROM displacement_scores ds
            WHERE ds.zip_code = ANY(:zips) AND ds.score IS NOT NULL
            ORDER BY ds.score DESC
        """), {"zips": [z for z, _ in ranked_zips]}).fetchall()
        if scored:
            median = float(scored[0].median or 0)
            hottest = scored[0]
            above = sum(1 for r in scored if float(r.score) > median)
            geo_lines.append(
                f"On the PulseCities displacement index, the highest-pressure ZIP it "
                f"buys in is {esc(hottest.zip_code)} at {float(hottest.score):.1f} out "
                f"of 100, against a citywide median of {median:.1f}."
            )
            if len(scored) > 1:
                geo_lines.append(
                    "None of its ZIP codes sits above that median."
                    if above == 0 else
                    f"{_count_open(above, 'of its ZIP code', 'of its ZIP codes')} "
                    f"{'sits' if above == 1 else 'sit'} above that median."
                )
        geo_lines.append(
            f'Each of those neighborhoods carries its own displacement signals. '
            f'<a href="/neighborhood/{esc(top[0])}">Signals for {esc(top[0])} &rarr;</a>'
        )
        geo_sec = _prose_section(f"Where {esc(name)} buys", _para(*geo_lines))

    # Timeline. A cluster of deeds in one month is a different operation from
    # the same count spread over a year, and only one of them is worth a look.
    time_sec = ""
    if dates:
        by_month: dict[str, list] = {}
        for d in dates:
            key = f"{d.year}-{d.month:02d}"
            slot = by_month.setdefault(key, [0, d])
            slot[0] += 1
        busiest = max(((k, v[0], v[1]) for k, v in by_month.items()),
                      key=lambda kv: (kv[1], kv[0]))
        time_lines = [
            f"Every deed PulseCities holds for {esc(name)} was recorded on the same "
            f"day, {_en_date(span_end)}."
            if span_start == span_end else
            f"The first deed PulseCities holds for {esc(name)} was recorded "
            f"{_en_date(span_start)}, the most recent {_en_date(span_end)}, "
            f"in {_count(len(by_month), 'different calendar month')}."
        ]
        if busiest[1] > 1:
            time_lines.append(
                f"The busiest month was {_month_year(busiest[2], 'en')}, with {_count(busiest[1], 'deed')} recorded."
            )
        if sells:
            sold_bbls = len({r.bbl for r in sells if r.bbl})
            time_lines.append(
                f"The entity also appears as seller on {_count(len(sells), 'deed')} "
                f"covering {_count(sold_bbls, 'lot')}."
            )
        else:
            time_lines.append(
                "No deed in the current record names this entity as the seller, so "
                "nothing here has been resold inside the window."
            )
        time_sec = _prose_section("The acquisition timeline", _para(*time_lines))

    # Eviction-before-purchase, with the buildings named.
    post_ev_line = ""
    if post_ev:
        noun = "property" if post_ev == 1 else "properties"
        ev_lines = [
            f"{_count_open(post_ev, noun, noun)} bought by {esc(name)} at a recorded price had a "
            f"residential eviction executed in the year before the purchase. That "
            f"sequence is what PulseCities tracks citywide: an eviction, then a "
            f"transfer, then in many cases a renovation permit."
        ]
        named = [r for r in ev_before if r.address]
        if named:
            listed = ", ".join(
                f'<a href="/property/{esc(str(r.bbl))}">{esc(_addr_title(r.address))}</a>'
                for r in named[:5]
            )
            ev_lines.append(f"Those buildings: {listed}.")
        ev_lines.append(
            'A recorded sequence is not a finding about conduct. '
            '<a href="/evictions">Citywide eviction tracker &rarr;</a>'
        )
        post_ev_line = _prose_section("Evictions before purchase", _para(*ev_lines))

    # FAQ, answered from this entity's rows. The demand arrives phrased this
    # way: Bing logged "bredif ms seller llc" and "how much did water view
    # castle llc purchase 1341 ocan parkway brooklyn ny for".
    faq: list[tuple[str, str]] = []
    faq.append((
        f"How many NYC properties does {name} own?",
        f"{name} appears as the buyer of record on {len(buys)} "
        f"{_plural(len(buys), 'deed')} covering {_count(n_bbls, 'tax lot')} in the "
        f"ACRIS record PulseCities holds. A deed names who took title, so this is "
        f"what the public record shows rather than a full ownership picture: "
        f"property held through other entities does not appear on this page.",
    ))
    if volume and not all_nominal:
        faq.append((
            f"How much has {name} paid for NYC property?",
            f"Stated consideration on the priced deeds totals {_fmt_amount(volume)} "
            f"across {_count(len(priced), 'deed')}"
            + (f", the largest being {_fmt_amount(priced[0].doc_amount)} for "
               f"{_addr_title(priced[0].address)}." if priced and priced[0].address else ".")
            + " Stated consideration is the figure filed with the deed. Deeds "
              "recorded at $0 are usually transfers between related parties.",
        ))
    if ranked_zips:
        top = ranked_zips[0]
        faq.append((
            f"Where does {name} buy in NYC?",
            f"Its recorded purchases fall in {_count(len(ranked_zips), 'ZIP code')}, "
            f"concentrated in {top[0]}"
            + (f", {top[1]['hood']}" if top[1]["hood"] else "")
            + f", where {_count(top[1]['n'], 'deed')} "
              f"{'is' if top[1]['n'] == 1 else 'are'} on record. Each neighborhood "
              f"page carries the displacement signals for that ZIP.",
        ))
    if all_nominal:
        faq.append((
            f"How much did {name} pay for its NYC property?",
            f"Nothing in the deed record states a real price. All "
            f"{len(priced)} priced deeds record a nominal consideration under "
            f"$1,000, which is how a transfer between related parties is filed. "
            f"The buildings changed hands on paper; the deeds do not say for what.",
        ))
    # Asked constantly and answered nowhere. The search console carries
    # "registered agent", "beneficial owner", "managing member" and
    # "controlling party" against these pages at position 5 to 9. The deed
    # record cannot answer any of them, and saying exactly why, plus what the
    # record does support, is a better answer than the silence that is there
    # now. Replace this once the DOS entity registry is ingested.
    faq.append((
        f"Who owns or controls {name}?",
        f"ACRIS names {name} as the grantee and stops there, because New York "
        f"does not require an LLC to name its managing member on a deed. "
        + (f"The state register adds what it can: DOS ID {dos.dos_id}"
           + (f", formed in {dos.jurisdiction}" if dos and dos.jurisdiction
              and dos.jurisdiction != "New York" else "")
           + (f", and a registered agent for service of process: "
              f"{_entity_title(dos.agent_name)}."
              if _dos_agent_kind(name, dos.agent_name) == "third_party"
              else ", and no registered agent beyond the company itself.")
           + " Membership itself is not filed with New York, so neither record "
             "names a beneficial owner."
           if dos else
           "Membership is not filed with New York either, so what this page can "
           "show is the mailing address on the deeds and which other buying "
           "entities file from it."),
    ))
    faq.append((
        f"Is {name} connected to other entities?",
        (f"Yes. PulseCities groups it into the {network.display_name} operator "
         f"network, which clusters entities that share a registered address or "
         f"principal in the public record."
         if network else
         f"Not in the current clustering. Numbered LLCs are the standard way NYC "
         f"property is held one building at a time, so an entity can be part of a "
         f"larger operation without the deed record saying so. PulseCities only "
         f"links entities where the public record supports it."),
    ))
    if post_ev:
        faq.append((
            f"Did any {name} building have an eviction before it was bought?",
            f"{_count_open(post_ev, 'of the properties', 'of the properties')} bought at a recorded price had a "
            f"residential eviction executed by a city marshal within the year "
            f"before the deed date. PulseCities reports the sequence in the record "
            f"and makes no claim about why either event happened.",
        ))
    faq_html = "".join(
        f'<div class="faq-item"><h3>{esc(q)}</h3><p>{esc(a)}</p></div>' for q, a in faq
    )
    faq_sec = f"<h2>Questions about {esc(name)}</h2>{faq_html}"

    # Follow this one entity. /llc is the second-biggest organic landing
    # surface on the site and carried no capture at all: the portfolio card
    # lives in entity_family_page, so it covers the published families and
    # none of the individual companies. Same JS-string escaping rule as the
    # portfolio card, plus an HTML-escaped copy for the heading.
    ent_js_label = json.dumps(name)[1:-1].replace("</", "<\\/")
    entity_follow_sec = (_ENTITY_FOLLOW_CARD
                         .replace("__SLUG__", slug)
                         .replace("__LABEL_HTML__", esc(name))
                         .replace("__LABEL__", ent_js_label))

    # Thin-page guard: a whole-condo purchase is one building however
    # many unit deeds it records, and servicers are not buyers.
    n_buildings = _building_count(r.bbl for r in buys)
    is_indexable = (_is_buyer_entity(name) and "LLC" in name
                    and n_bbls >= _LLC_MIN_LOTS
                    and n_buildings >= _LLC_MIN_BUILDINGS)
    robots = "index, follow" if is_indexable else "noindex, follow"
    # The search console says this page ranks 5 to 9 for roughly 1,700 monthly
    # impressions of deed-research queries and converts none of them. Those
    # queries carry a vocabulary the page did not use anywhere: grantor,
    # grantee, chain of title, conveyance, ACRIS. Ranking for words the snippet
    # never says is how you get a page-one result nobody clicks.
    title = f"{name}: ACRIS deeds, grantor and grantee | PulseCities"
    desc = (f"{name} is named on {_count(n_bbls, 'NYC deed')} in ACRIS. "
            f"Grantor and grantee, recording date, stated consideration and the "
            f"ACRIS document ID for every conveyance, with the chain of title on "
            f"each building.")
    url = f"https://pulsecities.com/llc/{slug}"
    jsonld = _jsonld({"@context": "https://schema.org", "@graph": [
        {"@type": "Organization", "name": name, "url": url},
        {"@type": "FAQPage", "mainEntity": [
            {"@type": "Question", "name": q,
             "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faq]},
        _crumbs(("Home", "/"), ("LLC buyers", "/llc"), (name, f"/llc/{slug}")),
    ]})

    stat_cells = (
        f'<div class="stat"><div class="stat-num">{n_bbls}</div><div class="stat-label">'
        f'{"lot" if n_bbls == 1 else "lots"} bought</div></div>'
    )
    if n_blocks and n_blocks < n_bbls:
        stat_cells += (f'<div class="stat"><div class="stat-num">{n_blocks}</div>'
                       f'<div class="stat-label">building' + ("s" if n_blocks != 1 else "")
                       + '</div></div>')
    if sells:
        stat_cells += (f'<div class="stat"><div class="stat-num">{len({r.bbl for r in sells if r.bbl})}</div>'
                       f'<div class="stat-label">sold</div></div>')
    if volume and not all_nominal:
        stat_cells += (f'<div class="stat"><div class="stat-num">{_fmt_amount(volume)}</div>'
                       f'<div class="stat-label">volume, priced deeds</div></div>')
    elif all_nominal:
        stat_cells += ('<div class="stat"><div class="stat-num">$0</div>'
                       '<div class="stat-label">arm\'s length volume</div></div>')

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
{_llc_head(title, desc, url, robots, jsonld)}
</head>
<body>
{_ssr_nav("", toggle_html="")}
<div class="container">
  <p style="margin-bottom:8px;font-size:0.75rem;color:var(--faint);"><a href="/llc">&#8592; All LLC buyers</a></p>
  <div class="eyebrow">NYC deed record</div>
  <h1>{esc(name)}</h1>
  <p class="sub">Every entry below is a recorded deed naming this entity, from ACRIS public records. The deed record here begins in 2025.</p>
  <div class="stats">{stat_cells}</div>
  {f'<p class="mono-note">Latest recorded deed {last_seen}</p>' if last_seen else ''}
  {network_line}
  {lede}

  <h2>Deeds bought</h2>
  <p class="section-sub">Conveyances where this entity is the grantee, newest first</p>
  <ul class="rec-list">
{buys_html}  </ul>
{sells_section}
{chain_sec}
{holdings_sec}
{filing_sec}
{dos_sec}
{geo_sec}
{time_sec}
{post_ev_line}
{entity_follow_sec}
{faq_sec}

  <p class="cross" style="margin-top:26px;">Looking up your own building? <a href="/who-owns-my-building">Who owns my building &rarr;</a></p>
  <p class="note">A deed names a buyer or seller of record. This page describes documents, not conduct, and makes no claim of wrongdoing. <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    if len(_llc_page_cache) >= 512:
        now = time.monotonic()
        expired = [k for k, v in _llc_page_cache.items() if now >= v[1]]
        for k in expired:
            _llc_page_cache.pop(k, None)
        if len(_llc_page_cache) >= 512:
            _llc_page_cache.clear()
    _llc_page_cache[slug] = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)


# --- Entity families: the operation behind the numbered LLCs ------------------
#
# The one genuinely new page type the SEO plan asks for. 122 LLC pages were
# isolated: an entity page linked to its own buildings and to nothing else, so
# a portfolio held one building at a time read as a portfolio of strangers.
#
# The clustering lives in api/entity_families.py, and the reason it is
# conservative is documented there: a shared filing address alone groups
# attorneys with their clients. 15 families survive corroboration, covering 166
# entities. The largest, FLGSP, is 80 entities across 80 buildings filed
# through three management addresses, which is invisible in the record as 80
# separate companies.

_FAMILY_TTL = 21600
_family_page_cache: dict[str, tuple[str, float]] = {}

# The follow card on every hub. Plain string, not an f-string: the JS needs
# its braces and the page assembly around it is already an f-string minefield.
_FOLLOW_CARD = """
<div id="follow" style="margin:26px 0;padding:20px 22px;background:#16202d;border:1px solid rgba(147,161,173,0.2);border-radius:10px;">
  <h2 style="margin-top:0;">Follow this portfolio</h2>
  <p class="prose">When a company in this network records a new NYC purchase, it
  shows up in a weekly email. Quiet weeks send nothing.</p>
  <form id="fol-form" style="display:flex;gap:10px;margin:14px 0 0;max-width:560px;">
    <input type="email" id="fol-email" placeholder="you@example.com" aria-label="Email address"
           style="flex:1;font-family:'JetBrains Mono',monospace;font-size:0.85rem;color:#e4e8ec;background:#111823;border:1px solid rgba(147,161,173,0.2);border-radius:8px;padding:12px 14px;min-width:0;">
    <button type="submit" id="fol-btn" style="font-family:'DM Sans',sans-serif;font-size:0.9rem;font-weight:600;color:#111823;background:#ed6317;border:none;border-radius:8px;padding:12px 22px;cursor:pointer;">Follow</button>
  </form>
  <p id="fol-status" style="display:none;margin:10px 0 0;font-size:0.85rem;"></p>
</div>
<script>
(function () {
  var form = document.getElementById('fol-form');
  var email = document.getElementById('fol-email');
  var btn = document.getElementById('fol-btn');
  var status = document.getElementById('fol-status');
  function show(msg, ok) {
    status.textContent = msg;
    status.style.color = ok ? '#6fa287' : '#ec6a5e';
    status.style.display = 'block';
  }
  form.addEventListener('submit', function (ev) {
    ev.preventDefault();
    var v = (email.value || '').trim();
    if (!v || v.indexOf('@') < 1) { show('Enter a valid email.', false); return; }
    btn.disabled = true;
    fetch('/api/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: v, family_slug: '__SLUG__' })
    }).then(function (r) {
      if (r.status === 201) {
        window.plausible && plausible('Family Follow');
        form.style.display = 'none';
        show("You're following __LABEL__. A confirmation is on its way.", true);
      } else if (r.status === 409) {
        show('Already following this portfolio.', true);
        btn.disabled = false;
      } else if (r.status === 422) {
        show('Enter a valid email.', false);
        btn.disabled = false;
      } else {
        show('Could not subscribe. Try again.', false);
        btn.disabled = false;
      }
    }).catch(function () {
      show('Could not subscribe. Try again.', false);
      btn.disabled = false;
    });
  });
})();
</script>
"""


_ENTITY_FOLLOW_CARD = """
<div id="follow" style="margin:26px 0;padding:20px 22px;background:#16202d;border:1px solid rgba(147,161,173,0.2);border-radius:10px;">
  <h2 style="margin-top:0;">Follow __LABEL_HTML__</h2>
  <p class="prose">When this company records another NYC deed, buying or
  selling, it shows up in a weekly email. Quiet weeks send nothing.</p>
  <form id="ent-form" style="display:flex;gap:10px;margin:14px 0 0;max-width:560px;">
    <input type="email" id="ent-email" placeholder="you@example.com" aria-label="Email address"
           style="flex:1;font-family:'JetBrains Mono',monospace;font-size:0.85rem;color:#e4e8ec;background:#111823;border:1px solid rgba(147,161,173,0.2);border-radius:8px;padding:12px 14px;min-width:0;">
    <button type="submit" id="ent-btn" style="font-family:'DM Sans',sans-serif;font-size:0.9rem;font-weight:600;color:#111823;background:#ed6317;border:none;border-radius:8px;padding:12px 22px;cursor:pointer;">Follow</button>
  </form>
  <p id="ent-status" style="display:none;margin:10px 0 0;font-size:0.85rem;"></p>
</div>
<script>
(function () {
  var form = document.getElementById('ent-form');
  var email = document.getElementById('ent-email');
  var btn = document.getElementById('ent-btn');
  var status = document.getElementById('ent-status');
  function show(msg, ok) {
    status.textContent = msg;
    status.style.color = ok ? '#6fa287' : '#ec6a5e';
    status.style.display = 'block';
  }
  form.addEventListener('submit', function (ev) {
    ev.preventDefault();
    var v = (email.value || '').trim();
    if (!v || v.indexOf('@') < 1) { show('Enter a valid email.', false); return; }
    btn.disabled = true;
    fetch('/api/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: v, entity_slug: '__SLUG__' })
    }).then(function (r) {
      if (r.status === 201) {
        window.plausible && plausible('Entity Follow');
        form.style.display = 'none';
        show("You're following __LABEL__. A confirmation is on its way.", true);
      } else if (r.status === 409) {
        show('Already following this company.', true);
        btn.disabled = false;
      } else if (r.status === 422) {
        show('Enter a valid email.', false);
        btn.disabled = false;
      } else {
        show('Could not subscribe. Try again.', false);
        btn.disabled = false;
      }
    }).catch(function () {
      show('Could not subscribe. Try again.', false);
      btn.disabled = false;
    });
  });
})();
</script>
"""


def _families(db) -> dict:
    """All families, memoised. The computation reads every buyer entity, so it
    runs once per TTL rather than once per request. The memo lives in
    api.entity_families because the subscribe endpoint and the weekly digest
    resolve the same slugs and should not each build their own."""
    from api.entity_families import families_cached
    return families_cached(db, _is_buyer_entity, _FAMILY_TTL)


_network_dir_cache: tuple[str, float] | None = None


def _family_shapes(db, fams: dict) -> dict[str, dict]:
    """How each family moved: the largest number of buildings it took on a
    single day, and on which day.

    One query over every member name rather than one per family. A portfolio
    that arrives on one date is a different animal from one assembled over a
    year, and it is the first thing a reader wants to know about a list of 26.
    """
    names = sorted({n for f in fams.values() for n in f["entities"]})
    if not names:
        return {}
    # Distinct building keys, aggregated per family in Python. Summing
    # per-name counts double-counted co-buys: eight REDROCK siblings on the
    # same nine lots read as "39 buildings on one day", and that number was
    # the ranking key.
    rows = db.execute(text(f"""
        SELECT DISTINCT party_name_normalized AS name, doc_date,
               ({_BUILDING_KEY_SQL}) AS bkey
        FROM ownership_raw
        WHERE doc_type = 'DEED' AND party_type = '2'
          AND party_name_normalized = ANY(:names) AND doc_date IS NOT NULL
    """), {"names": names}).fetchall()

    by_name: dict[str, list] = collections.defaultdict(list)
    for r in rows:
        by_name[r.name].append(r)

    out: dict[str, dict] = {}
    for slug, f in fams.items():
        per_date: dict = collections.defaultdict(set)
        for n in f["entities"]:
            for r in by_name.get(n, ()):
                per_date[r.doc_date].add(r.bkey)
        top_date, top_n = max(
            ((d, len(k)) for d, k in per_date.items()),
            key=lambda t: t[1], default=(None, 0))
        held, sold = f["buildings"], f.get("sold", 0)
        if top_n >= 5:
            shape, why = "bulk trade", f"{top_n} buildings on {_en_date(top_date)}"
        elif sold > held:
            shape, why = "unwinding", f"{sold} sold, {held} still held"
        elif held >= 5:
            shape, why = "assembling", f"{held} buildings, one deed at a time"
        else:
            shape, why = "holding", f"{held} buildings"
        out[slug] = {"shape": shape, "why": why, "bulk": top_n, "bulk_date": top_date}
    return out


@router.get("/network", include_in_schema=False)
def network_directory(db: Session = Depends(get_db)):
    """Index of the entity families.

    The 26 hub pages existed for a week with no parent: nothing linked them but
    the sitemap and one line on /llc, so the most distinctive thing here was
    reachable only by knowing the URL. This is the page a reporter should be
    sent to, and it is where the explanation of how the clustering works now
    lives in full, instead of being repeated in longhand on all 26.
    """
    global _network_dir_cache
    if _network_dir_cache and time.monotonic() < _network_dir_cache[1]:
        return HTMLResponse(_network_dir_cache[0])

    e = _html.escape
    fams = _families(db)
    shapes = _family_shapes(db, fams)

    ranked = sorted(fams.values(),
                    key=lambda f: (-(shapes.get(f["slug"], {}).get("bulk") or 0),
                                   -max(f["buildings"], f.get("sold", 0))))
    n_fam = len(ranked)
    n_ent = sum(len(f["entities"]) for f in ranked)
    n_bld = sum(max(f["buildings"], f.get("sold", 0)) for f in ranked)
    volume = sum(f.get("volume") or 0 for f in ranked)
    bulk = [f for f in ranked if shapes.get(f["slug"], {}).get("shape") == "bulk trade"]

    rows = ""
    for f in ranked:
        sh = shapes.get(f["slug"], {})
        held, sold = f["buildings"], f.get("sold", 0)
        counts = []
        if held:
            counts.append(f"{held} held")
        if sold:
            counts.append(f"{sold} sold")
        money = f" &middot; {_fmt_amount(f['volume'])}" if f.get("volume") else ""
        rows += (
            f'<li class="rec-row"><a href="/network/{e(f["slug"])}">'
            f'<div><div class="rec-addr">{e(f["label"])}</div>'
            f'<div class="rec-geo">{e(sh.get("shape", ""))} &middot; '
            f'{_count(len(f["entities"]), "company")} &middot; '
            f'{e(" and ".join(counts)) if counts else "no buildings on record"}'
            f'{money}</div></div>'
            f'<div class="rec-side"><div class="rec-amt">{e(sh.get("why", ""))}</div>'
            f'<div class="rec-date">{_en_date(f["last_deed"]) if f.get("last_deed") else ""}</div>'
            f'</div></a></li>')

    title = (f"NYC LLC networks: {n_fam} owner portfolios reassembled from the "
             f"deed record | PulseCities")
    desc = (f"{n_fam} groups of NYC limited liability companies the deed record links "
            f"to each other: {n_ent} companies, {n_bld:,} buildings, from ACRIS.")

    faq = [
        ("What is an entity family?",
         "A group of limited liability companies that the public deed record links "
         "to each other, usually because they share a naming pattern and file from "
         "the same address. NYC property is commonly held one building per company, "
         "so a single operation appears in the record as dozens of unrelated "
         "strangers. These pages put them back together."),
        ("How are the companies linked?",
         "Two independent things in the record have to agree: a shared naming stem "
         "across numbered siblings, and a shared filing address corroborated by a "
         "distinctive token that at least half the companies at that address carry. "
         "One signal alone is not enough. Dozens of unrelated companies file from a "
         "single attorney's or title company's address, and grouping on that would "
         "invent a landlord who does not exist. Roughly seven in ten shared-address "
         "groups in the deed record fail this test and appear nowhere on the site."),
        ("Does this prove common ownership?",
         "No, and nothing here claims it. The deeds say these companies share a name "
         "and a mailing address. They do not say who controls them. Confirming that "
         "means a New York Department of State entity search, or asking the parties."),
        ("Why do some families hold nothing?",
         "Because they have sold everything. A portfolio being unwound is as much a "
         "family as one being assembled, and often the more interesting one: it is "
         "the shape that shows a bulk exit."),
    ]
    faq_html = "".join(
        f'<div class="faq-item"><h3>{e(q)}</h3><p>{e(a)}</p></div>' for q, a in faq)
    jsonld = _jsonld({"@context": "https://schema.org", "@graph": [
        {"@type": "CollectionPage", "name": "NYC LLC owner networks",
         "url": "https://pulsecities.com/network", "description": desc},
        {"@type": "ItemList", "numberOfItems": n_fam, "itemListElement": [
            {"@type": "ListItem", "position": i + 1, "name": f["label"],
             "url": f"https://pulsecities.com/network/{f['slug']}"}
            for i, f in enumerate(ranked[:50])]},
        {"@type": "FAQPage", "mainEntity": [
            {"@type": "Question", "name": q,
             "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faq]},
        _crumbs(("Home", "/"), ("LLC buyers", "/llc"), ("Owner networks", "/network")),
    ]})

    stats = (
        f'<div class="stat"><div class="stat-num">{n_fam}</div>'
        f'<div class="stat-label">networks</div></div>'
        f'<div class="stat"><div class="stat-num">{n_ent}</div>'
        f'<div class="stat-label">companies</div></div>'
        f'<div class="stat"><div class="stat-num">{n_bld:,}</div>'
        f'<div class="stat-label">buildings</div></div>'
        + (f'<div class="stat"><div class="stat-num">{_fmt_amount(volume)}</div>'
           f'<div class="stat-label">stated consideration</div></div>' if volume else ""))

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
{_llc_head(title, desc, "https://pulsecities.com/network", "index, follow", jsonld)}
</head>
<body>
{_ssr_nav("", toggle_html="")}
<div class="container" style="max-width:760px;">
  <p style="margin-bottom:8px;font-size:0.75rem;color:var(--faint);">
    <a href="/">Home</a> &middot; <a href="/llc">LLC buyers</a>
  </p>
  <div class="eyebrow">NYC deed record</div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.6rem;letter-spacing:0;font-weight:600;">NYC owner networks</h1>
  <div class="stats">{stats}</div>
  <p class="prose">New York property is held one building at a time, each in its own
  limited liability company. That is ordinary practice, and the side effect is that a
  single operation appears in the public record as dozens of unrelated strangers.
  These are the {n_fam} groups the deed record links back together.</p>
  <p class="prose">{_count_open(len(bulk), 'network', 'networks')} moved as a block:
  five or more buildings changing hands on one date, which is one transaction wearing
  many names.
  The largest is {e(bulk[0]["label"]) if bulk else "none on record"}{
    f', {e(shapes[bulk[0]["slug"]]["why"])}' if bulk else ""}.</p>

  <h2>Every network</h2>
  <p class="prose">Ranked by the largest number of buildings taken on a single day,
  then by portfolio size. Each links to its companies, its buildings, and what those
  buildings carry.</p>
  <ul class="sib-list">{rows}</ul>

  <h2>How the record links these companies</h2>
  <p class="prose">Grouping needs two independent things in the record to agree: a
  shared naming pattern across numbered siblings, and a shared mailing address on the
  deed filings, corroborated by a distinctive token that at least half the companies
  at that address carry. A third pass adopts a company whose coined name belongs to a
  group and which files from a ZIP that group already uses, which is how a portfolio
  loses no member to a typo in a management address.</p>
  <p class="prose">One signal alone is not enough. Dozens of unrelated companies file
  from a single attorney's or title company's address, and grouping on that would
  invent a landlord who does not exist. Roughly seven in ten shared-address groups in
  the deed record fail this test and appear nowhere on this site. A transfer between
  two companies in the same family is not counted as a sale, and condominium unit
  deeds collapse to the building they sit in, so a whole-condo purchase does not read
  as a portfolio.</p>
  <p class="prose">What that leaves is a documented link, not a finding about
  ownership. The deeds say these companies share a name and a mailing address. They
  do not say who controls them.</p>

  <h2>Common questions</h2>
  {faq_html}

  <p class="note">A deed names a buyer of record. These pages describe documents, not
  conduct, and make no claim of wrongdoing.
  <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    _network_dir_cache = (page, time.monotonic() + _FAMILY_TTL)
    return HTMLResponse(page)


@router.get("/network/{slug}", include_in_schema=False)
def entity_family_page(slug: str, db: Session = Depends(get_db)):
    slug = (slug or "").lower()
    if not re.match(r"^[a-z0-9][a-z0-9-]{1,70}$", slug):
        return _not_found()

    hit = _family_page_cache.get(slug)
    if hit and time.monotonic() < hit[1]:
        return HTMLResponse(hit[0])

    fam = _families(db).get(slug)
    if not fam:
        return _not_found()

    # A curated operator profile already covers some of these. Two pages about
    # one operation would compete with each other, so the derived hub defers.
    curated = db.execute(text("""
        SELECT slug, display_name FROM operators
        WHERE operator_class = 'operator'
          AND EXISTS (
              SELECT 1 FROM jsonb_array_elements_text(llc_entities) AS ent(name)
              WHERE ent.name = ANY(:names)
          )
        LIMIT 1
    """), {"names": fam["entities"]}).first()
    if curated:
        return RedirectResponse(f"/operator/{curated.slug}", status_code=301)

    e = _html.escape
    label = fam["label"]
    names = fam["entities"]

    # Units, rent-stabilized registration, price and open violations come down
    # with the address. A family page that lists only addresses reads like every
    # other family page; what separates them is what each building carries.
    holdings = db.execute(text("""
        SELECT o.bbl, max(p.address) AS address, max(p.zip_code) AS zip_code,
               max(n.name) AS hood, max(o.doc_date) AS last_deed,
               max(o.doc_amount) AS amount,
               max(p.units_res) AS units_res,
               max(p.year_built) AS year_built,
               (SELECT rs.rs_unit_count FROM rs_buildings rs
                 WHERE rs.bbl = o.bbl AND rs.source = 'dhcr' AND rs.rs_unit_count > 0
                 ORDER BY rs.year DESC LIMIT 1) AS rs_units,
               (SELECT count(*) FROM violations_raw v
                 WHERE v.bbl = o.bbl AND v.current_status NOT IN :resolved) AS open_viol,
               bool_or(o.party_type = '2') AS bought,
               bool_or(o.party_type = '1') AS sold,
               bool_or(p.bbl IS NOT NULL) AS has_parcel
        FROM ownership_raw o
        LEFT JOIN parcels p ON p.bbl = o.bbl
        LEFT JOIN neighborhoods n ON n.zip_code = p.zip_code
        WHERE o.doc_type = 'DEED' AND o.party_type IN ('1', '2')
          AND o.party_name_normalized = ANY(:names)
        GROUP BY o.bbl
        ORDER BY max(o.doc_date) DESC NULLS LAST
    """), {"names": names, "resolved": _VIOLATION_RESOLVED}).fetchall()

    # Deeds with a family member on both sides. REDROCK's nine same-day deeds
    # all moved buildings from one REDROCK company to another for no stated
    # consideration, and a page that reads that as a nine-building purchase is
    # wrong about the only thing it is there to say.
    internal = db.execute(text("""
        SELECT count(*) FROM (
            SELECT document_id
            FROM ownership_raw
            WHERE doc_type = 'DEED' AND party_type IN ('1', '2')
              AND document_id IN (
                  SELECT document_id FROM ownership_raw
                  WHERE doc_type = 'DEED' AND party_name_normalized = ANY(:names)
              )
            GROUP BY document_id
            HAVING bool_or(party_type = '2' AND party_name_normalized = ANY(:names))
               AND bool_or(party_type = '1' AND party_name_normalized = ANY(:names))
        ) d
    """), {"names": names}).scalar() or 0

    # The other side of the same deeds. Naming the counterparty is a plain fact
    # of the record and it is what tells a reader whether this was one trade or
    # thirty separate ones.
    counterparties = db.execute(text("""
        SELECT o.party_name_normalized AS name, count(DISTINCT o.bbl) AS n,
               count(*) OVER () AS total
        FROM ownership_raw o
        WHERE o.doc_type = 'DEED'
          AND o.party_name_normalized IS NOT NULL
          AND o.party_name_normalized <> ALL(:names)
          AND o.document_id IN (
              SELECT document_id FROM ownership_raw
              WHERE doc_type = 'DEED' AND party_name_normalized = ANY(:names)
          )
        GROUP BY 1 ORDER BY n DESC, 1 LIMIT 3
    """), {"names": names}).fetchall()
    n_counterparties = int(counterparties[0].total) if counterparties else 0

    record = db.execute(text("""
        WITH held AS (
            SELECT DISTINCT bbl FROM ownership_raw
            WHERE doc_type = 'DEED' AND party_type = '2'
              AND party_name_normalized = ANY(:names)
        )
        SELECT
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM evictions_raw ev WHERE ev.bbl = h.bbl)) AS with_eviction,
            (SELECT count(*) FROM evictions_raw ev2
             WHERE ev2.bbl IN (SELECT bbl FROM held)) AS evictions,
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM violations_raw v WHERE v.bbl = h.bbl
                  AND v.current_status NOT IN :resolved)) AS with_violation,
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM rs_buildings rs WHERE rs.bbl = h.bbl
                  AND rs.source = 'dhcr' AND rs.rs_unit_count > 0)) AS with_rs
        FROM held h
    """), {"names": names, "resolved": _VIOLATION_RESOLVED}).first()

    def _para(*parts) -> str:
        body = " ".join(x for x in parts if x)
        return f'<p class="prose">{body}</p>' if body else ""

    zips: dict[str, dict] = {}
    for h in holdings:
        if h.zip_code:
            z = zips.setdefault(h.zip_code, {"n": 0, "hood": h.hood or ""})
            z["n"] += 1
            if h.hood and not z["hood"]:
                z["hood"] = h.hood
    ranked = sorted(zips.items(), key=lambda kv: (-kv[1]["n"], kv[0]))
    dates = [h.last_deed for h in holdings if h.last_deed]

    held, sold_n = fam["buildings"], fam.get("sold", 0)
    if held and sold_n:
        holding_line = (f", holding {_count(held, 'building')} between them and "
                        f"having sold {sold_n:,} more")
    elif held:
        holding_line = f", holding {_count(held, 'building')} between them"
    else:
        # A family that has sold everything still held those buildings, and the
        # exit is usually the more interesting half of the record.
        holding_line = (f". Between them they have sold "
                        f"{_count(sold_n, 'building')} and hold none in the "
                        f"current record")
    lede = _para(
        f"{e(label)} appears in the NYC deed record as "
        f"{_count(len(names), 'separate limited liability company')}"
        f"{holding_line}.",
        # One filing date across a portfolio this size is the story, not a
        # formatting artefact, so it gets its own sentence.
        (f"Every one of those deeds was recorded on the same day, "
         f"{_en_date(dates[0])}." if dates and min(dates) == max(dates)
         else f"The deeds run from {_en_date(min(dates))} to {_en_date(max(dates))}."
         if len(dates) > 1 else ""),
        (f"All of them sit in {e(ranked[0][0])}"
         + (f", {e(ranked[0][1]['hood'])}." if ranked[0][1]["hood"] else ".")
         if len(ranked) == 1 else
         f"They sit in {_count(len(ranked), 'ZIP code')}." if ranked else ""),
        (f"Stated consideration across the priced deeds totals "
         f"{_fmt_amount(fam['volume'])}." if fam.get("volume") else ""),
        (f"That is close to one building per company, which is what holding "
         f"property one LLC at a time looks like from the outside."
         if held and len(names) * 0.7 <= held <= len(names) * 1.3 else
         f"That is fewer buildings than companies: more shells than the "
         f"current holdings need, which is what a portfolio being sold down "
         f"looks like." if held and held < len(names) * 0.7 else
         f"That averages {held / len(names):.1f} buildings per company, so the "
         f"shells here are not strictly one per building." if held else ""),
    )

    ent_rows = "".join(
        f'<li class="rec-row"><a href="/llc/{e(re.sub(r"[^a-z0-9]+", "-", n.lower()).strip("-"))}">'
        f'<div><div class="rec-addr">{e(_entity_title(n))}</div></div></a></li>'
        for n in names
    )
    side_word = ("buyer" if held and not sold_n else
                 "seller" if sold_n and not held else "buyer or seller")
    ent_sec = (f"<h2>The {e(label)} entities</h2>"
               + _para(f"Every company below appears as a {side_word} of record "
                       f"under the {e(label)} name. Each has its own deed ledger.")
               + f'<ul class="sib-list">{ent_rows}</ul>')

    def _bld_meta(h) -> str:
        bits = []
        if h.units_res:
            bits.append(f"{int(h.units_res):,} unit" + ("s" if h.units_res != 1 else ""))
        if h.rs_units:
            bits.append(f"{int(h.rs_units):,} rent stabilized")
        if h.year_built:
            bits.append(f"built {int(h.year_built)}")
        if h.open_viol:
            bits.append(f"{int(h.open_viol):,} open violation"
                        + ("s" if h.open_viol != 1 else ""))
        return ", ".join(bits)

    def _bld_row(h) -> str:
        inner = (
            f'<div><div class="rec-addr">'
            f'{e(_addr_title(h.address)) if h.address else "BBL " + e(str(h.bbl))}</div>'
            f'<div class="rec-geo">{e((h.hood + ", ") if h.hood else "")}{e(h.zip_code or "")}'
            + (f' &middot; {e(_bld_meta(h))}' if _bld_meta(h) else "")
            + f'</div></div>'
            f'<div class="rec-side">'
            + (f'<div class="rec-amt">{_fmt_amount(float(h.amount))}</div>'
               if h.amount and float(h.amount) > 0 else "")
            + f'<div class="rec-date">{_en_date(h.last_deed)}</div></div>'
        )
        # A unit lot with no building file has no /property page to link.
        if h.has_parcel:
            return (f'<li class="rec-row"><a href="/property/{e(str(h.bbl))}">'
                    f'{inner}</a></li>')
        return f'<li class="rec-row"><div class="rec-static">{inner}</div></li>'

    bld_rows = "".join(_bld_row(h) for h in holdings[:40])
    priced = sorted(float(h.amount) for h in holdings if h.amount and float(h.amount) > 0)
    price_line = ""
    if len(priced) > 1:
        price_line = (f" Prices on the priced deeds run from "
                      f"{_fmt_amount(priced[0])} to {_fmt_amount(priced[-1])}.")
    elif priced:
        price_line = f" The one priced deed states {_fmt_amount(priced[0])}."
    bld_sec = (f"<h2>{'What ' + e(label) + ' holds' if held else 'What ' + e(label) + ' sold'}</h2>"
               + _para(f"The buildings behind those companies, newest deed first."
                       + (f" Showing 40 of {len(holdings):,}." if len(holdings) > 40 else "")
                       + price_line)
               + f'<ul class="sib-list">{bld_rows}</ul>') if holdings else ""

    geo_sec = ""
    if ranked:
        top = ranked[0]
        links = ", ".join(
            f'<a href="/neighborhood/{e(z)}">{e(v["hood"] or z)} {e(z)}</a> ({v["n"]})'
            for z, v in ranked[:8]
        )
        spread = top[1]["n"] == 1
        geo_sec = (f"<h2>Where {e(label)} {'buys' if held else 'sold'}</h2>"
                   ) + _para(
            (f"No ZIP holds more than one of them, so the portfolio is spread "
             f"across {_count(len(ranked), 'ZIP code')} rather than "
             f"concentrated in a neighborhood." if spread else
             f"The heaviest concentration is {e(top[0])}"
             + (f" ({e(top[1]['hood'])})" if top[1]["hood"] else "")
             + f", with {_count(top[1]['n'], 'building')}."),
            (f"The {_SPELLED[min(8, len(ranked))]} heaviest of {len(ranked)} "
             f"ZIP codes: {links}." if len(ranked) > 8
             else f"Across the portfolio: {links}.")
        )

    rec_sec = ""
    if record:
        rp = []
        if record.with_eviction:
            rp.append(
                f"{_count_open(int(record.with_eviction), 'building')} in this "
                f"portfolio {'carries' if record.with_eviction == 1 else 'carry'} "
                f"an executed marshal eviction, {int(record.evictions):,} in total."
            )
        else:
            rp.append("No building in this portfolio carries an executed "
                      "marshal eviction in the citywide record.")
        if record.with_violation:
            rp.append(
                f"{_count_open(int(record.with_violation), 'building')} "
                f"{'has' if record.with_violation == 1 else 'have'} unresolved "
                f"HPD or DOB violations on file."
            )
        if record.with_rs:
            rp.append(
                f"{_count_open(int(record.with_rs), 'building')} "
                f"{'appears' if record.with_rs == 1 else 'appear'} in DHCR "
                f"rent-stabilization registrations."
            )
        units_n = sum(int(h.units_res or 0) for h in holdings)
        rs_n = sum(int(h.rs_units or 0) for h in holdings)
        viol_n = sum(int(h.open_viol or 0) for h in holdings)
        totals = []
        if units_n:
            totals.append(f"{units_n:,} residential units are recorded across "
                          f"the portfolio in the city's tax lot file")
        if rs_n:
            totals.append(f"{rs_n:,} of them carry a DHCR rent-stabilization "
                          f"registration in the most recent year on file")
        if viol_n:
            totals.append(f"{viol_n:,} HPD and DOB violations are unresolved")
        if totals:
            rp.append((totals[0] if len(totals) == 1
                       else ", ".join(totals[:-1]) + ", and " + totals[-1]) + ".")
        rec_sec = f"<h2>What the buildings carry</h2>" + _para(*rp)

    party_sec = ""
    if internal:
        party_sec = f"<h2>Who was on the other side</h2>" + _para(
            f"{_count_open(int(internal), 'of these deeds moves a building', 'of these deeds move a building')} "
            f"from one {e(label)} company to another rather than to an outside "
            f"buyer. "
            + ("That is a restructuring, not a sale, and it usually states no "
               "price." if int(internal) == 1 else
               "Those are restructurings, not sales, and most of them state "
               "no price."),
            (f"On the rest, the record names "
             + ", ".join(f"{e(_entity_title(c.name))}" for c in counterparties)
             + "." if counterparties else ""))
    elif counterparties:
        repeats = max(int(c.n) for c in counterparties) > 1
        named = ", ".join(
            f"{e(_entity_title(c.name))}"
            + (f" ({c.n} buildings)" if c.n > 1 else "")
            for c in counterparties)
        others = n_counterparties - len(counterparties)
        party_sec = f"<h2>Who was on the other side</h2>" + _para(
            f"The same deeds name {named}"
            + (f", among {n_counterparties} counterparties in all,"
               if others > 0 else
               (" as the counterparties" if len(counterparties) > 1
                else " as the counterparty"))
            + (" with the building count each appears on." if repeats else "."),
            ("One counterparty repeating across a portfolio is the sign of a "
             "single transaction split across many documents rather than a run "
             "of separate sales."
             if repeats else
             "Each building came from its own selling company, which is the "
             "same one-building-one-LLC structure read from the other side."))

    how_sec = "<h2>How the record links these companies</h2>" + _para(
        f"Two independent things in the public record have to agree before "
        f"{e(label)} is treated as one group: a shared naming pattern across "
        f"numbered siblings, and a shared mailing address on the deed filings. "
        f"One signal alone is not enough, and roughly seven in ten "
        f"shared-address groups fail the test and appear nowhere on this site.",
        f"This is a documented link, not a finding about ownership. The deeds say "
        f"these companies share a name and a mailing address. They do not say who "
        f"controls them. "
        f'<a href="/network">The full method, and the other networks &rarr;</a>'
    )

    faq = [
        (f"Who is {label}?",
         f"{label} is the name shared by {len(names)} limited liability companies "
         f"that appear as parties in the NYC deed record, "
         + (f"holding {held} buildings between them. " if held else
            f"having sold {sold_n} buildings between them and holding none in the "
            f"current record. ")
         + f"The deed record shows the shared name and the shared filing address; "
           f"it does not name the people behind the companies."),
        ((f"How many NYC buildings does {label} own?" if held else
          f"What did {label} sell?"),
         (f"{held} buildings across {len(names)} companies, counted from deeds "
          f"filed with the city."
          + (f" It has also sold {sold_n}." if sold_n else "")
          if held else
          f"{sold_n} buildings across {len(names)} companies. Every one has been "
          f"sold on, so this family holds nothing in the current record: it is a "
          f"portfolio that has already been unwound.")
         + " Condominium unit deeds are collapsed to the building they sit in, "
           "so a whole-condo purchase does not read as a portfolio."),
        (f"Why is {label} split across so many companies?",
         "Holding each building in its own limited liability company is standard "
         "practice in New York: it separates liability between buildings, "
         "simplifies financing and makes a later sale cleaner. The side effect "
         "is that one owner appears in the public record as many unrelated "
         "names, which is what this page undoes."),
    ]
    if record and record.with_eviction:
        faq.append((
            f"Have there been evictions in {label} buildings?",
            f"{int(record.evictions):,} executed marshal evictions appear across "
            f"{int(record.with_eviction)} of its buildings. PulseCities reports "
            f"what the public record contains and makes no claim about why any "
            f"eviction happened.",
        ))
    faq_html = "".join(
        f'<div class="faq-item"><h3>{e(q)}</h3><p>{e(a)}</p></div>' for q, a in faq
    )

    # The slug is [a-z0-9-] by the route regex. The label lands inside a
    # double-quoted JS string: json.dumps gives exact escaping (quotes,
    # backslashes, control characters, non-ASCII); the one hazard it leaves
    # alone in a <script> block is "</", which is folded.
    js_label = json.dumps(label)[1:-1].replace("</", "<\\/")
    follow_sec = (_FOLLOW_CARD
                  .replace("__SLUG__", slug)
                  .replace("__LABEL__", js_label))

    n_shown = held or sold_n
    verb = "holding" if held else "that sold"
    title = (f"{label}: {n_shown} NYC buildings across {len(names)} LLCs | PulseCities"
             if held else
             f"{label}: {len(names)} NYC LLCs that sold {sold_n} buildings | PulseCities")
    desc = (f"{label} appears in the NYC deed record as {len(names)} separate LLCs "
            f"{verb} {n_shown} buildings. Every company and every building, from "
            f"ACRIS public records.")
    if len(desc) > 165:
        desc = desc[:162].rsplit(" ", 1)[0] + "."
    url = f"https://pulsecities.com/network/{slug}"
    jsonld = _jsonld({"@context": "https://schema.org", "@graph": [
        {"@type": "Organization", "name": label, "url": url,
         "subOrganization": [{"@type": "Organization", "name": n} for n in names[:50]]},
        {"@type": "FAQPage", "mainEntity": [
            {"@type": "Question", "name": q,
             "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faq]},
        _crumbs(("Home", "/"), ("LLC buyers", "/llc"), (label, f"/network/{slug}")),
    ]})

    stats = (
        f'<div class="stat"><div class="stat-num">{len(names)}</div>'
        f'<div class="stat-label">companies</div></div>'
        + (f'<div class="stat"><div class="stat-num">{held}</div>'
           f'<div class="stat-label">'
           f'{"building held" if held == 1 else "buildings held"}</div></div>'
           if held else "")
        + (f'<div class="stat"><div class="stat-num">{sold_n}</div>'
           f'<div class="stat-label">'
           f'{"building sold" if sold_n == 1 else "buildings sold"}</div></div>'
           if sold_n else "")
        + (f'<div class="stat"><div class="stat-num">{len(ranked)}</div>'
           f'<div class="stat-label">'
           f'{"ZIP code" if len(ranked) == 1 else "ZIP codes"}</div></div>'
           if ranked else "")
    )

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
{_llc_head(title, desc, url, "index, follow", jsonld)}
</head>
<body>
{_ssr_nav("", toggle_html="")}
<div class="container" style="max-width:760px;">
  <p style="margin-bottom:8px;font-size:0.75rem;color:var(--faint);">
    <a href="/">Home</a> &middot; <a href="/llc">LLC buyers</a>
    &middot; <a href="/network">Owner networks</a>
  </p>
  <div class="eyebrow">NYC deed record</div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.6rem;letter-spacing:0;font-weight:600;">{e(label)}</h1>
  <div class="stats">{stats}</div>
  {lede}
  {ent_sec}
  {bld_sec}
  {geo_sec}
  {rec_sec}
  {party_sec}
  {follow_sec}
  {how_sec}
  <h2>Common questions</h2>
  {faq_html}
  <p class="note">A deed names a buyer of record. This page describes documents,
  not conduct, and makes no claim of wrongdoing.
  <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    if len(_family_page_cache) >= 128:
        _family_page_cache.clear()
    _family_page_cache[slug] = (page, time.monotonic() + _FAMILY_TTL)
    return HTMLResponse(page)


# --- Rent-stabilization checker: the third tenant question --------------------

_rs_page_cache: tuple[str, float] | None = None
_RS_TTL = 21600


# --- Eviction case lookup: the query class nothing on the site took -------------
#
# "nyc marshal docket number search" is in the search export, and so is a run of
# address queries ending in "eviction cases", one of them at position 6.88 on
# Bing. Both are people holding a piece of paper with a number on it. The site
# had every one of those numbers and no way to type one in.
#
# Two numbers reach this page and they are not the same thing. The docket number
# is the marshal's own case number, five or six digits, and it is what the DOI
# dataset is keyed on. The index number is Housing Court's, shaped 312756/24,
# and it is the one printed on court papers. Both are matched here because a
# tenant cannot be expected to know which is which.
_EVICTION_CASE_TTL = 3600
_eviction_case_cache: tuple[str, float] | None = None
# Court index numbers are not digit/digit: a fifth of the source rows carry a
# borough letter prefix (B309066/25), a part suffix (326184/24A), or stray
# trailing punctuation (306624/25-). Both sides reduce to letters, digits and
# the slash before comparing, so a tenant typing what the papers say matches
# what the dataset stores.
_INDEX_RE = re.compile(r"^[A-Z]{0,3}\d{1,8}/\d{2,4}[A-Z]?$")


def _eviction_case_lookup(db, q: str) -> list:
    """Rows matching a docket or an index number. Docket numbers are stored
    with inconsistent leading zeros (065592 and 64865 in the same export), so
    they are compared with the zeros stripped from both sides."""
    term = q.strip().upper()
    if term.isdigit():
        term = term.lstrip("0") or "0"
        where = "ltrim(docket_number, '0') = :term"
    else:
        norm = re.sub(r"\s+", "", term)
        norm = re.sub(r"(?<=\d)-(?=\d)", "/", norm)
        norm = re.sub(r"[^A-Z0-9/]", "", norm)
        if not _INDEX_RE.match(norm):
            return []
        term = norm
        where = "regexp_replace(upper(court_index_number), '[^A-Z0-9/]', '', 'g') = :term"
    return db.execute(text(f"""
        SELECT docket_number, court_index_number, address, zip_code, borough,
               eviction_type, executed_date, bbl
        FROM evictions_raw
        WHERE {where}
        ORDER BY executed_date DESC
        LIMIT 25
    """), {"term": term}).fetchall()


# Rate limiting for the SSR routes lives in nginx (zone=ssr_heavy), not here:
# slowapi counts per worker, so a two-worker box enforces double what it says.
@router.get("/eviction-case", include_in_schema=False)
def eviction_case_page(q: str = "", db: Session = Depends(get_db)):
    """Look up one executed eviction by its marshal docket or court index number."""
    global _eviction_case_cache
    esc = _html.escape
    q = (q or "").strip()[:32]

    # Only the empty form is cached and indexed. A result page is one row of a
    # public dataset, not a landing page, and letting a crawler walk 37,905
    # docket numbers would be a doorway flood with a search box on it.
    if not q and _eviction_case_cache and time.monotonic() < _eviction_case_cache[1]:
        return HTMLResponse(_eviction_case_cache[0])

    totals = db.execute(text("""
        SELECT count(*) AS n,
               count(*) FILTER (WHERE eviction_type = 'Residential') AS res,
               min(executed_date) AS first, max(executed_date) AS last
        FROM evictions_raw
    """)).first()

    # Ten real rows, so the page shows what both numbers look like rather than
    # describing them, and so a crawler that lands here has somewhere to go.
    recent = db.execute(text("""
        SELECT docket_number, court_index_number, address, borough, zip_code,
               executed_date, bbl
        FROM evictions_raw
        WHERE eviction_type = 'Residential' AND address IS NOT NULL
        ORDER BY executed_date DESC, docket_number DESC
        LIMIT 10
    """)).fetchall()
    recent_rows = "".join(
        f'<li class="rec-row">'
        + (f'<a href="/property/{esc(r.bbl)}">' if r.bbl else '<div class="rec-static">')
        + f'<div><div class="rec-addr">{esc(_addr_title(r.address))}</div>'
        f'<div class="rec-geo">{esc((r.borough or "").title())} {esc(r.zip_code or "")} '
        f'&middot; docket {esc(r.docket_number or "n/a")} '
        f'&middot; index {esc(r.court_index_number or "n/a")}</div></div>'
        f'<div class="rec-side"><div class="rec-date">{_en_date(r.executed_date)}</div></div>'
        + ('</a>' if r.bbl else '</div>')
        + '</li>'
        for r in recent
    )

    result_html = ""
    if q:
        rows = _eviction_case_lookup(db, q)
        if rows:
            cards = ""
            for r in rows:
                place = ", ".join(x for x in [_addr_title(r.address) if r.address else "",
                                              (r.borough or "").title(), r.zip_code or ""] if x)
                link = (f'<a href="/property/{esc(r.bbl)}">The full record for this building &rarr;</a>'
                        if r.bbl else
                        '<span class="dim-note">This record carries no tax lot number, so it '
                        'has no building page here.</span>')
                cards += (
                    f'<div class="case-card">'
                    f'<div class="case-addr">{esc(place)}</div>'
                    f'<div class="case-meta">Executed {_en_date(r.executed_date)} &middot; '
                    f'{esc((r.eviction_type or "").lower() or "unspecified")} &middot; '
                    f'marshal docket {esc(r.docket_number or "n/a")} &middot; '
                    f'court index {esc(r.court_index_number or "n/a")}</div>'
                    f'<div class="case-link">{link}</div></div>')
            result_html = (
                f'<h2>{_count_open(len(rows), "executed eviction")} on this number</h2>'
                f'{cards}'
                f'<p class="sub" style="font-size:0.82rem;">A docket number is reused across '
                f'years and marshals, so more than one record can share it. The court index '
                f'number is the one that identifies a single case.</p>')
        else:
            result_html = (
                f'<h2>Nothing on {esc(q)}</h2>'
                f'<p class="sub" style="font-size:0.86rem;">That number matches no executed '
                f'eviction in the records held here, which cover '
                f'{_en_date(totals.first)} to {_en_date(totals.last)}. Three ordinary reasons: '
                f'the case never reached an execution, which is the outcome in most Housing '
                f'Court cases; the execution is older than this window; or the number is a '
                f'court index number typed without its year, which looks like 312756/24. '
                f'The court file itself is separate from this dataset and lives with the '
                f'court.</p>')

    title = ("NYC marshal eviction docket search: look up a docket or index number "
             "| PulseCities")
    desc = (f"Look up an NYC eviction by marshal docket or Housing Court index number. "
            f"{int(totals.n):,} executed evictions, {_en_date(totals.first)} to "
            f"{_en_date(totals.last)}.")

    faq = [
        ("What is a marshal docket number?",
         "The case number a city marshal assigns to a warrant of eviction. It is five "
         "or six digits and it is what the city's eviction dataset is keyed on. It is "
         "not the same as the Housing Court index number, and marshals reuse docket "
         "numbers across years, so one number can match more than one record."),
        ("What is the court index number?",
         "Housing Court's own case number, shaped like 312756/24, where the digits "
         "after the slash are the year the case was filed. It is printed on the court "
         "papers a tenant receives and it identifies a single case. This page matches "
         "either number."),
        ("My docket number returns nothing. What does that mean?",
         "Most Housing Court cases never reach an executed eviction, and only "
         "executions appear here. A blank result is more often a case that ended "
         "another way than a missing record. Records here run from "
         f"{_en_date(totals.first)} to {_en_date(totals.last)}."),
        ("Does this show executed evictions or only completed evictions?",
         f"Only completed ones. All {int(totals.n):,} records are warrants a marshal "
         f"or sheriff actually executed, {int(totals.res):,} of them residential. "
         "Filings are a much larger number and are not in this dataset."),
        ("Where does this data come from?",
         "NYC Open Data's evictions dataset, published by the Department of "
         "Investigation from marshal and sheriff filings, refreshed here nightly. It "
         "carries addresses, dates and case numbers. It carries no tenant names, and "
         "neither does this page."),
    ]
    faq_html = "".join(
        f'<div class="faq-item"><h3>{esc(a)}</h3><p>{esc(b)}</p></div>' for a, b in faq)
    jsonld = _jsonld({"@context": "https://schema.org", "@graph": [
        {"@type": "FAQPage", "mainEntity": [
            {"@type": "Question", "name": a,
             "acceptedAnswer": {"@type": "Answer", "text": b}} for a, b in faq]},
        _crumbs(("Home", "/"), ("Evictions", "/evictions"),
                ("Eviction case lookup", "/eviction-case")),
    ]})

    robots = "index, follow" if not q else "noindex, follow"
    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
{_llc_head(title, desc, "https://pulsecities.com/eviction-case", robots, jsonld)}
<style>
.case-card{{border:1px solid rgba(147,161,173,0.14);border-radius:10px;padding:14px 16px;margin-bottom:10px;background:#16202d}}
.case-addr{{font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#e4e8ec}}
.case-meta{{font-size:0.78rem;color:var(--dim);margin-top:5px;line-height:1.6}}
.case-link{{font-size:0.8rem;margin-top:8px}}
.case-link a{{color:var(--accent)}}
.dim-note{{font-size:0.8rem;color:var(--faint)}}
</style>
</head>
<body>
{_ssr_nav("", toggle_html="")}
<div class="container" style="max-width:720px;">
  <p style="margin-bottom:8px;font-size:0.75rem;color:var(--faint);">
    <a href="/">Home</a> &middot; <a href="/evictions">Evictions</a>
  </p>
  <div class="eyebrow">NYC eviction records</div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.7rem;letter-spacing:0;font-weight:600;">Look up an eviction case</h1>
  <p class="sub" style="font-size:0.86rem;">Type a marshal docket number or a Housing Court
  index number. {int(totals.n):,} executed evictions, {_en_date(totals.first)} to
  {_en_date(totals.last)}, from city marshal records.</p>
  <form style="display:flex;gap:10px;margin:22px 0 6px;max-width:560px;" action="/eviction-case" method="get">
    <input type="text" name="q" value="{esc(q)}" placeholder="Docket 65592, or index 312756/24"
      aria-label="Marshal docket number or court index number" inputmode="text"
      style="flex:1;font-family:'JetBrains Mono',monospace;font-size:0.85rem;color:#e4e8ec;background:#16202d;border:1px solid rgba(147,161,173,0.2);border-radius:8px;padding:12px 14px;min-width:0;">
    <button type="submit" style="font-family:'DM Sans',sans-serif;font-size:0.9rem;font-weight:600;color:#111823;background:#ed6317;border:none;border-radius:8px;padding:12px 22px;cursor:pointer;">Look up</button>
  </form>
  <p style="font-size:0.75rem;color:var(--faint);margin-bottom:4px;">Public records only. No signup, no tenant names</p>

  {result_html}

  <h2>Which number do you have?</h2>
  <p class="sub" style="font-size:0.86rem;">If it is five or six digits with nothing else,
  it is a marshal docket number. If it has a slash and a two-digit year, like 312756/24, it
  is the Housing Court index number from your court papers. This page takes either. The two
  are not interchangeable: a docket number can repeat across years and marshals, while the
  index number belongs to one case.</p>

  <h2>What this record can and cannot tell you</h2>
  <p class="sub" style="font-size:0.86rem;">It is the execution, not the case. Every row is a
  warrant a marshal or sheriff carried out, with the address, the date and the case numbers.
  It does not carry the reason, the outcome of any appeal, or anyone's name. For the case
  file itself, including filings that never reached an execution, the source is the court,
  not this dataset. Most Housing Court cases end without an execution, so a number that
  returns nothing here is usually a case that ended another way.</p>

  <h2>The ten most recent executions, with their numbers</h2>
  <p class="sub" style="font-size:0.86rem;">Newest first, residential only. Each row carries
  both numbers, so you can see which one you are holding.</p>
  <ul class="sib-list">{recent_rows}</ul>

  <h2>What you can do with an address</h2>
  <p class="sub" style="font-size:0.86rem;">Once you have the address, the building's own page
  carries every execution on record there, the deed history behind it, open violations and
  renovation permits. That is usually the more useful question: not what happened in one
  case, but what has been happening at that address.
  <a href="/evictions">The citywide eviction list &rarr;</a></p>

  <h2>Common questions</h2>
  {faq_html}

  <p class="note">Every record here comes from NYC Open Data. PulseCities describes documents,
  not conduct, and makes no claim about any case.
  <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    if not q:
        _eviction_case_cache = (page, time.monotonic() + _EVICTION_CASE_TTL)
    return HTMLResponse(page)


@router.get("/is-my-building-rent-stabilized", include_in_schema=False)
def rent_stabilized_page(db: Session = Depends(get_db)):
    """Intent landing for the "is my building rent stabilized" query class.

    The honest answer routes through DHCR's rent history; this page says so
    plainly, gives the building-level clues, and shows what the site tracks.
    """
    global _rs_page_cache
    if _rs_page_cache and time.monotonic() < _rs_page_cache[1]:
        return HTMLResponse(_rs_page_cache[0])

    esc = _html.escape

    latest = db.execute(text("""
        SELECT year, count(DISTINCT bbl) AS bldgs, sum(rs_unit_count) AS units
        FROM rs_buildings
        WHERE rs_unit_count > 0 AND source = 'dhcr'
        GROUP BY year ORDER BY year DESC LIMIT 1
    """)).first()
    rs_year = int(latest.year) if latest else None
    rs_units = int(latest.units) if latest and latest.units else 0
    rs_bldgs = int(latest.bldgs) if latest and latest.bldgs else 0

    title = "Is my building rent stabilized? How to check any NYC address | PulseCities"
    desc = ("How to find out if your NYC apartment is rent stabilized: the free "
            "official rent history request, the building-level clues, and what the "
            "registration record shows.")

    faq = [
        ("How do I check if my apartment is rent stabilized?",
         "The definitive answer is your rent history, free from New York State "
         "Homes and Community Renewal. Request it through the Ask HCR portal. "
         "Building-level clues help too: buildings built before 1974 with six or "
         "more units are commonly stabilized, and buildings receiving certain tax "
         "benefits must register stabilized units."),
        ("Is there an official public list of stabilized buildings?",
         "DHCR publishes building-level registration lists, but they lag and a "
         "building's presence or absence is not conclusive for any single "
         "apartment. That is why the rent history request exists."),
        ("Can a sale or renovation end stabilization?",
         "A sale does not end stabilization; the status attaches to the "
         "apartment, not the owner. Since the 2019 housing law, most paths that "
         "removed apartments from stabilization are closed, though earlier "
         "deregulations still stand. If your rent jumped after a renovation, the "
         "rent history shows how the increases were registered."),
        ("What does PulseCities track about stabilization?",
         "Neighborhood pages track registered stabilized-unit counts year over "
         "year as a displacement signal: a building that stops registering units "
         "is a building worth watching."),
    ]
    faq_html = "".join(
        f'<div class="faq-item"><h3>{esc(q)}</h3><p>{esc(a)}</p></div>' for q, a in faq
    )
    jsonld = _jsonld({"@context": "https://schema.org", "@graph": [
        {"@type": "FAQPage", "mainEntity": [
            {"@type": "Question", "name": q,
             "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faq]},
        _crumbs(("Home", "/"), ("Is my building rent stabilized", "/is-my-building-rent-stabilized")),
    ]})

    reg_line = ""
    if rs_year and rs_units:
        reg_line = (f"In {rs_year}, the most recent registration year PulseCities holds, "
                    f"{rs_units:,} stabilized units were registered across "
                    f"{rs_bldgs:,} NYC buildings. The state withdrew its annual snapshot "
                    f"dataset in April 2026, so nothing newer is published here.")

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
{_llc_head(title, desc, "https://pulsecities.com/is-my-building-rent-stabilized", "index, follow", jsonld)}
</head>
<body>
{_ssr_nav("", toggle_html="")}
<div class="container" style="max-width:720px;">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:var(--faint);">&#8592; Home</a>
  </div>
  <div class="eyebrow">NYC housing records</div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.7rem;letter-spacing:0;font-weight:600;">Is my building rent stabilized?</h1>
  <p class="sub" style="font-size:0.86rem;">There is one definitive answer and a set of useful clues. The definitive answer is free and official; the clues start with your address.</p>
  <form style="display:flex;gap:10px;margin:22px 0 6px;max-width:560px;" action="/map" method="get">
    <input type="text" name="q" placeholder="Enter an address, ZIP, or neighborhood" aria-label="Search an address, ZIP, or neighborhood" style="flex:1;font-family:'JetBrains Mono',monospace;font-size:0.85rem;color:#e4e8ec;background:#16202d;border:1px solid rgba(147,161,173,0.2);border-radius:8px;padding:12px 14px;min-width:0;">
    <button type="submit" style="font-family:'DM Sans',sans-serif;font-size:0.9rem;font-weight:600;color:#111823;background:#ed6317;border:none;border-radius:8px;padding:12px 22px;cursor:pointer;">Search</button>
  </form>
  <p style="font-size:0.75rem;color:var(--faint);margin-bottom:4px;">Free, no signup. Public records only</p>

  <h2>The definitive answer: your rent history</h2>
  <p class="sub" style="font-size:0.86rem;">New York State keeps the registration record for every stabilized apartment. Request your unit's rent history free through the <a href="https://portal.hcr.ny.gov/" target="_blank" rel="noopener noreferrer" style="color:#6fb1d8;">Ask HCR portal</a>, run by <a href="https://hcr.ny.gov/" target="_blank" rel="noopener noreferrer" style="color:#6fb1d8;">NYS Homes and Community Renewal</a>. It shows every registered rent for your apartment, which answers the question and often more.</p>

  <h2>The building-level clues</h2>
  <p class="sub" style="font-size:0.86rem;">Buildings built before 1974 with six or more units are commonly stabilized. Buildings that took certain tax benefits must register units for the benefit period. And the registration record itself is a signal: {esc(reg_line) if reg_line else "DHCR publishes building-level registration counts each year."} A building that stops registering units year over year is a building worth watching, which is exactly how PulseCities uses the data on every <a href="/neighborhoods" style="color:#6fb1d8;">neighborhood page</a>.</p>

  <h2>While you are checking</h2>
  <p class="sub" style="font-size:0.86rem;">The same address search shows who took the deed on your building, the eviction record, and open violations. <a href="/who-owns-my-building" style="color:#6fb1d8;">Who owns my building &rarr;</a> <a href="/eviction-case" style="color:#6fb1d8;margin-left:10px;">Look up an eviction case &rarr;</a></p>

  <h2>Common questions</h2>
  {faq_html}

  <p class="note">PulseCities reads public registration data; it does not determine any apartment's legal status. For a binding answer, request your rent history from DHCR. <a href="/methodology">How PulseCities reads the record &rarr;</a></p>
</div>
{_FOOTER_HTML}
</body>
</html>"""

    _rs_page_cache = (page, time.monotonic() + _RS_TTL)
    return HTMLResponse(page)
