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
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
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
    "311 housing complaints, eviction filings, ACRIS property deed transfers, DHCR "
    "rent-stabilized housing data, and MapPLUTO residential unit counts."
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
         "transferencias de escrituras de ACRIS, datos de renta estabilizada de DHCR y "
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


def _tier_info(score: float) -> tuple[str, str]:
    """
    Returns (display_label, hex_color) for the score tier.
    Bands must match the map legend, weekly digest, and _build_summary:
    Low 0-33, Moderate 34-66, High 67-84, Critical 85+.
    """
    if score >= 85: return "Critical", "#ef4444"
    if score >= 67: return "High",     "#f97316"
    if score >= 34: return "Moderate", "#C08B2D"
    return "Low", "#64748b"


def _idx_color(v: float) -> str:
    if v >= 70: return "#f97316"
    if v >= 45: return "#C08B2D"
    return "rgba(148,163,184,0.55)"


# One footer for every SSR page, same link set as the static pages.
# test_footer_consistency.py fails the suite if the two drift apart.
# Interpolate as {_FOOTER_HTML} inside the page f-strings.
_FOOTER_HTML = """<footer>
  <div style="font-size:11px;color:#64748b;margin-bottom:8px;text-align:center;">Built by Michael Espin</div>
  <div class="footer-links">
    <a href="/" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Home</a>
    <a href="/neighborhoods" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Neighborhoods</a>
    <a href="/displacement" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Displacement</a>
    <a href="/methodology" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Methodology</a>
    <a href="/about" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">About</a>
    <a href="/press" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Press</a>
    <a href="/status" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Status</a>
    <a href="mailto:nycdisplacement@gmail.com" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Contact</a>
    <a href="https://www.linkedin.com/in/michaelespin/" target="_blank" rel="noopener noreferrer" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">LinkedIn</a>
    <a href="https://x.com/PulseCities" target="_blank" rel="noopener noreferrer" aria-label="PulseCities on X" style="color:#64748b;text-decoration:none;display:inline-flex;align-items:center;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" focusable="false"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg></a>
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

# Neighborhood-page copy, both languages. Data (names, numbers, dates) flows
# in via format slots; everything a reader sees as prose lives here.
_NB_L = {
    "en": {
        "title_scored": "{name} ({zip}) Displacement Score {s}/100 | PulseCities",
        "social_scored": "{name} ({zip}) | Displacement Score {s}/100 | PulseCities",
        "title_unscored": "{name} ({zip}) NYC Displacement Signals | PulseCities",
        "desc_scored": ("{name} shows {tier} displacement-pressure signals based on NYC public records, "
                        "including LLC acquisitions, eviction filings, 311 complaints, HPD violations, "
                        "permits, and rent-stabilized housing data."),
        "desc_unscored": ("Track displacement-pressure signals in {name} ({zip}) from NYC public records: "
                          "LLC acquisitions, eviction filings, 311 complaints, HPD violations, permits, "
                          "and rent-stabilized housing data."),
        "nav": [("/map", "Map"), ("/methodology", "Methodology"), ("/about", "About"), ("/press", "Press")],
        "back_map": "&#8592; Back to map",
        "all_borough": "All {borough} ZIPs",
        "h1": "Displacement Signals | {name} ({zip})",
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
            "evictions": "Residential eviction filings",
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
        "h1": "Señales de desplazamiento | {name} ({zip})",
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
    petitions: dict | None = None,
    vacates: dict | None = None,
    flips: list | None = None,
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
        tier_label, tier_color = "Unknown", "#64748b"
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

    tier_line = L["tier_line"].format(tier=_TIER_WORDS[lang][tier_label])
    score_block = (
        f'<div class="score-block">'
        f'<span class="score-num" style="color:{tier_color};">{score_str}</span>'
        f'<span class="score-denom">/100</span>'
        f'<span class="score-tier" style="color:{tier_color};">{tier_line}</span>'
        f'</div>'
        if score is not None
        else f'<div class="score-block"><p style="color:rgba(148,163,184,0.5);font-size:0.9rem;">{L["no_score"]}</p></div>'
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
            f'<span style="font-family:\'JetBrains Mono\',monospace;color:#f1f5f9;">{delta_s}</span></p>'
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
            arrow_color = "#ef4444" if change >= 10 else ("#3E6B54" if change <= -10 else "var(--muted)")
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
                f'<tr onclick="location.href=\'/property/{e(str(f["bbl"]))}\'" style="cursor:pointer;">'
                f'<td class="sc">{e(f["address"])}<span class="sw">{e(f.get("buyer") or "")}</span></td>'
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

    # The alternate-language URL for the toggle and hreflang pair. English is
    # the parameterless canonical form; Spanish lives at ?lang=es.
    alt_url = f"{base_url}?lang=es" if lang == "en" else base_url
    nav_links = "".join(f'<a href="{href}">{label}</a>' for href, label in L["nav"])

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
        "function show(t,ok){m.textContent=t;m.style.color=ok?'#3E6B54':'#ef4444';m.style.display='block';}"
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
.table-wrap{{overflow-x:auto;margin-bottom:12px}}
table{{width:100%;border-collapse:collapse}}
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
.watch-card{{background:rgba(249,115,22,.05);border:1px solid rgba(249,115,22,.22);border-radius:10px;padding:20px 22px;margin-bottom:32px}}
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
<nav><div class="nav-inner">
  <a href="/" style="display:flex;align-items:center;gap:8px;">
    <svg width="20" height="20" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
    <span style="font-size:.85rem;color:rgba(148,163,184,.55);">PulseCities</span>
  </a>
  <div class="nav-links">{nav_links}<a href="{e(alt_url)}" id="lang-toggle" aria-label="{L['lang_toggle_aria']}">{L['lang_toggle_label']}</a></div>
</div></nav>
<main><div class="container">
  <p class="breadcrumb"><a href="/map">{L['back_map']}</a>{breadcrumb_borough}</p>
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
{petitions_section}{vacates_section}{flips_section}  <section style="margin-bottom:32px;">
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
  <p class="meth-link"><a href="/methodology">{L['meth_link']}</a></p>
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
    flip_rows = db.execute(text("""
        WITH llc_transfers AS (
            SELECT o.bbl, o.doc_date AS transfer_date,
                   o.party_name_normalized AS buyer, o.doc_amount, p.address
            FROM ownership_raw o
            JOIN parcels p ON p.bbl = o.bbl
            WHERE o.party_name_normalized LIKE '%LLC%'
              AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
              AND o.party_type = '2'
              AND o.doc_date >= CURRENT_DATE - INTERVAL '365 days'
              AND p.zip_code = :zip
        ),
        reno_permits AS (
            SELECT bbl, MIN(filing_date) AS first_permit_date
            FROM permits_raw
            WHERE raw_data->>'job_type' IN ('A1', 'A2')
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
    """), {"zip": zip_code}).fetchall()
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

    page_html = _build_neighborhood_page(
        zip_code, name, borough, score, breakdown, raw_counts, raw_hpd, summary, last_updated, history,
        petitions=petitions, vacates=vacates, flips=flips, lang=lang,
    )
    _page_cache[cache_key] = (page_html, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page_html)


_BOROUGH_SLUGS = {
    "Manhattan": "manhattan", "Brooklyn": "brooklyn", "Queens": "queens",
    "Bronx": "bronx", "Staten Island": "staten-island",
}


def _build_property_page(bbl, address, zip_code, borough, score, sig, op) -> str:
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
    has_signals = bool(owners or evicts or permits) or score is not None

    def _section(h2, note, heads, rows):
        if not rows:
            return ""
        th = "".join(f"<th>{c}</th>" for c in heads)
        return (f'<section style="margin-bottom:30px;"><h2>{h2}</h2>'
                f'<div class="table-wrap"><table><thead><tr>{th}</tr></thead>'
                f'<tbody>{rows}</tbody></table></div>'
                f'<p class="data-note">{note}</p></section>')

    own_rows = "".join(
        f'<tr><td class="sc">{e(o.get("buyer") or "")}<span class="sw">{e(o.get("doc_type") or "")}</span></td>'
        f'<td class="sr">{_d(o.get("date"))}</td><td class="si">{_money(o.get("amount"))}</td></tr>'
        for o in owners
    )
    own_sec = _section(
        "Ownership transfers",
        "Deeds recorded in ACRIS. Amount is the stated consideration; $0 often marks a "
        "non-arms-length transfer.",
        ("Buyer", "Recorded", "Amount"), own_rows,
    )

    ev_rows = "".join(
        f'<tr><td class="sc">{e(ev.get("type") or "Residential")}'
        f'<span class="sw">{e("docket " + ev["docket"] if ev.get("docket") else "")}</span></td>'
        f'<td class="sr">{_d(ev.get("date"))}</td><td class="si"></td></tr>'
        for ev in evicts
    )
    ev_sec = _section(
        "Executed evictions",
        "Marshal-executed residential evictions from the NYC evictions dataset, past 12 months.",
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
        "DOB job filings on this lot, past 12 months.",
        ("Work", "Filed", "Type"), pm_rows,
    )

    comp_sec = ""
    if complaints:
        comp_sec = (
            '<section style="margin-bottom:30px;"><h2>311 housing complaints</h2>'
            f'<p style="font-size:.95rem;margin-bottom:8px;"><span style="font-family:\'JetBrains Mono\','
            f'monospace;font-size:1.3rem;font-weight:600;">{len(complaints)}</span> '
            '<span style="color:var(--muted);">complaints in the past 12 months</span></p>'
            '<p class="data-note">NYC 311 housing and building complaints logged for this address.</p></section>'
        )

    score_block = ""
    if score is not None:
        tier, color = _tier_info(score)
        score_block = (
            f'<div class="score-block"><span class="score-num" style="color:{color}">{score:.1f}</span>'
            f'<span class="score-denom">/100</span>'
            f'<span class="score-tier" style="color:{color}">{tier.upper()} AREA PRESSURE</span></div>'
        )

    # Up-links: ZIP, owning operator, borough. These turn the property page from
    # a dead-end into a hub node and give crawlers a path back to the money pages.
    links = []
    if zip_code:
        links.append(f'<a href="/neighborhood/{e(zip_code)}" class="btn-map">Displacement signals for {e(zip_code)} &rarr;</a>')
    if op is not None:
        links.append(f'<a href="/operator/{e(op.slug)}" class="btn-copy">Owner network: {e(op.display_name or op.operator_root)} &rarr;</a>')
    links.append('<a href="/map" class="btn-copy">Open the map &rarr;</a>')
    links_html = "".join(links)

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
    score_part = f" | Displacement Score {score:.1f}/100" if score is not None else ""
    title = f"{address}, {borough}{score_part} | PulseCities"
    zloc = f" ({zip_code})" if zip_code else ""
    desc = (f"{address}, {borough}{zloc}: deed transfers, eviction filings, and renovation "
            f"permits from NYC public records"
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

    body_note = ("Sourced from NYC public records: ACRIS deeds, DOB permits, the NYC evictions dataset, "
                 "and 311. Records reflect what agencies have published and can lag events.")
    empty = "" if has_signals else (
        '<p class="section-sub" style="margin-top:8px;">No deed transfers, evictions, or permits are on '
        'record for this building in the current window. It is shown for reference.</p>'
    )

    head = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
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
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%231a1a2e'/%3E%3Cpolyline points='2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16' fill='none' stroke='%23f97316' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<script type="application/ld+json">{place_ld}</script>
<script type="application/ld+json">{bc_ld}</script>{_PLAUSIBLE}
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;600&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;600&display=swap"></noscript>
"""

    css = """<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0f172a;--border:rgba(148,163,184,.1);--text:#f1f5f9;--muted:rgba(148,163,184,.65);--faint:rgba(148,163,184,.35);--accent:#f97316}
body{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;line-height:1.6;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
nav{border-bottom:1px solid var(--border);padding:12px 0}
.nav-inner{max-width:720px;margin:0 auto;padding:0 20px;display:flex;align-items:center;gap:8px}
.brand{font-size:.85rem;color:rgba(148,163,184,.55)}
.container{max-width:720px;margin:0 auto;padding:28px 20px 72px}
.breadcrumb{font-size:.78rem;color:var(--muted);margin-bottom:18px}
.breadcrumb a:hover{color:var(--text)}
h1{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;font-weight:600;line-height:1.25;margin-bottom:6px}
.subline{font-size:.82rem;color:var(--muted);margin-bottom:22px;font-family:'JetBrains Mono',monospace}
.score-block{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap;padding:16px 20px;background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:8px;margin-bottom:26px}
.score-num{font-size:2.2rem;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1}
.score-denom{font-size:.9rem;color:var(--muted);font-family:'JetBrains Mono',monospace;align-self:flex-end;padding-bottom:3px}
.score-tier{font-size:.62rem;font-weight:600;letter-spacing:.08em;align-self:flex-end;padding-bottom:5px;margin-left:8px}
h2{font-size:.68rem;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--faint);margin-bottom:10px}
.section-sub{font-size:.82rem;color:var(--muted)}
.table-wrap{overflow-x:auto;margin-bottom:10px}
table{width:100%;border-collapse:collapse}
th{font-size:.64rem;font-weight:500;text-transform:uppercase;letter-spacing:.06em;color:var(--faint);padding:6px 0;border-bottom:1px solid var(--border);text-align:left}
th:not(:first-child){text-align:right}
td{padding:11px 0;border-bottom:1px solid rgba(148,163,184,.06);vertical-align:top}
.sc{font-size:.86rem}
.sw{display:block;font-size:.71rem;color:var(--faint);margin-top:2px}
.sr,.si{font-size:.85rem;font-family:'JetBrains Mono',monospace;text-align:right;white-space:nowrap}
.data-note{font-size:.73rem;color:var(--faint);margin-top:8px;line-height:1.5}
.cta-row{display:flex;gap:10px;flex-wrap:wrap;margin:28px 0 4px}
.btn-map{display:inline-flex;align-items:center;padding:10px 18px;background:var(--accent);color:#fff;border-radius:6px;font-size:.84rem;font-weight:500}
.btn-map:hover{opacity:.9}
.btn-copy{display:inline-flex;align-items:center;padding:10px 18px;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:.84rem}
.btn-copy:hover{color:var(--text);border-color:rgba(148,163,184,.3)}
.foot-note{font-size:.72rem;color:var(--faint);margin-top:20px;line-height:1.5}
footer{border-top:1px solid var(--border);padding:24px 20px calc(env(safe-area-inset-bottom,0px) + 24px);text-align:center;margin-top:20px;font-size:12px;color:#64748b}
.footer-links{display:flex;justify-content:center;gap:20px;flex-wrap:wrap}
</style>
"""

    body = f"""</head>
<body>
<nav><div class="nav-inner">
<a href="/" style="display:flex;align-items:center;gap:8px;">
<svg width="20" height="20" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
<span class="brand">PulseCities</span></a>
</div></nav>
<main><div class="container">
<p class="breadcrumb">{crumb_html}</p>
<h1>{e(address)}</h1>
<p class="subline">{e(borough)}{(" &middot; " + e(zip_code)) if zip_code else ""} &middot; BBL {e(bbl)}</p>
{score_block}
{empty}{own_sec}{ev_sec}{pm_sec}{comp_sec}
<div class="cta-row">{links_html}</div>
<p class="foot-note">{body_note}</p>
</div></main>
{_FOOTER_HTML}
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

    from api.routes.properties import _get_property_data
    sig = _get_property_data(clean, db).get("signals", {})
    op = db.execute(text(
        "SELECT o.slug, o.display_name, o.operator_root "
        "FROM operators o JOIN operator_parcels op ON op.operator_id = o.id "
        "WHERE op.bbl = :bbl AND o.operator_class = 'operator' LIMIT 1"
    ), {"bbl": clean}).fetchone()
    html = _build_property_page(clean, address, zip_code, borough, score, sig, op)

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
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
@media(max-width:600px){{.nav-inner{{flex-wrap:wrap;row-gap:4px}}.nav-inner>div{{flex-wrap:wrap;row-gap:4px}}}}
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
      <a href="/about" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">About</a><a href="/press" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Press</a>
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
  <p id="dir-sub" style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.55);margin-bottom:28px;">{n_visible} clusters tracked across an 18-month public records window.</p>
  <ul class="op-list">
{rows_html}  </ul>
</div>
{_FOOTER_HTML}
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  var i18n = {{
    en: {{
      heading: 'NYC Operator Networks',
      desc: 'Ownership clusters identified in NYC deed records. Each groups LLCs by naming patterns and acquisition activity. Public records only.',
      sub: '{n_visible} clusters tracked across an 18-month public records window.',
      acq: 'acquisitions',
      cta: 'View profile \\u2192',
      toggle: 'EN / ES'
    }},
    es: {{
      heading: 'Redes de operadores de NYC',
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
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": LL["dir_h1"],
        "description": desc,
        "url": canonical,
        "numberOfItems": n,
        "itemListElement": list_items,
    })

    page = f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
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
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
@media(max-width:600px){{.nav-inner{{flex-wrap:wrap;row-gap:4px}}.nav-inner>div{{flex-wrap:wrap;row-gap:4px}}}}
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
      <a href="/map" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_map']}</a>
      <a href="/operators" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_operators']}</a>
      <a href="/flips" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_flips']}</a>
      <a href="/radar" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_radar']}</a>
      <a href="/methodology" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_meth']}</a>
      <a href="{alt_url}" id="lang-toggle" aria-label="{LL['toggle_aria']}" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['toggle']}</a>
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">{LL['back_home']}</a>
  </div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">{LL['dir_h1']}</h1>
  <p style="font-size:0.82rem;color:#94a3b8;margin-bottom:8px;line-height:1.6;">
    {LL['dir_intro']}
  </p>
  <p style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.55);margin-bottom:28px;">{LL['dir_count'].format(n=n)}</p>
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
        f'<a href="/{s}{lsuf}" style="color:rgba(148,163,184,0.6);">{b}</a>'
        for s, b in _BOROUGH_SLUGS.items() if s != slug
    )

    page = f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
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
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{_html.escape(title)}">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
@media(max-width:600px){{.nav-inner{{flex-wrap:wrap;row-gap:4px}}.nav-inner>div{{flex-wrap:wrap;row-gap:4px}}}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.stat-row{{display:flex;gap:28px;flex-wrap:wrap;margin-bottom:28px}}
.stat{{display:flex;flex-direction:column;gap:2px}}
.stat-num{{font-family:'JetBrains Mono',monospace;font-size:1.15rem;font-weight:600;color:#e2e8f0}}
.stat-label{{font-size:0.68rem;color:rgba(148,163,184,0.55);text-transform:uppercase;letter-spacing:0.06em}}
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
      <a href="/map" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_map']}</a>
      <a href="/neighborhoods{lsuf}" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_nbhds']}</a>
      <a href="/flips" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_flips']}</a>
      <a href="/radar" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_radar']}</a>
      <a href="/methodology" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['nav_meth']}</a>
      <a href="{alt_url}" id="lang-toggle" aria-label="{LL['toggle_aria']}" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">{LL['toggle']}</a>
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/neighborhoods{lsuf}" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">{LL['back_all']}</a>
  </div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">{LL['b_h1'].format(borough=borough)}</h1>
  <p style="font-size:0.82rem;color:#94a3b8;margin-bottom:20px;line-height:1.6;">
    {LL['b_intro'].format(borough=borough)}
  </p>
  <div class="stat-row">
    <div class="stat"><span class="stat-num">{n}</span><span class="stat-label">{LL['b_stat_zips']}</span></div>
    <div class="stat"><span class="stat-num">{avg:.1f}</span><span class="stat-label">{LL['b_stat_avg']}</span></div>
    <div class="stat"><span class="stat-num">{float(top.score):.1f}</span><span class="stat-label">{LL['b_stat_top'].format(name=_html.escape(top.name or top.zip_code))}</span></div>
  </div>
  <ul class="nb-list">
{rows_html}  </ul>
  <p style="font-size:0.75rem;color:rgba(148,163,184,0.5);margin-top:24px;">{LL['b_others']} {others}</p>
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
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
@media(max-width:600px){{.nav-inner{{flex-wrap:wrap;row-gap:4px}}.nav-inner>div{{flex-wrap:wrap;row-gap:4px}}}}
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
  <p id="fw-sub" style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.55);margin-bottom:6px;">{n} flips detected across NYC in the past 12 months.</p>
  <p style="font-size:0.75rem;margin-bottom:28px;"><a href="/flips/editions" id="fw-editions-link" style="color:rgba(249,115,22,0.75);">Weekly reviewed editions &rarr;</a></p>
  <ul class="flip-list">
{rows_html}  </ul>
  <p id="fw-note" style="font-size:0.72rem;color:rgba(148,163,184,0.45);margin-top:24px;line-height:1.6;">
    A renovation permit alone is not wrongdoing. This page reports the public-record pattern, not a conclusion about any owner. <a href="/methodology" style="color:rgba(249,115,22,0.75);">How this is measured &rarr;</a>
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
  <h2 class="edition-week">{week}<span class="edition-date"> &middot; published {generated}</span></h2>
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
        "@type": "ItemList",
        "name": "NYC Eviction Flips, weekly editions",
        "description": desc,
        "url": "https://pulsecities.com/flips/editions",
        "numberOfItems": total_arcs,
        "itemListElement": list_items,
    })

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
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
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh;line-height:1.65}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
@media(max-width:600px){{.nav-inner{{flex-wrap:wrap;row-gap:4px}}.nav-inner>div{{flex-wrap:wrap;row-gap:4px}}}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
@media(max-width:767px){{.container{{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}}}
.edition{{margin-bottom:36px}}
.edition-week{{font-family:'JetBrains Mono',monospace;font-size:0.85rem;font-weight:600;color:#f97316;letter-spacing:0.06em;margin-bottom:14px;text-transform:uppercase}}
.edition-date{{color:rgba(148,163,184,0.5);font-weight:400;text-transform:none;letter-spacing:0}}
.arc-card{{background:#131e33;border:1px solid rgba(148,163,184,0.12);border-radius:12px;padding:16px 18px 14px;margin-bottom:12px}}
.arc-head{{display:flex;align-items:baseline;justify-content:space-between;gap:12px;flex-wrap:wrap}}
.arc-addr{{font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.02rem;font-weight:600;color:#e2e8f0}}
.arc-addr:hover{{color:#f97316}}
.arc-gain{{font-family:'JetBrains Mono',monospace;font-size:0.8rem;font-weight:600;color:#ef4444;border:1.5px solid rgba(239,68,68,0.5);border-radius:4px;padding:1px 8px;white-space:nowrap}}
.arc-geo{{font-family:'JetBrains Mono',monospace;font-size:0.7rem;color:rgba(148,163,184,0.55);margin:2px 0 12px}}
.arc-steps{{list-style:none;margin:0 0 12px;display:flex;flex-direction:column;gap:7px}}
.arc-steps li{{font-size:0.85rem;color:#cbd5e1}}
.arc-date{{font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:#f97316;display:block}}
.arc-ids{{font-family:'JetBrains Mono',monospace;font-size:0.68rem;color:rgba(148,163,184,0.45);border-top:1px solid rgba(148,163,184,0.08);padding-top:10px;word-break:break-word}}
.ed-empty{{font-size:0.85rem;color:#94a3b8;padding:24px 0}}
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
      <a href="/flips" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Flip Watch</a>
      <a href="/radar" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Radar</a>
      <a href="/press" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Press</a>
      <a href="/methodology" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Methodology</a>
      <button id="lang-toggle" style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.5);background:none;border:none;cursor:pointer;padding:4px 2px;min-height:32px;">EN / ES</button>
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/flips" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; Flip Watch</a>
  </div>
  <h1 id="ed-heading" style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.4rem;font-weight:600;margin-bottom:6px;">Eviction Flips: weekly editions</h1>
  <p id="ed-desc" style="font-size:0.82rem;color:#94a3b8;margin-bottom:28px;line-height:1.6;">
    The arc this site exists to document: a residential eviction, an LLC purchase, and a markup resale on the same lot. Every step is a public record with its ACRIS document ID. A new edition publishes each week after human review; nothing appears here unreviewed.
  </p>
{sections_html}
  <p id="ed-note" style="font-size:0.72rem;color:rgba(148,163,184,0.45);margin-top:24px;line-height:1.6;">
    An eviction followed by a sale is not by itself wrongdoing. This page reports the public-record pattern, not a conclusion about any owner. <a href="/methodology" style="color:rgba(249,115,22,0.75);">How this is measured &rarr;</a>
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
    if (note) note.innerHTML = s.note + ' <a href="/methodology" style="color:rgba(249,115,22,0.75);">' + s.how + '</a>';
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
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
@media(max-width:600px){{.nav-inner{{flex-wrap:wrap;row-gap:4px}}.nav-inner>div{{flex-wrap:wrap;row-gap:4px}}}}
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
{_FOOTER_HTML}
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

# --- Weekly review: shared history queries + the completed-week archive -------

_WEEK_SLUG_RE = re.compile(r"^(\d{4})-W(\d{2})$")
_week_page_cache: dict[str, tuple[str, float]] = {}   # slug -> (html, expires)
_week_index_cache: tuple[str, float] | None = None


def _movers_between(db, as_of: date, prior: date, limit: int = 8):
    """Top risers comparing the latest score on/before `as_of` to the latest on
    or before `prior`. DISTINCT ON walks back to the nearest earlier snapshot, so
    a missing exact date still resolves. Powers both /this-week and the archive."""
    return db.execute(text("""
        WITH now_s AS (
            SELECT DISTINCT ON (zip_code) zip_code, composite_score AS s
            FROM score_history WHERE scored_at <= :as_of
            ORDER BY zip_code, scored_at DESC
        ),
        then_s AS (
            SELECT DISTINCT ON (zip_code) zip_code, composite_score AS s
            FROM score_history WHERE scored_at <= :prior
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
    """), {"as_of": as_of, "prior": prior, "limit": limit}).fetchall()


def _counts_between(db, start: date, end_exclusive: date):
    """Public-record filings dated within [start, end_exclusive). Event-dated, so
    a past week reconstructs exactly from the retained raw tables."""
    return db.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM evictions_raw  WHERE executed_date   >= :s AND executed_date   < :e) AS evictions,
            (SELECT COUNT(*) FROM permits_raw    WHERE filing_date     >= :s AND filing_date     < :e) AS permits,
            (SELECT COUNT(*) FROM complaints_raw WHERE created_date    >= :s AND created_date    < :e) AS complaints,
            (SELECT COUNT(*) FROM violations_raw WHERE inspection_date >= :s AND inspection_date < :e) AS violations
    """), {"s": start, "e": end_exclusive}).fetchone()


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
        color = "#ef4444" if m.delta >= 5 else "#f97316"
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
            (counts.evictions,  "eviction filings"),
            (counts.permits,    "construction permits"),
            (counts.violations, "HPD violations"),
            (counts.complaints, "311 housing complaints"),
        ]
    )


_WEEK_CSS = """*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}
nav{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}
.nav-inner{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}
@media(max-width:600px){.nav-inner{flex-wrap:wrap;row-gap:4px}.nav-inner>div{flex-wrap:wrap;row-gap:4px}}
.container{max-width:860px;margin:0 auto;padding:32px 20px 80px}
a{color:inherit;text-decoration:none}
h2{font-size:0.78rem;font-weight:600;color:rgba(148,163,184,0.75);text-transform:uppercase;letter-spacing:0.1em;margin:32px 0 4px}
.tw-list{list-style:none;padding:0;margin:0}
.tw-row{border-bottom:1px solid rgba(148,163,184,0.07);cursor:pointer}
.tw-row:hover{background:rgba(148,163,184,0.04)}
.tw-row a{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:13px 0}
.tw-name{font-family:'JetBrains Mono',monospace;font-size:0.88rem;color:#e2e8f0;font-weight:500}
.tw-row:hover .tw-name{color:#f97316}
.tw-sub{font-family:'DM Sans',sans-serif;font-size:0.76rem;color:rgba(148,163,184,0.7);font-weight:400;margin-left:6px}
.tw-side{display:flex;flex-direction:column;align-items:flex-end;flex-shrink:0}
.tw-delta{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:500;line-height:1.1}
.tw-score{font-size:0.68rem;color:rgba(148,163,184,0.55)}
.tw-empty{padding:18px 0;font-size:0.8rem;color:#94a3b8}
.tw-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:12px}
.tw-stat{background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;padding:16px}
.tw-stat-n{font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:600;color:#f1f5f9}
.tw-stat-l{font-size:0.72rem;color:#94a3b8;margin-top:4px}
.wk-nav{display:flex;justify-content:space-between;gap:12px;margin-top:36px;font-family:'JetBrains Mono',monospace;font-size:0.75rem}
.wk-nav a{color:rgba(249,115,22,0.8)}
.wk-idx{list-style:none;padding:0;margin:0}
.wk-idx-row{border-bottom:1px solid rgba(148,163,184,0.07)}
.wk-idx-row a{display:flex;align-items:baseline;justify-content:space-between;gap:16px;padding:14px 0}
.wk-idx-row:hover{background:rgba(148,163,184,0.04)}
.wk-idx-range{font-family:'JetBrains Mono',monospace;font-size:0.85rem;color:#e2e8f0}
.wk-idx-row:hover .wk-idx-range{color:#f97316}
.wk-idx-top{font-size:0.76rem;color:rgba(148,163,184,0.7);text-align:right}
footer{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}
.footer-links{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}
@media(max-width:767px){.container{padding:32px 16px calc(env(safe-area-inset-bottom,0px) + 24px)}}"""


def _week_nav_html() -> str:
    return (
        '<nav>\n  <div class="nav-inner">\n'
        '    <a href="/" style="display:flex;align-items:center;gap:8px;color:#f1f5f9;">'
        '<svg width="22" height="22" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>'
        '<span style="font-size:0.85rem;color:rgba(148,163,184,0.6);">PulseCities</span></a>\n'
        '    <div style="display:flex;align-items:center;gap:16px;">'
        '<a href="/this-week" style="font-size:0.78rem;color:rgba(148,163,184,0.5);">This week</a>'
        '<a href="/map" style="font-size:0.78rem;color:rgba(148,163,184,0.5);">Map</a>'
        '<a href="/neighborhoods" style="font-size:0.78rem;color:rgba(148,163,184,0.5);">Neighborhoods</a>'
        '<a href="/methodology" style="font-size:0.78rem;color:rgba(148,163,184,0.5);">Methodology</a>'
        '</div>\n  </div>\n</nav>'
    )




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
        f"{counts.evictions:,} eviction filings, {counts.permits:,} construction permits. Public records only."
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
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>{_WEEK_CSS}</style>
</head>
<body>
{_week_nav_html()}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/this-week/archive" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; Weekly review archive</a>
  </div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;font-weight:600;margin-bottom:6px;">NYC displacement, week of {e(range_label)}</h1>
  <p style="font-size:0.82rem;color:#94a3b8;margin-bottom:8px;line-height:1.6;">
    Where displacement pressure moved that week, from the public records behind the map. Reconstructed from the score history and agency filings dated in this window.
  </p>

  <h2>Score movers</h2>
  <p style="font-size:0.75rem;color:rgba(148,163,184,0.6);margin-bottom:4px;">Largest displacement-pressure increases over the week.</p>
  <ul class="tw-list">
{_movers_rows_html(movers, e)}  </ul>

  <h2>New on the record</h2>
  <p style="font-size:0.75rem;color:rgba(148,163,184,0.6);">Citywide filings dated within this week.</p>
  <div class="tw-stats">{_stat_cells_html(counts)}</div>

  {wk_nav}

  <p style="font-size:0.72rem;color:rgba(148,163,184,0.45);margin-top:28px;line-height:1.6;">
    Counts reflect records published by NYC agencies, which can lag the events they describe. Scores are risk indicators, not claims of wrongdoing. <a href="/methodology" style="color:rgba(249,115,22,0.75);">How scores work &rarr;</a>
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
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"></noscript>
<style>{_WEEK_CSS}</style>
</head>
<body>
{_week_nav_html()}
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/this-week" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; This week</a>
  </div>
  <h1 style="font-family:'Bricolage Grotesque','DM Sans',sans-serif;font-size:1.5rem;font-weight:600;margin-bottom:6px;">Weekly review archive</h1>
  <p style="font-size:0.82rem;color:#94a3b8;margin-bottom:20px;line-height:1.6;">
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
<link rel="canonical" href="https://pulsecities.com/this-week">{_PLAUSIBLE}
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
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
@media(max-width:600px){{.nav-inner{{flex-wrap:wrap;row-gap:4px}}.nav-inner>div{{flex-wrap:wrap;row-gap:4px}}}}
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
    The week's movement across all NYC neighborhoods, from the same public records that drive the map. This page always shows the current week. <a href="/this-week/archive" style="color:rgba(249,115,22,0.8);">Past weeks &rarr;</a>
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
{_FOOTER_HTML}
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

    arcs = sorted(_approved_flip_arcs(), key=lambda a: a.get("gain_pct", 0), reverse=True)
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
        addr = esc((a.get("address") or f"BBL {a.get('bbl')}").title())
        zc = esc(str(a.get("zip_code") or ""))
        line = (
            f"Evicted {_my(a.get('eviction_date'))} &middot; bought {_my(a.get('buy_date'))} "
            f"for {_m(a.get('buy_amt'))} &middot; sold {_my(a.get('sell_date'))} for {_m(a.get('sell_amt'))}"
        )
        gain = int(a.get("gain_pct", 0))
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
            f'<span class="row-name">{name}<span class="row-sub">{esc(" &middot; ".join(meta))}</span></span>'
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
        span_txt = f"{span} days" if span is not None else "recent"
        amt = f" &middot; {_m(c['total_amount'])}" if c.get("total_amount") else ""
        cl_items += (
            f'<li class="row" onclick="location.href=\'/radar\'">'
            f'<a href="/radar">'
            f'<span class="row-name">{buyer}'
            f'<span class="row-sub">{c["building_count"]} buildings in {hood} ({zc}) over {span_txt}{amt}</span>'
            f'</span><span class="row-arrow">&rarr;</span>'
            f'</a></li>'
        )
    if not cl_items:
        cl_items = '<li class="empty">No active clusters in the current window.</li>'

    title = "The State of NYC Displacement | PulseCities"
    desc = (
        "A live read of NYC displacement pressure, rebuilt nightly from public records: "
        "hottest neighborhoods, largest landlords, eviction-to-resale flips, and buying clusters."
    )
    jsonld = _jsonld({
        "@context": "https://schema.org",
        "@type": "CollectionPage",
        "name": "The State of NYC Displacement",
        "description": desc,
        "url": "https://pulsecities.com/displacement",
        "isPartOf": {"@type": "WebSite", "name": "PulseCities", "url": "https://pulsecities.com"},
        "dateModified": date.today().isoformat(),
    })

    head = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{esc(title)}</title>
<meta name="description" content="{esc(desc)}">
<link rel="canonical" href="https://pulsecities.com/displacement">
<meta property="og:title" content="{esc(title)}">
<meta property="og:description" content="{esc(desc)}">
<meta property="og:url" content="https://pulsecities.com/displacement">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{esc(title)}">
<meta name="twitter:description" content="{esc(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%231a1a2e'/%3E%3Cpolyline points='2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16' fill='none' stroke='%23f97316' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<script type="application/ld+json">{jsonld}</script>{_PLAUSIBLE}
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600;12..96,700&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;600&display=swap" as="style" onload="this.onload=null;this.rel='stylesheet'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600;12..96,700&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;600&display=swap"></noscript>
"""

    css = """<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
nav{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0;position:sticky;top:0;background:rgba(15,23,42,0.92);backdrop-filter:blur(8px);z-index:5}
.nav-inner{max-width:900px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}
.brand{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;letter-spacing:-0.01em;color:#f1f5f9}
.nav-links{display:flex;gap:18px;font-size:0.82rem;color:#94a3b8;flex-wrap:wrap}
.nav-links a:hover{color:#f97316}
.wrap{max-width:900px;margin:0 auto;padding:40px 20px 72px}
.eyebrow{font-family:'JetBrains Mono',monospace;font-size:0.72rem;letter-spacing:0.18em;color:#f97316;text-transform:uppercase;margin-bottom:14px}
h1{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:clamp(2rem,5.4vw,3.1rem);line-height:1.04;letter-spacing:-0.02em;margin-bottom:14px}
.lede{font-size:1.05rem;color:#94a3b8;max-width:620px;line-height:1.5}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:34px 0 8px}
.stat{border:1px solid rgba(148,163,184,0.12);border-radius:12px;padding:16px 14px;background:rgba(148,163,184,0.03)}
.stat-num{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:1.9rem;line-height:1;color:#f1f5f9}
.stat-unit{font-size:0.9rem;color:#64748b;font-weight:600}
.stat-label{font-size:0.72rem;color:#94a3b8;margin-top:8px;line-height:1.3}
@media(max-width:640px){.stats{grid-template-columns:repeat(2,1fr)}}
.section{margin-top:44px}
.sec-h{font-family:'Bricolage Grotesque',sans-serif;font-weight:600;font-size:1.3rem;letter-spacing:-0.01em;display:flex;align-items:baseline;justify-content:space-between;gap:12px}
.sec-more{font-family:'JetBrains Mono',monospace;font-size:0.74rem;color:#f97316;white-space:nowrap}
.sec-more:hover{text-decoration:underline}
.sec-sub{font-size:0.86rem;color:#64748b;margin-top:5px;margin-bottom:14px;max-width:640px;line-height:1.45}
ul{list-style:none}
.arc{border-bottom:1px solid rgba(148,163,184,0.08);cursor:pointer}
.arc:hover{background:rgba(148,163,184,0.04)}
.arc a{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:15px 0}
.arc-addr{font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#e2e8f0;font-weight:600;letter-spacing:0.02em}
.arc:hover .arc-addr{color:#f97316}
.arc-sub{font-size:0.74rem;color:#64748b;margin-top:2px}
.arc-line{font-size:0.78rem;color:#94a3b8;margin-top:5px;line-height:1.4}
.arc-gain{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:1.35rem;color:#ef4444;white-space:nowrap}
.row{border-bottom:1px solid rgba(148,163,184,0.08);cursor:pointer}
.row:hover{background:rgba(148,163,184,0.04)}
.row a{display:flex;align-items:center;gap:14px;padding:13px 0}
.rank{font-family:'JetBrains Mono',monospace;font-size:0.8rem;color:#64748b;min-width:26px}
.row-name{flex:1;min-width:0;font-size:0.92rem;color:#e2e8f0;font-weight:500}
.row:hover .row-name{color:#f97316}
.row-sub{display:block;font-size:0.74rem;color:#64748b;font-weight:400;margin-top:2px}
.row-val{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:1.15rem;display:flex;flex-direction:column;align-items:flex-end;line-height:1}
.row-tier{font-family:'DM Sans',sans-serif;font-size:0.62rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;margin-top:4px;opacity:0.85}
.row-arrow{color:#475569;font-size:1.1rem}
.row:hover .row-arrow{color:#f97316}
.empty{padding:16px 0;color:#64748b;font-size:0.86rem}
.cta{margin-top:52px;border:1px solid rgba(249,115,22,0.25);border-radius:14px;padding:26px 22px;background:rgba(249,115,22,0.04);text-align:center}
.cta-h{font-family:'Bricolage Grotesque',sans-serif;font-weight:600;font-size:1.2rem;margin-bottom:6px}
.cta-sub{font-size:0.88rem;color:#94a3b8;margin-bottom:16px}
.cta-btns{display:flex;gap:12px;justify-content:center;flex-wrap:wrap}
.btn{padding:10px 20px;border-radius:8px;font-size:0.88rem;font-weight:600}
.btn-primary{background:#f97316;color:#0f172a}
.btn-primary:hover{background:#fb8c3a}
.btn-secondary{border:1px solid rgba(148,163,184,0.25);color:#e2e8f0}
.btn-secondary:hover{border-color:#f97316;color:#f97316}
.note{margin-top:26px;font-size:0.72rem;color:#475569;line-height:1.5;text-align:center}
.note a{color:rgba(249,115,22,0.75)}
footer{text-align:center;padding:24px 16px calc(env(safe-area-inset-bottom,0px) + 24px);border-top:1px solid rgba(148,163,184,0.08);margin-top:40px;font-size:12px;color:#64748b}
.footer-links{display:flex;justify-content:center;gap:20px;flex-wrap:wrap}
</style>
"""

    body = f"""</head>
<body>
<nav><div class="nav-inner">
<a href="/" class="brand">PulseCities</a>
<div class="nav-links">
<a href="/flips" onclick="plausible('Showcase Nav',{{props:{{to:'flips'}}}})">Flips</a>
<a href="/radar" onclick="plausible('Showcase Nav',{{props:{{to:'radar'}}}})">Radar</a>
<a href="/operators" onclick="plausible('Showcase Nav',{{props:{{to:'operators'}}}})">Landlords</a>
<a href="/neighborhoods" onclick="plausible('Showcase Nav',{{props:{{to:'neighborhoods'}}}})">Neighborhoods</a>
<a href="/map" onclick="plausible('Showcase Nav',{{props:{{to:'map'}}}})">Map</a>
</div>
</div></nav>
<div class="wrap">
<div class="eyebrow">PulseCities &middot; Citywide &middot; NYC public records</div>
<h1>The State of NYC Displacement</h1>
<p class="lede">What the public record shows right now. Every number below is rebuilt nightly from NYC open data: deeds, evictions, permits, violations, and complaints.</p>

<div class="stats">{stats_html}</div>

<div class="section">
<div class="sec-h">Evicted, then flipped <a class="sec-more" href="/flips/editions" onclick="plausible('Showcase Section',{{props:{{sec:'arcs'}}}})">All editions &rarr;</a></div>
<div class="sec-sub">Buildings where tenants were evicted, an LLC bought in, and the building resold at a markup within a year. Reviewed before listing. Every step is a public deed.</div>
<ul>{arc_items}</ul>
</div>

<div class="section">
<div class="sec-h">Highest pressure this week <a class="sec-more" href="/neighborhoods" onclick="plausible('Showcase Section',{{props:{{sec:'hot'}}}})">All neighborhoods &rarr;</a></div>
<div class="sec-sub">The neighborhoods with the strongest combined displacement signals across {n_hoods} scored ZIP codes.</div>
<ul>{hot_items}</ul>
</div>

<div class="section">
<div class="sec-h">The largest landlords <a class="sec-more" href="/operators" onclick="plausible('Showcase Section',{{props:{{sec:'operators'}}}})">All landlords &rarr;</a></div>
<div class="sec-sub">Owner networks with the most acquisitions across the city, resolved from ACRIS deeds through their LLC shells.</div>
<ul>{op_items}</ul>
</div>

<div class="section">
<div class="sec-h">Buying clusters <a class="sec-more" href="/radar" onclick="plausible('Showcase Section',{{props:{{sec:'radar'}}}})">Speculation radar &rarr;</a></div>
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
