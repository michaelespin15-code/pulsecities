"""
AI paragraph for the weekly ZIP digest — the plain-English read of the week.

The digest already shows the numbers; this writes the sentence a housing
reporter would put above them, grounded strictly in the figures the email
shows. The one thing a template can't phrase is the week-versus-typical
comparison across signals, so that is exactly what the model is handed.

Discipline mirrors api/routes/ai_summary.py:
  - The model sees only figures already computed for the subscriber's ZIP.
  - Any failure (no key, SDK error, refusal, empty text) returns None and the
    email renders without the section. The digest never blocks on the model.
  - One call per ZIP per run. Subscribers watching the same ZIP share it, and
    a failed call is not retried within the run.

Cost: a handful of calls per Sunday at a few hundred tokens each. The cron
script is not latency-sensitive, so no cap beyond the per-run cache is needed.
"""

import logging
import os

logger = logging.getLogger(__name__)

# PULSE_AI_MODEL flips every narrative surface to a different model without a
# deploy — the cost lever if volume ever makes Opus pricing matter. Read at
# call time so a cron env change takes effect on the next run.
DEFAULT_MODEL = "claude-opus-4-8"
MAX_TOKENS = 300
REQUEST_TIMEOUT = 30.0  # cron context — patient, but never hang a send run


def _model() -> str:
    return os.getenv("PULSE_AI_MODEL", DEFAULT_MODEL)

_SYSTEM_PROMPT = (
    "You write one short, plain-English paragraph for a weekly email that tells a "
    "resident what changed in their neighborhood's displacement-pressure data this "
    "week. You are given this week's figures for one NYC ZIP code, including how "
    "each count compares to the area's typical week. Work strictly from those "
    "figures. Do not invent addresses, landlord names, dollar amounts, dates, or "
    "any number you were not given, and do not speculate about causes or intent. "
    "Lead with the change that matters most, and when a count is well above the "
    "typical week, say so in everyday terms. Two to three sentences. Write like an "
    "experienced housing reporter: direct, specific, no hedging filler and no "
    "marketing tone. Never use an em dash; write two sentences or rephrase instead. "
    "Output only the paragraph: no preamble, no heading, no bullet points."
)

_SIGNAL_LABELS = {
    "permit_intensity":     "renovation permits",
    "eviction_rate":        "eviction filings",
    "llc_acquisition_rate": "LLC acquisitions",
    "complaint_rate":       "tenant complaints",
    "rs_unit_loss":         "rent-stabilized unit loss",
    "hpd_violations":       "housing code violations",
}

# One entry per ZIP per process run; the cron script is one-shot so this never
# goes stale. Failures are cached as None so a dead network costs one call.
_cache: dict[str, str | None] = {}

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    if not os.getenv("ANTHROPIC_API_KEY"):
        return None
    import anthropic
    _client = anthropic.Anthropic(timeout=REQUEST_TIMEOUT, max_retries=1)
    return _client


def _avg_text(avg: float | None) -> str:
    return f"{avg:.1f}" if avg is not None else "unknown"


def build_facts(summary: dict, reasons: list[str]) -> str:
    """Render the grounding numbers as a compact, unambiguous block for the model."""
    lines = [
        f"Neighborhood: {summary['name']}",
        f"ZIP: {summary['zip']}",
        f"Composite displacement-pressure score this week: {summary['score_now']:.1f} out of 100",
        f"Score last week: {summary['score_prev']:.1f} (change: {summary['delta']:+.1f})",
        "",
        "Public records newly published for this area in the past 7 days, each against",
        "the average week of newly published records over the prior 8 weeks. These counts",
        "reflect when records reached the public feeds, not when the events occurred, so",
        "describe them as records added, never as events happening or activity rising or falling:",
        f"  - residential eviction records: {summary['eviction_count']} added this week vs {_avg_text(summary['eviction_avg'])} in a typical week",
        f"  - LLC property acquisition records: {summary['llc_count']} added this week vs no baseline tracked",
        f"  - renovation permit records, 3+ unit buildings: {summary['permit_count']} added this week vs {_avg_text(summary['permit_avg'])} in a typical week",
        f"  - class B/C housing code violation records: {summary['hpd_count']} added this week vs {_avg_text(summary['hpd_avg'])} in a typical week",
        f"  - displacement-related tenant complaint records: {summary['complaint_count']} added this week vs {_avg_text(summary['complaint_avg'])} in a typical week",
    ]
    if summary.get("elevated"):
        lines.append("")
        lines.append("Signals most elevated in the composite score (0 to 100):")
        for key, value in summary["elevated"]:
            lines.append(f"  - {_SIGNAL_LABELS.get(key, key)}: {value:.1f}")
    if reasons:
        lines.append("")
        lines.append("Why this week triggered an update:")
        for reason in reasons:
            lines.append(f"  - {reason}")
    return "\n".join(lines)


def _generate(cache_key: str, system_prompt: str, facts: str) -> str | None:
    """One cached model call; any failure degrades to None, never an exception."""
    if cache_key in _cache:
        return _cache[cache_key]

    client = _get_client()
    if client is None:
        return None

    _cache[cache_key] = None  # a failure below still counts as this run's attempt
    try:
        message = client.messages.create(
            model=_model(),
            max_tokens=MAX_TOKENS,
            system=system_prompt,
            messages=[{"role": "user", "content": facts}],
        )
    except Exception as exc:  # noqa: BLE001 — the email must go out regardless
        logger.warning("digest narrative failed for %s: %r", cache_key, exc)
        return None

    if message.stop_reason == "refusal":
        logger.warning("digest narrative refused for %s", cache_key)
        return None

    usage = getattr(message, "usage", None)
    if usage is not None:
        # Spend visibility: grep the cron log for 'narrative usage' to audit
        # actual token consumption instead of estimating.
        logger.info("narrative usage key=%s in=%s out=%s model=%s",
                    cache_key, usage.input_tokens, usage.output_tokens, _model())

    text = "".join(b.text for b in message.content if b.type == "text").strip()
    _cache[cache_key] = text or None
    return _cache[cache_key]


def generate_narrative(summary: dict, reasons: list[str]) -> str | None:
    """Plain-English paragraph for one ZIP's week, or None when unavailable."""
    return _generate(summary["zip"], _SYSTEM_PROMPT, build_facts(summary, reasons))


_CITYWIDE_SYSTEM_PROMPT = (
    "You write one short, plain-English paragraph for a weekly email that tells a "
    "reader what changed in New York City's displacement-pressure data this week. "
    "You are given the citywide figures: the neighborhoods whose scores moved most, "
    "this week's public-record counts against a typical week, and any concentrated "
    "LLC buying detected. Work strictly from those figures. Do not invent addresses, "
    "landlord names, dollar amounts, dates, or any number you were not given, and do "
    "not speculate about causes or intent. Lead with the change that matters most "
    "citywide. Two to three sentences. Write like an experienced housing reporter: "
    "direct, specific, no hedging filler and no marketing tone. Never use an em "
    "dash; write two sentences or rephrase instead. Output only the paragraph: no "
    "preamble, no heading, no bullet points."
)


def build_citywide_facts(summary: dict) -> str:
    """Render the citywide grounding numbers for the model."""
    lines = [
        f"City: New York City ({summary['zip_count']} neighborhoods tracked)",
        f"Average displacement-pressure score: {summary['avg_score']:.1f} out of 100",
        f"Highest neighborhood score: {summary['max_score']:.1f}",
    ]
    if summary.get("movers"):
        lines.append("")
        lines.append("Neighborhoods whose scores moved most over the past 7 days:")
        for m in summary["movers"]:
            lines.append(f"  - {m['name']} ({m['zip']}): {m['delta']:+.1f} points, now {m['score']:.1f}")
    week = summary.get("week") or {}
    if week:
        lines.append("")
        lines.append("Citywide public records newly published in the past 7 days, each against the")
        lines.append("average week of newly published records over the prior 8 weeks. These counts")
        lines.append("reflect when records reached the public feeds, not when the events occurred, so")
        lines.append("describe them as records added, never as events happening or activity rising or falling:")
        lines.append(f"  - residential eviction records: {week['evictions']} added vs {week['evictions_avg']:.1f} typical")
        lines.append(f"  - LLC property acquisition records: {week['llc']} added vs {week['llc_avg']:.1f} typical")
        lines.append(f"  - renovation permit records, 3+ unit buildings: {week['permits']} added vs {week['permits_avg']:.1f} typical")
        lines.append(f"  - class B/C housing code violation records: {week['violations']} added vs {week['violations_avg']:.1f} typical")
    if summary.get("clusters"):
        lines.append("")
        lines.append("Concentrated LLC buying detected (one buyer, 3+ buildings, one ZIP, past 90 days, with deed activity this week):")
        for c in summary["clusters"]:
            amount = f", ${c['total_amount']:,.0f} total" if c.get("total_amount") else ""
            lines.append(
                f"  - {c['buyer']}: {c['building_count']} buildings in "
                f"{c.get('neighborhood') or c['zip_code']}{amount}"
            )
    return "\n".join(lines)


def generate_citywide_narrative(summary: dict) -> str | None:
    """Plain-English paragraph for the city's week, or None when unavailable."""
    return _generate("citywide", _CITYWIDE_SYSTEM_PROMPT, build_citywide_facts(summary))
