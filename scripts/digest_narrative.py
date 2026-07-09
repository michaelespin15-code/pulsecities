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

MODEL = "claude-opus-4-8"
MAX_TOKENS = 300
REQUEST_TIMEOUT = 30.0  # cron context — patient, but never hang a send run

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
        "This week's public-record counts, each against the area's average week over the prior 8 weeks:",
        f"  - residential evictions executed: {summary['eviction_count']} this week vs {_avg_text(summary['eviction_avg'])} in a typical week",
        f"  - LLC property acquisitions: {summary['llc_count']} this week vs no baseline tracked",
        f"  - renovation permits on 3+ unit buildings: {summary['permit_count']} this week vs {_avg_text(summary['permit_avg'])} in a typical week",
        f"  - class B/C housing code violations: {summary['hpd_count']} this week vs {_avg_text(summary['hpd_avg'])} in a typical week",
        f"  - displacement-related tenant complaints: {summary['complaint_count']} this week vs {_avg_text(summary['complaint_avg'])} in a typical week",
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


def generate_narrative(summary: dict, reasons: list[str]) -> str | None:
    """Plain-English paragraph for one ZIP's week, or None when unavailable."""
    zip_code = summary["zip"]
    if zip_code in _cache:
        return _cache[zip_code]

    client = _get_client()
    if client is None:
        return None

    _cache[zip_code] = None  # a failure below still counts as this run's attempt
    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": build_facts(summary, reasons)}],
        )
    except Exception as exc:  # noqa: BLE001 — the email must go out regardless
        logger.warning("digest narrative failed for %s: %r", zip_code, exc)
        return None

    if message.stop_reason == "refusal":
        logger.warning("digest narrative refused for %s", zip_code)
        return None

    text = "".join(b.text for b in message.content if b.type == "text").strip()
    _cache[zip_code] = text or None
    return _cache[zip_code]
