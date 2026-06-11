"""
Classify every operator cluster into the public taxonomy and write the result
back to operators.operator_class, then emit a human-readable audit.

Run:
    PYTHONPATH=. venv/bin/python scripts/classify_operators.py

Only clusters classified 'operator' are shown on public surfaces (see
api/routes/operators.py and api/routes/frontend.py). Everything else
(financial_institution / government / nonprofit_hdfc / unclassified) is screened
out. The audit at docs/operator_classification_audit.md records the class and
the reasons for each cluster so a human can see what was screened and why.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db
from scoring.operator_classification import (
    classify_operator,
    OPERATOR,
    _has_entity_structure,
)

_AUDIT_PATH = Path(__file__).parent.parent / "docs" / "operator_classification_audit.md"

# Confidence by primary reason, for operators.classification_confidence.
_CONFIDENCE = {
    "allowlist": 1.0,
    "known_finance_cluster": 0.90,
    "behavioral_majority_nominal": 0.90,
    "behavioral_single_non_llc_entity": 0.88,
    "acquisition_operator": 0.70,
    "insufficient_signal": 0.40,
}

_CLASS_LABEL = {
    "operator": "Operator",
    "financial_institution": "Financial institution",
    "government": "Government",
    "nonprofit_hdfc": "Nonprofit / HDFC",
    "unclassified": "Unclassified",
}


def _gather_stats(db, operator_id: int, llc_entities: list) -> tuple[dict, list]:
    """Behavioral stats and entity-name list for one operator, from operator_parcels."""
    rows = db.execute(
        text("""
            SELECT acquiring_entity, acquisition_price
            FROM operator_parcels
            WHERE operator_id = :id
        """),
        {"id": operator_id},
    ).fetchall()

    acq_count = len(rows)
    prop_count = db.execute(
        text("SELECT COUNT(DISTINCT bbl) FROM operator_parcels WHERE operator_id = :id"),
        {"id": operator_id},
    ).scalar() or 0

    nominal = sum(
        1 for r in rows
        if r.acquisition_price is None or r.acquisition_price < 100
    )
    nominal_ratio = (nominal / acq_count) if acq_count else None

    # Dominant acquiring entity and whether it carries an LLC/Corp suffix.
    counts: dict[str, int] = {}
    for r in rows:
        name = (r.acquiring_entity or "").strip()
        if name:
            counts[name] = counts.get(name, 0) + 1
    dominant_share = None
    dominant_is_llc = None
    dominant_name = None
    if counts and acq_count:
        dominant_name, dom_n = max(counts.items(), key=lambda kv: kv[1])
        dominant_share = dom_n / acq_count
        dominant_is_llc = _has_entity_structure(dominant_name.upper())

    stats = {
        "acquisition_count": acq_count,
        "property_count": prop_count,
        "nominal_ratio": nominal_ratio,
        "dominant_entity_share": dominant_share,
        "dominant_entity_is_llc": dominant_is_llc,
    }
    entity_names = list(llc_entities or []) + list(counts.keys())
    return stats, entity_names, dominant_name


def main() -> None:
    results = []
    with get_scraper_db() as db:
        operators = db.execute(text("""
            SELECT id, operator_root, slug, display_name, llc_entities,
                   total_acquisitions, total_properties
            FROM operators
            ORDER BY total_properties DESC
        """)).fetchall()

        now = datetime.now(timezone.utc)
        for op in operators:
            stats, entity_names, dominant = _gather_stats(db, op.id, op.llc_entities)
            klass, reasons = classify_operator(op.operator_root, entity_names, stats)
            confidence = max((_CONFIDENCE.get(r, 0.6) for r in reasons), default=0.6)

            db.execute(
                text("""
                    UPDATE operators
                    SET operator_class = :klass,
                        classification_reasons = CAST(:reasons AS jsonb),
                        classification_confidence = :conf,
                        classified_at = :ts
                    WHERE id = :id
                """),
                {
                    "klass": klass,
                    "reasons": json.dumps(reasons),
                    "conf": confidence,
                    "ts": now,
                    "id": op.id,
                },
            )
            results.append({
                "root": op.operator_root,
                "slug": op.slug,
                "class": klass,
                "reasons": reasons,
                "dominant_entity": dominant,
                "nominal_ratio": stats["nominal_ratio"],
                "dominant_share": stats["dominant_entity_share"],
                "acq": stats["acquisition_count"],
                "props": stats["property_count"],
            })
        # get_scraper_db commits on clean exit.

    _write_audit(results)
    shown = sum(1 for r in results if r["class"] == OPERATOR)
    print(f"Classified {len(results)} operators. {shown} shown publicly (class=operator).")
    for r in results:
        print(f"  {r['root']:14} -> {r['class']:22} {','.join(r['reasons'])}")


def _pct(x) -> str:
    return f"{x:.0%}" if x is not None else "n/a"


def _write_audit(results: list) -> None:
    _AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    shown = [r for r in results if r["class"] == OPERATOR]
    screened = [r for r in results if r["class"] != OPERATOR]

    lines = []
    lines.append("# Operator classification audit")
    lines.append("")
    lines.append(
        "Generated by `scripts/classify_operators.py`. Only clusters classed "
        "`operator` appear on public surfaces (`/api/operators`, the operators "
        "directory, and operator profile pages). Every other class is screened out."
    )
    lines.append("")
    lines.append(f"- Total clusters: {len(results)}")
    lines.append(f"- Shown publicly (operator): {len(shown)}")
    lines.append(f"- Screened out: {len(screened)}")
    lines.append("")
    lines.append("## All clusters")
    lines.append("")
    lines.append("| Root | Class | Reasons | Nominal $ share | Dominant entity (share) | Acq | Props |")
    lines.append("|------|-------|---------|-----------------|-------------------------|-----|-------|")
    for r in sorted(results, key=lambda x: (x["class"] != "operator", x["class"], -x["acq"])):
        dom = f"{r['dominant_entity'] or 'n/a'} ({_pct(r['dominant_share'])})"
        lines.append(
            f"| {r['root']} | {r['class']} | {', '.join(r['reasons'])} | "
            f"{_pct(r['nominal_ratio'])} | {dom} | {r['acq']} | {r['props']} |"
        )
    lines.append("")
    lines.append("## Screened-out institutional clusters")
    lines.append("")
    lines.append(
        "These previously appeared (or could appear) as operators. They acquire via "
        "nominal/$0 consideration, a single institutional entity name, or carry a "
        "bank/servicer/GSE/government/HDFC name. RIDGEWOOD (RIDGEWOOD SAVINGS BANK) "
        "was the reported leak; the rows below are what else the gate caught."
    )
    lines.append("")
    for r in sorted(screened, key=lambda x: -x["acq"]):
        label = _CLASS_LABEL.get(r["class"], r["class"])
        lines.append(
            f"- **{r['root']}** -> {label}. Dominant entity {r['dominant_entity'] or 'n/a'} "
            f"({_pct(r['dominant_share'])} of acquisitions), {_pct(r['nominal_ratio'])} at "
            f"nominal/$0. Reasons: {', '.join(r['reasons'])}."
        )
    lines.append("")

    _AUDIT_PATH.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
