"""
Recent changes report for PulseCities operator/founder use.

Surfaces meaningful new data from the last few days so you can spot
post-worthy or journalist-worthy changes without digging through raw tables.

Usage:
    python scripts/recent_changes_report.py           # default 7-day window
    python scripts/recent_changes_report.py --days 3  # shorter window

Read-only. No DB writes.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

# Score tier thresholds — kept in sync with api/routes/neighborhoods.py
TIER_HIGH = 56
TIER_CRITICAL = 76


def _db():
    from models.database import SessionLocal
    return SessionLocal()


def _divider(title: str) -> None:
    width = 72
    print()
    print(title.upper())
    print("-" * width)


def section_score_movers(db, days: int) -> None:
    _divider(f"Score movers (last {days} days)")

    rows = db.execute(text("""
        WITH latest AS (
            SELECT zip_code, composite_score AS score, scored_at
            FROM score_history
            WHERE scored_at = (
                SELECT MAX(h2.scored_at) FROM score_history h2
                WHERE h2.zip_code = score_history.zip_code
            )
        ),
        prior AS (
            SELECT zip_code, composite_score AS score, scored_at
            FROM score_history
            WHERE scored_at <= NOW() - INTERVAL '1 day' * :days
              AND scored_at = (
                SELECT MAX(h2.scored_at) FROM score_history h2
                WHERE h2.zip_code = score_history.zip_code
                  AND h2.scored_at <= NOW() - INTERVAL '1 day' * :days
            )
        )
        SELECT
            l.zip_code,
            n.name,
            n.borough,
            ROUND(p.score::numeric, 1) AS prev_score,
            ROUND(l.score::numeric, 1) AS latest_score,
            ROUND((l.score - p.score)::numeric, 1) AS delta
        FROM latest l
        JOIN prior p ON p.zip_code = l.zip_code
        LEFT JOIN neighborhoods n ON n.zip_code = l.zip_code
        WHERE ABS(l.score - p.score) >= 1.0
        ORDER BY delta DESC
    """), {"days": days}).fetchall()

    if not rows:
        print("No score changes >= 1.0 points found.")
        return

    increases = [r for r in rows if r.delta > 0]
    decreases = [r for r in rows if r.delta < 0]

    def _label(r):
        name = r.name or "Unknown"
        boro = f" ({r.borough})" if r.borough else ""
        return f"{r.zip_code}  {name}{boro}"

    if increases:
        print(f"Top increases ({min(len(increases), 10)} of {len(increases)}):")
        print(f"  {'ZIP / Name':<40} {'Prev':>6} {'Now':>6} {'Delta':>7}")
        for r in increases[:10]:
            print(f"  {_label(r):<40} {r.prev_score:>6.1f} {r.latest_score:>6.1f} {r.delta:>+7.1f}")

    if decreases:
        print()
        print(f"Top decreases ({min(len(decreases), 10)} of {len(decreases)}):")
        print(f"  {'ZIP / Name':<40} {'Prev':>6} {'Now':>6} {'Delta':>7}")
        for r in sorted(decreases, key=lambda r: r.delta)[:10]:
            print(f"  {_label(r):<40} {r.prev_score:>6.1f} {r.latest_score:>6.1f} {r.delta:>+7.1f}")


def section_new_high_risk(db, days: int) -> None:
    _divider(f"New high-risk ZIPs (crossed >= {TIER_HIGH} in last {days} days)")

    rows = db.execute(text("""
        WITH latest AS (
            SELECT zip_code, composite_score AS score
            FROM score_history
            WHERE scored_at = (
                SELECT MAX(h2.scored_at) FROM score_history h2
                WHERE h2.zip_code = score_history.zip_code
            )
        ),
        prior AS (
            SELECT zip_code, composite_score AS score
            FROM score_history
            WHERE scored_at <= NOW() - INTERVAL '1 day' * :days
              AND scored_at = (
                SELECT MAX(h2.scored_at) FROM score_history h2
                WHERE h2.zip_code = score_history.zip_code
                  AND h2.scored_at <= NOW() - INTERVAL '1 day' * :days
              )
        )
        SELECT l.zip_code, n.name, n.borough,
               ROUND(p.score::numeric, 1) AS prev_score,
               ROUND(l.score::numeric, 1) AS now_score
        FROM latest l
        JOIN prior p ON p.zip_code = l.zip_code
        LEFT JOIN neighborhoods n ON n.zip_code = l.zip_code
        WHERE l.score >= :threshold
          AND p.score < :threshold
        ORDER BY l.score DESC
    """), {"days": days, "threshold": TIER_HIGH}).fetchall()

    if not rows:
        print("No ZIPs crossed the high-risk threshold in this window.")
        return

    print(f"  {'ZIP':<8} {'Name':<30} {'Borough':<12} {'Before':>7} {'Now':>7}")
    for r in rows:
        tier = "CRITICAL" if r.now_score >= TIER_CRITICAL else "HIGH"
        name = (r.name or "")[:28]
        boro = (r.borough or "")[:10]
        print(f"  {r.zip_code:<8} {name:<30} {boro:<12} {r.prev_score:>7.1f} {r.now_score:>7.1f}  [{tier}]")


def section_operator_activity(db, days: int) -> None:
    _divider(f"Operator activity (new parcels linked in last {days} days)")

    rows = db.execute(text("""
        SELECT
            o.display_name,
            o.slug,
            COUNT(op.bbl) AS new_parcels,
            ARRAY_AGG(DISTINCT p.zip_code ORDER BY p.zip_code) FILTER (WHERE p.zip_code IS NOT NULL) AS zip_codes
        FROM operator_parcels op
        JOIN operators o ON o.id = op.operator_id
        LEFT JOIN parcels p ON p.bbl = op.bbl
        WHERE op.acquisition_date >= CURRENT_DATE - (:days || ' days')::interval
        GROUP BY o.display_name, o.slug
        ORDER BY new_parcels DESC
        LIMIT 15
    """), {"days": days}).fetchall()

    if not rows:
        print("No new operator-parcel links in this window.")
        return

    print(f"  {'Operator':<30} {'New parcels':>12}  Top ZIPs")
    for r in rows:
        zips = ", ".join((r.zip_codes or [])[:5])
        if r.zip_codes and len(r.zip_codes) > 5:
            zips += f" (+{len(r.zip_codes) - 5} more)"
        print(f"  {r.display_name:<30} {r.new_parcels:>12}  {zips}")


def section_recent_raw_events(db, days: int) -> None:
    window = min(days, 3)
    _divider(f"Recent raw events (last {window} days, high-risk ZIPs first)")

    # High-risk ZIP set for prioritization
    high_risk = db.execute(text("""
        SELECT zip_code FROM displacement_scores WHERE score >= :t ORDER BY score DESC
    """), {"t": TIER_HIGH}).fetchall()
    hr_zips = {r.zip_code for r in high_risk}

    def _print_table(label, rows, col_names, col_getters, empty_msg=None):
        print(f"\n{label}:")
        if not rows:
            print(f"  {empty_msg or 'No recent records found.'}")
            return
        header = "  " + "  ".join(f"{c:<{w}}" for c, (_, w) in zip(col_names, col_getters))
        print(header)
        for row in rows[:10]:
            line = "  " + "  ".join(
                f"{str(g(row))[:w]:<{w}}" for g, w in col_getters
            )
            hr_flag = "  *" if getattr(row, 'zip_code', None) in hr_zips else ""
            print(line + hr_flag)
        if len(rows) > 10:
            print(f"  ... and {len(rows) - 10} more")

    # Permits
    permits = db.execute(text("""
        SELECT p.zip_code, p.permit_type, p.address, p.filing_date,
               ds.score
        FROM permits_raw p
        LEFT JOIN displacement_scores ds ON ds.zip_code = p.zip_code
        WHERE p.filing_date >= CURRENT_DATE - :w * INTERVAL '1 day'
          AND p.permit_type IN ('A1', 'A2', 'NB')
        ORDER BY (ds.score IS NOT NULL) DESC, ds.score DESC NULLS LAST,
                 p.filing_date DESC
        LIMIT 50
    """), {"w": window}).fetchall()
    _print_table("Permits (A1/A2/NB)", permits,
        ["ZIP", "Type", "Address", "Filed"],
        [(lambda r: r.zip_code, 6),
         (lambda r: r.permit_type, 5),
         (lambda r: (r.address or "")[:35], 37),
         (lambda r: r.filing_date, 11)])

    # LLC transfers — uses full days window, not the 3-day cap, because ACRIS
    # deed records lag 2-6 weeks so short windows always return empty.
    llc = db.execute(text("""
        SELECT p.zip_code, o.party_name_normalized AS buyer, o.doc_date,
               o.doc_amount, ds.score
        FROM ownership_raw o
        JOIN parcels p ON p.bbl = o.bbl
        LEFT JOIN displacement_scores ds ON ds.zip_code = p.zip_code
        WHERE o.party_type = '2'
          AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
          AND o.party_name_normalized ILIKE '%LLC%'
          AND o.doc_date >= CURRENT_DATE - :days * INTERVAL '1 day'
        ORDER BY (ds.score IS NOT NULL) DESC, ds.score DESC NULLS LAST,
                 o.doc_date DESC
        LIMIT 50
    """), {"days": days}).fetchall()
    _print_table(f"LLC transfers (last {days} days)", llc,
        ["ZIP", "Buyer", "Date", "Amount"],
        [(lambda r: r.zip_code, 6),
         (lambda r: (r.buyer or "")[:30], 32),
         (lambda r: r.doc_date, 11),
         (lambda r: f"${int(r.doc_amount):,}" if r.doc_amount else "N/A", 12)],
        empty_msg=f"No records in last {days} days. ACRIS deed records lag 2-6 weeks; try --days 30.")

    # HPD violations
    viols = db.execute(text("""
        SELECT v.zip_code, v.violation_class, v.address, v.nov_issued_date,
               ds.score
        FROM violations_raw v
        LEFT JOIN displacement_scores ds ON ds.zip_code = v.zip_code
        WHERE v.nov_issued_date >= CURRENT_DATE - :w * INTERVAL '1 day'
        ORDER BY (ds.score IS NOT NULL) DESC, ds.score DESC NULLS LAST,
                 v.nov_issued_date DESC
        LIMIT 50
    """), {"w": window}).fetchall()
    _print_table("HPD violations", viols,
        ["ZIP", "Class", "Address", "Issued"],
        [(lambda r: r.zip_code, 6),
         (lambda r: r.violation_class or "?", 6),
         (lambda r: (r.address or "")[:35], 37),
         (lambda r: r.nov_issued_date, 11)])

    # Evictions
    evics = db.execute(text("""
        SELECT e.zip_code, e.eviction_type, e.address, e.executed_date,
               ds.score
        FROM evictions_raw e
        LEFT JOIN displacement_scores ds ON ds.zip_code = e.zip_code
        WHERE e.executed_date >= CURRENT_DATE - :w * INTERVAL '1 day'
        ORDER BY (ds.score IS NOT NULL) DESC, ds.score DESC NULLS LAST,
                 e.executed_date DESC
        LIMIT 50
    """), {"w": window}).fetchall()
    _print_table("Evictions", evics,
        ["ZIP", "Type", "Address", "Date"],
        [(lambda r: r.zip_code, 6),
         (lambda r: (r.eviction_type or "")[:12], 14),
         (lambda r: (r.address or "")[:35], 37),
         (lambda r: r.executed_date, 11)],
        empty_msg="No recent records found. Eviction data currently appears to lag about 6 days.")

    # 311 complaints
    comps = db.execute(text("""
        SELECT c.zip_code, c.complaint_type, c.address, c.created_date,
               ds.score
        FROM complaints_raw c
        LEFT JOIN displacement_scores ds ON ds.zip_code = c.zip_code
        WHERE c.created_date >= CURRENT_DATE - :w * INTERVAL '1 day'
        ORDER BY (ds.score IS NOT NULL) DESC, ds.score DESC NULLS LAST,
                 c.created_date DESC
        LIMIT 50
    """), {"w": window}).fetchall()
    _print_table("311 complaints", comps,
        ["ZIP", "Type", "Address", "Date"],
        [(lambda r: r.zip_code, 6),
         (lambda r: (r.complaint_type or "")[:25], 27),
         (lambda r: (r.address or "")[:30], 32),
         (lambda r: str(r.created_date)[:10] if r.created_date else "N/A", 11)])

    if hr_zips:
        print(f"\n  * = high-risk ZIP (score >= {TIER_HIGH})")


def main():
    parser = argparse.ArgumentParser(description="PulseCities recent changes report")
    parser.add_argument("--days", type=int, default=7,
                        help="Lookback window in days (default: 7)")
    args = parser.parse_args()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"PulseCities recent changes report")
    print(f"Generated: {now}  |  Window: last {args.days} days")
    print("=" * 72)

    db = _db()
    try:
        section_score_movers(db, args.days)
        section_new_high_risk(db, args.days)
        section_operator_activity(db, args.days)
        section_recent_raw_events(db, args.days)
    finally:
        db.close()

    print()
    print("Data freshness notes:")
    print("  ACRIS:     2-6 week recording lag (deeds recorded after signing)")
    print("  Evictions: usually several days behind source publication")
    print()


if __name__ == "__main__":
    main()
