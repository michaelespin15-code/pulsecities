"""
Weekly content brief — live data from the PulseCities DB.

Usage:
    python scripts/weekly_content_brief.py [--days N] [--zip ZZZZZ]

Output: markdown to stdout. Redirect to a file yourself:
    python scripts/weekly_content_brief.py > /tmp/brief.md

Crontab (Sunday 8 PM ET):
    0 20 * * 0 cd /root/pulsecities && PYTHONPATH=/root/pulsecities \
        /root/pulsecities/venv/bin/python scripts/weekly_content_brief.py \
        > /var/log/pulsecities/weekly_brief_$(date +%%Y%%m%%d).md 2>&1
"""

import argparse
import sys
import traceback
from datetime import date, timedelta, datetime, timezone
from typing import Optional

from sqlalchemy import text

from models.database import get_scraper_db

_SIGNAL_LABELS = {
    "permit_intensity":     "DOB permits",
    "eviction_rate":        "evictions",
    "llc_acquisition_rate": "LLC transfers",
    "complaint_rate":       "311 complaints",
    "hpd_violations":       "HPD violations",
    "rs_unit_loss":         "RS unit loss",
}


def _top_signal(row) -> str:
    candidates = {
        k: getattr(row, k) or 0
        for k in _SIGNAL_LABELS
        if hasattr(row, k)
    }
    if not candidates:
        return "—"
    key = max(candidates, key=candidates.get)
    return f"{_SIGNAL_LABELS[key]} ({candidates[key]:.1f})"


def _dates(days: int):
    today = date.today()
    return {
        "cutoff":     today - timedelta(days=days),
        "prior_30d":  today - timedelta(days=days + 30),
        "divisor":    max(30.0 / days, 1.0),
    }


# ── Section 1: Score movers ──────────────────────────────────────────────────

def score_movers(db, days: int, zip_filter: Optional[str]) -> str:
    d = _dates(days)
    zip_clause = "AND l.zip_code = :zip_f" if zip_filter else ""
    params = {"cutoff": d["cutoff"], **({"zip_f": zip_filter} if zip_filter else {})}

    rows = db.execute(text(f"""
        WITH latest AS (
            SELECT DISTINCT ON (zip_code)
                   zip_code, composite_score, scored_at,
                   permit_intensity, eviction_rate, llc_acquisition_rate,
                   complaint_rate, hpd_violations, rs_unit_loss
            FROM score_history
            ORDER BY zip_code, scored_at DESC
        ),
        prior AS (
            SELECT DISTINCT ON (zip_code)
                   zip_code, composite_score, scored_at AS prior_date
            FROM score_history
            WHERE scored_at <= :cutoff
            ORDER BY zip_code, scored_at DESC
        ),
        residential AS (
            SELECT zip_code
            FROM parcels
            WHERE units_res IS NOT NULL AND zip_code IS NOT NULL
            GROUP BY zip_code
            HAVING SUM(units_res) >= 100
        )
        SELECT
            l.zip_code,
            n.name                                                         AS neighborhood,
            ROUND(p.composite_score::numeric, 1)                           AS prev_score,
            ROUND(l.composite_score::numeric, 1)                           AS curr_score,
            ROUND((l.composite_score - p.composite_score)::numeric, 1)    AS delta,
            l.permit_intensity, l.eviction_rate, l.llc_acquisition_rate,
            l.complaint_rate, l.hpd_violations, l.rs_unit_loss
        FROM latest l
        JOIN prior p USING (zip_code)
        JOIN residential r USING (zip_code)
        LEFT JOIN neighborhoods n USING (zip_code)
        {zip_clause}
        ORDER BY ABS(l.composite_score - p.composite_score) DESC
        LIMIT 5
    """), params).fetchall()

    lines = [f"## 1. Score movers (last {days}d)\n"]
    if not rows:
        lines.append(f"_No score data found comparing latest vs on/before {d['cutoff']}._\n")
        return "\n".join(lines)

    lines.append(f"| ZIP | Neighborhood | {days}d ago | Now | Delta | Top signal |")
    lines.append("|-----|-------------|-----------|-----|-------|------------|")
    for r in rows:
        sign = "+" if r.delta > 0 else ""
        lines.append(
            f"| {r.zip_code} | {r.neighborhood or '—'} "
            f"| {r.prev_score} | {r.curr_score} "
            f"| {sign}{r.delta} | {_top_signal(r)} |"
        )
    lines.append(
        f"\n_Prior score: closest scored_at on or before {d['cutoff']}. "
        f"ZIPs with <100 residential units excluded._\n"
    )
    return "\n".join(lines)


# ── Section 2: Newly active operators ───────────────────────────────────────

def newly_active_operators(db, days: int, zip_filter: Optional[str]) -> str:
    d = _dates(days)
    zip_join = "JOIN parcels pc ON op.bbl = pc.bbl AND pc.zip_code = :zip_f" if zip_filter else ""
    params = {"cutoff": d["cutoff"], **({"zip_f": zip_filter} if zip_filter else {})}

    rows = db.execute(text(f"""
        SELECT
            o.display_name,
            o.slug,
            o.total_properties,
            COUNT(op.id)                                                   AS new_acqs,
            STRING_AGG(op.bbl, ', ' ORDER BY op.acquisition_date DESC)    AS bbls,
            MAX(op.acquisition_date)                                       AS latest_acq
        FROM operator_parcels op
        JOIN operators o ON o.id = op.operator_id
        {zip_join}
        WHERE op.acquisition_date >= :cutoff
        GROUP BY o.id, o.display_name, o.slug, o.total_properties
        ORDER BY new_acqs DESC
    """), params).fetchall()

    lines = [f"## 2. Newly active operators (last {days}d)\n"]
    if not rows:
        lines.append(
            f"_No operator_parcels entries with acquisition_date >= {d['cutoff']}. "
            f"operator_parcels is backfilled from ACRIS; reflects ingest lag._\n"
        )
        return "\n".join(lines)

    lines.append("| Operator | Slug | New acqs | Portfolio | Latest acq | BBLs |")
    lines.append("|----------|------|----------|-----------|------------|------|")
    for r in rows:
        bbl_str = r.bbls or ""
        bbl_preview = bbl_str[:80] + ("…" if len(bbl_str) > 80 else "")
        lines.append(
            f"| {r.display_name} | {r.slug} | {r.new_acqs} "
            f"| {r.total_properties} | {r.latest_acq} | {bbl_preview} |"
        )
    lines.append("")
    return "\n".join(lines)


# ── Section 3: Anomalous signal spikes ──────────────────────────────────────

def signal_spikes(db, days: int, zip_filter: Optional[str]) -> str:
    d = _dates(days)
    zip_where = "AND zip_code = :zip_f" if zip_filter else ""

    signals = [
        ("permits",       "permits_raw",    "filing_date",        ""),
        ("complaints",    "complaints_raw", "created_date::date", ""),
        ("evictions",     "evictions_raw",  "executed_date",      ""),
        ("LLC transfers", "ownership_raw",  "doc_date",           "AND party_type = '2'"),
        ("violations",    "violations_raw", "nov_issued_date",    ""),
    ]

    candidates = []
    for signal_name, table, date_col, extra_where in signals:
        params = {
            "cutoff":    d["cutoff"],
            "prior_30d": d["prior_30d"],
            "divisor":   d["divisor"],
            **({"zip_f": zip_filter} if zip_filter else {}),
        }
        try:
            rows = db.execute(text(f"""
                WITH counts AS (
                    SELECT
                        zip_code,
                        COUNT(*) FILTER (WHERE {date_col} >= :cutoff)::float        AS this_week,
                        COUNT(*) FILTER (
                            WHERE {date_col} >= :prior_30d AND {date_col} < :cutoff
                        )::float / :divisor                                          AS rolling_avg
                    FROM {table}
                    WHERE zip_code IS NOT NULL {extra_where} {zip_where}
                      AND {date_col} >= :prior_30d
                    GROUP BY zip_code
                )
                SELECT
                    zip_code,
                    ROUND(this_week::numeric)                              AS this_week,
                    ROUND(rolling_avg::numeric, 1)                        AS rolling_avg,
                    ROUND((this_week / NULLIF(rolling_avg, 0))::numeric, 1) AS multiplier
                FROM counts
                WHERE this_week >= 3
                  AND rolling_avg > 0
                  AND this_week >= 2.0 * rolling_avg
                ORDER BY multiplier DESC
                LIMIT 5
            """), params).fetchall()
            for r in rows:
                candidates.append((
                    signal_name, r.zip_code,
                    int(r.this_week), float(r.rolling_avg), float(r.multiplier),
                ))
        except Exception:
            db.rollback()

    candidates.sort(key=lambda x: -x[4])
    top = candidates[:5]

    lines = [f"## 3. Anomalous signal spikes (last {days}d)\n"]
    lines.append(
        f"_Threshold: this-{days}d count >= 2x trailing 30-day weekly average, min 3 events._\n"
    )
    if not top:
        lines.append("_No anomalous spikes detected._\n")
        return "\n".join(lines)

    lines.append("| Signal | ZIP | This period | 30d weekly avg | Multiplier |")
    lines.append("|--------|-----|-------------|----------------|------------|")
    for signal_name, zip_code, this_week, avg, mult in top:
        lines.append(f"| {signal_name} | {zip_code} | {this_week} | {avg:.1f} | {mult:.1f}x |")
    lines.append("")
    return "\n".join(lines)


# ── Section 4: Operator portfolio changes ───────────────────────────────────

def operator_portfolio_changes(db, days: int, zip_filter: Optional[str]) -> str:
    d = _dates(days)
    zip_join = "JOIN parcels pc ON op.bbl = pc.bbl AND pc.zip_code = :zip_f" if zip_filter else ""
    params = {"cutoff": d["cutoff"], **({"zip_f": zip_filter} if zip_filter else {})}

    def _query(event_table, date_col, label_expr):
        return db.execute(text(f"""
            SELECT
                o.display_name,
                o.slug,
                COUNT(*)    AS cnt,
                STRING_AGG(
                    {label_expr} || ' (' || {event_table}.{date_col}::text || ')',
                    E'\\n' ORDER BY {event_table}.{date_col} DESC
                )           AS events
            FROM {event_table}
            JOIN operator_parcels op ON op.bbl = {event_table}.bbl
            JOIN operators o ON o.id = op.operator_id
            {zip_join}
            WHERE {event_table}.{date_col} >= :cutoff
              AND {event_table}.{date_col} IS NOT NULL
            GROUP BY o.id, o.display_name, o.slug
            ORDER BY cnt DESC
        """), params).fetchall()

    viol_rows   = _query("violations_raw", "nov_issued_date",
                         "violation_class || ' — ' || LEFT(COALESCE(description, ''), 60) || ' ' || violations_raw.bbl")
    evic_rows   = _query("evictions_raw",  "executed_date",
                         "COALESCE(address, evictions_raw.bbl)")
    permit_rows = _query("permits_raw",    "filing_date",
                         "permit_type || '/' || COALESCE(work_type, '?') || ' — ' || COALESCE(address, permits_raw.bbl)")

    lines = [f"## 4. Operator portfolio changes (last {days}d)\n"]
    has_data = False

    def _block(title, rows):
        nonlocal has_data
        if not rows:
            return [f"**{title}:** none\n"]
        has_data = True
        out = [f"**{title}:**\n"]
        out.append("| Operator | Count | Top 3 events |")
        out.append("|----------|-------|--------------|")
        for r in rows:
            top3 = "<br>".join((r.events or "").split("\n")[:3])
            out.append(f"| {r.display_name} | {r.cnt} | {top3} |")
        out.append("")
        return out

    lines += _block(f"New HPD violations (since {d['cutoff']})", viol_rows)
    lines += _block(f"New eviction filings (since {d['cutoff']})", evic_rows)
    lines += _block(f"New DOB permits (since {d['cutoff']})", permit_rows)

    if not has_data:
        lines.append(f"_No activity on operator-owned BBLs since {d['cutoff']}._\n")

    return "\n".join(lines)


# ── Section 5: MTEK alerts ──────────────────────────────────────────────────

def mtek_alerts_section(db, days: int, zip_filter: Optional[str]) -> str:
    zip_join = "JOIN parcels pc ON m.bbl = pc.bbl AND pc.zip_code = :zip_f" if zip_filter else ""
    params = {
        "cutoff": datetime.now(timezone.utc) - timedelta(days=days),
        **({"zip_f": zip_filter} if zip_filter else {}),
    }

    rows = db.execute(text(f"""
        SELECT m.alert_type, m.bbl, m.address, m.entity,
               m.violation_class, m.event_date, m.detail, m.created_at
        FROM mtek_alerts m
        {zip_join}
        WHERE m.created_at >= :cutoff
        ORDER BY m.created_at DESC
    """), params).fetchall()

    lines = [f"## 5. MTEK alerts (last {days}d)\n"]
    if not rows:
        lines.append(f"_mtek_alerts: 0 rows in last {days} days. Monitor runs nightly._\n")
        return "\n".join(lines)

    by_type: dict[str, list] = {}
    for r in rows:
        by_type.setdefault(r.alert_type, []).append(r)

    for alert_type, items in sorted(by_type.items()):
        lines.append(f"**{alert_type}** ({len(items)})\n")
        lines.append("| BBL | Address | Entity | Class | Date | Detail |")
        lines.append("|-----|---------|--------|-------|------|--------|")
        for r in items:
            lines.append(
                f"| {r.bbl} | {r.address or '—'} | {r.entity or '—'} "
                f"| {r.violation_class or '—'} | {r.event_date} "
                f"| {(r.detail or '')[:80]} |"
            )
        lines.append("")

    return "\n".join(lines)


# ── Section 6: Data freshness ────────────────────────────────────────────────

def data_freshness(db) -> str:
    rows = db.execute(text("""
        SELECT DISTINCT ON (scraper_name)
               scraper_name, completed_at, status,
               records_processed, warning_message
        FROM scraper_runs
        WHERE status IN ('success', 'error')
        ORDER BY scraper_name, completed_at DESC NULLS LAST
    """)).fetchall()

    stuck = db.execute(text("""
        SELECT DISTINCT scraper_name, MAX(started_at) AS last_start
        FROM scraper_runs
        WHERE status = 'running'
          AND started_at < NOW() - INTERVAL '2 hours'
        GROUP BY scraper_name
    """)).fetchall()

    now = datetime.now(timezone.utc)
    lines = ["## 6. Data freshness\n"]
    lines.append("| Scraper | Last run (UTC) | Hours ago | Records | Warning |")
    lines.append("|---------|---------------|-----------|---------|---------|")

    for r in rows:
        if r.completed_at:
            hours_ago = (now - r.completed_at).total_seconds() / 3600
            flag = " ⚠" if hours_ago > 48 or r.status == "error" else ""
            warn = (r.warning_message or "")[:80]
            lines.append(
                f"| {r.scraper_name}{flag} "
                f"| {r.completed_at.strftime('%Y-%m-%d %H:%M')} "
                f"| {hours_ago:.1f}h | {r.records_processed:,} | {warn} |"
            )
        else:
            lines.append(f"| {r.scraper_name} ⚠ | never | — | — | — |")

    if stuck:
        lines.append("")
        lines.append(f"**Stuck in running state (>2h):** {', '.join(r.scraper_name for r in stuck)}")

    lines.append("")
    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Weekly content brief from live PulseCities data")
    parser.add_argument("--days", type=int, default=7, metavar="N",
                        help="Lookback window in days (default: 7)")
    parser.add_argument("--zip", dest="zip_filter", default=None, metavar="ZZZZZ",
                        help="Filter all sections to one ZIP code")
    args = parser.parse_args()

    days = args.days
    zf   = args.zip_filter

    header  = f"# PulseCities — Content Brief\n\n"
    header += f"Generated: {date.today().isoformat()}  \n"
    header += f"Window: {days} days"
    if zf:
        header += f"  \nZIP filter: {zf}"
    header += "\n"

    sections = [header]

    section_fns = [
        ("Score movers",             lambda db: score_movers(db, days, zf)),
        ("Newly active operators",   lambda db: newly_active_operators(db, days, zf)),
        ("Signal spikes",            lambda db: signal_spikes(db, days, zf)),
        ("Operator portfolio changes", lambda db: operator_portfolio_changes(db, days, zf)),
        ("MTEK alerts",              lambda db: mtek_alerts_section(db, days, zf)),
        ("Data freshness",           lambda db: data_freshness(db)),
    ]

    for name, fn in section_fns:
        try:
            with get_scraper_db() as db:
                sections.append(fn(db))
        except Exception as exc:
            sections.append(f"## {name}\n\n_Section failed: {exc}_\n")

    sys.stdout.write("\n".join(sections))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
