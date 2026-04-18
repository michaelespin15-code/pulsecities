"""
Weekly operator network analysis diff.

Pulls fresh ACRIS analysis from the DB, diffs against the saved
operator_network_analysis.json baseline, identifies new operators that
crossed the 10-property threshold this week, and refreshes profiles for
MTEK, PHANTOM, and BREDIF.

Output: scripts/weekly_operator_diff.json
Also overwrites: scripts/operator_network_analysis.json with fresh results.

Usage:
    python scripts/weekly_operator_diff.py
"""

import json
import logging
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db

logger = logging.getLogger(__name__)

DIFF_OUTPUT  = Path(__file__).parent / "weekly_operator_diff.json"
NET_OUTPUT   = Path(__file__).parent / "operator_network_analysis.json"
PROF_OUTPUT  = Path(__file__).parent / "top_operators_profiled.json"

# Operators whose profiles get a full refresh every week
WEEKLY_PROFILE_ROOTS = {"MTEK", "PHANTOM", "BREDIF"}

# New-entrant threshold: operators with this many unique properties are "on radar"
NEW_ENTRANT_THRESHOLD = 10

EVICTION_LOOKBACK_DAYS = 365

# ─── copied from operator_network_analysis.py ──────────────────────────────

_GENERIC = {
    "THE", "NEW", "ONE", "TWO", "NY", "NYC", "REAL", "URBAN",
    "CITY", "OLD", "EAST", "WEST", "NORTH", "SOUTH", "HOME",
    "LAND", "BAY", "FIRST", "SECOND", "THIRD", "ST", "AVE",
    "STREET", "AVENUE", "BLVD", "PARK", "LLC", "INC", "CORP",
    "LTD", "LP", "LLP", "GROUP", "HOLDINGS", "REALTY",
    "BUSINESS", "AMERICAN", "FLUSHING",
}

_BANK_ROOTS = {
    "BANK", "JPMORGAN", "NATIONSTAR", "MORTGAGE", "MERS", "WILMINGTON",
    "GOLDMAN", "FREEDOM", "SECRETARY", "LAKEVIEW", "NEWREZ", "CITI",
    "CITIBANK", "WELLS", "DEUTSCHE", "HSBC", "CHASE", "FLAGSTAR",
    "PENNYMAC", "LOANDEPOT", "CALIBER", "PHH", "OCWEN", "SERVICER",
    "TRUST", "TRUSTEE", "FEDERAL", "FANNIE", "FREDDIE", "HUD",
    "ELECTRONIC", "LOAN", "SAVINGS", "SACHS", "FARGO", "GSM", "WEBSTER",
    "NATIONAL", "SHELLPOINT", "MORGAN", "CITIZENS", "CARRINGTON", "FIRSTKEY",
    "CITIGROUP", "COMPUTERSHARE", "UNITED", "RCF", "VELOCITY", "NORTHEAST",
    "LOANCARE", "SELENE", "SPS", "BSI", "RUSHMORE", "ROUNDPOINT", "SERVIS",
    "STATEBRIDGE", "SPECIALIZED", "MAE", "CUSTOMERS", "CPC",
    "BATTALION", "FUNDING", "LENDING", "HABIB",
}


def _operator_root(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = re.sub(r"\b(LLC|L\.L\.C|CORP|INC|LTD|LP|LLP)\b\.?$", "", name).strip()
    tokens = cleaned.split()

    first_tok: str | None = None
    first_idx: int = -1
    for i, tok in enumerate(tokens[:2]):
        tok = tok.strip(".,;")
        if (
            len(tok) >= 3
            and tok not in _GENERIC
            and tok not in _BANK_ROOTS
            and not re.match(r"^\d+$", tok)
        ):
            first_tok = tok
            first_idx = i
            break

    if not first_tok:
        return None

    if len(first_tok) <= 4 and first_idx + 1 < len(tokens):
        next_tok = tokens[first_idx + 1].strip(".,;")
        if (
            3 <= len(next_tok) <= 4
            and next_tok not in _GENERIC
            and next_tok not in _BANK_ROOTS
            and not re.match(r"^\d+$", next_tok)
        ):
            return first_tok + next_tok

    return first_tok


# ─── network analysis (full, uncapped) ─────────────────────────────────────

def _load_high_displacement_zips(db) -> set[str]:
    try:
        rows = db.execute(
            text("SELECT zip_code FROM displacement_scores WHERE score >= 40")
        ).fetchall()
        return {r.zip_code for r in rows} if rows else set()
    except Exception as exc:
        logger.warning("displacement_scores unavailable: %s", exc)
        return set()


def _load_bbl_zips(db) -> dict[str, str]:
    rows = db.execute(text("""
        SELECT bbl, zip_code FROM (
            SELECT bbl, zip_code FROM violations_raw WHERE bbl IS NOT NULL AND zip_code IS NOT NULL
            UNION
            SELECT bbl, zip_code FROM permits_raw   WHERE bbl IS NOT NULL AND zip_code IS NOT NULL
        ) t
    """)).fetchall()
    return {r.bbl: r.zip_code for r in rows if r.bbl and r.zip_code}


def run_full_analysis(db) -> list[dict]:
    """
    Full 18-month analysis with no top-N cap — returns every operator meeting
    the min-LLC / min-acquisition / HD-zip criteria, sorted by portfolio size.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=548)).date()

    hd_zips = _load_high_displacement_zips(db)
    bbl_zip  = _load_bbl_zips(db)

    rows = db.execute(text("""
        SELECT party_name_normalized, bbl, doc_date, doc_amount
        FROM ownership_raw
        WHERE party_type = '2'
          AND doc_date >= :cutoff
          AND party_name_normalized IS NOT NULL
          AND bbl IS NOT NULL
        ORDER BY doc_date
    """), {"cutoff": cutoff}).fetchall()

    logger.info("%d LLC acquisition records in 18-month window", len(rows))

    groups: dict[str, dict] = defaultdict(lambda: {"llc_names": set(), "acquisitions": []})
    for r in rows:
        root = _operator_root(r.party_name_normalized)
        if not root:
            continue
        g = groups[root]
        g["llc_names"].add(r.party_name_normalized)
        g["acquisitions"].append({
            "bbl":        r.bbl,
            "doc_date":   r.doc_date,
            "doc_amount": float(r.doc_amount) if r.doc_amount else None,
            "party_name": r.party_name_normalized,
        })

    results = []
    for root, g in groups.items():
        llc_names    = g["llc_names"]
        acquisitions = g["acquisitions"]

        if len(llc_names) < 2 or len(acquisitions) < 5:
            continue

        avg_per_llc = len(acquisitions) / len(llc_names)
        if avg_per_llc < 2.0:
            continue

        hd_count = sum(1 for a in acquisitions if bbl_zip.get(a["bbl"]) in hd_zips)
        hd_pct   = hd_count / len(acquisitions) if acquisitions else 0.0

        if hd_pct < 0.30:
            continue

        zips_hit  = {bbl_zip[a["bbl"]] for a in acquisitions if a["bbl"] in bbl_zip}
        total_val = sum(a["doc_amount"] for a in acquisitions if a["doc_amount"])
        avg_price = total_val / len(acquisitions) if acquisitions else 0.0

        dates = sorted(a["doc_date"] for a in acquisitions if a["doc_date"])
        if len(dates) >= 2:
            span     = max((dates[-1] - dates[0]).days, 1)
            velocity = round(len(dates) / (span / 30), 2)
        else:
            velocity = 0.0

        results.append({
            "operator_root":                  root,
            "total_properties":               len({a["bbl"] for a in acquisitions}),
            "total_acquisitions":             len(acquisitions),
            "llc_count":                      len(llc_names),
            "llc_entities":                   sorted(llc_names),
            "total_portfolio_value":          round(total_val, 2),
            "avg_acquisition_price":          round(avg_price, 2),
            "acquisitions_per_month":         velocity,
            "first_acquisition":              dates[0].isoformat() if dates else None,
            "last_acquisition":               dates[-1].isoformat() if dates else None,
            "zip_codes_targeted":             sorted(zips_hit),
            "high_displacement_acquisitions": hd_count,
            "high_displacement_pct":          round(hd_pct * 100, 1),
            "targets_high_displacement":      hd_pct >= 0.5,
        })

    results.sort(key=lambda x: (x["total_properties"], x["total_portfolio_value"]), reverse=True)
    return results


# ─── profiling (adapted from top_operators_profiled.py) ────────────────────

def _load_displacement_scores(db) -> dict[str, float]:
    rows = db.execute(text("SELECT zip_code, score FROM displacement_scores")).fetchall()
    return {r.zip_code: float(r.score) for r in rows}


def _load_evictions_by_bbl(db) -> dict[str, list[date]]:
    rows = db.execute(text(
        "SELECT bbl, executed_date FROM evictions_raw "
        "WHERE bbl IS NOT NULL AND executed_date IS NOT NULL"
    )).fetchall()
    result: dict[str, list[date]] = {}
    for r in rows:
        result.setdefault(r.bbl, []).append(r.executed_date)
    for bbl in result:
        result[bbl].sort()
    return result


def _load_violations_by_bbl(db) -> dict[str, dict]:
    rows = db.execute(text(
        "SELECT bbl, violation_class, COUNT(*) AS cnt "
        "FROM violations_raw WHERE bbl IS NOT NULL "
        "GROUP BY bbl, violation_class"
    )).fetchall()
    result: dict[str, dict] = {}
    for r in rows:
        bbl = r.bbl
        vc  = (r.violation_class or "").upper()
        if bbl not in result:
            result[bbl] = {"total": 0, "class_a": 0, "class_b": 0, "class_c": 0}
        result[bbl]["total"] += r.cnt
        if vc in ("A", "B", "C"):
            result[bbl][f"class_{vc.lower()}"] += r.cnt
    return result


def _fetch_operator_bbls(db, llc_entities: list[str]) -> list[dict]:
    placeholders = ", ".join(f":name_{i}" for i in range(len(llc_entities)))
    params       = {f"name_{i}": name for i, name in enumerate(llc_entities)}
    rows = db.execute(text(f"""
        SELECT bbl, doc_date, doc_amount, party_name_normalized
        FROM ownership_raw
        WHERE party_type = '2'
          AND party_name_normalized IN ({placeholders})
          AND bbl IS NOT NULL
        ORDER BY doc_date
    """), params).fetchall()
    return [
        {
            "bbl":        r.bbl,
            "doc_date":   r.doc_date,
            "doc_amount": float(r.doc_amount) if r.doc_amount else None,
            "entity":     r.party_name_normalized,
        }
        for r in rows
    ]


def _profile_operator(root, base, acquisitions, disp_scores, bbl_zip,
                       evictions_by_bbl, violations_by_bbl) -> dict:
    unique_bbls = {a["bbl"] for a in acquisitions}

    zip_scores: list[float] = []
    zip_breakdown: dict[str, int] = {}
    for bbl in unique_bbls:
        z = bbl_zip.get(bbl)
        if not z:
            continue
        zip_breakdown[z] = zip_breakdown.get(z, 0) + 1
        if z in disp_scores:
            zip_scores.append(disp_scores[z])

    weighted_avg_score = round(sum(zip_scores) / len(zip_scores), 1) if zip_scores else 0.0

    tier_counts = {"score_80_plus": 0, "score_60_79": 0, "score_40_59": 0, "score_under_40": 0}
    for s in zip_scores:
        if   s >= 80: tier_counts["score_80_plus"] += 1
        elif s >= 60: tier_counts["score_60_79"]   += 1
        elif s >= 40: tier_counts["score_40_59"]   += 1
        else:         tier_counts["score_under_40"] += 1

    eviction_matches: list[dict] = []
    for a in acquisitions:
        bbl      = a["bbl"]
        acq_date = a["doc_date"]
        if not acq_date or bbl not in evictions_by_bbl:
            continue
        cutoff = acq_date - timedelta(days=EVICTION_LOOKBACK_DAYS)
        prior  = [d for d in evictions_by_bbl[bbl] if cutoff <= d < acq_date]
        if prior:
            eviction_matches.append({
                "bbl":              bbl,
                "acquisition_date": acq_date.isoformat() if acq_date else None,
                "evictions_before": len(prior),
                "latest_eviction":  max(prior).isoformat(),
                "days_gap":         (acq_date - max(prior)).days,
            })

    total_v = class_b = class_c = bbls_bc = 0
    for bbl in unique_bbls:
        v = violations_by_bbl.get(bbl, {})
        total_v += v.get("total", 0)
        b = v.get("class_b", 0)
        c = v.get("class_c", 0)
        class_b += b
        class_c += c
        if b + c > 0:
            bbls_bc += 1

    n_props  = len(unique_bbls) or 1
    viol_pp  = round(total_v / n_props, 2)
    bc_pp    = round((class_b + class_c) / n_props, 2)

    dates    = sorted(a["doc_date"] for a in acquisitions if a["doc_date"])
    if len(dates) >= 2:
        span     = max((dates[-1] - dates[0]).days, 1)
        velocity = round(len(dates) / (span / 30), 2)
    else:
        velocity = 0.0

    total_val = sum(a["doc_amount"] for a in acquisitions if a["doc_amount"])
    avg_price = round(total_val / len(acquisitions), 2) if acquisitions else 0.0

    hd_pct     = base.get("high_displacement_pct", 0) / 100
    etb_rate   = len(eviction_matches) / n_props
    norm_score = min(weighted_avg_score / 100, 1.0)
    norm_bc    = min(bc_pp / 10, 1.0)

    evidence_score = round(
        hd_pct     * 30 +
        etb_rate   * 25 +
        norm_score * 25 +
        norm_bc    * 20,
        1,
    )

    return {
        "operator_root":           root,
        "llc_entities":            base["llc_entities"],
        "llc_count":               base["llc_count"],
        "total_properties":        len(unique_bbls),
        "total_acquisitions":      len(acquisitions),
        "total_portfolio_value":   round(total_val, 2),
        "avg_acquisition_price":   avg_price,
        "acquisitions_per_month":  velocity,
        "first_acquisition":       dates[0].isoformat() if dates else None,
        "last_acquisition":        dates[-1].isoformat() if dates else None,
        "displacement_score": {
            "weighted_avg":       weighted_avg_score,
            "tier_distribution":  tier_counts,
            "zip_concentration":  dict(sorted(zip_breakdown.items(), key=lambda x: -x[1])[:10]),
        },
        "high_displacement_pct":   base.get("high_displacement_pct", 0),
        "eviction_then_buy": {
            "matched_properties": len(eviction_matches),
            "match_rate_pct":     round(len(eviction_matches) / n_props * 100, 1),
            "lookback_days":      EVICTION_LOOKBACK_DAYS,
            "matches":            sorted(eviction_matches, key=lambda x: x["days_gap"]),
        },
        "hpd_violations": {
            "total":                   total_v,
            "class_b":                 class_b,
            "class_c":                 class_c,
            "bbls_with_class_bc":      bbls_bc,
            "violations_per_property": viol_pp,
            "class_bc_per_property":   bc_pp,
        },
        "evidence_score": evidence_score,
    }


# ─── diff engine ────────────────────────────────────────────────────────────

def compute_diff(old_operators: list[dict], new_operators: list[dict], cutoff_date: date) -> dict:
    """
    Compare old and new operator lists.

    Returns a structured diff covering:
      - new_entrants_10_plus: operators not in old list, now >= 10 properties
      - portfolio_growth: per-operator change in properties, LLCs, last acquisition
      - threshold_crossings: operators that were < 10 props and are now >= 10
    """
    old_by_root = {op["operator_root"]: op for op in old_operators}
    new_by_root = {op["operator_root"]: op for op in new_operators}

    new_entrants = []
    threshold_crossings = []
    portfolio_growth = {}

    for root, new_op in new_by_root.items():
        old_op = old_by_root.get(root)

        if old_op is None:
            if new_op["total_properties"] >= NEW_ENTRANT_THRESHOLD:
                new_entrants.append({
                    "operator_root":        root,
                    "total_properties":     new_op["total_properties"],
                    "llc_count":            new_op["llc_count"],
                    "llc_entities":         new_op["llc_entities"],
                    "first_acquisition":    new_op["first_acquisition"],
                    "last_acquisition":     new_op["last_acquisition"],
                    "high_displacement_pct": new_op["high_displacement_pct"],
                    "zip_codes_targeted":   new_op["zip_codes_targeted"],
                    "acquisitions_per_month": new_op["acquisitions_per_month"],
                    "note": "Not in prior week baseline — entered analysis window this cycle",
                })
        else:
            old_props = old_op["total_properties"]
            new_props = new_op["total_properties"]
            old_llcs  = set(old_op["llc_entities"])
            new_llcs  = set(new_op["llc_entities"])
            new_llc_names = sorted(new_llcs - old_llcs)

            # Properties acquired since cutoff (last 7 days)
            # We can't get this from the diff alone — use last_acquisition as a signal
            prop_delta = new_props - old_props
            llc_delta  = new_op["llc_count"] - old_op["llc_count"]

            if prop_delta != 0 or llc_delta != 0 or new_llc_names:
                portfolio_growth[root] = {
                    "prev_properties":    old_props,
                    "curr_properties":    new_props,
                    "properties_added":   prop_delta,
                    "prev_llc_count":     old_op["llc_count"],
                    "curr_llc_count":     new_op["llc_count"],
                    "new_llc_entities":   new_llc_names,
                    "last_acquisition":   new_op["last_acquisition"],
                    "high_displacement_pct": new_op["high_displacement_pct"],
                }

            if old_props < NEW_ENTRANT_THRESHOLD and new_props >= NEW_ENTRANT_THRESHOLD:
                threshold_crossings.append({
                    "operator_root":     root,
                    "prev_properties":   old_props,
                    "curr_properties":   new_props,
                    "llc_entities":      new_op["llc_entities"],
                    "last_acquisition":  new_op["last_acquisition"],
                    "high_displacement_pct": new_op["high_displacement_pct"],
                })

    return {
        "new_entrants_10_plus":  sorted(new_entrants, key=lambda x: -x["total_properties"]),
        "threshold_crossings":   sorted(threshold_crossings, key=lambda x: -x["curr_properties"]),
        "portfolio_growth":      {
            k: v for k, v in sorted(
                portfolio_growth.items(),
                key=lambda x: -(x[1].get("properties_added") or 0),
            )
        },
    }


# ─── entry point ────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    today      = date.today()
    week_start = today - timedelta(days=7)

    # Load the saved baseline before overwriting it
    old_payload  = json.loads(NET_OUTPUT.read_text()) if NET_OUTPUT.exists() else {}
    old_operators = old_payload.get("operators", [])
    baseline_date = old_payload.get("generated_at", "unknown")
    logger.info("Baseline: %s (%d operators)", baseline_date, len(old_operators))

    with get_scraper_db() as db:
        logger.info("Running full operator network analysis...")
        new_operators = run_full_analysis(db)
        logger.info("%d operators found in fresh analysis", len(new_operators))

        # --- New records ingested this week ---
        ingest_stats = db.execute(text("""
            SELECT
                COUNT(*) AS total_records,
                COUNT(DISTINCT bbl) AS unique_bbls,
                COUNT(DISTINCT party_name_normalized) AS unique_parties
            FROM ownership_raw
            WHERE party_type = '2'
              AND created_at >= NOW() - INTERVAL '7 days'
              AND bbl IS NOT NULL
              AND party_name_normalized IS NOT NULL
        """)).fetchone()

        # --- Profile MTEK, PHANTOM, BREDIF ---
        logger.info("Loading profiling lookups...")
        disp_scores      = _load_displacement_scores(db)
        bbl_zip          = _load_bbl_zips(db)
        evictions_by_bbl = _load_evictions_by_bbl(db)
        violations_by_bbl = _load_violations_by_bbl(db)

        new_by_root  = {op["operator_root"]: op for op in new_operators}
        updated_profiles = {}

        for root in sorted(WEEKLY_PROFILE_ROOTS):
            if root not in new_by_root:
                logger.warning("%s not found in fresh analysis — skipping profile", root)
                continue
            base         = new_by_root[root]
            acquisitions = _fetch_operator_bbls(db, base["llc_entities"])
            logger.info("Profiling %s — %d acquisition records", root, len(acquisitions))
            updated_profiles[root] = _profile_operator(
                root, base, acquisitions,
                disp_scores, bbl_zip, evictions_by_bbl, violations_by_bbl,
            )

    # --- Compute diff ---
    diff = compute_diff(old_operators, new_operators, week_start)

    # --- Assemble output ---
    payload = {
        "generated_at":   today.isoformat(),
        "baseline_date":  baseline_date,
        "analysis_window": {
            "operator_lookback": "18 months",
            "acris_pull_window": "7 days (since last scraper run)",
        },
        "acris_ingest_stats": {
            "total_new_records":  ingest_stats.total_records,
            "unique_bbls":        ingest_stats.unique_bbls,
            "unique_parties":     ingest_stats.unique_parties,
            "window_start":       week_start.isoformat(),
            "window_end":         today.isoformat(),
        },
        "operator_universe": {
            "total_operators_found":    len(new_operators),
            "operators_10_plus_props":  sum(1 for o in new_operators if o["total_properties"] >= 10),
            "operators_20_plus_props":  sum(1 for o in new_operators if o["total_properties"] >= 20),
        },
        "diff": diff,
        "updated_profiles": updated_profiles,
        "full_operator_list": new_operators,
    }

    DIFF_OUTPUT.write_text(json.dumps(payload, indent=2, default=str))
    logger.info("Saved %s", DIFF_OUTPUT)

    # Overwrite operator_network_analysis.json with fresh top-20
    net_payload = {
        "generated_at":   today.isoformat(),
        "analysis_window": "18 months",
        "criteria": {
            "min_llc_entities":                  2,
            "min_acquisitions":                  5,
            "high_displacement_score_threshold": 40,
        },
        "operators_found": len(new_operators),
        "operators":       new_operators[:20],
    }
    NET_OUTPUT.write_text(json.dumps(net_payload, indent=2, default=str))
    logger.info("Updated %s (top 20 of %d)", NET_OUTPUT, len(new_operators))

    # --- Print summary ---
    print(f"\nWeekly operator diff — {baseline_date} → {today.isoformat()}")
    print(f"  ACRIS records ingested (7d): {ingest_stats.total_records:,}  "
          f"unique BBLs: {ingest_stats.unique_bbls:,}")
    print(f"  Operators found: {len(new_operators)}  "
          f"(10+ props: {payload['operator_universe']['operators_10_plus_props']})")

    xings = diff["threshold_crossings"]
    entrants = diff["new_entrants_10_plus"]
    growth   = diff["portfolio_growth"]

    print(f"\n  Threshold crossings (crossed {NEW_ENTRANT_THRESHOLD}+ props this week): "
          f"{len(xings)}")
    for x in xings:
        print(f"    {x['operator_root']:<20} {x['prev_properties']} → {x['curr_properties']} props  "
              f"HD%={x['high_displacement_pct']}")

    print(f"\n  New entrants (not in prior baseline, {NEW_ENTRANT_THRESHOLD}+ props): "
          f"{len(entrants)}")
    for e in entrants:
        print(f"    {e['operator_root']:<20} {e['total_properties']} props  "
              f"{e['llc_count']} LLCs  HD%={e['high_displacement_pct']}")

    print(f"\n  Portfolio changes (existing operators with new acquisitions): "
          f"{len(growth)}")
    for root, g in list(growth.items())[:10]:
        delta = g['properties_added']
        sign  = "+" if delta >= 0 else ""
        print(f"    {root:<20} {g['prev_properties']} → {g['curr_properties']} props  "
              f"({sign}{delta})  new LLCs: {len(g['new_llc_entities'])}")

    print(f"\n  Updated profiles: {', '.join(sorted(updated_profiles.keys()))}")
    for root, prof in sorted(updated_profiles.items(), key=lambda x: -x[1]["evidence_score"]):
        print(f"    {root:<12} evidence={prof['evidence_score']:>5.1f}  "
              f"props={prof['total_properties']:>3}  "
              f"ETB%={prof['eviction_then_buy']['match_rate_pct']:>5.1f}  "
              f"BC/prop={prof['hpd_violations']['class_bc_per_property']:>5.2f}")

    print(f"\n  Output: {DIFF_OUTPUT}")


if __name__ == "__main__":
    sys.exit(main())
