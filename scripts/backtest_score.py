"""
Does the composite score predict what happens next?

The site's central claim is that the composite ranks displacement pressure. It
has never been checked against an outcome. score_history holds 315 days across
177 ZIPs, and backfill_score_history replays a 365-day window ending on each
date, so the history is point-in-time: the score at T was computed from records
dated on or before T.

Method. Anchor T, outcome window (T, T+90]. Predictors are the composite and
its five active signals as of T. Outcomes are aggregated with scoring.compute's
own functions, so "an eviction" means here exactly what it means on the site,
and converted to a rate per 1,000 residential units. Rank correlation across
ZIPs, one figure per anchor, reported as a median with its range.

Three things to hold on to when reading the output.

  Anchors overlap, so 7 or 14 of them are not 7 or 14 independent
  observations. The ranges are reported because they are what makes the
  finding credible: every measurement here is stable across the whole period
  rather than resting on one window.

  Every relationship in this file is persistence, not causation. The strongest
  predictor of evictions in a ZIP next quarter is evictions in that ZIP last
  quarter. That is the baseline the composite has to beat to be worth
  computing, and it is reported as `persistence` throughout.

  Feed windows are held clear of the ingest lag. OCA lags executed_date by two
  to four weeks, so outcome windows stop 30 days short of each feed's newest
  record. Without that the last window looks like a collapse.

Usage:
    python -m scripts.backtest_score outcomes    # vs each real-world outcome
    python -m scripts.backtest_score index       # vs the composite's own recipe
    python -m scripts.backtest_score window      # does window length matter
    python -m scripts.backtest_score transform   # does the 0-100 transform matter
"""
import argparse
import sys
from datetime import date, timedelta

import numpy as np
from sqlalchemy import text

from api.freshness import real_date
from models.database import SessionLocal
from scoring import compute as C

LAG = 90          # outcome horizon, days
CLEAR = 30        # keep windows clear of each feed's ingest lag
MIN_UNITS = 500   # skip commercial ZIPs where one record swings the rate
STEP = 28         # days between anchors

SIGNALS = ["composite_score", "eviction_rate", "llc_acquisition_rate",
           "permit_intensity", "complaint_rate", "hpd_violations"]

# name -> (aggregate, live weight, window shift).
# _aggregate_violations derives its own 90-day sub-window as cutoff + 275, so
# it needs the shift to be asked about the window everything else is asked about.
OUTCOMES = {
    "llc_deeds":  (C._aggregate_llc_acquisitions, C.WEIGHT_LLC_ACQUISITIONS, 0),
    "permits":    (C._aggregate_permits,          C.WEIGHT_PERMITS,          0),
    "complaints": (C._aggregate_complaints,       C.WEIGHT_COMPLAINTS,       0),
    "evictions":  (C._aggregate_evictions,        C.WEIGHT_EVICTIONS,        0),
    "violations": (C._aggregate_violations,       C.WEIGHT_HPD_VIOLATIONS,   275),
}

# Newest real record per feed. `real_date` rather than a bare max(): these are
# filer-typed columns, and one row typed a month into the future would push the
# outcome windows past the data and read as a collapse. That is the same rule
# that put a future <lastmod> on 200 hub URLs for sixteen nights, and the
# reason this file's windows also stop CLEAR days short of the answer.
FEED_END = {
    "evictions":  f"SELECT MAX(executed_date)::date FROM evictions_raw"
                  f" WHERE {real_date('executed_date')}",
    "llc_deeds":  f"SELECT MAX(doc_date)::date FROM ownership_raw"
                  f" WHERE {real_date('doc_date')}",
    "complaints": f"SELECT MAX(created_date)::date FROM complaints_raw"
                  f" WHERE {real_date('created_date')}",
    "violations": f"SELECT MAX(inspection_date)::date FROM violations_raw"
                  f" WHERE {real_date('inspection_date')}",
    "permits":    f"SELECT MAX(filing_date)::date FROM permits_raw"
                  f" WHERE {real_date('filing_date')}",
}


def rank(a: np.ndarray) -> np.ndarray:
    """Ranks with ties averaged. Ties are not incidental here: the shipped
    transform clamps, which puts nine ZIPs on exactly 100.0 for every signal,
    and ordering them by array position would invent a ranking."""
    order = a.argsort()
    r = np.empty(len(a), float)
    r[order] = np.arange(len(a), dtype=float)
    _, inv, counts = np.unique(a, return_inverse=True, return_counts=True)
    sums = np.zeros(len(counts))
    np.add.at(sums, inv, r)
    return (sums / counts)[inv]


def spearman(x, y) -> float:
    rx, ry = rank(np.asarray(x, float)), rank(np.asarray(y, float))
    if rx.std() == 0 or ry.std() == 0:
        return float("nan")
    return float(np.corrcoef(rx, ry)[0, 1])


# --- the three 0-100 transforms, so they can be compared on one input --------

def clamped_linear(v):
    """What ships: scoring/compute.py _norm_map."""
    s = np.sort(v)
    n = len(s)
    p5, p95 = s[max(0, int(n * .05))], s[min(n - 1, int(n * .95))]
    if p5 == p95:
        p5, p95 = s[0], s[-1]
    if p95 == p5:
        return np.zeros(n)
    return np.clip((v - p5) / (p95 - p5) * 100, 0, 100)


def linear_minmax(v):
    return np.zeros(len(v)) if v.max() == v.min() else (v - v.min()) / (v.max() - v.min()) * 100


def rank_norm(v):
    return rank(v) / (len(v) - 1) * 100


class Backtest:
    def __init__(self, db):
        self.db = db
        self.units = {z: float(u) for z, u in db.execute(text("""
            SELECT zip_code, SUM(units_res) FROM parcels
            WHERE zip_code IS NOT NULL AND units_res > 0 GROUP BY zip_code""")).fetchall()}
        self.dates = [r[0] for r in db.execute(text(
            "SELECT DISTINCT scored_at FROM score_history ORDER BY scored_at")).fetchall()]
        self.zips = sorted(z for z in self.units if self.units[z] >= MIN_UNITS)

    def feed_end(self, name) -> date:
        return self.db.execute(text(FEED_END[name])).scalar()

    def anchors(self, usable_end):
        out, t = [], self.dates[0]
        while t + timedelta(days=LAG) <= usable_end:
            out.append((t, max(d for d in self.dates if d <= t)))
            t += timedelta(days=STEP)
        return out

    def rate(self, agg, shift, w_start, w_end, zips=None):
        zips = zips or self.zips
        d = dict(agg(self.db, cutoff=w_start - timedelta(days=shift), until=w_end))
        return np.array([d.get(z, 0) / self.units[z] * 1000 for z in zips], float)

    def history(self, scored_at, zips=None):
        rows = {r[0]: r for r in self.db.execute(text(
            f"SELECT zip_code, {', '.join(SIGNALS)} FROM score_history WHERE scored_at = :d"),
            {"d": scored_at}).fetchall()}
        zips = zips or [z for z in self.zips if z in rows]
        return rows, zips

    def index(self, w_start, w_end, transform=rank_norm):
        """The composite's recipe applied to one window."""
        total, mass = np.zeros(len(self.zips)), 0.0
        for agg, w, shift in OUTCOMES.values():
            total += w * transform(self.rate(agg, shift, w_start, w_end))
            mass += w
        return total / mass


def _report(title, subtitle, rows):
    print(f"\n{title}\n{subtitle}\n")
    print("  %-24s %8s %8s %8s" % ("predictor at T", "median", "min", "max"))
    for name, vals in rows.items():
        v = np.array([x for x in vals if not np.isnan(x)])
        if not len(v):
            print("  %-24s %8s" % (name, "no data"))
            continue
        print("  %-24s %8.3f %8.3f %8.3f" % (name, np.median(v), v.min(), v.max()))


def mode_outcomes(bt):
    """Against each real-world outcome, one at a time."""
    for name in ("evictions", "llc_deeds", "complaints", "violations"):
        agg, _, shift = OUTCOMES[name]
        usable = bt.feed_end(name) - timedelta(days=CLEAR)
        anchors = bt.anchors(usable)
        if len(anchors) < 3:
            print(f"\n{name}: only {len(anchors)} anchors fit before {usable}, skipping")
            continue
        per = {s: [] for s in SIGNALS}
        per["persistence"] = []
        for t, scored in anchors:
            rows, zips = bt.history(scored)
            fut = bt.rate(agg, shift, t + timedelta(days=1), t + timedelta(days=LAG), zips)
            past = bt.rate(agg, shift, t - timedelta(days=LAG - 1), t, zips)
            for i, s in enumerate(SIGNALS, start=1):
                per[s].append(spearman([float(rows[z][i] or 0.0) for z in zips], fut))
            per["persistence"].append(spearman(past, fut))
        _report(f"Outcome: {name} per 1,000 residential units in the next {LAG} days",
                f"{len(anchors)} overlapping anchors, {anchors[0][0]} .. {anchors[-1][0]}, "
                f"feed ends {bt.feed_end(name)}", per)


def mode_index(bt):
    """Against the composite's own recipe, which is the yardstick it was built for."""
    usable = min(bt.feed_end(n) for n in FEED_END) - timedelta(days=CLEAR)
    anchors = bt.anchors(usable)
    per = {s: [] for s in SIGNALS}
    per["persistence"] = []
    for t, scored in anchors:
        rows, zips = bt.history(scored, bt.zips)
        fut = bt.index(t + timedelta(days=1), t + timedelta(days=LAG))
        past = bt.index(t - timedelta(days=LAG - 1), t)
        for i, s in enumerate(SIGNALS, start=1):
            per[s].append(spearman([float(rows[z][i] or 0.0) if z in rows else 0.0
                                    for z in zips], fut))
        per["persistence"].append(spearman(past, fut))
    _report(f"Outcome: the composite's own weighted recipe over the next {LAG} days",
            f"{len(anchors)} anchors, {len(bt.zips)} ZIPs", per)


def mode_window(bt):
    """Is the 365-day window the problem?"""
    usable = min(bt.feed_end(n) for n in FEED_END) - timedelta(days=CLEAR)
    anchors = [t for t, _ in bt.anchors(usable)]
    per = {f"{w} days": [] for w in (30, 90, 180, 365)}
    for t in anchors:
        fut = bt.index(t + timedelta(days=1), t + timedelta(days=LAG))
        for w in (30, 90, 180, 365):
            per[f"{w} days"].append(
                spearman(bt.index(t - timedelta(days=w - 1), t), fut))
    _report("Same recipe and weights, only the trailing window changes",
            f"{len(anchors)} anchors, {len(bt.zips)} ZIPs", per)


def mode_transform(bt):
    """Is the 0-100 transform the problem?"""
    usable = min(bt.feed_end(n) for n in FEED_END) - timedelta(days=CLEAR)
    anchors = [t for t, _ in bt.anchors(usable)]
    forms = {"clamped_linear (ships)": clamped_linear,
             "linear_minmax": linear_minmax, "rank": rank_norm}
    per = {k: [] for k in forms}
    for t in anchors:
        # Aggregate once per window; the transforms must see identical inputs.
        past = [(bt.rate(a, s, t - timedelta(days=364), t), w)
                for a, w, s in OUTCOMES.values()]
        fut = bt.index(t + timedelta(days=1), t + timedelta(days=LAG))
        for k, f in forms.items():
            total, mass = np.zeros(len(bt.zips)), 0.0
            for v, w in past:
                total += w * f(v)
                mass += w
            per[k].append(spearman(total / mass, fut))
    _report("365-day window and live weights, only the 0-100 transform changes",
            f"{len(anchors)} anchors, {len(bt.zips)} ZIPs, identical aggregates", per)


MODES = {"outcomes": mode_outcomes, "index": mode_index,
         "window": mode_window, "transform": mode_transform}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=sorted(MODES))
    args = p.parse_args()
    db = SessionLocal()
    try:
        MODES[args.mode](Backtest(db))
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
