# Does the composite predict anything? A backtest

**Run it yourself:** `python -m scripts.backtest_score {outcomes,index,window,transform}`

The composite has been the site's central claim since launch and had never been
checked against an outcome. It has now. The short version is that it does not
beat any of its own components at predicting the thing that component measures,
it does not beat them at predicting its own recipe either, and one signal
carrying 21% of the weight points the wrong way.

There is a fix worth more than any reweighting, and it is one function.

## Method

`score_history` holds 315 days across 177 ZIPs, and `backfill_score_history`
replays a 365-day window ending on each date, so the history is point-in-time:
the score at T was computed from records dated on or before T.

Anchor T, outcome window (T, T+90]. Predictors are the composite and its five
active signals as of T. Outcomes are aggregated with `scoring.compute`'s own
functions, so "an eviction" means here exactly what it means on the site, and
converted to a rate per 1,000 residential units. Rank correlation across ZIPs,
one figure per anchor, reported as a median with its range.

Three things to hold on to.

- **Anchors overlap.** Six or seven of them are not six or seven independent
  observations. The ranges are reported because they are what makes this
  credible: every number below is stable across the whole ten months rather
  than resting on one window.
- **All of this is persistence, not causation.** The strongest predictor of
  evictions in a ZIP next quarter is evictions in that ZIP last quarter. That
  is the baseline the composite has to beat to be worth computing, and it is in
  every table as `persistence`.
- **Feed ends come from `api.freshness.real_date`, not `max()`.** This was not
  caution. A bare `max(doc_date)` on `ownership_raw` reads 2026-08-27 while the
  feed has been frozen since 2026-07-31, because the column is filer-typed. The
  first version of this analysis used it and ran the LLC-deed windows a month
  past the data; `tests/test_date_guards.py` refused the commit. Windows also
  stop 30 days short of each feed's newest real record, because OCA lags
  `executed_date` by two to four weeks and without that clearance the last
  window reads as a collapse that did not happen.

## The composite never wins

Median Spearman against each outcome in the next 90 days. Bold is the winner in
each column.

| predictor at T | evictions | llc deeds | complaints | violations |
|---|---|---|---|---|
| **composite_score** | 0.642 | 0.466 | 0.672 | 0.653 |
| eviction_rate | **0.904** | 0.200 | 0.661 | 0.631 |
| llc_acquisition_rate | 0.229 | **0.919** | 0.105 | 0.049 |
| permit_intensity | -0.131 | -0.096 | 0.184 | 0.262 |
| complaint_rate | 0.621 | 0.062 | **0.978** | 0.930 |
| hpd_violations | 0.582 | 0.057 | 0.923 | **0.966** |
| persistence | 0.847 | 0.839 | 0.957 | 0.964 |

For every outcome, the single signal that measures that outcome beats the blend
by a wide margin, and so does simply knowing what happened in the last 90 days.

Judging a summary index by how well it forecasts evictions is not quite fair:
it carries 13% weight there. So here it is against its own yardstick, the
composite's own weighted recipe applied to the next 90 days.

| predictor at T | median | min | max |
|---|---|---|---|
| persistence | 0.890 | 0.888 | 0.896 |
| complaint_rate | 0.818 | 0.804 | 0.847 |
| **composite_score** | **0.813** | 0.806 | 0.836 |
| hpd_violations | 0.807 | 0.796 | 0.819 |
| eviction_rate | 0.637 | 0.590 | 0.654 |
| llc_acquisition_rate | 0.442 | 0.435 | 0.448 |
| permit_intensity | 0.263 | 0.250 | 0.290 |

On the yardstick it was built for, the composite is beaten by one of its own
components at 17% weight, and by last quarter's numbers.

## What is actually wrong, and it is not the weights

The obvious hypothesis was the window: 365 days is slow, and a 90-day forecast
wants something recent. **That hypothesis is wrong.** Same recipe, same weights,
only the trailing window changes:

| trailing window | median | min | max |
|---|---|---|---|
| 30 days | 0.843 | 0.824 | 0.855 |
| 90 days | 0.890 | 0.888 | 0.896 |
| 180 days | 0.906 | 0.894 | 0.918 |
| 365 days | 0.906 | 0.901 | 0.928 |

Longer is better, flattening once past 180 days. Nothing to win here.

But read that last row against the composite. The recipe over 365 days scores
**0.906**. The shipped composite, over the same 365 days with the same weights,
scores **0.813**. The difference is the transform from a per-unit rate to a
0-100 signal. Holding the aggregates byte-identical:

| signal transform | median | min | max |
|---|---|---|---|
| clamped_linear (ships) | 0.840 | 0.832 | 0.872 |
| linear_minmax | 0.833 | 0.817 | 0.860 |
| **rank** | **0.906** | 0.901 | 0.928 |

**`_norm_map` scales linearly in value between the 5th and 95th percentile.
Replacing that with a percentile rank is worth +0.066 on the same inputs.**

Why: these rates are heavily right-skewed. Linear scaling packs most ZIPs into a
narrow band where measurement noise reorders them freely, while a rank transform
spreads them evenly and spends its resolution where the ZIPs actually differ.

**The clamp is not the culprit**, which was the other obvious guess. Clamping
scores slightly *better* than unclamped min-max, 0.840 against 0.833. It does
still have a real cost this measurement does not capture: on the latest scored
day it ties **exactly nine ZIPs at 100.0 on every one of the five signals**, and
9 to 20 more at 0.0. The top of every signal, which is the part the site is
about, carries no ordering at all.

## permit_intensity is 21% of the score and points the wrong way

It is the second-largest weight after LLC acquisitions. It is also:

- **negatively** correlated with future evictions (-0.131) and future LLC deeds
  (-0.096)
- the weakest predictor of the composite's own future recipe (0.263)

This is not necessarily a broken signal. Permit activity concentrates in
high-value areas where evictions are low, so an inverse relationship is real and
interpretable. But a signal that runs opposite to two of four outcomes should
not carry more weight than evictions and complaints combined. The weight was set
when the permit feed held 414 records. It holds 32,786 now, and this is the
first time anyone has asked what it is doing.

## What this does not say

The composite is a **summary**, and a summary legitimately trades forecasting
power for breadth: "under several kinds of pressure at once" is a real editorial
claim and no single signal makes it. Nothing here says the map is wrong.

What it does say is that the site should not imply the score forecasts, because
a reader asking "is my neighborhood getting worse" is better served by the
trailing 90-day trend than by the composite, on every outcome measured.

One caveat on the numbers: the reproduction of the live pipeline scores 0.840
where the live composite scores 0.813, so it is close but not exact, probably in
the per-unit denominators or the dormant-weight redistribution. The transform
comparison is unaffected, because those three rows share byte-identical inputs
and differ only in the function applied.

## Recommended, in order

1. **Change `_norm_map` to a percentile rank.** One function, +0.066, no new
   data. It rescales every score on the site, so it needs band recalibration
   (`project_score_bands`), a `score_history` backfill, and a look at the emails
   and the Spanish copy. It is a product decision, not a refactor.
2. **Recalibrate `WEIGHT_PERMITS`.** 0.21 on the only signal that runs opposite
   to evictions and deeds. This backtest is the target to fit against; there was
   not one before.
3. **Put the trailing 90-day trend on the page.** Persistence beat the composite
   on all four outcomes and on the composite's own recipe. That number is
   already computable from data on the box.
