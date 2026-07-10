# MTEK Press Pitch Re-verification + Bigger-Story Scan

Read-only check against the live DB on 2026-07-09, ahead of the David pitch. Supersedes the
2026-06-23 verification, which predates the 2026-06-24 ACRIS ingest by one day. No data changed.

## Data currency (state this honestly if asked)

| Feed | Latest record | Last ingest |
|---|---|---|
| ACRIS deeds | 2026-05-27 | 2026-06-24 |
| Evictions | 2026-07-07 | 2026-07-09 |
| HPD violations | 2026-07-07 | 2026-07-09 |

Safe public phrasing: "deed data current through late May; eviction and violation data current
through this week." The upstream ACRIS feed has been frozen ~43 days; do not volunteer that, but
the late-May watermark is the true answer.

## 1. 870 Belmont arc — HOLDS, with two required corrections

- **BBL is `3040400020`.** The draft notes circulating with `3040400032` are wrong; that BBL is
  274 Milford Street. Fix before handing over "document IDs and BBLs."
- Buy: MTEK NYC LLC, **2025-09-05, $712,775** (doc `2025091700354002`). Verified.
- Flip: to Fuad Omifisoye, **2026-02-27, $1,150,000** (doc `2026030500561001`). Gain **61.3%**. Verified.
- Gap: eviction 2025-04-07 → purchase 2025-09-05 = **5.0 months**. Verified.
- **"Two tenants were evicted" is not supported.** The two marshal dockets (142544, 145842) share
  the same execution date, the same case index (332669/23 / 332669/23K), and the same apartment
  ("UNIT 2, RIGHT SIDE EN"). This is one eviction event at one unit, recorded twice. A journalist
  pulling the court file will find one case. Say "a marshal executed an eviction" or "tenants were
  evicted" without a count.
- Known nuance (unchanged from June audit): MTEK bought through a same-week $10 pass-through
  (MCH SUB 1 LLC → Mariners Pac Ventures LLC → MTEK), not from the evicting owner.

## 2. "37 acquisitions" / "9 shells" — site-consistent but stale

Live page and API still say **37 acquisitions / 9 LLC entities** (operators row last rebuilt
2026-04-24). Raw deeds as of the 2026-06-24 ingest:

- **42 acquisition BBLs** across **11 buyer LLCs** (the 9 tracked plus MTEK CITY LLC and
  MTEK HENRY LLC; 39 BBLs fall within the tracked 9).
- Two further entities appear only as sellers (MTEK SALES LLC, MTEK BLUE LLC) → **13 MTEK-named
  entities total** in ACRIS.
- New acquisitions missing from the directory: 3048850019 (Fulton, 2026-03-17), 3013070034
  (Gold, 2026-03-19), 3011550007 (City, 2026-03-27), 3028310019 (Henry, 2026-04-16),
  3032950007 (City, 2026-05-12).
- The live page is also internally inconsistent: headline says 37, but its own
  "recent acquisitions" list renders 39 rows.

Pitch options: say "at least 37 acquisitions … at least 9 shells" (matches site, undercounts
honestly), or rebuild the directory to 42/11 and cite that. Do not cite 42/11 while the site
says 37/9.

Also: MTEK is a **flipper, not a holder** — 28 grantor-side deeds, including 10 of the 42
acquired BBLs already resold. Don't imply current ownership of 37 buildings.

## 3. "Five Brooklyn ZIP codes" — FALSE, will die on click-through

The site's own 37 parcels span **17 ZIP codes in 2 boroughs** (13 Brooklyn + 4 Queens:
11379, 11416, 11429, 11434). Raw 42 BBLs span 21 ZIPs. The operator page itself displays a
borough spread of 2. Replace with "more than a dozen ZIP codes across Brooklyn and Queens."

## 4. Bigger-story scan (citywide eviction → LLC buy ≤12mo → flip ≥25%)

17 arcs found. Standouts:

| Address | Ev. dates | Buy | Sell | Gain | Buyer scale |
|---|---|---|---|---|---|
| 104-06 104 Ave, Queens 11429 | 2025-02-07 (x2) | $292k 2025-05 | $969k 2025-09 | **+232%** | one-off (2 BBLs) |
| 388 Fenimore St, BK 11225 | 2025-03-27 (x3) | $714k 2025-06 | $1.74M 2026-03 | +144% | one-off |
| 708 Rockaway Pkwy, BK 11236 | 2025-04-10 (x3) | $500k 2025-06 | $1.175M 2025-12 | +135% | one-off |
| **4575 Furman Ave, BX 10470** | **2025-01-13 and 2025-04-21** | $500k 2025-07-08 | $999k 2025-09-30 | **+100% in 84 days** | **PHANTOM: 64 props, 32 shells** |
| 870 Belmont Ave, BK 11208 | 2025-04-07 (one case) | $712.8k 2025-09 | $1.15M 2026-02 | +61% | MTEK: 37–42 props |

**PHANTOM / 4575 Furman beats Belmont on every axis except flip recency:** two evictions on
genuinely distinct dates, purchase direct from the individual owner (Diane T. Miller — no
pass-through), a near-double gain in under three months, and a far bigger network (64 properties,
32 numbered shells; live page reads "65 acquisitions across 24 ZIP codes" and is consistent).
The higher-gain arcs above it are one-off LLCs with no network angle, i.e. weaker PulseCities
stories.

The scan itself is repeatable (single SQL over evictions_raw × ownership_raw) and is a candidate
"Eviction Flips" product feature / recurring press list.

## Verdict

The Belmont arc verifies. Before sending: fix the BBL, drop the "two tenants" count, fix the ZIP
claim, and hedge 37/9 as "at least" (or rebuild the directory first). Strongly consider pairing
Belmont with PHANTOM/Furman — "two operators, same playbook" turns an anecdote into a pattern,
which is both a better story and a better demo of the tool.

---

## Addendum 2026-07-10: directory rebuilt, five data-integrity bugs fixed

The rebuild surfaced and fixed real bugs, not just stale rows:

1. **Compound-brand fusion split MTEK entities** (`_operator_root`, three copies). The
   ICE CAP → ICECAP fusion rule also produced "MTEKGOLD"/"MTEKFORT" roots, silently dropping
   MTEK GOLD LLC (11 properties) and MTEK FORT GREENE LLC from the cluster. Fusion now fires
   only when the fused spelling exists as a standalone brand in the corpus (two-pass).
2. **BBL→ZIP lookup missed parcels-only BBLs**, deflating high-displacement percentages and
   silently dropping BREDIF below the 30% HD filter. `parcels` is now the authoritative source.
3. **Intra-cluster transfers counted as acquisitions.** BREDIF's LPA → REIT → issuer note hops
   inflated its acquisition count to 132. Transfers whose grantor resolves to the same operator
   root are now skipped (analysis + weekly diff).
4. **Allowlisted operators could be killed by noise heuristics.** PHANTOM (2.0 acquisitions/LLC
   by design) fell to the surname-cluster filter after dedupe. MTEK/PHANTOM/BREDIF now bypass
   the heuristic filters.
5. **Backfill wrote `public_operator` where every public surface gates on `operator`**, which
   404'd all operator pages on first rerun. Enum value now translated at write time. Also:
   `operator_parcels` is rebuilt from the exact parcel set behind the headline counts (carried
   in the JSON), so page tables can never drift from headlines again.

API fixes: operator property list LEFT JOINs parcels (no silently dropped rows);
eviction_then_buy requires doc_amount > 0 (a $0 note transfer after an eviction is not a buy).
Frontend: PATTERN_NOTES rewritten without driftable counts. config/mtek.py extended to 42 BBLs.

**Final live state (site = DB = what the pitch should say):**

| Operator | Properties | Acquisitions | LLC entities |
|---|---|---|---|
| MTEK | 42 | 42 | 11 |
| PHANTOM | 74 | 75 | 39 |
| BREDIF | 67 | 68 | 4 |

**New PHANTOM eviction-then-buy arcs surfaced by the fixes** (all real purchases, all Bronx):

| Address | Eviction | Purchase | Entity |
|---|---|---|---|
| 637 East 224 St, 10466 | 2025-09-11 | 2026-01-08, $1,450,000 | PHANTOM AFFORDABLE HOUSING LLC |
| 2937 Mickle Ave, 10469 | 2025-07-22 | 2026-01-23, $650,000 | PHANTOM CAPITAL BX LLC |
| 928 East 219 St, 10469 | 2025-07-10 | 2026-04-30, $550,000 | PHANTOM CAPITAL 18 LLC |

PHANTOM now has four buy-after-eviction properties on its live profile including Furman.
"PHANTOM AFFORDABLE HOUSING LLC buying $1.45M post-eviction" is a quotable detail on its own.

Updated pitch numbers: MTEK "42 acquisitions across 11 shells"; PHANTOM "74 properties through
39 numbered shells". Full test suite green after changes.
