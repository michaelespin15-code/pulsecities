# Morning Briefing — 2026-04-18

## Pipeline Runs

| Pipeline | Status | Generated |
|---|---|---|
| Weekly operator diff | Complete | 2026-04-18 (baseline: 2026-04-16) |
| Reno-eviction pipeline | Complete | 2026-04-18 |
| TOWNHOUSE investigation | Complete | 2026-04-18 |
| MELO investigation | Complete | 2026-04-18 |
| JOVIA investigation | Complete | 2026-04-18 |
| ARION investigation | Complete | 2026-04-18 |

**ACRIS ingest (2026-04-11 → 2026-04-18):** 70,677 new records across 60,358 unique BBLs, 49,284 unique parties. No ingest anomalies.

---

## Operator Findings: New Entrants This Cycle

The weekly diff expanded from a top-20 baseline to the full operator universe. 45 operators cleared the 10-property threshold this cycle. Four were individually investigated.

### Confirmed Real Operators

**TOWNHOUSE** — Brooklyn direct acquirer, add to nightly monitoring
- 27 properties | 5 LLCs | $95.6M recorded spend | 2.5 props/month
- 60.7% of properties in high-displacement zips (score ≥ 40)
- Geographic concentration: 11216 (ds 79.5), 11221 (ds 70.6), 11207/11233/11222
- LLC structure: roman-numeral fund sequence (RENTAL, II, VII, IX) consistent with per-fund capital raises
- 4 high-violation properties (≥3 Class B/C HPD violations); 286 Onderdonk Ave and 74 Bleecker St each have 6 B/C violations
- 0 eviction-then-buy matches
- Footprint overlaps PHANTOM CAPITAL's Brooklyn acquisition corridor

**MELO** — Bronx direct acquirer, add to nightly monitoring
- 10 properties | 2 entities | $7.2M recorded spend | 1.1 props/month
- 70% of properties in high-displacement zips — highest rate among this week's new entrants
- Primary entity is **MELO Z PHANTOM CAP LLC**, which embeds the PHANTOM CAPITAL brand name verbatim
- This entity is not in the current PHANTOM_ENTITIES list; if same principals, the PHANTOM portfolio is larger than reported
- 2 high-violation properties: 1936 Wallace Ave (10 B/C violations) and 2305 Loring Place North (3 B/C)
- 0 eviction-then-buy matches

### Confirmed False Positives

**JOVIA** — Federal credit union (Long Island), exclude from monitoring
- 12 BBLs, 16 records — all ASST (mortgage note assignments), zero DEED transfers
- Not an owner; holds/services mortgage paper
- High-displacement-zip rate (62.5%) reflects borrower geography, not operator behavior
- Fix: add to `_BANK_ROOTS`; consider adding `CREDIT` as a block token alongside BANK / MORTGAGE

**ARION** — Private mortgage fund / note investor, exclude from monitoring
- 12 BBLs, 15 records — all ASST, zero DEED transfers
- Entities include `ARION FUND LLC ISAOA/ATIMA` (mortgagee boilerplate for note assignments)
- Fix: add to `_BANK_ROOTS`; add `ISAOA` / `ATIMA` as party-name filters at extraction layer to catch all lender-side recordings pre-emptively

---

## Reno-Eviction Pipeline: 26 Stage-2 Buildings

7,475 buildings scanned in the 180-day acquisition / 90-day permit window.

| Stage | Count | Definition |
|---|---|---|
| Stage 1 | 7,449 | Acquired, no permits yet |
| **Stage 2** | **26** | **Acquired + active permits (renovation underway)** |
| Stage 3 | 0 | Acquired + permits + post-acquisition evictions |

No stage-3 signals. All 26 stage-2 buildings are in active renovation without recorded post-acquisition evictions. Notable buildings:

| Address | Zip | Acquiring Entity | Price | Permits |
|---|---|---|---|---|
| 280 Kent Ave | 11249 | DOMINO B PARTNERS LLC | N/A | 5 |
| 5601 13 Ave | 11219 | 5601 PROPERTIES LLC | N/A | 5 |
| 4 East 80 St | 10075 | 4E80NYC2025 LLC | $38,000,000 | 2 |
| 245 Eldridge St | 10002 | 245 ELDRIDGE LLC | $16,500,000 | 1 |
| 36-45 31st St | 11106 | 36-45 31ST STREET OWNER LLC | $7,000,000 | 2 |
| 411 7 Ave | 11215 | 411 7TH AVENUE LLC | $4,300,000 | 1 |
| 1422 East 15th St | 11230 | 1422EAST15THSTREET LLC | $1,362,000 | 2 |

280 Kent Ave (Williamsburg waterfront, 5 permits) and 5601 13 Ave (Borough Park, 5 permits) are the most active renovation sites. 4 East 80 St at $38M is the highest-priced acquisition in the cohort.

---

## Score Changes

No displacement score changes recorded this cycle.

---

## High-Signal Watchlist (unconfirmed, >60% high-displacement)

Three operators flagged by the diff have not yet been investigated:

| Operator | Props | LLCs | High-Disp % | Last Acquisition | Status |
|---|---|---|---|---|---|
| HAB | 10 | 3 | 72.7% | 2025-12-23 | Not investigated |
| ARGENTIC | 14 | 2 | 72.0% | 2026-03-26 | Not investigated |
| AMALGAMATED | 29 | 2 | 71.4% | 2026-03-20 | Not investigated (likely bank) |
| DEEPHAVEN | 11 | 2 | 64.3% | 2026-03-25 | Not investigated |
| ~~JOVIA~~ | ~~12~~ | ~~2~~ | ~~62.5%~~ | — | False positive — exclude |
| ~~ARION~~ | ~~12~~ | ~~3~~ | ~~60.0%~~ | — | False positive — exclude |

AMALGAMATED (29 properties, 71.4%) has "AMALGAMATED BANK" in its entity list — likely a bank false positive but volume warrants a quick check. HAB and ARGENTIC are the highest-priority unknowns.

---

## Data Health

- ACRIS pull: healthy, 70,677 records ingested
- Operator universe: 107 operators found, 63 with 10+ properties, 34 with 20+ properties
- No failed scraper runs, no missing pipeline outputs
- False-positive leakage confirmed: JOVIA and ARION both passed the 10-property threshold via ASST-only records; the `_BANK_ROOTS` and ISAOA/ATIMA fixes will prevent this class of noise in future runs

---

## Priorities for Today

1. **Add TOWNHOUSE and MELO to nightly operator monitoring** — both confirmed direct acquirers in high-displacement zips, clear upgrade from investigation to production tracking

2. **Fix false-positive leakage in `operator_network_analysis.py`**
   - Add JOVIA and ARION to `_BANK_ROOTS`
   - Add `CREDIT` as a block token (catches "FEDERAL CREDIT UNION", "FINANCIAL CREDIT UNION")
   - Add `ISAOA` / `ATIMA` as party-name substring filters at the ACRIS extraction layer

3. **Investigate HAB and ARGENTIC** — 72.7% and 72.0% high-displacement rates with small LLC counts; run the investigation script on both

4. **Quick-check AMALGAMATED** — 29 properties and 71.4% high-displacement but two entities both carry "BANK" in the name; likely a mortgage false positive but volume makes it worth a 2-minute doc-type check before dismissing

5. **Watch stage-2 reno-flip buildings** — 280 Kent Ave (5 permits, Williamsburg) and 5601 13 Ave (5 permits, Borough Park) are the most active; rerun pipeline in 2 weeks to catch any transition to stage 3

6. **MELO/PHANTOM connection** — pull the PHANTOM_ENTITIES list and check whether any PHANTOM principals appear in MELO Z PHANTOM CAP LLC registrations; if yes, the consolidated PHANTOM footprint needs to be updated in the public operator profiles
