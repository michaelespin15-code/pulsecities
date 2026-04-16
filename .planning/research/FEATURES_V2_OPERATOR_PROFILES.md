# Feature Research: Operator Profile Pages for PulseCities v2.0

**Domain:** Civic data tool for displacement investigation & housing intelligence
**Researched:** April 16, 2026
**Confidence:** HIGH (verified via JustFix Who Owns What, civic tech interviews, HPD/DOF patterns)

## Executive Summary

Operator profile pages are the core investigative feature in displacement-tracking civic tools. The ecosystem shows clear patterns: **table stakes** are portfolio visibility, violation/eviction metrics, and ownership timeline. **Differentiators** include pattern detection (eviction-then-buy sequences), rent-stabilized unit tracking, and network visualization of related entities. The research is grounded in JustFix Who Owns What (33K monthly users, gold standard for NYC landlord investigation), ANHD's Displacement Alert Project, and HPD violation taxonomy. 

PulseCities' advantage: We can show not just *what* landlords own, but *patterns* in how they own—acquisition sequences, signal breakdowns by property, and RS unit loss within their portfolio. This is richer than existing tools.

## Feature Landscape

### Table Stakes (Users Expect These)

Features users assume exist on any operator profile. Missing these = tool feels incomplete or untrustworthy.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| **Operator name + basic info** | Users need to confirm they've found the right entity | LOW | Display legal entity name, alternate names (DBAs, LLC names), registration date |
| **Property portfolio list** | Core investigative feature; users need to see all buildings owned/managed | LOW-MEDIUM | Sortable/filterable table: address, BBL, acquisition date, current owner entity, # units, RS unit count |
| **Total portfolio metrics** | Users want portfolio summary before drilling in | LOW | Total buildings owned, total units, total RS units, # buildings with evictions, # with violations |
| **Building violations breakdown** | Violations are the primary harm indicator in displacement tracking | MEDIUM | Aggregate counts by class (A/B/C), open vs closed, latest violation dates; ACRIS-sourced, HPD-verified |
| **Eviction history** | Evictions are the leading displacement signal | MEDIUM | Count by property, total evictions filed by entity, eviction-to-acquisition timing patterns |
| **Timeline or ownership changes** | Users need to understand when acquisitions happened relative to displacement signals | MEDIUM | Chronological chart: property acquisition dates, ownership transfers, major evictions, violations |
| **Link to existing block drill-down** | Seamless navigation back to property details | LOW | Clicking a property in operator profile opens the existing block-level panel |
| **Operator search from address** | User workflow: find building → discover owner → see full portfolio | LOW-MEDIUM | Reverse lookup: when viewing a building in main map, show "View all buildings by [operator]" link |
| **Searchable operator names** | Users find operators by name (tenant networks, reporting) | MEDIUM | Full-text search on operator legal name + DBAs, with autocomplete; indexed daily |

### Differentiators (Competitive Advantage)

Features that separate PulseCities from Who Owns What and other civic tools.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Eviction-then-acquisition pattern detection** | Reveals predatory acquisition sequences: identify buildings where owner evicted tenants, then bought the property | HIGH | Query ACRIS for ownership change date, OATH eviction table for filings <12 months prior; highlight matches with "acquired post-eviction" label |
| **Rent-stabilized unit loss per property** | Shows cumulative RS deregulation within a portfolio: which operators are losing RS units, and where | HIGH | Join RS building list to portfolio; calculate unit loss delta over time (RSU count by year); show % of portfolio that's RS-exempt |
| **Signal breakdown by property** | Not just violations—show which of the 6 PulseCities signals fired for each property | MEDIUM | For each property: LLC acquisitions (yes/no), permits filed (count), 311 complaints (count), evictions (count), RS unit loss (count), assessment spike (yes/no) |
| **Acquisition timeline chart** | Interactive timeline: when did operator buy buildings, in what sequence, which parcels were acquired together | HIGH | Gantt-style or dot chart: time axis, each property as a row, colored by acquisition type (market purchase vs deed transfer) |
| **Building violation class distribution** | Visual breakdown: what % of operator's portfolio has Class A, B, C violations open | MEDIUM | Pie or stacked bar chart; sortable by severity; helps identify systemic negligence patterns |
| **Eviction + violation correlation** | Show: for properties with evictions, what violations preceded or followed them | HIGH | Network or scatter plot: eviction date vs violation date; reveals whether operator uses violations as eviction tool |
| **Network view of related entities** | Visual map of shell company relationships: if operator owns via multiple LLCs, show those connections | HIGH | Graph visualization (node = entity, edge = shared address/managing member); use JustFix's graph-theory algorithm as reference |
| **Operator ranking by signal intensity** | Comparative view: top 5 operators by evictions, violations, RS loss, or combined "displacement intensity" score | MEDIUM | Leaderboard or comparison table; contextualizes operator within cohort of largest NYC landlords |
| **Downloadable portfolio export** | CSV/JSON export of operator's full property list with signals | LOW | Enables community organizers, lawyers, researchers to use data in their workflows (maps, organizing campaigns, litigation) |

### Anti-Features (Avoid These)

Features that seem useful but create problems in practice.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| **Real-time signal updates per property** | "Show me new evictions the day they're filed" | Eviction data is 2-6 weeks behind real filing; creates false urgency and expectation of live data we can't deliver | Show data freshness date prominently; publish weekly data refresh cycle (e.g., "Updated Tuesdays from DOE/OATH records") |
| **Predictive risk scores for operator future behavior** | "Which operator will evict next?" | No predictive model is bulletproof; wrong predictions erode trust; liability risk if operators contest findings | Stick to observable patterns (historical evictions, acquisitions); let users draw their own conclusions |
| **Blame-focused operator narrative** | "This landlord is evil, don't rent here" | Creates legal liability (defamation), makes tool unusable for researchers/lawyers who need neutral data; politicizes data | Present data neutrally; let users interpret; provide context without judgment |
| **Building-by-building recommendations (e.g., "risky building")** | "Should I move out?" | Outside scope of civic data tool; creates duty-of-care liability; better handled by tenant advocates | Link to tenant rights resources; don't editorialize on individual buildings |
| **Operator contact info or "report this landlord" buttons** | "I want to harass this operator directly" | Enables harassment, vigilantism, doxing; violates data ethics; could expose tool to legal liability | Don't include personal contact info; focus on institutional accountability mechanisms |

## Feature Dependencies

```
[Operator search + basic portfolio list]
    └──requires──> [Address/BBL → operator resolution] (already built in v1.0)
                        └──requires──> [ACRIS ownership data pipeline]
                        
[Operator profile page]
    └──requires──> [Operator search feature]
    └──enhances──> [Existing block drill-down panel] (reverse link: "View operator portfolio")
    
[Eviction-then-buy pattern detection]
    └──requires──> [Eviction + acquisition timeline] (depends on OATH + ACRIS tables)
    └──requires──> [Date proximity matching] (< 12 months)
    
[RS unit loss tracking]
    └──requires──> [RS building list ingestion] (HPD/DOE data)
    └──requires──> [Historical RS unit counts per year]
    
[Operator network visualization]
    └──requires──> [LLC relationship mapping] (graph-theory entity resolution)
    └──requires──> [Database schema for entity links]
    
[Operator map layer]
    └──requires──> [Operator profile pages] (so clicking map shows profile)
    └──requires──> [MapLibre layer rendering] (tiles or GeoJSON with operator colors)
    └──enhances──> [Existing search functionality] (visual discovery of operator portfolios)
    
[Signal breakdown per property]
    └──requires──> [All 6 signals already built in block panel]
    └──requires──> [Per-property signal aggregation] (query layer)
    
[Operator leaderboard/comparison]
    └──requires──> [Portfolio metrics + rankings]
    └──enhances──> [Operator profile page] (contextual comparison)
```

### Dependency Notes

- **Operator search + profile requires address → operator resolution:** Already built in v1.0 (BBL lookup via DOF). This is the foundation.
- **Eviction-then-buy detection requires OATH + ACRIS synchronization:** Must have reliable join on property + date ranges.
- **RS unit loss requires historical tracking:** Single snapshot doesn't show loss; need year-over-year comparisons (computationally heavier).
- **Network visualization requires entity resolution:** Most complex dependency; JustFix uses daily graph-theory runs; consider starting simple (same-address LLC clustering) and evolving.
- **Operator map layer enhances discovery:** Should launch after profile pages are stable; complements existing block-level map.

## MVP Definition

### Launch With (Operator Profiles v2.0)

**Core operator profile page with searchability:**
- [x] Operator name + legal entity info (name, registration, known DBAs)
- [x] Searchable operator discovery (search bar autocomplete, reverse lookup from property)
- [x] Portfolio table: address, BBL, acquisition date, current entity, RS unit count, # open violations, # evictions filed
- [x] Portfolio summary metrics: total buildings, total units, total RS units, avg violations/building, total evictions filed
- [x] Violation breakdown (Class A/B/C counts, open vs closed status)
- [x] Timeline chart: acquisition + transfer dates, major eviction/violation events
- [x] Link back to block-level drill-down (click property → existing panel opens)
- [x] Eviction history: total filed, count by property, trend

**Why essential:** Users need to move from "I found a building" to "I see everything this entity owns." These features answer the core question: "What's the pattern?"

### Add After Validation (v2.1 - v2.2)

**Differentiator features, once core is stable:**
- [ ] Eviction-then-buy pattern detection: flag properties where acquisition followed recent evictions
- [ ] RS unit loss tracking: show % of portfolio that's RS, trend over time
- [ ] Signal breakdown per property: (LLC acquisitions, permits, 311, evictions, RS loss, assessment spike)
- [ ] Operator map layer: toggle to see all properties owned by operator, colored by owner
- [ ] Operator leaderboard: top 10 operators by evictions, violations, RS loss

**Trigger for adding:** Post-launch feedback from organizers, tenant advocates, legal teams; validation that profile pages drive investigations.

### Future Consideration (v3+)

**Complex/speculative features:**
- [ ] Entity network visualization (shell company relationships)
- [ ] Predictive risk scoring (future behavior prediction) — **DO NOT BUILD** (liability, model risk)
- [ ] Building-by-building risk ratings — **DO NOT BUILD** (scope creep, liability)
- [ ] Operator contact info or harassment pathways — **DO NOT BUILD** (ethics, legal)

**Why defer:** Require deeper research, more data infrastructure, or carry legal/ethical risks. Better to establish core product first, then decide based on user needs.

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority | Notes |
|---------|------------|---------------------|----------|-------|
| Operator search + profile page | HIGH | MEDIUM | P1 | Core investigative feature; unlocks use cases |
| Portfolio property list + metrics | HIGH | LOW | P1 | Table stakes; most-used section of Who Owns What |
| Violation breakdown by class | HIGH | MEDIUM | P1 | Primary harm indicator in displacement tracking |
| Timeline (acquisition + events) | HIGH | MEDIUM | P1 | Shows predatory patterns over time |
| Eviction history + counts | HIGH | LOW-MEDIUM | P1 | Tied to 6-signal model; must have |
| Searchable operator discovery | HIGH | MEDIUM | P1 | Critical UX; reverse lookup from address |
| Link to block-level details | HIGH | LOW | P1 | Seamless navigation; already have infrastructure |
| Eviction-then-buy detection | MEDIUM-HIGH | HIGH | P2 | Differentiator; reveals predatory acquisition |
| RS unit loss tracking | MEDIUM-HIGH | HIGH | P2 | Shows systemic deregulation; complex queries |
| Signal breakdown per property | MEDIUM | MEDIUM | P2 | Nice-to-have; can derive from existing data |
| Operator map layer | MEDIUM | MEDIUM | P2 | Visual discovery; enhances but not essential |
| Entity network visualization | MEDIUM | HIGH | P3 | Complex; requires entity resolution research |
| Operator leaderboard | MEDIUM | LOW-MEDIUM | P2 | Contextual comparison; add after P1 stable |
| Downloadable CSV/JSON export | MEDIUM | LOW | P2 | Researcher/advocate workflows; low-effort |

**Priority key:**
- **P1 (Must have for launch):** Operator profile pages without these are incomplete. Users can't investigate operationally.
- **P2 (Should have, add v2.1–v2.2):** Differentiators that lock in competitive advantage. Add after P1 validation.
- **P3 (Nice to have, future):** Speculative or complex. Defer until product-market fit is proven.

## Competitor Feature Analysis

| Feature | Who Owns What (JustFix) | Displacement Alert (ANHD) | Our Approach (PulseCities) |
|---------|------------------------|-----------------------|--------------------------|
| Operator/landlord profile page | Yes, searchable; timeline + portfolio | District-level risk, not operator-focused | Yes, with signal breakdown + eviction-buy detection |
| Property portfolio list | Yes, with violations/evictions | N/A (neighborhood focus) | Yes, plus RS unit counts + signal per property |
| Violation breakdown | Yes, by class (A/B/C) | Yes (violations as signal) | Yes, same; plus correlation to evictions |
| Acquisition timeline | Yes, monthly HPD violations | No | Yes, plus evictions + major events |
| Eviction history | Yes, total count + per-property | Yes (evictions as signal) | Yes, plus acquisition timing correlation |
| RS unit loss tracking | No | No | **Yes (differentiator)** |
| Eviction-then-buy detection | No | No | **Yes (differentiator)** |
| Network graph of related entities | Yes, using graph theory (2022+) | No | Planned for v3; start simple, evolve |
| Searchable operator names | Yes | No | **Yes, with reverse lookup from property** |
| Signal breakdown per property | No | No | **Yes (differentiator)** |
| Interactive map layer of operator properties | No | No | **Yes (planned v2.1)** |
| Downloadable portfolio data | No | Via district report PDFs only | **Yes, CSV/JSON** |

**Strategic positioning:** PulseCities v2 aims to be "What Who Owns What does for landlord portfolios, but with better signal integration (RS units, eviction-buy patterns) and richer per-property context from our 6-signal model."

## Implementation Notes by Feature

### P1 Features: Core Operator Profile (Phase 1)

**Operator Search + Profile Page**
- Database layer: Denormalized operator view with aggregated portfolio metrics (pre-computed nightly)
- Search endpoint: Full-text index on operator legal name + DBAs + manager entity names
- Reverse lookup: One-click "View all properties by [operator]" from block drill-down
- Frontend: Single-page operator profile; tabs for portfolio, timeline, metrics

**Portfolio Table**
- Query: JOIN operator_portfolio → properties → signals; sort by acquisition date, violations, RS units
- Pagination: Show 20–50 properties per page (portfolios range 1–3K+ buildings)
- Filters: By acquisition year, violation class, eviction status, RS unit count

**Violation Breakdown**
- Aggregate by class (A/B/C): Count open + closed violations
- Display: Cards showing Class A: 120 open (8 closed), etc.
- Source: HPD dataset (already ingested in v1.0); join to operator portfolio

**Eviction History**
- Total evictions filed by entity (OATH Housing Court data)
- Timeline: Count by year (bar chart or table)
- Per-property: In portfolio table, show "# evictions filed" column

**Timeline Chart**
- X-axis: Date (monthly buckets, 5+ years historical)
- Y-axis: Acquisition count (or cumulative building count)
- Events overlay: Major evictions, violations spikes, ownership transfers
- Interaction: Click event to drill into properties from that timeframe

### P2 Features: Differentiators (Phase 2)

**Eviction-then-Buy Detection**
- Query logic: For each property in operator portfolio, check if evictions filed <12 months before acquisition
- Flag: Add "Acquired post-eviction (2023)" badge in portfolio table
- Visualization: Timeline highlights properties matching this pattern
- Research threshold: Recommend starting with 12-month window; tune based on feedback

**RS Unit Loss Tracking**
- Data source: HPD rent-stabilized building list (annual snapshot)
- Calculate: RS units in operator portfolio year-over-year; % of portfolio that's RS
- Display: Summary metric ("45% of portfolio is rent-stabilized, down from 48% last year")
- Per-property: In portfolio table, show RS unit count + year-over-year delta

**Signal Breakdown per Property**
- Extend portfolio table: For each property, show binary/count for each of 6 signals
  - LLC acquisitions: Yes/No (recent ACRIS transfer involving LLC)
  - Permits: Count (DOB filings, recent 2 years)
  - 311 complaints: Count (recent 1 year)
  - Evictions: Count (OATH filings by owner, recent 2 years)
  - RS unit loss: Delta (RSU count delta YoY)
  - Assessment spike: Yes/No (DOF assessed value spike >20% YoY)
- Display: Icon or colored cell for each signal (visual scanability)

### P3 Features: Complex/Deferred (Phase 3+)

**Entity Network Visualization**
- Dependency: LLC entity resolution (graph-theory clustering)
- Approach: Start simple—cluster LLCs by shared managing member address or business address
- Visualization: Node-link diagram; nodes = entities, edges = shared address/manager
- Reference: JustFix's implementation (daily graph runs); consider simpler weekly run initially

## Data Quality & Sourcing Notes

### Operator Name / Entity Identification
- Primary: DOF Property Tax owner; ACRIS deed grantor/grantee
- Challenge: Multiple legal entities for same operator (e.g., Phantom Capital LLC vs PHANTOM CAPITAL HOLDINGS)
- Approach: Daily entity resolution pass; cluster by address, managing member name, phone number
- Fallback: Manual mapping / curated list of known aliases (MTEK, PHANTOM CAPITAL, BREDIF, etc.)

### Acquisition Date Reliability
- Source: ACRIS deed transfer date
- Risk: Deed date may lag actual acquisition by months; DOF assessment date may be more reliable for some deals
- Approach: Use ACRIS date as primary; cross-check with DOF assessment date if >6 month delta

### Eviction-then-Buy Pattern
- Challenge: Event timing is critical; <12 months is an assumption, not a law
- Validation: Manual spot-check first 10–20 matches; adjust threshold if needed
- Caveat: Not causal; just temporal correlation; always present as pattern observation, not accusation

### Rent-Stabilized Unit Loss
- Source: HPD rent-stabilized building list (annual), joined to operator portfolio
- Challenge: RS status is property-level, not unit-level; can't calculate exact unit loss without deed records
- Approach: Treat as proxy; if building drops off RS list, assume all units deregulated
- Validation: Compare to public datasets (Housing Court, tenant advocacy reports)

---

## Sources

**Civic Tools & Precedent:**
- [Who Owns What (JustFix NYC)](https://whoownswhat.justfix.org/en/about) — Gold standard for NYC landlord investigation; 33K monthly users; timeline + portfolio features
- [Who Owns What Timeline Feature](https://medium.com/justfixorg/who-owns-what-timeline-feature-tracking-landlord-behavior-over-time-7aae50461e12) — Tracks HPD violations, ownership changes, maintenance issues
- [Untangling NYC's Web of Real Estate (JustFix 2022 release)](https://medium.com/justfixorg/untangling-nycs-web-of-real-estate-who-owns-what-s-latest-release-b22aac917617) — Graph-theory entity resolution for shell company networks
- [ANHD Displacement Alert Project](https://anhd.org/blog/anhd-releases-new-district-level-tenant-displacement-risk-tracking-tool/) — District-level displacement risk tracking; evictions + violations as signals
- [NYC Open Data & HPD resources](https://opendata.cityofnewyork.us/) — Data sources for violations, evictions, owner info
- [JustFix GitHub (Who Owns What)](https://github.com/JustFixNYC/who-owns-what) — Open-source reference for implementation patterns

**Violation Classification & Harm Indicators:**
- [HPD Violation Classes A, B, C (2026 Guide)](https://dobguard.com/resources/hpd-violation-classes-explained) — Class definitions (non-hazardous, hazardous, immediately hazardous), timeframes
- [Building Violation Investigation Guide](https://streetsmart.inc/ny/guide/hpd-violations-explained/) — Typology; remediation compliance patterns
- [NYC DOB Violations Resource](https://violationwatch.nyc/2025/10/20/nyc-building-violations-lookup/) — Lookup methodology; compliance tracking

**Displacement & Predatory Practices:**
- [UCLA Study: Predatory Corporate Landlords](https://www.housingisahumanright.org/predatory-corporate-landlords-target-black-tenants-for-eviction-says-new-ucla-study/) — 2025 research on eviction-then-acquisition patterns; corporate REIT behavior
- [Milwaukee Predatory Landlord Patterns](https://www.milwaukeeindependent.com/explainers/predatory-landlords-profit-milwaukee-eviction-corridors-become-high-turnover-business-zones/) — Acquisition after eviction; bulk purchase strategies
- [Housing Data Coalition](https://www.housingdatanyc.org/) — NYC community-focused displacement tracking
- [Guide to Measuring Neighborhood Change (Urban Institute)](https://www.urban.org/sites/default/files/publication/100135/guide_to_measuring_neighborhood_change_to_understand_and_prevent_displacement.pdf) — Framework for displacement indicators

**Technical & Real Estate Data:**
- [ACRIS (NYC Property Records)](https://regwatch.nyc/guides/how-to-search-acris-records) — Property transfer, deed, mortgage data; BBL structure
- [NYC Property Lookup & DOF Registry](https://regwatch.nyc/nyc-property-owner-search) — Ownership resolution; legal entity identification
- [Real Estate Data Visualization Best Practices](https://www.dealpath.com/blog/real-estate-dashboards/) — Portfolio visualization patterns; dashboard design
- [Landlord Studio Portfolio Analytics](https://help.landlordstudio.com/en/articles/7947979-the-landlord-studio-dashboard) — Example of portfolio metrics (portfolio summary, P&L, property filtering)

---

**Feature research for:** PulseCities v2.0 — Operator profile pages
**Researched:** April 16, 2026
**Confidence level:** HIGH — Grounded in existing civic tool patterns (Who Owns What verified), NYC data taxonomy (HPD/DOF/ACRIS), and displacement research (UCLA, ANHD, Housing Data Coalition)
