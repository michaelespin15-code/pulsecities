# Feature Landscape

**Domain:** Civic intelligence map — displacement risk, property ownership, and public records for NYC
**Researched:** 2026-04-07

## Table Stakes

Features users expect. Missing = product feels incomplete.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Interactive map with neighborhood overlays | Core product surface; without it there is no product | High | MapLibre GL JS; GeoJSON neighborhood boundaries from NYC Open Data |
| Displacement risk score per block (1-100) | The stated core value proposition; every other feature serves this | High | Composite of permits, LLC acquisitions, eviction filings, complaint trends |
| 311 complaint data per block | Most-searched NYC civic dataset; renters expect to see it | Medium | Socrata API; normalize and store per block |
| Building permit filings (DOB) | Construction activity is a leading indicator of displacement | Medium | NYC Open Data DOB endpoint |
| Eviction filing data | Primary displacement signal; users will specifically seek this | Medium | NYC Open Data Housing Court data |
| Property ownership / LLC tracing (ACRIS) | Reveals who is buying; LLC shell companies are a known displacement pattern | High | ACRIS deed transfer records; LLC chain tracing is the hard part |
| DOF property assessment records | Tax/assessment spikes precede rent pressure; grounds the risk model | Medium | NYC DOF dataset via Socrata |
| Neighborhood-level summary view | Users navigate by neighborhood, not just block; need roll-up | Medium | Aggregate block scores to neighborhood polygon |
| Block-level drill-down | Journalists and organizers need parcel-level specificity | Medium | Click map → show all events on that block |
| Color-coded risk choropleth | Standard expectation for risk visualization; monochrome map is unusable | Low | MapLibre data-driven styling; red/orange/yellow/green gradient |
| Data freshness indicator | Users need to know if data is current; stale data erodes trust | Low | Last-updated timestamp per dataset on the map panel |

## Differentiators

Features that set PulseCities apart. Not expected, but valued once seen.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| LLC ownership chain visualization | Most tools show owner name; PulseCities shows the shell company network | High | Requires graph traversal across ACRIS deeds; primary research tool for journalists |
| Composite displacement score (not raw data) | Competing tools dump raw records; PulseCities synthesizes into a single actionable number | High | Weighting formula is the IP; start simple (linear), tune over time |
| Early signal detection (before press release) | Score rises before news coverage; positions PulseCities as a leading indicator | High | Only possible after 6-12 months of historical data accumulates |
| Per-scraper source breakdown in the score | Show which signals are driving the score (e.g., "permits +40, evictions +30") | Medium | Transparency builds trust with activist and journalist users |
| Time-series view of risk score per block | Show trend, not just current score; rising trend is more alarming than static high score | Medium | Requires storing historical score snapshots; deferred to v1.1 |
| Neighborhood comparison | "Bushwick vs. Ridgewood" — side-by-side for tenant organizing and reporting | Low | Composable from existing per-neighborhood data; UI work mainly |

## Anti-Features

Features to explicitly NOT build in v1.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| AI neighborhood summaries | Adds latency, cost, and hallucination risk on top of a foundation that doesn't exist yet | Build the data layer first; AI summary is a v2 layer on proven data |
| Sentinel-2 satellite change detection | GeoTIFF pipeline, cloud coverage filtering, and band analysis are a separate engineering track; adds weeks with unclear v1 ROI | Civic records alone tell the displacement story; defer to v2 |
| User accounts / saved searches | Auth adds surface area with no data quality benefit for the prototype | Keep it fully public and read-only; no login required |
| Mobile app | Map interaction is complex on mobile; desktop is where journalists and organizers work | Web-first; ensure viewport is responsive but don't optimize for mobile touch in v1 |
| Chicago and Los Angeles data | Schema and scrapers built for NYC; generalizing prematurely adds abstraction without feedback | Nail NYC, then generalize with real cross-city schema learnings |
| Real-time data streaming | NYC Open Data updates are daily at best; real-time adds infrastructure complexity for zero user-visible benefit | Nightly cron is correct for this data cadence |
| Paid data sources | Undermines the "100% public data" differentiator and creates ongoing cost/dependency | NYC Open Data, ACRIS, and DOF cover the required signals |
| Comment or annotation layer (social features) | Community annotation is a separate product; distraction from the intelligence core | Keep v1 read-only; user-generated content is a v2+ consideration |

## Feature Dependencies

```
PostgreSQL + PostGIS schema
  → 311 scraper
  → DOB permits scraper
  → Eviction filings scraper
  → ACRIS deed transfer scraper
  → DOF property assessment scraper
    → Displacement risk score computation (requires all 5 scrapers populated)
      → Neighborhood-level roll-up score
        → Risk choropleth map overlay
          → Block-level drill-down
            → Per-scraper score breakdown
              → LLC ownership chain visualization

Scheduler
  → All scrapers (orchestration dependency)

FastAPI endpoints
  → All frontend features (API must exist before map can render data)

MapLibre GL JS map (base layer)
  → All map-rendered features (risk overlay, drill-down, comparison)
```

## MVP Recommendation

Prioritize (end-to-end slice, one scraper first):

1. PostgreSQL + PostGIS schema — foundation for everything; nothing else can be built without it
2. One scraper end-to-end (recommend DOB permits — high signal, simpler API than ACRIS) — validates the full data pipeline with real data
3. Displacement risk score stub — even a single-signal score proves the scoring pipeline works
4. FastAPI endpoint for neighborhood and block data — exposes data to the frontend
5. MapLibre map with risk choropleth — makes the data visible; validates the full slice from data source to map

Then add remaining scrapers (311, evictions, ACRIS, DOF) and refine the composite score formula.

Defer:
- LLC ownership chain visualization: requires all ACRIS data plus graph traversal logic; high value but high complexity — Phase 2
- Time-series score view: requires historical snapshots to exist first — cannot build until data has been accumulating
- Per-scraper score breakdown UI: straightforward once composite score is working — add in the same phase as score tuning
- Neighborhood comparison view: low complexity, but only useful after multiple neighborhoods have real data — quick win in v1.1

## Sources

- NYC Open Data Socrata API: https://data.cityofnewyork.us/
- NYC ACRIS deed records: https://a836-acris.nyc.gov/
- NYC DOB permit data: https://data.cityofnewyork.us/Housing-Development/DOB-Permit-Issuance/ipu4-2q9a
- NYC eviction data: https://data.cityofnewyork.us/City-Government/Evictions/6z8x-wfk4
- MapLibre GL JS docs: https://maplibre.org/maplibre-gl-js/docs/
- Project context: /root/pulsecities/.planning/PROJECT.md
