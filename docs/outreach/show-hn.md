# Show HN post

Post from your own account, weekday morning ET. Stay in the thread for the
first few hours; HN rewards authors who answer questions.

**Title:** Show HN: PulseCities – NYC displacement signals from public records

**URL:** https://pulsecities.com/

---

I built PulseCities to answer a question I kept having about NYC housing:
when a neighborhood starts to flip, what does the public record show, and how
early does it show it?

What it does: nightly ingest of six NYC open datasets (ACRIS deeds, DOB
permits, marshal evictions, HPD violations, 311 complaints, DHCR
rent-stabilization counts), a composite displacement-pressure score for 177
ZIP-level neighborhoods, and entity resolution that groups LLC shells into
operator networks. Everything is server-rendered and public: neighborhood
pages, per-building pages, an evictions tracker, and operator profiles with
downloadable paper-trail CSVs.

The finding that made it feel worthwhile: the deed record shows a repeatable
arc of marshal eviction, LLC purchase months later, resale at a markup.
Seventeen documented cases so far, each with ACRIS document IDs so you can
verify without trusting me: https://pulsecities.com/press

Stack: Python/FastAPI + Postgres on a single 4GB VPS, MapLibre for the map,
no framework on the frontend. The interesting problems were entity
resolution (LLC name normalization is a swamp: compound brands, surname
clusters, intra-network transfers that look like acquisitions) and data
honesty (upstream feeds freeze without notice, so every page states what
window its data actually covers).

Limitations, honestly: scores are risk indicators, not predictions; agencies
publish on a lag; the ACRIS feed freezes periodically and the site labels
its own staleness when it does.

Happy to answer questions about the pipeline, the scoring, or the records.

---

## Expected questions, so the answers are ready

- "Why ZIP codes and not census tracts?" ZIPs are what every source shares
  and what people actually search. Tract-level is on the list; the tradeoff
  is joinability
- "Couldn't landlords game this?" The inputs are their own public filings.
  Gaming it means filing fewer deeds and permits, which is the outcome
  tenants would want
- "Privacy?" Everything shown is already a public record published by the
  city; the site adds context, not new disclosure
- "Business model?" Free public tool. If it ever monetizes, it would be an
  API tier for institutions, never a paywall on the public pages
