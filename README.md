
# PulseCities

**What's actually changing in your neighborhood — before anyone else notices.**

PulseCities is a free public intelligence tool that combines NYC civic data, property ownership records, and satellite imagery into a single map. It surfaces displacement pressure, construction activity, and ownership patterns across every neighborhood in New York City.

## What it shows

- **Displacement Risk Score** — A composite 1–100 signal per ZIP combining permits, LLC acquisitions, eviction executions, and 311 complaint trends. Updated nightly.

- **Ownership Intelligence** — Deed transfers and LLC acquisition patterns from NYC ACRIS. Flags renovation-flip signals when an LLC buys a building and files a renovation permit within 60 days.

- **Neighborhood Pulse** — Recent LLC acquisitions and permit filings for a ZIP, showing specific addresses and dates — not just counts.

- **Building Lookup** — Search any NYC address to see its full civic event history: permits, evictions, complaints.

## Who uses it

Journalists investigating housing and displacement. Tenant organizations tracking neighborhood pressure. Urban planners and researchers. NYC residents who want to know what is happening on their block.

## Data sources

All data is public record.

- NYC Open Data — 311 complaints (erm2-nwe9), DOB permits (ipu4-2q9a), eviction executions (6z8x-wfk4)

- NYC ACRIS — Property deed transfers (bnx9-e6tj, 636b-3b5g, 8h5j-fqxa)

- NYC Department of Finance — Property assessments (w7rz-68fs)

- DHCR — Rent-stabilized building registrations (kj4p-ruqc)

## Stack

Python · FastAPI · PostgreSQL + PostGIS · MapLibre GL JS · DigitalOcean

## Status

Active development. Covers all NYC ZIP codes. Chicago and Los Angeles next.

## Built by

Michael Espin — CS/AI, Queens NY

