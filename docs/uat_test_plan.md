# PulseCities — User Acceptance Test Plan

**Site**: https://pulsecities.com  
**Plan version**: 2026-04-18

---

## How to use this document

Each test scenario is self-contained. Open the URL in a browser and follow the steps listed.
The scenario includes the exact URL to start from, the steps to take, what to assert, and what
failure looks like. Results can be logged in the "Known Issues and Acceptable Tradeoffs" section
at the bottom.

---

## Personas

**P1 — Investigative Journalist**  
Reporter at The City NYC or Gothamist covering housing and landlord accountability. On deadline,
needs to verify findings quickly, cite sources accurately, and share specific evidence with editors.
Arrives via direct links. Not interested in exploration — they need to confirm a specific thing.

**P2 — Housing Advocate / Tenant**  
Tenant organizer or researcher at ANHD, Legal Aid, or JustFix. Research workflow, not deadline
pressure. Needs to understand operator portfolios, find displacement patterns in their service
area, and export findings for reports or tenant outreach.

**P3 — First-Time Visitor**  
Clicked a link from Twitter or LinkedIn with no prior context. Needs to understand what the tool
is and whether to trust it within 30 seconds. No housing policy background assumed.

---

## Test Scenarios

---

### TS-01: Journalist arrives via direct operator link

**Persona:** P1 — Investigative Journalist  
**User goal:** Verify an operator's portfolio before publishing a story about MTEK  
**Entry point:** `https://pulsecities.com/operator/MTEK` (arrived via email link from a source)

**Expected journey:**
1. Page loads and shows operator name "MTEK" in the header
2. Subtitle reads "Operator cluster — 18-month ACRIS window · public records only"
3. Stats grid shows total properties, acquisitions, LLC entity count, and ZIP codes targeted
4. Pattern note section appears with narrative about MTEK's acquisition behavior
5. LLC entities list is visible (collapsible panel)
6. Recent acquisitions table shows rows with address, ZIP, buyer LLC, date, and dollar amount
7. Source attribution reads "Source: NYC ACRIS"

**Network requests to verify:**
- `GET https://pulsecities.com/api/operators/MTEK` — must return HTTP 200
- Response must include: `operator_root`, `llc_entities` (array), `total_properties`, `total_acquisitions`, `zip_codes`, `recent_acquisitions`

**Success criteria:**
- Page title reads "MTEK — PulseCities Operator Profile"
- `<link rel="canonical">` points to `https://pulsecities.com/operator/MTEK`
- `total_properties` shown as a number (expect ~37)
- At least one LLC entity name contains "MTEK" (e.g., "MTEK FORT GREENE LLC")
- At least one acquisition row is visible in the table with a valid date (YYYY-MM-DD format)
- Pattern note is visible above the acquisitions table
- Page is readable without horizontal scrolling at 1280px viewport width

**Failure modes:**
- Page shows "404 / Operator not found" — API root mismatch or missing data
- Stats grid shows all zeros — API returned empty or failed silently
- Acquisitions table shows "No acquisitions found" for an active operator — 18-month window query failure
- Pattern note is missing — hardcoded content block omitted in template
- Page title is generic ("PulseCities") — dynamic title injection not working

**Accessibility checks:**
- Tab through page in order: nav links → stats grid → entities toggle → table rows
- Screen reader announces "MTEK" as heading, stats as labeled values
- Color contrast: orange operator name (#f97316 or similar) against dark background must pass WCAG AA (4.5:1 for normal text)
- Table headers are `<th>` elements with scope attributes

**Mobile behavior (iPhone SE, 375px):**
- Stats grid stacks vertically (2×2 or 4×1)
- Table is horizontally scrollable or columns collapse to key fields only
- LLC entity list is still expandable via tap
- Pattern note does not overflow viewport

**Screenshots needed:**
- Full page above fold (hero stats + operator name)
- Acquisitions table with at least 3 visible rows
- Mobile layout of stats grid

---

### TS-02: Journalist cites a specific eviction finding

**Persona:** P1 — Investigative Journalist  
**User goal:** Find a specific eviction event in a neighborhood and confirm it can be cited with date and address  
**Entry point:** `https://pulsecities.com/` then navigate to ZIP 11221

**Expected journey:**
1. Load homepage
2. Click ZIP 11221 (Bushwick/Bed-Stuy boundary, Brooklyn) from the map or top-risk sidebar list
3. Neighborhood detail panel opens
4. Scroll to "Neighborhood Pulse" section
5. Locate "Recent evictions" subsection within the pulse panel
6. Each eviction row shows address, eviction type, and executed date
7. Verify the data lag notice is present near the evictions section

**Network requests to verify:**
- `GET /api/neighborhoods/11221/score` — displacement score and signal breakdown
- `GET /api/neighborhoods/11221/pulse` — recent evictions, acquisitions, permits in last 90 days

**Success criteria:**
- At least one eviction is shown, or an explicit empty state ("No recent evictions in the last 90 days")
- Each eviction entry includes: street address, eviction type (residential/commercial), executed date in human-readable format
- Lag notice is present: "Eviction filings typically appear 2–4 weeks after they occur."
- Executed dates are plausible (not in the future, not older than 90 days from today)
- The data source (NYC Open Data) is referenced in the methodology link or inline

**Failure modes:**
- Pulse section shows a spinner indefinitely — `/pulse` API failure
- Eviction rows show "null" for address or date — data normalization failure
- No lag notice — omitted from template, journalist may over-cite stale data
- Dates shown as raw ISO strings without formatting (e.g., "2026-03-12T00:00:00Z")

**Accessibility checks:**
- Eviction list items have sufficient color contrast for type labels
- List is navigable by keyboard (not requiring mouse hover to read)

**Mobile behavior:**
- Pulse sections stack vertically: acquisitions → permits → evictions
- Eviction rows wrap cleanly at 375px without truncating dates

**Screenshots needed:**
- Neighborhood detail panel showing the pulse section with eviction entries
- Close-up of the data lag notice

---

### TS-03: Journalist exports data for an article

**Persona:** P1 — Investigative Journalist  
**User goal:** Copy a landlord's portfolio list as plain text to paste into a spreadsheet or story notes  
**Entry point:** `https://pulsecities.com/` then use landlord search

**Expected journey:**
1. Load homepage
2. Find the landlord portfolio search box in the sidebar
3. Type "PHANTOM" (min 3 characters)
4. Press Enter or click Search
5. Results load: list of properties with address, ZIP, buyer LLC, date, and dollar amount
6. Summary line shows total property count, unique ZIP codes, LLC name count, estimated value
7. Click the "Copy" button
8. Clipboard now contains plain text with header, attribution, and one property per line

**Network requests to verify:**
- `GET /api/search/landlord?q=PHANTOM` — must return HTTP 200
- Response includes: `query`, `summary` (object), `results` (array up to 50)

**Success criteria:**
- Results appear within 3 seconds of search submission
- Summary line reads approximately: "Found N properties across M zip codes — K LLC names — ~$X.XM in acquisitions"
- Copy button changes to "Copied!" for ~2 seconds after click
- Pasting the clipboard content shows:
  - Header: "PHANTOM Portfolio — N properties across M zip codes"
  - Attribution: "Identified via NYC ACRIS public records by PulseCities (pulsecities.com)"
  - One row per property: "{address}, {zip} — {buyer_name} — {date} — ${amount}"
- Map updates to show pulsing circles at property coordinates

**Failure modes:**
- Search returns 0 results for "PHANTOM" — known active operator, indicates query failure
- Copy button is absent or throws clipboard permission error
- Clipboard content has garbled formatting or missing attribution line
- Map layer does not update (landlord-results GeoJSON source not refreshed)

**Accessibility checks:**
- Search input has label or aria-label
- Copy button has visible focus ring and announces "Copied!" to screen readers via aria-live

**Mobile behavior:**
- Search box and button are full-width on 375px viewport
- Results list is scrollable without horizontal overflow
- Copy button is reachable without zooming

**Screenshots needed:**
- Search results panel with summary line and at least 5 property rows
- Map view with pulsing circles from search results
- "Copied!" state of the copy button

---

### TS-04: Journalist verifies methodology claims

**Persona:** P1 — Investigative Journalist  
**User goal:** Confirm PulseCities' data sources and scoring algorithm before citing the site in an article  
**Entry point:** `https://pulsecities.com/methodology`

**Expected journey:**
1. Navigate directly to the methodology page
2. Read the scoring algorithm section — 6 signals, weights, time windows
3. Verify all data sources are listed with NYC Open Data dataset IDs
4. Find the update frequency disclosure
5. Confirm assessment spike signal is noted as currently dormant
6. Check the tech stack and author attribution

**Network requests to verify:**
- No API calls required — this is a static page
- `GET /methodology` — must return HTTP 200 with complete HTML

**Success criteria:**
- Page title: "How PulseCities Works — NYC Displacement Risk Methodology"
- `<link rel="canonical">` points to `https://pulsecities.com/methodology`
- OG tags present: `og:type` = "article", `og:title`, `og:description`
- JSON-LD structured data block present with `@type: "TechArticle"`, headline, author name "Michael Espin"
- Scoring section lists all 6 signals with weights summing to approximately 100%
- Assessment spike signal is explicitly marked as dormant or not yet active
- Data sources section includes NYC Open Data IDs for at least: 311 (erm2-nwe9), DOB Permits (ipu4-2q9a), HPD Violations (wvxf-dwi5), ACRIS (bnx9-e6tj), Evictions (6z8x-wfk4)
- ACRIS 2-week reporting lag is disclosed
- Eviction 2-4 week lag is disclosed
- Update frequency: nightly at 2am UTC
- Score thresholds documented: 0-25 Low, 26-50 Moderate, 51-80 High, 81-100 Critical
- Bonus signal values documented: +8 pts HPD Speculation Watch List, +4 pts portfolio >20 props, +3 pts lapsed registration
- Author contact (Michael Espin) and email present in footer or contact section
- Plausible analytics script is loading (privacy-friendly, not Google Analytics)
- Page loads without JavaScript errors in browser console

**Failure modes:**
- Page returns 404 — route not configured
- JSON-LD block malformed — structured data validator would reject it
- Dormant signal (assessment spike) shown as active — misleads about score accuracy
- NYC Open Data IDs missing — journalist cannot independently verify sources
- No author attribution — reduces credibility for citation

**Accessibility checks:**
- All section headings use proper `<h2>`, `<h3>` hierarchy
- Data source table (if present) has column headers
- Color-coded score ranges have text labels, not just color
- Sticky nav does not obscure content when navigating by anchor link

**Mobile behavior:**
- Sticky nav collapses or scrolls away on mobile
- Score range table or list is readable at 375px
- Tech stack chips wrap without overflow

**Screenshots needed:**
- Scoring algorithm section with all 6 signals visible
- Data sources section with NYC Open Data IDs
- JSON-LD block in browser DevTools (Sources or Elements panel)

---

### TS-05: Journalist shares a specific neighborhood link

**Persona:** P1 — Investigative Journalist  
**User goal:** Share a link to ZIP 10030 (Central Harlem) with an editor so they can see the same data  
**Entry point:** `https://pulsecities.com/` then navigate to ZIP 10030

**Expected journey:**
1. Load homepage
2. Select ZIP 10030 from the map or sidebar
3. URL in browser address bar updates to reflect the selected ZIP (e.g., `https://pulsecities.com/neighborhood/10030` or via hash/query param)
4. Copy the URL from the address bar
5. Open a new incognito tab and paste that URL
6. The new tab loads and shows the ZIP 10030 detail panel directly, without requiring any interaction

**Network requests to verify:**
- On deep-link load: `GET /api/neighborhoods/10030/score` fires automatically
- `GET /api/neighborhoods/10030/pulse` fires automatically
- `GET /api/neighborhoods` fires for the map layer

**Success criteria:**
- URL uniquely identifies the neighborhood (not just `pulsecities.com/` with no state)
- Incognito tab loads and immediately shows the detail panel for 10030 without clicking
- Score, signal breakdown, and pulse data appear without any manual navigation
- Page title (or document title) reflects the ZIP or neighborhood name
- OG preview (visible in Slack/iMessage when URL is pasted) shows: title "PulseCities", description about displacement, site preview image

**Failure modes:**
- URL stays as `pulsecities.com/` regardless of which ZIP is selected — not shareable
- Incognito tab loads homepage but detail panel stays closed — deep-link restore not working
- OG preview shows blank image or generic title — meta tags not populated

**Accessibility checks:**
- URL change does not trigger page reload or disrupt keyboard focus

**Mobile behavior:**
- On iPhone SE, the shared URL opens the mobile drawer in expanded state showing the ZIP detail

**Screenshots needed:**
- Browser address bar showing the neighborhood URL
- OG preview as shown when pasting URL into Slack or Twitter

---

### TS-06: Journalist searches for an operator name

**Persona:** P1 — Investigative Journalist  
**User goal:** Find a landlord they heard about ("BREDIF") and see what buildings they own  
**Entry point:** `https://pulsecities.com/` homepage

**Expected journey:**
1. Load homepage
2. Locate the landlord portfolio search box
3. Type "BREDIF"
4. Submit search
5. Review results list and summary
6. Click on one property address in the results
7. Map flies to that property's coordinates

**Network requests to verify:**
- `GET /api/search/landlord?q=BREDIF`
- Response: results array with at least one entry, summary object

**Success criteria:**
- Results appear within 3 seconds
- Summary mentions approximately 64 properties and $0 or near-$0 average transaction amount (bulk note transfer)
- At least one result shows `doc_amount` as $0 or null (bulk transfer characteristic)
- Results sorted by `doc_date` descending (most recent first)
- Clicking a property address in results flies the map to that lat/lon
- The property is highlighted or circled on the map after click

**Failure modes:**
- No results returned — case sensitivity issue or BREDIF not in ACRIS dataset
- Results show standard deed amounts instead of $0 for the bulk transfer entries — data ingestion issue
- Map does not respond to clicking a result row

**Accessibility checks:**
- Each result row is keyboard-accessible (can tab to it, Enter/Space activates it)
- Focus moves to map or gives visual feedback after activating a result row

**Mobile behavior:**
- Results list is scrollable in the sidebar drawer
- Tapping a result row still triggers map pan

**Screenshots needed:**
- Search results panel for BREDIF query with summary line
- Map centered on a BREDIF property with circle overlay

---

### TS-07: Advocate researches their neighborhood by ZIP

**Persona:** P2 — Housing Advocate / Tenant  
**User goal:** Get a full displacement risk picture for ZIP 10456 (Morrisania, Bronx) to use in a tenant meeting  
**Entry point:** `https://pulsecities.com/` homepage

**Expected journey:**
1. Load homepage
2. Locate top-risk sidebar or map
3. Click "Bronx" borough filter pill to narrow the list
4. Find or click ZIP 10456
5. Detail panel opens with score, signal breakdown, and pulse data
6. Review each of the 6 signals to understand what is driving the score
7. Check renovation-flip alert — is there a Stage 2 pipeline alert visible?
8. Review recent LLC acquisitions in the pulse section

**Network requests to verify:**
- `GET /api/neighborhoods/top-risk?limit=10&borough=Bronx`
- `GET /api/neighborhoods/10456/score`
- `GET /api/neighborhoods/10456/pulse`
- `GET /api/neighborhoods/10456/renovation-flip`

**Success criteria:**
- Borough filter pill shows active state when clicked
- Top-risk list refreshes and shows only Bronx ZIPs (10451–10475 range)
- Score displays with correct color: green (0-25), amber (26-50), red (51-80), or bright red (81-100)
- Risk label text matches score tier: "Low pressure" / "Moderate pressure" / "High pressure" / "Critical"
- Summary text is 1-2 natural sentences describing what is driving the score
- Signal breakdown shows 6 bars, each labeled, sorted by value descending
- Last updated timestamp is shown in relative time (e.g., "updated 3 hours ago") — not a raw ISO string
- If renovation-flip `detected: true`, alert banner shows count of matching properties
- LLC acquisitions in pulse show buyer names, addresses, and dates

**Failure modes:**
- Borough filter has no visual effect — API query not passing borough param
- Score shows 0 and all signals are 0 — data pipeline failure for this ZIP
- Signal breakdown bars are all the same height — normalization bug
- Last updated shows "Invalid Date" or raw UTC string
- Renovation-flip section is hidden even when `detected: true`

**Accessibility checks:**
- Borough filter pills have aria-pressed or aria-selected reflecting state
- Signal breakdown bars have text labels with numeric values, not just visual bars
- Risk label color is also expressed as text (not color-only communication)
- Score number has sufficient contrast against its background

**Mobile behavior:**
- Borough filter pills are scrollable horizontally on 375px
- Signal breakdown bars are readable at mobile width (labels don't overlap)
- Stage 2 alert banner is visible above the fold on mobile detail view

**Screenshots needed:**
- Detail panel with score, risk label, and signal breakdown (all 6 bars)
- Stage 2 renovation-flip alert (if present)
- Borough filter pills with "Bronx" in active state

---

### TS-08: Advocate researches a specific landlord by name

**Persona:** P2 — Housing Advocate / Tenant  
**User goal:** Research a specific operator ("PHANTOM") before a tenant organizing meeting to understand how many buildings they control and where  
**Entry point:** `https://pulsecities.com/operator/PHANTOM`

**Expected journey:**
1. Navigate directly to `/operator/PHANTOM`
2. Read stats: total properties, acquisitions, LLC entity count, ZIP codes
3. Expand the LLC entities panel to see all shell company names
4. Scroll to the pattern note section
5. Review acquisitions table — sort or scan by date
6. Note ZIP codes in the acquisitions data (South Bronx + East Brooklyn)
7. Check if related operators are shown (affiliated entities)

**Network requests to verify:**
- `GET /api/operators/PHANTOM`
- Response must include: `llc_entities` (array of ~32 shell names), `total_properties` (~64), `recent_acquisitions` (array), `related_operators` (may be empty)

**Success criteria:**
- Total properties ~64, LLC entities ~32 (PHANTOM CAPITAL 10 LLC, PHANTOM CAPITAL 11 LLC, etc.)
- Pattern note describes numbered LLC shell fragmentation strategy
- LLC entities panel is expandable and shows all entity names without truncation
- Acquisitions table shows rows with address, ZIP, date (YYYY-MM-DD), and dollar amount
- At least some acquisition rows are in ZIP codes 10451–10475 (South Bronx) or 11207-11212 (East Brooklyn)
- Page canonical URL is `https://pulsecities.com/operator/PHANTOM`

**Failure modes:**
- LLC entities panel shows fewer than 10 entities for an operator known to have 32 — pagination or truncation bug
- Pattern note does not match PHANTOM (shows wrong operator's note) — template logic error
- Acquisitions show $0 for all rows for an operator that paid market rate — amount parsing issue

**Accessibility checks:**
- LLC entities collapsible panel uses `<details>`/`<summary>` or proper ARIA expanded state
- When LLC panel expands, focus management is correct (focus does not jump unexpectedly)
- Acquisitions table is navigable row by row with keyboard

**Mobile behavior:**
- Stats grid is readable at 375px
- Acquisitions table scrolls horizontally without breaking layout
- LLC entities panel opens and closes cleanly with tap

**Screenshots needed:**
- Stats grid for PHANTOM
- LLC entities panel expanded showing numbered shells
- Pattern note section

---

### TS-09: Advocate identifies operators in their service area

**Persona:** P2 — Housing Advocate / Tenant  
**User goal:** Find all operator profiles active in the Bushwick/Bed-Stuy area (ZIP 11221) to build a watchlist  
**Entry point:** `https://pulsecities.com/neighborhood/11221` (or via map click)

**Expected journey:**
1. Navigate to ZIP 11221 detail
2. Scroll through the LLC acquisitions in the Neighborhood Pulse section
3. Note buyer LLC names from recent acquisitions
4. For each operator of interest, navigate to their operator profile page
5. Use browser back button to return to 11221 and continue reviewing

**Network requests to verify:**
- `GET /api/neighborhoods/11221/pulse` — must return `llc_acquisitions` array with `buyer_name` field
- Each buyer_name should be a recognizable LLC entity linked to an operator root

**Success criteria:**
- LLC acquisitions in pulse section show buyer names (not just "UNKNOWN" or nulls)
- Buyer names are plausible NYC LLC entities (contain LLC, CORP, INC, or similar)
- Dates are within the last 90 days (relative to today, 2026-04-18)
- At least one acquisition is shown if any activity occurred in this ZIP in the past 90 days
- Browser back button after visiting an operator page returns to the 11221 detail with state preserved

**Failure modes:**
- All buyer names are null — party name normalization failure
- Acquisitions list is empty for an active displacement zone — 90-day window query issue
- Back button reloads homepage instead of restoring neighborhood detail

**Accessibility checks:**
- Acquisition rows are in a list or table, not just divs with inline styles
- Buyer names are legible (sufficient font size, no truncation of key identifiers)

**Mobile behavior:**
- Acquisitions list is readable without horizontal scroll
- Each acquisition row fits on 375px viewport

**Screenshots needed:**
- Neighborhood pulse section showing LLC acquisition entries with buyer names and dates

---

### TS-10: Advocate finds buildings in Stage 2 reno-flip pipeline

**Persona:** P2 — Housing Advocate / Tenant  
**User goal:** Identify specific buildings in a neighborhood that match the reno-flip pattern (LLC acquisition + A1/A2 permit within 60 days) to flag for tenant outreach  
**Entry point:** `https://pulsecities.com/` then navigate to a high-risk ZIP

**Expected journey:**
1. Load homepage
2. Select a ZIP in the top-risk list (try 11221, 10456, or 10030)
3. In the detail panel, look for the "Stage 2 Reno-Flip Alert" section
4. If `detected: true`, expand the section to see the list of matching properties
5. For each property in the list, note the BBL, address, buyer name, transfer date, permit date, and days between them

**Network requests to verify:**
- `GET /api/neighborhoods/{zip}/renovation-flip`
- Response: `detected` (boolean), `count` (int), `properties` (array with `bbl`, `address`, `buyer`, `transfer_date`, `permit_date`, `days_between`)

**Success criteria:**
- When `detected: true`, an alert banner is visible with the count of matching properties
- The collapsible property list shows each match with: address, buyer name, transfer date, permit date, days between (should be ≤ 60)
- `days_between` values are plausible integers between 1 and 60
- Dates are in a readable format (not raw ISO)
- BBL is visible or accessible (can be used for further research)
- When `detected: false`, section is cleanly absent (not showing an empty alert)

**Failure modes:**
- Alert shown even when `detected: false` — incorrect conditional rendering
- Properties list is empty despite `count > 0` — array not passed to template
- `days_between` shown as negative or >60 — date math error

**Accessibility checks:**
- Alert banner uses appropriate role (e.g., `role="alert"` or a heading that screen readers announce)
- Collapsible list is keyboard-accessible

**Mobile behavior:**
- Alert banner is above fold in mobile drawer
- Property list items are tappable (≥44px height) for deeper investigation

**Screenshots needed:**
- Stage 2 alert banner with count visible
- Expanded property list showing address, buyer, and days_between values

---

### TS-11: First-time visitor lands on homepage from Twitter

**Persona:** P3 — First-Time Visitor  
**User goal:** Understand what PulseCities is within 30 seconds of landing  
**Entry point:** `https://pulsecities.com/` (Twitter link click)

**Expected journey:**
1. Page loads
2. Hero overlay animates in with three stats visible
3. Central stat shows a NYC ZIP code with a score
4. Flanking stats show LLC transfer count and eviction count
5. Hook line explains the relationship between these numbers
6. Map is visible in background showing colored NYC neighborhoods
7. Three ZIP cards are visible below the hero stats

**Network requests to verify:**
- `GET /api/stats` — homepage hero depends on this
- `GET /api/neighborhoods` — map color layer

**Success criteria:**
- Page fully interactive within 3 seconds on fast connection
- Above fold shows: site name, a NYC ZIP code, two citywide counts, and a one-sentence hook
- The hook line communicates urgency or scale (e.g., "37 LLC acquisitions × 18 evictions this month")
- Map shows colored neighborhoods (green to red) without requiring interaction
- Three ZIP cards are visible and clickable
- A visitor can understand "this is a tool for tracking displacement in NYC" without clicking anything
- Site name "PulseCities" is in the page title and header
- No login wall, no paywall, no required signup

**Failure modes:**
- Hero stats all show 0 — `/api/stats` failure
- Hook line shows template literal placeholder (e.g., "{{count}} acquisitions")
- Map tiles fail to load (blank map)
- Page has no visible content above the fold (hero hidden by CSS)
- Page takes >5 seconds to first contentful paint

**Accessibility checks:**
- Hero stats have text labels (not just numbers)
- Map has `aria-label` or title describing what it shows
- Site name is in an `<h1>` or otherwise marked as the primary heading
- Color on map is not the only indicator of risk level (hover states show text)

**Mobile behavior (375px):**
- Hero stats are visible without scrolling
- Stats do not overflow viewport
- Map is visible at reduced size
- Three ZIP cards stack vertically and are individually tappable
- Text is legible without zooming (minimum 16px effective font size)

**Screenshots needed:**
- Full above-fold view at 1280px
- Full above-fold view at 375px (iPhone SE)
- Hero overlay with all three stats and hook line visible

---

### TS-12: First-time visitor tries to understand the tool in 30 seconds

**Persona:** P3 — First-Time Visitor  
**User goal:** Determine if PulseCities is credible and relevant within 30 seconds  
**Entry point:** `https://pulsecities.com/`

**Expected journey:**
1. Land on homepage
2. See hero stats with a NYC zip code and numbers
3. See the colored map
4. Click one of the three ZIP cards or a neighborhood on the map
5. Detail panel opens with a score and 1-2 sentence explanation
6. Visitor understands: the tool measures displacement risk using public data

**Network requests to verify:**
- `GET /api/neighborhoods/{zip}/score` fires on ZIP click
- Summary text field returned in response is shown in UI

**Success criteria:**
- Summary text is human-readable and explains what the score means (not raw stats)
- The word "displacement" or "pressure" appears somewhere in the UI without requiring the visitor to read documentation
- The methodology link is visible and goes to `/methodology`
- Data source attribution ("NYC public records" or "ACRIS") is visible somewhere on the first screen or detail panel
- No dark patterns: no interstitials, no overlay asking for email before showing data

**Failure modes:**
- Summary text is missing or shows "N/A" — backend text generation failure
- No methodology link visible — credibility gap
- First click requires prior knowledge to interpret (no onboarding cue)

**Accessibility checks:**
- Color-only risk indicators have text equivalents
- Summary text is readable (adequate line-height, contrast)

**Mobile behavior:**
- Summary text is the first thing visible when detail drawer opens on mobile
- Methodology link is reachable without scrolling far on mobile detail panel

**Screenshots needed:**
- Detail panel showing summary text for a clicked ZIP
- Methodology link visible in nav or panel

---

### TS-13: First-time visitor explores the map without prior knowledge

**Persona:** P3 — First-Time Visitor  
**User goal:** Navigate the map purely through curiosity, without knowing any ZIP codes  
**Entry point:** `https://pulsecities.com/`

**Expected journey:**
1. Land on homepage
2. Ignore sidebar, focus on the colored map
3. Hover over a red neighborhood polygon
4. Tooltip appears showing ZIP, name, score, dominant signal
5. Click that neighborhood
6. Detail panel opens
7. Scroll through detail panel content
8. Click back or deselect to return to map view

**Network requests to verify:**
- No requests on hover (tooltip from already-loaded GeoJSON)
- `GET /api/neighborhoods/{zip}/score` on click

**Success criteria:**
- Map neighborhoods are visually distinct by color (green → amber → red spectrum)
- Hover tooltip shows ZIP code, neighborhood name, score number, and dominant signal label
- Click loads detail panel within 1 second
- Deselecting (clicking elsewhere on map) closes or hides the detail panel
- Map zoom and pan work as expected (scroll to zoom, drag to pan)
- Legend or color key is visible or accessible from the map

**Failure modes:**
- Hover tooltip does not appear — MapLibre hover event not bound
- Polygons are all the same color — score data not joined to GeoJSON
- Click fires but detail panel does not open — event listener failure
- Map is not responsive to scroll zoom (pinch zoom still works on mobile)

**Accessibility checks:**
- Map has a keyboard alternative (tab to borough filter or search to reach any neighborhood)
- Hover tooltip information is also available in the sidebar list (not map-exclusive)

**Mobile behavior:**
- Touch tap on a polygon fires the click event (not hover)
- Map pans by dragging without triggering neighborhood selection
- Pinch-to-zoom works
- Tap on a polygon shows detail drawer from bottom

**Screenshots needed:**
- Map with hover tooltip visible on a red neighborhood
- Map with two or more color tiers clearly visible (at least one red, one green)

---

### TS-14: First-time visitor encounters the methodology page

**Persona:** P3 — First-Time Visitor  
**User goal:** Decide whether to trust PulseCities' numbers before sharing with colleagues  
**Entry point:** Clicks "Methodology" link from the homepage or detail panel

**Expected journey:**
1. Click methodology link
2. Land on `/methodology`
3. Read the intro paragraph — what the tool does and who built it
4. Skim the scoring algorithm section — understand what the 6 signals are
5. Scan data sources — confirm they are real NYC government datasets
6. Note limitations: dormant signals, data lags
7. Find the author's name and contact info

**Success criteria:**
- Same criteria as TS-04 (journalist verification) plus:
- Intro paragraph exists above the algorithm section
- Signal names use plain language a non-expert can understand
- Limitations (dormant assessment spike signal, ACRIS 2-week lag, eviction 2-4 week lag) are visible without searching
- Author name and way to contact is visible by the end of the page
- Page loads without needing to scroll horizontally on any viewport

**Failure modes:**
- Page reads as purely technical (no plain-language intro) — first-time visitor cannot assess credibility
- Limitations section is absent — invites misinterpretation
- No author attribution — reduces trust for first-time visitor

**Accessibility checks:**
- Same as TS-04

**Mobile behavior:**
- Sticky nav does not occupy more than 15% of viewport height
- Section headings are large enough to scan quickly

**Screenshots needed:**
- Introduction section
- Data sources section showing NYC Open Data IDs
- Author/contact section at bottom of page

---

### TS-15: Mobile user — full flow on iPhone SE (375px)

**Persona:** All three personas  
**User goal:** Complete a full research workflow entirely on mobile  
**Entry point:** `https://pulsecities.com/` on a 375px viewport (simulated iPhone SE)

**Expected journey:**
1. Load homepage on 375px viewport
2. See hero stats above fold
3. Tap drawer handle at bottom to expand sidebar
4. Tap a ZIP from the top-risk list
5. Detail drawer slides up to show score and signal breakdown
6. Scroll within the drawer to see pulse data
7. Tap the "back" button to return to the map
8. Use the landlord search box to type "MTEK"
9. View search results in the drawer
10. Navigate to `/operator/MTEK` by copying the URL or tapping a link
11. Read the operator profile on mobile
12. Navigate to `/methodology` and read it on mobile

**Network requests to verify:** Same as desktop scenarios above

**Success criteria:**
- No horizontal scrolling required on any page at 375px
- Tap targets are all ≥44px in height and width
- Text is legible without pinch-to-zoom (minimum 16px body text)
- Hero stats fit on one screen without overflow
- Detail drawer is scrollable independent of the map
- Operator profile stats grid is readable (stacked or 2-column)
- Acquisitions table on operator profile has a scroll affordance
- All interactive elements respond to touch

**Failure modes:**
- Map is non-interactive on mobile (touch events not handled)
- Detail drawer does not close when back button tapped
- Landlord search returns results that overflow the viewport horizontally
- Operator profile table columns are too narrow to read on 375px
- Methodology page has large code blocks that force horizontal scroll

**Accessibility checks:**
- Font sizes do not drop below 14px at 375px
- Focus indicators are visible on touch (for switch-access users)
- Animations can be disabled (`prefers-reduced-motion` media query honored if implemented)

**Mobile behavior:** This IS the mobile test — document all layout differences from desktop

**Screenshots needed:**
- Homepage hero on 375px
- Expanded drawer with a ZIP detail
- Operator profile at 375px
- Methodology page at 375px

---

### TS-16: Accessibility audit — keyboard navigation

**Persona:** All three personas  
**User goal:** Complete all primary workflows using keyboard only (no mouse)  
**Entry point:** `https://pulsecities.com/`

**Expected journey:**
1. Tab from address bar into page
2. Tab to borough filter pills — activate one with Enter/Space
3. Tab to a ZIP in the top-risk list — activate with Enter
4. Detail panel opens — tab through score, signal bars, pulse section
5. Tab to landlord search box — type a query, press Enter
6. Tab through results, activate a result with Enter
7. Tab to methodology nav link — activate with Enter
8. On methodology page, tab through all sections

**Success criteria:**
- Focus order follows logical DOM order (top to bottom, left to right)
- All interactive elements have visible focus indicators (outline or highlight)
- No keyboard traps (can always Tab out of any region)
- Enter/Space activates buttons and links as expected
- Esc closes modals, dropdowns, or detail panels (if applicable)
- Skip-to-content link is the first focusable element (or first after page load)
- Borough filter pills reflect activation state visually when focused + activated

**Failure modes:**
- Focus disappears into the map (MapLibre canvas captures all keyboard input)
- No visible focus ring on map polygon layers
- Detail panel is not reachable by keyboard (requires mouse click on map)
- Borough pills require mouse to activate (click handler, no keydown handler)
- Collapsible LLC entities panel is not keyboard-operable

**Accessibility checks:**
- WAVE or axe DevTools: 0 critical errors on homepage and methodology page
- Color contrast for all text elements: minimum 4.5:1 (normal), 3:1 (large text)
- All images have alt text (or empty alt for decorative images)
- Form inputs (search, email subscribe) have associated labels

**Mobile behavior:** Tab order on mobile is less relevant; test with iOS VoiceOver gestures if possible

**Screenshots needed:**
- Focus ring visible on a borough filter pill
- Focus ring visible on a list item in top-risk sidebar
- Any keyboard trap identified (as a failure screenshot)

---

### TS-17: Performance check — load times

**Persona:** All three personas  
**User goal:** Verify the site loads within acceptable time on a simulated mobile 4G connection  
**Entry point:** `https://pulsecities.com/`

**Setup:** Use Chrome DevTools Network throttling at "Fast 4G" (20 Mbps down, 1.5 Mbps up, 20ms RTT)

**Expected journey:**
1. Open DevTools → Network tab, enable "Fast 4G" throttle
2. Hard refresh `https://pulsecities.com/`
3. Record: First Contentful Paint (FCP), Largest Contentful Paint (LCP), Time to Interactive (TTI)
4. Measure time until hero stats are fully rendered
5. Measure time until map polygons are colored
6. Navigate to `/operator/MTEK`, measure time until acquisitions table is fully populated
7. Navigate to `/methodology`, measure time until all content is visible

**Success criteria:**
- Homepage FCP < 3 seconds on Fast 4G
- Homepage LCP < 4 seconds on Fast 4G
- Hero stats fully rendered < 4 seconds (dependent on `/api/stats` response)
- Map polygons colored < 5 seconds (dependent on `/api/neighborhoods` GeoJSON response)
- Operator profile table fully populated < 3 seconds on Fast 4G
- Methodology page LCP < 2.5 seconds (static, no API calls)
- No layout shift after initial paint (CLS < 0.1)
- Total homepage transfer size < 3MB

**Failure modes:**
- FCP > 5 seconds — blocking scripts or large unoptimized assets
- Map polygons load but are uncollored for >3 seconds — score join happens client-side and is slow
- Layout shift when hero stats count up — reserved space for numbers not set
- Transfer size > 5MB — GeoJSON not simplified, or map tiles loading a large region

**Network requests to inspect:**
- `/api/neighborhoods` response size (should be <500KB with simplified geometry)
- `/api/stats` response time (should be <500ms)
- Map tile requests (should be lazy-loaded only for visible tiles)

**Screenshots needed:**
- Chrome DevTools Performance panel waterfall for homepage
- Network tab showing key API responses and their timing

---

### TS-18: Empty state — searches that return nothing

**Persona:** All three personas  
**User goal:** Verify that empty states are clear and helpful, not broken-looking  
**Entry point:** `https://pulsecities.com/`

**Expected journey:**
1. Search for a landlord that does not exist: "ZZZZZZZ"
2. Observe the empty state message
3. Select a ZIP that has no pulse activity (if any such ZIP exists)
4. Observe the empty pulse state
5. Navigate to `/operator/UNKNOWNXYZ` (non-existent operator)
6. Observe the 404 error state

**Success criteria:**
- Landlord search for "ZZZZZZZ": shows "No properties found for "ZZZZZZZ" in ACRIS records." (no spinner, no blank space)
- Neighborhood pulse with no data: shows "No recent acquisitions, permits, or evictions in the last 90 days." (not an empty unstyled list)
- `/operator/UNKNOWNXYZ`: shows "404 / Operator not found. ← Back to map" (not a browser 404 page)
- Empty state messages are styled consistently with the rest of the UI (same font, color, padding)
- No JavaScript errors in console when an empty state is triggered

**Failure modes:**
- Empty landlord search shows a spinner permanently — async handler not resolving on empty result
- Empty pulse section is invisible (section header shown with nothing below it)
- Operator 404 is an unstyled browser error page — Python 404 exception not caught

**Accessibility checks:**
- Empty state messages are in the DOM (not just CSS pseudo-content)
- 404 page has a working "Back to map" link

**Mobile behavior:**
- Empty state messages are centered and readable on 375px
- 404 page is usable on mobile (link is tappable)

**Screenshots needed:**
- Empty landlord search result
- Operator 404 page
- Empty pulse section (if a ZIP with no activity can be found)

---

### TS-19: Error state — API failure handling

**Persona:** All three personas  
**User goal:** Verify that API failures produce helpful error messages, not blank UI or uncaught exceptions  
**Entry point:** `https://pulsecities.com/`

**Setup:** Use Chrome DevTools → Network tab → Right-click a pending request → Block request URL for `/api/neighborhoods/{zip}/score`

**Expected journey:**
1. Select a neighborhood from the map
2. Block the score API request in DevTools
3. Observe what happens to the detail panel
4. Unblock the request
5. Block `/api/search/landlord`
6. Submit a landlord search
7. Observe the error state

**Success criteria:**
- Score API failure: detail panel shows "Network error — please try again." or similar, not an empty panel
- Landlord search failure: search results area shows "Network error — please try again."
- No unhandled JavaScript exceptions in console (no red errors)
- UI is still interactive after an error (can retry, can click elsewhere)
- Error messages do not expose internal API details (stack traces, SQL, server paths)

**Failure modes:**
- Score panel shows empty content (no score, no bars, no message) — silent failure
- Console shows `Uncaught TypeError: Cannot read properties of undefined` — missing null check
- Error message exposes server internals ("psycopg2 error: relation 'displacement_scores' does not exist")

**Accessibility checks:**
- Error messages are announced to screen readers (aria-live region or focus management)

**Mobile behavior:**
- Error messages are visible in the mobile drawer without scrolling

**Screenshots needed:**
- Detail panel in error state (score blocked)
- Browser console showing clean error handling (no uncaught exceptions)

---

### TS-20: SEO and sharing — Open Graph preview

**Persona:** All three personas  
**User goal:** Verify that PulseCities links look correct when shared on Twitter, Slack, iMessage, and LinkedIn  
**Entry point:** Various URLs

**Expected journey:**
1. Paste `https://pulsecities.com/` into Twitter compose box — observe link preview
2. Paste `https://pulsecities.com/methodology` into a Slack message — observe link unfurl
3. Paste `https://pulsecities.com/operator/MTEK` into a text message — observe preview
4. Use a tool like https://www.opengraph.xyz or browser DevTools to inspect OG tags

**OG tags to verify:**

| Page | `og:title` | `og:description` | `og:type` | `og:image` |
|------|-----------|-----------------|-----------|------------|
| Homepage | "PulseCities — NYC Displacement Risk Map" | Contains "real-time displacement" | website | Site logo or map preview |
| Methodology | "How PulseCities Works — NYC Displacement Risk Methodology" | Contains "six signals" and "public data" | article | Same image |
| Operator (MTEK) | "MTEK — PulseCities Operator Profile" | Contains "ACRIS public records" | website | Same image |

**Twitter card tags to verify:**
- `twitter:card` = "summary_large_image"
- `twitter:title` matches `og:title`
- `twitter:description` matches `og:description`
- `twitter:image` is set and resolves to a real image URL

**Success criteria:**
- All three pages have all four core OG tags (`og:title`, `og:description`, `og:type`, `og:image`)
- `og:image` URL resolves to a 200 response (image loads)
- Twitter card renders as "summary_large_image" type (wider card, not just favicon)
- No OG tag longer than 200 characters (Twitter truncates)
- Methodology page JSON-LD block parses without errors (paste into Google's Rich Results Test)

**Failure modes:**
- `og:image` URL returns 404 — image file missing or path wrong
- Operator page has no OG title or generic title — dynamic template injection missing
- JSON-LD block malformed (unclosed bracket, missing comma) — structured data test fails

**Accessibility checks:** N/A (OG is a sharing/SEO concern, not accessibility)

**Mobile behavior:** N/A for this test

**Screenshots needed:**
- Browser DevTools Elements view showing `<meta property="og:*">` tags for each page
- Twitter link preview card (screenshot from Twitter compose box)
- Google Rich Results Test output for the methodology page

---

## Known Issues and Acceptable Tradeoffs

Use this section to log findings that are not bugs but deliberate product decisions. Format: **[FINDING]** Description. *Decision: rationale.*

**[DESIGN]** Assessment spike signal (year-over-year property assessment change) is currently dormant — it is shown in the methodology but contributes 0 points to the score because DOF historical data has not yet been collected for 2 full years.  
*Decision: Disclosed explicitly in methodology. Score still valid without this signal.*

**[DATA]** ACRIS recording lag of approximately 2 weeks means that very recent sales may not appear in the tool.  
*Decision: Disclosed in methodology and in the UI near eviction data.*

**[DATA]** Evictions data lag of 2-4 weeks (from marshal execution to NYC Open Data appearance) means very recent evictions may not appear.  
*Decision: Disclosed in methodology and in the UI near eviction data.*

**[COVERAGE]** Operator profiles are limited to the 18-month ACRIS window. Operators who were active before that window will show incomplete portfolios.  
*Decision: Subtitle on operator pages explicitly states "18-month ACRIS window."*

**[PERFORMANCE]** The neighborhood GeoJSON endpoint returns simplified (but not minimal) geometry for all 178 neighborhoods on every homepage load. This trades bandwidth for instant map coloring without a second request.  
*Decision: Geometry is simplified via PostGIS ST_SimplifyPreserveTopology. Acceptable tradeoff for current traffic levels.*

**[UX]** The hero section was built to be functional but not launch-quality. It will be polished before press outreach.  
*Decision: Deliberate deferral documented internally. Do not block Phase 9/10 on this.*

**[I18N]** The Spanish translation (EN/ES toggle) applies only to static UI strings. API-generated text (summary_text, dominant signal labels) remains in English.  
*Decision: Backend i18n is out of scope for current phase.*

**[API]** The `/api/neighborhoods/{zip}/summary` (automated neighborhood summary) endpoint is not yet implemented.  
*Decision: Deferred to a future phase. Must include per-IP rate limiting before launch.*

---

*End of UAT Test Plan — pulsecities.com — 2026-04-18*
