# Operator Affiliation Report

Generated from `scripts/entity_resolution_audit.json` (2026-04-18).

## Decision: No merges — 2026-04-18

All five candidate pairs were reviewed and **no clusters were merged**.
Transparency takes precedence over a cleaner map. Every operator stays
as the public records show them.

Relationships are instead published on each operator's profile page
(`/operator/{root}`) as evidence for readers to evaluate themselves.
Shared properties, name signals, and geographic overlap are shown as
data — not collapsed into a single cluster. Counts and scores are never
combined across operators.

This decision is final for this data set. The affiliation signals remain
in `entity_resolution_audit.json` under `by_operator` for API use.

---

---

## 1. ICECAP + ICE CAP

**Combined confidence:** 0.988  
**Recommended action:** `merge_candidate`  
**Signals:** root_substring (conf=0.85), shared_bbl (conf=0.92)  
**Trigger:** 'ICE CAP FUND III SPV-C LLC' whitespace-collapses to the same string as 'ICECAP FUND III SPV-C LLC'

### ICECAP cluster — LLC names

- `ICECAP FUND III SPV-C LLC`
- `ICECAP REAL`
- `ICECAP REAL ESTATE DEBT FUND 111 LLC`
- `ICECAP REAL ESTATE DEBT FUND IHI LLC`
- `ICECAP REAL ESTATE DEBT FUND III LLC`
- `ICECAP SUB-REIT SPV I LLC`
- `ICECAP SUB-REIT SPV-I LLC`

### ICE cluster — LLC names

- `ICE CAP FUND III SPV-C LLC`
- `ICE LENDER HOLDINGS`
- `ICE LENDER HOLDINGS LLC`
- `ICE LENDER HOLDINGS LLC ISAOA/ATIMA`

### ICECAP cluster — sample properties (up to 5)

  - BBL 1017200003 | 206 LENOX AVENUE, MANHATTAN, 10027 | acquired by `ICECAP REAL ESTATE DEBT FUND III LLC` (2025-05-13)
  - BBL 2022610020 | (address unavailable) | acquired by `ICECAP REAL ESTATE DEBT FUND III LLC` (2026-01-26)
  - BBL 2024360005 | (address unavailable) | acquired by `ICECAP REAL ESTATE DEBT FUND III LLC` (2026-02-10)
  - BBL 2024520022 | 1131 GRANT AVENUE, BRONX, 10456 | acquired by `ICECAP FUND III SPV-C LLC` (2025-05-19)
  - BBL 2024520071 | 1175 GRANT AVENUE, BRONX, 10456 | acquired by `ICECAP REAL ESTATE DEBT FUND III LLC` (2025-08-11)

### ICE cluster — sample properties (up to 5)

  - BBL 1013391180 | (address unavailable) | acquired by `ICE LENDER HOLDINGS LLC` (2025-05-09)
  - BBL 1017200003 | 206 LENOX AVENUE, MANHATTAN, 10027 | acquired by `ICE LENDER HOLDINGS LLC` (2025-05-07)
  - BBL 2024520082 | 1151 GRANT AVENUE, BRONX, 10456 | acquired by `ICE LENDER HOLDINGS LLC ISAOA/ATIMA` (2026-01-09)
  - BBL 2025090061 | 1139 ANDERSON AVENUE, BRONX, 10452 | acquired by `ICE LENDER HOLDINGS LLC` (2025-05-13)
  - BBL 2028260102 | (address unavailable) | acquired by `ICE LENDER HOLDINGS LLC` (2025-08-11)

### Shared BBLs

- `1017200003` — 206 LENOX AVENUE, MANHATTAN, 10027
- `2032950030` — (no address on file)
- `2045730046` — 3011 BRONXWOOD AVENUE, BRONX, 10469
- `3005860020` — 115 DIKEMAN STREET, BROOKLYN, 11231
- `3050390033` — (no address on file)

### Decision

- [ ] Approve merge
- [x] Reject — separate operators
- [ ] Needs further investigation

**Notes:**


---

## 2. MELO + PHANTOM

**Combined confidence:** 0.9  
**Recommended action:** `merge_candidate`  
**Signals:** name_embedding (conf=0.90)  
**Trigger:** 'MELO Z PHANTOM CAP LLC' (in MELO cluster) embeds root token 'PHANTOM'

### MELO cluster — LLC names

- `MELO HECTOR B`
- `MELO Z PHANTOM CAP LLC`

### PHANTOM cluster — LLC names

- `PHANTOM & Z CAPITAL LLC`
- `PHANTOM AFFORDABLE HOUSING LLC`
- `PHANTOM CAP HOLDINGS 206 LLC`
- `PHANTOM CAP MAP LLC`
- `PHANTOM CAPITAL 10 LLC`
- `PHANTOM CAPITAL 107 LLC`
- `PHANTOM CAPITAL 11 LLC`
- `PHANTOM CAPITAL 12 LLC`
- `PHANTOM CAPITAL 21 LLC`
- `PHANTOM CAPITAL 22 LLC`
- `PHANTOM CAPITAL 44 LLC`
- `PHANTOM CAPITAL 55 LLC`
- `PHANTOM CAPITAL 71 LLC`
- `PHANTOM CAPITAL 800 LLC`
- `PHANTOM CAPITAL ACQUISITIONS LLC`
- `PHANTOM CAPITAL BX LLC`
- `PHANTOM HOUSING LLC`
- `PHANTOM KOACH LLC`
- `PHANTOM LANDLORDS LLC`
- `PHANTOM NYC HOLDINGS LLC`
- `PHANTOM PARTNERS 89 LLC`
- `PHANTOM PARTNERS BH LLC`
- `PHANTOM PARTNERS PROPERTIES LLC`
- `PHANTOM PARTNERS RE LLC`
- `PHANTOM RISE LLC`
- `PHANTOM TERRITORY LLC`
- `PHANTOM TITANS LLC`
- `PHANTOM TOWN LLC`
- `PHANTOM TROPHIES LLC`

### MELO cluster — sample properties (up to 5)

  - BBL 2026730042 | 1231 UNION AVENUE, BRONX, 10459 | acquired by `MELO Z PHANTOM CAP LLC` (2026-01-07)
  - BBL 2027070056 | 776 BECK STREET, BRONX, 10455 | acquired by `MELO Z PHANTOM CAP LLC` (2025-08-13)
  - BBL 2029710049 | 847 FREEMAN STREET, BRONX, 10459 | acquired by `MELO Z PHANTOM CAP LLC` (2025-04-24)
  - BBL 2031330112 | 962 EAST 181 STREET, BRONX, 10460 | acquired by `MELO HECTOR B` (2025-06-26)
  - BBL 2032250180 | 2305 LORING PLACE NORTH, BRONX, 10468 | acquired by `MELO Z PHANTOM CAP LLC` (2026-01-12)

### PHANTOM cluster — sample properties (up to 5)

  - BBL 2022780019 | 422 EAST 134 STREET, BRONX, 10454 | acquired by `PHANTOM PARTNERS 89 LLC` (2025-11-05)
  - BBL 2027010052 | 735 KELLY STREET, BRONX, 10455 | acquired by `PHANTOM CAPITAL 71 LLC` (2025-08-26)
  - BBL 2027190046 | 1140 FOX STREET, BRONX, 10459 | acquired by `PHANTOM CAPITAL 107 LLC` (2025-09-26)
  - BBL 2027560038 | 1039 LONGFELLOW AVENUE, BRONX, 10459 | acquired by `PHANTOM RISE LLC` (2025-10-20)
  - BBL 2027560067 | 1032 LONGFELLOW AVENUE, BRONX, 10459 | acquired by `PHANTOM CAPITAL 55 LLC` (2026-03-10)

### Shared BBLs

_(none — match driven by name signal only)_

### Decision

- [ ] Approve merge
- [x] Reject — separate operators
- [ ] Needs further investigation

**Notes:**


---

## 3. BROAD + CHURCHILL

**Combined confidence:** 0.92  
**Recommended action:** `merge_candidate`  
**Signals:** shared_bbl (conf=0.92)  
**Trigger:** 11 BBLs acquired by entities from both clusters

### BROAD cluster — LLC names

- `BROAD X FUNDING II LLC`
- `BROAD X FUNDING III LLC`
- `BROAD X FUNDING LLC`

### CHURCHILL cluster — LLC names

- `CHURCHILL DANIELE`
- `CHURCHILL FUNDING I LLC`
- `CHURCHILL MARA FUNDING I LLC`
- `CHURCHILL MRA FUNDING I LLC`
- `FB CHURCHILL FACILITY LLC`

### BROAD cluster — sample properties (up to 5)

  - BBL 2024370036 | 1025 COLLEGE AVENUE, BRONX, 10456 | acquired by `BROAD X FUNDING LLC` (2025-08-21)
  - BBL 2024520014 | 1130 SHERMAN AVENUE, BRONX, 10456 | acquired by `BROAD X FUNDING II LLC` (2025-07-09)
  - BBL 2024720004 | (address unavailable) | acquired by `BROAD X FUNDING II LLC` (2026-03-11)
  - BBL 2027830067 | (address unavailable) | acquired by `BROAD X FUNDING LLC` (2026-03-18)
  - BBL 2027940061 | (address unavailable) | acquired by `BROAD X FUNDING LLC` (2026-03-25)

### CHURCHILL cluster — sample properties (up to 5)

  - BBL 1000780043 | 90 NASSAU STREET, MANHATTAN, 10038 | acquired by `CHURCHILL MRA FUNDING I LLC` (2025-11-13)
  - BBL 1001790063 | (address unavailable) | acquired by `CHURCHILL MRA FUNDING I LLC` (2025-12-03)
  - BBL 1002081027 | (address unavailable) | acquired by `CHURCHILL MRA FUNDING I LLC` (2026-03-10)
  - BBL 1004290039 | (address unavailable) | acquired by `CHURCHILL MRA FUNDING I LLC` (2025-09-26)
  - BBL 1004480049 | 313 EAST 6 STREET, MANHATTAN, 10003 | acquired by `CHURCHILL MRA FUNDING I LLC` (2025-06-05)

### Shared BBLs

- `2024520014` — 1130 SHERMAN AVENUE, BRONX, 10456
- `2024720004` — (no address on file)
- `2028740044` — 1435 NELSON AVENUE, BRONX, 10452
- `2028740045` — (no address on file)
- `2028800333` — (no address on file)

### Decision

- [ ] Approve merge
- [x] Reject — separate operators
- [ ] Needs further investigation

**Notes:**


---

## 4. CHURCHILL + DERBY

**Combined confidence:** 0.92  
**Recommended action:** `merge_candidate`  
**Signals:** shared_bbl (conf=0.92)  
**Trigger:** 10 BBLs acquired by entities from both clusters

### CHURCHILL cluster — LLC names

- `CHURCHILL DANIELE`
- `CHURCHILL FUNDING I LLC`
- `CHURCHILL MARA FUNDING I LLC`
- `CHURCHILL MRA FUNDING I LLC`
- `FB CHURCHILL FACILITY LLC`

### DERBY cluster — LLC names

- `DERBY 538 LLC`
- `DERBY BOW 9 LLC`
- `DERBY DENVER 303 LENDER II LLC`
- `DERBY FIG HOLDINGS 1 LP`
- `DERBY FIG HOLDINGS I LLC`
- `DERBY ROADRUNNER 355 LLC`
- `DERBY SLED 580 LLC`
- `DERBY WRIGHT 1571 LLC`

### CHURCHILL cluster — sample properties (up to 5)

  - BBL 1000780043 | 90 NASSAU STREET, MANHATTAN, 10038 | acquired by `CHURCHILL MRA FUNDING I LLC` (2025-11-13)
  - BBL 1001790063 | (address unavailable) | acquired by `CHURCHILL MRA FUNDING I LLC` (2025-12-03)
  - BBL 1002081027 | (address unavailable) | acquired by `CHURCHILL MRA FUNDING I LLC` (2026-03-10)
  - BBL 1004290039 | (address unavailable) | acquired by `CHURCHILL MRA FUNDING I LLC` (2025-09-26)
  - BBL 1004480049 | 313 EAST 6 STREET, MANHATTAN, 10003 | acquired by `CHURCHILL MRA FUNDING I LLC` (2025-06-05)

### DERBY cluster — sample properties (up to 5)

  - BBL 1000780043 | 90 NASSAU STREET, MANHATTAN, 10038 | acquired by `DERBY FIG HOLDINGS 1 LP` (2025-08-07)
  - BBL 1004880025 | (address unavailable) | acquired by `DERBY FIG HOLDINGS 1 LP` (2025-08-14)
  - BBL 1006060007 | (address unavailable) | acquired by `DERBY BOW 9 LLC` (2025-09-25)
  - BBL 1006060093 | (address unavailable) | acquired by `DERBY DENVER 303 LENDER II LLC` (2025-09-25)
  - BBL 1006250002 | (address unavailable) | acquired by `DERBY ROADRUNNER 355 LLC` (2025-08-06)

### Shared BBLs

- `1000780043` — 90 NASSAU STREET, MANHATTAN, 10038
- `1004880025` — (no address on file)
- `3002410034` — 32 PIERREPONT STREET, BROOKLYN, 11201
- `3002640013` — (no address on file)
- `3006490028` — (no address on file)

### Decision

- [ ] Approve merge
- [x] Reject — separate operators
- [ ] Needs further investigation

**Notes:**


---

## 5. TOMPKINS + TOWNHOUSE

**Combined confidence:** 0.9  
**Recommended action:** `merge_candidate`  
**Signals:** name_embedding (conf=0.90)  
**Trigger:** '51 TOMPKINS TOWNHOUSE LLC' (in TOMPKINS cluster) embeds root token 'TOWNHOUSE'

### TOMPKINS cluster — LLC names

- `116 TOMPKINS AVENUE CORPORATION`
- `145 TOMPKINS REALTY LLC`
- `51 TOMPKINS TOWNHOUSE LLC`
- `M TOMPKINS LLC`
- `TOMPKINS & BALLONZOLI TRUST`
- `TOMPKINS COMMUNITY BANK`
- `TOMPKINS EUGENE`

### TOWNHOUSE cluster — LLC names

- `M5 TOWNHOUSE 47 LLC`
- `TOWNHOUSE RENTAL II LLC`
- `TOWNHOUSE RENTAL IX LLC`
- `TOWNHOUSE RENTAL LLC`
- `TOWNHOUSE RENTAL VII LLC`

### TOMPKINS cluster — sample properties (up to 5)

  - BBL 1015831001 | (address unavailable) | acquired by `TOMPKINS COMMUNITY BANK` (2025-08-01)
  - BBL 1015831043 | (address unavailable) | acquired by `TOMPKINS COMMUNITY BANK` (2025-08-01)
  - BBL 1015831062 | (address unavailable) | acquired by `TOMPKINS COMMUNITY BANK` (2025-08-01)
  - BBL 1015831071 | (address unavailable) | acquired by `TOMPKINS COMMUNITY BANK` (2025-07-10)
  - BBL 2026900143 | 868 EAST 164 STREET, BRONX, 10459 | acquired by `TOMPKINS COMMUNITY BANK` (2025-05-16)

### TOWNHOUSE cluster — sample properties (up to 5)

  - BBL 1010570110 | (address unavailable) | acquired by `M5 TOWNHOUSE 47 LLC` (2026-03-03)
  - BBL 3002850046 | (address unavailable) | acquired by `TOWNHOUSE RENTAL II LLC` (2025-12-19)
  - BBL 3003820023 | (address unavailable) | acquired by `TOWNHOUSE RENTAL VII LLC` (2025-04-24)
  - BBL 3009340045 | (address unavailable) | acquired by `TOWNHOUSE RENTAL VII LLC` (2025-04-24)
  - BBL 3009380067 | 33 PARK PLACE, BROOKLYN, 11217 | acquired by `TOWNHOUSE RENTAL II LLC` (2026-03-11)

### Shared BBLs

_(none — match driven by name signal only)_

### Decision

- [ ] Approve merge
- [x] Reject — separate operators
- [ ] Needs further investigation

**Notes:**


---
