# PulseCities Pre-Deploy Release Checklist

Run before every production deploy. Automated checks in `scripts/smoke_public_routes.sh`.
Manual checks below cover what scripts cannot detect.

---

## 1. Run automated smoke tests

```bash
# Full automated check (API routes, sitemap, grep guards)
./scripts/smoke_public_routes.sh

# HTML page content markers (existing)
./scripts/smoke_routes.sh
```

Both must exit 0 before deploying.

---

## 2. Mobile checks (375px viewport, Safari or Chrome DevTools)

Visit each route and verify:

| Route | Bottom nav readable | No footer nav on mobile | No content behind nav | Active item correct | Top nav readable |
|---|---|---|---|---|---|
| `/` | Home active | | | Home | |
| `/map` | Map active | n/a (no footer) | drawer clears nav | Map | |
| `/operators` | Operators active | | | Operators | |
| `/operator/mtek-nyc` | Operators active | | | Operators | |
| `/methodology` | no active | | | none | |
| `/about` | About active | | | About | |

Checks:
- [ ] Bottom nav shows 4 items: Home / Map / Operators / About
- [ ] Items do not run together (no "HomeMapOperatorsAbout" in one blob)
- [ ] Each item is individually tappable (min 56px height)
- [ ] Safe-area padding present on notched devices (test with iPhone viewport)
- [ ] No page content hidden behind bottom nav (scroll to bottom on each page)
- [ ] Footer on mobile shows byline only, no duplicate nav links

---

## 3. Desktop checks (1280px viewport)

- [ ] Header links work: Home, Map, Operators, Methodology, About
- [ ] Footer links work on all pages
- [ ] Sidebar on `/map` is visible and scrollable
- [ ] Operator profile page table renders (not blank)

---

## 4. Data checks

- [ ] `/` — Hero stat chips load (LLC transfers count, evictions count)
- [ ] `/` — "Highest-risk areas right now" list populates (not skeleton)
- [ ] `/map` — Type `11216` in search, ZIP card appears
- [ ] `/map` — Type `MTEK` in search, operator result appears
- [ ] `/operator/mtek-nyc` — Portfolio table loads, stats non-zero
- [ ] `/operators` — Operator directory lists at least 10 entries

---

## 5. Copy checks

- [ ] No em dashes (`—`) used as connectors in any visible UI text
- [ ] No placeholder text visible (`Loading...` only appears transiently)
- [ ] `/about` route loads (not 404) — link present in footer and bottom nav
- [ ] No `TODO`, `FIXME`, or `[DRAFT]` text visible anywhere

---

## 6. API spot checks (can also run via smoke script)

```bash
BASE=https://pulsecities.com

curl -sf "$BASE/api/health"
curl -sf "$BASE/api/neighborhoods/top-risk" | python3 -c "import sys,json; d=json.load(sys.stdin); assert len(d['neighborhoods'])>0"
curl -sf "$BASE/api/search?q=mtek" | python3 -c "import sys,json; d=json.load(sys.stdin); assert any(r.get('type')=='operator' for r in d.get('results',[]))"
curl -sf "$BASE/api/search?q=11216" | python3 -c "import sys,json; d=json.load(sys.stdin); assert len(d.get('results',[]))>0"
curl -sf "$BASE/sitemap.xml" | grep -q "pulsecities.com"
```

---

## 7. Post-deploy confirmation

After restarting the service:

```bash
# Confirm service is up and cache warmed
curl -w "Total: %{time_total}s\n" -o /dev/null -s https://pulsecities.com/api/neighborhoods/top-risk
# Should be under 500ms (served from in-process cache after warmup)
```

- [ ] Response under 500ms on second hit
- [ ] `systemctl status pulsecities` shows active (running)
- [ ] No errors in `journalctl -u pulsecities -n 50`
