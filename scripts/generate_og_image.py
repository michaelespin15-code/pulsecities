#!/usr/bin/env python3
"""Regenerate frontend/og-image.png, the default social card.

The card is the first thing anyone sees when the site is linked, and it had
been shipping a wrong number: "all 178 NYC ZIP codes". 178 is the row count of
the neighborhoods table, which includes a 99999 sentinel used for records whose
ZIP could not be matched. Every query that matters already filters it; the
card, hand-made once and never regenerated, did not. The real figure is the one
the rest of the site quotes, and it is read from the database here rather than
typed, so the next boundary change updates the card instead of dating it.

The map behind the text is re-shot from the live app each run, so the card can
never drift from the ramp the site actually renders. Everything else is drawn
in HTML with the real house fonts, which is why this uses a browser rather than
PIL: the per-neighborhood cards in api/routes/og_images.py are PIL and have to
fall back to DejaVu, which is fine for a data card and wrong for the wordmark.

    ./venv/bin/python scripts/generate_og_image.py [--base https://pulsecities.com]

Facebook and LinkedIn cache aggressively; expect a lag before the new card
shows on already-shared links.
"""

import argparse
import asyncio
import base64
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "frontend" / "og-image.png"

W, H = 1200, 630

HEADLINE = "The displacement story\nNYC doesn't officially tell."


def scored_zip_count() -> int:
    sys.path.insert(0, str(ROOT))
    from models.database import SessionLocal
    from sqlalchemy import text

    db = SessionLocal()
    try:
        n = db.execute(text(
            "SELECT count(DISTINCT zip_code) FROM displacement_scores "
            "WHERE score IS NOT NULL AND zip_code <> '99999'"
        )).scalar()
    finally:
        db.close()
    if not n:
        raise SystemExit("og: refusing to render a card with no scored ZIPs")
    return int(n)


def card_html(map_data_uri: str, n_zips: int) -> str:
    headline = HEADLINE.replace("\n", "<br>")
    return f"""<!doctype html>
<html><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,700&family=DM+Sans:wght@400;500&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{width:{W}px;height:{H}px;overflow:hidden;background:#111823}}
  .card{{position:relative;width:{W}px;height:{H}px}}
  .map{{position:absolute;inset:0;background:url({map_data_uri}) center/cover no-repeat}}
  /* The text sits on the left, so the scrim is heaviest there and clears
     entirely by the right edge, where the choropleth should stay readable. */
  .scrim{{position:absolute;inset:0;background:linear-gradient(100deg,
      rgba(17,24,35,0.97) 0%, rgba(17,24,35,0.94) 38%,
      rgba(17,24,35,0.72) 56%, rgba(17,24,35,0.10) 82%)}}
  .body{{position:absolute;left:72px;top:132px;width:660px}}
  .rule{{display:block;margin-bottom:52px}}
  h1{{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:58px;
     line-height:1.08;letter-spacing:-0.02em;color:#eef2f5;text-wrap:balance}}
  .sub{{font-family:'DM Sans',sans-serif;font-size:24px;line-height:1.5;color:#93a1ad;
       margin-top:26px;max-width:560px}}
  .brand{{position:absolute;left:72px;top:462px;display:flex;align-items:center;gap:14px}}
  .mark{{width:40px;height:40px;border-radius:9px;background:#1a1a2e;
        display:flex;align-items:center;justify-content:center}}
  .name{{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:27px;color:#eef2f5}}
  .url{{font-family:'JetBrains Mono',monospace;font-size:19px;color:#ed6317;margin-left:6px}}
</style></head>
<body><div class="card">
  <div class="map"></div>
  <div class="scrim"></div>
  <div class="body">
    <svg class="rule" width="640" height="18" viewBox="0 0 640 18" fill="none">
      <polyline points="0,9 250,9 262,9 270,2 282,16 292,9 640,9" stroke="#ed6317"
        stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" fill="none"/>
    </svg>
    <h1>{headline}</h1>
    <p class="sub">Displacement-pressure scores for all {n_zips} NYC
       ZIP&nbsp;codes, built nightly from public records.</p>
  </div>
  <div class="brand">
    <span class="mark">
      <svg width="26" height="26" viewBox="0 0 32 32" fill="none">
        <polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none"
          stroke="#ed6317" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
    </span>
    <span class="name">PulseCities</span>
    <span class="url">pulsecities.com</span>
  </div>
</div></body></html>"""


async def shoot_map(base: str) -> str:
    """Screenshot the live map with the app chrome hidden, as a data URI."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await (await browser.new_context(
            viewport={"width": W, "height": H}, device_scale_factor=2)).new_page()
        await page.goto(f"{base}/map", wait_until="load", timeout=60000)
        # Strip the app furniture down to bare cartography. Naming the overlays
        # individually goes stale the first time one is added, so this hides
        # anything positioned over the map that MapLibre did not put there, and
        # the sidebar, which is a flex sibling rather than an overlay.
        left = await page.evaluate("""() => {
            document.querySelectorAll('aside').forEach(el => el.remove());
            // The legend, the sidebar handle and the LinkedIn badge are children
            // of the MapLibre container, so "inside the map" is not the test.
            // MapLibre's own furniture is what carries a maplibregl- class, and
            // the control container is kept deliberately: the card has to show
            // the CARTO and OpenStreetMap attribution.
            const keep = '.maplibregl-control-container, .maplibregl-canvas-container';
            [...document.querySelectorAll('body *')].forEach(el => {
                if (!el.isConnected || el.closest(keep)) return;
                const pos = getComputedStyle(el).position;
                if (pos === 'absolute' || pos === 'fixed') el.remove();
            });
            const mapRoot = document.querySelector('.maplibregl-map');
            if (mapRoot) {
                mapRoot.style.width = '100vw';
                mapRoot.style.height = '100vh';
            }
            return document.querySelectorAll('#map-legend, #last-visit-banner').length;
        }""")
        if left:
            raise SystemExit(f"og: {left} chrome element(s) survived the strip; card would ghost")
        await page.wait_for_timeout(9000)  # tiles, sprite and glyph ranges, after the resize
        png = await page.screenshot(type="png")
        await browser.close()
    return "data:image/png;base64," + base64.b64encode(png).decode()


async def render(base: str, n_zips: int) -> None:
    from playwright.async_api import async_playwright

    map_uri = await shoot_map(base)
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await (await browser.new_context(
            viewport={"width": W, "height": H}, device_scale_factor=1)).new_page()
        await page.set_content(card_html(map_uri, n_zips), wait_until="load")
        await page.wait_for_timeout(2500)  # webfonts
        png = await page.screenshot(type="png")
        await browser.close()

    # A screenshot of vector cartography is nearly all flat fill, so a 256-colour
    # palette is visually lossless here and cuts the file by well over half. Worth
    # doing: this is the one asset every social crawler and preview card fetches.
    from io import BytesIO

    from PIL import Image

    img = Image.open(BytesIO(png)).convert("RGB")
    img.quantize(colors=256, method=Image.MEDIANCUT,
                 dither=Image.FLOYDSTEINBERG).save(OUT, optimize=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://pulsecities.com")
    args = ap.parse_args()

    n = scored_zip_count()
    asyncio.run(render(args.base.rstrip("/"), n))
    print(f"og: wrote {OUT.relative_to(ROOT)} ({OUT.stat().st_size:,} bytes), {n} scored ZIPs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
