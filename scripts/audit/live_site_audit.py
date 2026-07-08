"""
Throwaway read-only audit of pulsecities.com production.

Drives a headless Chromium against the live site, recording for every URL:
  - final HTTP status
  - console errors / uncaught page exceptions
  - every network response >= 400 (url + status)
  - a render assertion (specific text/selector), not just a 200

Writes a JSON blob to scripts/audit/live_site_audit_result.json for the report.
No application code is touched. Run:

    PYTHONPATH=. venv/bin/python scripts/audit/live_site_audit.py
"""

import json
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "https://pulsecities.com"
OUT = Path(__file__).parent / "live_site_audit_result.json"

# (path, list of "assertions" each a (kind, value) tuple)
#   kind: "text" -> substring must appear in body text
#         "sel"  -> selector must exist
#         "js"   -> expression evaluated, must be truthy
OPERATOR_DETAIL = ["MTEK", "PHANTOM", "BREDIF", "TOWNHOUSE", "MELO"]
FALSE_POS = ["OCEANVIEW", "RIDGEWOOD", "VALLEY", "COMMUNITY", "METROPOLITAN",
             "TOORAK", "JOVIA", "BATTALION", "ARION", "HABIB"]


def audit_url(context, path, settle_ms=3500, extra_eval=None):
    page = context.new_page()
    console_errors = []
    page_errors = []
    failed_requests = []
    responses = {}

    page.on("console", lambda m: console_errors.append(f"{m.type}: {m.text}")
            if m.type in ("error",) else None)
    page.on("pageerror", lambda e: page_errors.append(str(e)))

    def on_response(resp):
        try:
            if resp.status >= 400:
                failed_requests.append({"url": resp.url, "status": resp.status})
        except Exception:
            pass
    page.on("response", on_response)

    final_status = None
    try:
        resp = page.goto(BASE + path, wait_until="domcontentloaded", timeout=30000)
        final_status = resp.status if resp else None
    except Exception as e:
        page_errors.append(f"navigation: {e}")

    # let client JS fire its fetches and render
    page.wait_for_timeout(settle_ms)

    extra = {}
    if extra_eval:
        for key, expr in extra_eval.items():
            try:
                extra[key] = page.evaluate(expr)
            except Exception as e:
                extra[key] = f"<eval error: {e}>"

    try:
        body_text = page.inner_text("body")
    except Exception:
        body_text = ""

    page.close()
    return {
        "path": path,
        "final_status": final_status,
        "console_errors": console_errors,
        "page_errors": page_errors,
        "failed_requests": failed_requests,
        "body_text": body_text,
        "extra": extra,
    }


def main():
    results = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="PulseCitiesAudit/1.0 (read-only QA)",
            viewport={"width": 1280, "height": 900},
        )

        # ---- A) Operator detail pages -------------------------------------
        for root in OPERATOR_DETAIL:
            ev = {
                # capture which /api/operators/* calls fired and their status
                "api_calls": """(() => window.performance.getEntriesByType('resource')
                    .map(e => e.name).filter(n => n.includes('/api/operators')))()""",
                "stat_props": "document.getElementById('stat-properties')?.textContent ?? null",
                "stat_acq": "document.getElementById('stat-acquisitions')?.textContent ?? null",
                "acq_rows": "document.querySelectorAll('#acq-rows tr').length",
                "portfolio_rows": "document.querySelectorAll('#portfolio-rows tr').length",
                "content_visible": "getComputedStyle(document.getElementById('content')).display !== 'none'",
                "error_visible": "getComputedStyle(document.getElementById('error-state')).display !== 'none'",
                "h1": "document.getElementById('op-root')?.textContent ?? null",
            }
            results[f"A:/operator/{root}"] = audit_url(context, f"/operator/{root}", extra_eval=ev)

        # ---- B) False-positive operators ---------------------------------
        for root in FALSE_POS:
            ev = {
                "content_visible": "!!document.getElementById('content') && getComputedStyle(document.getElementById('content')).display !== 'none'",
                "error_visible": "!!document.getElementById('error-state') && getComputedStyle(document.getElementById('error-state')).display !== 'none'",
                "is_minimal": "document.body.innerText.includes('Not an operator')",
                "api_calls": """(() => window.performance.getEntriesByType('resource')
                    .map(e => e.name).filter(n => n.includes('/api/operators')))()""",
            }
            results[f"B:/operator/{root}"] = audit_url(context, f"/operator/{root}", extra_eval=ev)

        # ---- C) Operators directory --------------------------------------
        ev = {
            "rows": """(() => Array.from(document.querySelectorAll('.op-row .op-name'))
                .map(e => e.textContent.trim()))()""",
            "links": """(() => Array.from(document.querySelectorAll('.op-row a'))
                .map(a => a.getAttribute('href')))()""",
            "count_label": "document.querySelector('p.mono')?.textContent ?? null",
        }
        results["C:/operators"] = audit_url(context, "/operators", extra_eval=ev)

        # ---- D) Core flows -----------------------------------------------
        # Home
        ev = {
            "loading_chips": """(() => Array.from(document.querySelectorAll('body *'))
                .filter(e => e.children.length === 0 && /Loading/i.test(e.textContent)).length)()""",
            "operator_cards": "document.body.innerText.includes('operator') || document.body.innerText.includes('Operator')",
            "has_watchlist": "/watchlist|watch list|saved/i.test(document.body.innerText)",
            "title": "document.title",
        }
        results["D:/"] = audit_url(context, "/", extra_eval=ev)

        # Map
        ev = {
            "has_canvas": "!!document.querySelector('canvas.maplibregl-canvas') || !!document.querySelector('canvas')",
            "maplibre": "!!window.maplibregl",
        }
        results["D:/map"] = audit_url(context, "/map", settle_ms=6000, extra_eval=ev)

        # Map autosearch
        ev = {
            "has_canvas": "!!document.querySelector('canvas')",
            "panel_text": "document.body.innerText.slice(0, 2000)",
            "has_score": "/score/i.test(document.body.innerText)",
        }
        results["D:/map?q=11216"] = audit_url(context, "/map?q=11216", settle_ms=7000, extra_eval=ev)

        # Neighborhood deep link
        ev = {
            "has_score": "/displacement/i.test(document.body.innerText)",
            "body_head": "document.body.innerText.slice(0, 1500)",
        }
        results["D:/neighborhood/11216"] = audit_url(context, "/neighborhood/11216", settle_ms=6000, extra_eval=ev)

        # Status
        ev = {
            "body": "document.body.innerText",
        }
        results["D:/status"] = audit_url(context, "/status", settle_ms=5000, extra_eval=ev)

        # Methodology
        results["D:/methodology"] = audit_url(context, "/methodology", settle_ms=2500,
            extra_eval={"title": "document.title", "h1": "document.querySelector('h1')?.textContent ?? null"})

        # About
        results["D:/about"] = audit_url(context, "/about", settle_ms=2500,
            extra_eval={"title": "document.title", "h1": "document.querySelector('h1')?.textContent ?? null"})

        browser.close()

    OUT.write_text(json.dumps(results, indent=2))
    print("WROTE", OUT)
    # quick console summary
    for k, v in results.items():
        ce = len(v["console_errors"]); pe = len(v["page_errors"]); fr = len(v["failed_requests"])
        print(f"{k:30s} status={v['final_status']} console_err={ce} page_err={pe} failed_req={fr}")


if __name__ == "__main__":
    main()
