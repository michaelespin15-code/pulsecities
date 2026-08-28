"""
Block digest — a monthly report on the tax block around a watched building.

Nine of thirteen real subscribers watch one building, and a building watch is
event-driven: quiet months send nothing. Across the twelve watched buildings
there were zero events in thirty days and three of them have had none in a
year, so the only channel those people have is silent most of the time. This is
the wider radius, measured before it was built:

    scope     blocks with activity in 30 days     volume
    building  3 of 12                             a handful
    block     9 of 12 by ingest, 5 of 12 by       1 to 36 records
              event date, 12 of 12 counting
              every 311 type
    ZIP       12 of 12                            1,000 to 4,200 records

A ZIP is too big to read and too big to care about. A tax block is a median of
59 parcels, which really is the reader's own street, and it is small enough
that every line names an address they can walk past.

The measurement also killed the obvious design. A digest of *what happened this
month* is empty for seven of twelve blocks, which is the same hole one radius
out, so this report carries two sections: what was recorded on the block since
the last run, and where the block stands right now. The second section reads
from standing state (open violations, twelve months of deeds and evictions) and
is never empty. Every one of the eleven watched blocks has between 8 and 400
open violations on it today.

    PYTHONPATH=. venv/bin/python -m scripts.block_digest [--dry-run] [--email X]

Cron runs this on the first of the month (see deploy/pulsecities.cron). State is
a watermark in block_digest_state.json; a missing file starts the window 30 days
back, so a fresh deploy cannot mail anyone a year of history.
"""

import argparse
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import resend
from sqlalchemy import text

from api.freshness import (FRESHNESS_SOURCES, db_through_sql, feed_anchor,
                           real_date, window_sql)
from config.logging_config import configure_logging
from config.nyc import DISPLACEMENT_COMPLAINT_TYPES
from models.database import get_scraper_db
from scripts.building_alerts import _wait_for_pipeline
from api.permit_kinds import DECONVERSION_PARAMS, deconversion_sql, trade_label
from scripts.lib import mailer
from scripts.lib.backfill_windows import exclusion

resend.api_key = os.getenv("RESEND_API_KEY", "")

configure_logging()
logger = logging.getLogger(__name__)

STATE_PATH = Path(__file__).parent / "block_digest_state.json"

# Twelve lines is what fits above the fold on a phone. The busiest watched
# block ran 36 records in thirty days and 1062 Elton Street's runs 108, so the
# cap is doing real work rather than guarding a hypothetical.
MAX_LINES = 12

# Per-feed fetch cap, so a block with 400 violations does not pull 400 rows to
# throw 388 of them away.
FETCH_CAP = 40

BOROUGH_NAMES = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn",
                 "4": "Queens", "5": "Staten Island"}

# What the reader would open first. Rank runs severity before recency: a deed
# recorded three weeks ago outranks a paint complaint filed yesterday, because
# the deed is the one that changes who they are dealing with.
WEIGHT = {"deed": 0, "eviction": 1, "vacate": 2, "hazard": 3,
          "permit": 4, "violation": 5, "complaint": 6}

# Feed slug per event kind, so each section can cite the date its source is
# current through. Same map the property page reads.
FEED_OF_KIND = {"deed": "acris", "eviction": "evictions", "permit": "permits",
                "vacate": "violations", "hazard": "violations",
                "violation": "violations", "complaint": "complaints"}

SOURCE_NAME = {"acris": "NYC ACRIS", "evictions": "NYC marshal eviction records",
               "permits": "NYC DOB job filings", "complaints": "NYC 311",
               "violations": "NYC HPD violations"}


def block_span(bbl: str) -> tuple[str, str]:
    """The BBL range covering one tax block, as (lowest lot, highest lot).

    A range rather than `bbl LIKE '304447%'`: the raw tables carry a plain btree
    on bbl and this database is not in the C collation, so the LIKE form cannot
    use it. The same twelve-block scan took 1.9s as a range and had not finished
    in two minutes as a prefix match.
    """
    return bbl[:6] + "0000", bbl[:6] + "9999"


def _addr_title(a: str) -> str:
    # str.title() capitalizes after digits ("233Rd St"); put ordinals back.
    return re.sub(r"(\d)(St|Nd|Rd|Th)\b", lambda m: m.group(1) + m.group(2).lower(),
                  (a or "").strip().title())


def _fmt_date(d) -> str:
    """Month and day for this year, and the year too for anything older.

    The window here is ingest time, so a record the source published late is a
    legitimate entry with an old date on it. Printed as "Apr 24" that reads as
    four months ago rather than two years, which is the difference between a
    report and a wrong one.
    """
    if not d:
        return "date not on record"
    return (d.strftime("%b %-d") if d.year == date.today().year
            else d.strftime("%b %-d, %Y"))


def _en_date(d) -> str:
    return d.strftime("%B %-d, %Y") if d else ""


def _money(n) -> str:
    return f"${float(n):,.0f}" if n else "no amount on record"


def _count(n: int, noun: str) -> str:
    return f"{n:,} {noun}" if n == 1 else f"{n:,} {noun}s"


def load_watchers(db) -> list[dict]:
    """One entry per subscriber, carrying every block they watch.

    Grouped by subscriber rather than by watch: one reader follows three
    buildings that sit on two blocks, and a per-watch loop would mail them the
    same block report twice.
    """
    rows = db.execute(text("""
        SELECT s.email, s.bbl, s.unsubscribe_token,
               COALESCE(p.address, c.address) AS address
        FROM subscribers s
        LEFT JOIN parcels p ON p.bbl = s.bbl
        LEFT JOIN condo_unit_addresses c ON c.bbl = s.bbl
        WHERE s.bbl IS NOT NULL AND s.confirmed = true
        ORDER BY s.email, s.bbl
    """)).fetchall()

    by_email: dict[str, dict] = {}
    for r in rows:
        who = by_email.setdefault(r.email, {"email": r.email, "token": r.unsubscribe_token,
                                            "blocks": {}})
        blk = who["blocks"].setdefault(r.bbl[:6], {"block": r.bbl[:6], "bbls": [], "addresses": []})
        blk["bbls"].append(r.bbl)
        if r.address:
            blk["addresses"].append(_addr_title(r.address))
    for who in by_email.values():
        who["blocks"] = list(who["blocks"].values())
    return list(by_email.values())


_EVENT_SQL = {
    # ownership_raw is the one feed with no address column of its own, so the
    # deed row carries NULL and the block's address map fills it in below.
    "deed": """
        SELECT bbl, NULL AS address, doc_date AS event_date, party_name, doc_amount,
               NULL AS a, NULL AS b, NULL AS c
        FROM ownership_raw
        WHERE bbl >= :lo AND bbl <= :hi AND created_at > :since
          AND doc_type IN ('DEED', 'DEEDP') AND party_type = '2'
        /*SKIP*/
        ORDER BY doc_date DESC NULLS LAST LIMIT :cap
    """,
    "eviction": """
        SELECT bbl, address, executed_date AS event_date, NULL AS party_name,
               NULL AS doc_amount, eviction_type AS a, docket_number AS b, NULL AS c
        FROM evictions_raw
        WHERE bbl >= :lo AND bbl <= :hi AND created_at > :since
        /*SKIP*/
        ORDER BY executed_date DESC NULLS LAST LIMIT :cap
    """,
    "permit": """
        SELECT bbl, address, filing_date AS event_date, NULL AS party_name,
               NULL AS doc_amount, work_type AS a, permit_type AS b,
               job_description AS c
        FROM permits_raw
        WHERE bbl >= :lo AND bbl <= :hi AND created_at > :since
        /*SKIP*/
        ORDER BY filing_date DESC NULLS LAST LIMIT :cap
    """,
    "violation": """
        SELECT bbl, address,
               COALESCE(nov_issued_date, inspection_date) AS event_date,
               NULL AS party_name, NULL AS doc_amount,
               violation_class AS a, description AS b, NULL AS c
        FROM violations_raw
        WHERE bbl >= :lo AND bbl <= :hi AND created_at > :since
        /*SKIP*/
        ORDER BY COALESCE(nov_issued_date, inspection_date) DESC NULLS LAST LIMIT :cap
    """,
    # Housing types only, the same list /neighborhood counts on. A block report
    # that opened with "Litter Basket Request" would teach the reader to ignore
    # the whole email.
    "complaint": """
        SELECT bbl, address, created_date AS event_date, NULL AS party_name,
               NULL AS doc_amount, complaint_type AS a, descriptor AS b,
               status AS c
        FROM complaints_raw
        WHERE bbl >= :lo AND bbl <= :hi AND created_at > :since
          AND upper(complaint_type) = ANY(:housing_types)
        /*SKIP*/
        ORDER BY created_date DESC NULLS LAST LIMIT :cap
    """,
}

_TABLE_OF_KIND = {
    "deed": "ownership_raw", "permit": "permits_raw", "eviction": "evictions_raw",
    "violation": "violations_raw", "complaint": "complaints_raw",
}

_COUNT_SQL = {
    "deed": """SELECT count(*) FROM ownership_raw WHERE bbl >= :lo AND bbl <= :hi
               AND created_at > :since AND doc_type IN ('DEED','DEEDP') AND party_type='2'""",
    "eviction": """SELECT count(*) FROM evictions_raw WHERE bbl >= :lo AND bbl <= :hi
                   AND created_at > :since""",
    "permit": """SELECT count(*) FROM permits_raw WHERE bbl >= :lo AND bbl <= :hi
                 AND created_at > :since""",
    "violation": """SELECT count(*) FROM violations_raw WHERE bbl >= :lo AND bbl <= :hi
                    AND created_at > :since""",
    "complaint": """SELECT count(*) FROM complaints_raw WHERE bbl >= :lo AND bbl <= :hi
                    AND created_at > :since AND upper(complaint_type) = ANY(:housing_types)""",
}

# Permit types that mean the building is being changed rather than maintained.
HEAVY_PERMITS = {"NB", "DM", "A1"}

# Every HPD description opens with the statute it was written under, and some
# open with six of them. The first draft of this report printed
# "HMC ADM CODE: 27-2017.4 ABATE THE INFESTATION CONSISTING OF ROACHES IN THE
# ENTIRE APARTMENT LOCATED AT APT 1L, 1st STO", which spends its whole line on
# the citation and truncates mid-word on the part a tenant would care about.
_CITATION_TAIL = re.compile(r"^.*:\s*", re.S)
_LEADING_SECTION = re.compile(r"^[\s\u00a7\d.,;()\-]+")
# HPD appends the apartment the inspector stood in. A block report is about
# buildings, the clause is what pushed the useful half of the sentence past the
# character budget, and printing a stranger's apartment number serves nobody.
_UNIT_TAIL = re.compile(r"\s+(located at|at apt\.?|in apt\.?)\b.*$", re.I | re.S)


def _plain(description: str, limit: int = 110) -> str:
    """The instruction inside an HPD violation, without its statute preamble.

    Cuts at the last colon, since the citation always precedes it and the order
    to the landlord always follows. Then drops any section numbers that survived
    and takes the shouting out: these arrive in capitals, and a paragraph of
    capitals in an email reads as spam to a person and to a filter.
    """
    body = (description or "").strip()
    if not body:
        return ""
    tail = _CITATION_TAIL.sub("", body).strip()
    if len(tail) >= 20:
        body = tail
    body = _LEADING_SECTION.sub("", body).strip()
    body = _UNIT_TAIL.sub("", body).strip()
    if not body:
        return ""
    if len(body) > limit:
        body = body[:limit].rsplit(" ", 1)[0]
    body = body[:1].upper() + body[1:].lower()
    return body.rstrip(" ,;.") + "."


def _classify(kind: str, row) -> str:
    """The rank bucket a row belongs to, which is not always its feed.

    HPD grades its own severity and the report should use it: class I is a
    vacate order and class C is immediately hazardous, so both outrank a
    new-building permit, while a class A violation does not.
    """
    if kind == "violation":
        cls = (row.a or "").strip().upper()
        if cls == "I":
            return "vacate"
        if cls == "C":
            return "hazard"
        return "violation"
    return kind


def _describe(kind: str, row) -> str:
    if kind == "deed":
        who = (row.party_name or "unknown party").strip()
        return f"Sold to {who} for {_money(row.doc_amount)}, {_fmt_date(row.event_date)}."
    if kind == "eviction":
        what = (row.a or "Eviction").strip()
        return f"{what} eviction executed, {_fmt_date(row.event_date)}, docket {row.b}."
    if kind == "permit":
        what = trade_label(row.a)
        heavy = (row.b or "").strip().upper() in HEAVY_PERMITS
        line = (f"Permit filed ({what}), {_fmt_date(row.event_date)}." if what
                else f"Permit filed, {_fmt_date(row.event_date)}.")
        desc = _plain(row.c)
        if heavy and desc:
            line += f" {desc}"
        return line
    if kind == "complaint":
        what = (row.a or "Housing").strip().title()
        desc = (row.b or "").strip().title()
        line = f"311 complaint: {what}"
        if desc and desc.upper() != what.upper():
            line += f", {desc}"
        return line + f", {_fmt_date(row.event_date)}."
    cls = (row.a or "").strip().upper()
    label = {"I": "class I, vacate order", "C": "class C, immediately hazardous",
             "B": "class B, hazardous", "A": "class A"}.get(cls, "unclassified")
    line = f"HPD violation issued ({label}), {_fmt_date(row.event_date)}."
    desc = _plain(row.b)
    if desc and cls in ("I", "C"):
        line += f" {desc}"
    return line


def block_addresses(db, lo: str, hi: str) -> dict[str, str]:
    """Every address on the block, keyed by BBL.

    One lookup instead of a join on each of the five event queries. It also
    fixes the feeds against each other: HPD and 311 spell the same building
    differently often enough that a report drawn straight from the raw rows
    lists one address twice under two spellings.
    """
    out: dict[str, str] = {}
    for sql in ("SELECT bbl, address FROM parcels WHERE bbl >= :lo AND bbl <= :hi",
                "SELECT bbl, address FROM condo_unit_addresses "
                "WHERE bbl >= :lo AND bbl <= :hi"):
        for bbl, address in db.execute(text(sql), {"lo": lo, "hi": hi}).fetchall():
            # PLUTO carries a bare street name for some lots, and "Locke Street"
            # with no number is not an address a reader can walk to. When that
            # happens the feed's own address usually has the number, so leave
            # the key unset and let the caller fall through to it.
            if address and address.strip()[:1].isdigit() and bbl not in out:
                out[bbl] = _addr_title(address)
    return out


def recent_events(db, block: dict, since: datetime) -> tuple[list[dict], int]:
    """Records added to this block since the last run, ranked, plus the true total.

    The window is ingest time rather than event date, for the reason the daily
    building alert uses it: ACRIS republishes deeds months after they were
    signed, and a report keyed on doc_date would skip every one of them.
    """
    lo, hi = block["block"] + "0000", block["block"] + "9999"
    params = {"lo": lo, "hi": hi, "since": since,
              "housing_types": list(DISPLACEMENT_COMPLAINT_TYPES)}
    watched = set(block["bbls"])
    known = block_addresses(db, lo, hi)

    events, total = [], 0
    for kind, sql in _EVENT_SQL.items():
        # History we imported is not activity that happened. The 2026-08-28 DOB
        # NOW load put 485,443 permits going back to 2021 inside this window;
        # without this, the first report after any backfill is a list of old
        # records under the heading "recorded since the last report".
        skip_sql, skip_params = exclusion(db, _TABLE_OF_KIND[kind])
        p = {**params, **skip_params}
        total += db.execute(text(_COUNT_SQL[kind] + skip_sql), p).scalar() or 0
        for row in db.execute(text(sql.replace("/*SKIP*/", skip_sql)),
                              {**p, "cap": FETCH_CAP}).fetchall():
            bucket = _classify(kind, row)
            events.append({
                "kind": kind,
                "bucket": bucket,
                "bbl": row.bbl,
                "address": (known.get(row.bbl) or _addr_title(row.address)
                            or f"BBL {row.bbl}"),
                "mine": row.bbl in watched,
                "date": row.event_date,
                "line": _describe(kind, row),
            })

    # The reader's own building first whatever it did, then severity, then
    # recency. Someone who watches 1062 Elton Street opens this to see 1062
    # Elton Street.
    events = _merge_repeats(events)
    events.sort(key=lambda e: (not e["mine"], WEIGHT[e["bucket"]],
                               -(e["date"].toordinal() if e["date"] else 0)))
    return _cap_per_address(events)[:MAX_LINES], total


# What a group of identical rows says instead of saying it four times.
_PLURAL_LINE = {
    "vacate": "{n} HPD vacate orders issued, {when}.",
    "hazard": "{n} HPD violations issued (class C, immediately hazardous), {when}.",
    "violation": "{n} HPD violations issued, {when}.",
    "complaint": "{n} 311 housing complaints filed, {when}.",
    "permit": "{n} permits filed, {when}.",
    "deed": "{n} deeds recorded, {when}.",
    "eviction": "{n} evictions executed, {when}.",
}


def _merge_repeats(events: list[dict]) -> list[dict]:
    """Collapse same building, same kind, same day into one line.

    HPD writes one violation per apartment, so an inspector's afternoon at a
    thirty-unit building arrives as thirty rows dated the same day. Printed
    straight, four identical lines for 33 Buffalo Avenue took a third of a
    twelve-line report and told the reader one fact.
    """
    groups: dict[tuple, list[dict]] = defaultdict(list)
    order: list[tuple] = []
    for e in events:
        key = (e["bbl"], e["bucket"], e["date"])
        if key not in groups:
            order.append(key)
        groups[key].append(e)

    merged = []
    for key in order:
        rows = groups[key]
        first = rows[0]
        if len(rows) == 1:
            merged.append({**first, "rows": 1})
            continue
        template = _PLURAL_LINE.get(first["bucket"]) or _PLURAL_LINE[first["kind"]]
        merged.append({**first, "rows": len(rows),
                       "line": template.format(n=len(rows), when=_fmt_date(first["date"]))})
    return merged


# One neighbour should not own the whole report. On a three-parcel condo block
# a single address filled seven of nine lines, which reads as a report about
# that building rather than about the block the reader lives on. Merging by day
# does not fix it, because those really were seven different days.
MAX_LINES_PER_ADDRESS = 4


def _cap_per_address(events: list[dict]) -> list[dict]:
    """Keep the ranking, but let no single address take more than its share.

    Runs after the sort, so what survives per address is that address's most
    serious and most recent. The watched building is exempt: someone who asked
    to be told about one building is not being told too much about it.
    """
    seen: dict[str, int] = defaultdict(int)
    kept = []
    for e in events:
        if not e["mine"]:
            if seen[e["bbl"]] >= MAX_LINES_PER_ADDRESS:
                continue
            seen[e["bbl"]] += 1
        kept.append(e)
    return kept


def standing_state(db, block: dict) -> dict:
    """Where the block sits today, independent of whether anything happened.

    This is the half of the report that cannot come up empty. Measured across
    the eleven watched blocks: 8 to 400 open violations each, 1 to 27 buildings
    carrying them, 0 to 7 deeds in the last twelve months.
    """
    lo, hi = block["block"] + "0000", block["block"] + "9999"
    # The twelve months of deeds end at the last day ACRIS published, not at
    # today. On 2026-08-28 that was 28 days back, so a calendar window spent a
    # month of its year on days that could not contain a sale, and this report
    # would have told a reader their block was quieter than it was.
    p = {"lo": lo, "hi": hi, "anchor": feed_anchor(db)}
    q = lambda sql: db.execute(text(sql), p).scalar() or 0  # noqa: E731

    open_where = ("bbl >= :lo AND bbl <= :hi "
                  "AND upper(COALESCE(current_status,'')) NOT LIKE '%CLOSE%' "
                  "AND upper(COALESCE(current_status,'')) NOT LIKE '%DISMISS%'")
    worst = db.execute(text(f"""
        SELECT bbl, max(address) AS address, count(*) AS n
        FROM violations_raw WHERE {open_where}
        GROUP BY bbl ORDER BY n DESC, bbl LIMIT 1
    """), p).fetchone()

    # Every window here is a real-date window. `real_date` is the one reader of
    # the rule that a record cannot be dated after the day we wrote it down;
    # without it a filer's typo puts a phantom sale in this month's report.
    return {
        "parcels": q("SELECT count(*) FROM parcels WHERE bbl >= :lo AND bbl <= :hi"),
        "units": int(q("SELECT COALESCE(sum(units_res),0) FROM parcels "
                       "WHERE bbl >= :lo AND bbl <= :hi")),
        "open_violations": q(f"SELECT count(*) FROM violations_raw WHERE {open_where}"),
        "violation_buildings": q(f"SELECT count(DISTINCT bbl) FROM violations_raw "
                                 f"WHERE {open_where}"),
        "deeds_12m": q(
            "SELECT count(*) FROM ownership_raw WHERE bbl >= :lo AND bbl <= :hi "
            "AND doc_type IN ('DEED','DEEDP') AND party_type = '2' "
            "AND " + window_sql("doc_date", 365) + " AND "
            + real_date("doc_date")),
        "sold_buildings_12m": q(
            "SELECT count(DISTINCT bbl) FROM ownership_raw WHERE bbl >= :lo AND bbl <= :hi "
            "AND doc_type IN ('DEED','DEEDP') AND party_type = '2' "
            "AND " + window_sql("doc_date", 365) + " AND "
            + real_date("doc_date")),
        "evictions_12m": q(
            "SELECT count(*) FROM evictions_raw WHERE bbl >= :lo AND bbl <= :hi "
            "AND executed_date > CURRENT_DATE - INTERVAL '365 days' AND "
            + real_date("executed_date")),
        # Filings that propose removing homes. Rare enough that a block with one
        # is worth a sentence, and specific enough that the reader can check it:
        # 853 buildings citywide across the whole DOB NOW era.
        "deconversions": db.execute(text(f"""
            SELECT count(DISTINCT pr.bbl) FROM permits_raw pr
            JOIN parcels pc ON pc.bbl = pr.bbl
            WHERE pr.bbl >= :lo AND pr.bbl <= :hi AND {deconversion_sql("pr", "pc")}
        """), {**p, **DECONVERSION_PARAMS}).scalar() or 0,
        "worst_address": _addr_title(worst.address) if worst and worst.address else None,
        "worst_bbl": worst.bbl if worst else None,
        "worst_open": int(worst.n) if worst else 0,
    }


def feeds_through(db) -> dict:
    """As-of date per feed, so each section can say when its source was current.

    Reads the same query /api/status publishes, which is the point: a number
    lifted out of this email and a number read off the site cannot disagree
    about what date it was true on.
    """
    out = {}
    for slug, _scraper, _table, _col, _days in FRESHNESS_SOURCES:
        try:
            v = db.execute(text(db_through_sql(slug))).scalar()
        except Exception:
            logger.warning("through-date query failed for %s", slug, exc_info=True)
            continue
        if v is not None:
            out[slug] = v.date() if hasattr(v, "date") else v
    return out


def build_report(db, watcher: dict, since: datetime, feeds: dict) -> dict:
    """Everything one subscriber's email needs, with nothing rendered yet."""
    blocks = []
    for block in watcher["blocks"]:
        events, total = recent_events(db, block, since)
        state = standing_state(db, block)
        label = (block["addresses"][0] if block["addresses"]
                 else f"BBL {block['bbls'][0]}")
        blocks.append({**block, "label": label, "events": events,
                       "total": total, "state": state,
                       "borough": BOROUGH_NAMES.get(block["block"][0], "New York")})
    return {"email": watcher["email"], "token": watcher["token"],
            "blocks": blocks, "feeds": feeds,
            "new_records": sum(b["total"] for b in blocks)}


_SHELL = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>__TITLE__</title>
</head>
<body style="margin:0;padding:0;background:#EFEBE2;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#EFEBE2;padding:36px 16px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:540px;">
        <tr><td style="background:#FBFAF7;border:1px solid #D9D4C9;border-top:3px solid #E4590F;padding:28px 28px 26px;">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr><td style="padding-bottom:10px;border-bottom:2px solid #1C2430;">
              <table width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:14px;font-weight:700;color:#1C2430;letter-spacing:0.2em;">PULSECITIES</td>
                <td align="right" style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;color:#6D7480;letter-spacing:0.14em;">BLOCK REPORT</td>
              </tr></table>
            </td></tr>
            __BODY__
          </table>
        </td></tr>
        <tr><td style="padding:16px 6px 0;">
          <p style="margin:0 0 8px;font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;color:#8A8578;line-height:1.7;">You watch a building at pulsecities.com, so once a month this covers its whole tax block. Deeds and violations name a party of record and imply no wrongdoing.</p>
          <p style="margin:0;font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;color:#8A8578;line-height:1.7;"><a href="https://pulsecities.com/api/unsubscribe?token=__TOKEN__" style="color:#8A8578;">Stop these emails</a> anytime, one click.</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

_STAMP = ('<tr><td style="padding:10px 0 20px;"><span style="font-family:Menlo,'
          "Consolas,'Courier New',monospace;font-size:10px;color:#9A948A;"
          'letter-spacing:0.14em;text-transform:uppercase;">{}</span></td></tr>')

_HEADING = ('<tr><td style="padding:6px 0 12px;border-top:1px solid #D9D4C9;">'
            "<span style=\"font-family:Menlo,Consolas,'Courier New',monospace;"
            'font-size:10px;color:#6D7480;letter-spacing:0.14em;'
            'text-transform:uppercase;">{}</span></td></tr>')

_LINE = ('<tr><td style="padding-bottom:14px;"><p style="margin:0;'
         "font-family:Georgia,'Times New Roman',serif;font-size:15px;"
         'color:#1C2430;line-height:1.7;"><strong>{addr}</strong><br>{body}</p></td></tr>')

_PROSE = ('<tr><td style="padding-bottom:14px;"><p style="margin:0;'
          "font-family:Georgia,'Times New Roman',serif;font-size:15px;"
          'color:#1C2430;line-height:1.7;">{}</p></td></tr>')

_CITE = ('<tr><td style="padding-bottom:18px;"><p style="margin:0;'
         "font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;"
         'color:#9A948A;line-height:1.6;">{}</p></td></tr>')


def _esc(s) -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _cite_text(feeds: dict, slugs: list[str]) -> str:
    """One source line naming every feed a section drew on, and its as-of date."""
    parts = []
    for slug in slugs:
        d = feeds.get(slug)
        if d and slug in SOURCE_NAME:
            parts.append(f"{SOURCE_NAME[slug]}, current through {_en_date(d)}")
    if not parts:
        return ""
    return "Source: " + "; ".join(parts) + "."


def _state_sentences(block: dict) -> list[str]:
    """The standing picture, in plain sentences a reader can quote."""
    s = block["state"]
    out = []
    if s["open_violations"]:
        out.append(
            f"{_count(s['open_violations'], 'open HPD violation')} across "
            f"{_count(s['violation_buildings'], 'building')} on this block."
        )
    else:
        out.append("No open HPD violations are on record anywhere on this block.")
    if s["worst_address"] and s["worst_open"]:
        out.append(f"The most cited address is {s['worst_address']}, "
                   f"carrying {_count(s['worst_open'], 'open violation')}.")
    if s["deeds_12m"]:
        sold = _count(s["sold_buildings_12m"], "building")
        if s["deeds_12m"] > s["sold_buildings_12m"]:
            # More deeds than buildings means at least one address traded twice,
            # which is the shape a flip leaves in the record and is worth saying.
            out.append(f"{sold} changed hands in the last twelve months, across "
                       f"{_count(s['deeds_12m'], 'recorded deed')}.")
        else:
            out.append(f"{sold} changed hands in the last twelve months.")
    else:
        out.append("No building on the block has recorded a deed in the last twelve months.")
    if s["evictions_12m"]:
        out.append(f"{_count(s['evictions_12m'], 'marshal eviction')} executed here in the "
                   f"last twelve months.")
    if s.get("deconversions"):
        # No date window: these are filings, not events, and one from 2022 that
        # took a building from six homes to one still describes the block.
        out.append(
            f"{_count(s['deconversions'], 'building')} on this block "
            f"{'has' if s['deconversions'] == 1 else 'have'} filed to reduce the "
            f"number of homes {'it holds' if s['deconversions'] == 1 else 'they hold'}, "
            f"on the applicant's own description.")
    if s["parcels"]:
        out.append(f"The block holds {_count(s['parcels'], 'parcel')}"
                   + (f" and {_count(s['units'], 'residential unit')}." if s["units"] else "."))
    return out


def render(report: dict) -> tuple[str, str, str, int]:
    """Returns (subject, html, text, content_items) for one subscriber."""
    feeds = report["feeds"]
    rows, text_lines = [], []
    items = 0

    for block in report["blocks"]:
        state = block["state"]
        stamp = (f"{block['label']} &middot; {block['borough']} block "
                 f"{block['block'][1:].lstrip('0') or '0'} &middot; "
                 f"{block['state']['parcels']} parcels")
        rows.append(_STAMP.format(stamp))
        text_lines.append(f"{block['label']} | {block['borough']} block "
                          f"{block['block'][1:].lstrip('0')} | {state['parcels']} parcels")
        text_lines.append("")

        rows.append(_HEADING.format("Recorded since the last report"))
        text_lines.append("RECORDED SINCE THE LAST REPORT")
        if block["events"]:
            for e in block["events"]:
                addr = e["address"] + (" (your building)" if e["mine"] else "")
                rows.append(_LINE.format(addr=_esc(addr), body=_esc(e["line"])))
                text_lines.append(f"- {addr}: {e['line']}")
                items += 1
            # Against records, not lines. A merged line stands for several
            # rows, and counting lines here claimed six records were hidden
            # when the four lines above already covered all ten.
            hidden = block["total"] - sum(e.get("rows", 1) for e in block["events"])
            if hidden > 0:
                more = f"And {_count(hidden, 'more record')} on this block."
                rows.append(_PROSE.format(_esc(more)))
                text_lines.append(f"- {more}")
        else:
            none = ("Nothing new was recorded on this block since the last report. "
                    "The standing picture below is unchanged.")
            rows.append(_PROSE.format(_esc(none)))
            text_lines.append(none)
        cite = _cite_text(feeds, ["acris", "evictions", "violations", "complaints"])
        if cite:
            rows.append(_CITE.format(_esc(cite)))
            text_lines.append(cite)
        text_lines.append("")

        rows.append(_HEADING.format("Where the block stands"))
        text_lines.append("WHERE THE BLOCK STANDS")
        for sentence in _state_sentences(block):
            rows.append(_PROSE.format(_esc(sentence)))
            text_lines.append(f"- {sentence}")
            items += 1
        cite = _cite_text(feeds, ["violations", "acris", "evictions"])
        if cite:
            rows.append(_CITE.format(_esc(cite)))
            text_lines.append(cite)
        text_lines.append("")

        links = []
        for bbl in block["bbls"]:
            links.append(f'<a href="https://pulsecities.com/property/{_esc(bbl)}" '
                         f'style="color:#C2410C;">pulsecities.com/property/{_esc(bbl)}</a>')
            text_lines.append(f"Your building: https://pulsecities.com/property/{bbl}")
        rows.append(_PROSE.format("The full record for your building:<br>" + "<br>".join(links)))
        if state["worst_bbl"] and state["worst_bbl"] not in block["bbls"]:
            rows.append(_PROSE.format(
                f'The most cited address on the block: <a href="https://pulsecities.com/'
                f'property/{_esc(state["worst_bbl"])}" style="color:#C2410C;">'
                f'{_esc(state["worst_address"] or state["worst_bbl"])}</a>'))
            text_lines.append("Most cited on the block: "
                              f"https://pulsecities.com/property/{state['worst_bbl']}")
        text_lines.append("")

    first = report["blocks"][0]
    n = report["new_records"]
    where = first["label"]
    others = len(report["blocks"]) - 1
    if others:
        where += (" and one other block" if others == 1
                  else f" and {others} other blocks")
    if n:
        subject = f"{_count(n, 'new record')} on your block, near {where}"
    else:
        open_v = first["state"]["open_violations"]
        subject = (f"Your block near {where}: {_count(open_v, 'open violation')} on record"
                   if open_v else f"Your block near {where}: a quiet month on record")

    html = (_SHELL.replace("__TITLE__", _esc(subject))
            .replace("__BODY__", "".join(rows))
            .replace("__TOKEN__", _esc(report["token"])))
    return subject, html, "\n".join(text_lines).strip(), items


def run(dry_run: bool = False, email_filter: str | None = None,
        limit: int | None = None) -> None:
    if not dry_run and not resend.api_key:
        logger.error("RESEND_API_KEY not set. Aborting block digest.")
        sys.exit(1)

    # Same reason the daily alert waits: this run advances a created_at
    # watermark, and scanning mid-ingest steps past rows that commit a minute
    # later. Those rows are then never in any window again.
    if not dry_run and not _wait_for_pipeline():
        sys.exit(1)

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=30)
    if STATE_PATH.exists():
        try:
            state = json.loads(STATE_PATH.read_text())
            if state.get("last_run"):
                since = datetime.fromisoformat(state["last_run"])
        except (ValueError, OSError):
            logger.warning("Unreadable state file %s; using the 30d window", STATE_PATH)

    with get_scraper_db() as db:
        watchers = load_watchers(db)
        if email_filter:
            watchers = [w for w in watchers if w["email"] == email_filter]
        if limit:
            watchers = watchers[:limit]
        feeds = feeds_through(db)
        reports = [build_report(db, w, since, feeds) for w in watchers]

    logger.info("Block digest: %d subscriber(s), window opens %s",
                len(reports), since.isoformat())

    sent = failed = 0
    for report in reports:
        subject, html, text_body, items = render(report)
        if dry_run:
            print(f"--- would send to {report['email']}: {subject}")
            print(text_body)
            print()
            continue
        try:
            ok = mailer.send(
                to=report["email"], subject=subject, html=html, text=text_body,
                content_items=items,
                headers={
                    "List-Unsubscribe":
                        f"<https://pulsecities.com/api/unsubscribe?token={report['token']}>",
                    "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
                },
            )
            sent += 1 if ok else 0
            failed += 0 if ok else 1
        except mailer.EmailRefused as exc:
            failed += 1
            logger.warning("Block digest refused for %s: %s", report["email"], exc)
        except Exception:
            failed += 1
            logger.exception("Failed to send block digest to %s", report["email"])

    logger.info("Block digest complete: %d sent, %d failed", sent, failed)

    if not dry_run:
        if failed:
            # Same contract as the daily alert: hold the watermark so the next
            # run covers what this one missed. A duplicate month beats a
            # skipped one.
            logger.warning("Send failures; watermark not advanced, next run covers the gap.")
            return
        tmp = STATE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"last_run": now.isoformat()}))
        os.replace(tmp, STATE_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monthly block digest for building watchers")
    parser.add_argument("--dry-run", action="store_true",
                        help="print reports instead of sending, don't advance the watermark")
    parser.add_argument("--email", help="only this subscriber")
    parser.add_argument("--limit", type=int, help="only the first N subscribers")
    args = parser.parse_args()
    run(dry_run=args.dry_run, email_filter=args.email, limit=args.limit)
