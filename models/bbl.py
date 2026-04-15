"""
BBL (Borough-Block-Lot) normalization utility.

BBL is the universal join key across all NYC data sources.
Every scraper must call normalize_bbl() before persisting any record.

Supported input formats:
  '1000010001'    — 10-digit zero-padded (ACRIS, MapPLUTO canonical)
  '1-00001-0001'  — hyphenated (NYC Open Data permits, 311, evictions)
  '1-1-1'         — short hyphenated (rare, but present in some exports)

Canonical output: 10-digit zero-padded string 'BBBBBBBBBLL'
  B = 1-digit borough code (1-5)
  B = 5-digit zero-padded block number
  L = 4-digit zero-padded lot number

Returns None if the input cannot be parsed — record goes to quarantine.
"""

import re

# Matches 10-digit plain BBL (already canonical)
_BBL_PLAIN = re.compile(r"^\d{10}$")

# Matches hyphenated BBL: B-BBBBB-LLLL or B-B-L (short form)
_BBL_HYPHEN = re.compile(r"^(\d{1})-(\d{1,5})-(\d{1,4})$")


def normalize_bbl(bbl: str | int | None) -> str | None:
    """
    Normalize a BBL value to canonical 10-digit zero-padded string.

    Returns None if the value is missing or unparseable.
    Callers must route None records to scraper_quarantine, not the raw table.
    """
    if bbl is None:
        return None

    raw = str(bbl).strip()

    if not raw:
        return None

    # Handle float-formatted BBL from MapPLUTO: "1000010010.00000000" → "1000010010"
    if "." in raw:
        raw = raw.split(".")[0]

    # Already canonical — validate borough code while we're here
    if _BBL_PLAIN.match(raw):
        if raw[0] not in "12345":
            return None
        return raw

    # Hyphenated format: 1-00001-0001 or 1-1-1
    m = _BBL_HYPHEN.match(raw)
    if m:
        borough = m.group(1)
        if borough not in "12345":
            return None
        block = m.group(2).zfill(5)
        lot = m.group(3).zfill(4)
        return f"{borough}{block}{lot}"

    return None


def bbl_to_parts(bbl: str) -> tuple[int, int, int] | None:
    """
    Split a canonical 10-digit BBL into (borough, block, lot) integers.
    Returns None if the input is not a valid canonical BBL.
    """
    canonical = normalize_bbl(bbl)
    if canonical is None:
        return None
    return int(canonical[0]), int(canonical[1:6]), int(canonical[6:10])
