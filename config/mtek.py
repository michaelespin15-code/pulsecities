"""
MTEK portfolio — 37 tracked BBLs with acquisition dates.

Update when MTEK files new deeds in ACRIS.
"""

from datetime import date

# bbl → {address, entity, zip_code, acquired}
# acquired filters alerts to post-acquisition events only.
MTEK_PORTFOLIO: dict[str, dict] = {
    "3040400020": {"address": "870 BELMONT AVENUE",   "entity": "MTEK NYC LLC",            "zip_code": "11208", "acquired": date(2025,  9,  5)},
    "3033960059": {"address": "131 WEIRFIELD STREET", "entity": "MTEK FRANKLIN LLC",        "zip_code": "11221", "acquired": date(2025,  9, 26)},
    "3033610038": {"address": "1277 MADISON STREET",  "entity": "MTEK GOLD LLC",            "zip_code": "11221", "acquired": date(2025, 12, 18)},
    "3018360075": {"address": "397A HANCOCK STREET",  "entity": "MTEK CAPITAL LLC",         "zip_code": "11216", "acquired": date(2026,  1, 29)},
    "3018510012": {"address": "134 MACON STREET",     "entity": "MTEK FRANKLIN LLC",        "zip_code": "11216", "acquired": date(2025,  9, 25)},
    "3021120045": {"address": "118 FT GREENE PLACE",  "entity": "MTEK FORT GREENE LLC",     "zip_code": "11217", "acquired": date(2026,  3, 16)},
    "3033210026": {"address": "1006 BUSHWICK AVENUE", "entity": "MTEK PARK LLC",            "zip_code": "11221", "acquired": date(2025, 11,  5)},
    "3034510027": {"address": "724 CHAUNCEY STREET",  "entity": "MTEK PARK LLC",            "zip_code": "11207", "acquired": date(2025,  6, 18)},
    "3039250019": {"address": "117 CLEVELAND STREET", "entity": "MTEK NYC LLC",             "zip_code": "11208", "acquired": date(2026,  1, 27)},
    "3012200017": {"address": None, "entity": "MTEK GOLD LLC",            "zip_code": None, "acquired": date(2026,  1, 29)},
    "3012480034": {"address": None, "entity": "MTEK GOLD LLC",            "zip_code": None, "acquired": date(2025, 11, 25)},
    "3012550070": {"address": None, "entity": "MTEK FRANKLIN LLC",        "zip_code": None, "acquired": date(2025,  9,  5)},
    "3013560069": {"address": None, "entity": "MTEK UNION LLC",           "zip_code": None, "acquired": date(2025, 10, 15)},
    "3016520018": {"address": None, "entity": "MTEK GOLD LLC",            "zip_code": None, "acquired": date(2025,  9, 18)},
    "3016590018": {"address": None, "entity": "MTEK PARK LLC",            "zip_code": None, "acquired": date(2025,  7, 10)},
    "3016610017": {"address": None, "entity": "MTEK GOLD LLC",            "zip_code": None, "acquired": date(2025, 11,  6)},
    "3016730065": {"address": None, "entity": "MTEK FRANKLIN LLC",        "zip_code": None, "acquired": date(2025,  8,  6)},
    "3018020104": {"address": None, "entity": "MTEK FRANKLIN LLC",        "zip_code": None, "acquired": date(2025,  6,  9)},
    "3018400029": {"address": None, "entity": "MTEK GOLD LLC",            "zip_code": None, "acquired": date(2025, 10,  6)},
    "3031730012": {"address": None, "entity": "MTEK 174 JEFFERSON LLC",   "zip_code": None, "acquired": date(2025,  9,  3)},
    "3033610043": {"address": None, "entity": "MTEK GOLD LLC",            "zip_code": None, "acquired": date(2025, 10, 22)},
    "3033890147": {"address": None, "entity": "MTEK PARK LLC",            "zip_code": None, "acquired": date(2025,  6,  5)},
    "3040300114": {"address": None, "entity": "MTEK PARK LLC",            "zip_code": None, "acquired": date(2025,  6, 18)},
    "3043110028": {"address": None, "entity": "MTEK PARK LLC",            "zip_code": None, "acquired": date(2025,  4, 29)},
    "3043300056": {"address": None, "entity": "MTEK GOLD LLC",            "zip_code": None, "acquired": date(2025, 11, 10)},
    "3044540004": {"address": None, "entity": "MTEK NYC LLC",             "zip_code": None, "acquired": date(2025,  8, 15)},
    "3047260021": {"address": None, "entity": "MTEK NYC LLC",             "zip_code": None, "acquired": date(2025,  8,  8)},
    "3048940042": {"address": None, "entity": "MTEK NYC LLC",             "zip_code": None, "acquired": date(2025,  7, 24)},
    "3052850035": {"address": None, "entity": "MTEK PARK LLC",            "zip_code": None, "acquired": date(2025,  7, 10)},
    "3077890041": {"address": None, "entity": "MTEK NYC LLC",             "zip_code": None, "acquired": date(2025,  8, 15)},
    "3078840023": {"address": None, "entity": "MTEK NYC LLC",             "zip_code": None, "acquired": date(2025,  6, 12)},
    "3082740032": {"address": None, "entity": "MTEK GOLD LLC",            "zip_code": None, "acquired": date(2026,  1,  6)},
    "3083270020": {"address": None, "entity": "MTEK FULTON LLC",          "zip_code": None, "acquired": date(2025,  5, 14)},
    "4027670028": {"address": None, "entity": "MTEK FULTON LLC",          "zip_code": None, "acquired": date(2025,  5, 13)},
    "4090620007": {"address": None, "entity": "MTEK FULTON LLC",          "zip_code": None, "acquired": date(2025,  5,  6)},
    "4111130040": {"address": None, "entity": "MTEK NYC LLC",             "zip_code": None, "acquired": date(2025, 11, 19)},
    "4123300034": {"address": None, "entity": "MTEK GOLD LLC",            "zip_code": None, "acquired": date(2026,  2,  5)},
}

MTEK_BBLS: frozenset[str] = frozenset(MTEK_PORTFOLIO.keys())

MTEK_EARLIEST_ACQUISITION: date = min(p["acquired"] for p in MTEK_PORTFOLIO.values())
