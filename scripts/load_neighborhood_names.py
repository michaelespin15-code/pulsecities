"""
One-time script to populate the neighborhoods.name column with human-readable
neighborhood names for all NYC MODZCTA zip codes.

Names are derived from the NYC Department of Health MODZCTA definitions and
USPS preferred city names. Outer-borough zips use the USPS city name (which is
specific: Bellerose, Flushing, Astoria, etc.). Manhattan zips use curated
sub-neighborhood names since USPS always returns "New York" for all of them.

Run once: python -m scripts.load_neighborhood_names
"""

import logging
import sys

from models.database import SessionLocal
from models.neighborhoods import Neighborhood

logger = logging.getLogger(__name__)

# fmt: off
ZIP_NAMES: dict[str, str] = {
    # Manhattan
    "10001": "Chelsea",
    "10002": "Chinatown",
    "10003": "East Village",
    "10004": "Financial District",
    "10005": "Financial District",
    "10006": "Financial District",
    "10007": "Tribeca",
    "10009": "East Village",
    "10010": "Gramercy",
    "10011": "Chelsea",
    "10012": "SoHo",
    "10013": "SoHo",
    "10014": "West Village",
    "10016": "Murray Hill",
    "10017": "Midtown East",
    "10018": "Garment District",
    "10019": "Hell's Kitchen",
    "10021": "Upper East Side",
    "10022": "Midtown East",
    "10023": "Upper West Side",
    "10024": "Upper West Side",
    "10025": "Morningside Heights",
    "10026": "Harlem",
    "10027": "Harlem",
    "10028": "Upper East Side",
    "10029": "East Harlem",
    "10030": "Harlem",
    "10031": "Hamilton Heights",
    "10032": "Washington Heights",
    "10033": "Washington Heights",
    "10034": "Inwood",
    "10035": "East Harlem",
    "10036": "Hell's Kitchen",
    "10037": "Harlem",
    "10038": "Two Bridges",
    "10039": "Harlem",
    "10040": "Inwood",
    "10044": "Roosevelt Island",
    "10065": "Upper East Side",
    "10069": "Upper West Side",
    "10075": "Upper East Side",
    "10115": "Morningside Heights",
    "10128": "Upper East Side",
    "10280": "Battery Park City",
    "10282": "Battery Park City",
    # Bronx
    "10451": "Mott Haven",
    "10452": "Highbridge",
    "10453": "Morris Heights",
    "10454": "Port Morris",
    "10455": "Longwood",
    "10456": "Morrisania",
    "10457": "East Tremont",
    "10458": "Belmont",
    "10459": "Longwood",
    "10460": "West Farms",
    "10461": "Morris Park",
    "10462": "Parkchester",
    "10463": "Kingsbridge",
    "10464": "City Island",
    "10465": "Throgs Neck",
    "10466": "Wakefield",
    "10467": "Norwood",
    "10468": "University Heights",
    "10469": "Pelham Bay",
    "10470": "Wakefield",
    "10471": "Riverdale",
    "10472": "Soundview",
    "10473": "Clason Point",
    "10474": "Hunts Point",
    "10475": "Co-op City",
    # Brooklyn
    "11201": "Brooklyn Heights",
    "11203": "East Flatbush",
    "11204": "Borough Park",
    "11205": "Clinton Hill",
    "11206": "Bushwick",
    "11207": "East New York",
    "11208": "Cypress Hills",
    "11209": "Bay Ridge",
    "11210": "Flatbush",
    "11211": "Williamsburg",
    "11212": "Brownsville",
    "11213": "Crown Heights",
    "11214": "Bensonhurst",
    "11215": "Park Slope",
    "11216": "Bedford-Stuyvesant",
    "11217": "Boerum Hill",
    "11218": "Kensington",
    "11219": "Borough Park",
    "11220": "Sunset Park",
    "11221": "Bushwick",
    "11222": "Greenpoint",
    "11223": "Gravesend",
    "11224": "Coney Island",
    "11225": "Crown Heights",
    "11226": "Flatbush",
    "11228": "Dyker Heights",
    "11229": "Marine Park",
    "11230": "Midwood",
    "11231": "Red Hook",
    "11232": "Sunset Park",
    "11233": "Brownsville",
    "11234": "Flatlands",
    "11235": "Sheepshead Bay",
    "11236": "Canarsie",
    "11237": "Bushwick",
    "11238": "Prospect Heights",
    "11239": "East New York",
    # Queens
    "11001": "Floral Park",
    "11004": "Glen Oaks",
    "11005": "Floral Park",
    "11040": "New Hyde Park",
    "11101": "Long Island City",
    "11102": "Astoria",
    "11103": "Astoria",
    "11104": "Sunnyside",
    "11105": "Astoria",
    "11106": "Astoria",
    "11354": "Flushing",
    "11355": "Flushing",
    "11356": "College Point",
    "11357": "Whitestone",
    "11358": "Fresh Meadows",
    "11359": "Bayside",
    "11360": "Bayside",
    "11361": "Douglaston",
    "11362": "Little Neck",
    "11363": "Douglas Manor",
    "11364": "Oakland Gardens",
    "11365": "Fresh Meadows",
    "11366": "Fresh Meadows",
    "11367": "Kew Gardens Hills",
    "11368": "Corona",
    "11369": "East Elmhurst",
    "11370": "East Elmhurst",
    "11371": "Jackson Heights",
    "11372": "Jackson Heights",
    "11373": "Elmhurst",
    "11374": "Rego Park",
    "11375": "Forest Hills",
    "11377": "Woodside",
    "11378": "Maspeth",
    "11379": "Middle Village",
    "11385": "Ridgewood",
    "11411": "Cambria Heights",
    "11412": "St. Albans",
    "11413": "Springfield Gardens",
    "11414": "Howard Beach",
    "11415": "Kew Gardens",
    "11416": "Ozone Park",
    "11417": "Ozone Park",
    "11418": "Richmond Hill",
    "11419": "South Richmond Hill",
    "11420": "South Ozone Park",
    "11421": "Woodhaven",
    "11422": "Rosedale",
    "11423": "Hollis",
    "11426": "Bellerose",
    "11427": "Queens Village",
    "11428": "Queens Village",
    "11429": "Queens Village",
    "11432": "Jamaica",
    "11433": "Jamaica",
    "11434": "Jamaica",
    "11435": "Jamaica",
    "11436": "South Jamaica",
    "11691": "Far Rockaway",
    "11692": "Arverne",
    "11693": "Rockaway Beach",
    "11694": "Belle Harbor",
    "11697": "Breezy Point",
    # Staten Island
    "10301": "St. George",
    "10302": "Port Richmond",
    "10303": "Mariners Harbor",
    "10304": "Stapleton",
    "10305": "Rosebank",
    "10306": "New Dorp",
    "10307": "Tottenville",
    "10308": "Great Kills",
    "10309": "Rossville",
    "10310": "West Brighton",
    "10311": "Willowbrook",
    "10312": "Eltingville",
    "10314": "Westerleigh",
}
# fmt: on


def main() -> int:
    db = SessionLocal()
    updated = 0
    skipped = 0
    try:
        for zip_code, name in ZIP_NAMES.items():
            hood = db.query(Neighborhood).filter(Neighborhood.zip_code == zip_code).first()
            if hood is None:
                skipped += 1
                continue
            hood.name = name
            updated += 1
        db.commit()
    finally:
        db.close()

    print(f"Updated {updated} neighborhoods, skipped {skipped} (not in DB)")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
