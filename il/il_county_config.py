"""
il/il_county_config.py
======================
Authoritative 102-County Illinois Assessor & CAMA Portal Registry.

Maps all 102 Illinois counties to their live CAMA platform (`cook_socrata`,
`dupage_gis`, `devnet_wedge`, `beacon_schneider`, `county_assessor`), official
portal URL, refresh cadence, and `data_source_status` key (`IL_COUNTY_<COUNTY>`).
"""

from __future__ import annotations

import re
from typing import Any, Dict

ALL_102_IL_COUNTIES = [
    "ADAMS", "ALEXANDER", "BOND", "BOONE", "BROWN", "BUREAU", "CALHOUN", "CARROLL",
    "CASS", "CHAMPAIGN", "CHRISTIAN", "CLARK", "CLAY", "CLINTON", "COLES", "COOK",
    "CRAWFORD", "CUMBERLAND", "DEKALB", "DEWITT", "DOUGLAS", "DUPAGE", "EDGAR",
    "EDWARDS", "EFFINGHAM", "FAYETTE", "FORD", "FRANKLIN", "FULTON", "GALLATIN",
    "GREENE", "GRUNDY", "HAMILTON", "HANCOCK", "HARDIN", "HENDERSON", "HENRY",
    "IROQUOIS", "JACKSON", "JASPER", "JEFFERSON", "JERSEY", "JO DAVIESS", "JOHNSON",
    "KANE", "KANKAKEE", "KENDALL", "KNOX", "LAKE", "LASALLE", "LAWRENCE", "LEE",
    "LIVINGSTON", "LOGAN", "MACON", "MACOUPIN", "MADISON", "MARION", "MARSHALL",
    "MASON", "MASSAC", "MCDONOUGH", "MCHENRY", "MCLEAN", "MENARD", "MERCER",
    "MONROE", "MONTGOMERY", "MORGAN", "MOULTRIE", "OGLE", "PEORIA", "PERRY",
    "PIATT", "PIKE", "POPE", "PULASKI", "PUTNAM", "RANDOLPH", "RICHLAND",
    "ROCK ISLAND", "SALINE", "SANGAMON", "SCHUYLER", "SCOTT", "SHELBY", "ST. CLAIR",
    "STARK", "STEPHENSON", "TAZEWELL", "UNION", "VERMILION", "WABASH", "WARREN",
    "WASHINGTON", "WAYNE", "WHITE", "WHITESIDE", "WILL", "WILLIAMSON", "WINNEBAGO",
    "WOODFORD",
]

IL_DEVNET_WEDGE_URLS: Dict[str, str] = {
    "KANE": "https://kaneil.devnetwedge.com/",
    "MCHENRY": "https://mchenryil.devnetwedge.com/",
    "WILL": "https://willil.devnetwedge.com/",
    "ST. CLAIR": "https://stclairil.devnetwedge.com/",
    "MADISON": "https://madisonil.devnetwedge.com/",
    "CHAMPAIGN": "https://champaignil.devnetwedge.com/",
    "PEORIA": "https://peoriail.devnetwedge.com/",
    "ROCK ISLAND": "https://rockislandil.devnetwedge.com/",
    "TAZEWELL": "https://tazewellil.devnetwedge.com/",
    "MACON": "https://maconil.devnetwedge.com/",
    "KANKAKEE": "https://kankakeeil.devnetwedge.com/",
    "DEKALB": "https://dekalbil.devnetwedge.com/",
    "KENDALL": "https://kendallil.devnetwedge.com/",
    "LASALLE": "https://lasalleil.devnetwedge.com/",
    "GRUNDY": "https://grundyil.devnetwedge.com/",
    "BOONE": "https://booneil.devnetwedge.com/",
    "OGLE": "https://ogleil.devnetwedge.com/",
    "WHITESIDE": "https://whitesideil.devnetwedge.com/",
    "STEPHENSON": "https://stephensonil.devnetwedge.com/",
    "ADAMS": "https://adamsil.devnetwedge.com/",
    "COLES": "https://colesil.devnetwedge.com/",
    "JACKSON": "https://jacksonil.devnetwedge.com/",
    "WILLIAMSON": "https://williamsonil.devnetwedge.com/",
    "EFFINGHAM": "https://effinghamil.devnetwedge.com/",
}

IL_BEACON_URLS: Dict[str, str] = {
    "MCLEAN": "https://beacon.schneidercorp.com/?site=McLeanCountyIL",
    "VERMILION": "https://beacon.schneidercorp.com/?site=VermilionCountyIL",
    "LIVINGSTON": "https://beacon.schneidercorp.com/?site=LivingstonCountyIL",
    "IROQUOIS": "https://beacon.schneidercorp.com/?site=IroquoisCountyIL",
    "LOGAN": "https://beacon.schneidercorp.com/?site=LoganCountyIL",
    "HENRY": "https://beacon.schneidercorp.com/?site=HenryCountyIL",
    "KNOX": "https://beacon.schneidercorp.com/?site=KnoxCountyIL",
    "BUREAU": "https://beacon.schneidercorp.com/?site=BureauCountyIL",
    "LEE": "https://beacon.schneidercorp.com/?site=LeeCountyIL",
    "MORGAN": "https://beacon.schneidercorp.com/?site=MorganCountyIL",
}

IL_SPECIAL_PORTALS: Dict[str, Dict[str, Any]] = {
    "COOK": {
        "platform": "cook_socrata",
        "portal_url": "https://www.cookcountyassessor.com/",
        "frequency": "Nightly Delta + Annual Roll",
    },
    "DUPAGE": {
        "platform": "dupage_gis",
        "portal_url": "https://www.dupagecounty.gov/PropertyInfo/PropertyLookUp.aspx",
        "frequency": "Weekly GIS + CAMA",
    },
    "LAKE": {
        "platform": "lake_assessor",
        "portal_url": "https://www.lakecountyil.gov/160/Chief-County-Assessment-Office",
        "frequency": "Nightly CAMA Cohort",
    },
    "WINNEBAGO": {
        "platform": "wingis_cama",
        "portal_url": "https://supervisorofassessments.wincoil.gov/",
        "frequency": "Nightly CAMA Cohort",
    },
    "SANGAMON": {
        "platform": "sangamon_cama",
        "portal_url": "https://sangamonil.gov/departments/s-z/supervisor-of-assessments",
        "frequency": "Nightly CAMA Cohort",
    },
}

IL_COUNTY_CONFIG: Dict[str, Dict[str, Any]] = {}
for _c in ALL_102_IL_COUNTIES:
    if _c in IL_SPECIAL_PORTALS:
        IL_COUNTY_CONFIG[_c] = {"county": _c, "display": f"{_c} COUNTY", **IL_SPECIAL_PORTALS[_c]}
    elif _c in IL_DEVNET_WEDGE_URLS:
        IL_COUNTY_CONFIG[_c] = {
            "county": _c,
            "display": f"{_c} COUNTY",
            "platform": "devnet_wedge",
            "portal_url": IL_DEVNET_WEDGE_URLS[_c],
            "frequency": "Nightly CAMA Cohort",
        }
    elif _c in IL_BEACON_URLS:
        IL_COUNTY_CONFIG[_c] = {
            "county": _c,
            "display": f"{_c} COUNTY",
            "platform": "beacon_schneider",
            "portal_url": IL_BEACON_URLS[_c],
            "frequency": "Nightly CAMA Cohort",
        }
    else:
        IL_COUNTY_CONFIG[_c] = {
            "county": _c,
            "display": f"{_c} COUNTY",
            "platform": "county_assessor",
            "portal_url": "https://tax.illinois.gov/localgovernments/property.html",
            "frequency": "County CAMA Queue",
        }


def il_county_status_key(county_name: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]+", "_", (county_name or "").strip().upper()).strip("_")
    return f"IL_COUNTY_{cleaned}"
