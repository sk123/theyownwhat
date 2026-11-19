"""
fl/fl_county_config.py
======================
Authoritative 67-County Florida Property Appraiser & CAMA Portal Registry.

Maps FDOR County Numbers (`11..77`, corresponding to `C11..C77` parcel prefixes in
`fl_properties`) to county names, official County Property Appraiser web portals,
CAMA platform types, and `data_source_status` keys (`FL_COUNTY_<COUNTY>`).
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

FL_COUNTY_CONFIG: Dict[int, Dict[str, Any]] = {
    11: {"county": "ALACHUA", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=1081", "frequency": "Weekly"},
    12: {"county": "BAKER", "platform": "county_pa", "portal_url": "https://www.bakerpa.com/", "frequency": "Semi-Annual FDOR + PA"},
    13: {"county": "BAY", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=856", "frequency": "Weekly"},
    14: {"county": "BRADFORD", "platform": "county_pa", "portal_url": "https://www.bradfordappraiser.com/", "frequency": "Semi-Annual FDOR + PA"},
    15: {"county": "BREVARD", "platform": "bcpao", "portal_url": "https://www.bcpao.us/PropertySearch/", "frequency": "Weekly"},
    16: {"county": "BROWARD", "platform": "bcpa", "portal_url": "https://web.bcpa.net/BcpaClient/#/Record-Search", "frequency": "Weekly"},
    17: {"county": "CALHOUN", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=825", "frequency": "Semi-Annual FDOR + PA"},
    18: {"county": "CHARLOTTE", "platform": "county_pa", "portal_url": "https://www.ccappraiser.com/", "frequency": "Semi-Annual FDOR + PA"},
    19: {"county": "CITRUS", "platform": "county_pa", "portal_url": "https://www.citruspa.org/", "frequency": "Semi-Annual FDOR + PA"},
    20: {"county": "CLAY", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=830", "frequency": "Weekly"},
    21: {"county": "COLLIER", "platform": "county_pa", "portal_url": "https://www.collierappraiser.com/", "frequency": "Weekly"},
    22: {"county": "COLUMBIA", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=857", "frequency": "Semi-Annual FDOR + PA"},
    23: {"county": "MIAMI-DADE", "platform": "miami_dade_pa", "portal_url": "https://www.miamidade.gov/Apps/PA/propertysearch/", "frequency": "Nightly"},
    24: {"county": "DESOTO", "platform": "county_pa", "portal_url": "https://www.desotopa.com/", "frequency": "Semi-Annual FDOR + PA"},
    25: {"county": "DIXIE", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=868", "frequency": "Semi-Annual FDOR + PA"},
    26: {"county": "DUVAL", "platform": "coj_pao", "portal_url": "https://paopropertysearch.coj.net/Basic/Search.aspx", "frequency": "Weekly"},
    27: {"county": "ESCAMBIA", "platform": "county_pa", "portal_url": "https://www.escpa.org/", "frequency": "Weekly"},
    28: {"county": "FLAGLER", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=598", "frequency": "Weekly"},
    29: {"county": "FRANKLIN", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=816", "frequency": "Semi-Annual FDOR + PA"},
    30: {"county": "GADSDEN", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=811", "frequency": "Semi-Annual FDOR + PA"},
    31: {"county": "GILCHRIST", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=824", "frequency": "Semi-Annual FDOR + PA"},
    32: {"county": "GLADES", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=818", "frequency": "Semi-Annual FDOR + PA"},
    33: {"county": "GULF", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=819", "frequency": "Semi-Annual FDOR + PA"},
    34: {"county": "HAMILTON", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=834", "frequency": "Semi-Annual FDOR + PA"},
    35: {"county": "HARDEE", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=838", "frequency": "Semi-Annual FDOR + PA"},
    36: {"county": "HENDRY", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=888", "frequency": "Semi-Annual FDOR + PA"},
    37: {"county": "HERNANDO", "platform": "county_pa", "portal_url": "https://www.hernandopa-fl.us/", "frequency": "Semi-Annual FDOR + PA"},
    38: {"county": "HIGHLANDS", "platform": "county_pa", "portal_url": "https://www.hcpao.org/", "frequency": "Semi-Annual FDOR + PA"},
    39: {"county": "HILLSBOROUGH", "platform": "hcpa_arcgis", "portal_url": "https://gis.hcpafl.org/propertysearch/", "frequency": "Nightly"},
    40: {"county": "HOLMES", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=820", "frequency": "Semi-Annual FDOR + PA"},
    41: {"county": "INDIAN RIVER", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=1100", "frequency": "Weekly"},
    42: {"county": "JACKSON", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=845", "frequency": "Semi-Annual FDOR + PA"},
    43: {"county": "JEFFERSON", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=823", "frequency": "Semi-Annual FDOR + PA"},
    44: {"county": "LAFAYETTE", "platform": "county_pa", "portal_url": "https://www.lafayettepa.com/", "frequency": "Semi-Annual FDOR + PA"},
    45: {"county": "LAKE", "platform": "county_pa", "portal_url": "https://www.lakecopropappr.com/", "frequency": "Weekly"},
    46: {"county": "LEE", "platform": "leepa", "portal_url": "https://www.leepa.org/Search/PropertySearch.aspx", "frequency": "Weekly"},
    47: {"county": "LEON", "platform": "county_pa", "portal_url": "https://www.leonpa.org/", "frequency": "Weekly"},
    48: {"county": "LEVY", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=903", "frequency": "Semi-Annual FDOR + PA"},
    49: {"county": "LIBERTY", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=839", "frequency": "Semi-Annual FDOR + PA"},
    50: {"county": "MADISON", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=908", "frequency": "Semi-Annual FDOR + PA"},
    51: {"county": "MANATEE", "platform": "county_pa", "portal_url": "https://www.manateepao.gov/search/", "frequency": "Weekly"},
    52: {"county": "MARION", "platform": "county_pa", "portal_url": "https://www.pa.marion.fl.us/PropertySearch.aspx", "frequency": "Weekly"},
    53: {"county": "MARTIN", "platform": "county_pa", "portal_url": "https://www.pa.martin.fl.us/", "frequency": "Weekly"},
    54: {"county": "MONROE", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=605", "frequency": "Weekly"},
    55: {"county": "NASSAU", "platform": "county_pa", "portal_url": "https://www.nassauflpa.com/", "frequency": "Semi-Annual FDOR + PA"},
    56: {"county": "OKALOOSA", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=853", "frequency": "Weekly"},
    57: {"county": "OKEECHOBEE", "platform": "county_pa", "portal_url": "https://www.okeechobeepa.com/", "frequency": "Semi-Annual FDOR + PA"},
    58: {"county": "ORANGE", "platform": "ocpa", "portal_url": "https://ocpaweb.ocpafl.org/parcelsearch", "frequency": "Weekly"},
    59: {"county": "OSCEOLA", "platform": "county_pa", "portal_url": "https://search.property-appraiser.org/", "frequency": "Weekly"},
    60: {"county": "PALM BEACH", "platform": "pbcpao", "portal_url": "https://pbcpao.gov/Property/Search", "frequency": "Weekly"},
    61: {"county": "PASCO", "platform": "county_pa", "portal_url": "https://search.pascopa.com/", "frequency": "Weekly"},
    62: {"county": "PINELLAS", "platform": "pcpao", "portal_url": "https://www.pcpao.gov/quick-search", "frequency": "Weekly"},
    63: {"county": "POLK", "platform": "polkpa", "portal_url": "https://www.polkpa.org/CamaDisplay.aspx", "frequency": "Weekly"},
    64: {"county": "PUTNAM", "platform": "county_pa", "portal_url": "https://pa.putnam-fl.com/", "frequency": "Semi-Annual FDOR + PA"},
    65: {"county": "ST. JOHNS", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=960", "frequency": "Weekly"},
    66: {"county": "ST. LUCIE", "platform": "county_pa", "portal_url": "https://www.paslc.gov/", "frequency": "Weekly"},
    67: {"county": "SANTA ROSA", "platform": "county_pa", "portal_url": "https://www.srcpa.org/", "frequency": "Semi-Annual FDOR + PA"},
    68: {"county": "SARASOTA", "platform": "county_pa", "portal_url": "https://www.sc-pa.com/propertysearch", "frequency": "Weekly"},
    69: {"county": "SEMINOLE", "platform": "county_pa", "portal_url": "https://www.scpafl.org/", "frequency": "Weekly"},
    70: {"county": "SUMTER", "platform": "county_pa", "portal_url": "https://www.sumterpa.com/", "frequency": "Semi-Annual FDOR + PA"},
    71: {"county": "SUWANNEE", "platform": "county_pa", "portal_url": "https://www.suwanneepa.com/", "frequency": "Semi-Annual FDOR + PA"},
    72: {"county": "TAYLOR", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=792", "frequency": "Semi-Annual FDOR + PA"},
    73: {"county": "UNION", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=826", "frequency": "Semi-Annual FDOR + PA"},
    74: {"county": "VOLUSIA", "platform": "county_pa", "portal_url": "https://vcpa.vcgov.org/search/real-property", "frequency": "Weekly"},
    75: {"county": "WAKULLA", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=833", "frequency": "Semi-Annual FDOR + PA"},
    76: {"county": "WALTON", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=836", "frequency": "Weekly"},
    77: {"county": "WASHINGTON", "platform": "qpublic", "portal_url": "https://qpublic.schneidercorp.com/Application.aspx?AppID=821", "frequency": "Semi-Annual FDOR + PA"},
}

FL_COUNTY_BY_NAME: Dict[str, Dict[str, Any]] = {
    cfg["county"]: {"co_no": co_no, **cfg}
    for co_no, cfg in FL_COUNTY_CONFIG.items()
}


def fl_county_status_key(county_name: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]+", "_", (county_name or "").strip().upper()).strip("_")
    return f"FL_COUNTY_{cleaned}"


def fl_co_no_from_bbl(bbl: Optional[str]) -> Optional[int]:
    if not bbl:
        return None
    m = re.match(r"^(?:C|FL)(\d{2})\b", bbl.strip().upper())
    if m:
        co = int(m.group(1))
        if co in FL_COUNTY_CONFIG:
            return co
    return None
