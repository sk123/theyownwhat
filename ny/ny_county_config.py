"""
ny/ny_county_config.py
======================
Authoritative 62-County New York Property Assessment & CAMA Registry.

Covers all 62 New York counties:
  - 5 NYC Borough Counties (`NEW YORK`, `KINGS`, `QUEENS`, `BRONX`, `RICHMOND`)
    backed by NYC DOF PLUTO + HPD Registrations/Contacts/Violations + Marshal Evictions.
  - 33 NYS ITS / ORPTS Public Tax Parcels counties (`NYS_Tax_Parcels_Public`).
  - Monroe County / Rochester (`MONROE_COUNTY_PARCELS_2025` Open GIS + Real Property Portal).
  - SDG `ImageMate Online` (`imo.*`) CAMA portals & County Real Property portals across
    the remaining non-NYC counties, with honest status reporting (`NY_COUNTY_<COUNTY>`).
"""

from __future__ import annotations

import re
from typing import Any, Dict

# 5 NYC Borough Counties
NYC_BOROUGH_COUNTIES: Dict[str, Dict[str, Any]] = {
    "NEW YORK": {"display": "MANHATTAN (NEW YORK COUNTY)", "platform": "nyc_hpd_pluto", "portal_url": "https://hpdonline.nyc.gov/hpdonline/", "frequency": "Nightly"},
    "KINGS": {"display": "BROOKLYN (KINGS COUNTY)", "platform": "nyc_hpd_pluto", "portal_url": "https://hpdonline.nyc.gov/hpdonline/", "frequency": "Nightly"},
    "QUEENS": {"display": "QUEENS (QUEENS COUNTY)", "platform": "nyc_hpd_pluto", "portal_url": "https://hpdonline.nyc.gov/hpdonline/", "frequency": "Nightly"},
    "BRONX": {"display": "BRONX (BRONX COUNTY)", "platform": "nyc_hpd_pluto", "portal_url": "https://hpdonline.nyc.gov/hpdonline/", "frequency": "Nightly"},
    "RICHMOND": {"display": "STATEN ISLAND (RICHMOND COUNTY)", "platform": "nyc_hpd_pluto", "portal_url": "https://hpdonline.nyc.gov/hpdonline/", "frequency": "Nightly"},
}

# Verified SDG ImageMate Online (`imo.*`) County Portals in New York State
NY_SDG_IMAGEMATE_URLS: Dict[str, str] = {
    "NIAGARA": "https://niagara.sdgnys.com/",
    "CHEMUNG": "https://imo.chemungcountyny.gov/",
    "JEFFERSON": "https://imo.co.jefferson.ny.us/",
    "CLINTON": "https://imo.clintoncountygov.com/",
    "COLUMBIA": "https://columbia.sdgnys.com/",
    "MADISON": "https://property.madisoncounty.ny.gov/",
    "SCHENECTADY": "https://imo.schenectadycounty.com/",
    "SARATOGA": "https://saratoga.sdgnys.com/",
    "CATTARAUGUS": "https://imo.cattco.org/",
    "CHENANGO": "https://chenango.sdgnys.com/",
    "FULTON": "https://imo.fultoncountyny.gov/",
    "HERKIMER": "https://herkimer.sdgnys.com/",
    "ORLEANS": "https://orleans.sdgnys.com/",
    "SENECA": "https://imo.co.seneca.ny.us/",
    "YATES": "https://imo.yatescounty.org/",
    "CAYUGA": "https://imo.cayugacounty.us/",
    "CORTLAND": "https://imo.cortland-co.org/",
    "GENESEE": "https://imo.co.genesee.ny.us/",
    "GREENE": "https://imo.greenegovernment.com/",
    "LIVINGSTON": "https://imo.co.livingston.ny.us/",
    "MONTGOMERY": "https://imo.co.montgomery.ny.us/",
    "ONTARIO": "https://ontario.sdgnys.com/",
    "OSWEGO": "https://imo.oswegocounty.com/",
    "OTSEGO": "https://imo.otsegocounty.com/",
    "RENSSELAER": "https://imo.rensco.com/",
    "SCHOHARIE": "https://imo.schohariecounty-ny.gov/",
    "SCHUYLER": "https://schuyler.sdgnys.com/",
    "STEUBEN": "https://imo.steubencountyny.gov/",
    "SULLIVAN": "https://imo.sullivanny.us/",
    "TIOGA": "https://imo.tiogacountyny.gov/",
    "TOMPKINS": "https://imo.tompkinscountyny.gov/",
    "ULSTER": "https://imo.ulstercountyny.gov/",
    "WARREN": "https://imo.warrencountyny.gov/",
    "WASHINGTON": "https://imo.washingtoncountyny.gov/",
    "WAYNE": "https://imo.co.wayne.ny.us/",
    "WYOMING": "https://imo.wyomingco.net/",
}

ALL_62_NY_COUNTIES = [
    "ALBANY", "ALLEGANY", "BRONX", "BROOME", "CATTARAUGUS", "CAYUGA", "CHAUTAUQUA",
    "CHEMUNG", "CHENANGO", "CLINTON", "COLUMBIA", "CORTLAND", "DELAWARE", "DUTCHESS",
    "ERIE", "ESSEX", "FRANKLIN", "FULTON", "GENESEE", "GREENE", "HAMILTON", "HERKIMER",
    "JEFFERSON", "KINGS", "LEWIS", "LIVINGSTON", "MADISON", "MONROE", "MONTGOMERY",
    "NASSAU", "NEW YORK", "NIAGARA", "ONEIDA", "ONONDAGA", "ONTARIO", "ORANGE",
    "ORLEANS", "OSWEGO", "OTSEGO", "PUTNAM", "QUEENS", "RENSSELAER", "RICHMOND",
    "ROCKLAND", "SARATOGA", "SCHENECTADY", "SCHOHARIE", "SCHUYLER", "SENECA",
    "ST. LAWRENCE", "STEUBEN", "SUFFOLK", "SULLIVAN", "TIOGA", "TOMPKINS", "ULSTER",
    "WARREN", "WASHINGTON", "WAYNE", "WESTCHESTER", "WYOMING", "YATES",
]

SPECIAL_NY_PORTALS: Dict[str, Dict[str, Any]] = {
    "MONROE": {"platform": "monroe_gis", "portal_url": "https://www.monroecounty.gov/etc/rp/", "frequency": "Weekly"},
    "DUTCHESS": {"platform": "dutchess_parcelaccess", "portal_url": "https://parcelaccess.dutchessny.gov/", "frequency": "Weekly"},
    "NASSAU": {"platform": "nassau_lrv", "portal_url": "https://lrv.nassaucountyny.gov/", "frequency": "RPTL 1590 / County Roll"},
    "ERIE": {"platform": "nys_orpts_plus_buffalo", "portal_url": "https://data.buffalony.gov/", "frequency": "Weekly"},
    "ONONDAGA": {"platform": "nys_orpts_plus_syracuse", "portal_url": "https://data.syr.gov/", "frequency": "Weekly"},
    "WESTCHESTER": {"platform": "nys_orpts_gis", "portal_url": "https://giswww.westchestergov.com/", "frequency": "Annual ORPTS + Muni"},
    "SUFFOLK": {"platform": "nys_orpts_gis", "portal_url": "https://gis.suffolkcountyny.gov/", "frequency": "Annual ORPTS + Muni"},
    "ALBANY": {"platform": "nys_orpts_gis", "portal_url": "https://www.albanycountyny.gov/departments/management-and-budget/real-property-tax-service-agency", "frequency": "Annual ORPTS + Muni"},
}

NY_COUNTY_CONFIG: Dict[str, Dict[str, Any]] = {}
for _cnty in ALL_62_NY_COUNTIES:
    if _cnty in NYC_BOROUGH_COUNTIES:
        NY_COUNTY_CONFIG[_cnty] = {"county": _cnty, **NYC_BOROUGH_COUNTIES[_cnty]}
    elif _cnty in SPECIAL_NY_PORTALS:
        NY_COUNTY_CONFIG[_cnty] = {
            "county": _cnty,
            "display": f"{_cnty} COUNTY",
            **SPECIAL_NY_PORTALS[_cnty],
        }
    elif _cnty in NY_SDG_IMAGEMATE_URLS:
        NY_COUNTY_CONFIG[_cnty] = {
            "county": _cnty,
            "display": f"{_cnty} COUNTY",
            "platform": "sdg_imagemate",
            "portal_url": NY_SDG_IMAGEMATE_URLS[_cnty],
            "frequency": "Nightly CAMA Cohort",
        }
    else:
        NY_COUNTY_CONFIG[_cnty] = {
            "county": _cnty,
            "display": f"{_cnty} COUNTY",
            "platform": "nys_orpts_gis",
            "portal_url": "https://data.gis.ny.gov/datasets/sharegisny::nys-tax-parcels-public/about",
            "frequency": "Annual ORPTS",
        }


def ny_county_status_key(county_name: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]+", "_", (county_name or "").strip().upper()).strip("_")
    return f"NY_COUNTY_{cleaned}"
