"""
ca/ca_county_config.py
======================
Authoritative 58-County California Assessor & Statutory Redaction Registry.

Explicitly surfaces California Government Code § 7928.205 (AB 1785), under which
county assessors withhold unrestricted online bulk publication of `Owner Name + APN`,
distinguishing:
  - `track_a_lessor_and_roll`: Live Open-Data parcel inventory enriched with
    municipal business-tax lessor registrations (`SF`, `LA`), code enforcement,
    eviction notices, NHPD subsidies, and `us_ca` LLC member/manager records.
  - `cpra_secured_roll_ready`: Ready for drop-in County Assessor CPRA Secured
    Assessment Roll import (`--import-cpra-roll`).
"""

from __future__ import annotations

import re
from typing import Any, Dict

ALL_58_CA_COUNTIES = [
    "ALAMEDA", "ALPINE", "AMADOR", "BUTTE", "CALAVERAS", "COLUSA", "CONTRA COSTA",
    "DEL NORTE", "EL DORADO", "FRESNO", "GLENN", "HUMBOLDT", "IMPERIAL", "INYO",
    "KERN", "KINGS", "LAKE", "LASSEN", "LOS ANGELES", "MADERA", "MARIN", "MARIPOSA",
    "MENDOCINO", "MERCED", "MODOC", "MONO", "MONTEREY", "NAPA", "NEVADA", "ORANGE",
    "PLACER", "PLUMAS", "RIVERSIDE", "SACRAMENTO", "SAN BENITO", "SAN BERNARDINO",
    "SAN DIEGO", "SAN FRANCISCO", "SAN JOAQUIN", "SAN LUIS OBISPO", "SAN MATEO",
    "SANTA BARBARA", "SANTA CLARA", "SANTA CRUZ", "SHASTA", "SIERRA", "SISKIYOU",
    "SOLANO", "SONOMA", "STANISLAUS", "SUTTER", "TEHAMA", "TRINITY", "TULARE",
    "TUOLUMNE", "VENTURA", "YOLO", "YUBA",
]

CA_METRO_ASSESSOR_PORTALS: Dict[str, Dict[str, Any]] = {
    "LOS ANGELES": {
        "platform": "track_a_lessor_and_roll",
        "portal_url": "https://portal.assessor.lacounty.gov/",
        "frequency": "Nightly LA Biz/Code/Evict + Annual Roll",
    },
    "SAN FRANCISCO": {
        "platform": "track_a_lessor_and_roll",
        "portal_url": "https://sfassessor.org/",
        "frequency": "Nightly SF Biz/DBI/Evict + Annual Roll",
    },
    "ALAMEDA": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://www.acassessor.org/",
        "frequency": "CPRA Secured Roll ($20 CD) + Oakland Code",
    },
    "SAN DIEGO": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://www.sdarcc.gov/",
        "frequency": "CPRA Secured Roll + SanGIS",
    },
    "ORANGE": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://www.ocassessor.gov/",
        "frequency": "CPRA Secured Roll",
    },
    "SANTA CLARA": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://www.sccassessor.org/",
        "frequency": "CPRA Secured Roll",
    },
    "SACRAMENTO": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://assessor.saccounty.gov/",
        "frequency": "CPRA Secured Roll",
    },
    "RIVERSIDE": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://www.asrclkrec.com/",
        "frequency": "CPRA Secured Roll",
    },
    "SAN BERNARDINO": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://arc.sbcounty.gov/",
        "frequency": "CPRA Secured Roll",
    },
    "CONTRA COSTA": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://www.contracosta.ca.gov/191/Assessor",
        "frequency": "CPRA Secured Roll",
    },
    "FRESNO": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://www.fresnocountyca.gov/Departments/Assessor",
        "frequency": "CPRA Secured Roll",
    },
    "SAN MATEO": {
        "platform": "cpra_secured_roll_ready",
        "portal_url": "https://www.smcacre.gov/assessor",
        "frequency": "CPRA Secured Roll",
    },
}

CA_COUNTY_CONFIG: Dict[str, Dict[str, Any]] = {}
for _c in ALL_58_CA_COUNTIES:
    if _c in CA_METRO_ASSESSOR_PORTALS:
        CA_COUNTY_CONFIG[_c] = {"county": _c, "display": f"{_c} COUNTY", **CA_METRO_ASSESSOR_PORTALS[_c]}
    else:
        CA_COUNTY_CONFIG[_c] = {
            "county": _c,
            "display": f"{_c} COUNTY",
            "platform": "cpra_secured_roll_ready",
            "portal_url": "https://www.boe.ca.gov/proptaxes/countycontacts.htm",
            "frequency": "CPRA Secured Roll (§ 408.3)",
        }


def ca_county_status_key(county_name: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]+", "_", (county_name or "").strip().upper()).strip("_")
    return f"CA_COUNTY_{cleaned}"
