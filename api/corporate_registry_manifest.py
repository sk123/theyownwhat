"""
api/corporate_registry_manifest.py
==================================
Canonical, source-verified corporate-registry freshness & static-snapshot
transparency manifest for all loaded `oc_companies` jurisdictions.

Governing principle (`docs/ASSURANCE_POLICY.md`):
  - Never imply a static historical dump is continuously updating.
  - Explicitly distinguish jurisdictions refreshed automatically from free
    official state open-data APIs/SFTP from jurisdictions whose corporate
    registry data is frozen at the `2026-11-17` OpenCorporates bulk snapshot
    once OpenCorporates SFTP access expires.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

DEFAULT_OC_SNAPSHOT_DATE = "2026-11-17"

# Refresh modes:
#   - "live_full":       Free official state bulk/API feed refreshes BOTH entities AND officers/principals.
#   - "live_partial":    Free official state bulk/API feed refreshes active entities & addresses (plus partial
#                        officer/true-name fields); historical/broader officer records remain pinned to OC snapshot.
#   - "snapshot_frozen": No free public bulk corporate API or unblocked bulk download exists (paid bulk unload,
#                        WAF/CAPTCHA-gated portal, or individual-lookup-only terms). Frozen at OC snapshot date.

CORPORATE_REGISTRY_MANIFEST: Dict[str, Dict[str, Any]] = {
    # --- LIVE FULL OFFICIAL PUBLIC REGISTRIES (Entities + Officers/Principals) ---
    "us_ct": {
        "state": "CT",
        "dataset_key": "CT",
        "label": "Connecticut Secretary of the State (SOTS)",
        "refresh_mode": "live_full",
        "entities_live": True,
        "officers_live": True,
        "live_source_name": "CT_SOTS_CORPORATIONS",
        "live_registry_name": "CT SOTS Open Data (Business Master + Principals)",
        "live_registry_url": "https://data.ct.gov/Business/Business-Master/n7gp-d28j",
        "cadence": "Nightly / Weekly automated Socrata sync",
        "frozen_reason": None,
        "coverage_note": "Entities and principals auto-refresh directly from official CT SOTS Socrata datasets (n7gp-d28j & ka36-64k6), merged with the 2026-11-17 OpenCorporates archive for cross-state links.",
    },
    "us_dc": {
        "state": "DC",
        "dataset_key": "DC",
        "label": "Washington, D.C. DLCP Corporate Registration & Beneficial Owners",
        "refresh_mode": "live_full",
        "entities_live": True,
        "officers_live": True,
        "live_source_name": "DC_DLCP_CORPORATIONS",
        "live_registry_name": "Open Data DC DLCP (Corporate Registration + Beneficial Owners + Trade Names)",
        "live_registry_url": "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Business_Licensing_and_Grants_WebMercator/FeatureServer",
        "cadence": "Weekly incremental ArcGIS REST sync (by last-modified timestamp)",
        "frozen_reason": None,
        "coverage_note": "Entities (Table 0), Trade Names (Table 1), and Beneficial Owners (Table 2) auto-refresh directly from official Open Data DC DLCP ArcGIS tables, merged non-destructively with the 2026-11-17 OpenCorporates archive.",
    },
    "us_fl": {
        "state": "FL",
        "dataset_key": "FL",
        "label": "Florida Division of Corporations (Sunbiz)",
        "refresh_mode": "live_full",
        "entities_live": True,
        "officers_live": True,
        "live_source_name": "FL_SUNBIZ_CORPORATIONS",
        "live_registry_name": "Florida Sunbiz Public SFTP Bulk Master (cordata.zip)",
        "live_registry_url": "https://dos.fl.gov/sunbiz/other-services/data-downloads/",
        "cadence": "Quarterly / watermark-triggered public SFTP master sync",
        "frozen_reason": None,
        "coverage_note": "Active FL corporations/LLCs and up to 6 officers/managers per filing auto-refresh from official Sunbiz public SFTP (cordata.zip), merged non-destructively with historical 2026-11-17 OpenCorporates filings.",
    },
    "us_pa": {
        "state": "PA",
        "dataset_key": "PHILADELPHIA",
        "label": "Pennsylvania Department of State",
        "refresh_mode": "live_full",
        "entities_live": True,
        "officers_live": True,
        "live_source_name": "PA_DOS_CORPORATIONS",
        "live_registry_name": "PA Open Data Registered Businesses & Governors/Officers (xvd7-5r2c)",
        "live_registry_url": "https://data.pa.gov/Businesses/Registered-Businesses-in-PA-Current-by-County/xvd7-5r2c",
        "cadence": "Weekly watermark-checked Socrata bulk CSV sync",
        "frozen_reason": None,
        "coverage_note": "PA corporations/LLCs/LPs and governors/officers/members auto-refresh from data.pa.gov (xvd7-5r2c), merged non-destructively with the 2026-11-17 OpenCorporates archive.",
    },
    # --- LIVE PARTIAL OFFICIAL REGISTRIES (Live Entities/Addresses; Historical Officers Frozen) ---
    "us_ny": {
        "state": "NY",
        "dataset_key": "NY",
        "label": "New York Department of State",
        "refresh_mode": "live_partial",
        "entities_live": True,
        "officers_live": False,
        "live_source_name": "NY_DOS_CORPORATIONS",
        "live_registry_name": "NY Open Data Active Corporations (n9v6-gdp6) + NYC HPD Contacts",
        "live_registry_url": "https://data.ny.gov/Economic-Development/Active-Corporations-Beginning-1800/n9v6-gdp6",
        "cadence": "Weekly watermark-checked Socrata bulk CSV sync",
        "frozen_reason": "NY DOS public bulk CSV includes active entities, DOS process/location addresses, and corporate CEOs, but does not publish LLC members/managers (historical OC officer rows remain frozen at 2026-11-17; NYC HPD registrations refresh nightly separately).",
        "coverage_note": "Active NY entities, addresses, and corporate CEOs auto-refresh from data.ny.gov (n9v6-gdp6); broader historical OpenCorporates officer records remain pinned to 2026-11-17.",
    },
    "us_co": {
        "state": "CO",
        "dataset_key": "CO",
        "label": "Colorado Secretary of State",
        "refresh_mode": "live_partial",
        "entities_live": True,
        "officers_live": False,
        "live_source_name": "CO_SOS_CORPORATIONS",
        "live_registry_name": "Colorado Open Data Business Entities (4ykn-tg5h) + True Names (qpft-q492)",
        "live_registry_url": "https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h",
        "cadence": "Weekly watermark-checked Socrata bulk CSV sync",
        "frozen_reason": "CO SOS bulk CSVs refresh active entities, principal/mailing addresses, registered agents, and True Names, but do not publish general officer/manager rosters (historical OC officer rows remain frozen at 2026-11-17).",
        "coverage_note": "Active CO entities, HQ/RA addresses, and True Names auto-refresh from data.colorado.gov (4ykn-tg5h & qpft-q492); broader historical OpenCorporates officer rows remain pinned to 2026-11-17.",
    },
    # --- SNAPSHOT-FROZEN REGISTRIES (Point-in-Time 2026-11-17 OpenCorporates Snapshot) ---
    "us_ca": {
        "state": "CA",
        "dataset_key": "CA",
        "label": "California Secretary of State",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://bizfileonline.sos.ca.gov/search/business",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "CA SOS charges a commercial fee for its Master Unload bulk file and does not offer an open free bulk download API.",
        "coverage_note": "Corporate entity & LLC manager/member records are frozen at the 2026-11-17 OpenCorporates snapshot (entities formed or officers changed after that date will not appear automatically; local property, lessor, and code feeds continue refreshing).",
    },
    "us_ma": {
        "state": "MA",
        "dataset_key": "MA",
        "label": "Massachusetts Secretary of the Commonwealth (Corporations Division)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "MA Corporations Division does not publish a free bulk download API and gates automated portal queries behind WAF/Incapsula bot protection.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (entities formed or officers changed after that date will not appear automatically; MassGIS & municipal CAMA property rolls continue refreshing).",
    },
    "us_il": {
        "state": "IL",
        "dataset_key": "IL",
        "label": "Illinois Secretary of State (Business Services)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://apps.ilsos.gov/businessentitysearch/",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Illinois SOS terms restrict its portal to individual searches only, forbid bulk scraping, and block automated access via anti-bot WAF.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (Cook/collar-county property rolls & Chicago building violations continue refreshing separately).",
    },
    "us_md": {
        "state": "MD",
        "dataset_key": "BALTIMORE",
        "label": "Maryland SDAT (State Dept. of Assessments & Taxation)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://egov.maryland.gov/BusinessExpress/EntitySearch",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Maryland Open Data only publishes aggregate counts; full SDAT corporate entity & officer extracts require formal paid data orders.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (Baltimore ownership, DHCD rental licenses, code violations & MD evictions continue refreshing separately).",
    },
    "us_mi": {
        "state": "MI",
        "dataset_key": "DETROIT",
        "label": "Michigan LARA (Corporations Division)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://cofs.lara.state.mi.us/SearchApi/Search/Search",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Michigan LARA restricts MiBusiness Registry to per-entity human lookups with no free public bulk download.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (Detroit parcels, BSEED rentals & blight data continue refreshing separately).",
    },
    "us_mn": {
        "state": "MN",
        "dataset_key": "MN",
        "label": "Minnesota Secretary of State",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://mblsportal.sos.state.mn.us/Business/Search",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Minnesota SOS portal is bot/CAPTCHA-gated and does not publish a free unauthenticated bulk entity/officer download API.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (Twin Cities CAMA parcels & Minneapolis Active Rental Licenses continue refreshing separately).",
    },
    "us_oh": {
        "state": "OH",
        "dataset_key": "OH",
        "label": "Ohio Secretary of State",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://businesssearch.ohiosos.gov/",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Ohio SOS full master bulk FTP downloads require a paid data purchase order and its web pages block automated HTTP fetching (403 WAF).",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (Ohio County Auditor CAMA rolls continue refreshing separately).",
    },
    "us_nj": {
        "state": "NJ",
        "dataset_key": "NJ",
        "label": "New Jersey Division of Revenue & Enterprise Services",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://www.njportal.com/DOR/BusinessNameSearch/",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "New Jersey does not publish a free bulk corporate entity/officer download on data.nj.gov; full extracts require paid licensing.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (NJ DCA BHI multi-family registrations continue refreshing separately).",
    },
    "us_nc": {
        "state": "NC",
        "dataset_key": "NC",
        "label": "North Carolina Secretary of State",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://www.sosnc.gov/online_services/search/by_title/_Business_Registration",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "North Carolina SOS does not offer a free public bulk CSV/API and protects sosnc.gov behind Cloudflare bot challenges (403).",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (NC OneMap 100-county parcels continue refreshing separately).",
    },
    "us_va": {
        "state": "VA",
        "dataset_key": "VA",
        "label": "Virginia State Corporation Commission (SCC)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://cis.scc.virginia.gov/",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Virginia SCC CIS does not offer a free public bulk download API; bulk corporate files require a paid subscription.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (VGIN & locality CAMA rolls continue refreshing separately).",
    },
    "us_wi": {
        "state": "WI",
        "dataset_key": "WI",
        "label": "Wisconsin Department of Financial Institutions (DFI)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://apps.dfi.wi.gov/apps/corpsearch/search.aspx",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Wisconsin DFI charges for bulk corporate extracts and blocks automated HTTP requests (403 WAF).",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (WI SCO/DOA statewide parcel rolls continue refreshing separately).",
    },
    "us_vt": {
        "state": "VT",
        "dataset_key": "VT",
        "label": "Vermont Secretary of State (Corporations Division)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://bizfilings.vermont.gov/business/businesssearch",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Vermont SOS does not publish a free public bulk download on data.vermont.gov; bulk extracts require a public-records request.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (VCGI Grand List parcel rolls continue refreshing separately).",
    },
    "us_ar": {
        "state": "AR",
        "dataset_key": "AR",
        "label": "Arkansas Secretary of State",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://www.ark.org/corp-search/index.php",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Arkansas SOS does not publish a free bulk corporate API and gates its lookup portal behind bot/CAPTCHA checks.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (Arkansas GIS Office parcels continue refreshing separately).",
    },
    "us_mt": {
        "state": "MT",
        "dataset_key": "MT",
        "label": "Montana Secretary of State",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://biz.sosmt.gov/search/business",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Montana SOS does not offer a free public bulk corporate download and returns 403 WAF blocks on automated requests.",
        "coverage_note": "Corporate entity & officer records are frozen at the 2026-11-17 OpenCorporates snapshot (Montana MSDI/DOR ORION cadastral rolls continue refreshing separately).",
    },
    # --- CROSS-STATE INCORPORATION-HAVEN REGISTRIES (Used for Foreign-LLC Piercing) ---
    "us_de": {
        "state": "DE",
        "dataset_key": "CROSS_STATE",
        "label": "Delaware Division of Corporations (Holding-LLC Piercing)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://icis.corp.delaware.gov/Ecorp/EntitySearch/NameSearch.aspx",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Delaware charges commercial fees for bulk corporate records, gates search with CAPTCHA, and does not publicly list LLC beneficial owners.",
        "coverage_note": "Used solely to resolve Delaware-domiciled holding LLCs that own property in covered jurisdictions; frozen at the 2026-11-17 OpenCorporates snapshot.",
    },
    "us_wy": {
        "state": "WY",
        "dataset_key": "CROSS_STATE",
        "label": "Wyoming Secretary of State (Holding-LLC Piercing)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://wyobiz.wyo.gov/Business/FilingSearch.aspx",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Wyoming SOS gates entity search with bot/CAPTCHA checks and does not provide a free bulk corporate API.",
        "coverage_note": "Used to resolve Wyoming-domiciled holding LLCs operating in covered jurisdictions; frozen at the 2026-11-17 OpenCorporates snapshot.",
    },
    "us_nv": {
        "state": "NV",
        "dataset_key": "CROSS_STATE",
        "label": "Nevada Secretary of State (Holding-LLC Piercing)",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": "https://esos.nv.gov/EntitySearch/OnlineEntitySearch",
        "cadence": "Frozen point-in-time snapshot (2026-11-17)",
        "frozen_reason": "Nevada SOS blocks automated access behind Incapsula WAF and charges for bulk SilverFlume commercial extracts.",
        "coverage_note": "Used to resolve Nevada-domiciled holding LLCs operating in covered jurisdictions; frozen at the 2026-11-17 OpenCorporates snapshot.",
    },
}


def get_registry_policy(jurisdiction_code: str) -> Dict[str, Any]:
    """Return canonical policy metadata for a `us_xx` jurisdiction code."""
    key = (jurisdiction_code or "").strip().lower()
    if key in CORPORATE_REGISTRY_MANIFEST:
        return dict(CORPORATE_REGISTRY_MANIFEST[key])
    st = key.replace("us_", "").upper() if key.startswith("us_") else key.upper()
    return {
        "state": st or "UNKNOWN",
        "dataset_key": st or "OTHER",
        "label": f"{st} Corporate Registry",
        "refresh_mode": "snapshot_frozen",
        "entities_live": False,
        "officers_live": False,
        "live_source_name": None,
        "live_registry_name": None,
        "live_registry_url": f"https://opencorporates.com/registers/{key}" if key else None,
        "cadence": f"Frozen point-in-time snapshot ({DEFAULT_OC_SNAPSHOT_DATE})",
        "frozen_reason": "No free official bulk corporate registry API configured; pinned to the archived OpenCorporates snapshot.",
        "coverage_note": f"Corporate entity & officer records reflect the {DEFAULT_OC_SNAPSHOT_DATE} OpenCorporates snapshot.",
    }


def refresh_status_for_mode(refresh_mode: str) -> str:
    """Map canonical refresh_mode to an honest `data_source_status.refresh_status` token."""
    if refresh_mode == "live_full":
        return "success"
    if refresh_mode == "live_partial":
        return "partial_live"
    return "snapshot_frozen"


def build_corporate_registry_summary(
    ds_by_name: Dict[str, Dict[str, Any]],
    snapshot_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a structured transparency summary across all loaded corporate registries
    for `/api/completeness` and `FreshnessModal.jsx`."""
    bulk_row = ds_by_name.get("OPENCORPORATES_BULK") or {}
    bulk_details = bulk_row.get("details") if isinstance(bulk_row.get("details"), dict) else {}
    snap = snapshot_date or bulk_details.get("snapshot") or DEFAULT_OC_SNAPSHOT_DATE

    registries: List[Dict[str, Any]] = []
    live_full_states: List[str] = []
    live_partial_states: List[str] = []
    snapshot_frozen_states: List[str] = []
    frozen_haven_states: List[str] = []

    for jur_code, policy in sorted(CORPORATE_REGISTRY_MANIFEST.items(), key=lambda kv: kv[1]["state"]):
        oc_key = f"OC_{jur_code.upper()}"
        oc_row = ds_by_name.get(oc_key) or {}
        oc_details = oc_row.get("details") if isinstance(oc_row.get("details"), dict) else {}

        live_key = policy.get("live_source_name")
        live_row = ds_by_name.get(live_key) if live_key else None
        live_details = live_row.get("details") if (live_row and isinstance(live_row.get("details"), dict)) else {}

        mode = policy["refresh_mode"]
        st = policy["state"]
        if mode == "live_full":
            live_full_states.append(st)
        elif mode == "live_partial":
            live_partial_states.append(st)
        else:
            if policy.get("dataset_key") == "CROSS_STATE":
                frozen_haven_states.append(st)
            else:
                snapshot_frozen_states.append(st)

        ext_dt = (live_row or {}).get("external_last_updated") or oc_row.get("external_last_updated")
        last_ref = (live_row or {}).get("last_refreshed_at") or oc_row.get("last_refreshed_at")
        ext_iso = ext_dt.isoformat() if hasattr(ext_dt, "isoformat") else (str(ext_dt) if ext_dt else None)
        ref_iso = last_ref.isoformat() if hasattr(last_ref, "isoformat") else (str(last_ref) if last_ref else None)

        registries.append(
            {
                "jurisdiction_code": jur_code,
                "state": st,
                "dataset_key": policy["dataset_key"],
                "label": policy["label"],
                "refresh_mode": mode,
                "status": refresh_status_for_mode(mode),
                "is_frozen_snapshot": mode == "snapshot_frozen",
                "entities_live": bool(policy["entities_live"]),
                "officers_live": bool(policy["officers_live"]),
                "snapshot_date": oc_details.get("snapshot") or snap,
                "companies_count": oc_details.get("companies_count") or live_details.get("companies_count"),
                "officers_count": oc_details.get("officers_count") or live_details.get("officers_count"),
                "external_last_updated": ext_iso,
                "last_refreshed_at": ref_iso,
                "live_registry_name": policy.get("live_registry_name"),
                "source_url": policy.get("live_registry_url") or oc_details.get("source_url"),
                "cadence": policy.get("cadence"),
                "frozen_reason": policy.get("frozen_reason"),
                "coverage_note": policy.get("coverage_note"),
            }
        )

    irs_row = ds_by_name.get("IRS_EO_BMF_NONPROFITS") or {}
    irs_ext = irs_row.get("external_last_updated")

    return {
        "snapshot_date": snap,
        "live_full_states": live_full_states,
        "live_partial_states": live_partial_states,
        "snapshot_frozen_states": snapshot_frozen_states,
        "frozen_holding_haven_states": frozen_haven_states,
        "irs_nonprofit_firewall_updated": irs_ext.isoformat() if hasattr(irs_ext, "isoformat") else (str(irs_ext) if irs_ext else None),
        "summary_note": (
            f"Corporate registry auto-refresh runs directly against free official public feeds for "
            f"{len(live_full_states)} full-coverage jurisdictions ({', '.join(live_full_states)}) and "
            f"{len(live_partial_states)} partial-coverage states ({', '.join(live_partial_states)}). "
            f"Business entity & officer data for {len(snapshot_frozen_states)} active jurisdictions "
            f"({', '.join(snapshot_frozen_states)}) plus {len(frozen_haven_states)} holding-company registries "
            f"({', '.join(frozen_haven_states)}) is frozen at the {snap} OpenCorporates point-in-time snapshot "
            f"because those states do not publish free unblocked bulk corporate APIs. Property tax rolls, "
            f"rental licenses, and code-enforcement feeds continue refreshing independently."
        ),
        "registries": registries,
    }
