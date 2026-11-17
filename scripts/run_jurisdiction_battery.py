#!/usr/bin/env python3
"""
scripts/run_jurisdiction_battery.py
===================================
Comprehensive two-pillar optimization & verification battery for They Own WHAT??:
  1) Site Functional & UI Reality Alignment:
     - Health, features, completeness, freshness, frontend bundle & contract alignment
     - Omnibar autocomplete, unified/typed search, broad-term & null-string leak guards
     - Top networks across sort modes (`buildings`, `units`, `code_cases`, `evictions`)
       and private-vs-institutional filter (`include_institutional=false`)
     - Network detail graph coherence (`building_count == len(properties)` without
       multi-registration row multiplication, `RELIABLE_UNIT_CITIES` unit consistency,
       jurisdiction geographic bounding-box sanity, `connection_signals` / human-owner
       untangling coherence, anchor & anti-meganet invariants)
     - Property detail, official-record embeds/links, rap sheets, and live HTTP probes
       against upstream municipal/state open-data APIs
  2) Data Currency, Untangling & Source-Only Self-Healing (`--heal`):
     - Sweeps stale `running` statuses (> 12h)
     - Repairs truncated/failed/missing source-only feeds for the target cohort
     - Records pre-heal vs post-heal metrics, writes dated benchmark report in
       `docs/benchmarks/`, optionally advances `jurisdiction-rotation.json`, and
       optionally dispatches an HTML summary email via `updater/send_audit_email.py`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import subprocess
import sys
import time
import unittest
from typing import Any, Callable, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from dotenv import load_dotenv

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, REPO_ROOT)

env_path = os.path.join(REPO_ROOT, ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()

from api.city_routes import INSTITUTIONAL_NETWORK_PATTERN, RELIABLE_UNIT_CITIES
from api.shared_utils import THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN
from scripts.rotate_jurisdiction_optimization import (
    DEFAULT_MANIFEST_PATH,
    advance_manifest,
    compute_status,
    format_iso_utc,
    load_manifest,
    save_manifest,
    update_latest_automation_status,
    utc_now,
)
from updater.update_data import sweep_stale_running_statuses

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("jurisdiction-battery")

INSTITUTIONAL_RE = re.compile(INSTITUTIONAL_NETWORK_PATTERN, re.IGNORECASE)
FORBIDDEN_CONTEXT_TOKENS = {"NULL", "NONE", "NAN", "UNDEFINED", "NO INFORMATION PROVIDED"}

# Geographic bounding boxes for coordinate reality verification (`(min_lat, max_lat, min_lon, max_lon)`)
JURISDICTION_BOUNDS: Dict[str, Tuple[float, float, float, float]] = {
    "CT": (40.90, 42.10, -73.80, -71.70),
    "NYC": (40.45, 40.95, -74.30, -73.65),
    "NJ": (38.85, 41.40, -75.60, -73.85),
    "DC": (38.78, 39.00, -77.15, -76.90),
    "BALTIMORE": (39.18, 39.40, -76.75, -76.50),
    "BOSTON": (42.20, 42.42, -71.20, -70.95),
    "DETROIT": (42.24, 42.46, -83.30, -82.90),
    "MIAMI": (25.10, 26.05, -80.90, -80.05),
    "MINNEAPOLIS": (44.88, 45.06, -93.35, -93.18),
    "PHILADELPHIA": (39.85, 40.15, -75.30, -74.95),
    "CHICAGO": (41.60, 42.10, -87.95, -87.50),
    "MA": (41.20, 42.90, -73.55, -69.90),
    "FL": (24.30, 31.10, -87.70, -79.90),
    "NYS": (40.45, 45.05, -79.80, -71.80),
    "CO": (36.95, 41.05, -109.10, -102.00),
    "NC": (33.75, 36.62, -84.35, -75.40),
    "WI": (42.45, 47.15, -92.95, -86.20),
    "VT": (42.70, 45.05, -73.48, -71.45),
    "AR": (33.00, 36.52, -94.65, -89.60),
    "VA": (36.52, 39.48, -83.69, -75.22),
}

# Live upstream public endpoints verified per jurisdiction
UPSTREAM_PROBES: Dict[str, List[Tuple[str, str, Optional[Dict[str, Any]]]]] = {
    "CT": [
        (
            "Hartford Code Enforcement FeatureServer",
            "https://utility.arcgis.com/usrsvcs/servers/7cf59bd708a94395be61954d6f430e3f/rest/services/HartfordOpenDataTables/FeatureServer/1/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "NYC": [
        ("NYC HPD Registrations (Socrata)", "https://data.cityofnewyork.us/resource/tesw-yqqr.json", {"$limit": 1}),
        ("NY DOS Active Corporations (Socrata)", "https://data.ny.gov/resource/n9v6-gdp6.json", {"$limit": 1}),
    ],
    "NJ": [
        ("NJGIN Parcels / DCA Endpoint Reachability", "https://data.nj.gov/api/views/metadata/v1", {"$limit": 1}),
    ],
    "DC": [
        (
            "DC GIS CAMA Layer 40",
            "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/40/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
        (
            "DC DLCP Basic Business Rental Licenses",
            "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/0/query",
            {"where": "BUSINESSACTIVITY='Apartment'", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "BALTIMORE": [
        (
            "Baltimore baltegis dmxOwnership",
            "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxOwnership/MapServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
        (
            "Baltimore DHCD Rental Registrations & Licenses",
            "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/OpenGov_RegAndLicense/MapServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "BOSTON": [
        (
            "Analyze Boston Building Violations CKAN",
            "https://data.boston.gov/api/3/action/datastore_search",
            {"resource_id": "800a2663-1d6a-46e7-9356-bedb70f5332c", "limit": 1},
        ),
    ],
    "DETROIT": [
        (
            "Detroit BSEED Rental Registrations",
            "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/bseed_rental_registrations/FeatureServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "MIAMI": [
        (
            "Miami-Dade Property Point View",
            "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaGISView_gdb/FeatureServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "MINNEAPOLIS": [
        (
            "Minneapolis Active Rental Licenses",
            "https://services.arcgis.com/afSMGVsC7QlRK1kZ/ArcGIS/rest/services/Active_Rental_Licenses/FeatureServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
        (
            "Minneapolis Regulatory Services CaseViolations",
            "https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/CaseViolations/FeatureServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "PHILADELPHIA": [
        (
            "Philadelphia Carto OPA Properties",
            "https://phl.carto.com/api/v2/sql",
            {"q": "SELECT cartodb_id FROM opa_properties_public LIMIT 1"},
        ),
        (
            "Philadelphia Carto L&I Rental Licenses",
            "https://phl.carto.com/api/v2/sql",
            {"q": "SELECT cartodb_id FROM business_licenses WHERE licensetype='Rental' LIMIT 1"},
        ),
    ],
    "CHICAGO": [
        (
            "Cook County Assessor Parcel Addresses (3723-97qp)",
            "https://datacatalog.cookcountyil.gov/resource/3723-97qp.json",
            {"$where": "year='2026' AND prop_address_city_name='CHICAGO'", "$limit": 1},
        ),
        (
            "Cook County Assessor Parcel Universe (pabr-t5kh)",
            "https://datacatalog.cookcountyil.gov/resource/pabr-t5kh.json",
            {"$where": "chicago_community_area_name IS NOT NULL", "$limit": 1},
        ),
    ],
    "MA": [
        (
            "MassGIS Level 3 Property Tax Parcels FeatureServer",
            "https://services1.arcgis.com/hGdibHYSPO59RG1h/ArcGIS/rest/services/Massachusetts_Property_Tax_Parcels/FeatureServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "FL": [
        (
            "FloridaGIO Statewide Parcels FeatureServer",
            "https://services9.arcgis.com/Gh9awoU677aKree0/arcgis/rest/services/Florida_Statewide_Cadastral/FeatureServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "NYS": [
        (
            "NYS ITS Public Tax Parcels FeatureServer",
            "https://services6.arcgis.com/EbVsqZ18sv1kVJ3k/arcgis/rest/services/NYS_Tax_Parcels_Public/FeatureServer/1/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
        (
            "Buffalo Rental Registry (Socrata)",
            "https://data.buffalony.gov/resource/hpqg-ihzt.json",
            {"$limit": 1},
        ),
    ],
    "CO": [
        (
            "Colorado OIT Public Parcels FeatureServer",
            "https://gis.colorado.gov/public/rest/services/Address_and_Parcel/Colorado_Public_Parcels/FeatureServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
        (
            "Colorado SOS Business Entities (Socrata)",
            "https://data.colorado.gov/resource/4ykn-tg5h.json",
            {"$limit": 1},
        ),
    ],
    "NC": [
        (
            "NC OneMap Statewide Parcels FeatureServer",
            "https://services.nconemap.gov/secure/rest/services/NC1Map_Parcels/FeatureServer/1/query",
            {"where": "cntyfips='183'", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "WI": [
        (
            "Wisconsin SCO/DOA V11 Statewide Parcels",
            "https://services3.arcgis.com/n6uYoouQZW75n5WI/arcgis/rest/services/Wisconsin_Statewide_Parcels/FeatureServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "VT": [
        (
            "Vermont VCGI Statewide Grand List Parcels",
            "https://services1.arcgis.com/BkFxaEFNwHqX3tAw/arcgis/rest/services/FS_VCGI_OPENDATA_Cadastral_VTPARCELS_poly_standardized_parcels_SP_v1/FeatureServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "AR": [
        (
            "Arkansas GIS Office Statewide Parcels FeatureServer",
            "https://gis.arkansas.gov/arcgis/rest/services/FEATURESERVICES/Planning_Cadastre/FeatureServer/6/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
    ],
    "VA": [
        (
            "Virginia VGIN Statewide Parcels MapServer",
            "https://vginmaps.vdem.virginia.gov/arcgis/rest/services/VA_Base_Layers/VA_Parcels/MapServer/0/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        ),
        (
            "Hampton Roads HRGeo Regional Parcels MapServer",
            "https://geo.hrsd.com/hrgeo/rest/services/regionalgis/HRGeo_Parcels_Public/MapServer/0/query",
            {"where": "OWNERNME1 IS NOT NULL", "returnCountOnly": "true", "f": "json"},
        ),
    ],
}


def resolve_db_connection():
    candidates = []
    if os.environ.get("DATABASE_URL"):
        candidates.append(os.environ["DATABASE_URL"])
    candidates.extend([
        "postgresql://user:password@127.0.0.1:5432/ctdata",
        "postgresql://user:password@ctdata_db:5432/ctdata",
    ])
    last_err = None
    for url in dict.fromkeys(candidates):
        try:
            return psycopg2.connect(url, connect_timeout=5), url
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"Could not connect to PostgreSQL: {last_err}")


def resolve_http_base(env_key: str, fallbacks: List[str], probe_path: str = "/api/health") -> Optional[str]:
    candidates = []
    if os.environ.get(env_key):
        candidates.append(os.environ[env_key].rstrip("/"))
    candidates.extend(fallbacks)
    unique_candidates = list(dict.fromkeys(candidates))
    for attempt in range(4):
        for base in unique_candidates:
            try:
                resp = requests.get(f"{base}{probe_path}", timeout=6)
                if resp.status_code < 500:
                    return base
            except Exception:
                continue
        if attempt < 3:
            time.sleep(2)
    return None


class CheckRecorder:
    def __init__(self):
        self.checks: List[Dict[str, Any]] = []

    def record(self, jurisdiction: str, category: str, name: str, passed: bool, details: str, metrics: Optional[Dict[str, Any]] = None):
        status = "PASS" if passed else "FAIL"
        self.checks.append({
            "jurisdiction": jurisdiction,
            "category": category,
            "name": name,
            "status": status,
            "details": details,
            "metrics": metrics or {},
        })
        icon = "✅" if passed else "❌"
        logger.info("%s [%s | %s] %s — %s", icon, jurisdiction, category, name, details)


def run_source_only_cmd(args: List[str], db_url: str, timeout: int = 900) -> Tuple[bool, str]:
    env = os.environ.copy()
    env["THEYOWNWHAT_SOURCE_ONLY"] = "true"
    env["DATABASE_URL"] = db_url
    logger.info("🔧 Self-heal executing: %s", " ".join(args))
    try:
        proc = subprocess.run(
            args,
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        out = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
        return proc.returncode == 0, out[-1200:]
    except subprocess.TimeoutExpired:
        return False, f"Timed out after {timeout}s"
    except Exception as exc:
        return False, str(exc)


def snapshot_jurisdiction_metrics(conn, code: str, api_city: Optional[str]) -> Dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if code == "CT":
            # Avoid full seqscan contention if safe_network_refresh is actively updating `properties`
            cur.execute("SELECT COUNT(*) AS cnt FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%UPDATE properties%'")
            ct_busy = (cur.fetchone()["cnt"] or 0) > 0
            if ct_busy:
                cur.execute("SELECT reltuples::bigint AS p_cnt FROM pg_class WHERE oid = 'public.properties'::regclass")
                p_cnt = int(cur.fetchone()["p_cnt"] or 0)
            else:
                cur.execute("SELECT COUNT(*) AS p_cnt FROM properties WHERE source IS DISTINCT FROM 'NYS_OPEN_DATA'")
                p_cnt = int(cur.fetchone()["p_cnt"] or 0)
            cur.execute("SELECT COUNT(*) AS n_cnt, COALESCE(MAX(total_properties), 0) AS max_net FROM networks")
            n_row = cur.fetchone()
            cur.execute("SELECT COUNT(*) AS ce_cnt FROM code_enforcement WHERE municipality = 'HARTFORD'")
            ce_cnt = int(cur.fetchone()["ce_cnt"] or 0)
            return {
                "properties": p_cnt,
                "networks": int(n_row["n_cnt"] or 0),
                "max_network_size": int(n_row["max_net"] or 0),
                "hartford_code_enforcement": ce_cnt,
            }
        if api_city:
            cur.execute(f"SELECT COUNT(*) AS p_cnt FROM {api_city}_properties")
            p_cnt = int(cur.fetchone()["p_cnt"] or 0)
            cur.execute(f"SELECT COUNT(*) FILTER (WHERE COALESCE(building_count, 0) > 0) AS n_cnt, COALESCE(MAX(building_count), 0) AS max_net FROM {api_city}_networks")
            n_row = cur.fetchone()
            cur.execute(f"SELECT COUNT(*) AS r_cnt FROM {api_city}_hpd_registrations")
            r_cnt = int(cur.fetchone()["r_cnt"] or 0)
            cur.execute(f"SELECT COUNT(*) AS c_cnt FROM {api_city}_hpd_contacts")
            c_cnt = int(cur.fetchone()["c_cnt"] or 0)
            cur.execute(f"SELECT COUNT(*) AS s_cnt, COALESCE(SUM(violations_total), 0) AS v_tot FROM {api_city}_bbl_stats")
            s_row = cur.fetchone()
            return {
                "properties": p_cnt,
                "networks": int(n_row["n_cnt"] or 0),
                "max_network_size": int(n_row["max_net"] or 0),
                "registrations": r_cnt,
                "contacts": c_cnt,
                "bbl_stats_rows": int(s_row["s_cnt"] or 0),
                "total_violations": int(s_row["v_tot"] or 0),
            }
    return {}


def heal_cohort_if_needed(conn, db_url: str, cohort: Dict[str, Any]) -> List[Dict[str, Any]]:
    healed: List[Dict[str, Any]] = []
    swept = sweep_stale_running_statuses(conn, max_age_hours=12)
    if swept:
        healed.append({"action": "sweep_stale_running_statuses", "rows_swept": swept, "success": True})

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT source_name, refresh_status, last_refreshed_at FROM data_source_status")
        status_map = {r["source_name"]: r for r in cur.fetchall()}

    for j in cohort["jurisdictions"]:
        code = j["code"]
        api_city = j.get("apiCity")
        pre = snapshot_jurisdiction_metrics(conn, code, api_city)

        if code == "CT":
            h_status = (status_map.get("HARTFORD_CODE_ENFORCEMENT") or {}).get("refresh_status", "").lower()
            if h_status in ("failed", "failure", "error", ""):
                ok, out = run_source_only_cmd([sys.executable, "updater/ingest_hartford_enforcement.py"], db_url, timeout=600)
                healed.append({
                    "jurisdiction": code,
                    "feed": "HARTFORD_CODE_ENFORCEMENT",
                    "success": ok,
                    "pre": pre,
                    "post": snapshot_jurisdiction_metrics(conn, code, api_city),
                    "log_tail": out[-300:],
                })

        elif code == "NYC":
            if "NY_DOS_CORPORATIONS" not in status_map or status_map["NY_DOS_CORPORATIONS"]["refresh_status"] != "success":
                ok, out = run_source_only_cmd(
                    [sys.executable, "-m", "nyc.enrich_ny_dos", "--limit-corps", "600"],
                    db_url,
                    timeout=600,
                )
                healed.append({
                    "jurisdiction": code,
                    "feed": "NY_DOS_CORPORATIONS",
                    "success": ok,
                    "pre": pre,
                    "post": snapshot_jurisdiction_metrics(conn, code, api_city),
                    "log_tail": out[-300:],
                })

        elif code == "DC":
            if pre.get("registrations", 0) == 0 or (status_map.get("DC_RENTAL_LICENSES") or {}).get("refresh_status") != "success":
                ok, out = run_source_only_cmd([sys.executable, "updater/enrich_dc_licenses.py"], db_url, timeout=900)
                healed.append({
                    "jurisdiction": code,
                    "feed": "DC_RENTAL_LICENSES",
                    "success": ok,
                    "pre": pre,
                    "post": snapshot_jurisdiction_metrics(conn, code, api_city),
                    "log_tail": out[-300:],
                })
            if (status_map.get("DC_CODE_ENFORCEMENT") or {}).get("refresh_status") != "success":
                ok, out = run_source_only_cmd([sys.executable, "updater/enrich_city_enforcement.py", "--city", "dc"], db_url, timeout=300)
                healed.append({"jurisdiction": code, "feed": "DC_CODE_ENFORCEMENT", "success": ok, "log_tail": out[-300:]})

        elif code == "BALTIMORE":
            if pre.get("registrations", 0) == 0 or (status_map.get("BALTIMORE_RENTAL_REGISTRATIONS") or {}).get("refresh_status") != "success":
                ok, out = run_source_only_cmd([sys.executable, "updater/enrich_baltimore_registrations.py"], db_url, timeout=900)
                healed.append({
                    "jurisdiction": code,
                    "feed": "BALTIMORE_RENTAL_REGISTRATIONS",
                    "success": ok,
                    "pre": pre,
                    "post": snapshot_jurisdiction_metrics(conn, code, api_city),
                    "log_tail": out[-300:],
                })
            b_enf = (status_map.get("BALTIMORE_ENFORCEMENT") or {}).get("refresh_status", "").lower()
            if b_enf in ("failed", "failure", "error", ""):
                ok, out = run_source_only_cmd([sys.executable, "updater/enrich_city_enforcement.py", "--city", "baltimore"], db_url, timeout=900)
                healed.append({"jurisdiction": code, "feed": "BALTIMORE_CODE_ENFORCEMENT", "success": ok, "log_tail": out[-300:]})

        elif code == "BOSTON":
            bos_enf = (status_map.get("BOSTON_ENFORCEMENT") or {}).get("refresh_status", "").lower()
            if bos_enf in ("failed", "failure", "error", "") or (status_map.get("BOSTON_CODE_ENFORCEMENT") or {}).get("refresh_status") != "success":
                ok, out = run_source_only_cmd([sys.executable, "updater/enrich_city_enforcement.py", "--city", "boston"], db_url, timeout=900)
                healed.append({"jurisdiction": code, "feed": "BOSTON_CODE_ENFORCEMENT", "success": ok, "log_tail": out[-300:]})

        elif code == "MINNEAPOLIS":
            if pre.get("properties", 0) < int(j.get("minProperties", 18000)):
                ok, out = run_source_only_cmd([sys.executable, "updater/update_cities.py", "--city", "minneapolis"], db_url, timeout=600)
                healed.append({
                    "jurisdiction": code,
                    "feed": "MINNEAPOLIS",
                    "success": ok,
                    "pre": pre,
                    "post": snapshot_jurisdiction_metrics(conn, code, api_city),
                    "log_tail": out[-300:],
                })
            cur_post = snapshot_jurisdiction_metrics(conn, code, api_city)
            if cur_post.get("total_violations", 0) == 0 or (status_map.get("MINNEAPOLIS_CODE_ENFORCEMENT") or {}).get("refresh_status") != "success":
                ok, out = run_source_only_cmd([sys.executable, "updater/enrich_city_enforcement.py", "--city", "minneapolis"], db_url, timeout=900)
                healed.append({
                    "jurisdiction": code,
                    "feed": "MINNEAPOLIS_CODE_ENFORCEMENT",
                    "success": ok,
                    "post": snapshot_jurisdiction_metrics(conn, code, api_city),
                    "log_tail": out[-300:],
                })

        elif code in {"VT", "CO", "NYS", "FL", "WI", "NC", "AR", "MA", "VA"} and api_city:
            st_status = (status_map.get(api_city.upper()) or {}).get("refresh_status", "").lower()
            if pre.get("properties", 0) < int(j.get("minProperties", 100000)) or st_status in ("failed", "failure", "error", ""):
                if api_city == "ma":
                    cmd = [sys.executable, "ma/ingest_massgis.py", "--rebuild-only"]
                elif api_city == "va":
                    cmd = [sys.executable, "states/ingest_va_parcels.py", "--rebuild-only"]
                else:
                    cmd = [sys.executable, "states/ingest_statewide_parcels.py", "--states", api_city, "--rebuild-only"]
                ok, out = run_source_only_cmd(cmd, db_url, timeout=1800)
                healed.append({
                    "jurisdiction": code,
                    "feed": api_city.upper(),
                    "success": ok,
                    "pre": pre,
                    "post": snapshot_jurisdiction_metrics(conn, code, api_city),
                    "log_tail": out[-300:],
                })

    return healed


def verify_global_platform(rec: CheckRecorder, api_base: str, nginx_base: Optional[str]):
    # 1. API Health
    r = requests.get(f"{api_base}/api/health", timeout=8)
    rec.record("GLOBAL", "functional", "api_health", r.status_code == 200, f"GET /api/health -> {r.status_code}")

    # 2. Nginx reverse proxy (when running)
    if nginx_base:
        rn = requests.get(f"{nginx_base}/api/health", timeout=8)
        rec.record("GLOBAL", "functional", "nginx_proxy_health", rn.status_code == 200, f"GET {nginx_base}/api/health -> {rn.status_code}")

    # 3. Freshness & Completeness contracts
    rf = requests.get(f"{api_base}/api/freshness", timeout=15)
    rf_data = rf.json() if rf.status_code == 200 else {}
    sys_f = rf_data.get("system_freshness") or {}
    rec.record(
        "GLOBAL",
        "freshness",
        "system_freshness_metadata",
        rf.status_code == 200 and bool(sys_f.get("principals_last_updated")) and bool(sys_f.get("businesses_last_updated")),
        f"principals={sys_f.get('principals_last_updated')}, businesses={sys_f.get('businesses_last_updated')}",
    )

    rc = requests.get(f"{api_base}/api/completeness", timeout=120)
    rc_data = rc.json() if rc.status_code == 200 else {}
    src_list = rc_data.get("sources") if isinstance(rc_data, dict) else rc_data
    rec.record(
        "GLOBAL",
        "freshness",
        "completeness_matrix_endpoint",
        rc.status_code == 200 and isinstance(src_list, list) and len(src_list) >= 100,
        f"GET /api/completeness returned {len(src_list) if isinstance(src_list, list) else 0} source rows",
    )

    # 4. Core Network Algorithm & Nationwide OC Linker Unit Suites
    from tests.test_network_algorithms import TestNetworkAlgorithmsAndNormalization
    from tests.test_oc_linker_and_schema import TestOCLinkerAndAssessorDemangler
    suite = unittest.TestSuite([
        unittest.defaultTestLoader.loadTestsFromTestCase(TestNetworkAlgorithmsAndNormalization),
        unittest.defaultTestLoader.loadTestsFromTestCase(TestOCLinkerAndAssessorDemangler),
    ])
    res = unittest.TextTestRunner(stream=open(os.devnull, "w")).run(suite)
    rec.record(
        "GLOBAL",
        "untangling",
        "network_algorithm_unit_suite",
        res.wasSuccessful(),
        f"Ran {res.testsRun} deterministic normalization/registrar/clustering unit assertions (failures={len(res.failures) + len(res.errors)})",
    )

    # 5. Frontend UI contract alignment (`DatasetLanding.jsx` vs `FreshnessModal.jsx`)
    landing_path = os.path.join(REPO_ROOT, "frontend", "src", "components", "DatasetLanding.jsx")
    freshness_path = os.path.join(REPO_ROOT, "frontend", "src", "components", "FreshnessModal.jsx")
    if os.path.exists(landing_path) and os.path.exists(freshness_path):
        with open(landing_path, "r", encoding="utf-8") as f:
            landing_src = f.read()
        with open(freshness_path, "r", encoding="utf-8") as f:
            freshness_src = f.read()
        has_miami_freshness = "'MIAMI'" in freshness_src
        has_mpls_code = "'MINNEAPOLIS'" in landing_src and "CODE_CASE_DATASET_KEYS" in landing_src
        rec.record(
            "GLOBAL",
            "ui_reality",
            "frontend_dataset_contract_alignment",
            has_miami_freshness and has_mpls_code,
            f"FreshnessModal includes MIAMI={has_miami_freshness}; DatasetLanding CODE_CASE_DATASET_KEYS includes MINNEAPOLIS={has_mpls_code}",
        )


def probe_upstream_sources(rec: CheckRecorder, code: str):
    probes = UPSTREAM_PROBES.get(code, [])
    for label, url, params in probes:
        try:
            resp = requests.get(url, params=params, timeout=15, headers={"User-Agent": "TheyOwnWhat-Audit/1.0"})
            ok = resp.status_code == 200
            detail = f"HTTP {resp.status_code}"
            if ok and params and params.get("f") == "json":
                payload = resp.json()
                if "error" in payload:
                    ok = False
                    detail = f"ArcGIS error: {payload['error']}"
                elif "count" in payload:
                    ok = int(payload["count"] or 0) > 0
                    detail = f"HTTP 200, live upstream count={payload['count']:,}"
            rec.record(code, "upstream_currency", f"upstream_probe:{label}", ok, detail)
        except Exception as exc:
            rec.record(code, "upstream_currency", f"upstream_probe:{label}", False, f"Probe error: {exc}")


def verify_ct_statewide(rec: CheckRecorder, conn, api_base: str, j_cfg: Dict[str, Any]):
    code = "CT"
    metrics = snapshot_jurisdiction_metrics(conn, "CT", None)
    min_props = int(j_cfg.get("minProperties", 1000000))
    min_nets = int(j_cfg.get("minNetworks", 30000))

    rec.record(
        code,
        "data_integrity",
        "ct_volume_and_hartford_code",
        metrics["properties"] >= min_props and metrics["networks"] >= min_nets and metrics["hartford_code_enforcement"] >= 5000,
        f"properties={metrics['properties']:,}, networks={metrics['networks']:,}, hartford_code={metrics['hartford_code_enforcement']:,}",
        metrics,
    )

    # Anchor assertions: Gurevitch >= 1,200 & Reiner separation from Rausman/Hollender
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT COALESCE(SUM(total_properties), 0) AS g_cnt FROM networks WHERE UPPER(primary_name) LIKE '%GUREVITCH%'")
        g_cnt = int(cur.fetchone()["g_cnt"] or 0)
        rec.record(
            code,
            "untangling",
            "anchor_gurevitch_portfolio",
            g_cnt >= 1200,
            f"Gurevitch linked properties={g_cnt:,} (threshold >= 1,200)",
            {"gurevitch_properties": g_cnt},
        )

        cur.execute(
            """
            SELECT COUNT(*) AS overlap
            FROM entity_networks r1
            JOIN entity_networks r2 ON r1.network_id = r2.network_id
            WHERE UPPER(r1.entity_name) LIKE '%REINER%'
              AND (UPPER(r2.entity_name) LIKE '%RAUSMAN%' OR UPPER(r2.entity_name) LIKE '%HOLLENDER%')
            """
        )
        reiner_overlap = int(cur.fetchone()["overlap"] or 0)
        rec.record(
            code,
            "untangling",
            "anchor_reiner_rausman_separation",
            reiner_overlap == 0,
            f"Reiner vs Rausman/Hollender shared-network entity overlap={reiner_overlap} (expected 0)",
        )

        cur.execute(
            """
            SELECT COUNT(DISTINCT n.id) AS tp_leaks
            FROM networks n
            LEFT JOIN entity_networks en ON en.network_id = n.id AND en.entity_type = 'business'
            WHERE COALESCE(n.primary_name, '') ~* %s
               OR COALESCE(en.entity_name, '') ~* %s
            """,
            (THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN, THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN),
        )
        ct_tp_leaks = int(cur.fetchone()["tp_leaks"] or 0)
        rec.record(
            code,
            "untangling",
            "ct_third_party_exclusion_hygiene",
            ct_tp_leaks == 0,
            f"CT 3rd-party bank/servicer/RA/institution network leaks={ct_tp_leaks} (expected 0)",
        )

    # Omnibar autocomplete & typed `/api/search`
    ra = requests.get(f"{api_base}/api/autocomplete", params={"q": "GUREVITCH", "type": "all", "state": "CT"}, timeout=15)
    ra_rows = ra.json() if ra.status_code == 200 else []
    bad_ctx = [r for r in ra_rows if str(r.get("context") or "").strip().upper() in FORBIDDEN_CONTEXT_TOKENS]
    rec.record(
        code,
        "functional",
        "ct_autocomplete_and_context_hygiene",
        ra.status_code == 200 and len(ra_rows) > 0 and len(bad_ctx) == 0,
        f"/api/autocomplete?q=GUREVITCH&type=all&state=CT -> {len(ra_rows)} results, bad_context={len(bad_ctx)}",
    )

    for stype in ("all", "business", "principal", "owner", "address"):
        q_term = "MAIN ST" if stype == "address" else ("CARABETTA" if stype == "business" else "GUREVITCH")
        rs = requests.get(f"{api_base}/api/search", params={"type": stype, "term": q_term, "state": "CT"}, timeout=20)
        rs_rows = rs.json() if rs.status_code == 200 else []
        rec.record(
            code,
            "functional",
            f"ct_search_type_{stype}",
            rs.status_code == 200 and isinstance(rs_rows, list) and len(rs_rows) > 0,
            f"/api/search?type={stype}&term={q_term} -> {len(rs_rows)} results",
        )

    # CT Dashboard Summary (STATEWIDE landing card + HARTFORD municipal filter) & Rap Sheet Monitor
    t_sw = time.time()
    rsw = requests.get(f"{api_base}/api/dashboard/summary", params={"city": "STATEWIDE"}, timeout=10)
    sw_elapsed = time.time() - t_sw
    rsw_json = rsw.json() if rsw.status_code == 200 else {}
    sw_ok = (
        rsw.status_code == 200
        and sw_elapsed < 2.5
        and int(rsw_json.get("property_count") or 0) >= 1000000
        and int(rsw_json.get("property_count") or 0) != 1045000
        and int(rsw_json.get("network_count") or 0) >= 30000
        and int(rsw_json.get("town_count") or 0) >= 160
        and not str(rsw_json.get("last_updated") or "").startswith("9999")
    )
    rec.record(
        code,
        "ui_reality",
        "ct_statewide_dashboard_summary_fast",
        sw_ok,
        f"STATEWIDE summary in {sw_elapsed:.3f}s: properties={rsw_json.get('property_count')}, networks={rsw_json.get('network_count')}, towns={rsw_json.get('town_count')}",
    )

    rd = requests.get(f"{api_base}/api/dashboard/summary", params={"city": "HARTFORD"}, timeout=60)
    rd_json = rd.json() if rd.status_code == 200 else {}
    not_fabricated = rd_json.get("property_count") != 1045000
    rec.record(
        code,
        "ui_reality",
        "ct_dashboard_summary_source_backed",
        rd.status_code == 200 and int(rd_json.get("property_count") or 0) > 10000 and not_fabricated,
        f"Hartford dashboard property_count={rd_json.get('property_count')}, network_count={rd_json.get('network_count')}",
    )

    rm = requests.get(f"{api_base}/api/monitor", params={"city": "HARTFORD", "limit": 10}, timeout=60)
    rm_json = rm.json() if rm.status_code == 200 else {}
    rm_rows = rm_json.get("results") if isinstance(rm_json, dict) else rm_json
    rec.record(
        code,
        "functional",
        "ct_hartford_rap_sheet_monitor",
        rm.status_code == 200 and isinstance(rm_rows, list) and len(rm_rows) > 0,
        f"/api/monitor?city=HARTFORD returned {len(rm_rows) if isinstance(rm_rows, list) else 0} ranked landlords",
    )

    probe_upstream_sources(rec, code)


def verify_city_jurisdiction(rec: CheckRecorder, conn, api_base: str, j_cfg: Dict[str, Any]):
    code = j_cfg["code"]
    city = j_cfg["apiCity"]
    min_props = int(j_cfg.get("minProperties", 1000))
    min_nets = int(j_cfg.get("minNetworks", 500))
    db_metrics = snapshot_jurisdiction_metrics(conn, code, city)

    # 1. DB volume & integrity check
    rec.record(
        code,
        "data_integrity",
        f"{city}_db_volume",
        db_metrics["properties"] >= min_props and db_metrics["networks"] >= min_nets,
        f"properties={db_metrics['properties']:,} (>={min_props:,}), networks={db_metrics['networks']:,} (>={min_nets:,}), registrations={db_metrics['registrations']:,}",
        db_metrics,
    )

    # 2. `/api/{city}/stats` reality alignment against live PostgreSQL
    rs = requests.get(f"{api_base}/api/{city}/stats", timeout=60)
    stats = rs.json() if rs.status_code == 200 else {}
    api_props = int(stats.get("pluto_lots") if stats.get("pluto_lots") is not None else (stats.get("total_properties") or 0))
    api_nets = int(stats.get("networks") if stats.get("networks") is not None else (stats.get("total_networks") or 0))
    props_aligned = abs(api_props - db_metrics["properties"]) == 0
    nets_aligned = abs(api_nets - db_metrics["networks"]) == 0
    rec.record(
        code,
        "ui_reality",
        f"{city}_stats_matches_postgres",
        rs.status_code == 200 and props_aligned and nets_aligned,
        f"API stats ({api_props:,} lots / {api_nets:,} nets) vs DB ({db_metrics['properties']:,} lots / {db_metrics['networks']:,} nets)",
    )

    # 3. Top networks, unit reliability contract & institutional filter
    rn_priv = requests.get(
        f"{api_base}/api/{city}/networks",
        params={"limit": 15, "min_buildings": 2, "sort_by": "buildings", "include_institutional": "false"},
        timeout=20,
    )
    priv_rows = rn_priv.json() if rn_priv.status_code == 200 else []
    inst_leaks = [
        r["display_name"]
        for r in priv_rows
        if r.get("display_name") and INSTITUTIONAL_RE.search(r["display_name"])
    ]
    unit_contract_ok = True
    for r in priv_rows[:5]:
        if city in RELIABLE_UNIT_CITIES:
            if r.get("unit_count") is None:
                unit_contract_ok = False
        else:
            if r.get("unit_count") is not None:
                unit_contract_ok = False

    rec.record(
        code,
        "functional",
        f"{city}_top_networks_and_institutional_filter",
        rn_priv.status_code == 200 and len(priv_rows) > 0 and len(inst_leaks) == 0 and unit_contract_ok,
        f"Top private networks={len(priv_rows)}, institutional_leaks={len(inst_leaks)}, unit_contract_ok={unit_contract_ok}",
    )

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) AS tp_leaks
            FROM {city}_networks n
            WHERE EXISTS (
                SELECT 1
                FROM unnest(array_prepend(n.display_name, COALESCE(n.member_names, ARRAY[]::text[]))) AS nm
                WHERE COALESCE(nm, '') ~* %s
            )
            """,
            (THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN,),
        )
        city_tp_leaks = int(cur.fetchone()["tp_leaks"] or 0)
    rec.record(
        code,
        "untangling",
        f"{city}_third_party_exclusion_hygiene",
        city_tp_leaks == 0,
        f"{city}_networks 3rd-party bank/servicer/RA/institution leaks={city_tp_leaks} (expected 0)",
    )

    # Sort by code_cases when city has enforcement records
    if db_metrics.get("total_violations", 0) > 0:
        rn_code = requests.get(
            f"{api_base}/api/{city}/networks",
            params={"limit": 5, "min_buildings": 1, "sort_by": "code_cases", "include_institutional": "false"},
            timeout=20,
        )
        code_rows = rn_code.json() if rn_code.status_code == 200 else []
        counts = [int(r.get("total_violations") or 0) for r in code_rows]
        code_inst_leaks = [
            r["display_name"]
            for r in code_rows
            if r.get("display_name") and INSTITUTIONAL_RE.search(r["display_name"])
        ]
        sorted_desc = counts == sorted(counts, reverse=True) and (counts[0] > 0 if counts else False)
        rec.record(
            code,
            "functional",
            f"{city}_code_cases_ranking",
            rn_code.status_code == 200 and sorted_desc and len(code_inst_leaks) == 0,
            f"Top violation network counts={counts}, institutional_leaks={len(code_inst_leaks)}",
        )

    # 4. Network detail, zero duplicate-BBL multiplication, coordinates & untangling coherence
    if priv_rows:
        sample_net = priv_rows[0]
        net_key = sample_net["network_key"]
        rd = requests.get(f"{api_base}/api/{city}/network/{net_key}", timeout=25)
        nd = rd.json() if rd.status_code == 200 else {}
        props = nd.get("properties") or []
        unique_bbls = {p.get("bbl") for p in props if p.get("bbl")}
        expected_blds = int(nd.get("building_count") or sample_net.get("building_count") or 0)
        no_row_multiplication = len(props) == len(unique_bbls) and (len(props) == expected_blds or city == "nyc")

        # Coordinate bounding-box reality check when coordinates are present
        bounds = JURISDICTION_BOUNDS.get(code)
        coords_checked = 0
        out_of_bounds = 0
        if bounds:
            min_lat, max_lat, min_lon, max_lon = bounds
            for p in props:
                lat = p.get("latitude")
                lon = p.get("longitude")
                if lat is not None and lon is not None:
                    coords_checked += 1
                    if not (min_lat <= float(lat) <= max_lat and min_lon <= float(lon) <= max_lon):
                        out_of_bounds += 1
        coord_ok = (out_of_bounds == 0)

        p_status = nd.get("principal_status")
        p_human = nd.get("primary_human_name")
        identity_coherent = (p_status == "source_listed_person" and bool(p_human)) or (p_status == "unresolved_entity" and not p_human)

        rec.record(
            code,
            "ui_reality",
            f"{city}_network_detail_and_coordinate_reality",
            rd.status_code == 200 and no_row_multiplication and coord_ok and identity_coherent,
            f"network={net_key}: props={len(props)} (unique={len(unique_bbls)}, expected={expected_blds}), coords_checked={coords_checked} (out_of_bounds={out_of_bounds}), principal_status={p_status}",
        )

        # 5. Search & Property Detail check using real top network / parcel
        search_q = (sample_net.get("display_name") or "")[:18].strip()
        if len(search_q) >= 2:
            rsearch = requests.get(f"{api_base}/api/{city}/search", params={"q": search_q, "limit": 10}, timeout=15)
            s_rows = rsearch.json() if rsearch.status_code == 200 else []
            rec.record(
                code,
                "functional",
                f"{city}_search_owner_and_network",
                rsearch.status_code == 200 and len(s_rows) > 0,
                f"/api/{city}/search?q={search_q!r} -> {len(s_rows)} results",
            )

        if props:
            sample_bbl = props[0]["bbl"]
            rp = requests.get(f"{api_base}/api/{city}/property/{sample_bbl}", timeout=15)
            rec.record(
                code,
                "functional",
                f"{city}_property_detail",
                rp.status_code == 200 and rp.json().get("bbl") == sample_bbl,
                f"/api/{city}/property/{sample_bbl} -> HTTP {rp.status_code}",
            )
            if city == "baltimore":
                ro = requests.get(f"{api_base}/api/baltimore/official-records/{sample_bbl}", timeout=20)
                has_live_records = (
                    ro.status_code == 200
                    and sample_bbl in ro.text
                    and "official record" in ro.text.lower()
                    and "Official source query failed" not in ro.text
                )
                rec.record(
                    code,
                    "ui_reality",
                    "baltimore_official_records_embed",
                    has_live_records,
                    f"/api/baltimore/official-records/{sample_bbl} -> HTTP {ro.status_code} ({len(ro.text)} bytes, live_records={has_live_records})",
                )

    # 6. Jurisdiction-specific untangling & anchor checks
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if code == "NYC":
            cur.execute(
                """
                SELECT COUNT(*) AS overlap
                FROM (SELECT unnest(bbl_list) AS bbl FROM nyc_networks WHERE UPPER(display_name) LIKE '%SPELIOTIS%') s
                JOIN (SELECT unnest(bbl_list) AS bbl FROM nyc_networks WHERE UPPER(display_name) LIKE '%BURGESS%') b
                  ON s.bbl = b.bbl
                """
            )
            overlap = int(cur.fetchone()["overlap"] or 0)
            cur.execute("SELECT to_regclass('public.nyc_dos_entities') AS tbl")
            has_dos_tbl = bool(cur.fetchone()["tbl"])
            dos_cnt = 0
            if has_dos_tbl:
                cur.execute("SELECT COUNT(*) AS cnt FROM nyc_dos_entities")
                dos_cnt = int(cur.fetchone()["cnt"] or 0)
            rec.record(
                code,
                "untangling",
                "nyc_speliotis_burgess_isolation_and_ny_dos",
                overlap == 0 and db_metrics["max_network_size"] <= 350 and dos_cnt > 0,
                f"Speliotis/Burgess overlap={overlap} (expected 0), max_network={db_metrics['max_network_size']}, nyc_dos_entities={dos_cnt:,}",
            )
        elif code == "DC":
            cur.execute(
                """
                SELECT COUNT(*) AS human_nets
                FROM dc_networks
                WHERE connection_signals::jsonb ->> 'principal_status' = 'source_listed_person'
                """
            )
            human_nets = int(cur.fetchone()["human_nets"] or 0)
            rec.record(
                code,
                "untangling",
                "dc_dlcp_rental_license_untangling",
                db_metrics["registrations"] >= 5000 and db_metrics["contacts"] >= 5000 and human_nets >= 1000,
                f"dc_registrations={db_metrics['registrations']:,}, dc_contacts={db_metrics['contacts']:,}, human_attributed_networks={human_nets:,}",
            )
        elif code == "BALTIMORE":
            rec.record(
                code,
                "untangling",
                "baltimore_dhcd_registrations_and_enforcement",
                db_metrics["registrations"] >= 10000 and db_metrics["bbl_stats_rows"] >= 10000,
                f"baltimore_registrations={db_metrics['registrations']:,}, bbl_stats_rows={db_metrics['bbl_stats_rows']:,}",
            )
        elif code == "MINNEAPOLIS":
            rec.record(
                code,
                "untangling",
                "minneapolis_full_licenses_and_violations",
                db_metrics["properties"] >= 18000 and db_metrics["total_violations"] >= 1000,
                f"minneapolis_properties={db_metrics['properties']:,}, total_violations={db_metrics['total_violations']:,}",
            )

    probe_upstream_sources(rec, code)


def verify_ct_municipal_cohort(rec: CheckRecorder, conn, api_base: str, j_cfg: Dict[str, Any]):
    code = j_cfg["code"]
    towns = [t.upper() for t in (j_cfg.get("sampleTowns") or [])]

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                source_name,
                refresh_status,
                last_refreshed_at,
                (
                    LOWER(COALESCE(refresh_status, '')) = 'running'
                    AND last_refreshed_at < NOW() - INTERVAL '12 hours'
                ) AS is_stuck_running
            FROM data_source_status
            WHERE UPPER(source_name) = ANY(%s)
            """,
            (towns,),
        )
        status_rows = {r["source_name"].upper(): r for r in cur.fetchall()}

    stuck_running = [
        t for t, r in status_rows.items()
        if bool(r.get("is_stuck_running"))
    ]
    rec.record(
        code,
        "freshness",
        f"{code.lower()}_no_stuck_running_locks",
        len(stuck_running) == 0,
        f"Audited {len(towns)} CT towns ({', '.join(towns)}); stuck_running={stuck_running}",
    )

    # Verify insights & dashboard summary work for sample CT municipalities
    for town in towns[:4]:
        ri = requests.get(f"{api_base}/api/insights", params={"city": town}, timeout=60)
        ok = ri.status_code == 200
        rec.record(
            code,
            "functional",
            f"ct_muni_insights:{town}",
            ok,
            f"GET /api/insights?city={town} -> HTTP {ri.status_code}",
        )


def write_markdown_report(summary: Dict[str, Any], cohort: Dict[str, Any]) -> str:
    date_prefix = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    slug = cohort["id"]
    report_dir = os.path.join(REPO_ROOT, "docs", "benchmarks")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{date_prefix}-{slug}-report.md")

    lines = [
        f"# Jurisdiction Optimization Report — `{cohort['id']}`",
        "",
        f"- **Generated At**: `{summary['generated_at']}`",
        f"- **Overall Status**: **{summary['overall_status']}** (`{summary['passed_checks']}/{summary['total_checks']}` checks passed)",
        f"- **Jurisdictions Verified**: `{', '.join(summary['jurisdictions_verified'])}`",
        f"- **Self-Heal Actions Executed**: `{len(summary['healed_feeds'])}`",
        "",
        "## 1. Self-Healing & Upstream Pipeline Actions",
        "",
    ]
    if summary["healed_feeds"]:
        for h in summary["healed_feeds"]:
            lines.append(f"- **`{h.get('feed') or h.get('action')}`** (`{h.get('jurisdiction', 'GLOBAL')}`): `success={h.get('success')}`")
    else:
        lines.append("- No self-healing interventions needed; all target feeds were already healthy.")

    lines.extend([
        "",
        "## 2. Verification Matrix",
        "",
        "| Jurisdiction | Category | Check | Status | Details |",
        "|---|---|---|---|---|",
    ])
    for chk in summary["checks"]:
        badge = "PASS" if chk["status"] == "PASS" else "FAIL"
        safe_details = str(chk["details"]).replace("|", "\\|")
        lines.append(f"| `{chk['jurisdiction']}` | `{chk['category']}` | `{chk['name']}` | **{badge}** | {safe_details} |")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    return os.path.relpath(report_path, REPO_ROOT)


def dispatch_digest_email(summary: Dict[str, Any], cohort: Dict[str, Any]) -> bool:
    try:
        from updater.send_audit_email import send_audit_email
        emoji = "✅" if summary["overall_status"] == "PASS" else "⚠️"
        subj = (
            f"[They Own WHAT?] {emoji} Jurisdiction Optimization ({cohort['id']}) — "
            f"{summary['passed_checks']}/{summary['total_checks']} Passed"
        )
        text_lines = [
            f"They Own WHAT?? — Scheduled Jurisdiction Optimization Report ({cohort['id']})",
            f"Generated: {summary['generated_at']}",
            f"Status: {summary['overall_status']} ({summary['passed_checks']}/{summary['total_checks']} checks passed)",
            f"Jurisdictions: {', '.join(summary['jurisdictions_verified'])}",
            "",
            "Checks:",
        ]
        for c in summary["checks"]:
            text_lines.append(f"  [{c['status']}] {c['jurisdiction']} / {c['category']} / {c['name']}: {c['details']}")

        rows_html = "".join(
            f"<tr><td><code>{c['jurisdiction']}</code></td><td>{c['category']}</td>"
            f"<td><code>{c['name']}</code></td>"
            f"<td style='color:{'#059669' if c['status'] == 'PASS' else '#dc2626'};font-weight:bold;'>{c['status']}</td>"
            f"<td>{c['details']}</td></tr>"
            for c in summary["checks"]
        )
        html_body = f"""
        <h2>They Own WHAT?? — Jurisdiction Optimization Report (<code>{cohort['id']}</code>)</h2>
        <p><strong>Timestamp:</strong> {summary['generated_at']}<br/>
        <strong>Status:</strong> {summary['overall_status']} ({summary['passed_checks']}/{summary['total_checks']} checks passed)<br/>
        <strong>Jurisdictions:</strong> {', '.join(summary['jurisdictions_verified'])}</p>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:sans-serif;font-size:12px;width:100%;">
          <thead style="background:#f1f5f9;">
            <tr><th>Jurisdiction</th><th>Category</th><th>Check</th><th>Status</th><th>Details</th></tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
        """
        return bool(send_audit_email(subj, "\n".join(text_lines), html_body))
    except Exception as exc:
        logger.warning("Could not send optimization digest email: %s", exc)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Run They Own WHAT?? jurisdiction optimization battery.")
    sub = parser.add_subparsers(dest="subcommand")

    plan_p = sub.add_parser("plan", help="Show the planned battery for the due cohort")
    plan_p.add_argument("--manifest", default=DEFAULT_MANIFEST_PATH)
    plan_p.add_argument("--json", action="store_true")

    run_p = sub.add_parser("run", help="Execute the functional + data/untangling battery")
    run_p.add_argument("--manifest", default=DEFAULT_MANIFEST_PATH)
    run_p.add_argument("--cohort", default="due", help="Cohort ID, 'due', or 'all'")
    run_p.add_argument("--heal", action="store_true", help="Run source-only self-healing for failed/stale cohort feeds before verification")
    run_p.add_argument("--output", default="/tmp/tow-battery-results/summary.json", help="Output summary JSON path")
    run_p.add_argument("--write-report", action="store_true", help="Write dated Markdown benchmark report in docs/benchmarks/")
    run_p.add_argument("--advance", action="store_true", help="Advance jurisdiction-rotation.json when battery passes")
    run_p.add_argument("--force-advance", action="store_true", help="Allow ledger advance even before minDaysBetweenAdvances")
    run_p.add_argument("--if-due", action="store_true", help="Exit 0 cleanly if the next cohort is not yet due")
    run_p.add_argument("--send-email", action="store_true", help="Dispatch summary digest email via SMTP")

    args = parser.parse_args()
    manifest = load_manifest(getattr(args, "manifest", DEFAULT_MANIFEST_PATH))
    status = compute_status(manifest)

    if args.subcommand == "plan" or not args.subcommand:
        due_cohort = manifest["cohorts"][manifest["nextCohortIndex"]]
        plan_payload = {"rotationStatus": status, "plannedCohort": due_cohort}
        if getattr(args, "json", False):
            print(json.dumps(plan_payload, indent=2))
        else:
            print(f"Planned Cohort : {due_cohort['id']} ({due_cohort['label']})")
            print(f"Cadence        : Cycle {status['currentCycle']} ({status['frequency']}) | Due Now: {status['isDue']}")
            for j in due_cohort["jurisdictions"]:
                print(f"  - {j['code']}: {j['name']}")
        return 0

    if args.if_due and args.cohort == "due" and not status["isDue"]:
        logger.info("⏭ Next cohort %s is not due until %s — nothing to run.", status["nextCohortId"], status["nextDueAt"])
        return 0

    cohorts_by_id = {c["id"]: c for c in manifest["cohorts"]}
    if args.cohort == "due":
        target_cohorts = [manifest["cohorts"][manifest["nextCohortIndex"]]]
    elif args.cohort == "all":
        target_cohorts = manifest["cohorts"]
    elif args.cohort in cohorts_by_id:
        target_cohorts = [cohorts_by_id[args.cohort]]
    else:
        print(f"❌ Unknown cohort {args.cohort!r}. Choices: due, all, {', '.join(cohorts_by_id)}", file=sys.stderr)
        return 1

    conn, db_url = resolve_db_connection()
    api_base = resolve_http_base("API_BASE_URL", ["http://127.0.0.1:8000", "http://ctdata_api:8000", "http://api:8000"])
    nginx_base = resolve_http_base("NGINX_BASE_URL", ["http://127.0.0.1:6262", "http://ctdata_nginx:80", "http://nginx:80"])
    if not api_base:
        conn.close()
        print("❌ Live FastAPI service is not reachable on :8000 or http://api:8000", file=sys.stderr)
        return 1

    start_ts = time.time()
    rec = CheckRecorder()
    healed_feeds: List[Dict[str, Any]] = []
    verified_codes: List[str] = []

    try:
        if args.heal:
            for cohort in target_cohorts:
                healed_feeds.extend(heal_cohort_if_needed(conn, db_url, cohort))

        verify_global_platform(rec, api_base, nginx_base)

        for cohort in target_cohorts:
            for j_cfg in cohort["jurisdictions"]:
                kind = j_cfg.get("kind")
                if kind == "statewide":
                    verify_ct_statewide(rec, conn, api_base, j_cfg)
                elif kind == "city":
                    verify_city_jurisdiction(rec, conn, api_base, j_cfg)
                elif kind == "ct_municipal_cohort":
                    verify_ct_municipal_cohort(rec, conn, api_base, j_cfg)
                verified_codes.append(j_cfg["code"])
    finally:
        conn.close()

    passed = sum(1 for c in rec.checks if c["status"] == "PASS")
    failed = sum(1 for c in rec.checks if c["status"] != "PASS")
    all_ok = failed == 0 and len(rec.checks) > 0

    primary_cohort = target_cohorts[0]
    summary: Dict[str, Any] = {
        "cohort_id": primary_cohort["id"] if len(target_cohorts) == 1 else "all",
        "cohort_label": primary_cohort["label"] if len(target_cohorts) == 1 else "Full Multi-Cohort Sweep",
        "generated_at": format_iso_utc(utc_now()),
        "duration_seconds": round(time.time() - start_ts, 2),
        "overall_status": "PASS" if all_ok else "FAIL",
        "all_checks_passed": all_ok,
        "total_checks": len(rec.checks),
        "passed_checks": passed,
        "failed_checks": failed,
        "jurisdictions_verified": verified_codes,
        "healed_feeds": healed_feeds,
        "checks": rec.checks,
    }

    if args.write_report and len(target_cohorts) == 1:
        summary["report_path"] = write_markdown_report(summary, primary_cohort)

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
            f.write("\n")
        logger.info("📄 Wrote battery summary to %s", args.output)

    if args.advance and len(target_cohorts) == 1:
        if all_ok:
            updated, entry = advance_manifest(
                manifest,
                summary,
                report_path=summary.get("report_path"),
                force=args.force_advance,
            )
            save_manifest(updated, args.manifest)
            if args.manifest == DEFAULT_MANIFEST_PATH:
                update_latest_automation_status(updated, entry)
            logger.info("✅ Advanced rotation ledger for %s", primary_cohort["id"])
        else:
            logger.error("❌ Battery had %d failure(s); rotation ledger NOT advanced.", failed)

    if args.send_email:
        dispatch_digest_email(summary, primary_cohort)

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
