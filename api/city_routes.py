"""
api/city_routes.py
==================
FastAPI routes for multi-city network data (NYC, DC, Baltimore, LA).
"""

import datetime as dt
import time
import json
import logging
import html
import re
from urllib.parse import urlencode
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
import psycopg2
from psycopg2.extras import RealDictCursor
import requests

import api.db as db_module
from api.network_profile import (
    dedupe_human_name_list,
    dedupe_network_contacts,
    derive_network_business_profile,
    fetch_batch_network_oc_profiles,
)
from api.shared_utils import (
    THIRD_PARTY_ENTITY_REGEX_BODY,
    THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    looks_like_person_owner,
    normalize_business_name,
)

logger = logging.getLogger("city-api")

router = APIRouter(prefix="/api/{city}", tags=["city"])

VALID_CITIES = {
    "nyc", "dc", "baltimore", "boston", "detroit", "philadelphia",
    "chicago", "miami", "minneapolis", "nj",
    "ma", "fl", "ny", "il", "ca", "co", "vt", "wi", "nc", "ar", "va", "mt", "mn", "oh",
}
RELIABLE_UNIT_CITIES = {"nyc", "baltimore", "philadelphia", "miami", "minneapolis", "nj", "fl", "ny", "ca", "mn"}
# Python `re` pattern (\b word boundaries) exported for Python callers & battery checks;
# PostgreSQL `!~*` queries use INSTITUTIONAL_NETWORK_SQL_PATTERN (\y word boundaries).
INSTITUTIONAL_NETWORK_PATTERN = THIRD_PARTY_ENTITY_REGEX_BODY
INSTITUTIONAL_NETWORK_SQL_PATTERN = THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN
BALTIMORE_PROPERTY_LAYER_URL = "https://baltegis.baltimorecity.gov/mapping/rest/services/CityView/RealProperty_OB/FeatureServer/0/query"
NYC_HPD_VIOLATIONS_URL = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"
NYC_HPD_VIOLATIONS_PAGE = "https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5"
NYC_HPD_CLASS_LABELS = {
    "A": "Non-hazardous",
    "B": "Hazardous",
    "C": "Immediately hazardous",
}
BALTIMORE_OFFICIAL_LAYERS = [
    {
        "label": "Property",
        "url": BALTIMORE_PROPERTY_LAYER_URL,
        "key_field": "BLOCKLOT",
        "date_field": None,
        "fields": ["BLOCKLOT", "FULLADDR", "OWNER_1", "OWNER_2", "USEGROUP", "SALEDATE", "SALEPRIC"],
    },
    {
        "label": "DHCD Rental Registration & License",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/OpenGov_RegAndLicense/MapServer/0/query",
        "key_field": "BlockLot",
        "date_field": "LicenseIssuedDate",
        "fields": ["RecordNUMBER", "LicenceOrRegistration", "LicenseIssuedDate", "LicenseExpirationDate", "DwellingCount", "Address"],
    },
    {
        "label": "FTA Citations",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/11/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "fields": ["CitationNum", "DateNotice", "Title", "ViolationText", "TotalFine", "CitationStatus"],
    },
    {
        "label": "Open Interior Notices",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/13/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "fields": ["NoticeNum", "DateNotice", "NoticeType", "Status", "Address"],
    },
    {
        "label": "Open Exterior Notices",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/14/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "fields": ["NoticeNum", "DateNotice", "NoticeType", "Status", "Address"],
    },
    {
        "label": "Open Interior/Exterior Notices",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/15/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "fields": ["NoticeNum", "DateNotice", "NoticeType", "Status", "Address"],
    },
    {
        "label": "Open Cleaning Work Orders",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/18/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Cleaning Work Orders 31-60 Days",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/19/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Cleaning Work Orders 61-90 Days",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/20/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Cleaning Work Orders 90+ Days",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/21/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Boarding Work Orders 1-5 Days",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/23/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Boarding Work Orders 6-10 Days",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/24/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Boarding Work Orders 11+ Days",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/25/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Vacant Building Notices",
        "url": "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/DHCD_Open_Baltimore_Datasets/FeatureServer/1/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateNotice",
        "fields": ["NoticeNum", "DateNotice", "NT", "Address", "DateCancel", "DateAbate"],
    },
]

def _validate_city(city: str):
    c = city.lower().strip()
    if c not in VALID_CITIES:
        raise HTTPException(status_code=400, detail=f"Unsupported city: {city}")
    return c

def _query(sql: str, params=None) -> list[dict]:
    conn = db_module.db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            rows = [dict(r) for r in cur.fetchall()]
        conn.commit()
        return rows
    except psycopg2.errors.UndefinedTable as exc:
        conn.rollback()
        raise HTTPException(status_code=404, detail="Dataset tables are not yet loaded") from exc
    except Exception:
        conn.rollback()
        raise
    finally:
        db_module.db_pool.putconn(conn)

_PLACEHOLDER_OWNER_SQL_PATTERN = (
    r"^\s*[*#_~\-\s]*(?:"
    r"UNAVAILABLE(?:\s+OWNERS?)?|OWNERS?\s+UNAVAILABLE|"
    r"(?:NAME|OWNER|OWNER\s+NAME|NO\s+OWNER|NO\s+NAME)\s+NOT\s+ON\s+FILE|NOT\s+ON\s+FILE|"
    r"(?:CURRENT\s+)?OWNERS?\s+OF(?:\s+RECORD)?|RECORD\s+OWNER|CURRENT\s+OWNER|PRESENT\s+OWNER|"
    r"(?:NAME|OWNER)\s+NOT\s+(?:AVAILABLE|PROVIDED|LISTED)|NOT\s+(?:AVAILABLE|PROVIDED|LISTED)|"
    r"UNKNOWN(?:\s+OWNERS?|\s+HEIRS(?:\s+OF)?)?|OWNER\s+UNKNOWN|"
    r"CONFIDENTIAL(?:\s+OWNERS?)?|REDACTED|WITHHELD|NAME\s+WITHHELD|PRIVATE\s+OWNERS?"
    r")[*#_~\-\s]*$"
)


def _institutional_filter(include_institutional: bool, column: str = "display_name") -> tuple[str, list]:
    display_col = "n.display_name" if "n.display_name" in column else column
    if include_institutional:
        return f" AND COALESCE({display_col}, '') !~* %s", [_PLACEHOLDER_OWNER_SQL_PATTERN]
    return (
        f" AND COALESCE({column}, '') !~* %s AND COALESCE({display_col}, '') !~* %s",
        [INSTITUTIONAL_NETWORK_SQL_PATTERN, _PLACEHOLDER_OWNER_SQL_PATTERN],
    )

_PROP_COLUMNS_CACHE: dict[str, tuple[float, set[str]]] = {}
_NET_COLUMNS_CACHE: dict[str, tuple[float, set[str]]] = {}


def _get_property_columns(city: str) -> set[str]:
    now = time.time()
    cached = _PROP_COLUMNS_CACHE.get(city)
    if cached and now - cached[0] < 120:
        return cached[1]
    rows = _query(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (f"{city}_properties",),
    )
    cols = {r["column_name"] for r in rows}
    _PROP_COLUMNS_CACHE[city] = (now, cols)
    return cols


def _get_network_columns(city: str) -> set[str]:
    now = time.time()
    cached = _NET_COLUMNS_CACHE.get(city)
    if cached and now - cached[0] < 120:
        return cached[1]
    rows = _query(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (f"{city}_networks",),
    )
    cols = {r["column_name"] for r in rows}
    _NET_COLUMNS_CACHE[city] = (now, cols)
    return cols


def _load_cached_representative_profiles(city: str, network_keys: list[str]) -> dict[str, dict]:
    if not network_keys:
        return {}
    try:
        rows = _query(
            """
            SELECT
                network_key,
                operator_name,
                operator_source,
                operator_basis,
                operating_entities,
                primary_hq_address,
                hq_entity_count,
                business_count,
                representative_human_name,
                key_officers,
                officer_count,
                sample_entities,
                external_source_url,
                external_source_label
            FROM network_representative_profiles
            WHERE jurisdiction = %s
              AND network_key = ANY(%s)
            """,
            (city, list(dict.fromkeys(network_keys))),
        )
        out: dict[str, dict] = {}
        for r in rows:
            out[r["network_key"]] = {
                "operator_name": r.get("operator_name"),
                "operator_source": r.get("operator_source"),
                "operator_basis": r.get("operator_basis"),
                "operating_entities": _parse_jsonish(r.get("operating_entities"), []),
                "primary_hq_address": r.get("primary_hq_address"),
                "hq_entity_count": r.get("hq_entity_count"),
                "business_count": r.get("business_count"),
                "representative_human_name": r.get("representative_human_name"),
                "key_officers": _parse_jsonish(r.get("key_officers"), []),
                "officer_count": int(r.get("officer_count") or 0),
                "sample_entities": _parse_jsonish(r.get("sample_entities"), []),
                "external_source_url": r.get("external_source_url"),
                "external_source_label": r.get("external_source_label"),
            }
        return out
    except Exception:
        return {}


def _resolve_network_business_profiles(city: str, rows: list[dict], compute_missing: bool = True) -> dict[str, dict]:
    """Return cached representative business profiles, computing deterministic profiles live for any uncached rows."""
    if not rows:
        return {}
    keys = [r["network_key"] for r in rows if r.get("network_key")]
    profiles = _load_cached_representative_profiles(city, keys)
    if not compute_missing:
        return profiles

    missing_rows = []
    for r in rows:
        nk = r.get("network_key")
        if nk and nk not in profiles:
            sigs = _parse_jsonish(r.get("connection_signals"), {})
            missing_rows.append({**r, "connection_signals": sigs})

    if missing_rows:
        oc_enr = fetch_batch_network_oc_profiles(_query, city, missing_rows)
        for r in missing_rows:
            nk = r["network_key"]
            profiles[nk] = derive_network_business_profile(
                r.get("display_name"),
                r.get("member_names") or [],
                r.get("member_addresses") or [],
                r.get("connection_signals") or {},
                oc_enr.get(nk),
            )
    return profiles


def _latest_registration_join(city: str, property_alias: str = "p", alias: str = "r") -> str:
    # Always select at most 1 registration row per parcel so multi-license / renewed
    # registration feeds (NYC, DC, Baltimore, Detroit, Minneapolis) never multiply parcel rows.
    return f"""
    LEFT JOIN LATERAL (
        SELECT
            r_latest.registration_id,
            r_latest.last_registration_date,
            r_latest.registration_end_date
        FROM {city}_hpd_registrations r_latest
        WHERE r_latest.bbl = {property_alias}.bbl
        ORDER BY
            r_latest.registration_end_date DESC NULLS LAST,
            r_latest.last_registration_date DESC NULLS LAST,
            r_latest.registration_id DESC
        LIMIT 1
    ) {alias} ON TRUE
    """

def _parse_jsonish(value, fallback=None):
    if fallback is None:
        fallback = {}
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return fallback
    return fallback


def _network_identity(display_name, member_names, connection_signals=None):
    signals = _parse_jsonish(connection_signals, {})
    source_human_names = dedupe_human_name_list(
        [name for name in (signals.get("source_human_names") or []) if name],
        max_names=25,
    )
    if not source_human_names:
        source_human_names = dedupe_human_name_list(
            sorted({
                name for name in (member_names or [])
                if looks_like_person_owner(name)
            }),
            max_names=25,
        )
    primary_human_name = signals.get("primary_human_name")
    if not primary_human_name and looks_like_person_owner(display_name):
        primary_human_name = display_name
    if not primary_human_name and source_human_names:
        primary_human_name = source_human_names[0]
    return {
        "primary_human_name": primary_human_name,
        "source_human_names": source_human_names[:25],
        "principal_status": "source_listed_person" if primary_human_name else "unresolved_entity",
        "registered_entity_name": display_name if primary_human_name and display_name != primary_human_name else None,
    }


def _soql_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _nyc_open_data_url(params: dict) -> str:
    return f"{NYC_HPD_VIOLATIONS_URL}?{urlencode(params)}"


def _hpd_profile_url(registration_id: Optional[str]) -> Optional[str]:
    if not registration_id:
        return None
    return f"https://hpdonline.nyc.gov/hpdonline/building-profile?registrationId={registration_id}"


def _shorten_complaint_type(value: Optional[str], max_len: int = 180) -> str:
    text = " ".join((value or "Unspecified HPD violation").split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "..."


def _city_refresh_snapshot(city: str) -> dict:
    prefix = city.upper()
    rows = _query("""
        SELECT
            source_name,
            refresh_status,
            last_refreshed_at,
            details,
            (
                LOWER(COALESCE(refresh_status, '')) = 'running'
                AND last_refreshed_at > NOW() - INTERVAL '6 hours'
            ) AS is_active
        FROM data_source_status
        WHERE source_name = %s OR source_name LIKE %s
        ORDER BY
            CASE WHEN LOWER(COALESCE(refresh_status, '')) = 'running' THEN 0 ELSE 1 END,
            last_refreshed_at DESC NULLS LAST
    """, (prefix, f"{prefix}_%"))

    active = []
    latest = rows[0] if rows else {}
    for row in rows:
        if not row.get("is_active"):
            continue
        details = _parse_jsonish(row.get("details"), {})
        active.append({
            "source_name": row.get("source_name"),
            "status": row.get("refresh_status"),
            "message": details.get("message") if isinstance(details, dict) else None,
            "last_refreshed_at": str(row["last_refreshed_at"]) if row.get("last_refreshed_at") else None,
        })

    return {
        "refresh_status": "running" if active else (latest.get("refresh_status") or "unknown"),
        "is_refreshing": bool(active),
        "active_refreshes": active,
        "last_refreshed_at": str(latest["last_refreshed_at"]) if latest.get("last_refreshed_at") else None,
    }


# ---------------------------------------------------------------------------
# /api/{city}/stats  — platform-level summary (24h persisted kv_cache + L1 cache)
# ---------------------------------------------------------------------------
import threading

_stats_cache: dict = {}
_stats_ts: dict = {}
_networks_cache: dict = {}
_networks_ts: dict = {}
_bg_refresh_inflight: set[str] = set()
_bg_refresh_lock = threading.Lock()
_city_stats_locks: dict[str, threading.Lock] = {}
STATS_TTL = 86400  # 24 hours (persisted in kv_cache)
L1_MEM_TTL = 15    # 15s L1 RAM cache so external kv_cache invalidations propagate immediately


def _kv_get(cache_key: str) -> tuple[dict | list | None, bool]:
    try:
        rows = _query(
            """
            SELECT value, (created_at > NOW() - INTERVAL '24 hours') AS is_fresh
            FROM kv_cache
            WHERE key = %s
            """,
            (cache_key,),
        )
        if rows and rows[0].get("value") is not None:
            return rows[0]["value"], bool(rows[0].get("is_fresh"))
    except Exception:
        pass
    return None, False


def _kv_set(cache_key: str, payload) -> None:
    try:
        conn = db_module.db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS kv_cache (
                        key text PRIMARY KEY,
                        value jsonb NOT NULL,
                        created_at timestamptz NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO kv_cache (key, value, created_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (key) DO UPDATE SET
                        value = EXCLUDED.value,
                        created_at = NOW()
                    """,
                    (cache_key, json.dumps(payload, default=str)),
                )
            conn.commit()
        finally:
            db_module.db_pool.putconn(conn)
    except Exception as exc:
        logger.debug("kv_cache write skipped for %s: %s", cache_key, exc)


def _spawn_kv_refresh(cache_key: str, fn) -> None:
    with _bg_refresh_lock:
        if cache_key in _bg_refresh_inflight:
            return
        _bg_refresh_inflight.add(cache_key)

    def _runner():
        try:
            fn()
        except Exception as exc:
            logger.warning("Background city cache refresh failed for %s: %s", cache_key, exc)
        finally:
            with _bg_refresh_lock:
                _bg_refresh_inflight.discard(cache_key)

    threading.Thread(target=_runner, daemon=True).start()


@router.get("/stats")
def city_stats(city: str, force_refresh: bool = False):
    c = _validate_city(city)
    global _stats_cache, _stats_ts
    cache_key = f"city_stats:v1:{c}"
    now = time.time()
    if not force_refresh:
        if cache_key in _stats_cache and cache_key in _stats_ts and now - _stats_ts[cache_key] < L1_MEM_TTL:
            return _stats_cache[cache_key]
        kv_val, is_fresh = _kv_get(cache_key)
        if kv_val is not None:
            _stats_cache[cache_key] = kv_val
            _stats_ts[cache_key] = now
            if not is_fresh:
                _spawn_kv_refresh(cache_key, lambda: city_stats(c, force_refresh=True))
            return kv_val

    with _bg_refresh_lock:
        city_lock = _city_stats_locks.setdefault(c, threading.Lock())

    with city_lock:
        if not force_refresh:
            now = time.time()
            if cache_key in _stats_cache and cache_key in _stats_ts and now - _stats_ts[cache_key] < L1_MEM_TTL:
                return _stats_cache[cache_key]
            kv_val, is_fresh = _kv_get(cache_key)
            if kv_val is not None:
                _stats_cache[cache_key] = kv_val
                _stats_ts[cache_key] = now
                return kv_val

    # Dynamically inject table names based on whitelisted city
    rows = _query(f"""
        SELECT
            COUNT(*) FILTER (WHERE COALESCE(building_count, 0) > 0)
                                                            AS networks,
            SUM(building_count)                             AS buildings,
            SUM(unit_count)                                 AS units,
            COUNT(*) FILTER (WHERE COALESCE(building_count, 0) > 10)
                                                            AS large_networks,
            MAX(building_count)                             AS largest_network,
            MAX(unit_count)                                 AS most_units
        FROM {c}_networks
    """)

    reg_row = _query(f"SELECT COUNT(*) AS registrations FROM {c}_hpd_registrations")
    prop_row = _query(f"SELECT COUNT(*) AS lots FROM {c}_properties")
    biz_row = _query(
        f"""
        SELECT COUNT(DISTINCT UPPER(TRIM(m)))::int AS businesses
        FROM {c}_networks n
        CROSS JOIN LATERAL UNNEST(n.member_names) AS m
        WHERE COALESCE(n.building_count, 0) > 0
          AND m ~* %s
        """,
        (
            r"(LLC|L\.L\.C|INC|CORP|CORPORATION|LTD|LP|L\.P|LLP|HOLDINGS|REALTY|PROPERTIES|PROPERTY|"
            r"ASSOCIATES|ASSOC|PARTNERS|PARTNERSHIP|VENTURES|TRUST|TRUSTEE|COMPANY|CO\y|ENTERPRISES|"
            r"GROUP|MANAGEMENT|INVESTMENTS|INVESTMENT|DEVELOPMENT|EQUITY|CAPITAL|BORROWER|OWNER|"
            r"ESTATES|FUND|APARTMENTS|HOMES|HOUSING|RESIDENTIAL)",
        ),
    )

    status_rows = _query("""
        SELECT last_refreshed_at, external_last_updated
        FROM data_source_status
        WHERE source_name = %s
        LIMIT 1
    """, (c.upper(),))
    status = status_rows[0] if status_rows else {}
    last_updated = status.get("external_last_updated") or status.get("last_refreshed_at")
    refresh_snapshot = _city_refresh_snapshot(c)

    # City-specific display details
    city_display_names = {
        "nyc": "NYC HPD Multiple Dwelling Registrations + PLUTO",
        "dc": "D.C. GIS Computer Assisted Mass Appraisal",
        "baltimore": "Baltimore City GIS Ownership Records",
        "philadelphia": "Philadelphia OPA Assessments + L&I Active Rental Licenses + PA DOS",
        "boston": "Boston Property Assessment Roll",
        "detroit": "Detroit Property Assessment Roll",
        "chicago": "Cook County Assessor Parcels (City of Chicago)",
        "miami": "Miami-Dade Parcels + Florida Sunbiz + DBPR Apartment Licenses",
        "minneapolis": "Minneapolis Active Rental Licenses + MapIT GIS",
        "nj": "New Jersey DCA BHI Active Building Records",
        "ma": "MassGIS Level 3 Property Tax Parcels + 112 Municipal CAMA Scrapers (All 351 Municipalities)",
        "fl": "Florida Unified 67-County Parcels (FDOR + Miami-Dade PA + Hillsborough HCPA) + Sunbiz + DBPR Ch. 509",
        "ny": "New York Unified 62-County Explorer (NYC PLUTO/HPD/Marshal Evictions + NYS ORPTS + Buffalo/Syracuse + SDG ImageMate)",
        "il": "Illinois Statewide Explorer (Cook County 1.67M PINs + DuPage GIS + Chicago DOB Violations + Devnet/Beacon CAMA)",
        "ca": "California 58-County Explorer (LA & SF Rolls + Registered Lessor Businesses + Evictions/Violations + CA SOS)",
        "co": "Colorado OIT Public Parcels + CO SOS + Denver Active Rental Licenses",
        "vt": "Vermont VCGI Standardized Parcels + Statewide Grand List",
        "wi": "Wisconsin SCO/DOA Statewide Parcels (Class 1 Residential & Class 2 Multifamily)",
        "nc": "North Carolina NC OneMap Parcels (All 100 Counties)",
        "ar": "Arkansas GIS Office Statewide Parcels (All 75 Counties) + AR SOS",
        "va": "Virginia VGIN Local Schema Parcels + HRGeo + County CAMA + VA SCC",
        "mt": "Montana MSDI / DOR ORION Cadastral Parcels (All 56 Counties) + MT SOS",
        "mn": "Minnesota Unified Explorer (Hennepin & Ramsey CAMA + Minneapolis Active Rental Licenses) + MN SOS",
        "oh": "Ohio County Auditor CAMA Parcels (Cuyahoga / Greater Cleveland) + OH SOS",
    }

    result = {
        "networks":        int(rows[0]["networks"]  or 0),
        "businesses":      int(biz_row[0]["businesses"] or 0) if biz_row else 0,
        "buildings":       int(rows[0]["buildings"] or 0),
        "units":           int(rows[0]["units"] or 0) if c in RELIABLE_UNIT_CITIES else None,
        "unit_data_available": c in RELIABLE_UNIT_CITIES,
        "large_networks":  int(rows[0]["large_networks"] or 0),
        "largest_network": int(rows[0]["largest_network"] or 0),
        "most_units":      int(rows[0]["most_units"] or 0) if c in RELIABLE_UNIT_CITIES else None,
        "registrations":   int(reg_row[0]["registrations"] or 0),
        "pluto_lots":      int(prop_row[0]["lots"] or 0),
        "data_source":     city_display_names.get(c, "Property Registrations"),
        "last_updated":    str(last_updated) if last_updated else None,
        "refresh_status":  refresh_snapshot["refresh_status"],
        "is_refreshing":   refresh_snapshot["is_refreshing"],
        "active_refreshes": refresh_snapshot["active_refreshes"],
        "eviction_data":   _city_eviction_summary(c),
        "code_data":       _city_code_summary(c),
    }

    _stats_cache[cache_key] = result
    _stats_ts[cache_key] = time.time()
    _kv_set(cache_key, result)
    return result


def _city_eviction_summary(city: str) -> dict:
    source_name = f"{city.upper()}_EVICTIONS"
    status_rows = _query("""
        SELECT external_last_updated, last_refreshed_at, refresh_status, details
        FROM data_source_status
        WHERE source_name = %s
        LIMIT 1
    """, (source_name,))
    status = status_rows[0] if status_rows else {}
    details = status.get("details") or {}
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            details = {}

    summary = {
        "source_available": status.get("refresh_status") == "success",
        "property_level_available": bool(details.get("property_level_available")),
        "refresh_status": status.get("refresh_status"),
        "last_updated": str(status.get("external_last_updated") or status.get("last_refreshed_at")) if status else None,
        "source_records": _safe_int(details.get("source_records")),
        "matched_parcels": _safe_int(details.get("matched_parcels")),
        "join_note": details.get("join_note") or details.get("message"),
    }

    if city != "baltimore":
        return summary

    table_rows = _query("SELECT to_regclass('public.city_eviction_events') AS table_name")
    if not table_rows or not table_rows[0].get("table_name"):
        return summary

    rows = _query("""
        SELECT
            COUNT(*) AS events_total,
            COUNT(DISTINCT case_number) AS cases_total,
            COUNT(*) FILTER (WHERE event_type ILIKE 'Petition - For Warrant of Restitution Filed') AS warrant_filings,
            COUNT(*) FILTER (WHERE event_type ILIKE '%%Evicted%%') AS evicted_events,
            MIN(event_date) AS min_event_date,
            MAX(event_date) AS max_event_date,
            MAX(evicted_date) AS max_evicted_date
        FROM city_eviction_events
        WHERE city = 'baltimore'
    """)
    ev = rows[0] if rows else {}
    summary.update({
        "events_total": _safe_int(ev.get("events_total")),
        "cases_total": _safe_int(ev.get("cases_total")),
        "warrant_filings": _safe_int(ev.get("warrant_filings")),
        "evicted_events": _safe_int(ev.get("evicted_events")),
        "min_event_date": str(ev["min_event_date"]) if ev.get("min_event_date") else None,
        "max_event_date": str(ev["max_event_date"]) if ev.get("max_event_date") else None,
        "max_evicted_date": str(ev["max_evicted_date"]) if ev.get("max_evicted_date") else None,
    })
    return summary


def _city_code_summary(city: str) -> dict:
    valid_bbl_filter = "bbl ~ '^[1-5][0-9]{9}$'" if city == "nyc" else "TRUE"
    rows = _query(f"""
        SELECT
            COUNT(*) FILTER (
                WHERE {valid_bbl_filter}
                  AND (
                      COALESCE(violations_total, 0) > 0
                   OR COALESCE(violations_open, 0) > 0
                   OR COALESCE(violations_open_c, 0) > 0
                  )
            ) AS bbls_with_records,
            SUM(COALESCE(violations_total, 0)) FILTER (WHERE {valid_bbl_filter}) AS total_violations,
            SUM(COALESCE(violations_open, 0)) FILTER (WHERE {valid_bbl_filter}) AS open_violations,
            SUM(COALESCE(violations_open_c, 0)) FILTER (WHERE {valid_bbl_filter}) AS open_violations_c,
            SUM(COALESCE(litigations_total, 0)) FILTER (WHERE {valid_bbl_filter}) AS total_litigations,
            SUM(COALESCE(litigations_open, 0)) FILTER (WHERE {valid_bbl_filter}) AS open_litigations,
            SUM(COALESCE(litigations_harassment, 0)) FILTER (WHERE {valid_bbl_filter}) AS harassment_findings,
            SUM(COALESCE(evictions_total, 0)) FILTER (WHERE {valid_bbl_filter}) AS evictions_total,
            COUNT(*) FILTER (WHERE {valid_bbl_filter} AND COALESCE(is_rent_stabilized, false)) AS rent_stabilized_buildings,
            SUM(COALESCE(rs_units, 0)) FILTER (WHERE {valid_bbl_filter}) AS rent_stabilized_units,
            COUNT(*) FILTER (WHERE {valid_bbl_filter} AND COALESCE(nhpd_subsidy, false)) AS subsidized_buildings,
            MAX(last_violation_date) FILTER (WHERE {valid_bbl_filter}) AS last_violation_date,
            COUNT(*) FILTER (WHERE NOT ({valid_bbl_filter})) AS invalid_bbl_rows
        FROM {city}_bbl_stats
    """)
    row = rows[0] if rows else {}

    status_candidates = (
        ["NYC_HPD_ENRICHMENT"]
        if city == "nyc"
        else (
            ["BUFFALO_ENFORCEMENT", "NY_CODE_ENFORCEMENT", "NY_ENFORCEMENT"]
            if city == "ny"
            else [f"{city.upper()}_CODE_ENFORCEMENT", f"{city.upper()}_ENFORCEMENT"]
        )
    )
    status_rows = _query("""
        SELECT source_name, external_last_updated, last_refreshed_at, refresh_status, details
        FROM data_source_status
        WHERE source_name = ANY(%s)
        ORDER BY last_refreshed_at DESC NULLS LAST
        LIMIT 1
    """, (status_candidates,))
    status = status_rows[0] if status_rows else {}
    status_source = status.get("source_name") or status_candidates[0]
    details = _parse_jsonish(status.get("details"), {})
    last_refreshed_at = status.get("external_last_updated") or status.get("last_refreshed_at")
    last_success_at = details.get("last_success_at")
    if status.get("refresh_status") == "success" and last_refreshed_at:
        last_success_at = str(last_refreshed_at)

    return {
        "source_available": bool(_safe_int(row.get("bbls_with_records"))),
        "status_source": status_source,
        "refresh_status": status.get("refresh_status"),
        "last_refreshed_at": str(last_refreshed_at) if last_refreshed_at else None,
        "last_success_at": last_success_at,
        "refresh_message": details.get("message"),
        "bbls_with_records": _safe_int(row.get("bbls_with_records")),
        "invalid_bbl_rows": _safe_int(row.get("invalid_bbl_rows")),
        "total_violations": _safe_int(row.get("total_violations")),
        "open_violations": _safe_int(row.get("open_violations")),
        "open_violations_c": _safe_int(row.get("open_violations_c")),
        "total_litigations": _safe_int(row.get("total_litigations")),
        "open_litigations": _safe_int(row.get("open_litigations")),
        "harassment_findings": _safe_int(row.get("harassment_findings")),
        "evictions_total": _safe_int(row.get("evictions_total")),
        "rent_stabilized_buildings": _safe_int(row.get("rent_stabilized_buildings")),
        "rent_stabilized_units": _safe_int(row.get("rent_stabilized_units")),
        "subsidized_buildings": _safe_int(row.get("subsidized_buildings")),
        "last_violation_date": str(row["last_violation_date"]) if row.get("last_violation_date") else None,
    }


@router.get("/official-records/{bbl}", response_class=HTMLResponse)
def city_official_records_frame(city: str, bbl: str):
    c = _validate_city(city)
    if c != "baltimore":
        raise HTTPException(status_code=404, detail="Embedded official records are not configured for this city.")

    props = _query("""
        SELECT bbl, address, owner_name
        FROM baltimore_properties
        WHERE bbl = %s
        LIMIT 1
    """, (bbl,))
    prop = props[0] if props else {"bbl": bbl, "address": None, "owner_name": None}
    sections = [_fetch_baltimore_official_section(layer, bbl) for layer in BALTIMORE_OFFICIAL_LAYERS]
    sections = [section for section in sections if section["records"] or section.get("error")]
    return HTMLResponse(_render_baltimore_official_records_html(prop, sections))


# ---------------------------------------------------------------------------
# /api/{city}/search  — landlord / address autocomplete
# ---------------------------------------------------------------------------
@router.get("/search")
def city_search(
    city:  str,
    q:     str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
):
    c = _validate_city(city)
    term = q.strip().upper()
    # Collapse dotted initialisms ("W.C." -> "WC", "D.R." -> "DR") then split on punctuation/spaces
    collapsed_term = re.sub(r"\b([A-Z])\.\s*([A-Z])\.?(?=\s|$)", r"\1\2", term)
    clean_term = re.sub(r"[,.;]+", " ", collapsed_term)
    terms = [t for t in clean_term.split() if t]
    if not terms:
        return []

    term_like = f"%{' '.join(terms)}%"
    multi_word = len(terms) >= 2

    def _term_clause(expr: str, word: str) -> tuple[str, str]:
        if multi_word and len(word) == 2 and word.isalpha():
            # Match literal "WC"/"DR" OR initial-expanded "W. C." / "WILLIAM CHRISTOPHER" / "D R"
            pat = rf"(?:{re.escape(word)}|\y{re.escape(word[0])}[A-Z.]*\s+{re.escape(word[1])}[A-Z.]*\y)"
            return f"{expr} ~* %s", pat
        return f"{expr} ILIKE %s", f"%{word}%"

    def _build_where(expr: str) -> tuple[str, list[str]]:
        clauses = []
        params = []
        for word in terms:
            cl, prm = _term_clause(expr, word)
            clauses.append(cl)
            params.append(prm)
        return " AND ".join(clauses), params

    def _py_matches_all(text: str) -> bool:
        up = (text or "").upper()
        for word in terms:
            if multi_word and len(word) == 2 and word.isalpha():
                pat = rf"(?:{re.escape(word)}|\b{re.escape(word[0])}[A-Z.]*\s+{re.escape(word[1])}[A-Z.]*\b)"
                if not re.search(pat, up):
                    return False
            elif word not in up:
                return False
        return True

    results = []

    def append_network_result(row, match_context=None, cached_prof=None):
        matched_member_names = [name for name in (row.get("matched_member_names") or []) if name]
        identity = _network_identity(
            row.get("display_name"), row.get("member_names"), row.get("connection_signals")
        )
        prof = cached_prof or {}
        op_name = prof.get("operator_name")
        rep_human = prof.get("representative_human_name") or identity.get("primary_human_name")
        context = match_context
        if not context:
            if op_name and _py_matches_all(op_name):
                context = f"Matched operator: {op_name}"
            elif rep_human and _py_matches_all(rep_human):
                context = f"Matched principal: {rep_human}"
            elif identity.get("primary_human_name") and _py_matches_all(identity["primary_human_name"]):
                context = f"Matched principal: {identity['primary_human_name']}"
            elif matched_member_names:
                context = "Matched network member: " + ", ".join(matched_member_names[:3])

        results.append({
            "type":          f"{c}_network",
            "network_key":   row["network_key"],
            "display_name":  row["display_name"],
            "building_count": int(row["building_count"] or 0),
            "business_count": prof.get("business_count"),
            "unit_count":    int(row["unit_count"] or 0) if c in RELIABLE_UNIT_CITIES else None,
            "borough_summary": row["borough_summary"],
            "match_context": context,
            "matched_member_names": matched_member_names,
            **identity,
            "operator_name": op_name,
            "operator_basis": prof.get("operator_basis"),
            "primary_hq_address": prof.get("primary_hq_address"),
            "representative_human_name": rep_human,
            "label":         op_name or rep_human or row["display_name"],
            "sublabel":      _borough_summary_text(row["borough_summary"]),
        })

    # 0. Fast representative-operator / representative-principal match (pre-audited top portfolios)
    direct_net_rows = []
    seen_network_keys: set[str] = set()
    try:
        where_rep_op, rep_op_params = _build_where(
            "CONCAT_WS(' ', COALESCE(rp.operator_name, ''), COALESCE(rp.representative_human_name, ''), COALESCE(rp.operating_entities::text, ''))"
        )
        rep_net_rows = _query(f"""
            SELECT
                n.network_key,
                n.display_name,
                n.building_count,
                n.unit_count,
                n.borough_summary,
                n.member_names,
                n.connection_signals
            FROM network_representative_profiles rp
            JOIN {c}_networks n ON n.network_key = rp.network_key
            WHERE rp.jurisdiction = %s
              AND ({where_rep_op})
            ORDER BY n.building_count DESC NULLS LAST, n.display_name
            LIMIT %s
        """, [c] + rep_op_params + [min(limit, 8)])
        for rr in rep_net_rows:
            if rr["network_key"] not in seen_network_keys:
                seen_network_keys.add(rr["network_key"])
                direct_net_rows.append(rr)
    except Exception:
        pass

    # 1. Direct network name or primary OpenCorporates/registration principal matches.
    # Split `display_name` from `primary_human_name` so `*_networks_display_trgm_idx` is always used.
    where_disp, disp_params = _build_where("display_name")
    disp_rows = _query(f"""
        SELECT
            network_key,
            display_name,
            building_count,
            unit_count,
            borough_summary,
            member_names,
            connection_signals
        FROM {c}_networks
        WHERE {where_disp}
        ORDER BY building_count DESC NULLS LAST, display_name
        LIMIT %s
    """, disp_params + [limit])
    for dr in disp_rows:
        if dr["network_key"] not in seen_network_keys:
            seen_network_keys.add(dr["network_key"])
            direct_net_rows.append(dr)

    if len(direct_net_rows) < limit:
        where_human, human_params = _build_where("COALESCE(connection_signals::jsonb->>'primary_human_name', '')")
        rem_direct = limit - len(direct_net_rows)
        human_net_rows = _query(f"""
            SELECT
                network_key,
                display_name,
                building_count,
                unit_count,
                borough_summary,
                member_names,
                connection_signals
            FROM {c}_networks
            WHERE COALESCE(building_count, 0) >= 2
              AND (connection_signals::jsonb->>'primary_human_name') IS NOT NULL
              AND ({where_human})
            ORDER BY building_count DESC NULLS LAST, display_name
            LIMIT %s
        """, human_params + [rem_direct])
        for hr in human_net_rows:
            if hr["network_key"] not in seen_network_keys:
                seen_network_keys.add(hr["network_key"])
                direct_net_rows.append(hr)

        if not direct_net_rows:
            single_human_rows = _query(f"""
                SELECT
                    network_key,
                    display_name,
                    building_count,
                    unit_count,
                    borough_summary,
                    member_names,
                    connection_signals
                FROM {c}_networks
                WHERE COALESCE(building_count, 0) = 1
                  AND (connection_signals::jsonb->>'primary_human_name') IS NOT NULL
                  AND ({where_human})
                LIMIT %s
            """, human_params + [min(limit, 8)])
            for hr in single_human_rows:
                if hr["network_key"] not in seen_network_keys:
                    seen_network_keys.add(hr["network_key"])
                    direct_net_rows.append(hr)

    # 2. Multi-entity network member / corporate officer matches (always check major multi-entity
    # portfolios so searching "Winn" or "Hamilton" surfaces the 908-parcel portfolio before 1-parcel matches).
    member_net_cap = min(5, limit)
    if member_net_cap > 0:
        where_hidden_exists, hidden_params = _build_where("n")
        where_member_like, member_params = _build_where("member_name")

        hidden_net_rows = _query(f"""
            SELECT
                network_key,
                display_name,
                building_count,
                unit_count,
                borough_summary,
                member_names,
                connection_signals,
                ARRAY(
                    SELECT member_name
                    FROM (
                        SELECT unnest(COALESCE(member_names, ARRAY[]::text[])) AS member_name
                        UNION
                        SELECT jsonb_array_elements_text(
                            CASE
                                WHEN jsonb_typeof(connection_signals::jsonb->'source_human_names') = 'array'
                                THEN connection_signals::jsonb->'source_human_names'
                                ELSE '[]'::jsonb
                            END
                        ) AS member_name
                    ) all_m
                    WHERE {where_member_like}
                    ORDER BY length(member_name), member_name
                    LIMIT 5
                ) AS matched_member_names
            FROM {c}_networks
            WHERE COALESCE(building_count, 0) >= 2
              AND (
                    COALESCE(array_length(member_names, 1), 0) > 1
                    OR connection_signals::text LIKE '%%source_human_names%%'
                  )
              AND NOT (network_key = ANY(%s))
              AND (
                  EXISTS (
                      SELECT 1 FROM unnest(COALESCE(member_names, ARRAY[]::text[])) AS n
                      WHERE {where_hidden_exists}
                  )
                  OR EXISTS (
                      SELECT 1 FROM jsonb_array_elements_text(
                          CASE
                              WHEN jsonb_typeof(connection_signals::jsonb->'source_human_names') = 'array'
                              THEN connection_signals::jsonb->'source_human_names'
                              ELSE '[]'::jsonb
                          END
                      ) AS n
                      WHERE {where_hidden_exists}
                  )
              )
            ORDER BY building_count DESC NULLS LAST, display_name
            LIMIT %s
        """, member_params + [list(seen_network_keys)] + hidden_params + hidden_params + [member_net_cap])

        for r in hidden_net_rows:
            if r["network_key"] not in seen_network_keys:
                seen_network_keys.add(r["network_key"])
                direct_net_rows.append(r)

    cached_search_profiles = _load_cached_representative_profiles(
        c, [r["network_key"] for r in direct_net_rows]
    )
    direct_net_rows.sort(
        key=lambda r: (
            0 if (
                (cached_search_profiles.get(r["network_key"]) or {}).get("operator_name")
                and _py_matches_all((cached_search_profiles.get(r["network_key"]) or {}).get("operator_name") or "")
            ) else (1 if _py_matches_all(r.get("display_name") or "") and not r.get("matched_member_names") else 2),
            -(int(r.get("building_count") or 0)),
            r.get("display_name") or "",
        )
    )
    top_search_nets = direct_net_rows[: min(limit, 8)]
    for r in top_search_nets:
        prof = cached_search_profiles.get(r["network_key"])
        ctx = None
        if prof and prof.get("operator_name") and _py_matches_all(prof["operator_name"]):
            ctx = f"Matched operator: {prof['operator_name']}"
        elif _py_matches_all(r.get("display_name") or "") and not r.get("matched_member_names"):
            ctx = "Matched network name"
        append_network_result(r, ctx, cached_prof=prof)

    # 3. Property address/municipality/owner matches for remaining slots.
    remaining = limit - len(results)
    if remaining > 0:
        where_paddr, paddr_params = _build_where("p.address")
        where_owner, owner_params = _build_where("p.owner_name")
        prop_rows = _query(f"""
            WITH matched_props AS (
                SELECT
                    p.bbl,
                    p.address,
                    p.borough,
                    p.zip_code,
                    p.owner_name,
                    p.units_res
                FROM {c}_properties p
                WHERE ({where_paddr})
                   OR ({where_owner})
                ORDER BY
                    CASE WHEN p.address ILIKE %s THEN 0
                         WHEN p.owner_name ILIKE %s THEN 1
                         ELSE 2 END,
                    p.units_res DESC NULLS LAST,
                    p.address
                LIMIT %s
            )
            SELECT
                mp.bbl,
                mp.address,
                mp.borough,
                mp.zip_code,
                mp.owner_name,
                mp.units_res,
                n.network_key,
                n.display_name AS network_name,
                n.building_count
            FROM matched_props mp
            LEFT JOIN LATERAL (
                SELECT
                    net.network_key,
                    net.display_name,
                    net.building_count
                FROM {c}_networks net
                WHERE ARRAY[mp.bbl]::text[] <@ net.bbl_list
                ORDER BY net.building_count DESC NULLS LAST
                LIMIT 1
            ) n ON TRUE
        """, paddr_params + owner_params + [term_like, term_like, remaining])

        if not prop_rows and not results:
            where_addr_boro, addr_boro_params = _build_where("CONCAT_WS(' ', p.address, p.borough)")
            prop_rows = _query(f"""
                WITH matched_props AS (
                    SELECT
                        p.bbl,
                        p.address,
                        p.borough,
                        p.zip_code,
                        p.owner_name,
                        p.units_res
                    FROM {c}_properties p
                    WHERE {where_addr_boro}
                    ORDER BY p.units_res DESC NULLS LAST, p.address
                    LIMIT %s
                )
                SELECT
                    mp.bbl,
                    mp.address,
                    mp.borough,
                    mp.zip_code,
                    mp.owner_name,
                    mp.units_res,
                    n.network_key,
                    n.display_name AS network_name,
                    n.building_count
                FROM matched_props mp
                LEFT JOIN LATERAL (
                    SELECT
                        net.network_key,
                        net.display_name,
                        net.building_count
                    FROM {c}_networks net
                    WHERE ARRAY[mp.bbl]::text[] <@ net.bbl_list
                    ORDER BY net.building_count DESC NULLS LAST
                    LIMIT 1
                ) n ON TRUE
            """, addr_boro_params + [remaining])

        for r in prop_rows:
            owner_name = r["owner_name"] or ""
            address = r["address"] or ""
            borough = r["borough"] or ""
            match_context = None
            if _py_matches_all(f"{address} {borough}"):
                match_context = "Matched address"
            elif _py_matches_all(owner_name):
                match_context = f"Matched owner: {owner_name}"

            results.append({
                "type":          f"{c}_property",
                "bbl":           r["bbl"],
                "address":       r["address"],
                "borough":       r["borough"],
                "zip_code":      r["zip_code"],
                "owner_name":    r["owner_name"],
                "units_res":     _safe_int(r["units_res"]) if c in RELIABLE_UNIT_CITIES else None,
                "network_key":   r["network_key"],
                "network_name":  r["network_name"],
                "building_count": int(r["building_count"] or 0),
                "label":         r["address"],
                "sublabel":      f"{r['borough']} · {r['owner_name'] or 'Unknown owner'}",
                "match_context": match_context,
            })

    return results


# ---------------------------------------------------------------------------
# /api/{city}/networks  — true top networks list
# ---------------------------------------------------------------------------
@router.get("/networks")
def city_top_networks(
    city: str,
    limit: int = Query(12, ge=1, le=100),
    min_buildings: int = Query(1, ge=0, le=1000),
    sort_by: str = Query("buildings"),
    include_institutional: bool = Query(False),
    force_refresh: bool = Query(False),
):
    c = _validate_city(city)
    sort_columns = {
        "buildings": "building_count",
        "units": "unit_count",
        "name": "display_name",
        "open_violations": "open_violations",
        "violations": "open_violations",
        "code": "open_violations",
        # Every code case on record, not just the ones still open. This is the
        # ranking the landing-page cards use so the count matches what they show.
        "code_cases": "total_violations",
        "total_violations": "total_violations",
        "evictions": "evictions_total",
        "evictions_total": "evictions_total",
    }
    sort_col = sort_columns.get(sort_by, "building_count")
    net_cache_key = f"city_top_nets:v2:{c}:{sort_col}:{limit}:{min_buildings}:{int(bool(include_institutional))}"
    now = time.time()
    if limit <= 25 and not force_refresh:
        if net_cache_key in _networks_cache and net_cache_key in _networks_ts and now - _networks_ts[net_cache_key] < L1_MEM_TTL:
            return _networks_cache[net_cache_key]
        kv_nets, is_fresh = _kv_get(net_cache_key)
        if kv_nets is not None:
            _networks_cache[net_cache_key] = kv_nets
            _networks_ts[net_cache_key] = now
            if not is_fresh:
                _spawn_kv_refresh(
                    net_cache_key,
                    lambda: city_top_networks(
                        c, limit=limit, min_buildings=min_buildings, sort_by=sort_by,
                        include_institutional=include_institutional, force_refresh=True,
                    ),
                )
            return kv_nets
    order_expr = "display_name ASC" if sort_col == "display_name" else f"{sort_col} DESC NULLS LAST, display_name ASC"
    institutional_text = (
        "CONCAT_WS(E'\\n', n.display_name, "
        "COALESCE(n.connection_signals::jsonb ->> 'primary_entity_name', ''), "
        "COALESCE(n.connection_signals::jsonb ->> 'primary_human_name', ''), "
        "CASE WHEN COALESCE(array_length(n.member_names, 1), 0) <= 3 "
        "THEN ARRAY_TO_STRING(n.member_names, E'\\n') ELSE '' END)"
    )
    institutional_sql, institutional_params = _institutional_filter(include_institutional, institutional_text)

    net_cols = _get_network_columns(c)
    oc_ids_expr = "n.oc_company_ids" if "oc_company_ids" in net_cols else "NULL::text[] AS oc_company_ids"
    member_addrs_expr = "n.member_addresses" if "member_addresses" in net_cols else "ARRAY[]::text[] AS member_addresses"

    candidate_limit = min(max(limit + 200, limit * 4), 1000)
    if sort_col in ("building_count", "unit_count", "display_name"):
        rows = _query(f"""
            WITH base_nets AS (
                SELECT
                    n.network_key,
                    n.display_name,
                    n.building_count,
                    n.unit_count,
                    n.borough_summary,
                    n.member_names,
                    {member_addrs_expr},
                    {oc_ids_expr},
                    n.connection_signals,
                    n.bbl_list
                FROM {c}_networks n
                WHERE COALESCE(n.building_count, 0) >= %s
                ORDER BY {order_expr}
                LIMIT %s
            ),
            ranked_candidates AS (
                SELECT
                    n.network_key,
                    n.display_name,
                    n.building_count,
                    n.unit_count,
                    n.borough_summary,
                    n.member_names,
                    n.member_addresses,
                    n.oc_company_ids,
                    n.connection_signals,
                    COALESCE(stats.open_violations, 0) AS open_violations,
                    COALESCE(stats.open_violations_c, 0) AS open_violations_c,
                    COALESCE(stats.total_violations, 0) AS total_violations,
                    COALESCE(stats.evictions_total, 0) AS evictions_total
                FROM base_nets n
                LEFT JOIN LATERAL (
                    SELECT
                        SUM(COALESCE(s.violations_open, 0))::int AS open_violations,
                        SUM(COALESCE(s.violations_open_c, 0))::int AS open_violations_c,
                        SUM(COALESCE(s.violations_total, 0))::int AS total_violations,
                        SUM(COALESCE(s.evictions_total, 0))::int AS evictions_total
                    FROM {c}_bbl_stats s
                    WHERE s.bbl = ANY(n.bbl_list)
                ) stats ON TRUE
            )
            SELECT *
            FROM ranked_candidates n
            WHERE TRUE
              {institutional_sql}
            ORDER BY {order_expr}
            LIMIT %s
        """, [min_buildings, candidate_limit] + institutional_params + [limit])
    else:
        rows = _query(f"""
            WITH bbl_agg AS (
                SELECT
                    s.bbl,
                    COALESCE(s.violations_open, 0) AS violations_open,
                    COALESCE(s.violations_open_c, 0) AS violations_open_c,
                    COALESCE(s.violations_total, 0) AS violations_total,
                    COALESCE(s.evictions_total, 0) AS evictions_total
                FROM {c}_bbl_stats s
                WHERE COALESCE(s.violations_total, 0) > 0
                   OR COALESCE(s.violations_open, 0) > 0
                   OR COALESCE(s.evictions_total, 0) > 0
            ),
            net_hits AS (
                SELECT
                    n.network_key,
                    SUM(ba.violations_open)::int AS open_violations,
                    SUM(ba.violations_open_c)::int AS open_violations_c,
                    SUM(ba.violations_total)::int AS total_violations,
                    SUM(ba.evictions_total)::int AS evictions_total
                FROM bbl_agg ba
                JOIN {c}_networks n ON ARRAY[ba.bbl]::text[] <@ n.bbl_list
                WHERE COALESCE(n.building_count, 0) >= %s
                GROUP BY n.network_key
            ),
            ranked_candidates AS (
                SELECT
                    n.network_key,
                    n.display_name,
                    n.building_count,
                    n.unit_count,
                    n.borough_summary,
                    n.member_names,
                    {member_addrs_expr},
                    {oc_ids_expr},
                    n.connection_signals,
                    nh.open_violations,
                    nh.open_violations_c,
                    nh.total_violations,
                    nh.evictions_total
                FROM net_hits nh
                JOIN {c}_networks n ON n.network_key = nh.network_key
                ORDER BY {order_expr}
                LIMIT %s
            )
            SELECT *
            FROM ranked_candidates n
            WHERE TRUE
              {institutional_sql}
            ORDER BY {order_expr}
            LIMIT %s
        """, [min_buildings, candidate_limit] + institutional_params + [limit])

    profiles_by_key = _resolve_network_business_profiles(c, rows, compute_missing=True)

    results = []
    for row in rows:
        identity = _network_identity(
            row.get("display_name"), row.get("member_names"), row.get("connection_signals")
        )
        prof = profiles_by_key.get(row["network_key"]) or {}
        member_names = row.get("member_names") or []
        biz_count = prof.get("business_count")
        if biz_count is None:
            biz_count = sum(
                1 for name in member_names
                if name and not looks_like_person_owner(name)
            )
            if biz_count == 0 and row.get("display_name") and not looks_like_person_owner(row["display_name"]):
                biz_count = 1
        rep_human = prof.get("representative_human_name") or identity["primary_human_name"]
        op_name = prof.get("operator_name")
        results.append({
            "type": f"{c}_network",
            "network_key": row["network_key"],
            "display_name": row["display_name"],
            "building_count": int(row["building_count"] or 0),
            "business_count": int(biz_count or 0),
            "unit_count": int(row["unit_count"] or 0) if c in RELIABLE_UNIT_CITIES else None,
            "open_violations": _safe_int(row.get("open_violations")),
            "open_violations_c": _safe_int(row.get("open_violations_c")),
            "total_violations": _safe_int(row.get("total_violations")),
            "evictions_total": _safe_int(row.get("evictions_total")),
            "borough_summary": row["borough_summary"],
            "connection_signals": _parse_jsonish(row.get("connection_signals"), {}),
            **identity,
            "operator_name": op_name,
            "operator_source": prof.get("operator_source"),
            "operator_basis": prof.get("operator_basis"),
            "operating_entities": prof.get("operating_entities") or [],
            "primary_hq_address": prof.get("primary_hq_address"),
            "hq_entity_count": prof.get("hq_entity_count"),
            "representative_human_name": rep_human,
            "key_officers": prof.get("key_officers") or [],
            "officer_count": int(prof.get("officer_count") or 0),
            "sample_entities": prof.get("sample_entities") or [],
            "external_source_url": prof.get("external_source_url"),
            "external_source_label": prof.get("external_source_label"),
            "label": op_name or rep_human or row["display_name"],
            "sublabel": _borough_summary_text(row["borough_summary"]),
        })
    if limit <= 25:
        _networks_cache[net_cache_key] = results
        _networks_ts[net_cache_key] = time.time()
        _kv_set(net_cache_key, results)
    return results


SIBLING_EXPLORER_PAIRS = {
    "nyc": ("ny", "New York Unified (All 62 Counties)"),
    "boston": ("ma", "Massachusetts Statewide"),
    "miami": ("fl", "Florida Unified (All 67 Counties)"),
    "chicago": ("il", "Illinois Statewide"),
    "minneapolis": ("mn", "Minnesota Unified"),
}


def _find_sibling_portfolios(
    city: str,
    display_name: Optional[str],
    member_names: Optional[List[str]],
    primary_human_name: Optional[str],
    oc_cids: Optional[List[str]],
) -> List[dict]:
    pair = SIBLING_EXPLORER_PAIRS.get(city)
    if not pair:
        return []
    sib_jur, sib_label = pair
    found_by_key: dict = {}

    try:
        # 1. Exact OpenCorporates entity match (`oc_jurisdiction_code`, `oc_company_number`)
        sib_prop_cols = _get_property_columns(sib_jur)
        if oc_cids and "oc_company_number" in sib_prop_cols:
            jurs, nums = [], []
            for cid in oc_cids[:25]:
                if isinstance(cid, str) and "/" in cid:
                    j, n = cid.split("/", 1)
                    if j and n:
                        jurs.append(j)
                        nums.append(n)
            if jurs:
                bbl_rows = _query(f"""
                    SELECT DISTINCT p.bbl
                    FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                    JOIN {sib_jur}_properties p
                      ON p.oc_jurisdiction_code = k.j AND p.oc_company_number = k.n
                    WHERE p.bbl IS NOT NULL
                    LIMIT 50
                """, (jurs, nums))
                matched_bbls = [r["bbl"] for r in bbl_rows if r.get("bbl")]
                if matched_bbls:
                    oc_rows = _query(f"""
                        SELECT
                            n.network_key,
                            n.display_name,
                            n.building_count,
                            n.unit_count,
                            n.connection_signals::jsonb ->> 'primary_human_name' AS primary_human_name
                        FROM {sib_jur}_networks n
                        WHERE n.bbl_list && %s::text[]
                        ORDER BY n.building_count DESC
                        LIMIT 3
                    """, (matched_bbls,))
                    for r in oc_rows:
                        found_by_key[r["network_key"]] = {**r, "match_basis": "corporate_registry"}

        # 2. Exact source-listed human principal match (indexed via `idx_*_nets_primary_human_upper`)
        if primary_human_name and len(primary_human_name.split()) >= 2:
            h_up = primary_human_name.strip().upper()
            if not is_excluded_third_party_entity(h_up):
                h_rows = _query(f"""
                    SELECT
                        network_key,
                        display_name,
                        building_count,
                        unit_count,
                        connection_signals::jsonb ->> 'primary_human_name' AS primary_human_name
                    FROM {sib_jur}_networks
                    WHERE UPPER(connection_signals::jsonb ->> 'primary_human_name') = %s
                    ORDER BY building_count DESC
                    LIMIT 3
                """, (h_up,))
                for r in h_rows:
                    found_by_key.setdefault(r["network_key"], {**r, "match_basis": "source_principal"})

        # 3. Exact registered entity / deed owner display_name match (indexed B-tree)
        cand_names = []
        for nm in [display_name, *(member_names or ())[:20]]:
            if nm and isinstance(nm, str):
                s = nm.strip()
                if len(s) >= 5 and not is_excluded_third_party_entity(s):
                    cand_names.append(s)
        if cand_names:
            d_rows = _query(f"""
                SELECT
                    network_key,
                    display_name,
                    building_count,
                    unit_count,
                    connection_signals::jsonb ->> 'primary_human_name' AS primary_human_name
                FROM {sib_jur}_networks
                WHERE display_name = ANY(%s)
                ORDER BY building_count DESC
                LIMIT 3
            """, (list(dict.fromkeys(cand_names)),))
            for r in d_rows:
                found_by_key.setdefault(r["network_key"], {**r, "match_basis": "entity_name"})
    except Exception as exc:
        logger.warning("Sibling portfolio lookup skipped for %s -> %s: %s", city, sib_jur, exc)
        return []

    ranked = sorted(found_by_key.values(), key=lambda x: int(x.get("building_count") or 0), reverse=True)[:2]
    return [
        {
            "jurisdiction": sib_jur,
            "jurisdiction_label": sib_label,
            "network_key": r["network_key"],
            "display_name": r["display_name"],
            "primary_human_name": r.get("primary_human_name"),
            "building_count": int(r.get("building_count") or 0),
            "unit_count": int(r["unit_count"] or 0) if sib_jur in RELIABLE_UNIT_CITIES else None,
            "match_basis": r["match_basis"],
        }
        for r in ranked
        if int(r.get("building_count") or 0) > 0
    ]


# ---------------------------------------------------------------------------
# /api/{city}/network/<network_key>  — full network detail
# ---------------------------------------------------------------------------
@router.get("/network/{network_key}")
def city_network_detail(city: str, network_key: str):
    c = _validate_city(city)
    net_cols = _get_network_columns(c)
    oc_ids_expr = "oc_company_ids" if "oc_company_ids" in net_cols else "NULL::text[] AS oc_company_ids"
    nets = _query(f"""
        SELECT
            network_key, anchor_type, display_name,
            member_names, member_addresses,
            {oc_ids_expr},
            registration_ids, bbl_list,
            building_count, unit_count, borough_summary,
            connection_signals,
            updated_at
        FROM {c}_networks
        WHERE network_key = %s
    """, (network_key,))

    if not nets:
        refresh_snapshot = _city_refresh_snapshot(c)
        if refresh_snapshot["is_refreshing"]:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{c.upper()} network data is refreshing. This network may be temporarily unavailable "
                    "while the ownership cache is recalculated. Please retry or search again shortly."
                ),
            )
        raise HTTPException(
            status_code=404,
            detail=(
                "Network not found. This network key may have been recalculated during a recent refresh; "
                "please go back and search again."
            ),
        )

    net = nets[0]

    if c == "nyc":
        mailing_select = """
            NULLIF(CONCAT_WS(', ', mail.business_address, mail.business_city, mail.business_state, mail.business_zip), '') AS mailing_address,
            NULL::text AS mailing_address_norm,
            NULL::text AS owner_email,
        """
        mailing_join = """
        LEFT JOIN LATERAL (
            SELECT
                co.business_address,
                co.business_city,
                co.business_state,
                co.business_zip
            FROM nyc_hpd_contacts co
            WHERE co.registration_id = r.registration_id
              AND co.contact_type IN ('HEADOFFICER', 'INDIVIDUALOWNER', 'CORPORATEOWNER', 'AGENT')
              AND COALESCE(co.business_address, '') != ''
            ORDER BY CASE co.contact_type
                WHEN 'HEADOFFICER' THEN 1
                WHEN 'INDIVIDUALOWNER' THEN 2
                WHEN 'CORPORATEOWNER' THEN 3
                WHEN 'AGENT' THEN 4
                ELSE 9
            END
            LIMIT 1
        ) mail ON TRUE
        """
    else:
        mailing_select = """
            p.mailing_address,
            p.mailing_address_norm,
            p.owner_email,
        """
        mailing_join = ""
    registration_join = _latest_registration_join(c)

    prop_cols = _get_property_columns(c)
    sale_select = f"""
        {'p.last_sale_date' if 'last_sale_date' in prop_cols else 'NULL::date'} AS last_sale_date,
        {'p.last_sale_price' if 'last_sale_price' in prop_cols else 'NULL::numeric'} AS last_sale_price,
        {'p.oc_jurisdiction_code' if 'oc_jurisdiction_code' in prop_cols else 'NULL::text'} AS oc_jurisdiction_code,
        {'p.oc_company_number' if 'oc_company_number' in prop_cols else 'NULL::text'} AS oc_company_number,
        {'p.oc_match_tier' if 'oc_match_tier' in prop_cols else 'NULL::smallint'} AS oc_match_tier,
        {'p.oc_match_confidence' if 'oc_match_confidence' in prop_cols else 'NULL::numeric'} AS oc_match_confidence,
    """

    # ---- Properties (buildings in this network) ----
    properties = _query(f"""
        SELECT
            p.bbl,
            p.address,
            p.borough,
            p.zip_code,
            p.owner_name,
            {mailing_select}
            {sale_select}
            p.land_use,
            p.bld_class,
            p.num_floors,
            p.units_res,
            p.units_total,
            p.year_built,
            p.assessed_total,
            p.latitude,
            p.longitude,
            p.compliance_active,
            p.compliance_record_id,
            p.compliance_expiration,
            r.registration_id,
            r.last_registration_date,
            r.registration_end_date,
            s.violations_total,
            s.violations_open,
            s.violations_class_a,
            s.violations_class_b,
            s.violations_class_c,
            s.violations_open_c,
            s.litigations_total,
            s.litigations_open,
            s.litigations_harassment,
            s.evictions_total,
            s.last_eviction_date,
            s.is_rent_stabilized,
            s.rs_units,
            s.nhpd_subsidy,
            s.nhpd_program,
            s.nhpd_expiration
        FROM {c}_properties p
        {registration_join}
        {mailing_join}
        LEFT JOIN {c}_bbl_stats s ON s.bbl = p.bbl
        WHERE p.bbl = ANY(%s)
        ORDER BY p.units_res DESC NULLS LAST, p.address
    """, (net["bbl_list"],))

    # ---- Contacts (officers behind this network) ----
    contacts = _query(f"""
        SELECT
            co.contact_type,
            co.first_name,
            co.last_name,
            co.full_name,
            co.full_name_norm,
            array_agg(DISTINCT co.corporation_name ORDER BY co.corporation_name)
                FILTER (WHERE co.corporation_name IS NOT NULL AND co.corporation_name != '') AS corporations,
            array_agg(DISTINCT co.registration_id ORDER BY co.registration_id)
                FILTER (WHERE co.registration_id IS NOT NULL AND co.registration_id != '') AS registration_ids,
            array_agg(DISTINCT r.bbl ORDER BY r.bbl)
                FILTER (WHERE r.bbl IS NOT NULL AND r.bbl != '') AS bbls,
            COUNT(DISTINCT r.bbl) AS building_count,
            (array_agg(co.business_address ORDER BY co.registration_id DESC)
                FILTER (WHERE co.business_address IS NOT NULL))[1] AS business_address,
            (array_agg(co.business_city ORDER BY co.registration_id DESC)
                FILTER (WHERE co.business_city IS NOT NULL))[1] AS business_city,
            (array_agg(co.business_state ORDER BY co.registration_id DESC)
                FILTER (WHERE co.business_state IS NOT NULL))[1] AS business_state,
            (array_agg(co.business_zip ORDER BY co.registration_id DESC)
                FILTER (WHERE co.business_zip IS NOT NULL))[1] AS business_zip
        FROM {c}_hpd_contacts co
        LEFT JOIN {c}_hpd_registrations r ON r.registration_id = co.registration_id
        WHERE co.registration_id = ANY(%s)
          AND co.contact_type IN ('HEADOFFICER','INDIVIDUALOWNER','CORPORATEOWNER','AGENT')
        GROUP BY co.contact_type, co.first_name, co.last_name, co.full_name, co.full_name_norm
        ORDER BY co.full_name_norm, co.contact_type
        LIMIT 200
    """, (net["registration_ids"],))

    # ---- Borough breakdown as structured object ----
    borough_summary = net["borough_summary"]
    if isinstance(borough_summary, str):
        try:
            borough_summary = json.loads(borough_summary)
        except Exception:
            borough_summary = {}

    # ---- Portfolio-level enrichment totals ----
    portfolio_stats = _query(f"""
        SELECT
            COUNT(s.bbl) AS bbl_stats_count,
            SUM(s.violations_open) AS open_violations,
            SUM(s.violations_open_c) AS open_violations_c,
            SUM(s.violations_total) AS total_violations,
            SUM(s.litigations_open) AS open_litigations,
            SUM(s.litigations_harassment) AS harassment_findings,
            SUM(s.evictions_total) AS evictions
        FROM {c}_bbl_stats s
        WHERE s.bbl = ANY(%s)
    """, (net["bbl_list"],))
    pstats = portfolio_stats[0] if portfolio_stats else {}
    has_enrichment = bool(_safe_int(pstats.get("bbl_stats_count")))

    # Parse connection_signals
    conn_signals = net.get("connection_signals") or {}
    if isinstance(conn_signals, str):
        try:
            conn_signals = json.loads(conn_signals)
        except Exception:
            conn_signals = {}
    identity = _network_identity(net["display_name"], net["member_names"], conn_signals)
    detail_profiles = _resolve_network_business_profiles(
        c, [{**net, "connection_signals": conn_signals}], compute_missing=True
    )
    prof = detail_profiles.get(net["network_key"]) or {}

    formatted_props = [_format_property(p, city=c) for p in properties]

    # Build source-backed parcel transaction summary (only where genuine source `last_sale_date` exists)
    today = dt.date.today()
    cutoff_12m = today - dt.timedelta(days=365)
    cutoff_36m = today - dt.timedelta(days=1095)
    valid_sales = []
    oc_ids_seen = set(conn_signals.get("oc_matched_companies") or ()) | set(conn_signals.get("oc_company_ids") or ())
    cid_to_bbls: dict[str, set[str]] = {}
    for fp in formatted_props:
        if fp.get("oc_jurisdiction_code") and fp.get("oc_company_number"):
            cid = f"{fp['oc_jurisdiction_code']}/{fp['oc_company_number']}"
            oc_ids_seen.add(cid)
            if fp.get("bbl"):
                cid_to_bbls.setdefault(cid, set()).add(fp["bbl"])
        raw_sd = fp.get("last_sale_date")
        if not raw_sd:
            continue
        try:
            sd = dt.date.fromisoformat(str(raw_sd)[:10])
        except ValueError:
            continue
        if sd.year < 1901 or sd > today + dt.timedelta(days=30):
            continue
        amt = fp.get("last_sale_price")
        clean_amt = amt if (amt is not None and amt > 0) else None
        valid_sales.append(
            (
                sd,
                {
                    "bbl": fp.get("bbl"),
                    "date": sd.isoformat(),
                    "sale_date": sd.isoformat(),
                    "amount": clean_amt,
                    "sale_price": clean_amt,
                    "address": fp.get("address"),
                    "borough": fp.get("borough"),
                    "owner_name": fp.get("owner_name"),
                    "direction": "acquired",
                },
            )
        )
    valid_sales.sort(key=lambda pair: (pair[0], pair[1].get("amount") or 0), reverse=True)
    acq_12m = sum(1 for sd, _ in valid_sales if sd >= cutoff_12m)
    acq_36m = sum(1 for sd, _ in valid_sales if sd >= cutoff_36m)
    transaction_summary = {
        "total_with_sale_date": len(valid_sales),
        "acquired_last_12m": acq_12m,
        "acquired_last_36m": acq_36m,
        "recent_transactions": [tx for _, tx in valid_sales[:250]],
    }
    formatted_contacts = [_format_contact(ct_row) for ct_row in contacts]
    if oc_ids_seen:
        oc_links, oc_contacts, oc_all_cids = _fetch_oc_enrichment_for_cids(oc_ids_seen, cid_to_bbls)
        conn_signals = {
            **conn_signals,
            "oc_matched_companies": oc_all_cids,
            "oc_company_links": oc_links,
        }
        formatted_contacts.extend(oc_contacts)

    # Deduplicate human principals across punctuation, case, LAST/FIRST inversions, and
    # compatible middle initials/names (e.g. DON R HANKEY + DON R. HANKEY + DON ROBERT HANKEY
    # + DON HANKEY -> 1 canonical principal with unioned entities), while keeping incompatible
    # middle initials (e.g. JOHN A SMITH vs JOHN B SMITH) strictly separate.
    formatted_contacts = dedupe_network_contacts(formatted_contacts)
    if conn_signals.get("source_human_names"):
        conn_signals = {
            **conn_signals,
            "source_human_names": dedupe_human_name_list(conn_signals.get("source_human_names") or [], max_names=25),
        }

    sibling_portfolios = _find_sibling_portfolios(
        c,
        net.get("display_name"),
        net.get("member_names"),
        identity.get("primary_human_name"),
        conn_signals.get("oc_matched_companies"),
    )

    return {
        "network_key":    net["network_key"],
        "anchor_type":    net["anchor_type"],
        "display_name":   net["display_name"],
        "building_count": int(net["building_count"] or 0),
        "unit_count":     int(net["unit_count"] or 0) if c in RELIABLE_UNIT_CITIES else None,
        "borough_summary": borough_summary,
        "member_names":   net["member_names"],
        "member_addresses": net["member_addresses"],
        "connection_signals": conn_signals,
        "sibling_portfolios": sibling_portfolios,
        **identity,
        "operator_name": prof.get("operator_name"),
        "operator_source": prof.get("operator_source"),
        "operator_basis": prof.get("operator_basis"),
        "operating_entities": prof.get("operating_entities") or [],
        "primary_hq_address": prof.get("primary_hq_address"),
        "hq_entity_count": prof.get("hq_entity_count"),
        "business_count": prof.get("business_count"),
        "representative_human_name": prof.get("representative_human_name") or identity.get("primary_human_name"),
        "key_officers": prof.get("key_officers") or [],
        "officer_count": int(prof.get("officer_count") or 0),
        "sample_entities": prof.get("sample_entities") or [],
        "external_source_url": prof.get("external_source_url"),
        "external_source_label": prof.get("external_source_label"),
        "updated_at":     str(net["updated_at"]) if net["updated_at"] else None,
        "portfolio_stats": {
            "enrichment_available": has_enrichment,
            "open_violations":   _safe_int(pstats.get("open_violations")) if has_enrichment else None,
            "open_violations_c": _safe_int(pstats.get("open_violations_c")) if has_enrichment else None,
            "total_violations":  _safe_int(pstats.get("total_violations")) if has_enrichment else None,
            "open_litigations":  _safe_int(pstats.get("open_litigations")) if has_enrichment else None,
            "harassment_findings": _safe_int(pstats.get("harassment_findings")) if has_enrichment else None,
            "evictions":         _safe_int(pstats.get("evictions")) if has_enrichment else None,
        },
        "transaction_summary": transaction_summary,
        "properties":     formatted_props,
        "contacts":       formatted_contacts,
    }


@router.get("/network/{network_key}/official-code-links")
def city_network_official_code_links(
    city: str,
    network_key: str,
    max_groups: int = Query(800, ge=50, le=2000),
):
    c = _validate_city(city)
    if c != "nyc":
        raise HTTPException(status_code=404, detail="Official HPD code links are available for NYC networks only.")

    nets = _query(f"""
        SELECT network_key, display_name, bbl_list
        FROM {c}_networks
        WHERE network_key = %s
    """, (network_key,))
    if not nets:
        refresh_snapshot = _city_refresh_snapshot(c)
        if refresh_snapshot["is_refreshing"]:
            raise HTTPException(
                status_code=409,
                detail="NYC network data is refreshing. Official links will be available after the network cache settles.",
            )
        raise HTTPException(status_code=404, detail="Network not found. Please search again for the current network key.")

    net = nets[0]
    bbls = [b for b in (net.get("bbl_list") or []) if b]
    if not bbls:
        return {
            "network_key": network_key,
            "display_name": net.get("display_name"),
            "summary": {"open_violations": 0, "open_violations_c": 0, "buildings_with_open": 0},
            "records": [],
            "source_page_url": NYC_HPD_VIOLATIONS_PAGE,
            "explainers": _nyc_hpd_explainers(),
        }

    registration_join = _latest_registration_join(c)
    props = _query(f"""
        SELECT
            p.bbl,
            p.address,
            p.borough,
            p.zip_code,
            r.registration_id,
            COALESCE(s.violations_open, 0) AS open_violations,
            COALESCE(s.violations_open_c, 0) AS open_violations_c,
            COALESCE(s.violations_total, 0) AS total_violations,
            s.last_violation_date
        FROM {c}_properties p
        {registration_join}
        LEFT JOIN {c}_bbl_stats s ON s.bbl = p.bbl
        WHERE p.bbl = ANY(%s)
    """, (bbls,))
    prop_by_bbl = {p["bbl"]: p for p in props}

    grouped_rows = []
    session = requests.Session()
    page_size = 50000
    for i in range(0, len(bbls), 40):
        chunk = bbls[i:i + 40]
        where = "bbl in({}) AND violationstatus = 'Open'".format(
            ",".join(_soql_quote(bbl) for bbl in chunk)
        )
        offset = 0
        while True:
            params = {
                "$select": "bbl, class, novdescription, COUNT(*) AS count, MAX(inspectiondate) AS last_inspection",
                "$where": where,
                "$group": "bbl, class, novdescription",
                "$order": "bbl, class",
                "$limit": page_size,
                "$offset": offset,
            }
            try:
                resp = session.get(NYC_HPD_VIOLATIONS_URL, params=params, timeout=90)
                resp.raise_for_status()
                rows = resp.json()
            except Exception as e:
                logger.warning("NYC HPD official link query failed for network %s: %s", network_key, e)
                rows = []
            grouped_rows.extend(rows)
            if len(rows) < page_size:
                break
            offset += len(rows)

    severity_order = {"C": 0, "B": 1, "A": 2}
    records = []
    for row in grouped_rows:
        bbl = (row.get("bbl") or "").strip()
        prop = prop_by_bbl.get(bbl, {})
        cls = (row.get("class") or "").strip().upper() or "Unknown"
        complaint_type = _shorten_complaint_type(row.get("novdescription"))
        count = _safe_int(row.get("count"))
        open_data_params = {
            "$limit": 5000,
            "$order": "inspectiondate DESC",
            "bbl": bbl,
            "violationstatus": "Open",
        }
        if cls != "Unknown":
            open_data_params["class"] = cls
        if row.get("novdescription"):
            open_data_params["novdescription"] = row.get("novdescription")

        records.append({
            "bbl": bbl,
            "address": prop.get("address"),
            "borough": prop.get("borough"),
            "zip_code": prop.get("zip_code"),
            "registration_id": prop.get("registration_id"),
            "class": cls,
            "class_label": NYC_HPD_CLASS_LABELS.get(cls, "Unclassified"),
            "complaint_type": complaint_type,
            "count": count,
            "last_inspection": str(row["last_inspection"])[:10] if row.get("last_inspection") else None,
            "hpd_profile_url": _hpd_profile_url(prop.get("registration_id")),
            "open_data_url": _nyc_open_data_url(open_data_params),
            "source_page_url": NYC_HPD_VIOLATIONS_PAGE,
            "building_open_violations": _safe_int(prop.get("open_violations")),
            "building_open_violations_c": _safe_int(prop.get("open_violations_c")),
        })

    records.sort(key=lambda r: (
        (r.get("address") or ""),
        r.get("complaint_type") or "",
        severity_order.get(r.get("class"), 9),
        -(r.get("count") or 0),
    ))
    truncated = len(records) > max_groups
    records = records[:max_groups]

    summary_rows = _query(f"""
        SELECT
            SUM(COALESCE(s.violations_open, 0)) AS open_violations,
            SUM(COALESCE(s.violations_open_c, 0)) AS open_violations_c,
            COUNT(*) FILTER (WHERE COALESCE(s.violations_open, 0) > 0) AS buildings_with_open,
            MAX(s.last_violation_date) AS last_violation_date
        FROM {c}_bbl_stats s
        WHERE s.bbl = ANY(%s)
    """, (bbls,))
    summary_row = summary_rows[0] if summary_rows else {}

    return {
        "network_key": network_key,
        "display_name": net.get("display_name"),
        "summary": {
            "open_violations": _safe_int(summary_row.get("open_violations")),
            "open_violations_c": _safe_int(summary_row.get("open_violations_c")),
            "buildings_with_open": _safe_int(summary_row.get("buildings_with_open")),
            "last_violation_date": str(summary_row["last_violation_date"]) if summary_row.get("last_violation_date") else None,
            "groups_returned": len(records),
            "truncated": truncated,
        },
        "records": records,
        "source_page_url": NYC_HPD_VIOLATIONS_PAGE,
        "explainers": _nyc_hpd_explainers(),
    }


def _nyc_hpd_explainers() -> dict:
    return {
        "open": "Open means the NYC HPD Housing Maintenance Code dataset currently marks the violation status field as Open.",
        "class_c": "Class C means HPD classifies the violation as immediately hazardous.",
        "counts": "Counts are violation records, not unique buildings or court cases. A single building can have many open records across apartments and inspection dates.",
        "links": "HPD Building Profile links show the official building page; NYC Open Data links show the matching source rows for that building, status, class, and complaint text.",
    }


# ---------------------------------------------------------------------------
# /api/{city}/property/<bbl>  — single property lookup
# ---------------------------------------------------------------------------
@router.get("/property/{bbl}")
def city_property_detail(city: str, bbl: str):
    c = _validate_city(city)
    registration_join = _latest_registration_join(c)
    props = _query(f"""
        SELECT 
            p.*, 
            r.registration_id, 
            r.last_registration_date, 
            r.registration_end_date,
            s.violations_total,
            s.violations_open,
            s.violations_class_a,
            s.violations_class_b,
            s.violations_class_c,
            s.violations_open_c,
            s.litigations_total,
            s.litigations_open,
            s.litigations_harassment,
            s.evictions_total,
            s.last_eviction_date,
            s.is_rent_stabilized,
            s.rs_units,
            s.nhpd_subsidy,
            s.nhpd_program,
            s.nhpd_expiration
        FROM {c}_properties p
        {registration_join}
        LEFT JOIN {c}_bbl_stats s ON s.bbl = p.bbl
        WHERE p.bbl = %s
        ORDER BY r.last_registration_date DESC NULLS LAST
        LIMIT 1
    """, (bbl,))

    if not props:
        raise HTTPException(status_code=404, detail="Property not found")

    prop = props[0]

    # Find network this property belongs to
    nets = _query(f"""
        SELECT network_key, display_name, building_count, unit_count
        FROM {c}_networks
        WHERE %s = ANY(bbl_list)
        LIMIT 1
    """, (bbl,))

    formatted = _format_property(prop, city=c)
    oc_company_links = []
    oc_contacts = []
    if formatted.get("oc_jurisdiction_code") and formatted.get("oc_company_number"):
        cid = f"{formatted['oc_jurisdiction_code']}/{formatted['oc_company_number']}"
        oc_company_links, oc_contacts, _ = _fetch_oc_enrichment_for_cids([cid], {cid: {bbl}})

    return {
        **formatted,
        "network": nets[0] if nets else None,
        "oc_company_links": oc_company_links,
        "oc_contacts": oc_contacts,
    }


def _fetch_oc_enrichment_for_cids(
    raw_cids: set[str] | list[str],
    cid_to_bbls: dict[str, set[str]] | None = None,
) -> tuple[list[dict], list[dict], list[str]]:
    """Resolve matched OpenCorporates company IDs (`us_xx/<num>`), 1-hop bidirectional
    foreign-branch / home-company corporate links, and verified beneficial principals
    & corporate parents from `oc_companies` and `oc_officers`.
    """
    seed_cids = [cid.strip() for cid in sorted(set(raw_cids))[:50] if isinstance(cid, str) and "/" in cid]
    if not seed_cids:
        return [], [], []

    bbl_map: dict[str, set[str]] = {
        k: set(v) for k, v in (cid_to_bbls or {}).items()
    }
    try:
        j_seed = [cid.split("/", 1)[0] for cid in seed_cids]
        n_seed = [cid.split("/", 1)[1] for cid in seed_cids]
        seed_rows = _query(
            """
            SELECT c.jurisdiction_code, c.company_number, c.name, c.company_type,
                   c.current_status, c.incorporation_date,
                   c.registered_address_raw, c.mailing_address_raw, c.agent_address_raw,
                   c.home_jurisdiction_code, c.home_company_number,
                   c.opencorporates_url, c.registry_url
            FROM UNNEST(%s::text[], %s::text[]) AS k(j, n)
            JOIN oc_companies c ON c.jurisdiction_code = k.j AND c.company_number = k.n
            """,
            (j_seed, n_seed),
        )
    except Exception:
        seed_rows = []

    comp_by_cid: dict[str, dict] = {}
    forward_homes: list[tuple[str, str, str]] = []
    for r in seed_rows:
        cid = f"{r['jurisdiction_code']}/{r['company_number']}"
        comp_by_cid[cid] = r
        hj, hn = r.get("home_jurisdiction_code"), r.get("home_company_number")
        if hj and hn:
            forward_homes.append((cid, hj, hn))

    # Expand 1-hop forward (`home_company`) & reverse (foreign branch registrations)
    try:
        if forward_homes:
            hj_arr = [x[1] for x in forward_homes]
            hn_arr = [x[2] for x in forward_homes]
            home_rows = _query(
                """
                SELECT c.jurisdiction_code, c.company_number, c.name, c.company_type,
                       c.current_status, c.incorporation_date,
                       c.registered_address_raw, c.mailing_address_raw, c.agent_address_raw,
                       c.home_jurisdiction_code, c.home_company_number,
                       c.opencorporates_url, c.registry_url
                FROM UNNEST(%s::text[], %s::text[]) AS k(j, n)
                JOIN oc_companies c ON c.jurisdiction_code = k.j AND c.company_number = k.n
                """,
                (hj_arr, hn_arr),
            )
            for r in home_rows:
                hcid = f"{r['jurisdiction_code']}/{r['company_number']}"
                comp_by_cid.setdefault(hcid, r)
            for child_cid, hj, hn in forward_homes:
                hcid = f"{hj}/{hn}"
                if child_cid in bbl_map:
                    bbl_map.setdefault(hcid, set()).update(bbl_map[child_cid])

        rev_rows = _query(
            """
            SELECT c.jurisdiction_code, c.company_number, c.name, c.company_type,
                   c.current_status, c.incorporation_date,
                   c.registered_address_raw, c.mailing_address_raw, c.agent_address_raw,
                   c.home_jurisdiction_code, c.home_company_number,
                   c.opencorporates_url, c.registry_url
            FROM UNNEST(%s::text[], %s::text[]) AS k(j, n)
            JOIN oc_companies c ON c.home_jurisdiction_code = k.j AND c.home_company_number = k.n
            LIMIT 40
            """,
            (j_seed, n_seed),
        )
        for r in rev_rows:
            bcid = f"{r['jurisdiction_code']}/{r['company_number']}"
            comp_by_cid.setdefault(bcid, r)
            hcid = f"{r['home_jurisdiction_code']}/{r['home_company_number']}"
            if hcid in bbl_map:
                bbl_map.setdefault(bcid, set()).update(bbl_map[hcid])
    except Exception:
        pass

    all_cids = sorted(set(seed_cids) | set(comp_by_cid.keys()))[:40]
    seed_cid_set = set(seed_cids)
    # Deduplicate foreign-branch registrations of the same company (e.g. PROPARK AMERICA WEST LLC
    # filed in CA, CO, CT, NV) into one canonical corporate filing entry while keeping `all_cids`
    # intact for officer/principal resolution below.
    grouped_oc_links: dict[str, dict] = {}
    for cid in sorted(all_cids, key=lambda x: (0 if x in seed_cid_set else 1, x)):
        r = comp_by_cid.get(cid) or {}
        j_code, c_num = cid.split("/", 1)
        hq_addr = None
        for cand_addr in (r.get("mailing_address_raw"), r.get("registered_address_raw")):
            if cand_addr and cand_addr != r.get("agent_address_raw") and not is_excluded_third_party_address(cand_addr):
                hq_addr = cand_addr
                break
        home_cid = (
            f"{r['home_jurisdiction_code']}/{r['home_company_number']}"
            if (r.get("home_jurisdiction_code") and r.get("home_company_number"))
            else None
        )
        cname = r.get("name") or cid
        cname_norm = normalize_business_name(cname) or cname.upper().strip()
        dedup_key = f"home:{home_cid}" if home_cid else f"name:{cname_norm}"
        existing = grouped_oc_links.get(dedup_key)
        if existing is None and home_cid:
            # Also coalesce with the home company entry itself if already inserted under its name
            existing = grouped_oc_links.get(f"name:{cname_norm}")
        if existing is not None:
            j_short = j_code.replace("us_", "").upper()
            if j_short and j_short != existing["jurisdiction_code"].replace("us_", "").upper():
                branches = existing.setdefault("branch_jurisdictions", [])
                if j_short not in branches:
                    branches.append(j_short)
            if not existing.get("registered_address") and hq_addr:
                existing["registered_address"] = hq_addr
            if not existing.get("registry_url") and r.get("registry_url"):
                existing["registry_url"] = r.get("registry_url")
            continue

        entry = {
            "id": cid,
            "jurisdiction_code": j_code,
            "company_number": c_num,
            "company_name": cname,
            "company_type": r.get("company_type"),
            "status": r.get("current_status"),
            "incorporation_date": str(r["incorporation_date"]) if r.get("incorporation_date") else None,
            "registered_address": hq_addr,
            "home_company_id": home_cid,
            "branch_jurisdictions": [],
            "registry_url": r.get("registry_url"),
            "url": r.get("opencorporates_url") or f"https://opencorporates.com/companies/{cid}",
        }
        grouped_oc_links[dedup_key] = entry
        grouped_oc_links.setdefault(f"name:{cname_norm}", entry)

    seen_link_ids = set()
    oc_links = []
    for entry in grouped_oc_links.values():
        if entry["id"] not in seen_link_ids:
            seen_link_ids.add(entry["id"])
            oc_links.append(entry)

    # Fetch beneficial principals & corporate parents across all linked OC companies
    oc_contacts: list[dict] = []
    if all_cids:
        j_all = [cid.split("/", 1)[0] for cid in all_cids]
        n_all = [cid.split("/", 1)[1] for cid in all_cids]
        ct_pl_rows = []
        try:
            ct_pl_rows = _query(
                """
                SELECT pl.jurisdiction_code, pl.company_number,
                       pl.canonical_principal_id, pl.canonical_name,
                       pl.position, pl.statewide_company_count, pl.name_variants
                FROM UNNEST(%s::text[], %s::text[]) AS k(j, n)
                JOIN oc_ct_principal_links pl
                  ON pl.jurisdiction_code = k.j AND pl.company_number = k.n
                LIMIT 250
                """,
                (j_all, n_all),
            )
        except Exception:
            ct_pl_rows = []

        ct_covered_cids = {f"{r['jurisdiction_code']}/{r['company_number']}" for r in ct_pl_rows}

        try:
            off_rows = _query(
                """
                SELECT o.jurisdiction_code, o.company_number, o.name, o.name_norm,
                       o.position, o.role_class, o.is_corporate_officer, o.address_raw
                FROM UNNEST(%s::text[], %s::text[]) AS k(j, n)
                JOIN oc_officers o ON o.jurisdiction_code = k.j AND o.company_number = k.n
                WHERE o.role_class IN ('beneficial_principal', 'corporate_parent')
                ORDER BY o.is_current DESC NULLS LAST, o.officer_row_id ASC
                LIMIT 250
                """,
                (j_all, n_all),
            )
        except Exception:
            off_rows = []

        grouped_off: dict[tuple[str, str], dict] = {}
        # 1. Populate BigQuery CT-deduplicated canonical human principals first
        for pr in ct_pl_rows:
            cname = (pr.get("canonical_name") or "").strip()
            pid = (pr.get("canonical_principal_id") or "").strip()
            if not cname or not pid or is_excluded_third_party_entity(cname):
                continue
            cid = f"{pr['jurisdiction_code']}/{pr['company_number']}"
            comp_row = comp_by_cid.get(cid) or {}
            comp_name = comp_row.get("name") or cid
            entry = grouped_off.setdefault(
                ("CORPORATE_PRINCIPAL", f"CTPID:{pid}"),
                {
                    "contact_type": "CORPORATE_PRINCIPAL",
                    "full_name": cname,
                    "corporation_self": None,
                    "corporations": set(),
                    "bbls": set(),
                    "registration_ids": set(),
                    "address": None,
                    "positions": [],
                    "statewide_company_count": int(pr.get("statewide_company_count") or 0) or None,
                    "name_variants": list(pr.get("name_variants") or []),
                },
            )
            entry["corporations"].add(comp_name)
            entry["registration_ids"].add(f"OC:{cid}")
            entry["bbls"].update(bbl_map.get(cid, set()))
            for pos_p in str(pr.get("position") or "").split(","):
                pos_c = pos_p.strip()
                if pos_c and pos_c not in entry["positions"]:
                    entry["positions"].append(pos_c)
            if not entry["address"]:
                for hq_cand in (comp_row.get("mailing_address_raw"), comp_row.get("registered_address_raw")):
                    if hq_cand and hq_cand != comp_row.get("agent_address_raw") and not is_excluded_third_party_address(hq_cand):
                        entry["address"] = hq_cand
                        break

        # 2. Populate corporate parents + fallback human principals for non-CT-synced companies
        for o in off_rows:
            raw_nm = (o.get("name") or "").strip()
            if not raw_nm or is_excluded_third_party_entity(raw_nm):
                continue
            cid = f"{o['jurisdiction_code']}/{o['company_number']}"
            comp_row = comp_by_cid.get(cid) or {}
            comp_name = comp_row.get("name") or cid
            is_corp = bool(o.get("is_corporate_officer")) or o.get("role_class") == "corporate_parent" or not looks_like_person_owner(raw_nm)
            if not is_corp and cid in ct_covered_cids:
                continue
            ctype = "CORPORATEOWNER" if is_corp else "CORPORATE_PRINCIPAL"
            norm_key = (o.get("name_norm") or raw_nm.upper()).strip()
            entry = grouped_off.setdefault(
                (ctype, norm_key),
                {
                    "contact_type": ctype,
                    "full_name": None if is_corp else raw_nm,
                    "corporation_self": raw_nm if is_corp else None,
                    "corporations": set(),
                    "bbls": set(),
                    "registration_ids": set(),
                    "address": None,
                    "positions": [],
                    "statewide_company_count": None,
                    "name_variants": [],
                },
            )
            entry["corporations"].add(comp_name)
            entry["registration_ids"].add(f"OC:{cid}")
            entry["bbls"].update(bbl_map.get(cid, set()))
            pos = (o.get("position") or "").strip()
            if pos and pos not in entry["positions"]:
                entry["positions"].append(pos)
            cand_addr = (o.get("address_raw") or "").strip()
            if not entry["address"] and cand_addr and not is_excluded_third_party_address(cand_addr):
                entry["address"] = cand_addr
            if not entry["address"]:
                for hq_cand in (comp_row.get("mailing_address_raw"), comp_row.get("registered_address_raw")):
                    if hq_cand and hq_cand != comp_row.get("agent_address_raw") and not is_excluded_third_party_address(hq_cand):
                        entry["address"] = hq_cand
                        break

        for (_ctype, _nkey), ent in grouped_off.items():
            corps_sorted = sorted(ent["corporations"])
            if ent["corporation_self"] and ent["corporation_self"] not in corps_sorted:
                corps_sorted.insert(0, ent["corporation_self"])
            bbls_sorted = sorted(ent["bbls"])
            contact_obj = {
                "contact_type": ent["contact_type"],
                "corporations": corps_sorted,
                "corporation": ent["corporation_self"] or (corps_sorted[0] if corps_sorted else None),
                "registration_ids": sorted(ent["registration_ids"]),
                "bbls": bbls_sorted,
                "building_count": len(bbls_sorted),
                "first_name": None,
                "last_name": None,
                "full_name": ent["full_name"],
                "address": ent["address"],
                "city": None,
                "state": None,
                "zip": None,
                "position": ", ".join(ent["positions"][:3]) if ent["positions"] else None,
                "source": "OpenCorporates",
            }
            if ent.get("statewide_company_count"):
                contact_obj["statewide_company_count"] = ent["statewide_company_count"]
            if len(ent.get("name_variants") or []) > 1:
                contact_obj["name_variants"] = ent["name_variants"]
            oc_contacts.append(contact_obj)

    return oc_links, dedupe_network_contacts(oc_contacts), all_cids


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
CITY_STATE_MAP = {
    "nyc": "NY",
    "dc": "DC",
    "baltimore": "MD",
    "boston": "MA",
    "detroit": "MI",
    "philadelphia": "PA",
    "chicago": "IL",
    "miami": "FL",
    "minneapolis": "MN",
    "nj": "NJ",
    "ma": "MA",
    "fl": "FL",
    "ny": "NY",
    "co": "CO",
    "vt": "VT",
    "wi": "WI",
    "nc": "NC",
    "ar": "AR",
    "va": "VA",
    "il": "IL",
    "ca": "CA",
    "mt": "MT",
    "mn": "MN",
    "oh": "OH",
}


def _format_property(r: dict, city: str | None = None) -> dict:
    oc_jur = r.get("oc_jurisdiction_code")
    oc_num = r.get("oc_company_number")
    raw_sale_price = _safe_num(r.get("last_sale_price"))
    return {
        "bbl":            r.get("bbl"),
        "city_dataset":   city,
        "property_state": CITY_STATE_MAP.get(city) if city else r.get("property_state"),
        "address":        r.get("address"),
        "borough":        r.get("borough"),
        "zip_code":       r.get("zip_code"),
        "owner_name":     r.get("owner_name"),
        "mailing_address": r.get("mailing_address"),
        "mailing_address_norm": r.get("mailing_address_norm"),
        "owner_email":    r.get("owner_email"),
        "land_use":       r.get("land_use"),
        "bld_class":      r.get("bld_class"),
        "num_floors":     _safe_num(r.get("num_floors")),
        "units_res":      _safe_int(r.get("units_res")),
        "units_total":    _safe_int(r.get("units_total")),
        "year_built":     _safe_int(r.get("year_built")),
        "assessed_total": _safe_num(r.get("assessed_total")),
        "last_sale_date": str(r["last_sale_date"]) if r.get("last_sale_date") else None,
        "last_sale_price": raw_sale_price if (raw_sale_price is not None and raw_sale_price > 0) else None,
        "latitude":       _safe_num(r.get("latitude")),
        "longitude":      _safe_num(r.get("longitude")),
        "last_reg_date":  str(r["last_registration_date"]) if r.get("last_registration_date") else None,
        "reg_end_date":   str(r["registration_end_date"])  if r.get("registration_end_date")  else None,
        "registration_id": r.get("registration_id"),
        "is_rent_stabilized": bool(r.get("is_rent_stabilized")) if r.get("is_rent_stabilized") is not None else None,
        "rs_units":       _safe_int(r.get("rs_units")),
        # Compliance
        "compliance_active": bool(r.get("compliance_active")) if r.get("compliance_active") is not None else None,
        "compliance_record_id": r.get("compliance_record_id"),
        "compliance_expiration": str(r["compliance_expiration"]) if r.get("compliance_expiration") else None,
        # Enrichment
        "violations_total":   _safe_int(r.get("violations_total")),
        "violations_open":    _safe_int(r.get("violations_open")),
        "violations_class_a": _safe_int(r.get("violations_class_a")),
        "violations_class_b": _safe_int(r.get("violations_class_b")),
        "violations_class_c": _safe_int(r.get("violations_class_c")),
        "violations_open_c":  _safe_int(r.get("violations_open_c")),
        "litigations_total": _safe_int(r.get("litigations_total")),
        "litigations_open":  _safe_int(r.get("litigations_open")),
        "litigations_harassment": _safe_int(r.get("litigations_harassment")),
        "evictions_total":   _safe_int(r.get("evictions_total")),
        "last_eviction_date": str(r["last_eviction_date"]) if r.get("last_eviction_date") else None,
        "nhpd_subsidy":    bool(r.get("nhpd_subsidy")) if r.get("nhpd_subsidy") is not None else None,
        "nhpd_program":    r.get("nhpd_program"),
        "nhpd_expiration": str(r["nhpd_expiration"]) if r.get("nhpd_expiration") else None,
        "oc_jurisdiction_code": oc_jur,
        "oc_company_number": oc_num,
        "oc_match_tier": _safe_int(r.get("oc_match_tier")),
        "oc_match_confidence": _safe_num(r.get("oc_match_confidence")),
        "opencorporates_url": f"https://opencorporates.com/companies/{oc_jur}/{oc_num}" if (oc_jur and oc_num) else None,
        "cama_site_link": r.get("cama_site_link"),
        "building_photo_url": r.get("building_photo_url"),
    }


def _format_contact(r: dict) -> dict:
    corps = r.get("corporations") or []
    return {
        "contact_type":   r.get("contact_type"),
        "corporations":   corps,                          # full list
        "corporation":    corps[0] if corps else None,    # first for backwards compat
        "registration_ids": r.get("registration_ids") or [],
        "bbls":           r.get("bbls") or [],
        "building_count": _safe_int(r.get("building_count")),
        "first_name":     r.get("first_name"),
        "last_name":      r.get("last_name"),
        "full_name":      r.get("full_name"),
        "address":        r.get("business_address"),
        "city":           r.get("business_city"),
        "state":          r.get("business_state"),
        "zip":            r.get("business_zip"),
    }


def _fetch_baltimore_official_section(layer: dict, bbl: str) -> dict:
    clean_bbl = (bbl or "").replace("'", "''").strip()
    where = f"{layer['key_field']} = '{clean_bbl}'"
    params = {
        "where": where,
        "outFields": ",".join(layer["fields"]),
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": 50,
    }
    if layer.get("date_field"):
        params["orderByFields"] = f"{layer['date_field']} DESC"
    try:
        response = requests.get(layer["url"], params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            raise RuntimeError(data["error"].get("message") or "Official source query failed.")
        records = [(feature.get("attributes") or {}) for feature in data.get("features", [])]
        return {
            "label": layer["label"],
            "fields": layer["fields"],
            "records": records,
            "source_url": response.url,
        }
    except Exception as e:
        logger.warning("Baltimore official record query failed for %s/%s: %s", layer["label"], bbl, e)
        return {
            "label": layer["label"],
            "fields": layer["fields"],
            "records": [],
            "source_url": layer["url"],
            "error": str(e),
        }


def _official_value(value, field_name: str = "") -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 8 and text.isdigit() and "DATE" in field_name.upper():
            return f"{text[:2]}/{text[2:4]}/{text[4:]}"
        return text
    if isinstance(value, (int, float)) and "DATE" in field_name.upper() and value > 100000000000:
        try:
            return time.strftime("%Y-%m-%d", time.gmtime(value / 1000))
        except Exception:
            return str(value)
    if isinstance(value, (int, float)) and "PRIC" in field_name.upper():
        try:
            return f"${int(value):,}"
        except Exception:
            return str(value)
    return str(value)


def _render_baltimore_official_records_html(prop: dict, sections: list[dict]) -> str:
    title = html.escape(prop.get("address") or prop.get("bbl") or "Baltimore property")
    subtitle = html.escape(" · ".join(part for part in [prop.get("bbl"), prop.get("owner_name")] if part) or "Official Baltimore records")
    section_html = []
    for section in sections:
        label = html.escape(section["label"])
        source_url = html.escape(section.get("source_url") or "")
        if section.get("error"):
            section_html.append(f"""
              <section>
                <div class="section-head">
                  <h2>{label}</h2>
                  <a href="{source_url}" target="_blank" rel="noreferrer">Source</a>
                </div>
                <div class="empty">Official source query failed for this layer.</div>
              </section>
            """)
            continue
        rows = []
        for record in section["records"]:
            cells = []
            for field in section["fields"]:
                value = html.escape(_official_value(record.get(field), field))
                rendered_value = value if value else '<span class="muted">—</span>'
                cells.append(f"<td>{rendered_value}</td>")
            rows.append("<tr>" + "".join(cells) + "</tr>")
        headers = "".join(f"<th>{html.escape(field)}</th>" for field in section["fields"])
        body = "".join(rows)
        count = len(section["records"])
        section_html.append(f"""
          <section>
            <div class="section-head">
              <div>
                <h2>{label}</h2>
                <p>{count} official record{'s' if count != 1 else ''}</p>
              </div>
              <a href="{source_url}" target="_blank" rel="noreferrer">Source</a>
            </div>
            <div class="table-wrap">
              <table>
                <thead><tr>{headers}</tr></thead>
                <tbody>{body}</tbody>
              </table>
            </div>
          </section>
        """)

    if not section_html:
        section_html.append("<section><div class=\"empty\">No official records returned for this parcel.</div></section>")

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {{
      color-scheme: light;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f8fafc;
      color: #0f172a;
    }}
    body {{ margin: 0; background: #f8fafc; }}
    header {{
      position: sticky;
      top: 0;
      z-index: 2;
      padding: 14px 16px 12px;
      background: rgba(255,255,255,.96);
      border-bottom: 1px solid #e2e8f0;
      backdrop-filter: blur(8px);
    }}
    h1 {{ margin: 0; font-size: 15px; line-height: 1.25; font-weight: 800; }}
    header p {{ margin: 4px 0 0; color: #64748b; font-size: 11px; font-weight: 600; }}
    main {{ padding: 12px; display: grid; gap: 12px; }}
    section {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; }}
    .section-head {{ display: flex; justify-content: space-between; gap: 12px; align-items: center; padding: 10px 12px; background: #f8fafc; border-bottom: 1px solid #e2e8f0; }}
    h2 {{ margin: 0; font-size: 12px; font-weight: 800; }}
    .section-head p {{ margin: 2px 0 0; color: #64748b; font-size: 10px; font-weight: 700; }}
    a {{ color: #2563eb; font-size: 10px; font-weight: 800; text-decoration: none; text-transform: uppercase; letter-spacing: .04em; }}
    a:hover {{ text-decoration: underline; }}
    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; min-width: 620px; border-collapse: collapse; font-size: 11px; }}
    th {{ text-align: left; padding: 8px 10px; color: #64748b; background: #fff; border-bottom: 1px solid #e2e8f0; font-size: 9px; text-transform: uppercase; letter-spacing: .04em; white-space: nowrap; }}
    td {{ padding: 8px 10px; border-bottom: 1px solid #f1f5f9; vertical-align: top; }}
    tr:last-child td {{ border-bottom: 0; }}
    .muted, .empty {{ color: #94a3b8; }}
    .empty {{ padding: 18px 12px; font-size: 12px; font-weight: 700; text-align: center; }}
  </style>
</head>
<body>
  <header>
    <h1>{title}</h1>
    <p>{subtitle}</p>
  </header>
  <main>
    {''.join(section_html)}
  </main>
</body>
</html>"""


def _borough_summary_text(summary) -> str:
    if not summary:
        return ""
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            return summary
    if isinstance(summary, dict):
        parts = sorted(summary.items(), key=lambda x: -x[1])
        return " · ".join(f"{b} ({n})" for b, n in parts[:4])
    return ""


def _safe_num(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _safe_int(v):
    try:
        return int(float(v)) if v is not None else None
    except Exception:
        return None
