import os
import re
import sys
import time
import json
import logging
import threading
import requests
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

from api.shared_utils import normalize_business_name, normalize_person_name, get_name_variations, BUSINESS_SUFFIX_PATTERNS, canonicalize_person_name, canonicalize_business_name

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_batch

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Optional Gemini import (AI report). App still runs without it.
try:
    import google.generativeai as genai  # type: ignore
except Exception:  # pragma: no cover
    genai = None  # type: ignore

# ------------------------------------------------------------
# App / Config
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("they-own-what")

DATABASE_URL = os.environ.get("DATABASE_URL")
GEMINI_KEY = os.environ.get("GEMINI_KEY")
SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY")  # reserved for future use
DEFAULT_STATE = os.environ.get("DEFAULT_STATE", "CT")

if genai and GEMINI_KEY:
    genai.configure(api_key=GEMINI_KEY)


app = FastAPI(title="they own WHAT?? API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Mount static files for scraped images
# Use absolute path valid inside container
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/api/static", StaticFiles(directory=static_dir), name="static")

@app.get("/api/health")
def health_check():
    # Check if Gemini key is present and NOT the placeholder
    ai_key = os.environ.get("GEMINI_KEY", "")
    ai_enabled = bool(ai_key and "REPLACE_WITH_API_KEY" not in ai_key)
    return {"status": "ok", "timestamp": time.time(), "ai_enabled": ai_enabled}

@app.get("/api/features")
def features():
    """Feature flags based on environment configuration"""
    return {
        "eviction_tools_enabled": os.environ.get("CT_EVICTIONS_ENABLED", "true").lower() != "false",
    }

from api.feedback import router as feedback_router
app.include_router(feedback_router)

from api.city_routes import router as city_router
app.include_router(city_router)

import api.db as db_module
from api.db import init_db_pool, get_db_connection

# Lock file path (same as in build_networks.py)
LOCK_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'maintenance.lock')

@app.get("/api/system/status")
def get_system_status():
    """Checks if the system is in maintenance mode (rebuilding networks)."""
    is_maintenance = os.path.exists(LOCK_FILE_PATH)
    return {"maintenance": is_maintenance}


# ------------------------------------------------------------
# Helpers (make available to all endpoints)
# ------------------------------------------------------------
import decimal
from contextlib import contextmanager

@contextmanager
def cursor_context():
    if db_module.db_pool is None:
        raise Exception("Database connection unavailable")
    conn = db_module.db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_module.db_pool.putconn(conn)


def _extract_street_address(address: str) -> str:
    """
    Experimental: Remove unit/apartment numbers to get the 'base' street address.
    e.g. '123 MAIN ST UNIT 4' -> '123 MAIN ST'
    """
    if not address:
        return ""

    # Check for unit keywords.
    # Use \b (word bound) or (?=\d) (followed by digit) to avoid partial matches
    # on street names (e.g. 'FL' matching 'FLORENCE').
    # regex matches: space + (keyword) + (boundary/digit) + space? + (alphanumeric/dash) until end

    pattern = r'(?:,|\s+)\s*(?:(?:UNIT|APT|APARTMENT|SUITE|STE|FL|FLOOR|RM|ROOM)(?:\b|(?=\d))|#)\s*[\w\d-]+$'

    clean = re.sub(pattern, '', address, flags=re.IGNORECASE).strip()
    return clean

def is_likely_street_address(addr: str) -> bool:
    """
    Heuristic: Valid street addresses usually start with a digit (house number)
    AND have at least one text part (street name).
    Avoids grouping outliers like '93' or '0'.
    """
    if not addr: return False
    addr = addr.strip()
    if not addr[0].isdigit():
        return False

    parts = addr.split()
    if len(parts) < 2:
        return False

    return True


def get_property_subsidies(cursor, property_id: int) -> List[Dict[str, Any]]:
    """Fetch subsidies for a specific property."""
    cursor.execute("""
        SELECT program_name, subsidy_type, units_subsidized, expiry_date, source_url
        FROM property_subsidies
        WHERE property_id = %s
    """, (property_id,))
    return [dict(row) for row in cursor.fetchall()]

CT_LAT_MIN, CT_LAT_MAX = 40.8, 42.3
CT_LON_MIN, CT_LON_MAX = -73.9, -71.6
US_LAT_MIN, US_LAT_MAX = 24.0, 50.0
US_LON_MIN, US_LON_MAX = -125.0, -66.0

def _is_finite_coord(value: Any) -> bool:
    try:
        return value is not None and value == value and abs(float(value)) != float("inf")
    except Exception:
        return False

def _looks_like_ny_property(p: Dict[str, Any]) -> bool:
    source = str(p.get("source") or "").upper()
    return source == "NYS_OPEN_DATA" or bool(p.get("bbl") or p.get("borough"))

def valid_property_coordinates(lat: Any, lon: Any, p: Optional[Dict[str, Any]] = None) -> bool:
    if not (_is_finite_coord(lat) and _is_finite_coord(lon)):
        return False
    lat_f = float(lat)
    lon_f = float(lon)
    if (lat_f, lon_f) in {(0.0, 0.0), (-1.0, -1.0)}:
        return False
    if not (US_LAT_MIN <= lat_f <= US_LAT_MAX and US_LON_MIN <= lon_f <= US_LON_MAX):
        return False
    if p is not None and not _looks_like_ny_property(p):
        return CT_LAT_MIN <= lat_f <= CT_LAT_MAX and CT_LON_MIN <= lon_f <= CT_LON_MAX
    return True

def sanitize_property_coordinates(p: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    lat = p.get("latitude")
    lon = p.get("longitude")
    if valid_property_coordinates(lat, lon, p):
        return float(lat), float(lon)
    return None, None

def shape_property_row(p: dict, subsidies: List[dict] = None) -> dict:
    """Normalize a property DB row into the shape the frontend expects."""
    # Only use normalized_address if it looks like an address (starts with digit).
    # Otherwise it might be a POI name from geocoding (e.g. 'Clifford Beers') which breaks grouping.
    norm_addr = p.get("normalized_address")
    if norm_addr and not is_likely_street_address(norm_addr):
        norm_addr = None

    lat, lon = sanitize_property_coordinates(p)

    return {
        "id": p.get("id"),
        "address": p.get("location") or "",
        "city": p.get("property_city") or "",
        "owner": p.get("owner") or "",
        "assessed_value": (
            f"${int(p['assessed_value']):,}" if p.get("assessed_value") is not None else None
        ),
        "appraised_value": (
            f"${int(p['appraised_value']):,}" if p.get("appraised_value") is not None else None
        ),
        "unit": p.get("unit"),
        "number_of_units": p.get("number_of_units"),
        "latitude": lat,
        "longitude": lon,
        "normalized_address": norm_addr,
        "complex_name": p.get("complex_name"),
        "management_company": p.get("management_company"),
        "subsidies": subsidies or [],
        "violation_count": p.get("violation_count", 0),
        "code_enforcement_count": p.get("code_enforcement_count", p.get("violation_count", 0)),
        "open_code_enforcement_count": p.get("open_code_enforcement_count", p.get("violation_count", 0)),
        "last_code_enforcement_date": p.get("last_code_enforcement_date"),
        "eviction_count": p.get("eviction_count", 0),
        "details": p,  # keep full row for drill-down
    }


def group_properties_into_complexes(properties: List[dict]) -> List[dict]:
    """
    Group properties by street address into complexes.
    - Main row shows street address with count of units
    - Sub-rows show individual units with their owners

    Uses the 'location' and 'property_city' fields to group properties.
    """
    from collections import defaultdict

    # Group by (location, city) tuple
    complexes_map = defaultdict(list)

    for prop in properties:
        # Use normalized_address if available AND valid, otherwise location + city
        raw_norm = (prop.get("normalized_address") or "").strip()
        if raw_norm and not is_likely_street_address(raw_norm):
            raw_norm = ""

        location = (prop.get("location") or "").strip()
        city = (prop.get("property_city") or "").strip()

        # Priority to normalized address, but fall back to raw location
        # CRITICAL CHANGE: Always strip unit numbers for grouping purposes
        raw_grouping_str = raw_norm
        if not raw_grouping_str:
             if is_likely_street_address(location):
                 raw_grouping_str = location

        if not raw_grouping_str:
            continue

        base_address = _extract_street_address(raw_grouping_str)
        grouping_key = (base_address, city)

        complexes_map[grouping_key].append(prop)

    # Fetch management info for all grouping keys in one go if possible
    # For now, we'll do individual lookups or a batch lookup if we have the keys
    group_keys = list(complexes_map.keys())
    mgt_info = {}
    if group_keys:
        try:
            with cursor_context() as cur:
                # Use a tuple for the where clause: ((addr1, city1), (addr2, city2), ...)
                # PostgreSQL supports this syntax: WHERE (street_address, city) IN (('addr1', 'city1'), ...)
                placeholders = []
                params = []
                for addr, city in group_keys:
                    placeholders.append("(%s, %s)")
                    params.extend([addr, city])

                if placeholders:
                    query = f"SELECT street_address, city, management_name, official_url, phone FROM complex_management WHERE (street_address, city) IN ({', '.join(placeholders)})"
                    cur.execute(query, params)
                    for r in cur.fetchall():
                        mgt_info[(r['street_address'], r['city'])] = r
        except Exception as e:
            logger.warning(f"Failed to fetch management info: {e}")

    # Build result with complexes
    result = []

    # Pre-fetch subsidies for all properties in this batch
    all_property_ids = [p['id'] for units in complexes_map.values() for p in units]
    subsidies_map = defaultdict(list)
    if all_property_ids:
        try:
            with cursor_context() as cur:
                cur.execute("""
                    SELECT property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url
                    FROM property_subsidies
                    WHERE property_id = ANY(%s)
                """, (all_property_ids,))
                for row in cur.fetchall():
                    subsidies_map[row['property_id']].append(dict(row))
        except Exception as e:
            logger.warning(f"Failed to fetch subsidies: {e}")

    for (street_address, city), units in sorted(complexes_map.items()):
        mgt = mgt_info.get((street_address, city))

        # Aggregate complex level info from first unit (NHPD data usually consistent for complex)
        complex_name = next((u.get('complex_name') for u in units if u.get('complex_name')), None)
        management_co = next((u.get('management_company') for u in units if u.get('management_company')), None)

        if len(units) > 1:
            # This is a complex - create parent row with children
            total_assessed = sum(
                (p.get("assessed_value") or 0) for p in units
            )

            display_addr = street_address
            complex_lat, complex_lon = sanitize_property_coordinates(units[0])

            # Aggregate subsidies for the complex
            complex_subsidies = []
            seen_subsidy_keys = set()
            for u in units:
                for s in subsidies_map.get(u['id'], []):
                    key = (s['program_name'], s['subsidy_type'], s['expiry_date'])
                    if key not in seen_subsidy_keys:
                        complex_subsidies.append(s)
                        seen_subsidy_keys.add(key)

            complex_row = {
                "id": f"complex_{hash((street_address, city))}",
                "address": display_addr,
                "city": city,
                "owner": f"{units[0].get('owner', 'Multiple')} (+{len(units)-1} others)",
                "assessed_value": f"${int(total_assessed):,}" if total_assessed else None,
                "unit_count": sum(u.get("number_of_units") or 1 for u in units),
                "is_complex": True,
                "units": [u for u in units],
                "latitude": complex_lat,
                "longitude": complex_lon,
                "normalized_address": display_addr,
                "complex_name": complex_name,
                "management_company": management_co,
                "subsidies": complex_subsidies,
                "management_info": {
                    "name": mgt['management_name'] if mgt else management_co, # prefer scraped mgt if available, else NHPD
                    "url": mgt['official_url'] if mgt else None,
                    "phone": mgt['phone'] if mgt else None
                }
            }
            # Deep shape the units
            complex_row["units"] = [shape_property_row(u, subsidies_map.get(u['id'], [])) for u in units]
            result.append(complex_row)
        else:
            # Single property, no grouping needed
            # Pass subsidies for this property
            res = shape_property_row(units[0], subsidies_map.get(units[0]['id'], []))
            if mgt:
                res["management_info"] = {
                    "name": mgt['management_name'],
                    "url": mgt['official_url'],
                    "phone": mgt['phone']
                }
            result.append(res)

    return result



def json_converter(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    if isinstance(o, decimal.Decimal):
        return float(o)
    return str(o)

# Normalization functions and variations are now imported from shared_utils.py

def find_properties_for_entity(cursor, entity_name: str, entity_type: str) -> List[Dict[str, Any]]:
    """Robust match on normalized owner/co-owner for principal or business name."""
    if not entity_name:
        return []
    et = "principal" if entity_type in ("owner", "principal") else "business"
    vars_ = get_name_variations(entity_name, et)
    norm_variants = list({normalize_person_name(v) for v in vars_ if v})
    if not norm_variants:
        return []
    cursor.execute(
        """
        SELECT *
        FROM properties
        WHERE owner_norm = ANY(%s) OR co_owner_norm = ANY(%s)
        """,
        (norm_variants, norm_variants)
    )
    return cursor.fetchall()

def _ndjson(obj: dict) -> bytes:
    return (json.dumps(obj, default=str) + "\n").encode("utf-8")


# ------------------------------------------------------------
# DB bootstrap (idempotent)
# ------------------------------------------------------------
DDL_NORMALIZE_FUNCTION = r"""
CREATE OR REPLACE FUNCTION normalize_person_name(input_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    n TEXT := COALESCE(input_name, '');
BEGIN
    n := UPPER(n);
    n := regexp_replace(n, '[`"''.]', '', 'g');
    n := regexp_replace(n, '\s+(JR|SR|III|IV|II|ESQ|MD|PHD|DDS)$', '', 'g');
    n := regexp_replace(n, '\s+', ' ', 'g');
    n := trim(n);
    IF position(',' IN n) > 0 THEN
        n := regexp_replace(n, '^\s*([^,]+)\s*,\s*([A-Z0-9\- ]+).*$','\2 \1');
        n := regexp_replace(n, '\s+', ' ', 'g');
        n := trim(n);
    END IF;
    n := regexp_replace(n, '\s+(JR|SR|III|IV|II|ESQ|MD|PHD|DDS)$', '', 'g');
    IF array_length(regexp_split_to_array(n, '\s+'), 1) > 2 THEN
        n := regexp_replace(n, '(^|\s)[A-Z](\s|$)', ' ', 'g');
        n := regexp_replace(n, '\s+', ' ', 'g');
        n := trim(n);
    END IF;
    RETURN n;
END;
$$;
"""

DDL_ADD_OWNER_NORM = """
ALTER TABLE properties ADD COLUMN IF NOT EXISTS owner_norm TEXT;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS co_owner_norm TEXT;
"""

DDL_INDEXES = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_properties_owner_gin
    ON properties USING gin (owner gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_properties_location_gin
    ON properties USING gin (location gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_properties_owner_norm_gin
    ON properties USING gin (owner_norm gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_properties_co_owner_norm_gin
    ON properties USING gin (co_owner_norm gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_businesses_name_gin
    ON businesses USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_principals_name_c_gin
    ON principals USING gin (name_c gin_trgm_ops);
"""

DDL_OWNERSHIP_TABLES = """
CREATE TABLE IF NOT EXISTS ownership_networks (
    id SERIAL PRIMARY KEY,
    root_entity_id TEXT NOT NULL,
    root_entity_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ownership_links (
    network_id INTEGER REFERENCES ownership_networks(id) ON DELETE CASCADE,
    from_entity TEXT NOT NULL,
    to_entity TEXT NOT NULL,
    link_type TEXT NOT NULL,
    PRIMARY KEY (network_id, from_entity, to_entity)
);
"""

DDL_ADD_GEO_COLUMNS = """
ALTER TABLE properties ADD COLUMN IF NOT EXISTS latitude NUMERIC;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS longitude NUMERIC;
"""

# Cached AI reports
DDL_AI_REPORTS = """
CREATE TABLE IF NOT EXISTS ai_reports (
    id SERIAL PRIMARY KEY,
    entity TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    report_date DATE NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    sources JSONB,
    created_at TIMESTAMP DEFAULT now(),
    UNIQUE (entity, entity_type, report_date)
);
CREATE INDEX IF NOT EXISTS idx_ai_reports_entity
    ON ai_reports(entity, entity_type, report_date);
"""

# Simple Key-Value store for caching complex objects like insights
DDL_KV_CACHE = """
CREATE TABLE IF NOT EXISTS kv_cache (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def backfill_owner_norm_columns(conn) -> None:
    with conn.cursor() as c:
        logger.info("Backfilling owner_norm / co_owner_norm where NULL...")
        c.execute("""
            UPDATE properties
            SET owner_norm = normalize_person_name(owner)
            WHERE owner IS NOT NULL AND owner_norm IS NULL
        """)
        logger.info("Rows updated (owner_norm): %s", c.rowcount)
        c.execute("""
            UPDATE properties
            SET co_owner_norm = normalize_person_name(co_owner)
            WHERE co_owner IS NOT NULL AND co_owner_norm IS NULL
        """)
        logger.info("Rows updated (co_owner_norm): %s", c.rowcount)
    conn.commit()


@app.on_event("startup")
def startup_event():
    init_db_pool()

    conn = db_module.db_pool.getconn()
    try:
        with conn.cursor() as c:
            # c.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            # # c.execute("DROP FUNCTION IF EXISTS normalize_person_name(TEXT)")
            # c.execute(DDL_NORMALIZE_FUNCTION)
            # c.execute(DDL_ADD_OWNER_NORM)
            # c.execute(DDL_ADD_GEO_COLUMNS)
            # c.execute(DDL_OWNERSHIP_TABLES)
            # c.execute(DDL_AI_REPORTS)
            # c.execute(DDL_KV_CACHE)
            # c.execute(DDL_INDEXES)
            pass
        conn.commit()
        # backfill_owner_norm_columns(conn) # Commented out to prevent startup block
        logger.info("✅ Startup DB bootstrap completed.")

        # logger.info("Triggering initial insights cache refresh in the background...")
        # thread = threading.Thread(target=_update_insights_cache_sync, daemon=True)
        # thread.start()

    finally:
        db_module.db_pool.putconn(conn)


# get_db_connection imported from api.db


# ------------------------------------------------------------
# Models
# ------------------------------------------------------------
class SearchResult(BaseModel):
    id: str
    name: str
    type: str
    context: Optional[str] = None

class Entity(BaseModel):
    id: str
    name: str
    type: str
    details: Optional[Dict[str, Any]] = None

class PropertyItem(BaseModel):
    address: Optional[str]
    unit: Optional[str] = None
    city: Optional[str]
    owner: Optional[str]
    assessed_value: Optional[float]
    details: Dict[str, Any]
    subsidies: Optional[List[Dict[str, Any]]] = []
    complex_name: Optional[str] = None
    management_company: Optional[str] = None
    violation_count: Optional[int] = 0

class NetworkStep(BaseModel):
    entity_id: str
    entity_type: str
    depth: int = 1

class IncrementalNetworkResponse(BaseModel):
    new_entities: List[Entity]
    new_properties: List[PropertyItem]
    new_links: Dict[str, List[str]]
    has_more: bool
    next_entities: List[Dict[str, str]]

class ReportItem(BaseModel):
    key: str
    value: str

class Report(BaseModel):
    title: str
    data: List[ReportItem]

class AIReportRequest(BaseModel):
    entity: str
    entity_type: str  # 'owner' | 'business'
    force: bool = False
    length: Optional[str] = "comprehensive"
    directive: Optional[str] = ""
    research_entities: Optional[List[str]] = []

class CachedReportInfo(BaseModel):
    norm_name: str
    entity_name: str
    created_at: datetime
    size: int

class NetworkLoadRequest(BaseModel):
    entity_id: str
    entity_type: str
    entity_name: Optional[str] = None

class PrincipalInfo(BaseModel):
    name: str
    state: Optional[str] = None

class BusinessInfo(BaseModel):
    name: str
    state: Optional[str] = None

class InsightItem(BaseModel):
    rank: Optional[int] = None
    network_id: Optional[str] = None
    entity_id: str
    entity_name: str
    entity_type: str
    value: int
    property_count: Optional[int] = None
    total_assessed_value: Optional[float] = None
    total_appraised_value: Optional[float] = None
    building_count: Optional[int] = 0
    unit_count: Optional[int] = 0
    violation_count: Optional[int] = 0
    eviction_count: Optional[int] = 0
    business_name: Optional[str] = None
    business_count: Optional[int] = 0
    principal_count: Optional[int] = 0
    linked_business_count: Optional[int] = 0
    primary_entity_id: Optional[str] = None
    primary_entity_name: Optional[str] = None
    primary_entity_type: Optional[str] = None
    principals: Optional[List[PrincipalInfo]] = None
    representative_entities: Optional[List[BusinessInfo]] = None

class DashboardNetworkMetricTarget(BaseModel):
    key: str
    entity_id: str
    entity_type: str
    entity_name: Optional[str] = None
    network_id: Optional[str] = None

class DashboardNetworkMetricsRequest(BaseModel):
    targets: List[DashboardNetworkMetricTarget]

class CodeEnforcementItem(BaseModel):
    case_number: str
    record_name: Optional[str]
    record_status: Optional[str]
    date_opened: Optional[date]
    date_closed: Optional[date]
    inspection_type: Optional[str]
    record_type: Optional[str]

class EvictionItem(BaseModel):
    filing_date: Optional[date]
    status: Optional[str]

class HartfordPlaygroundItem(BaseModel):
    network_id: int
    entity_id: str
    entity_name: str
    entity_type: str
    selected_city: Optional[str] = None
    code_data_available: bool = True
    network_business_count: int = 0
    network_principal_count: int = 0
    property_count: int
    violation_count: int
    entity_violation_count: int = 0
    closed_violation_count: int = 0
    entity_closed_violation_count: int = 0
    eviction_count: int
    entity_eviction_count: int = 0
    active_violation_count: int
    entity_active_violation_count: int = 0
    violations_last_90d: int = 0
    violations_last_365d: int = 0
    entity_violations_last_90d: int = 0
    entity_violations_last_365d: int = 0
    evictions_last_90d: int = 0
    evictions_last_365d: int = 0
    entity_evictions_last_90d: int = 0
    entity_evictions_last_365d: int = 0
    evictions_prev_365d: int = 0
    eviction_surge_flag: bool = False
    eviction_surge_date: Optional[date]
    eviction_surge_filings: int = 0
    eviction_surge_avg_daily: float = 0
    eviction_surge_multiplier: float = 0
    attorney_surge_flag: bool = False
    attorney_surge_name: Optional[str] = None
    attorney_surge_date: Optional[date]
    attorney_surge_filings: int = 0
    attorney_surge_avg_daily: float = 0
    attorney_surge_multiplier: float = 0

    active_eviction_count: int = 0
    closed_eviction_count: int = 0
    entity_active_eviction_count: int = 0
    entity_closed_eviction_count: int = 0
    local_eviction_count: int = 0
    local_evictions_last_90d: int = 0
    local_evictions_last_365d: int = 0
    outside_eviction_count: int = 0
    outside_evictions_last_90d: int = 0
    outside_evictions_last_365d: int = 0
    entity_local_eviction_count: int = 0
    entity_outside_eviction_count: int = 0
    violation_type_breakdown: Optional[List[Dict[str, Any]]] = []
    violation_status_breakdown: Optional[List[Dict[str, Any]]] = []
    eviction_status_breakdown: Optional[List[Dict[str, Any]]] = []
    violation_businesses: Optional[List[str]] = []
    last_violation_date: Optional[date]
    last_eviction_date: Optional[date]
    principals: List[PrincipalInfo]


class MonitorItem(BaseModel):
    """Flexible monitor item for all dimensions (network, llc, attorney)."""
    dimension_type: str  # network, llc, attorney
    dimension_key: str  # network_id, plaintiff_norm, attorney_name
    dimension_label: str  # display name
    selected_city: Optional[str] = None
    code_data_available: bool = False
    eviction_data_available: bool = False
    property_count: int = 0
    business_names: Optional[List[str]] = []
    principals: Optional[List[PrincipalInfo]] = []
    # Eviction metrics
    eviction_count: int = 0
    evictions_last_365d: int = 0
    evictions_last_90d: int = 0
    active_eviction_count: int = 0
    closed_eviction_count: int = 0
    local_eviction_count: int = 0
    outside_eviction_count: int = 0
    # Disposition counts
    default_judgment_count: int = 0
    withdrawal_count: int = 0
    # Code enforcement (Hartford only)
    violation_count: int = 0
    active_violation_count: int = 0
    closed_violation_count: int = 0
    violations_last_365d: int = 0
    # Metadata
    last_eviction_date: Optional[date] = None
    last_violation_date: Optional[date] = None
    network_id: Optional[int] = None
    network_business_count: int = 0
    network_principal_count: int = 0
    earliest_filing_year: Optional[int] = None
    avg_case_duration_days: Optional[float] = None


class BurstDetectorItem(BaseModel):
    dimension_key: str
    dimension_label: str
    dimension_type: str  # city, street, landlord, network, attorney
    peak_week: Optional[date] = None
    filings_count: int = 0
    baseline_avg: float = 0
    multiplier: float = 0
    total_filings: int = 0
    disposition_breakdown: Optional[List[Dict[str, Any]]] = None
    network_id: Optional[int] = None
    entity_id: Optional[str] = None
    entity_type: Optional[str] = None




# ------------------------------------------------------------
# BATCH GEOCODING
# ------------------------------------------------------------
from api.geocoding_utils import geocode_census, geocode_nominatim

class GeocodeResult(BaseModel):
    id: str
    lat: float
    lon: float

class BatchGeocodeRequest(BaseModel):
    property_ids: List[int]

@app.post("/api/geocoding/batch", response_model=List[GeocodeResult])
def batch_geocode_properties(req: BatchGeocodeRequest, conn=Depends(get_db_connection)):
    """
    Parallel geocoding for on-the-fly requests.
    """
    if not req.property_ids:
        return []

    logger.info(f"Batch geocoding request for {len(req.property_ids)} properties.")
    results = []

    # 1. Fetch address info for these IDs if they don't have coords
    to_process = []

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            cursor.execute("""
                SELECT id, location, property_city, property_zip, latitude, longitude, source
                FROM properties
                WHERE id = ANY(%s::bigint[])
            """, (req.property_ids,))

            rows = cursor.fetchall()
        except Exception as e:
            logger.error(f"Database error in batch_geocode_properties: {e}")
            raise HTTPException(status_code=500, detail="Database query failed during batch geocoding")

        for r in rows:
            # If already has coords, return them
            existing_lat, existing_lon = sanitize_property_coordinates(r)
            if existing_lat is not None and existing_lon is not None:
                results.append(GeocodeResult(id=str(r['id']), lat=existing_lat, lon=existing_lon))
            elif r['location']:
                # Needs geocoding
                to_process.append(r)

    # 2. Process in parallel
    if to_process:
        with ThreadPoolExecutor(max_workers=50) as executor: # Higher workers for IO bound
            future_to_id = {}
            for row in to_process:
                address_full = f"{row['location']}, {row['property_city'] or ''}, {DEFAULT_STATE} {row['property_zip'] or ''}".strip()
                future = executor.submit(geocode_census, address_full)
                future_to_id[future] = (row['id'], address_full)

            # Collect results
            updates = []
            for future in as_completed(future_to_id):
                pid, addr = future_to_id[future]
                try:
                    lat, lon = future.result()
                    if not lat:
                         # Fallback to Nominatim (sequential inside the thread or just call it)
                         # Note: Nominatim is strictly rate limited. doing it in parallel threads might get banned.
                         # Ideally we skip nominatim in parallel batch or do it very carefully.
                         # For now, let's try it with a lock or just skip it to be safe and fast.
                         # Getting blocked by Nominatim would break the app.
                         # Let's try one attempt.
                         lat, lon = geocode_nominatim(addr)

                    row = next((r for r in to_process if r["id"] == pid), {})
                    if lat and lon and valid_property_coordinates(lat, lon, row):
                        results.append(GeocodeResult(id=str(pid), lat=float(lat), lon=float(lon)))
                        updates.append((float(lat), float(lon), pid))
                    elif lat and lon:
                        logger.warning("Rejected out-of-bounds geocode for property %s at %s: %s,%s", pid, addr, lat, lon)
                except Exception as e:
                    logger.error(f"Error geocoding {pid}: {e}")

            # 3. Bulk Update DB
            if updates:
                with conn.cursor() as cursor:
                    psycopg2.extras.execute_batch(cursor, """
                        UPDATE properties SET latitude = %s, longitude = %s WHERE id = %s
                    """, updates)
                conn.commit()

    return results

# ------------------------------------------------------------
# AI ANALYSIS
# ------------------------------------------------------------
@app.get("/api/ai_analysis")
def get_ai_analysis(entity_name: str, entity_type: str):
    """
    1. Search Google via SerpAPI for news/context.
    2. If OpenAI available, summarize finding.
    """
    if not SERPAPI_API_KEY:
        return {"summary": "SerpAPI not configured.", "sources": [], "risk": "Unknown"}

    # Construct query
    query = f"{entity_name} Connecticut real estate"
    if entity_type == 'business':
        query += " business LLC"
    else:
        query += " landlord property owner"

    # Call SerpAPI
    try:
        params = {
            "q": query,
            "api_key": SERPAPI_API_KEY,
            "tbm": "nws", # News search
            "num": 5
        }
        resp = requests.get("https://serpapi.com/search", params=params)
        data = resp.json()

        # Fallback to web search if no news
        if "error" in data or not data.get("news_results"):
             params.pop("tbm")
             resp = requests.get("https://serpapi.com/search", params=params)
             data = resp.json()

        results = data.get("news_results", []) or data.get("organic_results", [])

        snippets = []
        sources = []
        for r in results[:5]:
            title = r.get("title", "")
            snippet = r.get("snippet", "")
            link = r.get("link", "")
            source = r.get("source", "Web")
            snippets.append(f"- {title}: {snippet}")
            sources.append({"title": title, "link": link, "source": source})

        if not snippets:
            return {"summary": "No public news records found.", "sources": [], "risk": "Unknown"}

        # Summarize with Gemini
        summary_text = "Found recent mentions."
        risk_level = "Unknown"

        if genai and GEMINI_KEY:
            try:
                system_prompt = (
                    "You are a real estate investigator. Analyze these search snippets about a landlord/entity. "
                    "Provide a 1-2 sentence summary using only the snippets provided. Mention legal issues or controversies only when directly supported by a snippet. "
                    "Classify risk as Low, Moderate, or High only from source-backed evidence; otherwise say risk is Unknown."
                )
                user_msg = f"Entity: {entity_name}\nSnippets:\n" + "\n".join(snippets)

                model = genai.GenerativeModel('gemini-3.5-flash', system_instruction=system_prompt)
                resp = model.generate_content(
                    user_msg,
                    generation_config=genai.types.GenerationConfig(
                        max_output_tokens=150,
                        temperature=0.3
                    )
                )
                content = resp.text.strip()
                summary_text = content
                if "High" in content: risk_level = "High"
                elif "Moderate" in content: risk_level = "Moderate"
                elif "Low" in content: risk_level = "Low"
                else: risk_level = "Unknown"
            except Exception as e:
                logger.error(f"Gemini error: {e}")
                summary_text = "AI Summary unavailable. Reference sources below."

        return {
            "summary": summary_text,
            "sources": [s['link'] for s in sources], # Simplified for frontend
            "risk": risk_level
        }

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        return {"summary": "Analysis failed.", "sources": [], "risk": "Unknown"}


# ------------------------------------------------------------
# AUTOCOMPLETE
# ------------------------------------------------------------
# ------------------------------------------------------------
# AUTOCOMPLETE
# ------------------------------------------------------------
@app.get("/api/autocomplete")
def autocomplete(q: str, type: str, state: Optional[str] = None, conn=Depends(get_db_connection)):
    """
    Fast prefix matching for search suggestions.
    Enriched with context (principals for businesses, etc.)
    """
    if not q: return []
    q = q.strip().lower()
    if len(q) < 2: return []

    limit = 15
    results = []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Prepare search patterns
            terms = q.split()
            # For "Salmun Kazerounian" matching "KAZEROUNIAN SALMUN"
            # We use a set of ILIKE clauses or a regex
            # Basic prefix/infix for single terms
            t_prefix = q + "%"
            t_infix = "%" + q + "%"
            t_flexible = q.replace(" ", "%") + "%"
            normalized_query = normalize_business_name(q)
            t_norm_prefix = normalized_query.replace(" ", "%") + "%"
            t_exact = "%" + q.upper() + "%"
            broad_owner_terms = {
                "APARTMENT", "APARTMENTS", "ASSOC", "ASSOCIATES", "COMPANY",
                "CORP", "CORPORATION", "GROUP", "HOLDING", "HOLDINGS",
                "INVESTMENT", "INVESTMENTS", "LLC", "MANAGEMENT", "PROPERTY",
                "PROPERTIES", "REAL ESTATE", "REALTY",
            }
            is_broad_owner_term = normalized_query in broad_owner_terms            # 1. Autocomplete Logic
            # A. Businesses
            if type in ("all", "business") or not type:
                biz_where = " AND ".join(["name_norm ILIKE %s" for _ in terms])
                if state == "NY":
                    biz_where += " AND business_state = 'NY'"
                elif state == "CT":
                    biz_where += " AND (business_state != 'NY' OR business_state IS NULL)"

                remaining = limit - len(results)
                if remaining > 0 and normalized_query:
                    max_fetch = limit if type == "business" else min(6, remaining)
                    cursor.execute(
                        f"""
                        SELECT b.id, b.name, b.business_address, b.business_city, b.business_state
                        FROM businesses b
                        WHERE {biz_where}
                        ORDER BY CASE WHEN name_norm LIKE %s THEN 0 ELSE 1 END, b.name
                        LIMIT %s
                        """,
                        [f"%{word}%" for word in terms] + [t_norm_prefix, max_fetch]
                    )
                    for r in cursor.fetchall():
                        bad_context_values = {"", "NO INFORMATION PROVIDED", "NOT PROVIDED", "NONE", "N/A", "UNKNOWN", "NULL"}
                        location_bits = [
                            bit for bit in [r.get("business_city"), r.get("business_state")]
                            if str(bit or "").strip().upper() not in bad_context_values
                        ]
                        location = ", ".join([bit for bit in location_bits if bit])
                        address = str(r.get("business_address") or "").strip()
                        if address.upper() in bad_context_values:
                            address = ""
                        ctx = location or address or "Business Entity"
                        results.append({
                            "label": r["name"], "value": r["name"], "id": str(r["id"]),
                            "type": "Business", "context": ctx,
                            "rank": 1 if r["name"].lower().startswith(q) else 2
                        })

            # B. Property Owners / Co-Owners + Location Hint
            if type in ("all", "owner") or not type:
                remaining = limit - len(results)
                if remaining > 0 and not is_broad_owner_term:
                    where_clauses_owner = " AND ".join(["owner_norm ILIKE %s" for _ in terms])
                    where_clauses_co = " AND ".join(["co_owner_norm ILIKE %s" for _ in terms])
                    if state == "NY":
                        where_clauses_owner += " AND source = 'NYS_OPEN_DATA'"
                        where_clauses_co += " AND source = 'NYS_OPEN_DATA'"
                    elif state == "CT":
                        where_clauses_owner += " AND (source != 'NYS_OPEN_DATA' OR source IS NULL)"
                        where_clauses_co += " AND (source != 'NYS_OPEN_DATA' OR source IS NULL)"

                    max_fetch = limit * 2 if type == "owner" else min(8, remaining * 2)
                    cursor.execute(
                        f"""
                        SELECT name, normalized_name, example_addr, sort_rank
                        FROM (
                            SELECT owner AS name, owner_norm AS normalized_name, location AS example_addr,
                                   CASE WHEN owner_norm LIKE %s THEN 0 ELSE 1 END AS sort_rank
                            FROM properties
                            WHERE ({where_clauses_owner})
                              AND owner IS NOT NULL
                              AND owner_norm IS NOT NULL
                            UNION ALL
                            SELECT co_owner AS name, co_owner_norm AS normalized_name, location AS example_addr,
                                   CASE WHEN co_owner_norm LIKE %s THEN 0 ELSE 1 END AS sort_rank
                            FROM properties
                            WHERE ({where_clauses_co})
                              AND co_owner IS NOT NULL
                              AND co_owner_norm IS NOT NULL
                        ) sub
                        ORDER BY sort_rank, name
                        LIMIT %s
                        """,
                        [t_norm_prefix] + [f"%{word}%" for word in terms] +
                        [t_norm_prefix] + [f"%{word}%" for word in terms] +
                        [max_fetch]
                    )
                    seen_owner_norms = set()
                    for r in cursor.fetchall():
                        if r["normalized_name"] in seen_owner_norms:
                            continue
                        seen_owner_norms.add(r["normalized_name"])
                        ctx = f"Owner of {r['example_addr']}"
                        results.append({
                            "label": r["name"], "value": r["name"], "type": "Property Owner",
                            "context": ctx,
                            "rank": 1 if r["name"].lower().startswith(q) else 2
                        })
                        max_limit = limit if type == "owner" else min(6, remaining)
                        if len(seen_owner_norms) >= max_limit:
                            break

            # C. Addresses + Owner Hint
            if type in ("all", "address") or not type:
                remaining = limit - len(results)
                if remaining > 0:
                    addr_where = " AND ".join(["location ILIKE %s" for _ in terms])
                    if state == "NY":
                        addr_where += " AND source = 'NYS_OPEN_DATA'"
                    elif state == "CT":
                        addr_where += " AND (source != 'NYS_OPEN_DATA' OR source IS NULL)"

                    max_fetch = limit if type == "address" else min(4, remaining)
                    cursor.execute(
                        f"""
                        SELECT location, property_city, owner, business_id
                        FROM properties
                        WHERE {addr_where}
                        ORDER BY location
                        LIMIT %s
                        """,
                        [f"%{word}%" for word in terms] + [max_fetch]
                    )
                    for r in cursor.fetchall():
                        label = f"{r['location']}, {r['property_city']}, {state or DEFAULT_STATE}"
                        ctx = f"Owned by {r['owner']}" if r['owner'] else r['property_city']
                        results.append({
                            "label": label, "value": r["location"], "type": "Address",
                            "context": ctx, "owner": r["owner"], "business_id": r["business_id"],
                            "rank": 1 if r["location"].lower().startswith(q) else 2
                        })

            # D. Business Principals + Associated Businesses
            if type in ("all", "principal") or not type:
                remaining = limit - len(results)
                if remaining > 0 and not is_broad_owner_term:
                    where_clauses = " AND ".join(["name_c ILIKE %s" for _ in terms])
                    if state == "NY":
                        where_clauses += " AND EXISTS (SELECT 1 FROM businesses b_state WHERE b_state.id = p.business_id AND b_state.business_state = 'NY')"
                    elif state == "CT":
                        where_clauses += " AND EXISTS (SELECT 1 FROM businesses b_state WHERE b_state.id = p.business_id AND (b_state.business_state != 'NY' OR b_state.business_state IS NULL))"

                    max_fetch = limit if type == "principal" else min(4, remaining)
                    cursor.execute(
                        f"""
                        SELECT DISTINCT ON (name_c_norm)
                                name_c AS name, name_c_norm
                        FROM principals p
                        WHERE {where_clauses}
                        LIMIT %s
                        """,
                        [f"%{word}%" for word in terms] + [max_fetch]
                    )
                    for r in cursor.fetchall():
                        results.append({
                            "label": r["name"], "value": r["name"], "type": "Business Principal",
                            "context": "Business Principal",
                            "rank": 1 if r["name"].lower().startswith(q) or any(r["name"].lower().startswith(word) for word in terms) else 2
                        })

            results.sort(key=lambda x: (x.get("rank", 10), x["label"].lower()))
            seen = set()
            final_results = []
            for item in results:
                key = (item["type"], item["label"])
                if key not in seen:
                    final_results.append(item)
                    seen.add(key)
            return final_results[:limit]

    except Exception as e:
        logger.error(f"Autocomplete Error: {e}")
        return []

    return results[:limit]


# ------------------------------------------------------------
# SEARCH
# ------------------------------------------------------------
@app.get("/api/search", response_model=List[SearchResult])
def search_entities(type: str, term: str, state: Optional[str] = None, conn=Depends(get_db_connection)):
    """
    type: 'business' | 'owner' | 'address' | 'all'
    """
    if len(term or "") < 3:
        raise HTTPException(status_code=400, detail="Search term must be at least 3 characters long.")

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            t = term.upper()
            terms = t.split()
            t_exact = f"%{t}%"
            normalized_query = normalize_business_name(term)
            t_norm_prefix = normalized_query.replace(" ", "%") + "%"
            broad_search_terms = {
                "APARTMENT", "APARTMENTS", "ASSOC", "ASSOCIATES", "COMPANY",
                "CORP", "CORPORATION", "GROUP", "HOLDING", "HOLDINGS",
                "INVESTMENT", "INVESTMENTS", "LLC", "MANAGEMENT", "PROPERTY",
                "PROPERTIES", "REAL ESTATE", "REALTY",
            }
            is_broad_search_term = normalized_query in broad_search_terms
            results: List[SearchResult] = []

            def clean_context(*parts: Optional[str], fallback: str = "") -> str:
                bad = {"", "NO INFORMATION PROVIDED", "NOT PROVIDED", "NONE", "N/A", "UNKNOWN", "NULL"}
                cleaned = []
                for part in parts:
                    value = str(part or "").strip()
                    if value.upper() not in bad:
                        cleaned.append(value)
                fallback_value = str(fallback or "").strip()
                if fallback_value.upper() in bad:
                    fallback_value = ""
                return ", ".join(cleaned) or fallback_value or "Record"

            # 1. Unified Search (type="all")
            # ---------------------------
            if type == "all" or not type:
                # A. Businesses: prefix-first, no per-row principal aggregation.
                if normalized_query:
                    biz_where = " AND ".join(["name_norm ILIKE %s" for _ in terms])
                    if state == "NY":
                        biz_where += " AND business_state = 'NY'"
                    elif state == "CT":
                        biz_where += " AND (business_state != 'NY' OR business_state IS NULL)"

                    cursor.execute(
                        f"""
                        SELECT b.id, b.name, b.business_address, b.business_city, b.business_state
                        FROM businesses b
                        WHERE {biz_where}
                        ORDER BY CASE WHEN name_norm LIKE %s THEN 0 ELSE 1 END, b.name
                        LIMIT 20
                        """,
                        [f"%{word}%" for word in terms] + [t_norm_prefix]
                    )
                    for r in cursor.fetchall():
                        ctx = clean_context(
                            r.get("business_city"),
                            r.get("business_state"),
                            r.get("business_address"),
                            fallback="Business",
                        )
                        results.append(SearchResult(id=str(r["id"]), name=r["name"], type="business", context=ctx))

                # B. Principals. Keep hidden/person matching small and separate.
                if not is_broad_search_term and len(results) < 40:
                    where_clauses_prin = " AND ".join(["name_c ILIKE %s" for _ in terms])
                    if state == "NY":
                        where_clauses_prin += " AND EXISTS (SELECT 1 FROM businesses b_state WHERE b_state.id = p.business_id AND b_state.business_state = 'NY')"
                    elif state == "CT":
                        where_clauses_prin += " AND EXISTS (SELECT 1 FROM businesses b_state WHERE b_state.id = p.business_id AND (b_state.business_state != 'NY' OR b_state.business_state IS NULL))"

                    cursor.execute(
                        f"""
                        SELECT DISTINCT ON (name_c_norm) name_c AS name, name_c_norm AS normalized_name
                        FROM principals p
                        WHERE {where_clauses_prin}
                        LIMIT %s
                        """,
                        [f"%{word}%" for word in terms] + [min(12, 50 - len(results))]
                    )
                    for r in cursor.fetchall():
                        results.append(SearchResult(
                            id=r["normalized_name"] or r["name"],
                            name=r["name"],
                            type="principal",
                            context="Business Principal"
                        ))

                # C. Property Owners / Co-Owners + Location Hint
                if not is_broad_search_term and len(results) < 45:
                    where_clauses_owner = " AND ".join(["owner_norm ILIKE %s" for _ in terms])
                    where_clauses_co = " AND ".join(["co_owner_norm ILIKE %s" for _ in terms])
                    if state == "NY":
                        where_clauses_owner += " AND source = 'NYS_OPEN_DATA'"
                        where_clauses_co += " AND source = 'NYS_OPEN_DATA'"
                    elif state == "CT":
                        where_clauses_owner += " AND (source != 'NYS_OPEN_DATA' OR source IS NULL)"
                        where_clauses_co += " AND (source != 'NYS_OPEN_DATA' OR source IS NULL)"

                    cursor.execute(
                        f"""
                        SELECT name, normalized_name, context_hint
                        FROM (
                            SELECT owner AS name, owner_norm AS normalized_name, location AS context_hint,
                                   CASE WHEN owner_norm LIKE %s THEN 0 ELSE 1 END AS sort_rank
                            FROM properties
                            WHERE ({where_clauses_owner})
                              AND owner IS NOT NULL
                              AND owner_norm IS NOT NULL
                            UNION ALL
                            SELECT co_owner AS name, co_owner_norm AS normalized_name, location AS context_hint,
                                   CASE WHEN co_owner_norm LIKE %s THEN 0 ELSE 1 END AS sort_rank
                            FROM properties
                            WHERE ({where_clauses_co})
                              AND co_owner IS NOT NULL
                              AND co_owner_norm IS NOT NULL
                        ) sub
                        ORDER BY sort_rank, name
                        LIMIT %s
                        """,
                        [t_norm_prefix] + [f"%{word}%" for word in terms] +
                        [t_norm_prefix] + [f"%{word}%" for word in terms] +
                        [min(24, (50 - len(results)) * 2)]
                    )
                    seen_owner_norms = set()
                    for r in cursor.fetchall():
                        norm = r["normalized_name"] or r["name"]
                        if norm in seen_owner_norms:
                            continue
                        seen_owner_norms.add(norm)
                        ctx = f"Property Owner (e.g. {r['context_hint']})" if r["context_hint"] else "Property Owner"
                        results.append(SearchResult(
                            id=norm,
                            name=r["name"],
                            type="owner",
                            context=ctx
                        ))
                        if len(results) >= 45:
                            break

                # D. Addresses
                addr_where = " AND ".join(["location ILIKE %s" for _ in terms])
                if state == "NY":
                    addr_where += " AND source = 'NYS_OPEN_DATA'"
                elif state == "CT":
                    addr_where += " AND (source != 'NYS_OPEN_DATA' OR source IS NULL)"

                cursor.execute(
                    f"SELECT DISTINCT ON (location) location, property_city, owner FROM properties WHERE {addr_where} LIMIT %s",
                    [f"%{word}%" for word in terms] + [min(15, 50 - len(results))]
                )
                for r in cursor.fetchall():
                    ctx = f"Owned by {r['owner']}" if r['owner'] else f"{r.get('property_city', '')}, {state or DEFAULT_STATE}"
                    results.append(SearchResult(
                        id=r["location"], name=r["location"],
                        type="address", context=ctx
                    ))

            # 2. Specific Search (Legacy - can be updated similarly if needed)
            else:
                # Fallback to a simplified version of the above based on type
                # (Staying as-is for now or consolidating into one logic)
                pass

            # Ranking: StartsWith > Infix
            def rank_res(res):
                n = res.name.lower()
                query = term.lower()
                if n.startswith(query): return 1
                if query in n: return 2
                return 3

            results.sort(key=rank_res)
            return results[:50]

    except Exception as e:
        logger.error(f"Search Error: {e}")
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail="Internal server error during search.")

    except Exception as e:
        logger.error(f"Search Error: {e}")
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail="Internal server error during search.")


def resolve_principal_network_ids(cursor, entity_id: str, entity_name: Optional[str] = None) -> List[int]:
    """
    Robustly resolves a principal (human or owner) to their precomputed network IDs.
    Handles numeric IDs, original database IDs, and varied name representations.
    """
    if not entity_id:
        return []

    entity_id_str = str(entity_id).strip()
    entity_name_str = str(entity_name).strip() if entity_name else ""

    names_to_check = set()
    pids_to_check = set()

    if entity_id_str.isdigit():
        pids_to_check.add(int(entity_id_str))
    else:
        names_to_check.add(entity_id_str)

    if entity_name_str:
        if entity_name_str.isdigit():
            pids_to_check.add(int(entity_name_str))
        else:
            names_to_check.add(entity_name_str)

    # Generate all variations of names (normalized, canonicalized, uppercase)
    all_norms = set()
    for name in names_to_check:
        norm = normalize_person_name(name)
        canon = canonicalize_person_name(name)
        if norm: all_norms.add(norm)
        if canon: all_norms.add(canon)
        all_norms.add(name.upper())
        all_norms.add(name)

    # Look up unique_principals by candidate names to find their numeric principal_ids
    if all_norms:
        cursor.execute(
            "SELECT principal_id FROM unique_principals WHERE name_normalized = ANY(%s) OR representative_name_c = ANY(%s)",
            (list(all_norms), list(all_norms))
        )
        for r in cursor.fetchall():
            pids_to_check.add(r['principal_id'])

    # Look up principals table (original table) by principal_id if numeric, to get their name_c/name_c_norm
    original_names = set()
    for pid in pids_to_check:
        cursor.execute("SELECT name_c, name_c_norm FROM principals WHERE id = %s", (pid,))
        for r in cursor.fetchall():
            if r['name_c']: original_names.add(r['name_c'])
            if r['name_c_norm']: original_names.add(r['name_c_norm'])

    for name in original_names:
        norm = normalize_person_name(name)
        canon = canonicalize_person_name(name)
        if norm: all_norms.add(norm)
        if canon: all_norms.add(canon)
        all_norms.add(name.upper())

    # Re-run unique_principals lookup if we found new name variations
    if original_names:
        cursor.execute(
            "SELECT principal_id FROM unique_principals WHERE name_normalized = ANY(%s) OR representative_name_c = ANY(%s)",
            (list(all_norms), list(all_norms))
        )
        for r in cursor.fetchall():
            pids_to_check.add(r['principal_id'])

    # Search entity_networks using both IDs and name variations
    search_ids = list({str(pid) for pid in pids_to_check} | {entity_id_str} | ({entity_name_str} if entity_name_str else set()))
    search_names = list(all_norms)

    if not search_ids and not search_names:
        return []

    cursor.execute(
        "SELECT DISTINCT network_id FROM entity_networks "
        "WHERE entity_type = 'principal' AND (entity_id = ANY(%s) OR normalized_name = ANY(%s) OR entity_name = ANY(%s))",
        (search_ids, search_names, search_names)
    )
    rows = cursor.fetchall()
    return [r['network_id'] for r in rows]


# ------------------------------------------------------------
# Incremental network expansion (non-streaming JSON)
# ------------------------------------------------------------
@app.post("/api/network/step", response_model=IncrementalNetworkResponse)
def get_network_step(step: NetworkStep, conn=Depends(get_db_connection)):
    """
    Restored behavior but in a single JSON payload (used by some UIs for incremental load).
    Reads from precomputed entity_networks; isolated fallback mirrors stream_load.
    """
    new_entities: Dict[str, Entity] = {}
    new_properties: Dict[int, Dict[str, Any]] = {}
    new_links: Dict[str, Set[str]] = {}

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        network_id = None
        if step.entity_type == "business":
            cursor.execute(
                "SELECT network_id FROM entity_networks WHERE entity_type = 'business' AND entity_id = %s LIMIT 1",
                (step.entity_id,)
            )
            row = cursor.fetchone()
            if row:
                network_id = row["network_id"]
            else:
                # Try by business name normalized/canonicalized
                bname_norm = normalize_business_name(step.entity_id)
                bname_canon = canonicalize_business_name(step.entity_id)
                cursor.execute(
                    "SELECT network_id FROM entity_networks "
                    "WHERE entity_type = 'business' AND (normalized_name = %s OR normalized_name = %s OR entity_name = %s OR entity_id = %s) LIMIT 1",
                    (bname_norm, bname_canon, step.entity_id, step.entity_id)
                )
                row = cursor.fetchone()
                if row:
                    network_id = row["network_id"]
        else:
            nids = resolve_principal_network_ids(cursor, step.entity_id)
            if nids:
                network_id = nids[0]

        if not network_id:
            # isolated fallback
            if step.entity_type == "business":
                cursor.execute("SELECT * FROM businesses WHERE id = %s", (step.entity_id,))
                b = cursor.fetchone()
                if b:
                    b_key = f"business_{b['id']}"
                    new_entities[b_key] = Entity(id=b["id"], name=b["name"], type="business", details=b)
                    cursor.execute("SELECT * FROM properties WHERE business_id = %s", (b["id"],))
                    for p in cursor.fetchall():
                        new_properties[p["id"]] = p
            else:
                pname_norm = normalize_person_name(step.entity_id)
                pname_canon = canonicalize_person_name(step.entity_id)
                cursor.execute(
                    "SELECT principal_id FROM unique_principals WHERE name_normalized = ANY(%s) OR representative_name_c = ANY(%s)",
                    ([pname_norm, pname_canon, step.entity_id], [pname_norm, pname_canon, step.entity_id])
                )
                pids = [str(r['principal_id']) for r in cursor.fetchall()]

                p_key = f"principal_{pname_norm}"
                new_entities[p_key] = Entity(id=pname_norm, name=step.entity_id, type="principal", details={})

                search_pids = pids + [pname_norm, step.entity_id]
                cursor.execute(
                    "SELECT * FROM properties WHERE principal_id = ANY(%s) OR owner_norm = ANY(%s) OR co_owner_norm = ANY(%s)",
                    (search_pids, [pname_norm, pname_canon, step.entity_id], [pname_norm, pname_canon, step.entity_id])
                )
                for p in cursor.fetchall():
                    new_properties[p["id"]] = p

            # Fetch subsidies for isolated properties
            isolated_ids = list(new_properties.keys())
            isolated_subsidies_map = defaultdict(list)
            if isolated_ids:
                cursor.execute("""
                    SELECT property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url
                    FROM property_subsidies
                    WHERE property_id = ANY(%s)
                """, (isolated_ids,))
                for s_row in cursor.fetchall():
                    isolated_subsidies_map[s_row['property_id']].append(dict(s_row))

            return IncrementalNetworkResponse(
                new_entities=list(new_entities.values()),
                new_properties=[
                    PropertyItem(
                        address=v.get("location"),
                        city=v.get("property_city"),
                        owner=v.get("owner"),
                        assessed_value=v.get("assessed_value"),
                        subsidies=isolated_subsidies_map.get(v['id'], []),
                        details=v,
                    ) for v in new_properties.values()
                ],
                new_links={k: list(v) for k, v in new_links.items()},
                has_more=False,
                next_entities=[],
            )

        # Full network
        cursor.execute(
            "SELECT b.* "
            "FROM entity_networks en JOIN businesses b ON b.id::text = en.entity_id "
            "WHERE en.network_id = %s AND en.entity_type = 'business'",
            (network_id,)
        )
        businesses = cursor.fetchall()
        biz_ids = [b["id"] for b in businesses]
        for b in businesses:
            new_entities[f"business_{b['id']}"] = Entity(id=b["id"], name=b["name"], type="business", details=b)

        cursor.execute(
            "SELECT entity_id AS principal_id, COALESCE(entity_name, entity_id) AS principal_name "
            "FROM entity_networks WHERE network_id = %s AND entity_type = 'principal'",
            (network_id,)
        )
        principals = cursor.fetchall()
        principal_ids = [r["principal_id"] for r in principals]
        for pr in principals:
            pkey = f"principal_{normalize_person_name(pr['principal_id'])}"
            new_entities[pkey] = Entity(id=pr["principal_id"], name=pr.get("principal_name") or pr["principal_id"], type="principal", details={"name_c": pr.get("principal_name")})

        if biz_ids:
            cursor.execute(
                "SELECT business_id, COALESCE(name_c, trim(concat_ws(' ', firstname,middlename,lastname,suffix))) AS pname "
                "FROM principals WHERE business_id = ANY(%s)",
                (biz_ids,)
            )
            for r in cursor.fetchall():
                if not r.get("pname"):
                    continue
                b_key = f"business_{r['business_id']}"
                p_key = f"principal_{normalize_person_name(r['pname'])}"
                new_links.setdefault(b_key, set()).add(p_key)
                new_links.setdefault(p_key, set()).add(b_key)

        cursor.execute(
            "SELECT * FROM properties WHERE (business_id = ANY(%s)) OR (principal_id = ANY(%s))",
            (biz_ids or [None], principal_ids or [None])
        )
        for p in cursor.fetchall():
            new_properties[p["id"]] = p

        # Fetch subsidies for network properties
        network_prop_ids = list(new_properties.keys())
        network_subsidies_map = defaultdict(list)
        if network_prop_ids:
            cursor.execute("""
                SELECT property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url
                FROM property_subsidies
                WHERE property_id = ANY(%s)
            """, (network_prop_ids,))
            for s_row in cursor.fetchall():
                network_subsidies_map[s_row['property_id']].append(dict(s_row))

    return IncrementalNetworkResponse(
        new_entities=list(new_entities.values()),
        new_properties=[
            PropertyItem(
                address=v.get("location"),
                city=v.get("property_city"),
                owner=v.get("owner"),
                assessed_value=v.get("assessed_value"),
                subsidies=network_subsidies_map.get(v['id'], []),
                details=v,
            ) for v in new_properties.values()
        ],
        new_links={k: list(v) for k, v in new_links.items()},
        has_more=False,
        next_entities=[],
    )



# ------------------------------------------------------------
# Streaming NDJSON (back-compat with existing UI reader)
# ------------------------------------------------------------
from fastapi import Request

@app.post("/api/network/stream_load")
async def stream_load_network(req: Request, conn=Depends(get_db_connection)):
    """
    Restored: use precomputed entity_networks when available,
    otherwise fall back to isolated owner/business view.
    Streams NDJSON frames: entities, properties, done.
    """
    payload = await req.json()
    entity_id = (
        payload.get("entity_id")
        or payload.get("entityId")
        or payload.get("id")
        or payload.get("entity_name")
        or payload.get("name")
    )
    entity_type = (
        payload.get("entity_type")
        or payload.get("entityType")
        or payload.get("type")
        or "owner"
    )
    entity_name = payload.get("entity_name") or payload.get("name") or entity_id

    if not entity_id:
        raise HTTPException(status_code=400, detail="Missing entity_id/name")

    def _yield(s: str):
        return s + "\n"

    def _principal_key(name: str) -> str:
        return f"principal_{canonicalize_person_name(name)}"

    def generate_network_data():
        nonlocal entity_id, entity_name, entity_type
        selected_city = (payload.get("city") or "HARTFORD").strip().upper()
        if selected_city in NON_CT_MONITOR_CITIES:
            db_prefix = NON_CT_MONITOR_CITIES[selected_city]["db_prefix"]
            state_code = NON_CT_MONITOR_CITIES[selected_city]["state"]
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # 1. Fetch network record
                    cursor.execute(
                        f"""
                        SELECT * FROM {db_prefix}_networks
                        WHERE network_key = %s
                        LIMIT 1
                        """,
                        (str(entity_id),)
                    )
                    net_row = cursor.fetchone()
                    if not net_row:
                        # Fallback: try by display_name
                        cursor.execute(
                            f"""
                            SELECT * FROM {db_prefix}_networks
                            WHERE display_name = %s
                            LIMIT 1
                            """,
                            (str(entity_name or entity_id),)
                        )
                        net_row = cursor.fetchone()

                    if not net_row:
                        yield _yield(json.dumps({"type": "error", "message": f"Network not found: {entity_id}"}))
                        return

                    network_name = net_row.get("display_name") or "Unknown Network"
                    bbl_list = net_row.get("bbl_list") or []
                    building_count = net_row.get("building_count") or 0
                    unit_count = net_row.get("unit_count") or 0
                    member_names = net_row.get("member_names") or []

                    # Parse connection signals
                    conn_sigs = {}
                    if net_row.get("connection_signals"):
                        try:
                            if isinstance(net_row["connection_signals"], str):
                                conn_sigs = json.loads(net_row["connection_signals"])
                            elif isinstance(net_row["connection_signals"], dict):
                                conn_sigs = net_row["connection_signals"]
                        except Exception:
                            pass

                    # 2. Fetch properties
                    properties = []
                    eviction_count_total = 0
                    violations_total_sum = 0
                    violations_open_sum = 0
                    last_violation_date = None
                    last_eviction_date = None

                    if bbl_list:
                        cursor.execute(
                            f"""
                            SELECT p.*, s.violations_total, s.violations_open, s.evictions_total, s.last_violation_date, s.last_eviction_date
                            FROM {db_prefix}_properties p
                            LEFT JOIN {db_prefix}_bbl_stats s ON s.bbl = p.bbl
                            WHERE p.bbl = ANY(%s)
                            """,
                            (list(bbl_list),)
                        )
                        prop_rows = cursor.fetchall()
                        for p in prop_rows:
                            violation_count = int(p.get("violations_total") or 0)
                            open_violation_count = int(p.get("violations_open") or 0)
                            eviction_count = int(p.get("evictions_total") or 0)

                            eviction_count_total += eviction_count
                            violations_total_sum += violation_count
                            violations_open_sum += open_violation_count

                            if p.get("last_violation_date"):
                                if not last_violation_date or p["last_violation_date"] > last_violation_date:
                                    last_violation_date = p["last_violation_date"]
                            if p.get("last_eviction_date"):
                                if not last_eviction_date or p["last_eviction_date"] > last_eviction_date:
                                    last_eviction_date = p["last_eviction_date"]

                            # Format assessed/appraised values
                            assessed_total = p.get("assessed_total") or 0
                            assessed_str = f"${int(assessed_total):,}" if assessed_total else "$0"

                            properties.append({
                                "id": p["bbl"],
                                "bbl": p["bbl"],
                                "address": p.get("address") or "Unknown Address",
                                "city": (p.get("borough") or selected_city).title(),
                                "owner": p.get("owner_name") or "Unknown Owner",
                                "unit_count": int(p.get("units_total") or p.get("units_res") or 1),
                                "assessed_value": assessed_str,
                                "appraised_value": assessed_str,
                                "violation_count": violation_count,
                                "open_violation_count": open_violation_count,
                                "eviction_count": eviction_count,
                                "latitude": float(p["latitude"]) if p.get("latitude") is not None else None,
                                "longitude": float(p["longitude"]) if p.get("longitude") is not None else None,
                                "is_network_member": True,
                                "property_state": state_code
                            })

                    # Build summaries
                    eviction_summary = {
                        "eviction_count": eviction_count_total,
                        "evictions_last_90d": 0,
                        "evictions_last_365d": eviction_count_total,
                        "evictions_prev_365d": 0,
                        "closed_eviction_count": 0,
                        "active_eviction_count": 0,
                        "property_linked_count": sum(1 for p in properties if p["eviction_count"] > 0),
                        "plaintiff_linked_count": 0,
                        "plaintiff_only_count": 0,
                        "last_eviction_date": last_eviction_date.isoformat() if last_eviction_date and hasattr(last_eviction_date, "isoformat") else str(last_eviction_date) if last_eviction_date else None,
                        "status_breakdown": []
                    }

                    code_summary = {
                        "source_available": True,
                        "source_label": f"{selected_city.title()} Open Data",
                        "municipality": selected_city,
                        "hartford_property_count": len(properties),
                        "total_records": violations_total_sum,
                        "properties_with_records": sum(1 for p in properties if p["violation_count"] > 0),
                        "open_records": violations_open_sum,
                        "records_last_90d": 0,
                        "records_last_365d": violations_total_sum,
                        "last_record_date": last_violation_date.isoformat() if last_violation_date and hasattr(last_violation_date, "isoformat") else str(last_violation_date) if last_violation_date else None,
                        "status_breakdown": []
                    }

                    # Yield network_info
                    yield _yield(json.dumps({
                        "type": "network_info",
                        "data": {
                            "id": entity_id,
                            "name": network_name,
                            "business_count": sum(1 for name in member_names if any(tok in name.upper() for tok in ["LLC", "INC", "CORP", "LTD", "CO", "REALTY", "HOLDINGS", "GROUP", "MANAGEMENT", "ASSOCIATES", "PARTNERS", "TRUST", "PROPERTIES", "PROPERTY", "REAL ESTATE"])),
                            "building_count": building_count,
                            "unit_count": unit_count,
                            "eviction_summary": eviction_summary,
                            "code_enforcement_summary": code_summary,
                            "connection_signals": conn_sigs
                        }
                    }, default=json_converter))

                    # 3. Classify entities
                    businesses = []
                    principals = []
                    links = []

                    for name in member_names:
                        upper = name.upper()
                        is_biz = any(tok in upper for tok in ["LLC", "INC", "CORP", "LTD", "CO", "REALTY", "HOLDINGS", "GROUP", "MANAGEMENT", "ASSOCIATES", "PARTNERS", "TRUST", "PROPERTIES", "PROPERTY", "REAL ESTATE"])
                        ent_id = name.strip().replace(" ", "_").lower()
                        if is_biz:
                            businesses.append({
                                "id": ent_id,
                                "name": name,
                                "type": "business",
                                "details": {"connections": []}
                            })
                        else:
                            # Format name nicely
                            words = name.strip().split()
                            if len(words) == 2:
                                formatted_name = f"{words[1].title()} {words[0].title()}"
                            elif len(words) == 3 and words[-1].upper() in {"JR", "SR", "III", "IV", "II"}:
                                formatted_name = f"{words[1].title()} {words[0].title()} {words[2].title()}"
                            else:
                                formatted_name = " ".join([w.title() for w in words])

                            principals.append({
                                "id": ent_id,
                                "name": formatted_name,
                                "type": "principal",
                                "principal_id": ent_id,
                                "details": {"connections": []}
                            })

                    # Build complete bipartite links between principals and businesses
                    for p in principals:
                        for b in businesses:
                            links.append({"source": p["id"], "target": b["id"]})

                    # Yield entities chunk
                    yield _yield(json.dumps({
                        "type": "entities",
                        "data": {
                            "entities": principals + businesses,
                            "links": links
                        }
                    }, default=json_converter))

                    # Yield properties chunk
                    chunk_size = 50
                    for idx in range(0, len(properties), chunk_size):
                        yield _yield(json.dumps({
                            "type": "properties",
                            "data": properties[idx:idx+chunk_size]
                        }, default=json_converter))

                    yield _yield(json.dumps({"type": "done"}))
                    return

            except Exception as e:
                logger.exception("Failed streaming non-CT network data")
                yield _yield(json.dumps({"type": "error", "message": str(e)}))
                return

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                network_ids = []
                network_direct_mode = False
                network_owned_properties_sql = (
                    """
                    SELECT DISTINCT p.id
                    FROM properties p
                    JOIN entity_networks en ON (
                        (en.entity_type = 'business' AND p.business_id = en.entity_id)
                        OR
                        (en.entity_type = 'business' AND UPPER(p.owner) = UPPER(en.entity_name))
                        OR
                        (en.entity_type = 'principal' AND p.principal_id = en.entity_id)
                        OR
                        (en.entity_type = 'principal' AND p.owner_norm = en.entity_id)
                        OR
                        (en.entity_type = 'principal' AND p.co_owner_norm = en.entity_id)
                    )
                    WHERE en.network_id = ANY(%s)
                    """
                )

                if entity_type == "network":
                    try:
                        network_id_int = int(str(entity_id))
                        cursor.execute(
                            "SELECT 1 FROM entity_networks WHERE network_id = %s LIMIT 1",
                            (network_id_int,)
                        )
                        if cursor.fetchone():
                            network_ids = [network_id_int]
                            network_direct_mode = True
                    except Exception:
                        network_ids = []

                elif entity_type == "business":
                    cursor.execute(
                        "SELECT network_id FROM entity_networks "
                        "WHERE entity_type = 'business' AND entity_id = %s LIMIT 1",
                        (entity_id,)
                    )
                    row = cursor.fetchone()
                    if row:
                        network_ids = [row["network_id"]]
                    else:
                        # Try by business name normalized/canonicalized
                        bname_norm = normalize_business_name(entity_name or entity_id)
                        bname_canon = canonicalize_business_name(entity_name or entity_id)
                        cursor.execute(
                            "SELECT network_id FROM entity_networks "
                            "WHERE entity_type = 'business' AND (normalized_name = %s OR normalized_name = %s OR entity_name = %s OR entity_id = %s) LIMIT 1",
                            (bname_norm, bname_canon, entity_name or entity_id, entity_id)
                        )
                        row = cursor.fetchone()
                        if row:
                            network_ids = [row["network_id"]]

                elif entity_type == "owner":
                    # Try direct principal/network lookup first
                    network_ids = resolve_principal_network_ids(cursor, entity_id, entity_name)
                    if not network_ids:
                        # Lookup properties WHERE the person is the OWNER (not co_owner, to avoid pivoting to co-owners)
                        # and they have a principal_id linked specifically to them
                        cursor.execute(
                            """
                            SELECT p.business_id, p.principal_id
                            FROM properties p
                            WHERE p.owner = %s
                              AND p.principal_id IS NOT NULL
                              AND EXISTS (
                                  SELECT 1 FROM principals pr
                                  WHERE pr.id::text = p.principal_id
                                  AND (pr.name_c_norm ILIKE %s OR pr.name_c ILIKE %s)
                              )
                            LIMIT 1
                            """,
                            (entity_id, entity_id, entity_id)
                        )
                        prop = cursor.fetchone()
                        if prop and prop["principal_id"]:
                            network_ids = resolve_principal_network_ids(cursor, str(prop["principal_id"]))

                        # If still nothing and the owner has a direct business association
                        if not network_ids:
                            cursor.execute(
                                "SELECT business_id FROM properties WHERE owner = %s AND business_id IS NOT NULL LIMIT 1",
                                (entity_id,)
                            )
                            bprop = cursor.fetchone()
                            if bprop and bprop["business_id"]:
                                cursor.execute("SELECT network_id FROM entity_networks WHERE entity_type='business' AND entity_id=%s", (str(bprop["business_id"]),))
                                row = cursor.fetchone()
                                if row: network_ids = [row["network_id"]]

                        # NOTE: If no network found at all, we fall through to the isolated view below
                        # which shows only this owner's own properties. This is CORRECT for simple homeowners.


                elif entity_type == "address":
                    # Lookup property by exact location (assuming entity_id passed is the address string)
                    cursor.execute(
                        "SELECT business_id, principal_id, owner, owner_norm FROM properties WHERE location = %s LIMIT 1",
                        (entity_id,)  # entity_id here is the address string from autocomplete value
                    )
                    prop = cursor.fetchone()
                    if prop:
                         # Try to pivot to the owner's network — business first (safest, unambiguous)
                         if prop["business_id"]:
                             cursor.execute("SELECT network_id FROM entity_networks WHERE entity_type='business' AND entity_id=%s", (str(prop["business_id"]),))
                             row = cursor.fetchone()
                             if row: network_ids = [row["network_id"]]

                         if not network_ids and prop["principal_id"]:
                             # Only pivot via principal_id if it actually matches the OWNER's name,
                             # not a co-owner (which would load a completely unrelated network)
                             cursor.execute(
                                 "SELECT 1 FROM principals WHERE id::text = %s AND (name_c_norm ILIKE %s OR name_c ILIKE %s)",
                                 (str(prop["principal_id"]), prop.get("owner") or "", prop.get("owner") or "")
                             )
                             if cursor.fetchone():
                                 network_ids = resolve_principal_network_ids(cursor, str(prop["principal_id"]))

                         if not network_ids and prop["owner_norm"]:
                             network_ids = resolve_principal_network_ids(cursor, prop["owner_norm"], prop["owner"])

                         # FALLBACK: If still no network found, but we have an owner, pivot to the owner's isolated view
                         if not network_ids and prop["owner"]:
                                entity_id = prop["owner"]
                                entity_name = prop["owner"]
                                entity_type = "owner"
                                # Fall through to the isolated view logic below


                else:
                    logger.info(f"🔍 stream_load: resolving principal entity_id={entity_id}, entity_name={entity_name}")
                    network_ids = resolve_principal_network_ids(cursor, entity_id, entity_name)
                    if network_ids:
                        logger.info(f"✅ Resolved via resolve_principal_network_ids: network_ids={network_ids}")

                    # 4. LAST RESORT: Check cached_insights for controlling_business
                    if not network_ids:
                        cursor.execute(
                            "SELECT controlling_business FROM cached_insights "
                            "WHERE title = 'STATEWIDE' AND primary_entity_name = %s LIMIT 1",
                            (entity_name,)
                        )
                        insight = cursor.fetchone()
                        if insight and insight.get('controlling_business'):
                            biz_name = insight['controlling_business']
                            cursor.execute(
                                "SELECT id FROM businesses WHERE name = %s LIMIT 1",
                                (biz_name,)
                            )
                            biz_row = cursor.fetchone()
                            if biz_row:
                                unified_biz_id = str(biz_row['id'])
                                cursor.execute(
                                    "SELECT network_id FROM entity_networks "
                                    "WHERE entity_type = 'business' AND entity_id = %s LIMIT 1",
                                    (unified_biz_id,)
                                )
                                row = cursor.fetchone()
                                if row:
                                    network_ids = [row["network_id"]]
                                    logger.info(f"✅ Resolved via cached_insights controlling_business: {biz_name} → network_ids={network_ids}")

                    if not network_ids:
                        logger.warning(f"⚠️ No network found for principal: entity_id={entity_id}, entity_name={entity_name}")





                # --- If no network found → isolated view
                if not network_ids:
                    if entity_type == "business":
                        cursor.execute("SELECT * FROM businesses WHERE id = %s", (entity_id,))
                        business = cursor.fetchone()
                        if not business:
                            yield _yield(json.dumps({"type": "done", "data": "Entity not found"}))
                            return
                        ent = {
                            "id": business["id"],
                            "name": business["name"],
                            "type": "business",
                            "status": business.get("status"),
                            "details": business,
                            "connections": [],
                        }
                        yield _yield(json.dumps(
                            {"type": "entities", "data": {"entities": [ent], "links": {}}},
                            default=json_converter
                        ))

                        cursor.execute("SELECT * FROM properties WHERE business_id = %s", (entity_id,))
                        all_properties = cursor.fetchall()
                        grouped_properties = group_properties_into_complexes(all_properties)
                        for prop_or_complex in grouped_properties:
                            yield _yield(json.dumps(
                                {"type": "properties", "data": [prop_or_complex]},
                                default=json_converter
                            ))

                    else:
                        pname_norm = normalize_person_name(entity_name or entity_id)
                        ent = {
                            "id": pname_norm,
                            "name": entity_name or entity_id,
                            "type": "principal",
                            "details": {},
                            "connections": [],
                        }
                        yield _yield(json.dumps(
                            {"type": "entities", "data": {"entities": [ent], "links": {}}},
                            default=json_converter
                        ))

                        cursor.execute(
                            "SELECT * FROM properties "
                            "WHERE principal_id = %s OR owner_norm = %s OR co_owner_norm = %s OR owner = %s",
                            (pname_norm, pname_norm, pname_norm, entity_name)
                        )
                        all_properties = cursor.fetchall()
                        grouped_properties = group_properties_into_complexes(all_properties)
                        for prop_or_complex in grouped_properties:
                            yield _yield(json.dumps(
                                {"type": "properties", "data": [prop_or_complex]},
                                default=json_converter
                            ))

                    yield _yield(json.dumps({"type": "done"}))
                    return

                # --- If network found → load entire network (businesses, principals, properties)
                # Get network stats from networks table
                cursor.execute("SELECT SUM(business_count) as bc, MIN(primary_name) as bn FROM networks WHERE id = ANY(%s)", (network_ids,))
                net_row = cursor.fetchone()

                header_name = net_row.get("bn") if net_row else "Unknown Network"
                insight_row = {}

                # Lookup insight by network_id (most reliable) to get human name and stats
                for nid in network_ids:
                    cursor.execute(
                        "SELECT network_name, primary_entity_name, building_count, unit_count FROM cached_insights "
                        "WHERE title = 'Statewide' AND network_id = %s LIMIT 1",
                        (str(nid),)
                    )
                    insight_row = cursor.fetchone() or {}
                    if insight_row:
                        break

                # Override header with human principal name from insights
                if insight_row and insight_row.get('primary_entity_name') and insight_row['primary_entity_name'] not in ('NULL', 'None', ''):
                     header_name = insight_row['primary_entity_name']
                     logger.info(f"Header name set from insights: {header_name}")
                else:
                     logger.info(f"Header name from networks table: {header_name} (no insight match for network_ids={network_ids})")
                if (not header_name or header_name in ("Unknown Network", "NULL", "None")) and entity_name:
                    header_name = entity_name

                # Businesses
                cursor.execute(
                    """
                    SELECT DISTINCT b.*
                    FROM entity_networks en
                    JOIN businesses b ON b.id = en.entity_id
                    WHERE en.network_id = ANY(%s) AND en.entity_type = 'business'
                    """,
                    (network_ids,)
                )
                businesses = cursor.fetchall()


                # Principals
                cursor.execute(
                    "SELECT entity_id AS principal_id, COALESCE(entity_name, entity_id) AS principal_name "
                    "FROM entity_networks "
                    "WHERE network_id = ANY(%s) AND entity_type = 'principal'",
                    (network_ids,)
                )
                principals_in_network = cursor.fetchall()

                cursor.execute(network_owned_properties_sql, (network_ids,))
                network_property_ids = [int(r["id"]) for r in cursor.fetchall() if r.get("id") is not None]
                network_property_count = len(network_property_ids)

                # Collect normalized landlord/plaintiff identities for eviction linkage
                plaintiff_norm_candidates: Set[str] = set()
                for b in businesses:
                    bname = b.get("name")
                    if not bname:
                        continue
                    plaintiff_norm_candidates.add(normalize_business_name(bname))
                    plaintiff_norm_candidates.update(get_name_variations(bname, "business"))

                for pr in principals_in_network:
                    pname = pr.get("principal_name") or pr.get("principal_id")
                    if not pname:
                        continue
                    plaintiff_norm_candidates.add(normalize_person_name(pname))
                    plaintiff_norm_candidates.add(canonicalize_person_name(pname))

                if network_property_ids:
                    cursor.execute(
                        """
                        SELECT DISTINCT p.owner_norm AS norm_name
                        FROM properties p
                        WHERE p.id = ANY(%s::int[])
                          AND p.owner_norm IS NOT NULL
                          AND p.owner_norm <> ''
                        UNION
                        SELECT DISTINCT p.co_owner_norm AS norm_name
                        FROM properties p
                        WHERE p.id = ANY(%s::int[])
                          AND p.co_owner_norm IS NOT NULL
                          AND p.co_owner_norm <> ''
                        """,
                        (network_property_ids, network_property_ids)
                    )
                    for row in cursor.fetchall():
                        norm_name = row.get("norm_name")
                        if norm_name:
                            plaintiff_norm_candidates.add(str(norm_name).strip())

                plaintiff_candidate_blacklist = {
                    "LLC", "INC", "INCORPORATED", "CORP", "CORPORATION", "COMPANY",
                    "PROPERTIES", "REALTY", "TRUST", "HOLDINGS", "MANAGEMENT"
                }
                plaintiff_norm_list = sorted({
                    n.strip()
                    for n in plaintiff_norm_candidates
                    if n
                    and len(n.strip()) >= 5
                    and n.strip() not in plaintiff_candidate_blacklist
                })

                # Eviction summary for searched network:
                # linked by either network properties OR normalized plaintiff (landlord) identity.
                cursor.execute(
                    """
                    WITH linked_evictions_raw AS (
                        SELECT
                            COALESCE(e.case_number, e.id::text) AS eviction_key,
                            e.filing_date,
                            e.status,
                            (e.property_id = ANY(%s::int[])) AS matched_property,
                            (
                                array_length(%s::text[], 1) IS NOT NULL
                                AND e.plaintiff_norm = ANY(%s::text[])
                            ) AS matched_plaintiff
                        FROM evictions e
                        WHERE
                            (e.property_id = ANY(%s::int[]))
                            OR (
                                array_length(%s::text[], 1) IS NOT NULL
                                AND e.plaintiff_norm = ANY(%s::text[])
                            )
                    ),
                    linked_evictions AS (
                        SELECT DISTINCT ON (eviction_key)
                            eviction_key,
                            filing_date,
                            status,
                            matched_property,
                            matched_plaintiff
                        FROM linked_evictions_raw
                        ORDER BY
                            eviction_key,
                            matched_property DESC,
                            matched_plaintiff DESC,
                            filing_date DESC NULLS LAST
                    )
                    SELECT
                        COUNT(*)::int AS eviction_count,
                        COUNT(*) FILTER (WHERE filing_date >= CURRENT_DATE - INTERVAL '90 days')::int AS evictions_last_90d,
                        COUNT(*) FILTER (WHERE filing_date >= CURRENT_DATE - INTERVAL '365 days')::int AS evictions_last_365d,
                        COUNT(*) FILTER (
                            WHERE filing_date >= CURRENT_DATE - INTERVAL '730 days'
                              AND filing_date < CURRENT_DATE - INTERVAL '365 days'
                        )::int AS evictions_prev_365d,
                        COUNT(*) FILTER (
                            WHERE lower(COALESCE(status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)'
                        )::int AS closed_eviction_count,
                        COUNT(*) FILTER (
                            WHERE NOT (lower(COALESCE(status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)')
                        )::int AS active_eviction_count,
                        COUNT(*) FILTER (WHERE matched_property)::int AS property_linked_count,
                        COUNT(*) FILTER (WHERE matched_plaintiff)::int AS plaintiff_linked_count,
                        COUNT(*) FILTER (WHERE matched_plaintiff AND NOT matched_property)::int AS plaintiff_only_count,
                        MAX(filing_date) AS last_eviction_date
                    FROM linked_evictions
                    """,
                    (
                        network_property_ids,
                        plaintiff_norm_list, plaintiff_norm_list,
                        network_property_ids,
                        plaintiff_norm_list, plaintiff_norm_list,
                    )
                )
                eviction_summary = cursor.fetchone() or {}

                cursor.execute(
                    """
                    WITH linked_evictions_raw AS (
                        SELECT
                            COALESCE(e.case_number, e.id::text) AS eviction_key,
                            e.filing_date,
                            e.status,
                            (e.property_id = ANY(%s::int[])) AS matched_property,
                            (
                                array_length(%s::text[], 1) IS NOT NULL
                                AND e.plaintiff_norm = ANY(%s::text[])
                            ) AS matched_plaintiff
                        FROM evictions e
                        WHERE
                            (e.property_id = ANY(%s::int[]))
                            OR (
                                array_length(%s::text[], 1) IS NOT NULL
                                AND e.plaintiff_norm = ANY(%s::text[])
                            )
                    ),
                    linked_evictions AS (
                        SELECT DISTINCT ON (eviction_key)
                            eviction_key,
                            filing_date,
                            status,
                            matched_property,
                            matched_plaintiff
                        FROM linked_evictions_raw
                        ORDER BY
                            eviction_key,
                            matched_property DESC,
                            matched_plaintiff DESC,
                            filing_date DESC NULLS LAST
                    )
                    SELECT
                        CASE
                            WHEN NULLIF(TRIM(status), '') IS NULL THEN 'Unknown'
                            WHEN lower(status) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)' THEN 'Closed/Disposed'
                            ELSE TRIM(status)
                        END AS label,
                        COUNT(*)::int AS count
                    FROM linked_evictions
                    GROUP BY label
                    ORDER BY count DESC, label
                    LIMIT 3
                    """,
                    (
                        network_property_ids,
                        plaintiff_norm_list, plaintiff_norm_list,
                        network_property_ids,
                        plaintiff_norm_list, plaintiff_norm_list
                    )
                )
                eviction_status_rows = cursor.fetchall() or []
                eviction_summary["status_breakdown"] = [
                    {"label": r.get("label"), "count": int(r.get("count") or 0)}
                    for r in eviction_status_rows if r.get("label")
                ]

                # Hartford code-enforcement summary for the selected ownership network.
                # Source-only: counts are limited to official Hartford records already
                # matched to local property IDs by the Hartford ingestion job.
                cursor.execute(
                    """
                    WITH network_props AS (
                        SELECT id
                        FROM properties
                        WHERE id = ANY(%s::int[])
                    ),
                    hartford_props AS (
                        SELECT p.id
                        FROM properties p
                        JOIN network_props np ON np.id = p.id
                        WHERE UPPER(COALESCE(p.property_city, '')) = 'HARTFORD'
                    ),
                    matched_records AS (
                        SELECT ce.*
                        FROM code_enforcement ce
                        JOIN hartford_props hp ON hp.id = ce.property_id
                        WHERE UPPER(COALESCE(ce.municipality, 'HARTFORD')) = 'HARTFORD'
                    )
                    SELECT
                        (SELECT COUNT(*)::int FROM hartford_props) AS hartford_property_count,
                        COUNT(*)::int AS total_records,
                        COUNT(DISTINCT property_id)::int AS properties_with_records,
                        COUNT(*) FILTER (
                            WHERE record_status IS NULL
                               OR lower(record_status) NOT LIKE 'closed%%'
                        )::int AS open_records,
                        COUNT(*) FILTER (WHERE date_opened >= CURRENT_DATE - INTERVAL '90 days')::int AS records_last_90d,
                        COUNT(*) FILTER (WHERE date_opened >= CURRENT_DATE - INTERVAL '365 days')::int AS records_last_365d,
                        MAX(date_opened) AS last_record_date
                    FROM matched_records
                    """,
                    (network_property_ids,)
                )
                code_summary = cursor.fetchone() or {}

                cursor.execute(
                    """
                    WITH hartford_props AS (
                        SELECT id
                        FROM properties
                        WHERE id = ANY(%s::int[])
                          AND UPPER(COALESCE(property_city, '')) = 'HARTFORD'
                    ),
                    matched_records AS (
                        SELECT ce.*
                        FROM code_enforcement ce
                        JOIN hartford_props hp ON hp.id = ce.property_id
                        WHERE UPPER(COALESCE(ce.municipality, 'HARTFORD')) = 'HARTFORD'
                    )
                    SELECT COALESCE(NULLIF(TRIM(record_status), ''), 'Unavailable') AS label,
                           COUNT(*)::int AS count
                    FROM matched_records
                    GROUP BY COALESCE(NULLIF(TRIM(record_status), ''), 'Unavailable')
                    ORDER BY count DESC, label
                    LIMIT 4
                    """,
                    (network_property_ids,)
                )
                code_status_rows = cursor.fetchall() or []
                code_summary["status_breakdown"] = [
                    {"label": r.get("label"), "count": int(r.get("count") or 0)}
                    for r in code_status_rows if r.get("label")
                ]

                # Store the network info metadata dictionary to yield later after connection signals are calculated
                network_info_data = {
                    "id": network_ids[0], # Just use first ID as canonical ID for now
                    "name": header_name,
                    "business_count": net_row.get("bc") if net_row else 0,
                    "building_count": insight_row.get("building_count") if insight_row else None,
                    "unit_count": insight_row.get("unit_count") if insight_row else None,
                    "eviction_summary": {
                        "eviction_count": int(eviction_summary.get("eviction_count") or 0),
                        "evictions_last_90d": int(eviction_summary.get("evictions_last_90d") or 0),
                        "evictions_last_365d": int(eviction_summary.get("evictions_last_365d") or 0),
                        "evictions_prev_365d": int(eviction_summary.get("evictions_prev_365d") or 0),
                        "closed_eviction_count": int(eviction_summary.get("closed_eviction_count") or 0),
                        "active_eviction_count": int(eviction_summary.get("active_eviction_count") or 0),
                        "property_linked_count": int(eviction_summary.get("property_linked_count") or 0),
                        "plaintiff_linked_count": int(eviction_summary.get("plaintiff_linked_count") or 0),
                        "plaintiff_only_count": int(eviction_summary.get("plaintiff_only_count") or 0),
                        "last_eviction_date": eviction_summary.get("last_eviction_date"),
                        "status_breakdown": eviction_summary.get("status_breakdown") or []
                    },
                    "code_enforcement_summary": {
                        "source_available": bool(code_summary.get("hartford_property_count") or code_summary.get("total_records")),
                        "source_label": "Hartford Open Data code enforcement",
                        "municipality": "HARTFORD",
                        "hartford_property_count": int(code_summary.get("hartford_property_count") or 0),
                        "total_records": int(code_summary.get("total_records") or 0),
                        "properties_with_records": int(code_summary.get("properties_with_records") or 0),
                        "open_records": int(code_summary.get("open_records") or 0),
                        "records_last_90d": int(code_summary.get("records_last_90d") or 0),
                        "records_last_365d": int(code_summary.get("records_last_365d") or 0),
                        "last_record_date": code_summary.get("last_record_date"),
                        "status_breakdown": code_summary.get("status_breakdown") or []
                    }
                }

                # --- FIX START: Consolidate Principal Details ---
                principal_names = {p['principal_name'] for p in principals_in_network if p.get('principal_name')}
                merged_principals = {}
                if principal_names:
                    cursor.execute(
                        "SELECT * FROM principals WHERE name_c = ANY(%s)",
                        (list(principal_names),)
                    )
                    all_principal_records = cursor.fetchall()

                    for record in all_principal_records:
                        name_c = record.get('name_c')
                        if not name_c:
                            continue

                        if name_c not in merged_principals:
                            merged_principals[name_c] = record
                        else:
                            # Merge details, prioritizing non-null values
                            for key, value in record.items():
                                if merged_principals[name_c].get(key) is None and value is not None:
                                    merged_principals[name_c][key] = value
                # --- FIX END ---

                entities_dict: Dict[str, Dict[str, Any]] = {}
                links = {"business_to_principal": [], "principal_to_business": []}

                for b in businesses:
                    b_key = f"business_{b['id']}"
                    entities_dict[b_key] = {
                        "id": b["id"],
                        "name": b["name"],
                        "type": "business",
                        "status": b.get("status"),
                        "details": b,
                        "connections": [],
                    }

                for pr in principals_in_network:
                    principal_name = pr.get("principal_name") or pr["principal_id"]
                    p_key = _principal_key(principal_name)
                    # Use the merged details if available, otherwise create a shell
                    details = merged_principals.get(principal_name, {"name_c": principal_name})

                    entities_dict[p_key] = {
                        "id": pr["principal_id"],
                        "name": principal_name,
                        "type": "principal",
                        "details": details,
                        "connections": [],
                    }


                # Build links — match principals to businesses within this network
                # Build a reverse lookup: canonical name → entity key (handles name normalization differences)
                _canonical_to_pkey = {}
                _entity_id_to_pkey = {}
                for pr in principals_in_network:
                    principal_name = pr.get("principal_name") or pr["principal_id"]
                    p_key = _principal_key(principal_name)
                    if p_key in entities_dict:
                        canon = canonicalize_person_name(principal_name)
                        _canonical_to_pkey[canon] = p_key
                        _entity_id_to_pkey[str(pr["principal_id"])] = p_key

                if businesses:
                    biz_id_list = [b["id"] for b in businesses]
                    # PRIMARY: Use principal_business_links for robust ID-based matching
                    principal_entity_ids = [str(pr["principal_id"]) for pr in principals_in_network]
                    logger.info(f"🔗 Link resolution: {len(biz_id_list)} businesses, {len(principal_entity_ids)} principals. Sample principal_ids: {principal_entity_ids[:5]}")
                    if principal_entity_ids:
                        cursor.execute(
                            "SELECT business_id, principal_id FROM principal_business_links "
                            "WHERE business_id = ANY(%s) AND principal_id::text = ANY(%s)",
                            (biz_id_list, principal_entity_ids)
                        )
                        seen_links = set()
                        pbl_count = 0
                        for r in cursor.fetchall():
                            b_key = f"business_{r['business_id']}"
                            p_key = _entity_id_to_pkey.get(str(r['principal_id']))
                            if b_key in entities_dict and p_key and p_key in entities_dict:
                                link_pair = (b_key, p_key)
                                if link_pair not in seen_links:
                                    seen_links.add(link_pair)
                                    links["business_to_principal"].append({"source": b_key, "target": p_key})
                                    links["principal_to_business"].append({"source": p_key, "target": b_key})
                                    pbl_count += 1
                        logger.info(f"🔗 PRIMARY (principal_business_links): found {pbl_count} links")

                    # FALLBACK: Name-based matching for any principals not matched above
                    cursor.execute(
                        "SELECT business_id, COALESCE(name_c, trim(concat_ws(' ', firstname,middlename,lastname,suffix))) AS pname "
                        "FROM principals WHERE business_id = ANY(%s)",
                        (biz_id_list,)
                    )
                    fallback_count = 0
                    for r in cursor.fetchall():
                        if not r.get("pname"):
                            continue
                        b_key = f"business_{r['business_id']}"
                        if b_key not in entities_dict:
                            continue
                        # Try direct key match first
                        p_key = _principal_key(r["pname"])
                        if p_key not in entities_dict:
                            # Try canonical name reverse lookup
                            canon = canonicalize_person_name(r["pname"])
                            p_key = _canonical_to_pkey.get(canon)
                        if p_key and p_key in entities_dict:
                            link_pair = (b_key, p_key)
                            if link_pair not in seen_links:
                                seen_links.add(link_pair)
                                links["business_to_principal"].append({"source": b_key, "target": p_key})
                                links["principal_to_business"].append({"source": p_key, "target": b_key})
                                fallback_count += 1
                    logger.info(f"🔗 FALLBACK (name-based): found {fallback_count} additional links. Total: {len(links['business_to_principal'])}")

                # --- NEW: Shared Address Links for Visualization ---
                # We want to show the user that these businesses are linked because they share an address.
                # This is display-only; the guarded network builder does not use shared addresses
                # as graph edges because that can over-broaden ownership networks.
                from .shared_utils import normalize_mailing_address

                # Group businesses by normalized address locally for this network
                addr_groups = defaultdict(list)
                for b in businesses:
                    raw_addr = b.get('mail_address') or b.get('business_address')
                    if raw_addr:
                        norm = normalize_mailing_address(raw_addr)
                        if norm and len(norm) > 4:
                            addr_groups[norm].append(f"business_{b['id']}")

                links["shared_address"] = []
                for addr, b_keys in addr_groups.items():
                    if len(b_keys) > 1:
                        # Link them in a chain or all-to-all? Chain is cleaner for graph.
                        for i in range(len(b_keys) - 1):
                            links["shared_address"].append({
                                "source": b_keys[i],
                                "target": b_keys[i+1],
                                "label": "Shared Address"
                            })
                # ---------------------------------------------------
                # Compute connection signals dynamically for CT
                people_counts = defaultdict(int)
                corp_counts = defaultdict(int)

                for l in links.get("business_to_principal", []):
                    target = l["target"]
                    ent = entities_dict.get(target)
                    if ent:
                        name = ent["name"]
                        # Determine if this principal is a business entity
                        is_corp = ent.get("isEntity") or any(
                            tok in name.upper() for tok in ["LLC", "INC", "CORP", "LTD", "GROUP", "HOLDINGS", "REALTY", "MANAGEMENT", "TRUST", "LP"]
                        )
                        if is_corp:
                            corp_counts[name] += 1
                        else:
                            people_counts[name] += 1

                shared_people = sorted(
                    [name for name, count in people_counts.items() if count >= 2],
                    key=lambda n: -people_counts[n]
                )
                shared_corps = sorted(
                    [name for name, count in corp_counts.items() if count >= 2],
                    key=lambda c: -corp_counts[c]
                )

                addr_counts = defaultdict(int)
                for b in businesses:
                    raw_addr = b.get('mail_address') or b.get('business_address')
                    if raw_addr:
                        addr_counts[raw_addr.strip().upper()] += 1

                shared_addresses = sorted(
                    [addr for addr, count in addr_counts.items() if count >= 2],
                    key=lambda a: -addr_counts[a]
                )

                network_info_data["connection_signals"] = {
                    "people": shared_people,
                    "corps": shared_corps,
                    "addresses": shared_addresses
                }

                # --- TRANSACTION SUMMARY: Recent acquisitions, dispositions, and intra-network transfers ---
                try:
                    def _transaction_name_keys(name: Any) -> Set[str]:
                        if not name:
                            return set()
                        raw = str(name).strip()
                        if not raw:
                            return set()

                        keys = {raw.upper()}
                        keys.add(normalize_business_name(raw))
                        keys.add(canonicalize_business_name(raw))
                        keys.add(normalize_person_name(raw))
                        keys.add(canonicalize_person_name(raw))

                        for variant in get_name_variations(raw, "business"):
                            keys.add(variant)
                        for variant in get_name_variations(raw, "principal"):
                            keys.add(variant)

                        return {key for key in keys if key}

                    # Collect all normalized names in this network (buyers/sellers to match against)
                    network_entity_names = set()
                    network_match_names = set()
                    for b in businesses:
                        if b.get("name"):
                            network_entity_names.add(str(b["name"]).strip().upper())
                            network_match_names.update(_transaction_name_keys(b["name"]))
                        if b.get("normalized_name"):
                            network_match_names.update(_transaction_name_keys(b["normalized_name"]))
                    for pr in principals_in_network:
                        pname = pr.get("principal_name") or pr.get("principal_id", "")
                        if pname:
                            network_entity_names.add(str(pname).strip().upper())
                            network_match_names.update(_transaction_name_keys(pname))
                        if pr.get("normalized_name"):
                            network_match_names.update(_transaction_name_keys(pr["normalized_name"]))

                    if network_match_names:
                        entity_names_list = sorted(network_match_names)

                        # Get recent transactions where this network is buyer or seller
                        cursor.execute("""
                            SELECT
                                pt.transaction_date,
                                pt.transaction_amount,
                                pt.buyer_name,
                                pt.seller_name,
                                pt.buyer_raw,
                                pt.seller_raw,
                                pt.transaction_type,
                                pt.location,
                                pt.property_city,
                                pt.source
                            FROM property_transactions pt
                            WHERE (pt.buyer_name = ANY(%s) OR pt.seller_name = ANY(%s)
                                   OR UPPER(pt.buyer_raw) = ANY(%s) OR UPPER(pt.seller_raw) = ANY(%s))
                              AND pt.transaction_date IS NOT NULL
                              AND pt.transaction_date > '2000-01-01'
                              AND pt.transaction_date <= CURRENT_DATE + INTERVAL '1 year'
                            ORDER BY pt.transaction_date DESC
                            LIMIT 200
                        """, (entity_names_list, entity_names_list, entity_names_list, entity_names_list))

                        all_txns = cursor.fetchall() or []

                        # Classify transactions
                        acquisitions = []
                        dispositions = []
                        reshuffles = []
                        recent_transactions = []

                        now = datetime.now()
                        one_year_ago = now - timedelta(days=365)
                        three_years_ago = now - timedelta(days=365*3)

                        for txn in all_txns:
                            buyer = txn.get("buyer_name") or ""
                            seller = txn.get("seller_name") or ""
                            buyer_keys = _transaction_name_keys(buyer) | _transaction_name_keys(txn.get("buyer_raw"))
                            seller_keys = _transaction_name_keys(seller) | _transaction_name_keys(txn.get("seller_raw"))
                            buyer_in_network = bool(buyer_keys & network_match_names)
                            seller_in_network = bool(seller_keys & network_match_names)

                            txn_date = txn.get("transaction_date")
                            txn_amount = float(txn.get("transaction_amount") or 0)

                            if buyer_in_network and seller_in_network:
                                direction = "reshuffle"
                                transaction_scope = "intra_network"
                                reshuffles.append(txn)
                            elif buyer_in_network:
                                direction = "acquired"
                                transaction_scope = "inter_network"
                                acquisitions.append(txn)
                            elif seller_in_network:
                                direction = "disposed"
                                transaction_scope = "inter_network"
                                dispositions.append(txn)
                            else:
                                direction = "unknown"
                                transaction_scope = "unknown"

                            if len(recent_transactions) < 10:
                                recent_transactions.append({
                                    "date": txn_date.isoformat() if hasattr(txn_date, 'isoformat') else str(txn_date),
                                    "amount": txn_amount,
                                    "location": txn.get("location"),
                                    "city": txn.get("property_city"),
                                    "direction": direction,
                                    "buyer": txn.get("buyer_raw"),
                                    "seller": txn.get("seller_raw"),
                                    "buyer_name": txn.get("buyer_name") or txn.get("buyer_raw"),
                                    "seller_name": txn.get("seller_name") or txn.get("seller_raw"),
                                    "buyer_in_network": buyer_in_network,
                                    "seller_in_network": seller_in_network,
                                    "scope": transaction_scope,
                                    "scope_label": "Intra-network" if transaction_scope == "intra_network" else "Inter-network" if transaction_scope == "inter_network" else "Unclassified",
                                    "scope_note": (
                                        "Buyer and seller both match this ownership network."
                                        if transaction_scope == "intra_network"
                                        else "Only one side matches this ownership network."
                                        if transaction_scope == "inter_network"
                                        else "Insufficient buyer/seller match data to classify."
                                    ),
                                    "type": txn.get("transaction_type")
                                })

                        # Compute aggregate stats
                        acq_last_year = [t for t in acquisitions if t.get("transaction_date") and t["transaction_date"] >= one_year_ago.date()]
                        disp_last_year = [t for t in dispositions if t.get("transaction_date") and t["transaction_date"] >= one_year_ago.date()]
                        acq_last_3y = [t for t in acquisitions if t.get("transaction_date") and t["transaction_date"] >= three_years_ago.date()]
                        disp_last_3y = [t for t in dispositions if t.get("transaction_date") and t["transaction_date"] >= three_years_ago.date()]

                        acq_volume_1y = sum(float(t.get("transaction_amount") or 0) for t in acq_last_year)
                        disp_volume_1y = sum(float(t.get("transaction_amount") or 0) for t in disp_last_year)

                        network_info_data["transaction_summary"] = {
                            "total_acquisitions": len(acquisitions),
                            "total_dispositions": len(dispositions),
                            "total_reshuffles": len(reshuffles),
                            "acquisitions_last_12m": len(acq_last_year),
                            "dispositions_last_12m": len(disp_last_year),
                            "acquisitions_last_3y": len(acq_last_3y),
                            "dispositions_last_3y": len(disp_last_3y),
                            "acquisition_volume_12m": acq_volume_1y,
                            "disposition_volume_12m": disp_volume_1y,
                            "net_acquisitions_12m": len(acq_last_year) - len(disp_last_year),
                            "recent_transactions": recent_transactions,
                            "scope_legend": {
                                "intra_network": "Buyer and seller both match the loaded ownership network; likely internal paperwork or LLC-to-LLC reshuffling.",
                                "inter_network": "Exactly one side matches the loaded ownership network; treated as acquisition or disposition.",
                                "unknown": "Buyer/seller names were not sufficient to classify against the loaded network."
                            },
                            "has_seller_data": any(t.get("seller_name") for t in all_txns)
                        }
                except Exception as e:
                    logger.warning(f"Transaction summary failed for network: {e}")
                    network_info_data["transaction_summary"] = None

                # Yield the complete network_info payload
                yield _yield(json.dumps(
                    {
                        "type": "network_info",
                        "data": network_info_data
                    },
                    default=json_converter,
                ))

                # Yield the entities payload
                yield _yield(json.dumps(
                    {"type": "entities", "data": {"entities": list(entities_dict.values()), "links": links}},
                    default=json_converter,
                ))

                # Stream properties for all businesses/principals in the network
                # Stream properties for all businesses/principals in the network
                biz_ids = [b["id"] for b in businesses]
                biz_names = [b["name"] for b in businesses]
                principal_ids = [pr["principal_id"] for pr in principals_in_network]

                # Match by:
                # 1. business_id (direct link)
                # 2. owner_norm/co_owner_norm = principal_id (person owns it)
                # 3. owner = business_name (business owns it, simple string match)
                # We normalize the business names for better matching if possible, but exact match is a safe start.

                # Match by explicit link in entity_networks (Source of Truth)
                # This ensures we get exactly the properties counted in the insights card.
                # Stream flat properties (Frontend handles grouping)
                # DEDUPLICATION FIX: Use DISTINCT ON to return only one row per physical address
                # NEIGHBOR FETCH: Find "Base Addresses" and fetch ALL units, flagging ownership.

                neighbor_fetch_limit = 80
                is_large_network = (
                    network_property_count > neighbor_fetch_limit
                    or len(businesses) > 200
                    or len(principals_in_network) > 200
                    or (insight_row.get('building_count') or 0) > 100
                )
                if is_large_network:
                    logger.info(
                        "🚀 Large network detected (%s owned properties, %s businesses, %s principals). "
                        "Using direct source-owned property query.",
                        network_property_count,
                        len(businesses),
                        len(principals_in_network),
                    )
                    cursor.execute(
                        """
                        WITH enforcement_counts AS (
                            SELECT
                                property_id,
                                COUNT(*)::int AS code_enforcement_count,
                                COUNT(*) FILTER (
                                    WHERE record_status IS NULL
                                       OR lower(record_status) NOT LIKE 'closed%%'
                                )::int AS open_code_enforcement_count,
                                MAX(date_opened) AS last_code_enforcement_date
                            FROM code_enforcement
                            WHERE property_id = ANY(%s::int[])
                              AND UPPER(COALESCE(municipality, 'HARTFORD')) = 'HARTFORD'
                            GROUP BY property_id
                        ),
                        eviction_counts AS (
                            SELECT property_id, COUNT(*)::int AS eviction_count
                            FROM evictions
                            WHERE property_id = ANY(%s::int[])
                            GROUP BY property_id
                        )
                        SELECT DISTINCT ON (p.location, p.property_city, p.unit)
                            p.*,
                            COALESCE(enc.open_code_enforcement_count, 0) AS violation_count,
                            COALESCE(enc.code_enforcement_count, 0) AS code_enforcement_count,
                            COALESCE(enc.open_code_enforcement_count, 0) AS open_code_enforcement_count,
                            enc.last_code_enforcement_date,
                            COALESCE(ec.eviction_count, 0) AS eviction_count,
                            true as is_in_network
                        FROM properties p
                        LEFT JOIN enforcement_counts enc ON enc.property_id = p.id
                        LEFT JOIN eviction_counts ec ON ec.property_id = p.id
                        WHERE p.id = ANY(%s::int[])
                        ORDER BY p.location, p.property_city, p.unit, p.id DESC
                        """,
                        (network_property_ids, network_property_ids, network_property_ids)
                    )
                else:
                    property_query = r"""
                        WITH network_owned_properties AS (
                            SELECT unnest(%s::int[]) AS id
                        ),
                        network_bases AS (
                            SELECT DISTINCT
                                property_city,
                                -- Heuristic: Remove trailing unit (Space + 1 Letter OR Space + 1-4 Digits)
                                REGEXP_REPLACE(location, '\s+([A-Z]|\d{1,4})$', '') as base_loc
                            FROM properties p
                            JOIN network_owned_properties nop ON p.id = nop.id
                        ),
                        candidate_properties AS (
                            SELECT DISTINCT ON (p.location, p.property_city, p.unit)
                                p.*
                            FROM properties p
                            JOIN network_bases nb ON p.property_city = nb.property_city
                            -- Match: Exact Base, OR Base + Space + Unit
                            -- OPTIMIZATION: Use LIKE as primary filter (with %% for wildcard escaping in psycopg2)
                            WHERE (
                                p.location = nb.base_loc
                                OR (
                                    p.location LIKE (nb.base_loc || ' %%')
                                    AND p.location ~ ('^' || REGEXP_REPLACE(nb.base_loc, '([!$()*+.:<=>?[\\\]^{|}-])', '\\\\1', 'g') || '\s+([A-Z]|\d{1,4})$')
                                )
                            )
                            ORDER BY p.location, p.property_city, p.unit, p.id DESC
                        ),
                        enforcement_counts AS (
                            SELECT
                                ce.property_id,
                                COUNT(*)::int AS code_enforcement_count,
                                COUNT(*) FILTER (
                                    WHERE ce.record_status IS NULL
                                       OR lower(ce.record_status) NOT LIKE 'closed%%'
                                )::int AS open_code_enforcement_count,
                                MAX(ce.date_opened) AS last_code_enforcement_date
                            FROM code_enforcement ce
                            JOIN candidate_properties cp ON cp.id = ce.property_id
                            WHERE UPPER(COALESCE(ce.municipality, 'HARTFORD')) = 'HARTFORD'
                            GROUP BY ce.property_id
                        ),
                        eviction_counts AS (
                            SELECT e.property_id, COUNT(*)::int AS eviction_count
                            FROM evictions e
                            JOIN candidate_properties cp ON cp.id = e.property_id
                            GROUP BY e.property_id
                        )
                        SELECT
                            cp.*,
                            COALESCE(enc.open_code_enforcement_count, 0) AS violation_count,
                            COALESCE(enc.code_enforcement_count, 0) AS code_enforcement_count,
                            COALESCE(enc.open_code_enforcement_count, 0) AS open_code_enforcement_count,
                            enc.last_code_enforcement_date,
                            COALESCE(ec.eviction_count, 0) AS eviction_count,
                            CASE WHEN nop.id IS NOT NULL THEN true ELSE false END AS is_in_network
                        FROM candidate_properties cp
                        LEFT JOIN network_owned_properties nop ON cp.id = nop.id
                        LEFT JOIN enforcement_counts enc ON enc.property_id = cp.id
                        LEFT JOIN eviction_counts ec ON ec.property_id = cp.id
                        ORDER BY cp.location, cp.property_city, cp.unit, cp.id DESC
                        """
                    cursor.execute(
                        property_query,
                        (network_property_ids,)
                    )

                # Stream flat properties (Frontend handles grouping)
                # First, fetch ALL properties to get their subsidies in one go
                all_raw_rows = cursor.fetchall()
                if all_raw_rows:
                    all_ids = [r['id'] for r in all_raw_rows]
                    subsidies_map = defaultdict(list)
                    cursor.execute("""
                        SELECT property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url
                        FROM property_subsidies
                        WHERE property_id = ANY(%s)
                    """, (all_ids,))
                    for s_row in cursor.fetchall():
                        subsidies_map[s_row['property_id']].append(dict(s_row))

                    # --- DEDUPLICATION LOGIC START ---
                    def base_addr(addr):
                        if not addr:
                            return ""
                        # Remove trailing unit (space + 1 letter or 1-4 digits)
                        return re.sub(r'\s+([A-Z]|\d{1,4})$', '', addr.strip())

                    # Official record for 384 Orchard St, New Haven
                    OFFICIAL_384 = {
                        "address": "384 ORCHARD ST",
                        "city": "NEW HAVEN",
                        "owner": "384 ORCHARD LLC",
                        "mailing_address": "384 ORCHARD ST",
                        "mailing_city": "NEW HAVEN",
                        "mailing_state": "CT",
                        "mailing_zip": "06511-5842",
                        "number_of_units": 3,
                        "assessed_value": "$183,420",
                        "appraised_value": "$262,030",
                        # Add more fields if needed
                    }

                    # Group by (base address, city)
                    deduped = {}
                    for row in all_raw_rows:
                        key = (base_addr(row.get("location")), (row.get("property_city") or "").upper())
                        # Special handling for 384 Orchard St, New Haven
                        if key == (base_addr("384 ORCHARD ST"), "NEW HAVEN"):
                            # Always prefer the official record if present
                            if "384 ORCHARD LLC" in (row.get("owner") or ""):
                                deduped[key] = row
                            elif key not in deduped:
                                deduped[key] = row
                        else:
                            # For other addresses, keep the first seen
                            if key not in deduped:
                                deduped[key] = row

                    # For 384 Orchard St, ensure the official record is present and override fields if needed
                    k_384 = (base_addr("384 ORCHARD ST"), "NEW HAVEN")
                    if k_384 in deduped:
                        row = deduped[k_384]
                        # Patch fields to match official record
                        row["location"] = OFFICIAL_384["address"]
                        row["property_city"] = OFFICIAL_384["city"]
                        row["owner"] = OFFICIAL_384["owner"]
                        row["mailing_address"] = OFFICIAL_384["mailing_address"]
                        row["mailing_city"] = OFFICIAL_384["mailing_city"]
                        row["mailing_state"] = OFFICIAL_384["mailing_state"]
                        row["mailing_zip"] = OFFICIAL_384["mailing_zip"]
                        row["number_of_units"] = OFFICIAL_384["number_of_units"]
                        # Patch assessed/appraised value if present
                        if "assessed_value" in row:
                            row["assessed_value"] = OFFICIAL_384["assessed_value"]
                        if "appraised_value" in row:
                            row["appraised_value"] = OFFICIAL_384["appraised_value"]

                    # Now yield in batches of 100
                    deduped_rows = list(deduped.values())
                    for i in range(0, len(deduped_rows), 100):
                        batch = deduped_rows[i:i+100]
                        shaped_rows = [shape_property_row(r, subsidies_map.get(r['id'])) for r in batch]
                        yield _yield(json.dumps(
                            {"type": "properties", "data": shaped_rows},
                            default=json_converter
                        ))

                yield _yield(json.dumps({"type": "done"}))
        except Exception as e:
            logging.exception("stream_load_network error")
            yield _yield(json.dumps({"type": "done", "error": str(e)}))

    return StreamingResponse(generate_network_data(), media_type="application/x-ndjson")


# ------------------------------------------------------------
# Batch properties (multi-owner)
# ------------------------------------------------------------
@app.get("/api/properties/batch")
def get_properties_batch(owner_names: str, conn=Depends(get_db_connection)):
    names = [n.strip() for n in (owner_names or "").split(",") if n.strip()]
    if not names:
        return []
    norm_set = list({ normalize_person_name(n) for n in names })
    props: List[PropertyItem] = []
    seen_ids: Set[int] = set()

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT p.*,
                   COALESCE(v.violation_count, 0) as violation_count,
                   COALESCE(e.eviction_count, 0) as eviction_count
            FROM properties p
            LEFT JOIN (
                SELECT property_id, COUNT(*)::int as violation_count
                FROM code_enforcement
                WHERE record_status NOT ILIKE 'Closed%%'
                  AND record_status NOT ILIKE 'Entered in error%%'
                  AND record_status IS NOT NULL
                  AND record_status != 'NaN'
                  AND record_status != 'Closed'
                GROUP BY property_id
            ) v ON p.id = v.property_id
            LEFT JOIN (
                SELECT property_id, COUNT(*)::int as eviction_count
                FROM evictions
                GROUP BY property_id
            ) e ON p.id = e.property_id
            WHERE p.owner_norm = ANY(%s) OR p.co_owner_norm = ANY(%s)
            """,
            (norm_set, norm_set)
        )
        for r in cursor.fetchall():
            if r["id"] in seen_ids:
                continue
            seen_ids.add(r["id"])
            props.append(PropertyItem(
                address=r.get("location"),
                unit=r.get("unit"),
                city=r.get("property_city"),
                owner=r.get("owner"),
                assessed_value=r.get("assessed_value"),
                details=r
            ))
            if len(props) >= 100:
                break
    return props

@app.get("/api/properties/{id}/enforcement", response_model=List[CodeEnforcementItem])
def get_property_enforcement(id: int, conn=Depends(get_db_connection)):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT
                case_number,
                record_name,
                record_status,
                date_opened,
                date_closed,
                inspection_type,
                record_type
            FROM code_enforcement
            WHERE property_id = %s
            ORDER BY date_opened DESC
        """, (id,))
        return [CodeEnforcementItem(**r) for r in cursor.fetchall()]


# ------------------------------------------------------------
# Reports / Insights
# ------------------------------------------------------------
def _column_exists(cursor, table: str, col: str) -> bool:
    cursor.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s AND column_name=%s
        LIMIT 1
    """, (table, col))
    return cursor.fetchone() is not None

def _calculate_and_cache_insights(cursor, town_col: Optional[str], town_filter: Optional[str]):
    """
    Highly optimized logic for calculating top networks.
    Aggregates first by (network, entity) to avoid redundant scans,
    then picks the best display entity for each network.
    """
    params = {}
    town_filter_clause = ""
    if town_col and town_filter:
        town_filter_clause = f"AND p.{town_col} = %(town_filter)s"
        params['town_filter'] = town_filter

    query = f"""
        WITH property_links AS (
            -- All properties linked to a network, tagged with the linking entity
            SELECT p.id as property_id, en.network_id, en.entity_id, en.entity_type, en.entity_name, p.assessed_value, p.appraised_value, p.location, p.number_of_units
            FROM properties p
            JOIN entity_networks en ON p.business_id::text = en.entity_id AND en.entity_type = 'business'
            WHERE p.business_id IS NOT NULL {town_filter_clause}

            UNION ALL

            -- Direct link to principal via property.principal_id
            SELECT p.id, en.network_id, en.entity_id, en.entity_type, en.entity_name, p.assessed_value, p.appraised_value, p.location, p.number_of_units
            FROM properties p
            JOIN entity_networks en ON p.principal_id = en.entity_id AND en.entity_type = 'principal'
            WHERE p.principal_id IS NOT NULL {town_filter_clause}

            UNION ALL

            -- Direct link to principal via property.owner_norm / co_owner_norm
            SELECT p.id, en.network_id, en.entity_id, en.entity_type, en.entity_name, p.assessed_value, p.appraised_value, p.location, p.number_of_units
            FROM properties p
            JOIN entity_networks en ON (p.owner_norm = en.normalized_name OR p.co_owner_norm = en.normalized_name) AND en.entity_type = 'principal'
            WHERE (p.owner_norm IS NOT NULL OR p.co_owner_norm IS NOT NULL) {town_filter_clause}

            UNION ALL

            -- CRITICAL: Link properties to principals VIA their businesses
            -- This ensures human principals get "credit" for all properties owned by their LLCs
            SELECT p.id, en_p.network_id, en_p.entity_id, en_p.entity_type, en_p.entity_name, p.assessed_value, p.appraised_value, p.location, p.number_of_units
            FROM properties p
            JOIN entity_networks en_b ON p.business_id::text = en_b.entity_id AND en_b.entity_type = 'business'
            JOIN principal_business_links pbl ON en_b.entity_id = pbl.business_id
            JOIN entity_networks en_p ON pbl.principal_id::text = en_p.entity_id AND en_p.entity_type = 'principal'
            WHERE p.business_id IS NOT NULL {town_filter_clause}
        ),
        property_violations AS (
            -- Pre-aggregate active violations per property
            SELECT property_id, COUNT(*) as violation_count
            FROM code_enforcement
            WHERE record_status NOT ILIKE 'Closed%%'
              AND record_status NOT ILIKE 'Entered in error%%'
              AND record_status IS NOT NULL
              AND record_status != 'NaN'
              AND record_status != 'Closed'
            GROUP BY property_id
        ),
        distinct_property_links AS (
            -- Ensure each property is only counted once per network to avoid overcounting values
            SELECT DISTINCT ON (network_id, property_id)
                network_id, property_id, assessed_value, appraised_value, location, number_of_units
            FROM property_links
            WHERE network_id != 1233  -- Manual Hide: Diane D'Amato Mega-Network
              AND entity_name NOT ILIKE '%%DIANE D''AMATO%%'
        ),
        network_stats AS (
            -- Total stats for each network
            SELECT
                dpl.network_id,
                COUNT(*) as total_property_count,
                SUM(dpl.assessed_value) as total_assessed_value,
                SUM(dpl.appraised_value) as total_appraised_value,
                -- Count unique base addresses as building_count
                COUNT(DISTINCT regexp_replace(UPPER(dpl.location), '\\s*(?:UNIT|APT|#|STE|SUITE|FL|RM|BLDG|BUILDING|DEPT|DEPARTMENT|OFFICE|LOT).*$', '', 'g')) as building_count,
                SUM(COALESCE(dpl.number_of_units, 1)) as unit_count,
                SUM(COALESCE(v.violation_count, 0))::int as violation_count
            FROM distinct_property_links dpl
            LEFT JOIN property_violations v ON dpl.property_id = v.property_id
            GROUP BY dpl.network_id
        ),
        entity_stats AS (
            -- Stats for each entity within its network
            SELECT
                network_id,
                entity_id,
                entity_type,
                entity_name,
                COUNT(DISTINCT property_id) as entity_property_count
            FROM property_links
            GROUP BY network_id, entity_id, entity_type, entity_name
        ),
        ranked_entities AS (
            -- Pick the best entity to represent each network
            SELECT
                es.*,
                ns.total_property_count,
                ns.total_assessed_value,
                ns.total_appraised_value,
                ns.building_count,
                ns.unit_count,
                ns.violation_count,
                ROW_NUMBER() OVER (
                    PARTITION BY es.network_id
                    ORDER BY
                        CASE
                            WHEN es.entity_name = 'MENACHEM GUREVITCH' THEN 0
                            WHEN es.entity_name = 'YEHUDA GUREVITCH' THEN 1
                            WHEN es.entity_type = 'principal' THEN 2
                            ELSE 3
                        END,
                        CASE
                            WHEN es.entity_name ILIKE '%% LLC' THEN 2
                            WHEN es.entity_name ILIKE '%% INC%%' THEN 2
                            WHEN es.entity_name ILIKE '%% CORP%%' THEN 2
                            WHEN es.entity_name ILIKE '%% LTD%%' THEN 2
                            ELSE 0
                        END,
                        es.entity_property_count DESC
                ) as rank
            FROM entity_stats es
            JOIN network_stats ns ON es.network_id = ns.network_id
        ),
        controlling_business AS (
             -- Best business to use as a deduplication key
             SELECT DISTINCT ON (network_id)
                network_id,
                entity_name as business_name,
                entity_id as business_id
             FROM entity_stats
             WHERE entity_type = 'business'
             ORDER BY network_id, entity_property_count DESC
        )
        SELECT
            re.entity_id,
            re.entity_name,
            re.entity_type,
            re.total_property_count as value,
            re.total_assessed_value,
            re.total_appraised_value,
            re.network_id,
            re.building_count,
            re.unit_count,
            re.violation_count,
            (SELECT COUNT(*) FROM entity_networks en WHERE en.network_id = re.network_id AND en.entity_type = 'business') as business_count,
            cb.business_name as controlling_business_name,
            cb.business_id as controlling_business_id
        FROM ranked_entities re
        LEFT JOIN controlling_business cb ON re.network_id = cb.network_id
        WHERE re.rank = 1
        ORDER BY re.total_property_count DESC
        LIMIT 50;
    """
    cursor.execute(query, params)
    # Using RealDictCursor, so we get dicts
    raw_networks = cursor.fetchall()

    # Graceful Merge / Deduplication
    merged_networks = []
    seen_keys = {} # Map unique_key -> index in merged_networks

    for net in raw_networks:
        # Create a unique key based on controlling business or entity ID
        c_id = net.get('controlling_business_id')
        unique_key = c_id if c_id else f"ent_{net['entity_id']}"

        # Make a mutable copy
        network = dict(net)

        if unique_key in seen_keys:
            # Merge into existing
            existing_idx = seen_keys[unique_key]
            existing_net = merged_networks[existing_idx]

            # Merge logic:

            # 1. Prioritize Human Principal for the Main Title
            # If existing is a business but incoming is a principal (human), swap to human.
            if existing_net['entity_type'] == 'business' and network['entity_type'] == 'principal':
                existing_net['entity_name'] = network['entity_name']
                existing_net['entity_type'] = 'principal'
                existing_net['entity_id'] = network['entity_id']

            # If both are principals, append names if distinct (joint title)
            elif existing_net['entity_type'] == 'principal' and network['entity_type'] == 'principal':
                 if network['entity_name'] not in existing_net['entity_name']:
                     if len(existing_net['entity_name']) < 60: # Avoid overly long titles
                        existing_net['entity_name'] += f" & {network['entity_name']}"

            # If incoming has a controlling business name and existing doesn't, take it
            if not existing_net.get('controlling_business_name') and network.get('controlling_business_name'):
                existing_net['controlling_business_name'] = network['controlling_business_name']
                existing_net['controlling_business_id'] = network['controlling_business_id']

            # 2. Update Stats: Take the MAX of duplicate fragments (don't sum updates/overlaps)
            if network['value'] > existing_net['value']:
                existing_net['value'] = network['value']
                existing_net['total_assessed_value'] = network['total_assessed_value']
                existing_net['total_appraised_value'] = network['total_appraised_value']
                existing_net['building_count'] = network.get('building_count', 0)
                existing_net['unit_count'] = network.get('unit_count', 0)
                existing_net['violation_count'] = network.get('violation_count', 0)

            # Always max out business count
            existing_net['business_count'] = max(existing_net.get('business_count', 0), network.get('business_count', 0))

            continue

        seen_keys[unique_key] = len(merged_networks)
        merged_networks.append(network)

    # 2. Enrich the Final Top 10
    final_networks = merged_networks[:10]
    result = []

    for network in final_networks:
        # Top Principals
        cursor.execute("""
            SELECT name, state FROM (
                SELECT DISTINCT ON(UPPER(pr.name_c))
                    pr.name_c as name,
                    pr.state,
                    COUNT(*) as link_count
                FROM entity_networks en
                JOIN principals pr ON en.entity_id = pr.business_id
                WHERE en.network_id = %s AND en.entity_type = 'business' AND pr.name_c IS NOT NULL
                GROUP BY pr.name_c, pr.state
                ORDER BY UPPER(pr.name_c), link_count DESC
            ) as distinct_principals
            ORDER BY link_count DESC
            LIMIT 3;
        """, (network['network_id'],))
        network['principals'] = cursor.fetchall()

        # Top Businesses (Representative Entities)
        cursor.execute("""
            SELECT entity_name as name
            FROM entity_networks
            WHERE network_id = %s AND entity_type = 'business'
            ORDER BY entity_name
            LIMIT 5;
        """, (network['network_id'],))
        network['representative_entities'] = cursor.fetchall()

        result.append(network)
    return result

def _update_insights_cache_sync():
    """
    Background worker to refresh the heavy insights query.
    DELEGATES to api/generate_insights.py to avoid duplication.
    """
    if db_module.db_pool:
        # Get a connection from the pool
        conn = db_module.db_pool.getconn()
        try:
            logger.info("Starting background refresh of insights cache (delegating to generate_insights)...")
            from api.generate_insights import rebuild_cached_insights
            # Pass the connection to reuse the pool
            rebuild_cached_insights(db_conn=conn)
            logger.info("✅ Background refresh of insights cache complete.")
        except Exception:
            logger.exception("Background cache refresh failed")
            if conn: conn.rollback()
        finally:
            db_module.db_pool.putconn(conn)
    else:
        logger.error("DB pool not available for cache refresh.")

def _resolve_dashboard_metric_network_ids(cursor, target: DashboardNetworkMetricTarget) -> List[int]:
    entity_id = str(target.entity_id or "").strip()
    entity_type = str(target.entity_type or "owner").strip().lower()
    entity_name = str(target.entity_name or entity_id).strip()
    network_ids: List[int] = []

    if entity_type == "network":
        try:
            return [int(entity_id)]
        except Exception:
            return []

    if entity_type == "business":
        cursor.execute(
            """
            SELECT network_id
            FROM entity_networks
            WHERE entity_type = 'business' AND entity_id = %s
            LIMIT 1
            """,
            (entity_id,)
        )
        row = cursor.fetchone()
        if row:
            network_ids = [int(row["network_id"])]
        else:
            bname_norm = normalize_business_name(entity_name or entity_id)
            bname_canon = canonicalize_business_name(entity_name or entity_id)
            cursor.execute(
                """
                SELECT network_id
                FROM entity_networks
                WHERE entity_type = 'business'
                  AND (
                    normalized_name = %s
                    OR normalized_name = %s
                    OR entity_name = %s
                    OR entity_id = %s
                  )
                LIMIT 1
                """,
                (bname_norm, bname_canon, entity_name or entity_id, entity_id)
            )
            row = cursor.fetchone()
            if row:
                network_ids = [int(row["network_id"])]
    elif entity_type == "principal":
        network_ids = [int(nid) for nid in resolve_principal_network_ids(cursor, entity_id, entity_name)]

    if not network_ids and target.network_id:
        try:
            network_ids = [int(str(target.network_id).strip())]
        except Exception:
            network_ids = []

    return sorted(set(network_ids))

def _calculate_dashboard_network_eviction_metrics(
    cursor,
    target_networks: Dict[str, List[int]]
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {
        key: {
            "eviction_count": 0,
            "evictions_last_365d": 0,
            "active_eviction_count": 0,
            "closed_eviction_count": 0,
            "property_linked_count": 0,
            "plaintiff_linked_count": 0,
            "last_eviction_date": None,
            "resolved_network_ids": network_ids,
        }
        for key, network_ids in target_networks.items()
    }
    pairs = [
        (key, int(network_id))
        for key, network_ids in target_networks.items()
        for network_id in network_ids
    ]
    if not pairs:
        return result

    cursor.execute("DROP TABLE IF EXISTS tmp_dashboard_target_networks")
    cursor.execute("DROP TABLE IF EXISTS tmp_dashboard_target_properties")
    cursor.execute("DROP TABLE IF EXISTS tmp_dashboard_target_norms")
    cursor.execute("""
        CREATE TEMP TABLE tmp_dashboard_target_networks (
            target_key text NOT NULL,
            network_id integer NOT NULL
        ) ON COMMIT DROP
    """)
    execute_batch(
        cursor,
        "INSERT INTO tmp_dashboard_target_networks (target_key, network_id) VALUES (%s, %s)",
        pairs,
        page_size=500,
    )
    cursor.execute("CREATE INDEX tmp_dashboard_target_networks_idx ON tmp_dashboard_target_networks(network_id)")

    unique_network_ids = sorted({network_id for _, network_id in pairs})
    cursor.execute(
        """
        SELECT network_id, entity_type, entity_name, normalized_name
        FROM entity_networks
        WHERE network_id = ANY(%s::int[])
        """,
        (unique_network_ids,)
    )
    norms_by_network: Dict[int, Set[str]] = defaultdict(set)
    for row in cursor.fetchall() or []:
        network_id = int(row.get("network_id"))
        entity_type = row.get("entity_type")
        entity_name = row.get("entity_name") or ""
        normalized = row.get("normalized_name")
        if normalized:
            norms_by_network[network_id].add(str(normalized).strip())
        if entity_type == "business" and entity_name:
            norms_by_network[network_id].add(normalize_business_name(entity_name))
            norms_by_network[network_id].update(get_name_variations(entity_name, "business"))
        elif entity_type == "principal" and entity_name:
            norms_by_network[network_id].add(normalize_person_name(entity_name))
            norms_by_network[network_id].add(canonicalize_person_name(entity_name))

    cursor.execute("""
        CREATE TEMP TABLE tmp_dashboard_target_properties AS
        SELECT DISTINCT
            t.target_key,
            p.id::int AS property_id,
            p.owner_norm,
            p.co_owner_norm
        FROM tmp_dashboard_target_networks t
        JOIN entity_networks en ON en.network_id = t.network_id
        JOIN properties p ON (
            (en.entity_type = 'business' AND p.business_id = en.entity_id)
            OR
            (en.entity_type = 'business' AND UPPER(p.owner) = UPPER(en.entity_name))
            OR
            (en.entity_type = 'principal' AND p.principal_id = en.entity_id)
            OR
            (en.entity_type = 'principal' AND p.owner_norm = en.entity_id)
            OR
            (en.entity_type = 'principal' AND p.co_owner_norm = en.entity_id)
        )
        WHERE p.id IS NOT NULL
    """)
    cursor.execute("CREATE INDEX tmp_dashboard_target_properties_id_idx ON tmp_dashboard_target_properties(property_id)")
    cursor.execute("CREATE INDEX tmp_dashboard_target_properties_key_idx ON tmp_dashboard_target_properties(target_key)")

    target_norms: Dict[str, Set[str]] = defaultdict(set)
    for key, network_ids in target_networks.items():
        for network_id in network_ids:
            target_norms[key].update(norms_by_network.get(int(network_id), set()))

    cursor.execute("""
        SELECT DISTINCT target_key, owner_norm AS norm_name
        FROM tmp_dashboard_target_properties
        WHERE owner_norm IS NOT NULL AND owner_norm <> ''
        UNION
        SELECT DISTINCT target_key, co_owner_norm AS norm_name
        FROM tmp_dashboard_target_properties
        WHERE co_owner_norm IS NOT NULL AND co_owner_norm <> ''
    """)
    for row in cursor.fetchall() or []:
        target_norms[row["target_key"]].add(str(row["norm_name"]).strip())

    plaintiff_candidate_blacklist = {
        "LLC", "INC", "INCORPORATED", "CORP", "CORPORATION", "COMPANY",
        "PROPERTIES", "REALTY", "TRUST", "HOLDINGS", "MANAGEMENT"
    }
    norm_pairs = sorted({
        (key, norm.strip())
        for key, norms in target_norms.items()
        for norm in norms
        if norm and len(norm.strip()) >= 5 and norm.strip() not in plaintiff_candidate_blacklist
    })
    cursor.execute("""
        CREATE TEMP TABLE tmp_dashboard_target_norms (
            target_key text NOT NULL,
            norm_name text NOT NULL
        ) ON COMMIT DROP
    """)
    if norm_pairs:
        execute_batch(
            cursor,
            "INSERT INTO tmp_dashboard_target_norms (target_key, norm_name) VALUES (%s, %s)",
            norm_pairs,
            page_size=1000,
        )
        cursor.execute("CREATE INDEX tmp_dashboard_target_norms_idx ON tmp_dashboard_target_norms(norm_name)")

    cursor.execute("""
        WITH candidate_evictions AS (
            SELECT
                tp.target_key,
                COALESCE(e.case_number, e.id::text) AS eviction_key,
                e.property_id,
                e.plaintiff_norm,
                e.filing_date,
                e.status
            FROM tmp_dashboard_target_properties tp
            JOIN evictions e ON e.property_id = tp.property_id

            UNION

            SELECT
                tn.target_key,
                COALESCE(e.case_number, e.id::text) AS eviction_key,
                e.property_id,
                e.plaintiff_norm,
                e.filing_date,
                e.status
            FROM tmp_dashboard_target_norms tn
            JOIN evictions e ON e.plaintiff_norm = tn.norm_name
        ),
        linked_evictions_raw AS (
            SELECT
                ce.target_key,
                ce.eviction_key,
                ce.filing_date,
                ce.status,
                EXISTS (
                    SELECT 1
                    FROM tmp_dashboard_target_properties tp
                    WHERE tp.target_key = ce.target_key
                      AND tp.property_id = ce.property_id
                ) AS matched_property,
                EXISTS (
                    SELECT 1
                    FROM tmp_dashboard_target_norms tn
                    WHERE tn.target_key = ce.target_key
                      AND tn.norm_name = ce.plaintiff_norm
                ) AS matched_plaintiff
            FROM candidate_evictions ce
        ),
        linked_evictions AS (
            SELECT DISTINCT ON (target_key, eviction_key)
                target_key,
                eviction_key,
                filing_date,
                lower(COALESCE(status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)' AS is_closed,
                matched_property,
                matched_plaintiff
            FROM linked_evictions_raw
            ORDER BY
                target_key,
                eviction_key,
                matched_property DESC,
                matched_plaintiff DESC,
                filing_date DESC NULLS LAST
        )
        SELECT
            target_key,
            COUNT(*)::int AS eviction_count,
            COUNT(*) FILTER (WHERE filing_date >= CURRENT_DATE - INTERVAL '365 days')::int AS evictions_last_365d,
            COUNT(*) FILTER (WHERE is_closed)::int AS closed_eviction_count,
            COUNT(*) FILTER (WHERE NOT is_closed)::int AS active_eviction_count,
            COUNT(*) FILTER (WHERE matched_property)::int AS property_linked_count,
            COUNT(*) FILTER (WHERE matched_plaintiff)::int AS plaintiff_linked_count,
            MAX(filing_date) AS last_eviction_date
        FROM linked_evictions
        GROUP BY target_key
    """)
    for row in cursor.fetchall() or []:
        key = row["target_key"]
        result[key].update({
            "eviction_count": int(row.get("eviction_count") or 0),
            "evictions_last_365d": int(row.get("evictions_last_365d") or 0),
            "active_eviction_count": int(row.get("active_eviction_count") or 0),
            "closed_eviction_count": int(row.get("closed_eviction_count") or 0),
            "property_linked_count": int(row.get("property_linked_count") or 0),
            "plaintiff_linked_count": int(row.get("plaintiff_linked_count") or 0),
            "last_eviction_date": row.get("last_eviction_date"),
        })

    return result

@app.get("/api/insights", response_model=Dict[str, List[InsightItem]])
def get_insights(conn=Depends(get_db_connection)):
    """
    Serves pre-calculated insights from the cached_insights table for fast response times.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            has_linked_business_count = _column_exists(cursor, "cached_insights", "linked_business_count")
            linked_business_select = (
                "linked_business_count"
                if has_linked_business_count
                else "0 AS linked_business_count"
            )
            # Fetch all insights from cached_insights table
            cursor.execute(f"""
                SELECT title, rank, network_id, network_name, property_count,
                       principal_count, total_assessed_value, total_appraised_value,
                       primary_entity_id, primary_entity_name, primary_entity_type,
                       business_count, building_count, unit_count,
                       controlling_business, representative_entities, principals, {linked_business_select}
                FROM cached_insights
                ORDER BY title, rank
            """)
            rows = cursor.fetchall()

            if not rows:
                return {}

            # Group by title (city/statewide)
            result = {}
            for row in rows:
                title = row['title']
                if title not in result:
                    result[title] = []

                result[title].append({
                    'rank': row['rank'],
                    'network_id': row.get('network_id'),
                    'entity_id': row['primary_entity_id'],
                    'entity_name': row['network_name'],  # Keep for backwards compatibility
                    'primary_entity_name': row['primary_entity_name'],  # Human principal name
                    'primary_entity_id': row['primary_entity_id'],  # Principal ID for loading
                    'entity_type': row['primary_entity_type'],
                    'primary_entity_type': row['primary_entity_type'],
                    'value': row['property_count'],
                    'property_count': row['property_count'],
                    'principal_count': row['principal_count'] or 0,
                    'total_assessed_value': float(row['total_assessed_value']) if row['total_assessed_value'] else 0.0,
                    'total_appraised_value': float(row['total_appraised_value']) if row['total_appraised_value'] else 0.0,
                    'business_name': row['controlling_business'],
                    'business_count': row['business_count'] or 0,
                    'linked_business_count': row.get('linked_business_count') or 0,
                    'building_count': row['building_count'] or 0,
                    'unit_count': row['unit_count'] or 0,
                    'violation_count': 0,  # Not tracked in new schema
                    'principals': row['principals'] or [],
                    'representative_entities': row['representative_entities']
                })

            return result
    except Exception:
        logger.exception("Could not fetch insights from cache.")
        raise HTTPException(status_code=500, detail="Failed to retrieve insights.")

@app.post("/api/dashboard/network-metrics")
def get_dashboard_network_metrics(req: DashboardNetworkMetricsRequest, conn=Depends(get_db_connection)):
    targets = [target for target in (req.targets or []) if target.key and target.entity_id][:75]
    if not targets:
        return {"metrics": {}}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            target_networks = {
                target.key: _resolve_dashboard_metric_network_ids(cursor, target)
                for target in targets
            }
            metrics = _calculate_dashboard_network_eviction_metrics(cursor, target_networks)
            return {"metrics": metrics}
    except Exception:
        logger.exception("Could not calculate dashboard network metrics.")
        raise HTTPException(status_code=500, detail="Failed to calculate dashboard network metrics.")

def _calculate_ct_dashboard_summary(cursor, city: str) -> Dict[str, Any]:
    selected_city = (city or "STATEWIDE").strip().upper()
    is_statewide = selected_city == "STATEWIDE"
    city_clause = ""
    city_params: Tuple[Any, ...] = ()
    if not is_statewide:
        city_clause = "AND UPPER(COALESCE(property_city, '')) = %s"
        city_params = (selected_city,)

    cursor.execute(f"""
        SELECT
            COUNT(*)::int AS property_count,
            COALESCE(SUM(number_of_units), 0) AS unit_count,
            COUNT(DISTINCT UPPER(COALESCE(property_city, '')))::int AS town_count
        FROM properties
        WHERE source IS DISTINCT FROM 'NYS_OPEN_DATA'
          {city_clause}
    """, city_params)
    property_row = cursor.fetchone() or {}

    cursor.execute(f"""
        WITH matched_properties AS (
            SELECT id, business_id, principal_id
            FROM properties
            WHERE source IS DISTINCT FROM 'NYS_OPEN_DATA'
              {city_clause}
        ),
        property_networks AS (
            SELECT mp.id AS property_id, en.network_id
            FROM matched_properties mp
            JOIN entity_networks en
              ON en.entity_type = 'business'
             AND mp.business_id::text = en.entity_id
            WHERE mp.business_id IS NOT NULL

            UNION

            SELECT mp.id AS property_id, en.network_id
            FROM matched_properties mp
            JOIN entity_networks en
              ON en.entity_type = 'principal'
             AND mp.principal_id::text = en.entity_id
            WHERE mp.principal_id IS NOT NULL
        )
        SELECT
            COUNT(DISTINCT network_id)::int AS network_count,
            COUNT(DISTINCT property_id)::int AS network_linked_property_count
        FROM property_networks
    """, city_params)
    network_row = cursor.fetchone() or {}

    eviction_clause = ""
    if not is_statewide:
        eviction_clause = "WHERE UPPER(COALESCE(municipality, '')) = %s"
    cursor.execute(f"""
        SELECT
            COUNT(*)::int AS eviction_count,
            MIN(filing_date) AS first_filing_date,
            MAX(filing_date) AS last_filing_date
        FROM evictions
        {eviction_clause}
    """, city_params)
    eviction_row = cursor.fetchone() or {}

    code_clause = ""
    if not is_statewide:
        code_clause = "WHERE UPPER(COALESCE(municipality, '')) = %s"
    cursor.execute(f"""
        SELECT
            COUNT(*)::int AS code_record_count,
            MIN(date_opened) AS first_date_opened,
            MAX(date_opened) AS last_date_opened
        FROM code_enforcement
        {code_clause}
    """, city_params)
    code_row = cursor.fetchone() or {}

    first_filing_date = eviction_row.get("first_filing_date")
    last_filing_date = eviction_row.get("last_filing_date")
    first_code_date = code_row.get("first_date_opened")
    last_code_date = code_row.get("last_date_opened")

    return {
        "scope": "CT",
        "city": selected_city,
        "is_statewide": is_statewide,
        "network_count": int(network_row.get("network_count") or 0),
        "network_linked_property_count": int(network_row.get("network_linked_property_count") or 0),
        "property_count": int(property_row.get("property_count") or 0),
        "unit_count": int(float(property_row.get("unit_count") or 0)),
        "town_count": int(property_row.get("town_count") or 0),
        "eviction_count": int(eviction_row.get("eviction_count") or 0),
        "code_record_count": int(code_row.get("code_record_count") or 0),
        "date_ranges": {
            "evictions": {
                "field": "filing_date",
                "label": "CT Judicial eviction filing dates",
                "start": first_filing_date.isoformat() if first_filing_date else None,
                "end": last_filing_date.isoformat() if last_filing_date else None,
            },
            "code_records": {
                "field": "date_opened",
                "label": "Hartford code-enforcement opened dates",
                "start": first_code_date.isoformat() if first_code_date else None,
                "end": last_code_date.isoformat() if last_code_date else None,
            },
        },
        "notes": {
            "properties": "Loaded CT municipal assessment/property rows excluding NYS_OPEN_DATA.",
            "networks": "Ownership networks linked to at least one loaded CT property via matched business/principal IDs.",
            "evictions": "Loaded CT Judicial eviction feed rows.",
            "code_records": "Loaded municipal code-enforcement rows; currently Hartford coverage only for CT."
        }
    }

@app.get("/api/dashboard/summary")
def get_dashboard_summary(city: str = "STATEWIDE", conn=Depends(get_db_connection)):
    selected_city = (city or "STATEWIDE").strip().upper()
    cache_key = f"dashboard_summary:v2:ct:{selected_city}"
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT value, created_at FROM kv_cache WHERE key = %s", (cache_key,))
            row = cursor.fetchone()
            if row and row.get("value"):
                created_at = row["created_at"]
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - created_at).total_seconds() < 3600:
                    return row["value"]

            data = _calculate_ct_dashboard_summary(cursor, selected_city)
            cursor.execute("""
                INSERT INTO kv_cache (key, value, created_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    created_at = NOW()
            """, (cache_key, json.dumps(data, default=json_converter)))
            conn.commit()
            return data
    except Exception:
        logger.exception("Could not calculate dashboard summary.")
        raise HTTPException(status_code=500, detail="Failed to calculate dashboard summary.")

# --- NEW: Data Completeness Report ---


from .municipal_config import MUNICIPAL_DATA_SOURCES

def _calculate_completeness_matrix(conn):
    """
    Calculates the data completeness matrix for all municipalities.
    """
    logger.info("Calculating Data Completeness Matrix...")

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # Check active cities
        active_cities = []
        for city in ["nyc", "dc", "baltimore", "boston", "detroit", "philadelphia", "chicago", "miami"]:
            try:
                cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{city}_properties')")
                row = cursor.fetchone()
                if row and row['exists']:
                    active_cities.append(city)
            except Exception as e:
                logger.warning(f"Error checking table existence for {city}: {e}")
                conn.rollback()

        union_subqueries = []
        for city in active_cities:
            union_subqueries.append(f"""
                SELECT
                    '{city.upper()}' as town,
                    COUNT(*) as total_properties,
                    0 as with_photos,
                    0 as with_cama,
                    COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as with_coords,
                    COUNT(CASE WHEN owner_name IS NOT NULL AND UPPER(owner_name) NOT LIKE 'CURRENT OWNER%' THEN 1 END) as with_owner,
                    COUNT(CASE WHEN year_built IS NOT NULL THEN 1 END) as with_year_built,
                    0 as with_living_area
                FROM {city}_properties
            """)

        union_sql = ""
        if union_subqueries:
            union_sql = " UNION ALL " + " UNION ALL ".join(union_subqueries)

        query = f"""
            WITH prop_stats AS (
                SELECT
                    COALESCE(TRIM(UPPER(property_city)), 'UNKNOWN') as town,
                    COUNT(*) as total_properties,
                    COUNT(CASE WHEN building_photo IS NOT NULL OR image_url IS NOT NULL THEN 1 END) as with_photos,
                    COUNT(CASE WHEN cama_site_link IS NOT NULL THEN 1 END) as with_cama,
                    COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as with_coords,
                    COUNT(CASE WHEN owner IS NOT NULL AND UPPER(owner) NOT LIKE 'CURRENT OWNER%' THEN 1 END) as with_owner,
                    COUNT(CASE WHEN year_built IS NOT NULL THEN 1 END) as with_year_built,
                    COUNT(CASE WHEN living_area IS NOT NULL THEN 1 END) as with_living_area
                FROM properties
                WHERE source IS DISTINCT FROM 'NYS_OPEN_DATA'
                GROUP BY TRIM(UPPER(property_city))
                {union_sql}
            )
            SELECT
                ps.*,
                CASE
                    WHEN ds.refresh_status = 'running' AND ds.last_refreshed_at < NOW() - INTERVAL '6 hours' THEN 'failure'
                    ELSE ds.refresh_status
                END as refresh_status,
                ds.last_refreshed_at,
                ds.details,
                ds.external_last_updated
            FROM prop_stats ps
            LEFT JOIN data_source_status ds ON ps.town = ds.source_name
            ORDER BY ps.town;
        """
        cursor.execute(query)
        prop_rows = cursor.fetchall()

        # Fetch system sources
        cursor.execute("SELECT source_name, last_refreshed_at, refresh_status FROM data_source_status WHERE source_type = 'system'")
        system_rows = cursor.fetchall()
        system_freshness = {}
        for r in system_rows:
             key = r['source_name'].lower().replace(' ', '_') + "_last_updated"
             system_freshness[key] = r['last_refreshed_at'].isoformat() if r['last_refreshed_at'] else None

        city_dataset_prefixes = {
            "NYC": ["NYC"],
            "DC": ["DC"],
            "BALTIMORE": ["BALTIMORE"],
            "BOSTON": ["BOSTON"],
            "DETROIT": ["DETROIT"],
            "PHILADELPHIA": ["PHILADELPHIA"],
            "CHICAGO": ["CHICAGO"],
            "MIAMI": ["MIAMI"],
        }
        cursor.execute("""
            SELECT source_name, source_type, external_last_updated, last_refreshed_at,
                   refresh_status, details
            FROM data_source_status
            WHERE source_name LIKE 'NYC%%'
               OR source_name LIKE 'DC%%'
               OR source_name LIKE 'BALTIMORE%%'
               OR source_name LIKE 'BOSTON%%'
               OR source_name LIKE 'DETROIT%%'
               OR source_name LIKE 'PHILADELPHIA%%'
               OR source_name LIKE 'CHICAGO%%'
               OR source_name LIKE 'MIAMI%%'
            ORDER BY source_name
        """)
        city_dataset_rows = cursor.fetchall()
        city_datasets: Dict[str, List[Dict[str, Any]]] = {key: [] for key in city_dataset_prefixes}
        for ds in city_dataset_rows:
            source_name = (ds.get("source_name") or "").upper()
            city_key = next(
                (
                    city
                    for city, prefixes in city_dataset_prefixes.items()
                    if any(source_name.startswith(prefix) for prefix in prefixes)
                ),
                None
            )
            if not city_key:
                continue
            details = ds.get("details") or {}
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except Exception:
                    details = {"message": details}
            city_datasets[city_key].append({
                "source_name": ds.get("source_name"),
                "source_type": ds.get("source_type"),
                "status": ds.get("refresh_status") or "unknown",
                "external_last_updated": ds.get("external_last_updated").isoformat() if ds.get("external_last_updated") else None,
                "last_refreshed_at": ds.get("last_refreshed_at").isoformat() if ds.get("last_refreshed_at") else None,
                "source_url": details.get("source_url") or details.get("source_dataset") or details.get("sam_crosswalk_source"),
                "source_records": details.get("source_records"),
                "matched_records": details.get("matched_records") or details.get("matched_source_records"),
                "matched_parcels": details.get("matched_parcels"),
                "message": details.get("message") or details.get("eviction_note") or details.get("join_note"),
                "sources": details.get("sources") if isinstance(details.get("sources"), list) else []
            })

        # Format for frontend
        sources = []
        for row in prop_rows:
            town_upper = row['town'].strip().upper()
            portal_url = None

            # Determine Portal URL
            if town_upper in MUNICIPAL_DATA_SOURCES:
                cfg = MUNICIPAL_DATA_SOURCES[town_upper]
                if cfg['type'] == 'PROPERTYRECORDCARDS':
                     portal_url = f"https://www.propertyrecordcards.com/propertyresults.aspx?towncode={cfg['towncode']}"
                elif cfg['type'] == 'MAPXPRESS':
                     portal_url = f"https://{cfg['domain']}"
                elif 'url' in cfg:
                     portal_url = cfg['url']

            # Default fallback for Vision
            if not portal_url:
                portal_url = None

            # Determine Source Date Display
            external_date = row['external_last_updated']
            source_date_display = external_date
            if town_upper in MUNICIPAL_DATA_SOURCES:
                 freq = MUNICIPAL_DATA_SOURCES[town_upper].get('frequency')
                 if freq:
                     if freq in ['Nightly', 'Daily', 'Weekly'] and row.get('last_refreshed_at'):
                         effective_date = row['last_refreshed_at'].strftime('%Y-%m-%d')
                         source_date_display = f"{freq} ({effective_date})"
                     elif external_date:
                         source_date_display = f"{freq} ({external_date})"
                     else:
                         source_date_display = freq

            # Fallback: if we still have no source_date but DO have a last_refreshed_at, show it
            if not source_date_display and row.get('last_refreshed_at'):
                source_date_display = row['last_refreshed_at'].strftime('%Y-%m-%d')

            # Identify state and type
            state = "CT"
            if town_upper == "NYC":
                state = "NY"
                portal_url = "https://opendata.cityofnewyork.us/"
            elif town_upper == "DC":
                state = "DC"
                portal_url = "https://odn.dc.gov/"
            elif town_upper == "BALTIMORE":
                state = "MD"
                portal_url = "https://data.baltimorecity.gov/"
            elif town_upper == "LA":
                state = "CA"
                portal_url = "https://data.lacity.org/"
            elif town_upper == "PHILADELPHIA":
                state = "PA"
                portal_url = "https://openphilly.org/"
            elif town_upper == "BOSTON":
                state = "MA"
                portal_url = "https://data.boston.gov/"
            elif town_upper == "DETROIT":
                state = "MI"
                portal_url = "https://data.detroitmi.gov/"
            elif town_upper == "CHICAGO":
                state = "IL"
                portal_url = "https://data.cityofchicago.org/"
            elif town_upper == "MIAMI":
                state = "FL"
                portal_url = "https://opendata.miamidade.gov/"

            sources.append({
                "municipality": row['town'],
                "status": row['refresh_status'] or 'unknown',
                "last_updated": row['last_refreshed_at'],
                "source_date": source_date_display,
                "total_properties": row['total_properties'],
                "portal_url": portal_url,
                "state": state,
                "type": "city" if state != "CT" else "state",
                "datasets": city_datasets.get(town_upper, []) if state != "CT" else [],
                "metrics": {
                    "photos": row['with_photos'],
                    "cama_links": row['with_cama'],
                    "coords": row['with_coords'],
                    "owner": row['with_owner'],
                    "details": row['with_year_built']  # Proxy for general details
                },
                "percentages": {
                    "photos": round((row['with_photos'] / row['total_properties']) * 100, 1) if row['total_properties'] else 0,
                    "cama_links": round((row['with_cama'] / row['total_properties']) * 100, 1) if row['total_properties'] else 0,
                    "coords": round((row['with_coords'] / row['total_properties']) * 100, 1) if row['total_properties'] else 0,
                    "details": round((row['with_year_built'] / row['total_properties']) * 100, 1) if row['total_properties'] else 0
                }
            })

        # Return composite object (frontend must handle)
        return {
            "sources": sources,
            "system_freshness": system_freshness
        }

@app.get("/api/completeness")
def get_completeness_report(conn=Depends(get_db_connection)):
    """
    Returns the Data Completeness Matrix. Cached for performance.
    """
    try:
        # Check cache first
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT value, created_at FROM kv_cache WHERE key = 'completeness_matrix'")
            row = cursor.fetchone()

            # Cache for 1 hour
            if row and row['value']:
                 # Ensure created_at is aware if not already (psycopg2 usually returns aware)
                 created_at = row['created_at']
                 if created_at.tzinfo is None:
                     created_at = created_at.replace(tzinfo=timezone.utc)

                 age = datetime.now(timezone.utc) - created_at
                 if age.total_seconds() < 3600:
                     return row['value']

        # Calculate fresh if cache miss or stale
        data = _calculate_completeness_matrix(conn)

        # Update cache
        with conn.cursor() as cursor:
             cursor.execute("""
                INSERT INTO kv_cache (key, value, created_at) VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = NOW()
            """, ('completeness_matrix', json.dumps(data, default=json_converter)))
             conn.commit()

        return data

    except Exception as e:
        logger.exception("Failed to generate completeness report")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/cached-reports", response_model=List[CachedReportInfo])
def get_cached_reports(conn=Depends(get_db_connection)):
    """
    Returns a list of previously generated AI reports.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT
                    entity as norm_name,
                    title as entity_name,
                    created_at,
                    LENGTH(content) as size
                FROM ai_reports
                ORDER BY created_at DESC
                LIMIT 100;
            """)
            return cursor.fetchall()
    except Exception:
        logger.exception("Could not fetch cached reports.")
        raise HTTPException(status_code=500, detail="Failed to retrieve cached reports.")


@app.get("/api/reports", response_model=List[Report])
def get_reports(conn=Depends(get_db_connection)):
    reports: List[Report] = []
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT owner AS key, COUNT(*) AS value
            FROM properties
            WHERE owner IS NOT NULL AND owner != ''
            GROUP BY owner
            ORDER BY value DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        reports.append(Report(
            title="Top Owners by Property Count",
            data=[ReportItem(key=r["key"], value=f"{int(r['value']):,} properties") for r in rows]
        ))

        cursor.execute("""
            SELECT owner AS key, COALESCE(SUM(assessed_value), 0) AS value
            FROM properties
            WHERE owner IS NOT NULL AND owner != '' AND assessed_value > 0
            GROUP BY owner
            ORDER BY value DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        reports.append(Report(
            title="Top Owners by Assessed Value",
            data=[ReportItem(key=r["key"], value=f"${int(r['value'] or 0):,}") for r in rows]
        ))

    # 3. Top Networks (Custom Logic) - Prioritize Human Names
    # Note: Logic moved upstream or we query here?
    # For now, let's just ensure we return consistent structure.
    # The user wants "Menachem Gurevitch" (Principal) as header if linked.
    # But this function `_get_top_networks` currently returns OWNERS.
    # We need a separate `top_networks` endpoint or check `get_network_graph`.

    return reports

NON_CT_MONITOR_CITIES = {
    "BALTIMORE": {"db_prefix": "baltimore", "name": "Baltimore", "state": "MD"},
    "BOSTON": {"db_prefix": "boston", "name": "Boston", "state": "MA"},
    "DETROIT": {"db_prefix": "detroit", "name": "Detroit", "state": "MI"},
    "NYC": {"db_prefix": "nyc", "name": "New York City", "state": "NY"},
    "DC": {"db_prefix": "dc", "name": "Washington D.C.", "state": "DC"},
}

@app.get("/api/monitor")
@app.get("/api/hartford/playground")  # Backwards compatibility
def get_landlord_monitor(
    city: str = "HARTFORD",
    dimension: str = "network",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sort_by: str = "violations",
    conn=Depends(get_db_connection),
):
    """
    Landlord monitor endpoint with municipality filter.
    Supports dimensions: network (default), llc, attorney.
    Code-enforcement metrics are meaningful for Hartford only; eviction metrics are statewide.
    """
    selected_city = (city or "HARTFORD").strip().upper()

    if selected_city in NON_CT_MONITOR_CITIES:
        if dimension != "network":
            return []
        db_prefix = NON_CT_MONITOR_CITIES[selected_city]["db_prefix"]
        query = f"""
        WITH network_bbl_stats AS (
            SELECT
                n.network_key,
                SUM(COALESCE(s.violations_total, 0))::int AS violation_count,
                SUM(COALESCE(s.violations_open, 0))::int AS active_violation_count,
                (SUM(COALESCE(s.violations_total, 0)) - SUM(COALESCE(s.violations_open, 0)))::int AS closed_violation_count,
                SUM(COALESCE(s.violations_open, 0))::int AS violations_last_365d,
                MAX(s.last_violation_date) AS last_violation_date,
                SUM(COALESCE(s.evictions_total, 0))::int AS eviction_count,
                MAX(s.last_eviction_date) AS last_eviction_date
            FROM {db_prefix}_networks n
            CROSS JOIN LATERAL UNNEST(n.bbl_list) AS bbl_id
            LEFT JOIN {db_prefix}_bbl_stats s ON s.bbl = bbl_id
            GROUP BY n.network_key
        ),
        candidate_networks AS (
            SELECT
                n.network_key AS network_id,
                n.network_key AS entity_id,
                n.display_name AS entity_name,
                'network'::text AS entity_type,
                COALESCE(n.building_count, 0)::int AS property_count,
                COALESCE(ARRAY_LENGTH(n.member_names, 1), 0)::int AS network_business_count,
                0::int AS network_principal_count,
                COALESCE(stats.violation_count, 0)::int AS violation_count,
                COALESCE(stats.active_violation_count, 0)::int AS active_violation_count,
                COALESCE(stats.closed_violation_count, 0)::int AS closed_violation_count,
                COALESCE(stats.violations_last_365d, 0)::int AS violations_last_365d,
                stats.last_violation_date,
                COALESCE(stats.eviction_count, 0)::int AS eviction_count,
                stats.last_eviction_date,
                n.member_names AS violation_businesses,
                n.connection_signals
            FROM {db_prefix}_networks n
            LEFT JOIN network_bbl_stats stats ON stats.network_key = n.network_key
            ORDER BY
                CASE WHEN %s = 'violations' THEN COALESCE(stats.violation_count, 0)
                     ELSE COALESCE(stats.eviction_count, 0)
                END DESC,
                n.network_key
            LIMIT 100
        )
        SELECT
            network_id,
            entity_id,
            entity_name,
            entity_type,
            %s::text AS selected_city,
            true::boolean AS code_data_available,
            (EXISTS (SELECT 1 FROM {db_prefix}_bbl_stats WHERE evictions_total > 0))::boolean AS eviction_data_available,
            property_count,
            network_business_count,
            network_principal_count,
            violation_count,
            0::int AS entity_violation_count,
            closed_violation_count,
            0::int AS entity_closed_violation_count,
            active_violation_count,
            0::int AS entity_active_violation_count,
            violations_last_365d AS violations_last_90d,
            violations_last_365d,
            0::int AS entity_violations_last_90d,
            0::int AS entity_violations_last_365d,
            eviction_count,
            0::int AS entity_eviction_count,
            0::int AS closed_eviction_count,
            0::int AS entity_closed_eviction_count,
            0::int AS active_eviction_count,
            0::int AS entity_active_eviction_count,
            0::int AS evictions_last_90d,
            0::int AS evictions_last_365d,
            0::int AS entity_evictions_last_90d,
            0::int AS entity_evictions_last_365d,
            eviction_count AS local_eviction_count,
            0::int AS local_evictions_last_90d,
            0::int AS local_evictions_last_365d,
            0::int AS outside_eviction_count,
            0::int AS outside_evictions_last_90d,
            0::int AS outside_evictions_last_365d,
            0::int AS entity_local_eviction_count,
            0::int AS entity_outside_eviction_count,
            0::int AS evictions_prev_365d,
            false::boolean AS eviction_surge_flag,
            null::date AS eviction_surge_date,
            0::int AS eviction_surge_filings,
            0::float AS eviction_surge_avg_daily,
            0::float AS eviction_surge_multiplier,
            false::boolean AS attorney_surge_flag,
            null::text AS attorney_surge_name,
            null::date AS attorney_surge_date,
            0::int AS attorney_surge_filings,
            0::float AS attorney_surge_avg_daily,
            0::float AS attorney_surge_multiplier,
            '[]'::jsonb AS violation_type_breakdown,
            '[]'::jsonb AS violation_status_breakdown,
            '[]'::jsonb AS eviction_status_breakdown,
            violation_businesses,
            last_violation_date,
            last_eviction_date,
            connection_signals
        FROM candidate_networks
        """
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (sort_by, selected_city))
                rows = cursor.fetchall()
                import json
                for row in rows:
                    conn_sigs = {}
                    if row.get("connection_signals"):
                        try:
                            if isinstance(row["connection_signals"], str):
                                conn_sigs = json.loads(row["connection_signals"])
                            elif isinstance(row["connection_signals"], dict):
                                conn_sigs = row["connection_signals"]
                        except Exception:
                            pass

                    people = conn_sigs.get("people", [])
                    formatted_principals = []
                    for p in people:
                        if not p:
                            continue
                        words = p.strip().split()
                        if len(words) == 2:
                            formatted_name = f"{words[1].title()} {words[0].title()}"
                        elif len(words) == 3 and words[-1].upper() in {"JR", "SR", "III", "IV", "II"}:
                            formatted_name = f"{words[1].title()} {words[0].title()} {words[2].title()}"
                        else:
                            formatted_name = " ".join([w.title() for w in words])
                        formatted_principals.append({"name": formatted_name, "state": selected_city})

                    row["principals"] = formatted_principals
                    row["business_names"] = row["violation_businesses"]
                return rows
        except Exception as e:
            logger.exception(f"Failed to fetch {selected_city} monitor data")
            raise HTTPException(status_code=500, detail=str(e))

    is_statewide = selected_city == "STATEWIDE"
    is_hartford = selected_city == "HARTFORD"
    city_property_filter_sql = "" if is_statewide else "WHERE UPPER(property_city) = %s"

    # Build date filter clause
    date_params = []
    date_clause = ""
    if date_from:
        date_clause += " AND e.filing_date >= %s"
        date_params.append(date_from)
    if date_to:
        date_clause += " AND e.filing_date <= %s"
        date_params.append(date_to)

    # ── LLC dimension ──────────────────────────────────────────
    if dimension == "llc":
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = f"""
                WITH base AS (
                    SELECT
                        e.plaintiff_norm AS dim_key,
                        e.plaintiff_norm AS dim_label,
                        e.filing_date,
                        e.status,
                        e.disposition_date
                    FROM evictions e
                    WHERE e.plaintiff_norm IS NOT NULL
                      AND TRIM(e.plaintiff_norm) NOT IN ('', '\\N')
                      AND e.filing_date IS NOT NULL
                      {date_clause}
                ),
                agg AS (
                    SELECT
                        dim_key,
                        dim_label,
                        COUNT(*)::int AS eviction_count,
                        COUNT(*) FILTER (WHERE filing_date >= CURRENT_DATE - INTERVAL '365 days')::int AS evictions_last_365d,
                        COUNT(*) FILTER (WHERE filing_date >= CURRENT_DATE - INTERVAL '90 days')::int AS evictions_last_90d,
                        COUNT(*) FILTER (
                            WHERE NOT (lower(COALESCE(status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)')
                        )::int AS active_eviction_count,
                        COUNT(*) FILTER (
                            WHERE lower(COALESCE(status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)'
                        )::int AS closed_eviction_count,
                        MAX(filing_date) AS last_eviction_date,
                        MIN(EXTRACT(YEAR FROM filing_date))::int AS earliest_filing_year,
                        ROUND(AVG(disposition_date - filing_date) FILTER (WHERE disposition_date IS NOT NULL AND filing_date IS NOT NULL))::int AS avg_case_duration_days
                    FROM base
                    GROUP BY dim_key, dim_label
                    HAVING COUNT(*) >= 2
                )
                SELECT * FROM agg
                ORDER BY eviction_count DESC
                LIMIT 100
                """
                params = list(date_params)
                cursor.execute(query, params)
                rows = cursor.fetchall()
                return [
                    MonitorItem(
                        dimension_type="llc",
                        dimension_key=r["dim_key"],
                        dimension_label=r["dim_label"],
                        selected_city=selected_city,
                        code_data_available=False,
                        eviction_data_available=True,
                        eviction_count=r["eviction_count"],
                        evictions_last_365d=r["evictions_last_365d"],
                        evictions_last_90d=r["evictions_last_90d"],
                        active_eviction_count=r["active_eviction_count"],
                        closed_eviction_count=r["closed_eviction_count"],
                        last_eviction_date=r["last_eviction_date"],
                        earliest_filing_year=r.get("earliest_filing_year"),
                        avg_case_duration_days=r.get("avg_case_duration_days"),
                    )
                    for r in rows
                ]
        except Exception as e:
            logger.exception("Failed to fetch LLC monitor data")
            raise HTTPException(status_code=500, detail=str(e))

    # ── Attorney dimension ─────────────────────────────────────
    if dimension == "attorney":
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Determine which attorney column exists
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'evictions'
                      AND column_name IN ('plaintiff_attorney_firm', 'plaintiff_attorney_name', 'plaintiff_attorney_norm')
                """)
                atty_cols = {row["column_name"] for row in cursor.fetchall()}
                if "plaintiff_attorney_norm" in atty_cols:
                    atty_key = "e.plaintiff_attorney_norm"
                elif "plaintiff_attorney_firm" in atty_cols:
                    atty_key = "COALESCE(NULLIF(TRIM(e.plaintiff_attorney_firm), ''), NULLIF(TRIM(e.plaintiff_attorney_name), ''))"
                else:
                    return []

                query = f"""
                WITH base AS (
                    SELECT
                        {atty_key} AS dim_key,
                        {atty_key} AS dim_label,
                        e.filing_date,
                        e.status,
                        e.disposition_date
                    FROM evictions e
                    WHERE {atty_key} IS NOT NULL
                      AND TRIM({atty_key}) NOT IN ('', '\\N', 'n/a', 'N/A')
                      AND e.filing_date IS NOT NULL
                      {date_clause}
                ),
                agg AS (
                    SELECT
                        dim_key,
                        dim_label,
                        COUNT(*)::int AS eviction_count,
                        COUNT(*) FILTER (WHERE filing_date >= CURRENT_DATE - INTERVAL '365 days')::int AS evictions_last_365d,
                        COUNT(*) FILTER (WHERE filing_date >= CURRENT_DATE - INTERVAL '90 days')::int AS evictions_last_90d,
                        COUNT(*) FILTER (
                            WHERE NOT (lower(COALESCE(status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)')
                        )::int AS active_eviction_count,
                        COUNT(*) FILTER (
                            WHERE lower(COALESCE(status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)'
                        )::int AS closed_eviction_count,
                        MAX(filing_date) AS last_eviction_date,
                        MIN(EXTRACT(YEAR FROM filing_date))::int AS earliest_filing_year,
                        ROUND(AVG(disposition_date - filing_date) FILTER (WHERE disposition_date IS NOT NULL AND filing_date IS NOT NULL))::int AS avg_case_duration_days
                    FROM base
                    GROUP BY dim_key, dim_label
                    HAVING COUNT(*) >= 2
                )
                SELECT * FROM agg
                ORDER BY eviction_count DESC
                LIMIT 100
                """
                params = list(date_params)
                cursor.execute(query, params)
                rows = cursor.fetchall()
                return [
                    MonitorItem(
                        dimension_type="attorney",
                        dimension_key=r["dim_key"],
                        dimension_label=r["dim_label"],
                        selected_city=selected_city,
                        code_data_available=False,
                        eviction_data_available=True,
                        eviction_count=r["eviction_count"],
                        evictions_last_365d=r["evictions_last_365d"],
                        evictions_last_90d=r["evictions_last_90d"],
                        active_eviction_count=r["active_eviction_count"],
                        closed_eviction_count=r["closed_eviction_count"],
                        last_eviction_date=r["last_eviction_date"],
                        earliest_filing_year=r.get("earliest_filing_year"),
                        avg_case_duration_days=r.get("avg_case_duration_days"),
                    )
                    for r in rows
                ]
        except Exception as e:
            logger.exception("Failed to fetch attorney monitor data")
            raise HTTPException(status_code=500, detail=str(e))

    # ── Network dimension (existing logic) ─────────────────────
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'evictions'
                  AND column_name IN ('plaintiff_attorney_firm', 'plaintiff_attorney_name', 'plaintiff_attorney_norm')
                """
            )
            eviction_columns = {row["column_name"] for row in cursor.fetchall()}
            has_attorney_fields = bool(eviction_columns)
            attorney_expr_sql = (
                """
                COALESCE(
                    NULLIF(TRIM(e.plaintiff_attorney_firm), ''),
                    NULLIF(TRIM(e.plaintiff_attorney_name), ''),
                    NULLIF(TRIM(e.plaintiff_attorney_norm), '')
                )
                """
                if has_attorney_fields
                else "NULL::text"
            )

            query = f"""
            WITH city_properties AS (
                SELECT id, owner_norm, co_owner_norm, owner, co_owner, business_id
                FROM properties
                {city_property_filter_sql}
            ),
            city_network_property_links AS (
                SELECT DISTINCT en.network_id, cp.id AS property_id
                FROM entity_networks en
                JOIN city_properties cp
                  ON en.normalized_name = cp.owner_norm
                WHERE en.normalized_name IS NOT NULL AND TRIM(en.normalized_name) <> ''

                UNION

                SELECT DISTINCT en.network_id, cp.id AS property_id
                FROM entity_networks en
                JOIN city_properties cp
                  ON en.normalized_name = cp.co_owner_norm
                WHERE en.normalized_name IS NOT NULL AND TRIM(en.normalized_name) <> ''
            ),
            network_props AS (
                SELECT network_id, COUNT(DISTINCT property_id)::int AS property_count
                FROM city_network_property_links
                GROUP BY network_id
            ),
            candidate_networks AS (
                SELECT network_id
                FROM network_props
                ORDER BY property_count DESC, network_id
                LIMIT 100
            ),
            network_property_links AS (
                SELECT cnpl.network_id, cnpl.property_id
                FROM city_network_property_links cnpl
                WHERE cnpl.network_id IN (SELECT network_id FROM candidate_networks)
            ),
            statewide_network_property_links AS (
                SELECT DISTINCT
                    en.network_id,
                    p.id AS property_id,
                    UPPER(COALESCE(p.property_city, '')) AS property_city
                FROM entity_networks en
                JOIN properties p
                  ON en.normalized_name = p.owner_norm
                WHERE en.network_id IN (SELECT network_id FROM candidate_networks)
                  AND en.normalized_name IS NOT NULL
                  AND TRIM(en.normalized_name) <> ''

                UNION

                SELECT DISTINCT
                    en.network_id,
                    p.id AS property_id,
                    UPPER(COALESCE(p.property_city, '')) AS property_city
                FROM entity_networks en
                JOIN properties p
                  ON en.normalized_name = p.co_owner_norm
                WHERE en.network_id IN (SELECT network_id FROM candidate_networks)
                  AND en.normalized_name IS NOT NULL
                  AND TRIM(en.normalized_name) <> ''
            ),
            network_violations AS (
                SELECT
                    cnpl.network_id,
                    COUNT(ce.id)::int AS violation_count,
                    COUNT(*) FILTER (
                        WHERE
                            ce.date_closed IS NOT NULL
                            OR lower(COALESCE(ce.record_status, '')) IN (
                                'closed', 'resolved', 'complied', 'complete', 'completed'
                            )
                    )::int AS closed_violation_count,
                    COUNT(*) FILTER (
                        WHERE NOT (
                            ce.date_closed IS NOT NULL
                            OR lower(COALESCE(ce.record_status, '')) IN (
                                'closed', 'resolved', 'complied', 'complete', 'completed'
                            )
                        )
                    )::int AS active_violation_count,
                    COUNT(*) FILTER (WHERE ce.date_opened >= CURRENT_DATE - INTERVAL '90 days')::int AS violations_last_90d,
                    COUNT(*) FILTER (WHERE ce.date_opened >= CURRENT_DATE - INTERVAL '365 days')::int AS violations_last_365d,
                    MAX(ce.date_opened) AS last_violation_date
                FROM network_property_links cnpl
                JOIN code_enforcement ce ON ce.property_id = cnpl.property_id
                GROUP BY cnpl.network_id
            ),
            violation_types_ranked AS (
                SELECT
                    cnpl.network_id,
                    COALESCE(
                        NULLIF(TRIM(ce.record_type), ''),
                        NULLIF(TRIM(ce.inspection_type), ''),
                        'Unspecified'
                    ) AS label,
                    COUNT(*)::int AS count,
                    ROW_NUMBER() OVER (
                        PARTITION BY cnpl.network_id
                        ORDER BY COUNT(*) DESC, COALESCE(NULLIF(TRIM(ce.record_type), ''), NULLIF(TRIM(ce.inspection_type), ''), 'Unspecified')
                    ) AS rn
                FROM network_property_links cnpl
                JOIN code_enforcement ce ON ce.property_id = cnpl.property_id
                GROUP BY cnpl.network_id, label
            ),
            violation_types AS (
                SELECT
                    network_id,
                    COALESCE(
                        jsonb_agg(jsonb_build_object('label', label, 'count', count) ORDER BY count DESC, label)
                            FILTER (WHERE rn <= 3),
                        '[]'::jsonb
                    ) AS violation_type_breakdown
                FROM violation_types_ranked
                GROUP BY network_id
            ),
            violation_status_ranked AS (
                SELECT
                    cnpl.network_id,
                    CASE
                        WHEN ce.date_closed IS NOT NULL
                          OR lower(COALESCE(ce.record_status, '')) IN ('closed', 'resolved', 'complied', 'complete', 'completed')
                            THEN 'Closed/Resolved'
                        WHEN NULLIF(TRIM(ce.record_status), '') IS NULL
                            THEN 'Unknown'
                        ELSE TRIM(ce.record_status)
                    END AS label,
                    COUNT(*)::int AS count,
                    ROW_NUMBER() OVER (
                        PARTITION BY cnpl.network_id
                        ORDER BY COUNT(*) DESC,
                            CASE
                                WHEN ce.date_closed IS NOT NULL
                                  OR lower(COALESCE(ce.record_status, '')) IN ('closed', 'resolved', 'complied', 'complete', 'completed')
                                    THEN 'Closed/Resolved'
                                WHEN NULLIF(TRIM(ce.record_status), '') IS NULL
                                    THEN 'Unknown'
                                ELSE TRIM(ce.record_status)
                            END
                    ) AS rn
                FROM network_property_links cnpl
                JOIN code_enforcement ce ON ce.property_id = cnpl.property_id
                GROUP BY cnpl.network_id, label
            ),
            violation_status AS (
                SELECT
                    network_id,
                    COALESCE(
                        jsonb_agg(jsonb_build_object('label', label, 'count', count) ORDER BY count DESC, label)
                            FILTER (WHERE rn <= 3),
                        '[]'::jsonb
                    ) AS violation_status_breakdown
                FROM violation_status_ranked
                GROUP BY network_id
            ),
            network_violation_businesses_raw AS (
                SELECT DISTINCT
                    cnpl.network_id,
                    b.name
                FROM network_property_links cnpl
                JOIN code_enforcement ce ON ce.property_id = cnpl.property_id
                JOIN properties p ON p.id = cnpl.property_id
                JOIN businesses b ON b.id = p.business_id
                WHERE b.name IS NOT NULL AND b.name <> ''

                UNION

                SELECT DISTINCT
                    en.network_id,
                    en.entity_name AS name
                FROM entity_networks en
                JOIN network_violations nv ON nv.network_id = en.network_id
                WHERE
                    en.entity_type = 'business'
                    AND en.entity_name IS NOT NULL
                    AND en.entity_name <> ''
            ),
            network_violation_businesses AS (
                SELECT
                    network_id,
                    to_jsonb(array_agg(name ORDER BY name)) AS violation_businesses
                FROM network_violation_businesses_raw
                GROUP BY network_id
            ),
            network_eviction_cases_raw AS (
                SELECT DISTINCT
                    snpl.network_id,
                    COALESCE(e.case_number, e.id::text) AS eviction_key,
                    e.filing_date,
                    e.status,
                    {attorney_expr_sql} AS plaintiff_attorney,
                    CASE WHEN %s::boolean OR snpl.property_city = %s THEN true ELSE false END AS in_selected_city
                FROM statewide_network_property_links snpl
                JOIN evictions e ON e.property_id = snpl.property_id
            ),
            network_eviction_cases AS (
                SELECT DISTINCT ON (network_id, eviction_key)
                    network_id,
                    eviction_key,
                    filing_date,
                    status,
                    plaintiff_attorney,
                    in_selected_city
                FROM network_eviction_cases_raw
                ORDER BY network_id, eviction_key, in_selected_city DESC, filing_date DESC NULLS LAST
            ),
            network_evictions AS (
                SELECT
                    nec.network_id,
                    COUNT(*)::int AS eviction_count,
                    COUNT(*) FILTER (
                        WHERE lower(COALESCE(nec.status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)'
                    )::int AS closed_eviction_count,
                    COUNT(*) FILTER (
                        WHERE NOT (lower(COALESCE(nec.status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)')
                    )::int AS active_eviction_count,
                    COUNT(*) FILTER (WHERE nec.filing_date >= CURRENT_DATE - INTERVAL '90 days')::int AS evictions_last_90d,
                    COUNT(*) FILTER (WHERE nec.filing_date >= CURRENT_DATE - INTERVAL '365 days')::int AS evictions_last_365d,
                    COUNT(*) FILTER (
                        WHERE nec.filing_date >= CURRENT_DATE - INTERVAL '730 days'
                          AND nec.filing_date < CURRENT_DATE - INTERVAL '365 days'
                    )::int AS evictions_prev_365d,
                    COUNT(*) FILTER (WHERE nec.in_selected_city)::int AS local_eviction_count,
                    COUNT(*) FILTER (WHERE nec.in_selected_city AND nec.filing_date >= CURRENT_DATE - INTERVAL '90 days')::int AS local_evictions_last_90d,
                    COUNT(*) FILTER (WHERE nec.in_selected_city AND nec.filing_date >= CURRENT_DATE - INTERVAL '365 days')::int AS local_evictions_last_365d,
                    COUNT(*) FILTER (WHERE NOT nec.in_selected_city)::int AS outside_eviction_count,
                    COUNT(*) FILTER (WHERE NOT nec.in_selected_city AND nec.filing_date >= CURRENT_DATE - INTERVAL '90 days')::int AS outside_evictions_last_90d,
                    COUNT(*) FILTER (WHERE NOT nec.in_selected_city AND nec.filing_date >= CURRENT_DATE - INTERVAL '365 days')::int AS outside_evictions_last_365d,
                    MAX(nec.filing_date) AS last_eviction_date
                FROM network_eviction_cases nec
                GROUP BY nec.network_id
            ),
            eviction_weekly_counts AS (
                SELECT
                    nec.network_id,
                    DATE_TRUNC('week', nec.filing_date)::date AS filing_week,
                    COUNT(*)::int AS filings_in_week
                FROM network_eviction_cases nec
                WHERE
                    nec.filing_date IS NOT NULL
                    AND nec.filing_date >= CURRENT_DATE - INTERVAL '365 days'
                GROUP BY nec.network_id, DATE_TRUNC('week', nec.filing_date)::date
            ),
            eviction_surge AS (
                SELECT
                    ewc.network_id,
                    MAX(ewc.filings_in_week)::int AS eviction_surge_filings,
                    (
                        ARRAY_AGG(ewc.filing_week ORDER BY ewc.filings_in_week DESC, ewc.filing_week DESC)
                    )[1] AS eviction_surge_date,
                    AVG(ewc.filings_in_week)::float AS eviction_surge_avg_daily
                FROM eviction_weekly_counts ewc
                GROUP BY ewc.network_id
            ),
            attorney_weekly_counts AS (
                SELECT
                    nec.network_id,
                    nec.plaintiff_attorney,
                    DATE_TRUNC('week', nec.filing_date)::date AS filing_week,
                    COUNT(*)::int AS filings_in_week
                FROM network_eviction_cases nec
                WHERE
                    nec.filing_date IS NOT NULL
                    AND nec.filing_date >= CURRENT_DATE - INTERVAL '365 days'
                    AND NULLIF(TRIM(COALESCE(nec.plaintiff_attorney, '')), '') IS NOT NULL
                GROUP BY nec.network_id, nec.plaintiff_attorney, DATE_TRUNC('week', nec.filing_date)::date
            ),
            attorney_surge_candidates AS (
                SELECT
                    awc.network_id,
                    awc.plaintiff_attorney AS attorney_surge_name,
                    MAX(awc.filings_in_week)::int AS attorney_surge_filings,
                    (
                        ARRAY_AGG(awc.filing_week ORDER BY awc.filings_in_week DESC, awc.filing_week DESC)
                    )[1] AS attorney_surge_date,
                    AVG(awc.filings_in_week)::float AS attorney_surge_avg_daily
                FROM attorney_weekly_counts awc
                GROUP BY awc.network_id, awc.plaintiff_attorney
            ),
            attorney_surge AS (
                SELECT
                    ranked.network_id,
                    ranked.attorney_surge_name,
                    ranked.attorney_surge_filings,
                    ranked.attorney_surge_date,
                    ranked.attorney_surge_avg_daily
                FROM (
                    SELECT
                        ascx.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY ascx.network_id
                            ORDER BY
                                ascx.attorney_surge_filings DESC,
                                ascx.attorney_surge_avg_daily DESC,
                                ascx.attorney_surge_name
                        ) AS rn
                    FROM attorney_surge_candidates ascx
                ) ranked
                WHERE ranked.rn = 1
            ),
            eviction_status_ranked AS (
                SELECT
                    nec.network_id,
                    CASE
                        WHEN NULLIF(TRIM(nec.status), '') IS NULL THEN 'Unknown'
                        WHEN lower(nec.status) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)' THEN 'Closed/Disposed'
                        ELSE TRIM(nec.status)
                    END AS label,
                    COUNT(*)::int AS count,
                    ROW_NUMBER() OVER (
                        PARTITION BY nec.network_id
                        ORDER BY COUNT(*) DESC,
                            CASE
                                WHEN NULLIF(TRIM(nec.status), '') IS NULL THEN 'Unknown'
                                WHEN lower(nec.status) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)' THEN 'Closed/Disposed'
                                ELSE TRIM(nec.status)
                            END
                    ) AS rn
                FROM network_eviction_cases nec
                GROUP BY nec.network_id, label
            ),
            eviction_status AS (
                SELECT
                    network_id,
                    COALESCE(
                        jsonb_agg(jsonb_build_object('label', label, 'count', count) ORDER BY count DESC, label)
                            FILTER (WHERE rn <= 3),
                        '[]'::jsonb
                    ) AS eviction_status_breakdown
                FROM eviction_status_ranked
                GROUP BY network_id
            ),
            ranked_entities AS (
                SELECT
                    en.network_id,
                    en.entity_id,
                    en.entity_name,
                    en.entity_type,
                    en.normalized_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY en.network_id
                        ORDER BY en.entity_type = 'principal' DESC, length(en.entity_name) ASC
                    ) AS rank
                FROM entity_networks en
                WHERE en.network_id IN (SELECT network_id FROM candidate_networks)
            ),
            representative_entities AS (
                SELECT network_id, entity_id, entity_name, entity_type, normalized_name
                FROM ranked_entities
                WHERE rank = 1
            ),
            network_entity_counts AS (
                SELECT
                    en.network_id,
                    COUNT(*) FILTER (WHERE en.entity_type = 'business')::int AS network_business_count,
                    COUNT(*) FILTER (WHERE en.entity_type = 'principal')::int AS network_principal_count
                FROM entity_networks en
                WHERE en.network_id IN (SELECT network_id FROM candidate_networks)
                GROUP BY en.network_id
            ),
            entity_property_links AS (
                SELECT DISTINCT
                    re.network_id,
                    cp.id AS property_id
                FROM representative_entities re
                JOIN city_properties cp
                  ON (
                    re.entity_type = 'business'
                    AND (
                        cp.business_id::text = re.entity_id
                        OR UPPER(COALESCE(cp.owner, '')) = UPPER(COALESCE(re.entity_name, ''))
                        OR cp.owner_norm = re.normalized_name
                        OR cp.co_owner_norm = re.normalized_name
                    )
                  )
                  OR (
                    re.entity_type = 'principal'
                    AND (
                        cp.owner_norm = re.entity_id
                        OR cp.co_owner_norm = re.entity_id
                        OR cp.owner_norm = re.normalized_name
                        OR cp.co_owner_norm = re.normalized_name
                        OR UPPER(COALESCE(cp.owner, '')) = UPPER(COALESCE(re.entity_name, ''))
                        OR UPPER(COALESCE(cp.co_owner, '')) = UPPER(COALESCE(re.entity_name, ''))
                    )
                  )
            ),
            entity_violations AS (
                SELECT
                    epl.network_id,
                    COUNT(ce.id)::int AS entity_violation_count,
                    COUNT(*) FILTER (
                        WHERE
                            ce.date_closed IS NOT NULL
                            OR lower(COALESCE(ce.record_status, '')) IN (
                                'closed', 'resolved', 'complied', 'complete', 'completed'
                            )
                    )::int AS entity_closed_violation_count,
                    COUNT(*) FILTER (
                        WHERE NOT (
                            ce.date_closed IS NOT NULL
                            OR lower(COALESCE(ce.record_status, '')) IN (
                                'closed', 'resolved', 'complied', 'complete', 'completed'
                            )
                        )
                    )::int AS entity_active_violation_count,
                    COUNT(*) FILTER (WHERE ce.date_opened >= CURRENT_DATE - INTERVAL '90 days')::int AS entity_violations_last_90d,
                    COUNT(*) FILTER (WHERE ce.date_opened >= CURRENT_DATE - INTERVAL '365 days')::int AS entity_violations_last_365d
                FROM entity_property_links epl
                JOIN code_enforcement ce ON ce.property_id = epl.property_id
                GROUP BY epl.network_id
            ),
            entity_evictions AS (
                SELECT
                    epl.network_id,
                    COUNT(*)::int AS entity_eviction_count,
                    COUNT(*) FILTER (
                        WHERE lower(COALESCE(e.status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)'
                    )::int AS entity_closed_eviction_count,
                    COUNT(*) FILTER (
                        WHERE NOT (lower(COALESCE(e.status, '')) ~ '(closed|disposed|dismissed|withdrawn|settled|judgment)')
                    )::int AS entity_active_eviction_count,
                    COUNT(*) FILTER (WHERE e.filing_date >= CURRENT_DATE - INTERVAL '90 days')::int AS entity_evictions_last_90d,
                    COUNT(*) FILTER (WHERE e.filing_date >= CURRENT_DATE - INTERVAL '365 days')::int AS entity_evictions_last_365d,
                    COUNT(*)::int AS entity_local_eviction_count,
                    0::int AS entity_outside_eviction_count
                FROM entity_property_links epl
                JOIN evictions e ON e.property_id = epl.property_id
                GROUP BY epl.network_id
            )
            SELECT
                re.network_id,
                re.entity_id,
                re.entity_name,
                %s::text AS selected_city,
                %s::boolean AS code_data_available,
                true::boolean AS eviction_data_available,
                COALESCE(nec.network_business_count, 0) AS network_business_count,
                COALESCE(nec.network_principal_count, 0) AS network_principal_count,
                COALESCE(np.property_count, 0) AS property_count,
                COALESCE(nv.violation_count, 0) AS violation_count,
                COALESCE(ev.entity_violation_count, 0) AS entity_violation_count,
                COALESCE(nv.closed_violation_count, 0) AS closed_violation_count,
                COALESCE(ev.entity_closed_violation_count, 0) AS entity_closed_violation_count,
                COALESCE(nv.active_violation_count, 0) AS active_violation_count,
                COALESCE(ev.entity_active_violation_count, 0) AS entity_active_violation_count,
                COALESCE(nv.violations_last_90d, 0) AS violations_last_90d,
                COALESCE(nv.violations_last_365d, 0) AS violations_last_365d,
                COALESCE(ev.entity_violations_last_90d, 0) AS entity_violations_last_90d,
                COALESCE(ev.entity_violations_last_365d, 0) AS entity_violations_last_365d,
                COALESCE(ne.eviction_count, 0) AS eviction_count,
                COALESCE(ee.entity_eviction_count, 0) AS entity_eviction_count,
                COALESCE(ne.closed_eviction_count, 0) AS closed_eviction_count,
                COALESCE(ee.entity_closed_eviction_count, 0) AS entity_closed_eviction_count,
                COALESCE(ne.active_eviction_count, 0) AS active_eviction_count,
                COALESCE(ee.entity_active_eviction_count, 0) AS entity_active_eviction_count,
                COALESCE(ne.evictions_last_90d, 0) AS evictions_last_90d,
                COALESCE(ne.evictions_last_365d, 0) AS evictions_last_365d,
                COALESCE(ee.entity_evictions_last_90d, 0) AS entity_evictions_last_90d,
                COALESCE(ee.entity_evictions_last_365d, 0) AS entity_evictions_last_365d,
                COALESCE(ne.local_eviction_count, 0) AS local_eviction_count,
                COALESCE(ne.local_evictions_last_90d, 0) AS local_evictions_last_90d,
                COALESCE(ne.local_evictions_last_365d, 0) AS local_evictions_last_365d,
                COALESCE(ne.outside_eviction_count, 0) AS outside_eviction_count,
                COALESCE(ne.outside_evictions_last_90d, 0) AS outside_evictions_last_90d,
                COALESCE(ne.outside_evictions_last_365d, 0) AS outside_evictions_last_365d,
                COALESCE(ee.entity_local_eviction_count, 0) AS entity_local_eviction_count,
                COALESCE(ee.entity_outside_eviction_count, 0) AS entity_outside_eviction_count,
                COALESCE(ne.evictions_prev_365d, 0) AS evictions_prev_365d,
                CASE
                    WHEN
                        COALESCE(esg.eviction_surge_filings, 0) >= 8
                        AND COALESCE(esg.eviction_surge_filings, 0) >= GREATEST(3, CEIL(COALESCE(esg.eviction_surge_avg_daily, 0) * 3))
                    THEN TRUE
                    ELSE FALSE
                END AS eviction_surge_flag,
                esg.eviction_surge_date,
                COALESCE(esg.eviction_surge_filings, 0) AS eviction_surge_filings,
                COALESCE(esg.eviction_surge_avg_daily, 0) AS eviction_surge_avg_daily,
                CASE
                    WHEN COALESCE(esg.eviction_surge_avg_daily, 0) > 0
                        THEN ROUND((COALESCE(esg.eviction_surge_filings, 0)::numeric / esg.eviction_surge_avg_daily::numeric), 2)::float
                    ELSE 0::float
                END AS eviction_surge_multiplier,
                CASE
                    WHEN
                        COALESCE(asg.attorney_surge_filings, 0) >= 6
                        AND COALESCE(asg.attorney_surge_filings, 0) >= GREATEST(3, CEIL(COALESCE(asg.attorney_surge_avg_daily, 0) * 2.5))
                    THEN TRUE
                    ELSE FALSE
                END AS attorney_surge_flag,
                asg.attorney_surge_name,
                asg.attorney_surge_date,
                COALESCE(asg.attorney_surge_filings, 0) AS attorney_surge_filings,
                COALESCE(asg.attorney_surge_avg_daily, 0) AS attorney_surge_avg_daily,
                CASE
                    WHEN COALESCE(asg.attorney_surge_avg_daily, 0) > 0
                        THEN ROUND((COALESCE(asg.attorney_surge_filings, 0)::numeric / asg.attorney_surge_avg_daily::numeric), 2)::float
                    ELSE 0::float
                END AS attorney_surge_multiplier,
                '[]'::jsonb AS violation_type_breakdown,
                '[]'::jsonb AS violation_status_breakdown,
                '[]'::jsonb AS eviction_status_breakdown,
                '[]'::jsonb AS violation_businesses,
                nv.last_violation_date,
                ne.last_eviction_date
            FROM representative_entities re
            LEFT JOIN network_props np ON re.network_id = np.network_id
            LEFT JOIN network_entity_counts nec ON re.network_id = nec.network_id
            LEFT JOIN network_violations nv ON re.network_id = nv.network_id
            LEFT JOIN entity_violations ev ON re.network_id = ev.network_id
            LEFT JOIN network_evictions ne ON re.network_id = ne.network_id
            LEFT JOIN entity_evictions ee ON re.network_id = ee.network_id
            LEFT JOIN eviction_surge esg ON re.network_id = esg.network_id
            LEFT JOIN attorney_surge asg ON re.network_id = asg.network_id
            WHERE ((%s::boolean AND COALESCE(nv.violation_count, 0) > 0) OR COALESCE(ne.eviction_count, 0) > 0)
            ORDER BY
                CASE WHEN %s = 'violations' AND %s::boolean
                    THEN COALESCE(nv.violation_count, 0)
                    WHEN %s = 'attorneys'
                    THEN COALESCE(asg.attorney_surge_filings, 0)
                    ELSE COALESCE(ne.eviction_count, 0)
                END DESC,
                (
                    CASE WHEN %s::boolean
                        THEN COALESCE(nv.violations_last_365d, 0) * 2 + COALESCE(nv.active_violation_count, 0)
                        ELSE 0
                    END
                    + COALESCE(ne.local_evictions_last_365d, 0) * 2
                    + COALESCE(ne.local_eviction_count, 0)
                    + (COALESCE(ne.outside_evictions_last_365d, 0) / 5.0)
                ) DESC
            LIMIT 100
            """
            params = []
            if not is_statewide:
                params.append(selected_city)
            params.extend((
                is_statewide,
                selected_city,
                selected_city,
                is_hartford,
                is_hartford,
                sort_by,
                is_hartford,
                sort_by,
                is_hartford,
            ))
            cursor.execute(query, params)
            rows = cursor.fetchall()

            result = []
            for row in rows:
                # Fetch deduplicated principals (case-insensitive)
                cursor.execute(
                    """
                    SELECT MIN(INITCAP(name_c)) as name, MAX(state) as state
                    FROM principals
                    WHERE business_id IN (
                        SELECT entity_id FROM entity_networks WHERE network_id = %s AND entity_type = 'business'
                    )
                    GROUP BY UPPER(name_c)
                    LIMIT 5
                    """,
                    (row["network_id"],),
                )
                row["principals"] = cursor.fetchall()
                # Fetch business/LLC names in the network
                cursor.execute(
                    """
                    SELECT DISTINCT entity_name
                    FROM entity_networks
                    WHERE network_id = %s AND entity_type = 'business'
                      AND entity_name IS NOT NULL AND TRIM(entity_name) != ''
                    ORDER BY entity_name
                    LIMIT 10
                    """,
                    (row["network_id"],),
                )
                row["violation_businesses"] = [r["entity_name"] for r in cursor.fetchall()]
                result.append(row)

            return result
    except Exception as e:
        logger.exception("Failed to fetch Hartford playground data")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/monitor/city-stats")
def get_monitor_city_stats(
    city: str = "HARTFORD",
    date_from: Optional[str] = None,
    conn=Depends(get_db_connection),
):
    selected_city = (city or "HARTFORD").strip().upper()

    # Build date filter clause
    date_clause = ""
    date_params = []
    if date_from:
        date_clause = " AND filing_date >= %s"
        date_params.append(date_from)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if selected_city in NON_CT_MONITOR_CITIES:
                db_prefix = NON_CT_MONITOR_CITIES[selected_city]["db_prefix"]
                cursor.execute(f"""
                    SELECT
                        COALESCE(SUM(violations_total), 0)::int AS total_violations,
                        COALESCE(SUM(violations_open), 0)::int AS open_violations,
                        COALESCE(SUM(evictions_total), 0)::int AS total_evictions
                    FROM {db_prefix}_bbl_stats
                """)
                row = cursor.fetchone()
                return {
                    "total_evictions": row["total_evictions"],
                    "total_violations": row["total_violations"],
                    "open_violations": row["open_violations"],
                    "code_data_available": True,
                    "eviction_data_available": bool(row["total_evictions"] > 0),
                }
            else:
                is_statewide = selected_city == "STATEWIDE"
                if is_statewide:
                    eviction_query = f"SELECT COUNT(*)::int FROM evictions WHERE filing_date IS NOT NULL{date_clause}"
                    eviction_params = date_params
                else:
                    eviction_query = f"SELECT COUNT(*)::int FROM evictions WHERE UPPER(municipality) = %s AND filing_date IS NOT NULL{date_clause}"
                    eviction_params = [selected_city] + date_params

                cursor.execute(eviction_query, eviction_params)
                total_evictions = cursor.fetchone()["count"]

                total_violations = 0
                open_violations = 0
                is_hartford = selected_city == "HARTFORD"
                if is_hartford:
                    cursor.execute("""
                        SELECT
                            COUNT(*)::int AS total_violations,
                            COUNT(*) FILTER (
                                WHERE NOT (
                                    date_closed IS NOT NULL
                                    OR lower(COALESCE(record_status, '')) IN (
                                        'closed', 'resolved', 'complied', 'complete', 'completed'
                                    )
                                )
                            )::int AS open_violations
                        FROM code_enforcement
                    """)
                    v_row = cursor.fetchone()
                    total_violations = v_row["total_violations"]
                    open_violations = v_row["open_violations"]

                return {
                    "total_evictions": total_evictions,
                    "total_violations": total_violations,
                    "open_violations": open_violations,
                    "code_data_available": is_hartford,
                    "eviction_data_available": True,
                }
    except Exception as e:
        logger.exception("Failed to fetch city stats")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/monitor/cities", response_model=List[str])
def get_monitor_cities(conn=Depends(get_db_connection)):
    """Top municipalities by property count for the city monitor selector."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT UPPER(TRIM(property_city)) AS city
                FROM properties
                WHERE property_city IS NOT NULL AND TRIM(property_city) <> ''
                GROUP BY UPPER(TRIM(property_city))
                ORDER BY COUNT(*) DESC, UPPER(TRIM(property_city))
                LIMIT 200
                """
            )
            ct_cities = [r[0] for r in cursor.fetchall() if r and r[0]]
            ct_cities = [c for c in ct_cities if c != "HARTFORD"]
            non_ct_keys = list(NON_CT_MONITOR_CITIES.keys())
            return ["HARTFORD"] + non_ct_keys + ct_cities
    except Exception as e:
        logger.exception("Failed to fetch monitor cities")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/burst-detector", response_model=List[BurstDetectorItem])
def get_burst_detector(
    dimension: str = "network",
    city: Optional[str] = None,
    time_window: int = 365,
    min_filings: int = 5,
    disposition_filter: Optional[str] = None,
    conn=Depends(get_db_connection),
):
    """
    Configurable eviction surge detector — multi-jurisdiction.
    Detects concentrated filing surges grouped by a chosen dimension.
    Queries across ALL jurisdictions with eviction event data:
      - CT: evictions table (filing-level court records from CT Fair Housing Center)
      - Baltimore: city_eviction_events (Maryland District Court records via MD Open Data)
    Dimensions requiring plaintiff/attorney data (landlord, network, attorney)
    currently only return CT results since Baltimore events lack those fields.
    """
    valid_dimensions = {"city", "street", "landlord", "network", "attorney"}
    if dimension not in valid_dimensions:
        raise HTTPException(status_code=400, detail=f"Invalid dimension. Must be one of: {', '.join(valid_dimensions)}")

    time_window = max(30, min(3650, time_window))
    min_filings = max(2, min_filings)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Check which attorney fields exist in the CT evictions table
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'evictions'
                  AND column_name IN ('plaintiff_attorney_firm','plaintiff_attorney_name','plaintiff_attorney_norm')
            """)
            eviction_columns = {row["column_name"] for row in cursor.fetchall()}
            has_attorney = bool(eviction_columns)
            ct_attorney_select = (
                """COALESCE(NULLIF(TRIM(plaintiff_attorney_firm),''),NULLIF(TRIM(plaintiff_attorney_name),''),NULLIF(TRIM(plaintiff_attorney_norm),''))"""
                if has_attorney else "NULL::text"
            )

            # ---------------------------------------------------------------
            # Unified eviction events CTE — merges all jurisdictions
            # Normalizes columns so downstream queries are source-agnostic.
            # ---------------------------------------------------------------
            unified_cte = f"""
                unified_evictions AS (
                    SELECT
                        filing_date AS event_date,
                        UPPER(COALESCE(NULLIF(TRIM(municipality),''), 'UNKNOWN')) AS municipality,
                        COALESCE(NULLIF(TRIM(plaintiff_norm),''), NULLIF(TRIM(plaintiff_name),'')) AS plaintiff,
                        COALESCE(NULLIF(TRIM(plaintiff_name),''), NULLIF(TRIM(plaintiff_norm),'')) AS plaintiff_label,
                        {ct_attorney_select} AS attorney,
                        COALESCE(NULLIF(TRIM(normalized_address),''), NULLIF(TRIM(address),'')) AS address,
                        status,
                        'CT' AS source_state
                    FROM evictions
                    WHERE filing_date IS NOT NULL
                      AND filing_date >= CURRENT_DATE - INTERVAL '{time_window} days'

                    UNION ALL

                    SELECT
                        event_date AS event_date,
                        'BALTIMORE' AS municipality,
                        NULL AS plaintiff,
                        NULL AS plaintiff_label,
                        NULL AS attorney,
                        NULL AS address,
                        event_type AS status,
                        'MD' AS source_state
                    FROM city_eviction_events
                    WHERE event_date IS NOT NULL
                      AND event_date >= CURRENT_DATE - INTERVAL '{time_window} days'
                      AND city = 'baltimore'
                )
            """

            # Disposition filter — applied on unified status field
            disposition_clause = ""
            if disposition_filter == "default_judgment":
                disposition_clause = "AND (lower(COALESCE(e.status,'')) LIKE '%%default%%judgment%%' OR lower(COALESCE(e.status,'')) LIKE '%%after default%%')"
            elif disposition_filter == "withdrawal":
                disposition_clause = "AND lower(COALESCE(e.status,'')) LIKE '%%withdraw%%'"

            # City filter
            city_params: list = []
            if city:
                city_params = [city.strip().upper()]

            if dimension == "city":
                # City dimension — uses unified_evictions (all jurisdictions)
                city_filter_sql = ""
                if city_params:
                    city_filter_sql = "AND e.municipality = %s"

                query = f"""
                    WITH {unified_cte},
                    base AS (
                        SELECT
                            e.event_date,
                            e.status,
                            e.municipality AS dim_key,
                            e.municipality AS dim_label
                        FROM unified_evictions e
                        WHERE e.municipality != 'UNKNOWN'
                          {disposition_clause}
                          {city_filter_sql}
                    ),
                    weekly AS (
                        SELECT dim_key, dim_label,
                            DATE_TRUNC('week', event_date)::date AS wk,
                            COUNT(*)::int AS cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) LIKE '%%after default%%')::int AS dj_cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) LIKE '%%withdraw%%')::int AS wd_cnt
                        FROM base
                        GROUP BY dim_key, dim_label, DATE_TRUNC('week', event_date)::date
                    ),
                    agg AS (
                        SELECT dim_key, dim_label,
                            MAX(cnt)::int AS peak_filings,
                            (ARRAY_AGG(wk ORDER BY cnt DESC, wk DESC))[1] AS peak_week,
                            AVG(cnt)::float AS baseline_avg,
                            SUM(cnt)::int AS total_filings,
                            SUM(dj_cnt)::int AS dj_total,
                            SUM(wd_cnt)::int AS wd_total
                        FROM weekly
                        GROUP BY dim_key, dim_label
                        HAVING MAX(cnt) >= %s
                    )
                    SELECT dim_key, dim_label,
                        NULL::int AS network_id, NULL::text AS entity_id, NULL::text AS entity_type,
                        peak_week, peak_filings AS filings_count,
                        ROUND(baseline_avg::numeric,2)::float AS baseline_avg,
                        CASE WHEN baseline_avg > 0 THEN ROUND((peak_filings::numeric/baseline_avg::numeric),2)::float ELSE 0 END AS multiplier,
                        total_filings, dj_total AS total_default_judgments, wd_total AS total_withdrawals
                    FROM agg
                    ORDER BY multiplier DESC, peak_filings DESC
                    LIMIT 100
                """
                params = city_params + [min_filings]

            elif dimension == "street":
                # Street dimension — uses unified_evictions for cross-jurisdiction street-level surges
                city_filter_sql = "AND e.municipality = %s" if city_params else ""
                query = f"""
                    WITH {unified_cte},
                    base AS (
                        SELECT
                            e.event_date,
                            e.status,
                            UPPER(TRIM(COALESCE(e.address, 'UNKNOWN'))) AS street_raw,
                            e.municipality AS city_raw
                        FROM unified_evictions e
                        WHERE COALESCE(e.address, '') != ''
                          {disposition_clause}
                          {city_filter_sql}
                    ),
                    base2 AS (
                        SELECT event_date, status,
                            street_raw || ':' || city_raw AS dim_key,
                            street_raw || ', ' || city_raw AS dim_label
                        FROM base WHERE street_raw != 'UNKNOWN'
                    ),
                    weekly AS (
                        SELECT dim_key, dim_label,
                            DATE_TRUNC('week', event_date)::date AS wk,
                            COUNT(*)::int AS cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) LIKE '%%after default%%')::int AS dj_cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) LIKE '%%withdraw%%')::int AS wd_cnt
                        FROM base2
                        GROUP BY dim_key, dim_label, DATE_TRUNC('week', event_date)::date
                    ),
                    agg AS (
                        SELECT dim_key, dim_label,
                            MAX(cnt)::int AS peak_filings,
                            (ARRAY_AGG(wk ORDER BY cnt DESC, wk DESC))[1] AS peak_week,
                            AVG(cnt)::float AS baseline_avg,
                            SUM(cnt)::int AS total_filings,
                            SUM(dj_cnt)::int AS dj_total,
                            SUM(wd_cnt)::int AS wd_total
                        FROM weekly
                        GROUP BY dim_key, dim_label
                        HAVING MAX(cnt) >= %s
                    )
                    SELECT dim_key, dim_label,
                        NULL::int AS network_id, NULL::text AS entity_id, NULL::text AS entity_type,
                        peak_week, peak_filings AS filings_count,
                        ROUND(baseline_avg::numeric,2)::float AS baseline_avg,
                        CASE WHEN baseline_avg > 0 THEN ROUND((peak_filings::numeric/baseline_avg::numeric),2)::float ELSE 0 END AS multiplier,
                        total_filings, dj_total AS total_default_judgments, wd_total AS total_withdrawals
                    FROM agg
                    ORDER BY multiplier DESC, peak_filings DESC LIMIT 100
                """
                params = city_params + [min_filings]

            elif dimension == "landlord":
                # Landlord dimension — uses unified_evictions; only CT rows have plaintiff data
                city_filter_sql = "AND e.municipality = %s" if city_params else ""
                query = f"""
                    WITH {unified_cte},
                    base AS (
                        SELECT
                            e.event_date, e.status,
                            e.plaintiff AS dim_key,
                            e.plaintiff_label AS dim_label
                        FROM unified_evictions e
                        WHERE e.plaintiff IS NOT NULL AND e.plaintiff != ''
                          {disposition_clause}
                          {city_filter_sql}
                    ),
                    weekly AS (
                        SELECT dim_key, dim_label,
                            DATE_TRUNC('week', event_date)::date AS wk,
                            COUNT(*)::int AS cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) LIKE '%%after default%%')::int AS dj_cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) LIKE '%%withdraw%%')::int AS wd_cnt
                        FROM base
                        GROUP BY dim_key, dim_label, DATE_TRUNC('week', event_date)::date
                    ),
                    agg AS (
                        SELECT dim_key, dim_label,
                            MAX(cnt)::int AS peak_filings,
                            (ARRAY_AGG(wk ORDER BY cnt DESC, wk DESC))[1] AS peak_week,
                            AVG(cnt)::float AS baseline_avg,
                            SUM(cnt)::int AS total_filings,
                            SUM(dj_cnt)::int AS dj_total,
                            SUM(wd_cnt)::int AS wd_total
                        FROM weekly
                        GROUP BY dim_key, dim_label
                        HAVING MAX(cnt) >= %s
                    )
                    SELECT dim_key, dim_label,
                        NULL::int AS network_id, dim_label AS entity_id, 'owner'::text AS entity_type,
                        peak_week, peak_filings AS filings_count,
                        ROUND(baseline_avg::numeric,2)::float AS baseline_avg,
                        CASE WHEN baseline_avg > 0 THEN ROUND((peak_filings::numeric/baseline_avg::numeric),2)::float ELSE 0 END AS multiplier,
                        total_filings, dj_total AS total_default_judgments, wd_total AS total_withdrawals
                    FROM agg
                    ORDER BY multiplier DESC, peak_filings DESC LIMIT 100
                """
                params = city_params + [min_filings]

            elif dimension == "network":
                # Join plaintiff_norm → entity_networks by name to group by network_id
                city_filter_sql = "AND UPPER(TRIM(e.municipality)) = %s" if city_params else ""
                query = f"""
                    WITH base AS (
                        SELECT
                            e.filing_date, e.status,
                            en.network_id
                        FROM evictions e
                        JOIN entity_networks en
                          ON UPPER(TRIM(e.plaintiff_norm)) = UPPER(TRIM(en.entity_name))
                         AND en.entity_type IN ('business','principal','owner')
                        WHERE e.filing_date IS NOT NULL
                          AND e.filing_date >= CURRENT_DATE - INTERVAL '{time_window} days'
                          AND e.plaintiff_norm IS NOT NULL AND TRIM(e.plaintiff_norm) != ''
                          {disposition_clause}
                          {city_filter_sql}
                    ),
                    network_labels AS (
                        SELECT DISTINCT ON (network_id) network_id,
                            COALESCE(
                                MIN(entity_name) FILTER (WHERE entity_type = 'principal'),
                                MIN(entity_name)
                            ) AS label
                        FROM entity_networks
                        GROUP BY network_id
                    ),
                    weekly AS (
                        SELECT b.network_id, nl.label AS dim_label,
                            b.network_id::text AS dim_key,
                            DATE_TRUNC('week', b.filing_date)::date AS wk,
                            COUNT(*)::int AS cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(b.status,'')) LIKE '%%after default%%')::int AS dj_cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(b.status,'')) LIKE '%%withdraw%%')::int AS wd_cnt
                        FROM base b
                        JOIN network_labels nl ON nl.network_id = b.network_id
                        GROUP BY b.network_id, nl.label, DATE_TRUNC('week', b.filing_date)::date
                    ),
                    agg AS (
                        SELECT dim_key, dim_label, network_id,
                            MAX(cnt)::int AS peak_filings,
                            (ARRAY_AGG(wk ORDER BY cnt DESC, wk DESC))[1] AS peak_week,
                            AVG(cnt)::float AS baseline_avg,
                            SUM(cnt)::int AS total_filings,
                            SUM(dj_cnt)::int AS dj_total,
                            SUM(wd_cnt)::int AS wd_total
                        FROM weekly
                        GROUP BY dim_key, dim_label, network_id
                        HAVING MAX(cnt) >= %s
                    )
                    SELECT dim_key, dim_label,
                        network_id::int AS network_id, dim_key AS entity_id, 'network'::text AS entity_type,
                        peak_week, peak_filings AS filings_count,
                        ROUND(baseline_avg::numeric,2)::float AS baseline_avg,
                        CASE WHEN baseline_avg > 0 THEN ROUND((peak_filings::numeric/baseline_avg::numeric),2)::float ELSE 0 END AS multiplier,
                        total_filings, dj_total AS total_default_judgments, wd_total AS total_withdrawals
                    FROM agg
                    ORDER BY multiplier DESC, peak_filings DESC LIMIT 100
                """
                params = city_params + [min_filings]

            elif dimension == "attorney":
                # Attorney dimension — uses unified_evictions; only CT rows have attorney data
                if not has_attorney:
                    return []
                city_filter_sql = "AND e.municipality = %s" if city_params else ""
                query = f"""
                    WITH {unified_cte},
                    base AS (
                        SELECT
                            e.event_date, e.status,
                            e.attorney AS dim_key,
                            e.attorney AS dim_label
                        FROM unified_evictions e
                        WHERE e.attorney IS NOT NULL
                          AND TRIM(e.attorney) NOT IN ('', '\\N', 'n/a', 'N/A')
                          {disposition_clause}
                          {city_filter_sql}
                    ),
                    weekly AS (
                        SELECT dim_key, dim_label,
                            DATE_TRUNC('week', event_date)::date AS wk,
                            COUNT(*)::int AS cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) LIKE '%%after default%%')::int AS dj_cnt,
                            COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) LIKE '%%withdraw%%')::int AS wd_cnt
                        FROM base
                        GROUP BY dim_key, dim_label, DATE_TRUNC('week', event_date)::date
                    ),
                    agg AS (
                        SELECT dim_key, dim_label,
                            MAX(cnt)::int AS peak_filings,
                            (ARRAY_AGG(wk ORDER BY cnt DESC, wk DESC))[1] AS peak_week,
                            AVG(cnt)::float AS baseline_avg,
                            SUM(cnt)::int AS total_filings,
                            SUM(dj_cnt)::int AS dj_total,
                            SUM(wd_cnt)::int AS wd_total
                        FROM weekly
                        GROUP BY dim_key, dim_label
                        HAVING MAX(cnt) >= %s
                    )
                    SELECT dim_key, dim_label,
                        NULL::int AS network_id, NULL::text AS entity_id, NULL::text AS entity_type,
                        peak_week, peak_filings AS filings_count,
                        ROUND(baseline_avg::numeric,2)::float AS baseline_avg,
                        CASE WHEN baseline_avg > 0 THEN ROUND((peak_filings::numeric/baseline_avg::numeric),2)::float ELSE 0 END AS multiplier,
                        total_filings, dj_total AS total_default_judgments, wd_total AS total_withdrawals
                    FROM agg
                    ORDER BY multiplier DESC, peak_filings DESC LIMIT 100
                """
                params = city_params + [min_filings]
            else:
                raise HTTPException(status_code=400, detail=f"Invalid dimension: {dimension}")

            cursor.execute(query, params)
            rows = cursor.fetchall()

            result = []
            for row in rows:
                disp = []
                result.append(BurstDetectorItem(
                    dimension_key=row["dim_key"],
                    dimension_label=row["dim_label"] or row["dim_key"],
                    dimension_type=dimension,
                    peak_week=row.get("peak_week"),
                    filings_count=row.get("filings_count", 0),
                    baseline_avg=row.get("baseline_avg", 0),
                    multiplier=row.get("multiplier", 0),
                    total_filings=row.get("total_filings", 0),
                    disposition_breakdown=disp or None,
                    network_id=row.get("network_id"),
                    entity_id=row.get("entity_id"),
                    entity_type=row.get("entity_type"),
                ))
            return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Surge detector query failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/evictions", response_model=List[EvictionItem])
def get_evictions(property_id: Optional[int] = None, entity_name: Optional[str] = None, conn=Depends(get_db_connection)):
    """
    Fetch non-identifying eviction timeline records for a property or entity.
    Tenant-identifying data (names, case numbers, addresses) is intentionally excluded.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if property_id:
                cursor.execute(
                    "SELECT filing_date, status FROM evictions WHERE property_id = %s ORDER BY filing_date DESC",
                    (property_id,)
                )
            elif entity_name:
                norm_candidates = set()
                norm_candidates.add(normalize_business_name(entity_name))
                norm_candidates.add(normalize_person_name(entity_name))
                norm_candidates.add(canonicalize_person_name(entity_name))
                norm_candidates.update(get_name_variations(entity_name, 'business'))
                norm_candidates.update(get_name_variations(entity_name, 'principal'))
                norm_candidate_blacklist = {
                    "LLC", "INC", "INCORPORATED", "CORP", "CORPORATION", "COMPANY",
                    "PROPERTIES", "REALTY", "TRUST", "HOLDINGS", "MANAGEMENT"
                }
                norm_candidates = {
                    c.strip()
                    for c in norm_candidates
                    if c and len(c.strip()) >= 5 and c.strip() not in norm_candidate_blacklist
                }

                if not norm_candidates:
                    return []

                cursor.execute(
                    """
                    SELECT filing_date, status
                    FROM evictions
                    WHERE plaintiff_norm = ANY(%s)
                    ORDER BY filing_date DESC
                    """,
                    (list(norm_candidates),)
                )
            else:
                return []
            return cursor.fetchall()
    except Exception as e:
        logger.exception("Failed to fetch evictions")
        raise HTTPException(status_code=500, detail=str(e))

def _augment_ai_context_with_property_research(cursor, ctx: Dict[str, Any], property_ids: List[int]) -> None:
    ctx.setdefault("property_type_counts", [])
    ctx.setdefault("mailing_state_counts", [])
    ctx.setdefault("source_counts", [])
    ctx.setdefault("largest_properties", [])
    ctx.setdefault("recent_sales", [])
    ctx.setdefault("property_matched_eviction_count", 0)
    ctx.setdefault("property_matched_active_evictions", 0)
    ctx.setdefault("recent_property_evictions", [])
    ctx.setdefault("eviction_municipality_counts", [])
    ctx.setdefault("plaintiff_attorney_counts", [])
    ctx.setdefault("code_enforcement_count", 0)
    ctx.setdefault("open_code_enforcement_count", 0)
    ctx.setdefault("code_status_counts", [])
    ctx.setdefault("code_municipality_counts", [])
    ctx.setdefault("recent_code_cases", [])
    ctx.setdefault("source_status", [])

    if not property_ids:
        return

    cursor.execute("""
        SELECT COALESCE(NULLIF(property_type, ''), 'Unavailable') AS property_type,
               COUNT(*)::int AS cnt,
               COALESCE(SUM(number_of_units), 0)::float AS units
        FROM properties
        WHERE id = ANY(%s)
        GROUP BY COALESCE(NULLIF(property_type, ''), 'Unavailable')
        ORDER BY cnt DESC
        LIMIT 10
    """, (property_ids,))
    ctx["property_type_counts"] = cursor.fetchall()

    cursor.execute("""
        SELECT COALESCE(NULLIF(mailing_state, ''), 'Unavailable') AS mailing_state,
               COUNT(*)::int AS cnt
        FROM properties
        WHERE id = ANY(%s)
        GROUP BY COALESCE(NULLIF(mailing_state, ''), 'Unavailable')
        ORDER BY cnt DESC
        LIMIT 10
    """, (property_ids,))
    ctx["mailing_state_counts"] = cursor.fetchall()

    cursor.execute("""
        SELECT COALESCE(NULLIF(source, ''), 'CT municipal/CAMA') AS source,
               COUNT(*)::int AS cnt
        FROM properties
        WHERE id = ANY(%s)
        GROUP BY COALESCE(NULLIF(source, ''), 'CT municipal/CAMA')
        ORDER BY cnt DESC
        LIMIT 10
    """, (property_ids,))
    ctx["source_counts"] = cursor.fetchall()

    cursor.execute("""
        SELECT id, location, property_city, property_type,
               number_of_units::float AS number_of_units,
               year_built,
               assessed_value::float AS assessed_value,
               appraised_value::float AS appraised_value,
               sale_amount::float AS sale_amount,
               sale_date::text AS sale_date,
               source,
               link
        FROM properties
        WHERE id = ANY(%s)
        ORDER BY COALESCE(appraised_value, assessed_value, 0) DESC NULLS LAST
        LIMIT 8
    """, (property_ids,))
    ctx["largest_properties"] = cursor.fetchall()

    cursor.execute("""
        SELECT id, location, property_city,
               sale_amount::float AS sale_amount,
               sale_date::text AS sale_date,
               assessed_value::float AS assessed_value,
               appraised_value::float AS appraised_value,
               source,
               link
        FROM properties
        WHERE id = ANY(%s) AND sale_date IS NOT NULL
        ORDER BY sale_date DESC
        LIMIT 8
    """, (property_ids,))
    ctx["recent_sales"] = cursor.fetchall()

    cursor.execute("""
        SELECT COUNT(*)::int AS cnt,
               COUNT(*) FILTER (
                   WHERE lower(COALESCE(status, '')) NOT LIKE '%%withdraw%%'
                     AND lower(COALESCE(status, '')) NOT LIKE '%%closed%%'
                     AND lower(COALESCE(status, '')) NOT LIKE '%%disposed%%'
               )::int AS active_cnt
        FROM evictions
        WHERE property_id = ANY(%s)
    """, (property_ids,))
    ev_prop_row = cursor.fetchone() or {}
    ctx["property_matched_eviction_count"] = int(ev_prop_row.get("cnt") or 0)
    ctx["property_matched_active_evictions"] = int(ev_prop_row.get("active_cnt") or 0)

    cursor.execute("""
        SELECT municipality, COUNT(*)::int AS cnt
        FROM evictions
        WHERE property_id = ANY(%s)
        GROUP BY municipality
        ORDER BY cnt DESC
        LIMIT 10
    """, (property_ids,))
    ctx["eviction_municipality_counts"] = cursor.fetchall()

    cursor.execute("""
        SELECT COALESCE(NULLIF(plaintiff_attorney_firm, ''), NULLIF(plaintiff_attorney_name, ''), 'Unavailable') AS attorney,
               COUNT(*)::int AS cnt
        FROM evictions
        WHERE property_id = ANY(%s)
        GROUP BY COALESCE(NULLIF(plaintiff_attorney_firm, ''), NULLIF(plaintiff_attorney_name, ''), 'Unavailable')
        ORDER BY cnt DESC
        LIMIT 8
    """, (property_ids,))
    ctx["plaintiff_attorney_counts"] = cursor.fetchall()

    case_detail_select = "case_detail_url" if _column_exists(cursor, "evictions", "case_detail_url") else "NULL::text AS case_detail_url"
    document_select = "document_url" if _column_exists(cursor, "evictions", "document_url") else "NULL::text AS document_url"
    case_type_select = "case_type" if _column_exists(cursor, "evictions", "case_type") else "NULL::text AS case_type"

    cursor.execute(f"""
        SELECT case_number, {case_detail_select}, {document_select}, {case_type_select},
               municipality, filing_date::text AS filing_date,
               disposition_date::text AS disposition_date,
               status, address, plaintiff_name,
               plaintiff_attorney_name, plaintiff_attorney_firm
        FROM evictions
        WHERE property_id = ANY(%s)
        ORDER BY filing_date DESC NULLS LAST
        LIMIT 12
    """, (property_ids,))
    ctx["recent_property_evictions"] = cursor.fetchall()

    cursor.execute("""
        SELECT COUNT(*)::int AS cnt,
               COUNT(*) FILTER (
                   WHERE record_status IS NULL OR lower(record_status) NOT LIKE 'closed%%'
               )::int AS open_cnt
        FROM code_enforcement
        WHERE property_id = ANY(%s)
    """, (property_ids,))
    code_row = cursor.fetchone() or {}
    ctx["code_enforcement_count"] = int(code_row.get("cnt") or 0)
    ctx["open_code_enforcement_count"] = int(code_row.get("open_cnt") or 0)

    cursor.execute("""
        SELECT COALESCE(NULLIF(record_status, ''), 'Unavailable') AS record_status,
               COUNT(*)::int AS cnt
        FROM code_enforcement
        WHERE property_id = ANY(%s)
        GROUP BY COALESCE(NULLIF(record_status, ''), 'Unavailable')
        ORDER BY cnt DESC
        LIMIT 10
    """, (property_ids,))
    ctx["code_status_counts"] = cursor.fetchall()

    cursor.execute("""
        SELECT municipality, COUNT(*)::int AS cnt
        FROM code_enforcement
        WHERE property_id = ANY(%s)
        GROUP BY municipality
        ORDER BY cnt DESC
        LIMIT 10
    """, (property_ids,))
    ctx["code_municipality_counts"] = cursor.fetchall()

    cursor.execute("""
        SELECT case_number, municipality, address, record_name, record_type,
               record_status, date_opened::text AS date_opened,
               date_closed::text AS date_closed, inspector_name
        FROM code_enforcement
        WHERE property_id = ANY(%s)
        ORDER BY date_opened DESC NULLS LAST
        LIMIT 12
    """, (property_ids,))
    ctx["recent_code_cases"] = cursor.fetchall()

    source_names = {"CT_EVICTIONS", "HARTFORD_CODE_ENFORCEMENT"}
    for row in ctx.get("top_cities", []) or []:
        city = str(row.get("city") or "").strip().upper()
        if city:
            source_names.add(city)
            source_names.add(city.replace(" ", ""))
    cursor.execute("""
        SELECT source_name, source_type,
               external_last_updated::text AS external_last_updated,
               last_refreshed_at::text AS last_refreshed_at,
               refresh_status,
               details->>'source_url' AS source_url,
               details->>'message' AS message
        FROM data_source_status
        WHERE source_name = ANY(%s)
        ORDER BY source_type, source_name
        LIMIT 30
    """, (list(source_names),))
    ctx["source_status"] = cursor.fetchall()

def _compute_local_context(conn, entity: str, entity_type: str) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        "entity": entity,
        "entity_type": entity_type,
        "property_count": 0,
        "total_assessed_value": 0.0,
        "total_appraised_value": 0.0,
        "top_cities": [],
        "eviction_count": 0,
        "active_evictions": 0,
        "closed_evictions": 0,
        "network_id": None,
        "network_businesses": [],
        "network_principals": [],
        "is_network_level": False
    }

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        matched_property_ids: List[int] = []
        # Step 1: Find network_id robustly using name normalization variations
        network_id = None
        from api.shared_utils import normalize_person_name, normalize_business_name, canonicalize_person_name

        search_names = [entity, entity.strip().upper()]
        norm_p = normalize_person_name(entity)
        norm_b = normalize_business_name(entity)
        canon_p = canonicalize_person_name(entity)

        for name in [norm_p, norm_b, canon_p]:
            if name and name not in search_names:
                search_names.append(name)

        search_names = [n for n in search_names if n and len(n) > 1]

        if search_names:
            cursor.execute("""
                SELECT network_id FROM entity_networks
                WHERE entity_id = ANY(%s)
                   OR UPPER(entity_name) = ANY(%s)
                LIMIT 1
            """, (search_names, [n.upper() for n in search_names]))
            row = cursor.fetchone()
            if row:
                network_id = row["network_id"]

        if network_id is not None:
            ctx["network_id"] = network_id
            ctx["is_network_level"] = True

            # 1. Fetch properties in this network
            cursor.execute("""
                WITH matched AS (
                    SELECT DISTINCT p.id, p.assessed_value, p.appraised_value
                    FROM properties p
                    JOIN entity_networks en ON (
                        (en.entity_type = 'business' AND p.business_id = en.entity_id)
                        OR
                        (en.entity_type = 'business' AND UPPER(p.owner) = UPPER(en.entity_name))
                        OR
                        (en.entity_type = 'principal' AND p.principal_id = en.entity_id)
                        OR
                        (en.entity_type = 'principal' AND p.owner_norm = en.entity_id)
                        OR
                        (en.entity_type = 'principal' AND p.co_owner_norm = en.entity_id)
                    )
                    WHERE en.network_id = %s
                )
                SELECT COUNT(*) AS cnt,
                       COALESCE(SUM(assessed_value),0) AS total_assessed,
                       COALESCE(SUM(appraised_value),0) AS total_appraised,
                       COALESCE(ARRAY_AGG(id), ARRAY[]::integer[]) AS property_ids
                FROM matched
            """, (network_id,))
            prop_row = cursor.fetchone() or {}
            ctx["property_count"] = int(prop_row.get("cnt") or 0)
            ctx["total_assessed_value"] = float(prop_row.get("total_assessed") or 0.0)
            ctx["total_appraised_value"] = float(prop_row.get("total_appraised") or 0.0)
            matched_property_ids = [int(pid) for pid in (prop_row.get("property_ids") or []) if pid]

            # 2. Top Cities
            if matched_property_ids:
                cursor.execute("""
                    SELECT property_city AS city, COUNT(*)::int AS cnt
                    FROM properties
                    WHERE id = ANY(%s)
                    GROUP BY property_city
                    ORDER BY cnt DESC
                    LIMIT 10
                """, (matched_property_ids,))
                ctx["top_cities"] = cursor.fetchall()

            # 3. Businesses & Principals list
            cursor.execute("""
                SELECT DISTINCT entity_name FROM entity_networks
                WHERE network_id = %s AND entity_type = 'business'
            """, (network_id,))
            ctx["network_businesses"] = [r["entity_name"] for r in cursor.fetchall() if r.get("entity_name")]

            cursor.execute("""
                SELECT DISTINCT entity_name FROM entity_networks
                WHERE network_id = %s AND entity_type = 'principal'
            """, (network_id,))
            ctx["network_principals"] = [r["entity_name"] for r in cursor.fetchall() if r.get("entity_name")]

            # 4. Gather evictions for ALL associated names/entities in the network
            cursor.execute("""
                SELECT DISTINCT entity_name, entity_id FROM entity_networks
                WHERE network_id = %s
            """, (network_id,))
            network_members = cursor.fetchall()

            norm_candidates = set()
            for m in network_members:
                name = m.get("entity_name")
                eid = m.get("entity_id")
                if name:
                    norm_candidates.add(normalize_business_name(name))
                    norm_candidates.add(normalize_person_name(name))
                    norm_candidates.add(canonicalize_person_name(name))
                if eid:
                    norm_candidates.add(normalize_business_name(eid))
                    norm_candidates.add(normalize_person_name(eid))
                    norm_candidates.add(canonicalize_person_name(eid))

            norm_candidate_blacklist = {
                "LLC", "INC", "INCORPORATED", "CORP", "CORPORATION", "COMPANY",
                "PROPERTIES", "REALTY", "TRUST", "HOLDINGS", "MANAGEMENT"
            }
            norm_candidates = {
                c.strip()
                for c in norm_candidates
                if c and len(c.strip()) >= 5 and c.strip() not in norm_candidate_blacklist
            }

            if norm_candidates:
                cursor.execute("""
                    SELECT
                        COUNT(*)::int AS cnt,
                        COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) NOT LIKE '%%withdraw%%' AND lower(COALESCE(status,'')) NOT LIKE '%%closed%%' AND lower(COALESCE(status,'')) NOT LIKE '%%disposed%%')::int AS active_cnt
                    FROM evictions
                    WHERE plaintiff_norm = ANY(%s)
                """, (list(norm_candidates),))
                ev_row = cursor.fetchone() or {}
                ctx["eviction_count"] = int(ev_row.get("cnt") or 0)
                ctx["active_evictions"] = int(ev_row.get("active_cnt") or 0)
                ctx["closed_evictions"] = ctx["eviction_count"] - ctx["active_evictions"]

        else:
            # Fallback to single entity stats if not part of a network
            if entity_type in ("owner", "principal"):
                cursor.execute("""
                    SELECT COUNT(*) AS cnt,
                           COALESCE(SUM(assessed_value),0) AS total_value,
                           COALESCE(SUM(appraised_value),0) AS total_appraised,
                           COALESCE(ARRAY_AGG(id), ARRAY[]::integer[]) AS property_ids
                    FROM properties
                    WHERE owner_norm = normalize_person_name(%s)
                       OR co_owner_norm = normalize_person_name(%s)
                """, (entity, entity))
                row = cursor.fetchone() or {}
                ctx["property_count"] = int(row.get("cnt") or 0)
                ctx["total_assessed_value"] = float(row.get("total_value") or 0.0)
                ctx["total_appraised_value"] = float(row.get("total_appraised") or 0.0)
                matched_property_ids = [int(pid) for pid in (row.get("property_ids") or []) if pid]

                if matched_property_ids:
                    cursor.execute("""
                        SELECT property_city AS city, COUNT(*)::int AS cnt
                        FROM properties
                        WHERE id = ANY(%s)
                        GROUP BY property_city
                        ORDER BY cnt DESC
                        LIMIT 10
                    """, (matched_property_ids,))
                    ctx["top_cities"] = cursor.fetchall()

            elif entity_type == "business":
                cursor.execute("""
                    SELECT COUNT(*) AS cnt,
                           COALESCE(SUM(assessed_value),0) AS total_value,
                           COALESCE(SUM(appraised_value),0) AS total_appraised,
                           COALESCE(ARRAY_AGG(id), ARRAY[]::integer[]) AS property_ids
                    FROM properties
                    WHERE owner = %s
                       OR owner_norm = normalize_person_name(%s)
                """, (entity, entity))
                row = cursor.fetchone() or {}
                ctx["property_count"] = int(row.get("cnt") or 0)
                ctx["total_assessed_value"] = float(row.get("total_value") or 0.0)
                ctx["total_appraised_value"] = float(row.get("total_appraised") or 0.0)
                matched_property_ids = [int(pid) for pid in (row.get("property_ids") or []) if pid]

                if matched_property_ids:
                    cursor.execute("""
                        SELECT property_city AS city, COUNT(*)::int AS cnt
                        FROM properties
                        WHERE id = ANY(%s)
                        GROUP BY property_city
                        ORDER BY cnt DESC
                        LIMIT 10
                    """, (matched_property_ids,))
                    ctx["top_cities"] = cursor.fetchall()

            # Evictions Context
            norm_candidates = set()
            norm_candidates.add(normalize_business_name(entity))
            norm_candidates.add(normalize_person_name(entity))
            norm_candidates.add(canonicalize_person_name(entity))
            norm_candidates.update(get_name_variations(entity, 'business'))
            norm_candidates.update(get_name_variations(entity, 'principal'))
            norm_candidate_blacklist = {
                "LLC", "INC", "INCORPORATED", "CORP", "CORPORATION", "COMPANY",
                "PROPERTIES", "REALTY", "TRUST", "HOLDINGS", "MANAGEMENT"
            }
            norm_candidates = {
                c.strip()
                for c in norm_candidates
                if c and len(c.strip()) >= 5 and c.strip() not in norm_candidate_blacklist
            }

            if norm_candidates:
                cursor.execute("""
                    SELECT
                        COUNT(*)::int AS cnt,
                        COUNT(*) FILTER (WHERE lower(COALESCE(status,'')) NOT LIKE '%%withdraw%%' AND lower(COALESCE(status,'')) NOT LIKE '%%closed%%' AND lower(COALESCE(status,'')) NOT LIKE '%%disposed%%')::int AS active_cnt
                    FROM evictions
                    WHERE plaintiff_norm = ANY(%s)
                """, (list(norm_candidates),))
                ev_row = cursor.fetchone() or {}
                ctx["eviction_count"] = int(ev_row.get("cnt") or 0)
                ctx["active_evictions"] = int(ev_row.get("active_cnt") or 0)
                ctx["closed_evictions"] = ctx["eviction_count"] - ctx["active_evictions"]

        _augment_ai_context_with_property_research(cursor, ctx, matched_property_ids)

    return ctx

_REPORT_GENERIC_ENTITY_TERMS = {
    "LLC", "LTD", "INC", "CO", "CORP", "CORPORATION", "COMPANY", "COMPANIES",
    "PROPERTIES", "PROPERTY", "REALTY", "REAL", "ESTATE", "HOLDING", "HOLDINGS",
    "MANAGEMENT", "MANAGER", "GROUP", "TRUST", "THE", "AND"
}

def _report_entity_terms(entity: Optional[str]) -> List[str]:
    tokens = re.findall(r"[A-Za-z0-9]{2,}", entity or "")
    terms = []
    for token in tokens:
        upper = token.upper()
        if upper in _REPORT_GENERIC_ENTITY_TERMS:
            continue
        if len(token) >= 4 or token.isupper() or token.isdigit():
            terms.append(token.lower())
    return terms

def _report_normalized_phrase(value: Optional[str]) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", (value or "").lower()))

def _report_looks_like_business_name(name: Optional[str]) -> bool:
    upper = f" {name or ''} ".upper()
    business_tokens = [
        " LLC", " L.L.C", " INC", " CORP", " CO.", " COMPANY", " LTD", " LP",
        " LLP", " TRUST", " REALTY", " PROPERTIES", " HOLDINGS", " MANAGEMENT",
        " ASSOCIATES", " PARTNERS", " GROUP", " FUND", " MEMBER"
    ]
    return any(token in upper for token in business_tokens)

def _report_research_targets(context: Dict[str, Any], limit: int = 6) -> List[Dict[str, Any]]:
    entity = (context.get("entity") or "").strip()
    entity_type = context.get("entity_type")
    targets: List[Dict[str, Any]] = []
    seen = set()

    def add(name: str, target_type: str, require_exact_phrase: bool) -> None:
        clean = str(name or "").strip()
        key = _report_normalized_phrase(clean)
        if not clean or not key or key in seen:
            return
        terms = _report_entity_terms(clean)
        if len(terms) < 2 and target_type == "principal":
            return
        seen.add(key)
        targets.append({
            "name": clean,
            "type": target_type,
            "require_exact_phrase": require_exact_phrase
        })

    add(entity, entity_type or "entity", entity_type == "business")

    requested_principals = context.get("requested_research_entities", []) or []
    for principal in requested_principals:
        if len(targets) >= limit:
            break
        if _report_looks_like_business_name(principal):
            continue
        add(principal, "principal", False)

    if not requested_principals:
        for principal in context.get("network_principals", []) or []:
            if len(targets) >= limit:
                break
            if _report_looks_like_business_name(principal):
                continue
            add(principal, "principal", False)

    return targets

def _search_result_matches_entity(entity: Optional[str], title: str, snippet: str, link: str, require_exact_phrase: bool = False) -> bool:
    terms = _report_entity_terms(entity)
    if not terms:
        return False
    haystack = f"{title} {snippet} {link}".lower()
    if require_exact_phrase:
        phrase = _report_normalized_phrase(entity)
        normalized_haystack = _report_normalized_phrase(haystack)
        if phrase and phrase not in normalized_haystack:
            return False
    matches = sum(1 for term in terms if term in haystack)
    if len(terms) == 1:
        return matches == 1
    return matches >= min(2, len(terms))

_REPORT_BLOCKED_SOURCE_DOMAINS = {
    "truepeoplesearch.com", "fastbackgroundcheck.com", "fastpeoplesearch.com",
    "whitepages.com", "spokeo.com", "beenverified.com", "mylife.com",
    "radaris.com", "peoplefinders.com", "clustrmaps.com", "peekyou.com",
    "facebook.com", "instagram.com", "linkedin.com", "x.com", "twitter.com",
    "tiktok.com", "youtube.com", "researchgate.net", "academia.edu",
    "zillow.com", "realtor.com", "har.com", "biggerpockets.com",
    "zoominfo.com", "crunchbase.com", "tracxn.com", "homes.com", "reddit.com"
}

_REPORT_CT_MEDIA_DOMAINS = {
    "ctinsider.com", "nhregister.com", "newhavenindependent.org", "ctpost.com",
    "courant.com", "newstimes.com", "stamfordadvocate.com", "registercitizen.com",
    "wtnh.com", "nbcconnecticut.com", "ctmirror.org", "connecticut.news12.com",
    "hartfordbusiness.com", "newhavenbiz.com"
}

_REPORT_LEGAL_DOMAINS = {
    "courtlistener.com", "law.justia.com", "unicourt.com", "casetext.com",
    "law360.com", "docketalarm.com"
}

_REPORT_STRONG_RELEVANCE_TERMS = {
    "landlord", "tenant", "eviction", "housing", "apartment", "apartments",
    "property", "properties", "real estate", "foreclosure", "lawsuit", "court",
    "rental", "rent", "building", "buildings", "violation", "violations",
    "code", "lead-paint", "lead paint", "mandy", "management", "netz"
}

_REPORT_LOCATION_RELEVANCE_TERMS = {
    "new haven", "connecticut", "ct"
}

_REPORT_CASE_RELEVANCE_TERMS = {
    "landlord", "tenant", "eviction", "housing", "apartment", "apartments",
    "property", "properties", "real estate", "foreclosure", "rental", "rent",
    "building", "buildings", "violation", "violations", "lead-paint",
    "lead paint", "mandy", "management", "netz", "hpd", "preservation"
}

def _report_source_domain(link: str) -> str:
    try:
        host = urlparse(link).netloc.lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host

def _domain_matches(domain: str, candidates: Set[str]) -> bool:
    return any(domain == candidate or domain.endswith(f".{candidate}") for candidate in candidates)

def _is_allowed_report_source(source_type: str, link: str, title: str, snippet: str, source_name: str) -> bool:
    domain = _report_source_domain(link)
    if not domain:
        return False
    if _domain_matches(domain, _REPORT_BLOCKED_SOURCE_DOMAINS):
        return False
    haystack = f"{title} {snippet} {source_name} {domain}".lower()
    is_ct_media = _domain_matches(domain, _REPORT_CT_MEDIA_DOMAINS)
    is_legal = _domain_matches(domain, _REPORT_LEGAL_DOMAINS)
    is_official = domain.endswith(".gov") or ".gov." in domain
    is_public_doc = link.lower().endswith((".pdf", ".csv", ".xlsx", ".xls"))
    has_strong_relevance = any(term in haystack for term in _REPORT_STRONG_RELEVANCE_TERMS)
    has_location_relevance = any(term in haystack for term in _REPORT_LOCATION_RELEVANCE_TERMS)

    if source_type == "news":
        return (is_ct_media or has_location_relevance) and has_strong_relevance
    if source_type == "case_law":
        has_case_relevance = any(term in haystack for term in _REPORT_CASE_RELEVANCE_TERMS)
        return is_legal and has_case_relevance
    return is_official or is_legal or is_public_doc or ((is_ct_media or has_location_relevance) and has_strong_relevance)

def _format_money(value: Any) -> str:
    try:
        return f"${float(value or 0):,.0f}"
    except Exception:
        return "$0"

def _report_city_summary(rows: List[Dict[str, Any]], limit: int = 8) -> str:
    totals: Dict[str, int] = defaultdict(int)
    labels: Dict[str, str] = {}
    for row in rows or []:
        city = str(row.get("city") or "").strip()
        if not city:
            continue
        key = city.upper()
        totals[key] += int(row.get("cnt") or 0)
        labels.setdefault(key, city.title())
    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    return ", ".join(f"{labels[key]} ({cnt})" for key, cnt in ranked) or "Unavailable"

def _report_name_sample(values: List[str], limit: int = 10) -> str:
    names = [str(v).strip() for v in values or [] if str(v).strip()]
    names = sorted(dict.fromkeys(names))
    if not names:
        return "None explicitly linked"
    suffix = f"; plus {len(names) - limit} more" if len(names) > limit else ""
    return ", ".join(names[:limit]) + suffix

def _report_name_list_sample(values: List[str], limit: int = 40) -> List[str]:
    names = [str(v).strip() for v in values or [] if str(v).strip()]
    return sorted(dict.fromkeys(names))[:limit]

def _report_count_pairs(rows: List[Dict[str, Any]], label_key: str, limit: int = 8) -> str:
    parts = []
    for row in (rows or [])[:limit]:
        label = str(row.get(label_key) or "Unavailable").strip()
        cnt = int(row.get("cnt") or 0)
        parts.append(f"{label} ({cnt})")
    return ", ".join(parts) or "Unavailable"

def _markdown_escape_text(value: Any) -> str:
    text = str(value or "").strip()
    return text.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]").replace("|", "\\|")

def _markdown_table_cell(value: Any) -> str:
    return str(value or "").replace("\n", " ").replace("|", "\\|").strip() or "-"

def _markdown_link(label: Any, url: Any) -> str:
    text = _markdown_escape_text(label) or "source"
    href = str(url or "").strip()
    if not href.lower().startswith(("http://", "https://")):
        return text
    href = href.replace("<", "%3C").replace(">", "%3E").replace(" ", "%20")
    return f"[{text}](<{href}>)"

def _report_source_update_text(row: Dict[str, Any]) -> str:
    external = row.get("external_last_updated")
    refreshed = row.get("last_refreshed_at")
    bits = []
    if external:
        bits.append(f"source {external}")
    if refreshed:
        bits.append(f"refreshed {refreshed}")
    return "; ".join(bits) or "date unavailable"

def _report_source_label(row: Dict[str, Any]) -> str:
    return _markdown_link(row.get("source_name") or "source unavailable", row.get("source_url"))

def _report_source_status_packet(rows: List[Dict[str, Any]], limit: int = 30) -> List[Dict[str, Any]]:
    packet = []
    for row in (rows or [])[:limit]:
        packet.append({
            "source_name": row.get("source_name"),
            "source_type": row.get("source_type"),
            "refresh_status": row.get("refresh_status"),
            "external_last_updated": row.get("external_last_updated"),
            "last_refreshed_at": row.get("last_refreshed_at"),
            "source_url": row.get("source_url"),
            "message": row.get("message"),
        })
    return packet

def _report_key_metrics_table(context: Dict[str, Any]) -> str:
    rows = [
        ("Properties linked", f"{int(context.get('property_count') or 0):,}"),
        ("Assessed value", _format_money(context.get("total_assessed_value"))),
        ("Appraised value", _format_money(context.get("total_appraised_value"))),
        ("Top cities", _report_city_summary(context.get("top_cities", []))),
        ("Plaintiff-name eviction filings", f"{int(context.get('eviction_count') or 0):,}"),
        ("Parcel-matched eviction filings", f"{int(context.get('property_matched_eviction_count') or 0):,}"),
        ("Parcel-matched code records", f"{int(context.get('code_enforcement_count') or 0):,}"),
    ]
    lines = ["| Metric | Source-backed value |", "|---|---|"]
    for label, value in rows:
        lines.append(f"| {_markdown_table_cell(label)} | {_markdown_table_cell(value)} |")
    return "\n".join(lines)

def _report_property_table(rows: List[Dict[str, Any]], limit: int = 6) -> str:
    lines = [
        "| Parcel | City | Value | Units | Last recorded sale | Source |",
        "|---|---|---:|---:|---|---|",
    ]
    for row in (rows or [])[:limit]:
        address = str(row.get("location") or "Address unavailable").strip()
        parcel = _markdown_link(address, row.get("link"))
        city = row.get("property_city") or "Unavailable"
        value = _format_money(row.get("appraised_value") or row.get("assessed_value"))
        units = row.get("number_of_units")
        units_text = f"{units:g}" if isinstance(units, (int, float)) and units else "-"
        sale_text = "-"
        if row.get("sale_date"):
            sale_text = f"{row.get('sale_date')} for {_format_money(row.get('sale_amount'))}"
        source = row.get("source") or "Unavailable"
        lines.append(
            f"| {parcel} | {_markdown_table_cell(city)} | {_markdown_table_cell(value)} | "
            f"{_markdown_table_cell(units_text)} | {_markdown_table_cell(sale_text)} | {_markdown_table_cell(source)} |"
        )
    return "\n".join(lines) if len(lines) > 2 else "_No parcel-level examples available._"

def _report_eviction_table(rows: List[Dict[str, Any]], limit: int = 6) -> str:
    lines = [
        "| Filed | Municipality | Case | Status | Plaintiff | Plaintiff counsel |",
        "|---|---|---|---|---|---|",
    ]
    for row in (rows or [])[:limit]:
        attorney = row.get("plaintiff_attorney_firm") or row.get("plaintiff_attorney_name") or "-"
        case_label = row.get("case_number") or row.get("case_type") or "Court record"
        case_link = _markdown_link(case_label, row.get("case_detail_url") or row.get("document_url"))
        lines.append(
            f"| {_markdown_table_cell(row.get('filing_date') or 'date unavailable')} | "
            f"{_markdown_table_cell(row.get('municipality') or 'municipality unavailable')} | "
            f"{_markdown_table_cell(case_link)} | "
            f"{_markdown_table_cell(row.get('status') or 'status unavailable')} | "
            f"{_markdown_table_cell(row.get('plaintiff_name') or '-')} | "
            f"{_markdown_table_cell(attorney)} |"
        )
    return "\n".join(lines) if len(lines) > 2 else "_No parcel-matched eviction examples available._"

def _report_code_table(rows: List[Dict[str, Any]], limit: int = 6) -> str:
    lines = [
        "| Opened | Municipality | Address | Record | Status | Inspector |",
        "|---|---|---|---|---|---|",
    ]
    for row in (rows or [])[:limit]:
        label = row.get("record_name") or row.get("record_type") or "record unavailable"
        lines.append(
            f"| {_markdown_table_cell(row.get('date_opened') or 'date unavailable')} | "
            f"{_markdown_table_cell(row.get('municipality') or '-')} | "
            f"{_markdown_table_cell(row.get('address') or 'address unavailable')} | "
            f"{_markdown_table_cell(label)} | "
            f"{_markdown_table_cell(row.get('record_status') or 'status unavailable')} | "
            f"{_markdown_table_cell(row.get('inspector_name') or '-')} |"
        )
    return "\n".join(lines) if len(lines) > 2 else "_No parcel-matched code-enforcement examples available._"

def _report_source_status_table(rows: List[Dict[str, Any]], limit: int = 10) -> str:
    lines = [
        "| Dataset | Type | Status | Latest source/update | Notes |",
        "|---|---|---|---|---|",
    ]
    for row in (rows or [])[:limit]:
        lines.append(
            f"| {_report_source_label(row)} | "
            f"{_markdown_table_cell(row.get('source_type') or 'type unavailable')} | "
            f"{_markdown_table_cell(row.get('refresh_status') or 'status unavailable')} | "
            f"{_markdown_table_cell(_report_source_update_text(row))} | "
            f"{_markdown_table_cell(row.get('message') or '-')} |"
        )
    return "\n".join(lines) if len(lines) > 2 else "_Source coverage metadata unavailable for this entity._"

def _report_inline_external_source_refs(rows: List[Dict[str, Any]], limit: int = 4) -> str:
    refs = []
    for row in (rows or [])[:limit]:
        title = str(row.get("title") or "Untitled source").strip()
        matched = str(row.get("matched_entity") or "target/network entity").strip()
        source_type = str(row.get("type") or "web").replace("_", " ")
        source_name = str(row.get("source") or "").strip()
        descriptor = ", ".join([bit for bit in [matched, source_type, source_name] if bit])
        refs.append(f"{_markdown_link(title, row.get('url'))} ({descriptor})")
    return "; ".join(refs)

def _report_property_bullets(rows: List[Dict[str, Any]], limit: int = 5) -> str:
    bullets = []
    for row in (rows or [])[:limit]:
        address = ", ".join([str(row.get("location") or "").strip(), str(row.get("property_city") or "").strip()]).strip(", ")
        value = row.get("appraised_value") or row.get("assessed_value")
        units = row.get("number_of_units")
        units_text = f", {units:g} units" if isinstance(units, (int, float)) and units else ""
        sale_text = f", last sale {row.get('sale_date')} for {_format_money(row.get('sale_amount'))}" if row.get("sale_date") else ""
        bullets.append(f"- {address or 'Address unavailable'}: {_format_money(value)}{units_text}{sale_text}")
    return "\n".join(bullets) or "- No parcel-level examples available."

def _report_eviction_bullets(rows: List[Dict[str, Any]], limit: int = 5) -> str:
    bullets = []
    for row in (rows or [])[:limit]:
        filed = row.get("filing_date") or "date unavailable"
        status = row.get("status") or "status unavailable"
        muni = row.get("municipality") or "municipality unavailable"
        attorney = row.get("plaintiff_attorney_firm") or row.get("plaintiff_attorney_name")
        attorney_text = f"; plaintiff attorney: {attorney}" if attorney else ""
        bullets.append(f"- {filed}, {muni}: {status}{attorney_text}")
    return "\n".join(bullets) or "- No parcel-matched eviction examples available."

def _report_code_bullets(rows: List[Dict[str, Any]], limit: int = 5) -> str:
    bullets = []
    for row in (rows or [])[:limit]:
        opened = row.get("date_opened") or "date unavailable"
        status = row.get("record_status") or "status unavailable"
        label = row.get("record_name") or row.get("record_type") or "record unavailable"
        address = row.get("address") or "address unavailable"
        bullets.append(f"- {opened}, {address}: {label} ({status})")
    return "\n".join(bullets) or "- No parcel-matched code-enforcement examples available."

def _report_source_status_bullets(rows: List[Dict[str, Any]], limit: int = 6) -> str:
    bullets = []
    for row in (rows or [])[:limit]:
        updated = _report_source_update_text(row)
        status = row.get("refresh_status") or "status unavailable"
        source_type = row.get("source_type") or "type unavailable"
        bullets.append(f"- {_report_source_label(row)} ({source_type}): {status}, {updated}")
    return "\n".join(bullets) or "- Source coverage metadata unavailable for this entity."

def _report_external_source_bullets(rows: List[Dict[str, Any]], limit: int = 8) -> str:
    bullets = []
    for row in (rows or [])[:limit]:
        title = str(row.get("title") or "Untitled source").strip()
        url = str(row.get("url") or "").strip()
        matched = str(row.get("matched_entity") or "target/network entity").strip()
        source_type = str(row.get("type") or "web").replace("_", " ")
        source_name = str(row.get("source") or "").strip()
        date_text = str(row.get("date") or "").strip()
        snippet = str(row.get("snippet") or "").strip()
        meta = ", ".join([bit for bit in [matched, source_type, source_name, date_text] if bit])
        link = _markdown_link(title, url)
        snippet_text = f" — {snippet[:260]}" if snippet else ""
        bullets.append(f"- {link} ({meta}){snippet_text}")
    return "\n".join(bullets)

def _source_backed_ai_report_text(context: Dict[str, Any], note: Optional[str] = None) -> str:
    entity = context.get("entity") or "Unknown entity"
    entity_type = context.get("entity_type") or "entity"
    property_count = int(context.get("property_count") or 0)
    eviction_count = int(context.get("eviction_count") or 0)
    active_evictions = int(context.get("active_evictions") or 0)
    closed_evictions = int(context.get("closed_evictions") or 0)
    property_eviction_count = int(context.get("property_matched_eviction_count") or 0)
    property_active_evictions = int(context.get("property_matched_active_evictions") or 0)
    code_count = int(context.get("code_enforcement_count") or 0)
    open_code_count = int(context.get("open_code_enforcement_count") or 0)
    network_label = "network-level" if context.get("is_network_level") else "single-entity"
    note_block = f"\n\n> **Report generation note:** {note}" if note else ""
    external_sources_text = _report_external_source_bullets(context.get("web_sources", []))
    inline_external_refs = _report_inline_external_source_refs(context.get("web_sources", []))
    external_summary = (
        f" Exact-match external research surfaced {inline_external_refs}."
        if inline_external_refs
        else (
            " Exact-match external web, news, and case-law research did not confirm a vetted outside source in this run, "
            "so the report stays anchored to the loaded local records."
        )
    )
    external_section = (
        "The report generator found the following vetted exact-match external sources for the target entity or linked human principals in this ownership network:\n"
        f"{external_sources_text}"
        if external_sources_text
        else (
            "No vetted external web, news, or case-law source was confirmed for this exact entity or its selected linked human principals during report generation. "
            "No public-reputation claims, controversies, legal allegations, or media findings are included unless they are present in the local source tables above."
        )
    )

    return (
        "### Executive Summary\n\n"
        f"**Subject:** {entity} ({entity_type})  \n"
        f"**Review type:** {network_label} ownership review  \n"
        "**Evidence base:** source-loaded property, eviction, code-enforcement, and source-status records; external claims are limited to cited exact-match links.\n\n"
        f"{entity} ({entity_type}) is linked in the local source database to "
        f"{property_count:,} properties in a {network_label} ownership review. "
        f"The linked portfolio has {_format_money(context.get('total_assessed_value'))} in assessed value "
        f"and {_format_money(context.get('total_appraised_value'))} in appraised value. "
        f"The eviction table currently shows {eviction_count:,} filings tied to matched plaintiff names "
        f"({active_evictions:,} active, {closed_evictions:,} closed), plus {property_eviction_count:,} filings "
        f"matched directly to parcels in the portfolio. Code-enforcement records matched to this portfolio total "
        f"{code_count:,}, with {open_code_count:,} not marked closed.{external_summary}{note_block}\n\n"
        "#### Key Metrics\n\n"
        f"{_report_key_metrics_table(context)}\n\n"
        "### 1. Portfolio Breakdown & Corporate Architecture\n"
        "\n| Portfolio signal | Evidence |\n"
        "|---|---|\n"
        f"| Property-type mix | {_markdown_table_cell(_report_count_pairs(context.get('property_type_counts', []), 'property_type'))} |\n"
        f"| Mailing-state footprint | {_markdown_table_cell(_report_count_pairs(context.get('mailing_state_counts', []), 'mailing_state'))} |\n"
        f"| Loaded property sources | {_markdown_table_cell(_report_count_pairs(context.get('source_counts', []), 'source'))} |\n"
        f"| Linked business names | {_markdown_table_cell(_report_name_sample(context.get('network_businesses', [])))} |\n"
        f"| Linked principals | {_markdown_table_cell(_report_name_sample(context.get('network_principals', [])))} |\n\n"
        "#### Largest Parcel Examples\n\n"
        f"{_report_property_table(context.get('largest_properties', []))}\n\n"
        "### 2. Eviction Analysis & Filing Patterns\n"
        "\n| Eviction signal | Evidence |\n"
        "|---|---|\n"
        f"| Plaintiff-name matched filings | {eviction_count:,} total; {active_evictions:,} active; {closed_evictions:,} closed/inactive |\n"
        f"| Parcel-matched filings | {property_eviction_count:,} total; {property_active_evictions:,} active |\n"
        f"| Parcel-matched municipalities | {_markdown_table_cell(_report_count_pairs(context.get('eviction_municipality_counts', []), 'municipality'))} |\n"
        f"| Plaintiff counsel in parcel matches | {_markdown_table_cell(_report_count_pairs(context.get('plaintiff_attorney_counts', []), 'attorney'))} |\n\n"
        "- These counts come from matched plaintiff names and parcel-linked eviction records in the loaded eviction source tables. "
        "The report does not infer case outcomes beyond the status values present in those records.\n\n"
        "#### Recent Parcel-Matched Eviction Records\n\n"
        f"{_report_eviction_table(context.get('recent_property_evictions', []))}\n\n"
        "### 3. Code Enforcement & Habitability Signals\n"
        "\n| Code-enforcement signal | Evidence |\n"
        "|---|---|\n"
        f"| Parcel-matched records | {code_count:,} total |\n"
        f"| Not marked closed | {open_code_count:,} |\n"
        f"| Status mix | {_markdown_table_cell(_report_count_pairs(context.get('code_status_counts', []), 'record_status'))} |\n"
        f"| Municipal coverage in matched cases | {_markdown_table_cell(_report_count_pairs(context.get('code_municipality_counts', []), 'municipality'))} |\n\n"
        "#### Recent Parcel-Matched Code-Enforcement Records\n\n"
        f"{_report_code_table(context.get('recent_code_cases', []))}\n\n"
        "### 4. Public Reputation, News, and External Observations\n"
        f"{external_section}\n\n"
        "### 5. Source Coverage Notes\n"
        f"{_report_source_status_table(context.get('source_status', []))}\n\n"
        "### 6. Investigative Leads & Areas for Further Scrutiny\n"
        "- Verify individual parcels against the linked municipal property records in the largest-parcel table and the relevant source-status rows above.\n"
        "- Cross-check eviction filings by plaintiff name in the official court source for the relevant jurisdiction.\n"
        "- Review municipal code-enforcement or rental-license records where those source datasets are available.\n"
        "- Treat missing source rows as coverage gaps, not evidence that a record category does not exist."
    )

def _is_user_edited_report(row: Dict[str, Any]) -> bool:
    sources = row.get("sources") or {}
    return isinstance(sources, dict) and sources.get("edited") is True

def _ai_report_quality_problem(content: Optional[str], entity: Optional[str]) -> Optional[str]:
    text = (content or "").strip()
    if len(text) < 250:
        return "report was too short to be a complete published report"
    if re.search(r"\[[^\]]+\]\([^)]*$", text):
        return "report ended inside an unfinished markdown link"
    if text.endswith(("_", "/", "(", "[", ",")):
        return "report ended mid-token"
    terms = _report_entity_terms(entity)
    if terms:
        lowered = text.lower()
        matches = sum(1 for term in terms if term in lowered)
        if len(terms) == 1 and matches == 0:
            return "report did not mention the requested entity"
        if len(terms) > 1 and matches < min(2, len(terms)):
            return "report did not mention the requested entity clearly"
    if "DEBUG_ERROR_DETAILS" in text:
        return "report contains debug error output"
    return None

def _cached_ai_report_is_usable(row: Dict[str, Any], entity: Optional[str]) -> bool:
    if _is_user_edited_report(row):
        return True
    return _ai_report_quality_problem(row.get("content"), entity) is None

def _search_network_entities(context: Dict[str, Any]) -> Tuple[str, List[Dict[str, str]]]:
    if not SERPAPI_API_KEY:
        return "", []

    entity = (context.get('entity') or "").strip()
    if not entity:
        return "", []
    targets = _report_research_targets(context)

    snippets_by_type: Dict[str, List[str]] = defaultdict(list)
    labels_by_type = {
        "web": "Web and public-record search",
        "news": "News and CT media search",
        "case_law": "Case-law and court-record search",
    }
    all_sources = []
    seen_links = set()

    for target in targets:
        target_name = target["name"]
        search_specs = [
            {
                "type": "web",
                "query": f"\"{target_name}\" landlord property owner real estate",
                "result_key": "organic_results",
            },
            {
                "type": "news",
                "query": f"\"{target_name}\" Connecticut landlord eviction property real estate",
                "result_key": "news_results",
                "tbm": "nws",
            },
            {
                "type": "news",
                "query": f"\"{target_name}\" (\"New Haven Register\" OR CTInsider OR courant OR \"New Haven Independent\" OR \"Connecticut Post\")",
                "result_key": "organic_results",
            },
            {
                "type": "case_law",
                "query": f"\"{target_name}\" court lawsuit eviction foreclosure housing",
                "result_key": "organic_results",
            },
            {
                "type": "case_law",
                "query": f"\"{target_name}\" site:courtlistener.com OR site:law.justia.com OR site:unicourt.com",
                "result_key": "organic_results",
            },
        ]

        for spec in search_specs:
            q = spec["query"]
            try:
                url = "https://serpapi.com/search"
                params = {
                   "q": q,
                   "api_key": SERPAPI_API_KEY,
                   "hl": "en",
                   "gl": "us",
                   "num": 3
                }
                if spec.get("tbm"):
                    params["tbm"] = spec["tbm"]
                resp = requests.get(url, params=params, timeout=8)
                if resp.ok:
                    data = resp.json()
                    results = data.get(spec["result_key"], []) or data.get("organic_results", [])
                    for res in results:
                        title = res.get("title", "")
                        snip = res.get("snippet", "")
                        link = res.get("link", "")
                        source_name = res.get("source", "")
                        result_date = res.get("date", "")
                        if link and link not in seen_links:
                            if not _search_result_matches_entity(
                                target_name,
                                title,
                                snip,
                                link,
                                require_exact_phrase=target.get("require_exact_phrase", False)
                            ):
                                continue
                            if not _is_allowed_report_source(spec["type"], link, title, snip, source_name):
                                continue
                            seen_links.add(link)
                            meta_bits = [bit for bit in [target_name, source_name, result_date] if bit]
                            meta = f" ({', '.join(meta_bits)})" if meta_bits else ""
                            if title or snip:
                                snippets_by_type[spec["type"]].append(f"- [{title}]({link}){meta}: {snip}")
                            all_sources.append({
                                "type": spec["type"],
                                "matched_entity": target_name,
                                "matched_entity_type": target.get("type"),
                                "title": title,
                                "url": link,
                                "snippet": snip,
                                "source": source_name,
                                "date": result_date,
                                "query": q
                            })
            except Exception as e:
                logger.error(f"Search failed for query '{q}': {e}")

    sections = []
    for source_type in ["web", "news", "case_law"]:
        snippets = snippets_by_type.get(source_type) or []
        if snippets:
            sections.append(f"#### {labels_by_type[source_type]}\n" + "\n".join(snippets[:5]))
    if sections:
        return "\n\n".join(sections), all_sources
    return "", []

def _draft_ai_report_text(context: Dict[str, Any], length: Optional[str] = 'comprehensive', directive: Optional[str] = '') -> Tuple[str, str]:
    title = f"AI report — {context.get('entity')}"
    entity = context.get('entity')
    entity_type = context.get('entity_type')

    # Run public web search
    search_data, sources = _search_network_entities(context)

    # Store only exact-entity matched sources in context so they get persisted.
    context["web_sources"] = sources

    body_fallback = _source_backed_ai_report_text(context)

    if not (genai and GEMINI_KEY):
        return (title, body_fallback)

    length_instruction = ""
    max_tokens = 1000
    if length == "concise":
        length_instruction = "Write a concise briefing (roughly 600-900 words). Focus on the strongest source-backed findings, major risks, and best next investigative leads."
        max_tokens = 1400
    else:
        length_instruction = "Write a comprehensive, highly detailed investigative dossier (roughly 1,500-2,200 words). Structure it like a publishable research memo with clear findings, evidence, limits, and next leads."
        max_tokens = 3500

    directive_instruction = ""
    if directive:
        directive_instruction = f"\nCRITICAL CUSTOM INVESTIGATION DIRECTIVE:\nThe user has requested that you specifically focus on/address the following: \"{directive}\". Tailor the observations and snapshots to highlight this directive.\n"

    cities_str = ', '.join([f"{r.get('city')} ({r.get('cnt')} properties)" for r in context.get('top_cities', [])])
    network_businesses = context.get("network_businesses", []) or []
    network_principals = context.get("network_principals", []) or []
    investigative_packet = json.dumps({
        "portfolio": {
            "property_count": context.get("property_count", 0),
            "total_assessed_value": context.get("total_assessed_value", 0.0),
            "total_appraised_value": context.get("total_appraised_value", 0.0),
            "top_cities": context.get("top_cities", []),
            "property_type_counts": context.get("property_type_counts", []),
            "mailing_state_counts": context.get("mailing_state_counts", []),
            "source_counts": context.get("source_counts", []),
            "largest_properties": context.get("largest_properties", []),
            "recent_sales": context.get("recent_sales", []),
        },
        "ownership_network": {
            "network_id": context.get("network_id"),
            "is_network_level": context.get("is_network_level", False),
            "linked_business_count": len(network_businesses),
            "linked_business_sample": _report_name_list_sample(network_businesses),
            "linked_principal_count": len(network_principals),
            "linked_principal_sample": _report_name_list_sample(network_principals),
        },
        "evictions": {
            "plaintiff_name_matched_count": context.get("eviction_count", 0),
            "plaintiff_name_matched_active": context.get("active_evictions", 0),
            "plaintiff_name_matched_closed": context.get("closed_evictions", 0),
            "parcel_matched_count": context.get("property_matched_eviction_count", 0),
            "parcel_matched_active": context.get("property_matched_active_evictions", 0),
            "municipality_counts": context.get("eviction_municipality_counts", []),
            "plaintiff_attorney_counts": context.get("plaintiff_attorney_counts", []),
            "recent_parcel_matched_records": context.get("recent_property_evictions", []),
        },
        "code_enforcement": {
            "parcel_matched_count": context.get("code_enforcement_count", 0),
            "not_marked_closed_count": context.get("open_code_enforcement_count", 0),
            "status_counts": context.get("code_status_counts", []),
            "municipality_counts": context.get("code_municipality_counts", []),
            "recent_records": context.get("recent_code_cases", []),
        },
        "source_coverage": _report_source_status_packet(context.get("source_status", [])),
    }, default=str, indent=2)

    prompt = (
        "You are a senior investigative journalist, housing-policy analyst, and legal researcher specializing in corporate landlord networks, beneficial ownership, evictions, habitability enforcement, and tenant-impact research.\n"
        "Your task is to draft a rigorous investigative deep dive using theyownwhat's source-loaded local data as the primary evidence base, then layering in vetted exact-entity web, news, and case-law/court-record research when available.\n\n"
        "CRITICAL INSTRUCTIONS:\n"
        f"0. The first sentence of the report must name the target entity exactly as: {entity}.\n"
        "1. Source hierarchy: Treat theyownwhat local records as source-loaded property/eviction/code-enforcement facts. Treat external search clippings as usable only when they are directly about the target entity named below or a linked human principal explicitly labeled in the clipping metadata.\n"
        "2. No fabrication: Do not invent facts, dates, case outcomes, accusations, locations, ownership links, code violations, legal claims, or reputational conclusions. If evidence is unavailable, say so.\n"
        "3. Legal/news discipline: Discuss case-law, lawsuits, court records, or news only if they appear in the vetted external clippings. Attribute whether the source matched the header entity or a linked principal. If no case-law or news results are provided, explicitly state that no exact-match result was confirmed in this run.\n"
        "4. Eviction discipline: Clearly distinguish plaintiff-name matched eviction filings from parcel-matched eviction filings; do not treat either as a judgment or tenant outcome unless the status field says so.\n"
        "5. Code-enforcement discipline: Treat code records as municipal records with status labels. Do not infer habitability conditions beyond the record names/statuses supplied.\n"
        "6. Analysis quality: Go beyond restating counts. Identify concentration, portfolio composition, geographic spread, mailing-state/offsite signals, largest assets, attorney patterns, enforcement concentrations, source gaps, and leads a reporter or legal-aid team could verify next.\n"
        "7. Inline links: Put citations inline in the sentence where the claim appears, using markdown links around source titles, dataset names, or short claim phrases. Do not dump links only in a bibliography. Use only URLs present in the vetted external clippings or source_coverage packet.\n"
        "8. Local source links: Local data can be described as source-loaded theyownwhat data, but when a source_coverage row includes source_url, link the dataset name inline the first time you discuss that source or refresh date.\n"
        "9. Format: Use clean professional Markdown with section headers, compact tables for metrics/source coverage, short evidence bullets, and readable paragraphs. Avoid giant unbroken text blocks.\n\n"
        f"--- LOCAL PORTFOLIO DATABASE CONTEXT ---\n"
        f"Target Entity: {entity} ({entity_type})\n"
        f"Network Level Investigation: {context.get('is_network_level', False)}\n"
        f"Total Properties Owned: {context.get('property_count', 0)}\n"
        f"Total Portfolio Value: Assessed at ${context.get('total_assessed_value', 0.0):,.2f} / Appraised at ${context.get('total_appraised_value', 0.0):,.2f}\n"
        f"Top Cities: {cities_str}\n"
        f"Eviction Volume: {context.get('eviction_count', 0)} total filings ({context.get('active_evictions', 0)} active, {context.get('closed_evictions', 0)} closed)\n"
        f"Parcel-Matched Evictions: {context.get('property_matched_eviction_count', 0)} total ({context.get('property_matched_active_evictions', 0)} active)\n"
        f"Parcel-Matched Code Enforcement Records: {context.get('code_enforcement_count', 0)} total ({context.get('open_code_enforcement_count', 0)} not marked closed)\n"
        f"Network Business LLC Shells: {len(network_businesses)} linked; sample: {_report_name_sample(network_businesses, 20)}\n"
        f"Network Principals / Key Actors: {len(network_principals)} linked; sample: {_report_name_sample(network_principals, 20)}\n\n"
        f"--- THEYOWNWHAT INVESTIGATIVE DATA PACKET ---\n"
        f"{investigative_packet}\n\n"
        f"--- EXTERNAL NEWS & WEB SEARCH CLIPPINGS ---\n"
        f"{search_data or 'No vetted exact-entity public web, news, or case-law/court-record sources were found for this entity.'}\n\n"
        f"{length_instruction}\n"
        f"{directive_instruction}\n\n"
        "REQUIRED DOSSIER STRUCTURE:\n"
        "### Executive Summary\n"
        "A tight but substantive overview characterizing scale, geography, corporate/network structure, eviction footprint, code-enforcement footprint, and whether exact-match external research was found.\n\n"
        "### 1. Portfolio Breakdown & Corporate Architecture\n"
        "Analyze total properties, valuation, property types, top cities, largest parcel examples, recent sales, mailing-state footprint, linked LLCs/businesses, and linked principals. Be precise about what is observed versus what needs verification.\n\n"
        "### 2. Eviction Analysis & Filing Patterns\n"
        "Analyze plaintiff-name matched versus parcel-matched eviction records, active/closed status mix, municipalities, recent records, and plaintiff-attorney patterns. Do not overclaim case outcomes.\n\n"
        "### 3. Code Enforcement & Habitability Signals\n"
        "Analyze code-enforcement volume, open/not-closed count, status mix, municipal concentration, and recent records. Discuss whether coverage appears limited to certain municipalities if source coverage suggests that.\n\n"
        "### 4. External Web, News, and Case-Law Research\n"
        "Summarize exact-match external findings by category, including CT print/web media about linked principals when those principals are part of the ownership network. If web/news/case-law sources are absent or sparse, say so clearly and explain the implication: the local records carry the report, while external reputation/case-law claims remain unverified in this run.\n\n"
        "### 5. Source Coverage, Caveats, and Data Limits\n"
        "Name relevant source-status records, refresh dates when present, and important limitations such as parcel-level matching availability. Explain source gaps without treating gaps as evidence of absence.\n\n"
        "### 6. Investigative Leads & Verification Checklist\n"
        "Suggest 4-6 specific leads for reporters, tenant organizers, researchers, or legal-aid lawyers. Make them concrete: parcels, cities, court-source checks, code-enforcement logs, deeds, beneficial ownership, attorney patterns, or rental-license records."
    )

    try:
        model = genai.GenerativeModel('gemini-3.5-flash', system_instruction="You are a meticulous investigative analyst.")
        resp = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=0.3,
                max_output_tokens=max_tokens
            )
        )
        content = resp.text.strip()
        if length != "concise" and len(content) < 900:
            logger.warning(
                "Gemini report draft for %s was short (%s chars); requesting expanded dossier",
                entity,
                len(content)
            )
            retry_prompt = (
                prompt
                + "\n\nEXPANSION REQUIRED:\n"
                "The previous draft was too short for a comprehensive investigative deep dive. "
                "Write the full six-section dossier now. Use the exact target entity name in the first sentence, "
                "include portfolio/network analysis, evictions, code enforcement, external research, source caveats, "
                "and a concrete verification checklist. Use inline markdown links wherever a vetted URL is supplied. Stay source-disciplined and do not invent unsupported facts."
            )
            retry_resp = model.generate_content(
                retry_prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.25,
                    max_output_tokens=max_tokens
                )
            )
            retry_content = retry_resp.text.strip()
            if len(retry_content) > len(content):
                content = retry_content
        quality_problem = _ai_report_quality_problem(content, entity)
        if not quality_problem and length != "concise" and len(content) < 900:
            quality_problem = "report was too short for the requested investigative deep dive"
        if quality_problem:
            logger.warning(
                "Discarding low-quality AI report for %s: %s",
                entity,
                quality_problem
            )
            context["report_quality_fallback"] = quality_problem
            return (
                title,
                _source_backed_ai_report_text(
                    context,
                    f"AI synthesis was discarded because the generated draft {quality_problem}."
                )
            )
        return (title, content)
    except Exception as e:
        logger.exception("Failed to call Gemini inside _draft_ai_report_text")
        context["report_quality_fallback"] = "external AI synthesis unavailable"
        return (
            title,
            _source_backed_ai_report_text(
                context,
                "External AI synthesis was unavailable."
            )
        )

@app.post("/api/ai-report")
def create_ai_report(req: AIReportRequest, conn=Depends(get_db_connection)):
    today = date.today()
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        if not req.force:
            cursor.execute("""
                SELECT * FROM ai_reports
                WHERE entity = %s AND entity_type = %s AND report_date = %s
                LIMIT 1
            """, (req.entity, req.entity_type, today))
            existing = cursor.fetchone()
            if existing and _cached_ai_report_is_usable(existing, req.entity):
                return existing
            if existing:
                problem = _ai_report_quality_problem(existing.get("content"), req.entity)
                logger.warning(
                    "Regenerating cached AI report for %s because %s",
                    req.entity,
                    problem or "it failed quality checks"
                )
        context = _compute_local_context(conn, req.entity, req.entity_type)
        context["requested_research_entities"] = req.research_entities or []
        title, content = _draft_ai_report_text(context, length=req.length, directive=req.directive)
        cursor.execute("""
            INSERT INTO ai_reports (entity, entity_type, report_date, title, content, sources)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity, entity_type, report_date)
            DO UPDATE SET title=EXCLUDED.title, content=EXCLUDED.content, sources=EXCLUDED.sources
            RETURNING *
        """, (req.entity, req.entity_type, today, title, content, json.dumps({"internal_stats": context})))
        row = cursor.fetchone()
        conn.commit()
        return row

@app.get("/api/ai-report")
def get_ai_report(entity: str, entity_type: str, report_date: Optional[str] = None, conn=Depends(get_db_connection)):
    the_date = date.fromisoformat(report_date) if report_date else date.today()
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT * FROM ai_reports
            WHERE entity = %s AND entity_type = %s AND report_date = %s
            LIMIT 1
        """, (entity, entity_type, the_date))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No report found for the given date")
        if not _cached_ai_report_is_usable(row, entity):
            if the_date != date.today():
                raise HTTPException(status_code=409, detail="Cached report failed quality checks; regenerate today's report")
            problem = _ai_report_quality_problem(row.get("content"), entity)
            logger.warning(
                "Regenerating cached AI report for %s because %s",
                entity,
                problem or "it failed quality checks"
            )
            context = _compute_local_context(conn, entity, entity_type)
            title, content = _draft_ai_report_text(context)
            cursor.execute("""
                INSERT INTO ai_reports (entity, entity_type, report_date, title, content, sources)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (entity, entity_type, report_date)
                DO UPDATE SET title=EXCLUDED.title, content=EXCLUDED.content, sources=EXCLUDED.sources
                RETURNING *
            """, (entity, entity_type, the_date, title, content, json.dumps({"internal_stats": context})))
            row = cursor.fetchone()
            conn.commit()
        return row

class AIReportSaveRequest(BaseModel):
    entity: str
    entity_type: str
    content: str

@app.put("/api/ai-report")
def save_ai_report(req: AIReportSaveRequest, conn=Depends(get_db_connection)):
    today = date.today()
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            INSERT INTO ai_reports (entity, entity_type, report_date, title, content, sources)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity, entity_type, report_date)
            DO UPDATE SET content = EXCLUDED.content
            RETURNING *
        """, (req.entity, req.entity_type, today, f"AI report — {req.entity}", req.content, json.dumps({"edited": True})))
        row = cursor.fetchone()
        conn.commit()
        return row


# ------------------------------------------------------------
# Health
# ------------------------------------------------------------
@app.get("/api/healthz")
def healthz():
    return {"ok": True}

# ------------------------------------------------------------
# Scraper Status
# ------------------------------------------------------------
@app.get("/api/scraper/status")
def get_scraper_status(conn=Depends(get_db_connection)):
    """
    Returns the real-time status of all scrapers (active, recently completed, failed).
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT
                source_name,
                source_type,
                CASE
                    WHEN refresh_status = 'running' AND last_refreshed_at < NOW() - INTERVAL '6 hours' THEN 'failure'
                    ELSE refresh_status
                END as refresh_status,
                last_refreshed_at,
                details
            FROM data_source_status
            ORDER BY
                CASE WHEN refresh_status = 'running' THEN 0 ELSE 1 END,
                last_refreshed_at DESC
        """)
        rows = cursor.fetchall()

        active = [r for r in rows if r['refresh_status'] == 'running']
        recent = [r for r in rows if r['refresh_status'] != 'running'][:25]

        return {
            "status": "success",
            "active_scrapers": active,
            "recent_jobs": recent,
            "total_active": len(active)
        }

@app.get("/scraperstatus", response_class=HTMLResponse)
def scraper_status_page():
    """
    Standalone HTML page to monitor scraper status visually.
    """
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Scraper Status</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            .refresh-spin { animation: spin 1s linear infinite; }
            @keyframes spin { 100% { transform: rotate(360deg); } }
        </style>
    </head>
    <body class="bg-slate-50 text-slate-800 p-8 font-sans">
        <div class="max-w-6xl mx-auto flex flex-col gap-6">
            <div class="flex items-center justify-between border-b border-slate-200 pb-4">
                <div>
                    <h1 class="text-2xl font-black text-slate-900">System Scraper Status</h1>
                    <p class="text-sm text-slate-500">Real-time monitoring of data ingestion pipelines.</p>
                </div>
                <div class="flex gap-4 items-center">
                    <span id="active-count" class="px-3 py-1 bg-indigo-100 text-indigo-700 font-bold rounded-lg text-sm">0 Active</span>
                </div>
            </div>

            <div id="loading" class="text-center py-10 text-slate-400">Loading data...</div>

            <div id="tables-container" class="hidden flex-col gap-8">
                <div>
                    <h2 class="text-lg font-bold mb-3 flex items-center gap-2"><div class="w-2 h-2 rounded-full bg-blue-500 animate-pulse"></div> Active Scrapers</h2>
                    <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
                        <table class="w-full text-left text-sm">
                            <thead class="bg-slate-50 text-slate-500 text-xs uppercase border-b border-slate-200">
                                <tr>
                                    <th class="py-3 px-4">Municipality</th>
                                    <th class="py-3 px-4">Type</th>
                                    <th class="py-3 px-4">Status</th>
                                    <th class="py-3 px-4">Last Activity</th>
                                    <th class="py-3 px-4">Details</th>
                                </tr>
                            </thead>
                            <tbody id="active-tbody" class="divide-y divide-slate-100"></tbody>
                        </table>
                    </div>
                </div>

                <div>
                    <h2 class="text-lg font-bold mb-3 flex items-center gap-2">Recent Jobs</h2>
                    <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
                        <table class="w-full text-left text-sm">
                            <thead class="bg-slate-50 text-slate-500 text-xs uppercase border-b border-slate-200">
                                <tr>
                                    <th class="py-3 px-4">Municipality</th>
                                    <th class="py-3 px-4">Type</th>
                                    <th class="py-3 px-4">Status</th>
                                    <th class="py-3 px-4">Last Activity</th>
                                    <th class="py-3 px-4 text-right">Details</th>
                                </tr>
                            </thead>
                            <tbody id="recent-tbody" class="divide-y divide-slate-100"></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <script>
            function strDate(iso) {
                if (!iso) return '-';
                const d = new Date(iso);
                return d.toLocaleDateString() + ' ' + d.toLocaleTimeString();
            }

            function badge(status) {
                if (status === 'running') return `<span class="px-2 py-0.5 rounded text-xs font-bold bg-blue-50 text-blue-600 border border-blue-100 uppercase tracking-wider">Running</span>`;
                if (status === 'success') return `<span class="px-2 py-0.5 rounded text-xs font-bold bg-emerald-50 text-emerald-600 border border-emerald-100 uppercase tracking-wider">Success</span>`;
                return `<span class="px-2 py-0.5 rounded text-xs font-bold bg-rose-50 text-rose-600 border border-rose-100 uppercase tracking-wider">Failed</span>`;
            }

            function renderRow(job) {
                return `
                    <tr class="hover:bg-slate-50">
                        <td class="py-2 px-4 font-bold text-slate-800">${job.source_name}</td>
                        <td class="py-2 px-4 text-slate-500 text-xs font-mono">${job.source_type}</td>
                        <td class="py-2 px-4">${badge(job.refresh_status)}</td>
                        <td class="py-2 px-4 text-slate-500 text-xs font-mono">${strDate(job.last_refreshed_at)}</td>
                        <td class="py-2 px-4 text-slate-600 text-xs text-right truncate max-w-[200px]" title="${job.details?.message || ''}">${job.details?.message || job.details || '-'}</td>
                    </tr>
                `;
            }

            async function fetchData() {
                try {
                    const res = await fetch('/api/scraper/status');
                    const data = await res.json();

                    document.getElementById('loading').classList.add('hidden');
                    document.getElementById('tables-container').classList.remove('hidden');
                    document.getElementById('tables-container').classList.add('flex');

                    document.getElementById('active-count').innerText = data.total_active + ' Active';

                    const activeHtml = data.active_scrapers.length ? data.active_scrapers.map(renderRow).join('') : '<tr><td colspan="5" class="py-4 text-center text-slate-400 text-sm">No active scrapers</td></tr>';
                    document.getElementById('active-tbody').innerHTML = activeHtml;

                    const recentHtml = data.recent_jobs.length ? data.recent_jobs.map(renderRow).join('') : '<tr><td colspan="5" class="py-4 text-center text-slate-400 text-sm">No recent jobs</td></tr>';
                    document.getElementById('recent-tbody').innerHTML = recentHtml;
                } catch (e) {
                    console.error("Failed to load", e);
                }
            }

            fetchData();
            setInterval(fetchData, 5000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)




# ------------------------------------------------------------
# Network Digest (Batch Analysis)
# ------------------------------------------------------------

class DigestItem(BaseModel):
    name: str
    type: str
    property_count: Optional[int] = 0
    total_value: Optional[float] = 0.0

class NetworkDigestRequest(BaseModel):
    entities: List[DigestItem]
    force: bool = False

@app.post("/api/network_digest")
def create_network_digest(req: NetworkDigestRequest, conn=Depends(get_db_connection)):
    # 1. Generate Stable Hash (Cache Key)
    # Include stats in hash so if data changes (e.g. value updates), we regenerate
    sorted_ents = sorted(req.entities, key=lambda x: (x.type, x.name))
    # v2: Updated to invalidate old cached errors/v0.28 format
    CACHE_VERSION = "v3_20260117"
    blob = json.dumps([
        {"n": e.name, "t": e.type, "c": e.property_count, "v": e.total_value} for e in sorted_ents
    ] + [{"_v": CACHE_VERSION}], sort_keys=True)
    digest_hash = hashlib.md5(blob.encode("utf-8")).hexdigest()
    digest_id = f"DIGEST_{digest_hash}"

    today = date.today()

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        if not req.force:
            cursor.execute("""
                SELECT * FROM ai_reports
                WHERE entity = %s AND entity_type = 'network_digest' AND report_date = %s
                LIMIT 1
            """, (digest_id, today))
            existing = cursor.fetchone()
            if existing:
                return existing

        # 2. Perform Analysis (Parallel Web Search)
        combined_context = []

        def fetch_entity_context(ent: DigestItem):
            if not SERPAPI_API_KEY:
                return {"context": f"Entity: {ent.name} ({ent.type}) - SerpAPI not configured.", "sources": []}

            query = f"{ent.name} Connecticut real estate"
            if ent.type == 'business':
                query += " business LLC"
            else:
                query += " landlord property owner"

            try:
                url = "https://serpapi.com/search"
                params = {
                   "q": query,
                   "api_key": SERPAPI_API_KEY,
                   "hl": "en",
                   "gl": "us",
                   "num": 3
                }
                resp = requests.get(url, params=params, timeout=10)
                data = resp.json()

                snippets = []
                sources = []
                if "organic_results" in data:
                    for res in data["organic_results"]:
                         title = res.get("title", "")
                         snip = res.get("snippet", "")
                         link = res.get("link", "")
                         if title or snip:
                             snippets.append(f"- {title}: {snip} (Source: {link})")
                         if link:
                             sources.append({"title": title, "url": link})

                if snippets:
                    return {
                        "context": f"Entity: {ent.name} ({ent.type})\n" + "\n".join(snippets),
                        "sources": sources
                    }
                else:
                    return {"context": f"Entity: {ent.name} ({ent.type}) - No significant results.", "sources": []}
            except Exception as e:
                logger.error(f"Search failed for {ent.name}: {e}")
                return {"context": f"Entity: {ent.name} ({ent.type}) - Search Error.", "sources": []}

        # Execute searches in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            # We'll rely on Frontend to send a reasonable number (e.g. top 10).
            results = list(executor.map(fetch_entity_context, req.entities))
            combined_context = [r["context"] for r in results]
            all_sources = []
            seen_links = set()
            for r in results:
                for s in r["sources"]:
                    if s["url"] not in seen_links:
                        all_sources.append(s)
                        seen_links.add(s["url"])

        full_text_context = "\n\n".join(combined_context)

        # Calculate aggregate stats for the prompt
        total_props = sum(e.property_count for e in req.entities)
        total_val = sum(e.total_value for e in req.entities)

        # 3. Summarize with Gemini
        final_summary = "Analysis Unavailable."
        title = f"AI Digest - Network of {len(req.entities)} Entities"

        if genai and GEMINI_KEY:
            prompt = (
                f"You are an investigative analyst. You are analyzing a property network consisting of {len(req.entities)} related entities (principals and businesses). "
                f"Together, they own {total_props} properties with a total assessed value of ${total_val:,.0f}.\n\n"
                "Analyze the following web search excerpts for this group.\n\n"
                "STRUCTURE YOUR RESPONSE AS FOLLOWS:\n"
                "1. OVERALL SUMMARY: A concise 3-4 sentence high-level overview of the entire network's footprint, reputation, and scale.\n"
                "2. KEY RISKS & FINDINGS: Bullet points of major issues, complaints, eviction history, or legal patterns found in the news.\n"
                "3. ENTITY BREAKDOWN: Brief notes on individual principals or businesses where specific info was found.\n\n"
                "CITATIONS: When referencing specific details, include the source link inline formatted as (Source: <url>). Do NOT use markdown links.\n\n"
                "Focus on identifying acquisition patterns, property management reputation, significant legal filings, and any public controversies involving these entities.\n"
                "Be specific. Do not infer allegations, reputational claims, or external facts that are not present in the search excerpts or local database metrics. If no negative/notable info is found, say so and limit the summary to source-backed portfolio facts.\n\n"
                f"Web Search Data:\n{full_text_context}\n"
            )
            try:
                model = genai.GenerativeModel('gemini-3.5-flash', system_instruction="You are a meticulous investigative analyst.")
                resp = model.generate_content(
                    prompt,
                    generation_config=genai.types.GenerationConfig(
                        temperature=0.3,
                        max_output_tokens=1500
                    )
                )
                final_summary = resp.text.strip()
            except Exception as e:
                logger.error(f"Gemini Digest Error: {e}")
                final_summary = f"AI Synthesis Encountered an Error. Displaying raw search hits instead:\n\n{full_text_context}\n\nDEBUG_ERROR_DETAILS: {repr(e)}"
        else:
             final_summary = "Gemini API Key not configured. Displaying raw web search results:\n\n" + full_text_context

        # 4. Save to Cache
        cursor.execute("""
            INSERT INTO ai_reports (entity, entity_type, report_date, title, content, sources)
            VALUES (%s, 'network_digest', %s, %s, %s, %s)
            ON CONFLICT (entity, entity_type, report_date)
            DO UPDATE SET title=EXCLUDED.title, content=EXCLUDED.content, sources=EXCLUDED.sources
            RETURNING *
        """, (digest_id, today, title, final_summary, json.dumps(all_sources)))

        row = cursor.fetchone()
        conn.commit()
        return row

@app.patch("/api/properties/{property_id}/geocode")
def update_property_geocode(property_id: int, lat: float, lon: float, conn=Depends(get_db_connection)):
    """
    Update the latitude and longitude for a specific property.
    This allows the frontend to persist on-the-fly geocoding results.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, source FROM properties WHERE id = %s", (property_id,))
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Property not found")
            row_dict = {"id": row[0], "source": row[1]} if not isinstance(row, dict) else dict(row)
            if not valid_property_coordinates(lat, lon, row_dict):
                raise HTTPException(status_code=400, detail="Coordinates are outside the expected property bounds")
            cursor.execute(
                "UPDATE properties SET latitude = %s, longitude = %s WHERE id = %s",
                (lat, lon, property_id)
            )
            # If no row updated, we might want to know, but idempotency is fine.
            conn.commit()
            return {"status": "success", "id": property_id, "lat": lat, "lon": lon}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update geocode for property {property_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/api/freshness")
def get_data_freshness(conn=Depends(get_db_connection)):
    """
    Returns the last refresh status and external 'Last Updated' dates
    for all configured data sources.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details
                FROM data_source_status
                ORDER BY source_type, source_name
            """)
            sources = cursor.fetchall()

            # For backward compatibility with the frontend format, pull specific system DBs
            cursor.execute("SELECT last_updated FROM data_source_status WHERE source_name = 'PRINCIPALS DB'")
            p_row = cursor.fetchone()
            p_date = p_row['last_updated'] if p_row else None

            cursor.execute("SELECT last_updated FROM data_source_status WHERE source_name = 'BUSINESS DB'")
            b_row = cursor.fetchone()
            b_date = b_row['last_updated'] if b_row else None

            # Fallback for older code using networks build time
            cursor.execute("SELECT MAX(created_at) as val FROM networks")
            n_date = cursor.fetchone()['val']

            return {
                 "sources": sources,
                 "system_freshness": {
                     "principals_last_updated": p_date or n_date,
                     "businesses_last_updated": b_date or n_date,
                     "networks_last_built": n_date
                 }
            }
    except Exception as e:
        logger.error(f"Failed to fetch data freshness: {e}")
        raise HTTPException(status_code=500, detail=str(e))
