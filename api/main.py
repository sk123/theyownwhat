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
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_batch

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# Optional OpenAI import (AI report). App still runs without it.
try:
    import openai  # type: ignore
except Exception:  # pragma: no cover
    openai = None  # type: ignore

# ------------------------------------------------------------
# App / Config
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("they-own-what")

DATABASE_URL = os.environ.get("DATABASE_URL")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY")  # reserved for future use

if openai and OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

app = FastAPI(title="they own WHAT?? API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

@app.get("/api/health")
def health_check():
    # Check if OpenAI key is present and NOT the placeholder
    ai_key = os.environ.get("OPENAI_API_KEY", "")
    ai_enabled = bool(ai_key and "REPLACE_WITH_API_KEY" not in ai_key)
    return {"status": "ok", "timestamp": time.time(), "ai_enabled": ai_enabled}

db_pool: Optional[pool.SimpleConnectionPool] = None


# ------------------------------------------------------------
# Helpers (make available to all endpoints)
# ------------------------------------------------------------
_PERSON_SUFFIXES = {'JR', 'SR', 'III', 'IV', 'II', 'ESQ', 'MD', 'PHD', 'DDS'}
import decimal

def shape_property_row(p: dict) -> dict:
    """Normalize a property DB row into the shape the frontend expects."""
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
        "latitude": float(p['latitude']) if p.get("latitude") is not None else None,
        "longitude": float(p['longitude']) if p.get("longitude") is not None else None,
        "details": p,  # keep full row for drill-down
    }


def json_converter(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    if isinstance(o, decimal.Decimal):
        return float(o)
    return str(o)

def normalize_person_name_py(name: str) -> str:
    """Robust normalization for FIRST LAST vs LAST, FIRST, suffixes, punctuation."""
    if not name:
        return ''
    n = name.upper().strip()
    n = re.sub(r"[`\"'.]", "", n)         # remove quotes/periods
    n = re.sub(r"\s+", " ", n).strip()    # collapse whitespace
    parts = n.split()
    if parts and parts[-1] in _PERSON_SUFFIXES:
        parts = parts[:-1]
    n = " ".join(parts)
    if ',' in n:
        m = re.match(r"^\s*([^,]+)\s*,\s*([A-Z0-9\- ]+)", n)
        if m:
            n = f"{m.group(2).strip()} {m.group(1).strip()}"
    
    # Specific typo fixes
    n = n.replace("GUREVITOH", "GUREVITCH")
    n = n.replace("MANACHEM", "MENACHEM")
    n = n.replace("MENACHERM", "MENACHEM")
    n = n.replace("MENAHEM", "MENACHEM")
    n = n.replace("GURAVITCH", "GUREVITCH")
    
    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()
    
    # Middle Initial Stripping Strategy:
    # Only strip single-letter middle parts if the First and Last parts are robust (>1 char).
    # This protects short business names like "A B LLC".
    parts = n.split()
    if len(parts) >= 3:
        # Check if first and last name are likely real names (>1 char)
        if len(parts[0]) > 1 and len(parts[-1]) > 1:
            # Filter out single-letter middle tokens
            middle = parts[1:-1]
            # Keep middle tokens only if length > 1
            middle_robust = [p for p in middle if len(p) > 1]
            n = " ".join([parts[0]] + middle_robust + [parts[-1]])

    return n

def get_name_variations(name: str, entity_type: str) -> Set[str]:
    """Small set of useful variants (principal vs business)."""
    vars_: Set[str] = set()
    if not name:
        return vars_
    u = name.upper().strip()
    vars_.add(u)

    if entity_type == "principal":
        n = normalize_person_name_py(name)
        if n:
            vars_.add(n)
        tokens = n.split()
        if len(tokens) >= 2:
            vars_.add(f"{tokens[-1]} {tokens[0]}")  # LAST FIRST
    elif entity_type == "business":
        no_punct = re.sub(r"[^\w\s&]", " ", u)
        no_punct = re.sub(r"\s+", " ", no_punct).strip()
        vars_.add(no_punct)
        if '&' in u:
            vars_.add(u.replace('&', 'AND'))
        if ' AND ' in u:
            vars_.add(u.replace(' AND ', '&'))
        # strip common suffixes (iterate until none)
        suffixes = [
            'LIMITED LIABILITY COMPANY','LIMITED LIABILITY PARTNERSHIP',
            'PROFESSIONAL LIMITED LIABILITY COMPANY','LIMITED PARTNERSHIP',
            'INCORPORATED','CORPORATION','L L C','L L P','L P',
            'LLC','LLP','LTD','INC','CORP','LP','CO'
        ]
        n = u
        changed = True
        while changed:
            changed = False
            for s in suffixes:
                pat = re.compile(r"\s+" + re.escape(s) + r"$")
                if pat.search(n):
                    n = pat.sub('', n).strip()
                    changed = True
                    break
        if n:
            vars_.add(n)
    return {v for v in vars_ if v}

def find_properties_for_entity(cursor, entity_name: str, entity_type: str) -> List[Dict[str, Any]]:
    """Robust match on normalized owner/co-owner for principal or business name."""
    if not entity_name:
        return []
    et = "principal" if entity_type in ("owner", "principal") else "business"
    vars_ = get_name_variations(entity_name, et)
    norm_variants = list({normalize_person_name_py(v) for v in vars_ if v})
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
    global db_pool
    retries = 10
    while retries > 0:
        try:
            db_pool = pool.SimpleConnectionPool(1, 10, dsn=DATABASE_URL)
            break
        except psycopg2.OperationalError:
            retries -= 1
            logger.warning("DB not ready; retrying... (%s left)", retries)
            time.sleep(5)

    if db_pool is None:
        logger.error("Could not connect to DB after retries. Exiting.")
        sys.exit(1)

    conn = db_pool.getconn()
    try:
        with conn.cursor() as c:
            # c.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            # c.execute("DROP FUNCTION IF EXISTS normalize_person_name(TEXT)")
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
        
        logger.info("Triggering initial insights cache refresh in the background...")
        thread = threading.Thread(target=_update_insights_cache_sync, daemon=True)
        thread.start()

    finally:
        db_pool.putconn(conn)


def get_db_connection():
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database connection unavailable")
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        if conn:
            db_pool.putconn(conn)


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
    entity_id: str
    entity_name: str
    entity_type: str
    value: int
    total_assessed_value: Optional[float] = None
    total_appraised_value: Optional[float] = None
    business_name: Optional[str] = None
    business_count: Optional[int] = 0
    principals: Optional[List[PrincipalInfo]] = None
    businesses: Optional[List[BusinessInfo]] = None




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
                SELECT id, location, property_city, property_zip, latitude, longitude
                FROM properties
                WHERE id = ANY(%s::bigint[])
            """, (req.property_ids,))
            
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(f"Database error in batch_geocode_properties: {e}")
            raise HTTPException(status_code=500, detail="Database query failed during batch geocoding")
        
        for r in rows:
            # If already has coords, return them
            if r['latitude'] and r['longitude']:
                results.append(GeocodeResult(id=str(r['id']), lat=float(r['latitude']), lon=float(r['longitude'])))
            elif r['location']:
                # Needs geocoding
                to_process.append(r)

    # 2. Process in parallel
    if to_process:
        with ThreadPoolExecutor(max_workers=50) as executor: # Higher workers for IO bound
            future_to_id = {}
            for row in to_process:
                address_full = f"{row['location']}, {row['property_city'] or ''}, CT {row['property_zip'] or ''}".strip()
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

                    if lat and lon:
                        results.append(GeocodeResult(id=str(pid), lat=lat, lon=lon))
                        updates.append((lat, lon, pid))
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
            return {"summary": "No public news records found.", "sources": [], "risk": "Low"}

        # Summarize with OpenAI
        summary_text = "Found recent mentions."
        risk_level = "Unknown"
        
        if openai and OPENAI_API_KEY:
            try:
                system_prompt = (
                    "You are a real estate investigator. Analyze these search snippets about a landlord/entity. "
                    "Provide a 1-2 sentence summary of their reputation and mention any legal issues or controversies. "
                    "Classify risk as Low, Moderate, or High."
                )
                user_msg = f"Entity: {entity_name}\nSnippets:\n" + "\n".join(snippets)
                
                chat_completion = openai.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_msg}
                    ],
                    max_tokens=100
                )
                content = chat_completion.choices[0].message.content
                summary_text = content
                if "High" in content: risk_level = "High"
                elif "Moderate" in content: risk_level = "Moderate"
                else: risk_level = "Low"
            except Exception as e:
                logger.error(f"OpenAI error: {e}")
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
@app.get("/api/autocomplete")
def autocomplete(q: str, type: str, conn=Depends(get_db_connection)):
    """
    Fast prefix matching for search suggestions.
    type: 'business' | 'owner' | 'address'
    """
    if not q: return []
    q = q.strip()
    
    if len(q) < 2:
        return []

    limit = 50
    limit_extended = 20 # Fetch more to allow for deduping
    results = []
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            t_prefix = q.lower() + "%"
            t_infix = "%" + q + "%"

            if type == "business":
                cursor.execute(
                    "SELECT DISTINCT id, name FROM businesses WHERE lower(name) LIKE %s ORDER BY name ASC LIMIT %s",
                    (t_prefix, limit)
                )
                results = [{"label": r["name"], "value": r["name"], "id": str(r["id"]), "type": "Business", "context": "Business Entity"} for r in cursor.fetchall()]
                
                if len(results) < limit:
                    cursor.execute(
                        "SELECT id, name FROM businesses WHERE name ILIKE %s LIMIT %s",
                        (t_infix, limit - len(results))
                    )
                    existing = {x["value"] for x in results}
                    for r in cursor.fetchall():
                        if r["name"] not in existing:
                            results.append({"label": r["name"], "value": r["name"], "id": str(r["id"]), "type": "Business", "context": "Business Entity"})

            elif type == "owner":
                # 1. Search Principals
                cursor.execute(
                    """
                    SELECT 
                        mode() WITHIN GROUP (ORDER BY p.name_c) as name, 
                        string_agg(DISTINCT b.name, '||') as businesses 
                    FROM principals p 
                    LEFT JOIN businesses b ON p.business_id = b.id
                    WHERE p.name_c_norm IS NOT NULL AND lower(p.name_c_norm) LIKE %s 
                    GROUP BY p.name_c_norm 
                    ORDER BY lower(p.name_c_norm) ASC 
                    LIMIT %s
                    """,
                    (t_prefix, limit)
                )

                def fmt_princ(biz):
                    if not biz: return "Business Principal"
                    bs = [x for x in biz.split('||') if x]
                    if not bs: return "Business Principal"
                    return bs[0] + (f" + {len(bs)-1} more" if len(bs)>1 else "")

                results.extend([{
                    "label": r["name"], "value": r["name"], 
                    "type": "Business Principal", "context": fmt_princ(r["businesses"])
                } for r in cursor.fetchall()])
                
                # Infix Principals
                if len(results) < limit:
                    cursor.execute(
                        """
                        SELECT 
                            mode() WITHIN GROUP (ORDER BY p.name_c) as name, 
                            string_agg(DISTINCT b.name, '||') as businesses 
                        FROM principals p 
                        LEFT JOIN businesses b ON p.business_id = b.id
                        WHERE p.name_c_norm IS NOT NULL AND p.name_c_norm ILIKE %s 
                        GROUP BY p.name_c_norm
                        LIMIT %s
                        """,
                        (t_infix, limit - len(results))
                    )
                    existing = {x["value"].upper() for x in results} 
                    for r in cursor.fetchall():
                        if r["name"] and r["name"].upper() not in existing:
                            results.append({
                                "label": r["name"], "value": r["name"], 
                                "type": "Business Principal", "context": fmt_princ(r["businesses"])
                            })
                            existing.add(r["name"].upper())

                # 2. Search Property Owners
                if len(results) < limit:
                    remaining = limit - len(results)
                    
                    def fmt_owner(loc, city, zip_code):
                        parts = []
                        if loc: parts.append(loc)
                        if city: parts.append(city)
                        parts.append("CT")
                        if zip_code:
                            z = str(zip_code).strip()
                            if len(z) < 5 and z.isdigit(): z = z.zfill(5)
                            parts.append(z)
                        return ", ".join(parts) if loc else "Property Owner"

                    # Prefix Property Owners
                    cursor.execute(
                        """
                        SELECT 
                            mode() WITHIN GROUP (ORDER BY owner) as name, 
                            location as loc, property_city as city, property_zip as zip
                        FROM properties 
                        WHERE owner_norm IS NOT NULL AND lower(owner_norm) LIKE %s 
                        GROUP BY owner_norm, location, property_city, property_zip
                        ORDER BY lower(owner_norm) ASC 
                        LIMIT %s
                        """,
                        (t_prefix, remaining)
                    )
                    
                    existing_keys = {x["value"].upper() + x.get("context","") for x in results}
                    for r in cursor.fetchall():
                        ctx = fmt_owner(r["loc"], r["city"], r["zip"])
                        key = r["name"].upper() + ctx
                        if key not in existing_keys:
                            results.append({
                                "label": r["name"], "value": r["name"], 
                                "type": "Property Owner", "context": ctx
                            })
                            existing_keys.add(key)
                    
                    # Infix Property Owners (Multi-Word Support)
                    if len(results) < limit:
                        remaining = limit - len(results)
                        parts = q.split()
                        
                        if len(parts) > 1:
                            where_clauses = ["owner_norm IS NOT NULL"]
                            params = [] 
                            for part in parts:
                                where_clauses.append("owner_norm ILIKE %s")
                                params.append(f"%{part}%")
                            
                            sql = f"""
                                SELECT 
                                    mode() WITHIN GROUP (ORDER BY owner) as name, 
                                    location as loc, property_city as city, property_zip as zip
                                FROM properties 
                                WHERE {' AND '.join(where_clauses)}
                                GROUP BY owner_norm, location, property_city, property_zip
                                LIMIT %s
                            """
                            params.append(remaining)
                            cursor.execute(sql, tuple(params))
                        else:
                            cursor.execute(
                                """
                                SELECT 
                                    mode() WITHIN GROUP (ORDER BY owner) as name, 
                                    location as loc, property_city as city, property_zip as zip
                                FROM properties 
                                WHERE owner_norm IS NOT NULL AND owner_norm ILIKE %s 
                                GROUP BY owner_norm, location, property_city, property_zip
                                LIMIT %s
                                """,
                                (t_infix, remaining)
                            )
                        
                        for r in cursor.fetchall():
                            if not r["name"]: continue
                            ctx = fmt_owner(r["loc"], r["city"], r["zip"])
                            key = r["name"].upper() + ctx
                            if key not in existing_keys:
                                results.append({
                                    "label": r["name"], "value": r["name"], 
                                    "type": "Property Owner", "context": ctx
                                })
                                existing_keys.add(key)

                # 3. Search Co-Owners
                if len(results) < limit:
                    remaining = limit - len(results)
                    # Prefix Co-Owners
                    cursor.execute(
                        """
                        SELECT 
                            mode() WITHIN GROUP (ORDER BY co_owner) as name, 
                            location as loc, property_city as city, property_zip as zip
                        FROM properties 
                        WHERE co_owner_norm IS NOT NULL AND lower(co_owner_norm) LIKE %s 
                        GROUP BY co_owner_norm, location, property_city, property_zip
                        ORDER BY lower(co_owner_norm) ASC 
                        LIMIT %s
                        """,
                        (t_prefix, remaining)
                    )
                    existing_keys = {x["value"].upper() + x.get("context","") for x in results}
                    for r in cursor.fetchall():
                        if not r["name"]: continue
                        ctx = fmt_owner(r["loc"], r["city"], r["zip"])
                        key = r["name"].upper() + ctx
                        if key not in existing_keys:
                            results.append({
                                "label": r["name"], "value": r["name"], 
                                "type": "Property Co-Owner", "context": ctx
                            })
                            existing_keys.add(key)

                    # Infix Co-Owners (Multi-Word)
                    if len(results) < limit:
                        remaining = limit - len(results)
                        parts = q.split()
                        if len(parts) > 1:
                            where_clauses = ["co_owner_norm IS NOT NULL"]
                            params = [] 
                            for part in parts:
                                where_clauses.append("co_owner_norm ILIKE %s")
                                params.append(f"%{part}%")
                            
                            sql = f"""
                                SELECT 
                                    mode() WITHIN GROUP (ORDER BY co_owner) as name, 
                                    location as loc, property_city as city, property_zip as zip
                                FROM properties 
                                WHERE {' AND '.join(where_clauses)}
                                GROUP BY co_owner_norm, location, property_city, property_zip
                                LIMIT %s
                            """
                            params.append(remaining)
                            cursor.execute(sql, tuple(params))
                        else:
                            cursor.execute(
                                """
                                SELECT 
                                    mode() WITHIN GROUP (ORDER BY co_owner) as name, 
                                    location as loc, property_city as city, property_zip as zip
                                FROM properties 
                                WHERE co_owner_norm IS NOT NULL AND co_owner_norm ILIKE %s 
                                GROUP BY co_owner_norm, location, property_city, property_zip
                                LIMIT %s
                                """,
                                (t_infix, remaining)
                            )
                        
                        for r in cursor.fetchall():
                            if not r["name"]: continue
                            ctx = fmt_owner(r["loc"], r["city"], r["zip"])
                            key = r["name"].upper() + ctx
                            if key not in existing_keys:
                                results.append({
                                    "label": r["name"], "value": r["name"], 
                                    "type": "Property Co-Owner", "context": ctx
                                })
                                existing_keys.add(key)

            elif type == "address":
                # Search properties by location (address) and include City/Zip
                cursor.execute(
                    """
                    SELECT DISTINCT location, property_city, property_zip 
                    FROM properties 
                    WHERE location IS NOT NULL AND location LIKE %s 
                    ORDER BY location ASC 
                    LIMIT %s
                    """,
                    (t_prefix, limit)
                )
                
                results = []
                for r in cursor.fetchall():
                    parts = [r["location"]]
                    if r.get("property_city"): parts.append(r["property_city"])
                    parts.append("CT")
                    if r.get("property_zip"):
                        z = str(r["property_zip"]).strip()
                        if len(z) < 5 and z.isdigit(): z = z.zfill(5)
                        parts.append(z)
                    results.append(", ".join(parts))

    except Exception as e:
        logger.error(f"Autocomplete Error: {e}")
        return []

    return results[:limit]


# ------------------------------------------------------------
# SEARCH
# ------------------------------------------------------------
@app.get("/api/search", response_model=List[SearchResult])
def search_entities(type: str, term: str, conn=Depends(get_db_connection)):
    """
    type: 'business' | 'owner' | 'address'
    """
    if len(term or "") < 3:
        raise HTTPException(status_code=400, detail="Search term must be at least 3 characters long.")

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            t = term.upper()

            if type == "business":
                cursor.execute(
                    "SELECT id, name, business_address AS context FROM businesses WHERE upper(name) LIKE %s LIMIT 50",
                    (f"%{t}%",)
                )
                rows = cursor.fetchall()
                return [SearchResult(id=str(r["id"]), name=r["name"], type="business", context=r.get("context")) for r in rows]

            elif type == "owner":
                # UNIFIED SEARCH: "Owner" now means "All" (Principals, Businesses, Properties)
                results: List[SearchResult] = []
                t_exact = f"%{t}%"

                # 1. Search Principals
                # ------------------------------------------------------------------
                name_vars = get_name_variations(term, "principal")
                norm_set = list({ normalize_person_name_py(v) for v in name_vars if v })
                
                # Direct Principal Match
                cursor.execute(
                    """
                    SELECT DISTINCT name_c AS name
                    FROM principals
                    WHERE name_c IS NOT NULL AND upper(name_c) LIKE %s
                    LIMIT 50
                    """,
                    (t_exact,)
                )
                for r in cursor.fetchall():
                    if r["name"]:
                        results.append(SearchResult(id=r["name"], name=r["name"], type="owner", context="Principal"))

                # Property Owner Match (if we have normalized vars)
                # Property Owner Match (if we have normalized vars)
                # Strategy: 
                # 1. Try strict match on owner_norm (indexed, fast)
                # 2. If valid vars exist, also try a LIKE match on raw owner/co_owner columns for robustness
                #    (This helps when normalization might be slightly off or for partial names)
                
                params = []
                where_clauses = []
                
                if norm_set:
                    where_clauses.append("owner_norm = ANY(%s)")
                    params.append(norm_set)
                    where_clauses.append("co_owner_norm = ANY(%s)")
                    params.append(norm_set)
                
                # Also Add partial match on the input term itself against raw columns
                where_clauses.append("upper(owner) LIKE %s")
                params.append(t_exact)
                where_clauses.append("upper(co_owner) LIKE %s")
                params.append(t_exact)
                
                if where_clauses:
                    sql = f"""
                        SELECT DISTINCT owner AS name
                        FROM properties
                        WHERE {' OR '.join(where_clauses)}
                        LIMIT 50
                    """
                    cursor.execute(sql, params)
                    for r in cursor.fetchall():
                        if r["name"]:
                            # Avoid duplicates if possible, but list append is fast
                            if not any(x.id == r["name"] and x.type == "owner" for x in results):
                                results.append(SearchResult(id=r["name"], name=r["name"], type="owner", context="Property Owner"))


                # 2. Search Businesses
                # ------------------------------------------------------------------
                cursor.execute(
                    "SELECT id, name, business_address AS context FROM businesses WHERE upper(name) LIKE %s LIMIT 20",
                    (t_exact,)
                )
                for r in cursor.fetchall():
                    results.append(SearchResult(id=str(r["id"]), name=r["name"], type="business", context=r.get("context")))

                # 3. Search Properties (Addresses)
                # ------------------------------------------------------------------
                # Use pg_trgm for fuzzy matching only if search term is long enough, otherwise simple LIKE
                # Using the exact same logic as 'address' type but limiting count
                cursor.execute(
                    """
                    SELECT location, owner, co_owner, property_city, business_id
                    FROM properties
                    WHERE location %% %s
                    ORDER BY similarity(location, %s) DESC
                    LIMIT 20
                    """,
                    (t, t)
                )

                prop_rows = cursor.fetchall()

                # Resolve context for properties (Business vs Owner)
                if prop_rows:
                    # Batch collect business IDs
                    business_ids = {str(r['business_id']) for r in prop_rows if r.get('business_id')}
                    # If we really want full context we could fetch business names here, 
                    # but for speed let's just stick to basic info or reuse the logic from type='address' block if strictly needed.
                    # Simplified context generation:
                    
                    # Fetch business names if needed
                    biz_map = {}
                    if business_ids:
                         cursor.execute("SELECT id, name FROM businesses WHERE id::text = ANY(%s)", (list(business_ids),))
                         for b in cursor.fetchall():
                             biz_map[str(b['id'])] = b['name']

                    seen_locs = set()
                    for r in prop_rows:
                        loc = r['location']
                        if loc in seen_locs: continue
                        
                        # Determine what ID to pass. 
                        # Ideally, clicking an address should open the analysis for that PROPERTY's network.
                        # The 'type' needs to be handled by frontend. 
                        # If frontend receives type='property', does it work? 
                        # Frontend `loadNetwork` usually expects `principal` or `business`.
                        # However, the 'address' search block returns type='owner' or 'business' based on who owns the property.
                        # We should mimic that so the graph loads the OWNER of the property.
                        
                        target_id = None
                        target_type = None
                        ctx = ""

                        if r.get('business_id') and str(r['business_id']) in biz_map:
                             target_id = str(r['business_id'])
                             target_type = "business"
                             ctx = f"Owned by {biz_map[target_id]}"
                        elif r.get('owner'):
                             target_id = r['owner']
                             target_type = "owner"
                             ctx = f"Owned by {target_id}"
                        elif r.get('co_owner'):
                             target_id = r['co_owner']
                             target_type = "owner"
                             ctx = f"Co-owned by {target_id}"
                        
                        if target_id:
                            results.append(SearchResult(
                                id=target_id,
                                name=loc,          # Display the ADDRESS
                                type=target_type,  # But link to the OWNER entity
                                context=ctx
                            ))
                            seen_locs.add(loc)

                # Deduplicate final list by ID+Type just in case
                unique_results = []
                seen_ids = set()
                for res in results:
                    key = f"{res.type}:{res.id}:{res.name}"
                    if key not in seen_ids:
                        unique_results.append(res)
                        seen_ids.add(key)
                
                return unique_results[:50]

            elif type == "address":
                # Use pg_trgm for fuzzy matching, order by similarity, and fetch co_owner.
                cursor.execute(
                    """
                    SELECT location, owner, co_owner, property_city, business_id
                    FROM properties
                    WHERE location %% %s
                    ORDER BY similarity(location, %s) DESC
                    LIMIT 50
                    """,
                    (t, t)
                )
                rows = cursor.fetchall()
                if not rows:
                    return []

                # Batch collect business IDs and potential owner names for efficient lookup
                business_ids = {str(r['business_id']) for r in rows if r.get('business_id')}
                owner_names = {name.upper() for r in rows for name in (r.get('owner'), r.get('co_owner')) if name}

                # Create lookup maps for businesses found by ID or name
                business_info_by_id = {}
                business_info_by_name = {}

                if business_ids:
                    cursor.execute(
                        "SELECT id, name FROM businesses WHERE id::text = ANY(%s)",
                        (list(business_ids),)
                    )
                    for b in cursor.fetchall():
                        business_info_by_id[str(b['id'])] = {'name': b['name'], 'id': str(b['id'])}

                if owner_names:
                    cursor.execute(
                        "SELECT id, name, upper(name) as upper_name FROM businesses WHERE upper(name) = ANY(%s)",
                        (list(owner_names),)
                    )
                    for b in cursor.fetchall():
                        business_info_by_name[b['upper_name']] = {'name': b['name'], 'id': str(b['id'])}

                results: List[SearchResult] = []
                seen_locations = set()
                for r in rows:
                    if r.get('location') in seen_locations:
                        continue

                    entity_id, entity_type, context_owner_name = None, None, None

                    # Determine the primary entity for this property result, prioritizing businesses.
                    # Priority 1: Direct business_id link on the property record.
                    if r.get('business_id') and str(r['business_id']) in business_info_by_id:
                        biz = business_info_by_id[str(r['business_id'])]
                        entity_id = biz['id']
                        entity_type = 'business'
                        context_owner_name = biz['name']
                    # Priority 2: Owner name is a known business.
                    elif r.get('owner') and r['owner'].upper() in business_info_by_name:
                        biz = business_info_by_name[r['owner'].upper()]
                        entity_id = biz['id']
                        entity_type = 'business'
                        context_owner_name = biz['name']
                    # Priority 3: Co-owner name is a known business.
                    elif r.get('co_owner') and r['co_owner'].upper() in business_info_by_name:
                        biz = business_info_by_name[r['co_owner'].upper()]
                        entity_id = biz['id']
                        entity_type = 'business'
                        context_owner_name = biz['name']
                    # Priority 4: Fallback to owner or co-owner as a principal (person).
                    else:
                        primary_owner = r.get('owner') or r.get('co_owner')
                        if primary_owner:
                            entity_id = primary_owner
                            entity_type = 'owner'
                            context_owner_name = primary_owner

                    if entity_id:
                        results.append(SearchResult(
                            id=str(entity_id),
                            name=r["location"],
                            type=entity_type,
                            context=f"Owner: {context_owner_name}"
                        ))
                        seen_locations.add(r['location'])

                return results

            else:
                raise HTTPException(status_code=400, detail="Invalid search type.")
    except psycopg2.Error:
        logger.exception("Database search error")
        raise HTTPException(status_code=500, detail="Database query failed.")


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
            pname_norm = normalize_person_name_py(step.entity_id)
            cursor.execute(
                "SELECT network_id FROM entity_networks WHERE entity_type = 'principal' AND entity_id = %s LIMIT 1",
                (pname_norm,)
            )
            row = cursor.fetchone()
            if row:
                network_id = row["network_id"]

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
                pname_norm = normalize_person_name_py(step.entity_id)
                p_key = f"principal_{pname_norm}"
                new_entities[p_key] = Entity(id=pname_norm, name=step.entity_id, type="principal", details={})
                cursor.execute(
                    "SELECT * FROM properties WHERE principal_id = %s OR owner_norm = %s OR co_owner_norm = %s",
                    (pname_norm, pname_norm, pname_norm)
                )
                for p in cursor.fetchall():
                    new_properties[p["id"]] = p

            return IncrementalNetworkResponse(
                new_entities=list(new_entities.values()),
                new_properties=[
                    PropertyItem(
                        address=v.get("location"),
                        city=v.get("property_city"),
                        owner=v.get("owner"),
                        assessed_value=v.get("assessed_value"),
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
            pkey = f"principal_{normalize_person_name_py(pr['principal_id'])}"
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
                p_key = f"principal_{normalize_person_name_py(r['pname'])}"
                new_links.setdefault(b_key, set()).add(p_key)
                new_links.setdefault(p_key, set()).add(b_key)

        cursor.execute(
            "SELECT * FROM properties WHERE (business_id = ANY(%s)) OR (principal_id = ANY(%s))",
            (biz_ids or [None], principal_ids or [None])
        )
        for p in cursor.fetchall():
            new_properties[p["id"]] = p

    return IncrementalNetworkResponse(
        new_entities=list(new_entities.values()),
        new_properties=[
            PropertyItem(
                address=v.get("location"),
                city=v.get("property_city"),
                owner=v.get("owner"),
                assessed_value=v.get("assessed_value"),
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
        return f"principal_{normalize_person_name_py(name)}"

    def generate_network_data():
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                network_id = None

                if entity_type == "business":
                    cursor.execute(
                        "SELECT network_id FROM entity_networks "
                        "WHERE entity_type = 'business' AND entity_id = %s LIMIT 1",
                        (entity_id,)
                    )
                    row = cursor.fetchone()
                    if row:
                        network_id = row["network_id"]

                else:
                    pname_norm = normalize_person_name_py(entity_name or entity_id)
                    cursor.execute(
                        "SELECT network_id FROM entity_networks "
                        "WHERE entity_type = 'principal' AND entity_id = %s LIMIT 1",
                        (pname_norm,)
                    )
                    row = cursor.fetchone()
                    if row:
                        network_id = row["network_id"]

                # --- If no network found → isolated view
                if not network_id:
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
                        for p in cursor.fetchall():
                            yield _yield(json.dumps(
                                {"type": "properties", "data": [shape_property_row(p)]},
                                default=json_converter
                            ))

                    else:
                        pname_norm = normalize_person_name_py(entity_name or entity_id)
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
                            "WHERE principal_id = %s OR owner_norm = %s OR co_owner_norm = %s",
                            (pname_norm, pname_norm, pname_norm)
                        )
                        for p in cursor.fetchall():
                            yield _yield(json.dumps(
                                {"type": "properties", "data": [shape_property_row(p)]},
                                default=json_converter
                            ))

                    yield _yield(json.dumps({"type": "done"}))
                    return

                # --- If network found → load entire network (businesses, principals, properties)
                # Businesses
                cursor.execute(
                    "SELECT b.* FROM entity_networks en "
                    "JOIN businesses b ON b.id::text = en.entity_id "
                    "WHERE en.network_id = %s AND en.entity_type = 'business'",
                    (network_id,)
                )
                businesses = cursor.fetchall()

                # Principals
                cursor.execute(
                    "SELECT entity_id AS principal_id, COALESCE(entity_name, entity_id) AS principal_name "
                    "FROM entity_networks "
                    "WHERE network_id = %s AND entity_type = 'principal'",
                    (network_id,)
                )
                principals_in_network = cursor.fetchall()

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


                # Build links
                if businesses:
                    cursor.execute(
                        "SELECT business_id, COALESCE(name_c, trim(concat_ws(' ', firstname,middlename,lastname,suffix))) AS pname "
                        "FROM principals WHERE business_id = ANY(%s)",
                        ([b["id"] for b in businesses],)
                    )
                    for r in cursor.fetchall():
                        if not r.get("pname"):
                            continue
                        b_key = f"business_{r['business_id']}"
                        p_key = _principal_key(r["pname"])
                        if b_key in entities_dict and p_key in entities_dict:
                            links["business_to_principal"].append({"source": b_key, "target": p_key})
                            links["principal_to_business"].append({"source": p_key, "target": b_key})

                yield _yield(json.dumps(
                    {"type": "entities", "data": {"entities": list(entities_dict.values()), "links": links}},
                    default=json_converter,
                ))

                # Stream properties for all businesses/principals in the network
                biz_ids = [b["id"] for b in businesses]
                principal_ids = [pr["principal_id"] for pr in principals_in_network]

                cursor.execute(
                    "SELECT * FROM properties WHERE (business_id = ANY(%s)) OR (principal_id = ANY(%s))",
                    (biz_ids or [None], principal_ids or [None])
                )
                for p in cursor.fetchall():
                    yield _yield(json.dumps(
                        {"type": "properties", "data": [shape_property_row(p)]},
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
    norm_set = list({ normalize_person_name_py(n) for n in names })
    props: List[PropertyItem] = []
    seen_ids: Set[int] = set()

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT *
            FROM properties
            WHERE owner_norm = ANY(%s) OR co_owner_norm = ANY(%s)
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
    This is the definitive, correct logic for calculating top networks.
    It counts properties and sums values at the network level by joining through
    all possible links (business_id, principal_id, and owner_norm).
    """
    town_where_clause = ""
    params = []
    if town_col and town_filter:
        town_where_clause = f"WHERE p.{town_col} = %s"
        params.append(town_filter)

    query = f"""
        WITH property_to_network AS (
            -- Link properties via business_id
            SELECT p.id as property_id, en.network_id
            FROM properties p
            JOIN entity_networks en ON p.business_id::text = en.entity_id AND en.entity_type = 'business'
            WHERE p.business_id IS NOT NULL
            
            UNION
            
            -- Link properties via principal_id (linking principal ID to principal NAME in entity_networks)
            SELECT p.id, en.network_id
            FROM properties p
            JOIN principals pr ON p.principal_id = pr.id::text
            JOIN entity_networks en ON pr.name_c = en.entity_id AND en.entity_type = 'principal'
            WHERE p.principal_id IS NOT NULL

            UNION

            -- Link properties via owner_norm (for principals owning property directly)
            SELECT p.id, en.network_id
            FROM properties p
            JOIN entity_networks en ON p.owner_norm = en.entity_id AND en.entity_type = 'principal'
            WHERE p.owner_norm IS NOT NULL
        ),
        top_networks AS (
            SELECT
                ptn.network_id,
                COUNT(DISTINCT ptn.property_id) as property_count,
                COALESCE(SUM(p.assessed_value), 0) as total_assessed_value,
                COALESCE(SUM(p.appraised_value), 0) as total_appraised_value,
                (SELECT COUNT(*) FROM entity_networks en WHERE en.network_id = ptn.network_id AND en.entity_type = 'business') as business_count
            FROM property_to_network ptn
            JOIN properties p ON ptn.property_id = p.id
            {town_where_clause}
            GROUP BY ptn.network_id
            HAVING COUNT(DISTINCT ptn.property_id) > 0
            ORDER BY property_count DESC
            LIMIT 10
        ),
        network_display_entity AS (
            SELECT DISTINCT ON (tn.network_id)
                tn.network_id,
                tn.property_count,
                tn.total_assessed_value,
                tn.total_appraised_value,
                tn.business_count,
                en.entity_id,
                en.entity_type,
                en.entity_name,
                (
                    SELECT COUNT(p_inner.id)
                    FROM properties p_inner
                    WHERE (p_inner.business_id::text = en.entity_id AND en.entity_type = 'business')
                       OR (p_inner.principal_id = en.entity_id AND en.entity_type = 'principal')
                       OR (p_inner.owner_norm = en.entity_id AND en.entity_type = 'principal')
                ) as entity_property_count
            FROM top_networks tn
            JOIN entity_networks en ON tn.network_id = en.network_id
            ORDER BY tn.network_id, entity_property_count DESC, en.entity_name
        )
        SELECT
            nde.entity_id,
            nde.entity_name,
            nde.entity_type,
            nde.property_count as value,
            nde.total_assessed_value,
            nde.total_appraised_value,
            nde.business_count,
            nde.network_id
        FROM network_display_entity nde
        ORDER BY nde.property_count DESC
    """
    cursor.execute(query, params)
    top_networks = cursor.fetchall()

    for network in top_networks:
        # Top Principals
        cursor.execute("""
            SELECT name, state FROM (
                SELECT DISTINCT ON(pr.name_c)
                    pr.name_c as name,
                    pr.state,
                    COUNT(*) as link_count
                FROM entity_networks en
                JOIN principals pr ON en.entity_id = pr.business_id
                WHERE en.network_id = %s AND en.entity_type = 'business' AND pr.name_c IS NOT NULL
                GROUP BY pr.name_c, pr.state
                ORDER BY pr.name_c, link_count DESC
            ) as distinct_principals
            ORDER BY link_count DESC
            LIMIT 3;
        """, (network['network_id'],))
        principals = cursor.fetchall()
        
        if network['entity_type'] == 'principal':
            cursor.execute("SELECT name_c as name, state FROM principals WHERE name_c = %s LIMIT 1", (network['entity_name'],))
            principal_info = cursor.fetchone()
            if principal_info and principal_info not in principals:
                principals.insert(0, principal_info)
        
        network['principals'] = principals[:3]

        # Top Businesses
        cursor.execute("""
            SELECT name, status, business_address, state FROM (
                SELECT DISTINCT ON(b.name)
                    b.name,
                    b.status,
                    b.business_address,
                    b.business_state as state,
                    COUNT(*) as link_count
                FROM entity_networks en
                JOIN businesses b ON en.entity_id = b.id::text
                WHERE en.network_id = %s AND en.entity_type = 'business'
                GROUP BY b.name, b.status, b.business_address, b.business_state
            ) as distinct_businesses
            ORDER BY link_count DESC
            LIMIT 3;
        """, (network['network_id'],))
        businesses = cursor.fetchall()
        network['businesses'] = businesses[:3]

    return top_networks

def _update_insights_cache_sync():
    """Synchronous version of the cache update logic to be run in a thread."""
    if not db_pool:
        logger.error("DB pool not available for cache refresh.")
        return

    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            logger.info("Starting background refresh of insights cache...")
            
            insights_by_municipality = {}
            
            town_col = None
            for c in ["property_city", "town", "city", "municipality"]:
                if _column_exists(cursor, "properties", c):
                    town_col = c
                    break
            
            insights_by_municipality['STATEWIDE'] = _calculate_and_cache_insights(cursor, None, None)

            if town_col:
                cursor.execute(f"SELECT {town_col} AS town FROM properties WHERE {town_col} IS NOT NULL AND {town_col} <> '' GROUP BY {town_col} ORDER BY COUNT(*) DESC LIMIT 10")
                top_towns = [r["town"] for r in cursor.fetchall() if r["town"]]
                
                for t in top_towns:
                    town_networks = _calculate_and_cache_insights(cursor, town_col, t)
                    if town_networks:
                        insights_by_municipality[t.upper()] = town_networks
            
            insights_json = json.dumps(insights_by_municipality, default=json_converter)
            cursor.execute("""
                INSERT INTO kv_cache (key, value)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    created_at = now();
            """, ('insights', insights_json))
            conn.commit()
            logger.info("✅ Background refresh of insights cache complete.")

    except Exception as e:
        logger.exception("Error during background cache refresh")
        if conn:
            conn.rollback()
    finally:
        if conn and db_pool:
            db_pool.putconn(conn)

@app.get("/api/insights", response_model=Dict[str, List[InsightItem]])
def get_insights(conn=Depends(get_db_connection)):
    """
    Serves pre-calculated insights from the cache table for fast response times.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT value FROM kv_cache WHERE key = 'insights'")
            row = cursor.fetchone()
            if not row or not row['value']:
                return {}
            return row['value']
    except Exception:
        logger.exception("Could not fetch insights from cache.")
        raise HTTPException(status_code=500, detail="Failed to retrieve insights.")

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
    return reports

# ------------------------------------------------------------
# AI Report (cached per day)
# ------------------------------------------------------------
def _compute_local_context(conn, entity: str, entity_type: str) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {"entity": entity, "entity_type": entity_type}
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        if entity_type in ("owner", "principal"):
            cursor.execute("""
                SELECT COUNT(*) AS cnt, COALESCE(SUM(assessed_value),0) AS total_value
                FROM properties
                WHERE owner_norm = normalize_person_name(%s)
                   OR co_owner_norm = normalize_person_name(%s)
            """, (entity, entity))
            row = cursor.fetchone() or {}
            ctx["property_count"] = int(row.get("cnt") or 0)
            ctx["total_assessed_value"] = float(row.get("total_value") or 0.0)

            cursor.execute("""
                SELECT property_city AS city, COUNT(*) AS cnt
                FROM properties
                WHERE owner_norm = normalize_person_name(%s)
                   OR co_owner_norm = normalize_person_name(%s)
                GROUP BY property_city
                ORDER BY cnt DESC
                LIMIT 10
            """, (entity, entity))
            ctx["top_cities"] = cursor.fetchall()

        elif entity_type == "business":
            cursor.execute("""
                SELECT COUNT(*) AS cnt, COALESCE(SUM(assessed_value),0) AS total_value
                FROM properties
                WHERE owner = %s
                   OR owner_norm = normalize_person_name(%s)
            """, (entity, entity))
            row = cursor.fetchone() or {}
            ctx["property_count"] = int(row.get("cnt") or 0)
            ctx["total_assessed_value"] = float(row.get("total_value") or 0.0)

            cursor.execute("""
                SELECT property_city AS city, COUNT(*) AS cnt
                FROM properties
                WHERE owner = %s
                   OR owner_norm = normalize_person_name(%s)
                GROUP BY property_city
                ORDER BY cnt DESC
                LIMIT 10
            """, (entity, entity))
            ctx["top_cities"] = cursor.fetchall()
    return ctx

def _draft_ai_report_text(context: Dict[str, Any]) -> Tuple[str, str]:
    title = f"AI report — {context.get('entity')}"
    if not (openai and OPENAI_API_KEY):
        cities = ", ".join([f"{r['city']} ({r['cnt']})" for r in context.get("top_cities", []) if r.get("city")])
        body = (
            f"Summary for {context.get('entity_type')}: {context.get('entity')}\n\n"
            f"- Properties found: {context.get('property_count', 0)}\n"
            f"- Total assessed value: ${int(context.get('total_assessed_value', 0)):,}\n"
            f"- Top cities by count: {cities or 'N/A'}\n"
            "\n(Generated without external AI due to missing API key.)"
        )
        return (title, body)
    prompt = (
        "You are generating a concise investigative briefing for a property-ownership network tool. "
        "Write in clear, scannable bullets with short sections. Include actionable insights. "
        "Use only the structured context provided.\n\n"
        f"CONTEXT JSON:\n{json.dumps(context, default=str)}\n\n"
        "Output sections:\n"
        "1) Snapshot (counts, value)\n"
        "2) Geography focus (top cities)\n"
        "3) Next steps for tenants/organizers (generic, non-legal)\n"
    )
    try:
        resp = openai.ChatCompletion.create(  # type: ignore[attr-defined]
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a meticulous analyst."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=700,
        )
        content = resp["choices"][0]["message"]["content"].strip()
        return (title, content)
    except Exception as e:  # pragma: no cover
        logger.warning("OpenAI error: %s", e)
        # deterministic fallback
        cities = ", ".join([f"{r['city']} ({r['cnt']})" for r in context.get("top_cities", []) if r.get("city")])
        body = (
            f"Summary for {context.get('entity_type')}: {context.get('entity')}\n\n"
            f"- Properties found: {context.get('property_count', 0)}\n"
            f"- Total assessed value: ${int(context.get('total_assessed_value', 0)):,}\n"
            f"- Top cities by count: {cities or 'N/A'}\n"
            "\n(OpenAI call failed; generated fallback.)"
        )
        return (title, body)

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
            if existing:
                return existing
        context = _compute_local_context(conn, req.entity, req.entity_type)
        title, content = _draft_ai_report_text(context)
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
        return row


# ------------------------------------------------------------
# Health
# ------------------------------------------------------------
@app.get("/api/healthz")
def healthz():
    return {"ok": True}


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
        
        # 3. Summarize with OpenAI
        final_summary = "Analysis Unavailable."
        title = f"AI Digest - Network of {len(req.entities)} Entities"
        
        if openai and OPENAI_API_KEY:
            prompt = (
                f"You are an investigative analyst. You are analyzing a property network consisting of {len(req.entities)} related entities (principals and businesses). "
                f"Together, they own {total_props} properties with a total assessed value of ${total_val:,.0f}.\n\n"
                "Analyze the following web search excerpts for this group.\n\n"
                "STRUCTURE YOUR RESPONSE AS FOLLOWS:\n"
                "1. OVERALL SUMMARY: A concise 3-4 sentence high-level overview of the entire network's footprint, reputation, and scale.\n"
                "2. KEY RISKS & FINDINGS: Bullet points of major issues, complaints, eviction history, or legal patterns found in the news.\n"
                "3. ENTITY BREAKDOWN: Brief notes on individual principals or businesses where specific info was found.\n\n"
                "CITATIONS: When referencing specific details, include the source link inline formatted as (Source: <url>). Do NOT use markdown links.\n\n"
                "Focus on identifying problems, complaints, violations, poor conditions, or anything of interest to tenants/organizers.\n"
                "Be specific. If no negative info is found, focus on characterizing the portfolio based on the property count and value provided above.\n\n"
                f"Web Search Data:\n{full_text_context}\n"
            )
            try:
                 # Check for v1.0+ vs older SDK
                try:
                    # Try v1.0.0+ Client first
                    from openai import OpenAI
                    client = OpenAI(api_key=OPENAI_API_KEY)
                    resp = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": "You are a meticulous investigative analyst."},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.3,
                        max_tokens=1500,
                    )
                    final_summary = resp.choices[0].message.content.strip()
                except ImportError:
                    # Fallback to older <1.0.0 interface
                    resp = openai.ChatCompletion.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are a meticulous investigative analyst."},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.3,
                        max_tokens=1500,
                    )
                    final_summary = resp["choices"][0]["message"]["content"].strip()
            except Exception as e:
                logger.error(f"OpenAI Digest Error: {e}")
                final_summary = f"AI Synthesis Encountered an Error. Displaying raw search hits instead:\n\n{full_text_context}\n\nDEBUG_ERROR_DETAILS: {repr(e)}"
        else:
             final_summary = "OpenAI API Key not configured. Displaying raw web search results:\n\n" + full_text_context

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
            cursor.execute(
                "UPDATE properties SET latitude = %s, longitude = %s WHERE id = %s",
                (lat, lon, property_id)
            )
            # If no row updated, we might want to know, but idempotency is fine.
            conn.commit()
            return {"status": "success", "id": property_id, "lat": lat, "lon": lon}
    except Exception as e:
        logger.error(f"Failed to update geocode for property {property_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))