#!/usr/bin/env python3
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values
import requests
import logging
from email.utils import parsedate_to_datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
HARTFORD_ENFORCEMENT_URL = "https://data.hartford.gov/api/download/v1/items/7cf59bd708a94395be61954d6f430e3f/csv?layers=1"

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def parse_http_date(value):
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).date()
    except Exception:
        return None

def update_status(conn, status, details, external_last_updated=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO data_source_status
                (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
            VALUES ('HARTFORD_CODE_ENFORCEMENT', 'city_enforcement', %s, NOW(), %s, %s)
            ON CONFLICT (source_name)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = EXCLUDED.external_last_updated,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
        """, (external_last_updated, status, Json(details)))
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()

def ensure_schema(conn):
    schema_path = os.path.join(os.path.dirname(__file__), "..", "scripts", "code_enforcement.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def download_latest_csv():
    """Download the latest CSV from Hartford Open Data"""
    logger.info(f"Downloading latest data from {HARTFORD_ENFORCEMENT_URL}...")
    try:
        response = requests.get(HARTFORD_ENFORCEMENT_URL, timeout=30)
        response.raise_for_status()
        temp_path = "/tmp/hartford_enforcement.csv"
        with open(temp_path, "wb") as f:
            f.write(response.content)
        return temp_path, parse_http_date(response.headers.get("Last-Modified"))
    except Exception as e:
        logger.error(f"Failed to download latest CSV: {e}")
        return None, None

def ingest_csv(csv_path, external_last_updated=None):
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return

    logger.info(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    
    # Pre-processing
    # Data has BOME in headers, cleaning
    df.columns = [c.lstrip('\ufeff') for c in df.columns]
    
    logger.info(f"Processing {len(df)} records...")
    
    conn = get_db_connection()
    ensure_schema(conn)
    cur = conn.cursor()
    
    # 1. Build ParcelID -> property_id mapping for Hartford
    logger.info("Building ParcelID -> property_id mapping...")
    cur.execute("""
        SELECT id, split_part(cama_site_link, '-', 2) as parcel_id 
        FROM properties 
        WHERE property_city ILIKE 'Hartford' 
        AND cama_site_link IS NOT NULL 
        AND cama_site_link LIKE '%-%'
    """)
    parcel_map = {row[1]: row[0] for row in cur.fetchall() if row[1]}
    logger.info(f"Mapped {len(parcel_map)} parcels in Hartford.")

    # 2. Prepare data for upsert
    ingest_data = []

    def clean_val(val):
        if pd.isna(val) or pd.isnull(val):
            return None
        val_str = str(val).strip()
        if val_str.lower() in ('nan', 'none', 'null', ''):
            return None
        return val_str

    for _, row in df.iterrows():
        parcel_id = clean_val(row.get('ParcelID'))
        property_id = parcel_map.get(parcel_id)
        
        # Date parsing
        date_opened = clean_val(row.get('Date_Opened'))
        date_closed = clean_val(row.get('Date_Closed'))
        
        # Clean record module
        record_module = clean_val(row.get('Record_Module'))
        
        # Contact Name
        first = clean_val(row.get('Contact_First_Name')) or ""
        last = clean_val(row.get('Contact_Last_Name')) or ""
        contact_name = f"{first} {last}".strip() or None
        
        ingest_data.append((
            property_id,
            'HARTFORD',
            clean_val(row.get('Case_Number')),
            parcel_id,
            clean_val(row.get('Address')),
            clean_val(row.get('Unit')),
            record_module,
            clean_val(row.get('Dept_Division')),
            clean_val(row.get('Record_Name')),
            clean_val(row.get('Record_Type')),
            clean_val(row.get('Record_Status')),
            date_opened,
            date_closed,
            clean_val(row.get('Property_Owner')),
            contact_name,
            clean_val(row.get('Inspector_Name')),
            clean_val(row.get('Inspection_Type')),
            clean_val(row.get('GlobalID'))
        ))

    # 3. Upsert into database
    logger.info("Upserting records into code_enforcement...")
    upsert_query = """
        INSERT INTO code_enforcement (
            property_id, municipality, case_number, parcel_id, address, unit,
            record_module, dept_division, record_name, record_type, record_status,
            date_opened, date_closed, property_owner, contact_name,
            inspector_name, inspection_type, global_id
        ) VALUES %s
        ON CONFLICT (global_id) DO UPDATE SET
            property_id = EXCLUDED.property_id,
            property_owner = EXCLUDED.property_owner,
            record_status = EXCLUDED.record_status,
            date_closed = EXCLUDED.date_closed,
            updated_at = CURRENT_TIMESTAMP
    """
    
    if ingest_data:
        execute_values(cur, upsert_query, ingest_data)
        current_global_ids = [(row[17],) for row in ingest_data if row[17]]
        if not current_global_ids:
            raise RuntimeError("Official Hartford feed did not include any GlobalID values; refusing snapshot cleanup.")
        cur.execute("""
            CREATE TEMP TABLE hartford_current_global_ids (
                global_id TEXT PRIMARY KEY
            ) ON COMMIT DROP
        """)
        execute_values(
            cur,
            "INSERT INTO hartford_current_global_ids (global_id) VALUES %s ON CONFLICT DO NOTHING",
            current_global_ids,
        )
        cur.execute("""
            DELETE FROM code_enforcement ce
            WHERE ce.municipality = 'HARTFORD'
              AND NOT EXISTS (
                  SELECT 1
                  FROM hartford_current_global_ids current_ids
                  WHERE current_ids.global_id = ce.global_id
              )
        """)
        deleted_stale = cur.rowcount
    else:
        deleted_stale = 0
    conn.commit()
    logger.info(f"Successfully ingested/updated {len(ingest_data)} enforcement records.")

    linked_count = sum(1 for row in ingest_data if row[0] is not None)
    update_status(conn, "success", {
        "source_url": HARTFORD_ENFORCEMENT_URL,
        "source_records": len(ingest_data),
        "matched_records": linked_count,
        "records_without_local_property": len(ingest_data) - linked_count,
        "removed_records_not_in_current_source": deleted_stale,
    }, external_last_updated)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    latest_path, external_date = download_latest_csv()
    if not latest_path:
        conn = get_db_connection()
        try:
            update_status(conn, "failed", {
                "source_url": HARTFORD_ENFORCEMENT_URL,
                "message": "Official Hartford source download failed; no fallback data was ingested.",
            }, None)
        finally:
            conn.close()
        sys.exit(1)
    ingest_csv(latest_path, external_date)
