#!/usr/bin/env python3
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
import datetime
import logging

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
HARTFORD_ENFORCEMENT_URL = "https://data.hartford.gov/api/download/v1/items/7cf59bd708a94395be61954d6f430e3f/csv?layers=1"
LOCAL_CSV_PATH = "/home/sk/dev/theyownwhat/data/Hartford_Code_Enforcement_20200101_to_20260131.csv"

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def download_latest_csv():
    """Download the latest CSV from Hartford Open Data"""
    logger.info(f"Downloading latest data from {HARTFORD_ENFORCEMENT_URL}...")
    try:
        response = requests.get(HARTFORD_ENFORCEMENT_URL, timeout=30)
        response.raise_for_status()
        temp_path = "/tmp/hartford_enforcement.csv"
        with open(temp_path, "wb") as f:
            f.write(response.content)
        return temp_path
    except Exception as e:
        logger.error(f"Failed to download latest CSV: {e}")
        return None

def ingest_csv(csv_path):
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
    for _, row in df.iterrows():
        parcel_id = str(row['ParcelID']) if pd.notnull(row['ParcelID']) else None
        property_id = parcel_map.get(parcel_id)
        
        # Date parsing
        date_opened = row['Date_Opened'] if pd.notnull(row['Date_Opened']) else None
        date_closed = row['Date_Closed'] if pd.notnull(row['Date_Closed']) else None
        
        # Clean record module
        record_module = row['Record_Module'] if pd.notnull(row['Record_Module']) else None
        
        # Contact Name
        first = str(row['Contact_First_Name']) if pd.notnull(row['Contact_First_Name']) else ""
        last = str(row['Contact_Last_Name']) if pd.notnull(row['Contact_Last_Name']) else ""
        contact_name = f"{first} {last}".strip() or None
        
        ingest_data.append((
            property_id,
            'HARTFORD',
            row['Case_Number'],
            parcel_id,
            row['Address'],
            row['Unit'],
            record_module,
            row['Dept_Division'],
            row['Record_Name'],
            row['Record_Type'],
            row['Record_Status'],
            date_opened,
            date_closed,
            row['Property_Owner'],
            contact_name,
            row['Inspector_Name'],
            row['Inspection_Type'],
            row['GlobalID']
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
            record_status = EXCLUDED.record_status,
            date_closed = EXCLUDED.date_closed,
            updated_at = CURRENT_TIMESTAMP
    """
    
    execute_values(cur, upsert_query, ingest_data)
    conn.commit()
    logger.info(f"Successfully ingested/updated {len(ingest_data)} enforcement records.")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    # If standard run, try latest first, fallback to local
    latest_path = download_latest_csv()
    if latest_path:
        ingest_csv(latest_path)
    else:
        logger.info("Falling back to local CSV...")
        ingest_csv(LOCAL_CSV_PATH)
