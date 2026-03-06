#!/usr/bin/env python3
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
import datetime
import logging
import re
from io import StringIO

# Add parent directory to path for sibling imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from api.shared_utils import normalize_business_name, normalize_person_name, extract_base_address
except ImportError:
    # Fallback to simple normalization if shared_utils is not available in current environment
    def normalize_business_name(n): return n.upper().strip() if n else ""
    def normalize_person_name(n): return n.upper().strip() if n else ""
    def extract_base_address(a): return a.upper().strip() if a else ""

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
EVICTIONS_URL = os.environ.get("EVICTION_DATA_URL", "")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def normalize_address_for_match(addr):
    if not addr: return ""
    # Use the same logic as updater/update_data.py
    normalized = ' '.join(str(addr).upper().strip().split())
    # Standardize common suffixes
    normalizations = {
        ' STREET': ' ST', ' AVENUE': ' AVE', ' ROAD': ' RD',
        ' DRIVE': ' DR', ' LANE': ' LN', ' COURT': ' CT',
        ' PLACE': ' PL', ' BOULEVARD': ' BLVD', ' CIRCLE': ' CIR'
    }
    for old, new in normalizations.items():
        normalized = normalized.replace(old, new)
    return normalized

BUSINESS_PLAINTIFF_PATTERN = re.compile(
    r"\b(LLC|L\.L\.C|INC|INCORPORATED|CORP|CORPORATION|CO\b|COMPANY|LP|LTD|TRUST|BANK|ASSOCIATION|HOUSING|PROPERTIES|REALTY|MANAGEMENT|HOLDINGS|SERVICING|MORTGAGE)\b",
    re.IGNORECASE
)

def normalize_plaintiff_name(name):
    raw = (name or "").strip()
    if not raw:
        return ""
    if BUSINESS_PLAINTIFF_PATTERN.search(raw):
        return normalize_business_name(raw)
    return normalize_person_name(raw)

def ingest_evictions():
    if not EVICTIONS_URL:
        logger.error("EVICTION_DATA_URL not set — skipping eviction ingest.")
        return
    logger.info(f"Downloading eviction data from {EVICTIONS_URL}...")
    try:
        response = requests.get(EVICTIONS_URL, timeout=120)
        response.raise_for_status()
        # Use StringIO to feed CSV to pandas
        csv_data = StringIO(response.text)
        # Based on inspection: sep=';', quotechar='"', no header
        df = pd.read_csv(csv_data, sep=';', quotechar='"', header=None, low_memory=False)
    except Exception as e:
        logger.error(f"Failed to download or parse eviction CSV: {e}")
        return

    logger.info(f"Processing {len(df)} eviction records...")
    
    # Mapping based on inspection:
    # 5: Case Number
    # 11: Plaintiff Name
    # 17: Filing Date
    # 20: Address
    # 21: Unit
    # 22: Town
    # 26: Status
    # 29: DispositionDate
    # 30: Plaintiff Attorney Juris ID
    # 31: Plaintiff Attorney Name
    # 32: Plaintiff Attorney Firm
    # 34: Defendant Attorney Juris ID
    # 35: Defendant Attorney Name
    # 36: Defendant Attorney Last
    # 37: Defendant Attorney First
    
    df_clean = pd.DataFrame()
    df_clean['case_number'] = df[5]
    df_clean['plaintiff_name'] = df[11].fillna("")
    df_clean['filing_date'] = pd.to_datetime(df[17], errors='coerce').dt.date
    df_clean['address'] = df[20].fillna("")
    df_clean['unit'] = df[21].fillna("")
    df_clean['town'] = df[22].fillna("")
    df_clean['status'] = df[26].fillna("")
    df_clean['disposition_date'] = pd.to_datetime(df[29], errors='coerce').dt.date
    df_clean['plaintiff_attorney_juris_id'] = df[30].fillna("")
    df_clean['plaintiff_attorney_name'] = df[31].fillna("")
    df_clean['plaintiff_attorney_firm'] = df[32].fillna("")
    df_clean['defendant_attorney_juris_id'] = df[34].fillna("")
    df_clean['defendant_attorney_name'] = df[35].fillna("")
    df_clean['defendant_attorney_last'] = df[36].fillna("")
    df_clean['defendant_attorney_first'] = df[37].fillna("")
    
    # Strictly filter out empty case numbers
    df_clean = df_clean[df_clean['case_number'].notnull()]
    
    # Normalization
    logger.info("Normalizing plaintiff names and addresses...")
    df_clean['plaintiff_norm'] = df_clean['plaintiff_name'].apply(normalize_plaintiff_name)
    df_clean['plaintiff_attorney_norm'] = (
        df_clean['plaintiff_attorney_firm'].replace(r'^\s*\\N\s*$', "", regex=True).fillna("").astype(str).str.strip()
    )
    missing_attorney_firm = df_clean['plaintiff_attorney_norm'] == ""
    df_clean.loc[missing_attorney_firm, 'plaintiff_attorney_norm'] = (
        df_clean.loc[missing_attorney_firm, 'plaintiff_attorney_name']
        .replace(r'^\s*\\N\s*$', "", regex=True)
        .fillna("")
        .astype(str)
        .str.strip()
    )
    df_clean['plaintiff_attorney_norm'] = df_clean['plaintiff_attorney_norm'].apply(normalize_business_name)
    df_clean['norm_addr'] = df_clean['address'].apply(normalize_address_for_match)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Build Address+Town -> property_id mapping
    # We only care about towns present in the eviction data to save memory
    towns = df_clean['town'].unique().tolist()
    logger.info(f"Fetching property mapping for {len(towns)} towns...")
    
    # Use normalized_address if available, otherwise location
    cur.execute("""
        SELECT id, property_city, normalized_address, location 
        FROM properties 
        WHERE property_city = ANY(%s)
    """, (towns,))
    
    property_map = {} # (norm_addr, town) -> property_id
    for pid, town, norm_addr, loc in cur.fetchall():
        t = town.upper()
        if norm_addr:
            property_map[(normalize_address_for_match(norm_addr), t)] = pid
        if loc:
            property_map[(normalize_address_for_match(loc), t)] = pid

    logger.info(f"Mapped {len(property_map)} property addresses.")
    
    # 2. Prepare data for upsert
    ingest_data = []
    for _, row in df_clean.iterrows():
        town_upper = str(row['town']).upper()
        prop_id = property_map.get((row['norm_addr'], town_upper))
        
        ingest_data.append((
            row['case_number'],
            prop_id,
            row['plaintiff_name'],
            row['plaintiff_norm'],
            row['plaintiff_attorney_juris_id'],
            row['plaintiff_attorney_name'],
            row['plaintiff_attorney_firm'],
            row['plaintiff_attorney_norm'],
            row['defendant_attorney_juris_id'],
            row['defendant_attorney_name'],
            row['defendant_attorney_last'],
            row['defendant_attorney_first'],
            row['town'],
            row['filing_date'],
            row['status'],
            row['address'],
            row['norm_addr'],
            row['disposition_date'] if pd.notna(row['disposition_date']) else None
        ))

    # 3. Upsert into database
    logger.info(f"Upserting {len(ingest_data)} records into evictions...")
    upsert_query = """
        INSERT INTO evictions (
            case_number, property_id, plaintiff_name, plaintiff_norm, 
            plaintiff_attorney_juris_id, plaintiff_attorney_name, plaintiff_attorney_firm, plaintiff_attorney_norm,
            defendant_attorney_juris_id, defendant_attorney_name, defendant_attorney_last, defendant_attorney_first,
            municipality, filing_date, status, address, normalized_address, disposition_date
        ) VALUES %s
        ON CONFLICT (case_number) DO UPDATE SET
            property_id = EXCLUDED.property_id,
            plaintiff_name = EXCLUDED.plaintiff_name,
            plaintiff_norm = EXCLUDED.plaintiff_norm,
            plaintiff_attorney_juris_id = EXCLUDED.plaintiff_attorney_juris_id,
            plaintiff_attorney_name = EXCLUDED.plaintiff_attorney_name,
            plaintiff_attorney_firm = EXCLUDED.plaintiff_attorney_firm,
            plaintiff_attorney_norm = EXCLUDED.plaintiff_attorney_norm,
            defendant_attorney_juris_id = EXCLUDED.defendant_attorney_juris_id,
            defendant_attorney_name = EXCLUDED.defendant_attorney_name,
            defendant_attorney_last = EXCLUDED.defendant_attorney_last,
            defendant_attorney_first = EXCLUDED.defendant_attorney_first,
            municipality = EXCLUDED.municipality,
            filing_date = EXCLUDED.filing_date,
            status = EXCLUDED.status,
            address = EXCLUDED.address,
            normalized_address = EXCLUDED.normalized_address,
            disposition_date = EXCLUDED.disposition_date,
            updated_at = CURRENT_TIMESTAMP
    """
    
    # Batch processing for efficiency
    batch_size = 5000
    for i in range(0, len(ingest_data), batch_size):
        batch = ingest_data[i:i+batch_size]
        execute_values(cur, upsert_query, batch)
        conn.commit()
        logger.info(f"  Ingested {min(i+batch_size, len(ingest_data))}/{len(ingest_data)}...")
    
    logger.info("Eviction ingestion complete.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    ingest_evictions()
