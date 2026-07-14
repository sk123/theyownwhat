#!/usr/bin/env python3
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values
import requests
import logging
import re
from email.utils import parsedate_to_datetime
from io import BytesIO

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
DEFAULT_EVICTIONS_URL = "https://evictions.ctfairhousing.com/data/evictionlab/ct_evictions.csv"
EVICTION_LANDING_URLS = {
    "https://ctfairhousing.com/data/evictionlab",
    "https://ctfairhousing.com/metabase/evictionlab",
    "https://data.ctfairhousing.com/metabase/evictionlab",
}

def resolve_evictions_url():
    raw = (os.environ.get("EVICTION_DATA_URL") or DEFAULT_EVICTIONS_URL).strip().strip('"')
    normalized = raw.rstrip("/")
    if normalized in EVICTION_LANDING_URLS:
        logger.info("Resolved CT Fair Housing Metabase landing page to raw CT Judicial CSV feed.")
        return DEFAULT_EVICTIONS_URL
    return raw

EVICTIONS_URL = resolve_evictions_url()

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def parse_http_date(value):
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).date()
    except Exception:
        return None

def clean_source_text(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.upper() in {"NAN", "NULL", "\\N"}:
        return ""
    return text

def none_if_blank(value):
    text = clean_source_text(value)
    return text or None

def download_eviction_csv(url):
    response = requests.get(
        url,
        headers={"Accept": "text/csv,text/plain;q=0.9,*/*;q=0.1"},
        timeout=(20, 300),
    )
    response.raise_for_status()
    external_last_updated = parse_http_date(response.headers.get("Last-Modified"))

    sample = response.content[:4096].lstrip().lower()
    content_type = (response.headers.get("Content-Type") or "").lower()
    if sample.startswith(b"<") or "text/html" in content_type:
        raise ValueError(
            "Eviction data URL returned HTML instead of a CSV export. "
            f"Use the raw feed URL: {DEFAULT_EVICTIONS_URL}"
        )
    if not response.content:
        raise ValueError("Eviction data URL returned an empty response.")

    # Based on source inspection: semicolon-delimited, quoted fields, no header row.
    df = pd.read_csv(BytesIO(response.content), sep=';', quotechar='"', header=None, low_memory=False)
    return df, external_last_updated

def ensure_schema(conn):
    schema_path = os.path.join(os.path.dirname(__file__), "setup_evictions_table.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def update_status(conn, status, details, external_last_updated=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO data_source_status
                (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
            VALUES ('CT_EVICTIONS', 'court_evictions', %s, NOW(), %s, %s)
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
        raise RuntimeError("No eviction feed URL configured.")
    logger.info(f"Downloading eviction data from {EVICTIONS_URL}...")
    try:
        df, external_last_updated = download_eviction_csv(EVICTIONS_URL)
    except Exception as e:
        logger.exception(f"Failed to download or parse eviction CSV: {e}")
        raise

    logger.info(f"Processing {len(df)} eviction records...")
    
    # Mapping based on inspection:
    # 5: Case Number
    # 6: Official CT Judicial case-detail URL
    # 7: Official CT Judicial document URL
    # 10: Case Type
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
    df_clean['case_number'] = df[5].apply(clean_source_text)
    df_clean['case_detail_url'] = df[6].apply(clean_source_text)
    df_clean['document_url'] = df[7].apply(clean_source_text)
    df_clean['case_type'] = df[10].apply(clean_source_text)
    df_clean['plaintiff_name'] = df[11].apply(clean_source_text)
    df_clean['filing_date'] = pd.to_datetime(df[17], errors='coerce').dt.date
    df_clean['address'] = df[20].apply(clean_source_text)
    df_clean['unit'] = df[21].apply(clean_source_text)
    df_clean['town'] = df[22].apply(clean_source_text)
    df_clean['status'] = df[26].apply(clean_source_text)
    df_clean['disposition_date'] = pd.to_datetime(df[29], errors='coerce').dt.date
    df_clean['plaintiff_attorney_juris_id'] = df[30].apply(clean_source_text)
    df_clean['plaintiff_attorney_name'] = df[31].apply(clean_source_text)
    df_clean['plaintiff_attorney_firm'] = df[32].apply(clean_source_text)
    df_clean['defendant_attorney_juris_id'] = df[34].apply(clean_source_text)
    df_clean['defendant_attorney_name'] = df[35].apply(clean_source_text)
    df_clean['defendant_attorney_last'] = df[36].apply(clean_source_text)
    df_clean['defendant_attorney_first'] = df[37].apply(clean_source_text)
    
    # Strictly filter out empty case numbers
    df_clean = df_clean[df_clean['case_number'] != ""]
    
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
    ensure_schema(conn)
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
            none_if_blank(row['case_detail_url']),
            none_if_blank(row['document_url']),
            none_if_blank(row['case_type']),
            prop_id,
            none_if_blank(row['plaintiff_name']),
            none_if_blank(row['plaintiff_norm']),
            none_if_blank(row['plaintiff_attorney_juris_id']),
            none_if_blank(row['plaintiff_attorney_name']),
            none_if_blank(row['plaintiff_attorney_firm']),
            none_if_blank(row['plaintiff_attorney_norm']),
            none_if_blank(row['defendant_attorney_juris_id']),
            none_if_blank(row['defendant_attorney_name']),
            none_if_blank(row['defendant_attorney_last']),
            none_if_blank(row['defendant_attorney_first']),
            none_if_blank(row['town']),
            row['filing_date'],
            none_if_blank(row['status']),
            none_if_blank(row['address']),
            none_if_blank(row['norm_addr']),
            row['disposition_date'] if pd.notna(row['disposition_date']) else None
        ))

    # 3. Upsert into database
    logger.info(f"Upserting {len(ingest_data)} records into evictions...")
    upsert_query = """
        INSERT INTO evictions (
            case_number, case_detail_url, document_url, case_type,
            property_id, plaintiff_name, plaintiff_norm,
            plaintiff_attorney_juris_id, plaintiff_attorney_name, plaintiff_attorney_firm, plaintiff_attorney_norm,
            defendant_attorney_juris_id, defendant_attorney_name, defendant_attorney_last, defendant_attorney_first,
            municipality, filing_date, status, address, normalized_address, disposition_date
        ) VALUES %s
        ON CONFLICT (case_number) DO UPDATE SET
            case_detail_url = EXCLUDED.case_detail_url,
            document_url = EXCLUDED.document_url,
            case_type = EXCLUDED.case_type,
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
        if batch:
            execute_values(cur, upsert_query, batch)
        conn.commit()
        logger.info(f"  Ingested {min(i+batch_size, len(ingest_data))}/{len(ingest_data)}...")

    if ingest_data:
        current_cases = [(row[0],) for row in ingest_data if row[0]]
        if not current_cases:
            raise RuntimeError("Official eviction feed did not include any case numbers; refusing snapshot cleanup.")
        cur.execute("""
            CREATE TEMP TABLE ct_current_eviction_cases (
                case_number TEXT PRIMARY KEY
            ) ON COMMIT DROP
        """)
        for i in range(0, len(current_cases), batch_size):
            execute_values(
                cur,
                "INSERT INTO ct_current_eviction_cases (case_number) VALUES %s ON CONFLICT DO NOTHING",
                current_cases[i:i+batch_size],
            )
        cur.execute("""
            DELETE FROM evictions e
            WHERE NOT EXISTS (
                SELECT 1
                FROM ct_current_eviction_cases current_cases
                WHERE current_cases.case_number = e.case_number
            )
        """)
        deleted_stale = cur.rowcount
        conn.commit()
    else:
        deleted_stale = 0
    
    linked_count = sum(1 for row in ingest_data if row[4] is not None)
    filing_dates = [d for d in df_clean['filing_date'].tolist() if pd.notna(d)]
    update_status(conn, "success", {
        "source_url": EVICTIONS_URL,
        "source_records": len(ingest_data),
        "matched_records": linked_count,
        "records_without_local_property": len(ingest_data) - linked_count,
        "removed_records_not_in_current_source": deleted_stale,
        "min_filing_date": str(min(filing_dates)) if filing_dates else None,
        "max_filing_date": str(max(filing_dates)) if filing_dates else None,
    }, external_last_updated)

    logger.info("Eviction ingestion complete.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    ingest_evictions()
