import os
import sys
import time
import json
import logging
import requests
import psycopg2
from typing import Dict, Tuple, Optional

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")

# REST endpoints for CAMA tables on maps2.dcgis.dc.gov
CAMA_ENDPOINTS = {
    23: "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/23/query", # COMMERCIAL
    24: "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/24/query", # CONDOMINIUM
    25: "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/25/query", # RESIDENTIAL
}

def clean_ssl(ssl: str) -> str:
    """Collapses spaces to handle slight formatting variations."""
    if not ssl:
        return ""
    return "".join(ssl.upper().split())

def fetch_cama_table(table_id: int, fields: str) -> Dict[str, Tuple[Optional[int], Optional[float]]]:
    """Fetches all records from a CAMA table and maps normalized SSL -> (year_built, units)."""
    url = CAMA_ENDPOINTS[table_id]
    logger.info(f"Fetching CAMA table {table_id}...")
    
    mapping = {}
    chunk_size = 2000
    offset = 0
    
    while True:
        params = {
            'where': 'SSL IS NOT NULL',
            'outFields': fields,
            'resultOffset': offset,
            'resultRecordCount': chunk_size,
            'returnGeometry': 'false',
            'f': 'json'
        }
        
        features = []
        retries = 3
        while retries > 0:
            try:
                r = requests.get(url, params=params, timeout=25)
                r.raise_for_status()
                data = r.json()
                features = data.get('features', [])
                break
            except Exception as e:
                retries -= 1
                if retries == 0:
                    logger.error(f"Failed fetching CAMA chunk at offset {offset}: {e}")
                    break
                logger.warning(f"Error fetching CAMA chunk: {e}. Retrying...")
                time.sleep(2)
                
        if not features:
            break
            
        for f in features:
            attrs = f.get('attributes', {})
            ssl = attrs.get('SSL')
            if not ssl:
                continue
                
            norm_ssl = clean_ssl(ssl)
            
            # Extract year built (AYB)
            ayb = attrs.get('AYB')
            year_built = int(ayb) if ayb and float(ayb) > 1000 else None
            
            # Extract units
            units = None
            if 'NUM_UNITS' in attrs:
                num_units = attrs.get('NUM_UNITS')
                if num_units is not None:
                    units = float(num_units)
            elif table_id == 24: # Condominiums are single units
                units = 1.0
                
            mapping[norm_ssl] = (year_built, units)
            
        offset += chunk_size
        if offset % 10000 == 0:
            logger.info(f"  Processed {offset} records from table {table_id}...")
            
    logger.info(f"Finished table {table_id}. Total records parsed: {len(mapping)}")
    return mapping

def main():
    logger.info("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    
    # 1. Load DC BBLs from database
    bbl_map = {} # norm_bbl -> raw_bbl
    with conn.cursor() as cur:
        cur.execute("SELECT bbl FROM dc_properties")
        for (bbl,) in cur.fetchall():
            if bbl:
                bbl_map[clean_ssl(bbl)] = bbl
                
    logger.info(f"Loaded {len(bbl_map)} DC property BBLs from the database.")
    if not bbl_map:
        logger.error("No DC properties found in database. Exiting.")
        conn.close()
        return

    # 2. Fetch CAMA datasets
    # Table 23 (Commercial): SSL, AYB, NUM_UNITS
    comm_data = fetch_cama_table(23, "SSL,AYB,NUM_UNITS")
    # Table 24 (Condominium): SSL, AYB
    condo_data = fetch_cama_table(24, "SSL,AYB")
    # Table 25 (Residential): SSL, AYB, NUM_UNITS
    res_data = fetch_cama_table(25, "SSL,AYB,NUM_UNITS")
    
    # Merge mappings (Residential takes priority, then Commercial, then Condominium)
    merged_data = {}
    
    # Condo mapping
    for k, v in condo_data.items():
        merged_data[k] = v
    # Commercial mapping
    for k, v in comm_data.items():
        merged_data[k] = v
    # Residential mapping
    for k, v in res_data.items():
        merged_data[k] = v
        
    logger.info(f"Total merged CAMA records: {len(merged_data)}")

    # 3. Match and prepare updates
    updates = []
    matched_count = 0
    
    for norm_bbl, raw_bbl in bbl_map.items():
        if norm_bbl in merged_data:
            year_built, units = merged_data[norm_bbl]
            updates.append((year_built, units, units, raw_bbl))
            matched_count += 1
            
    logger.info(f"Matched {matched_count} / {len(bbl_map)} properties ({matched_count / len(bbl_map) * 100:.2f}%)")

    if not updates:
        logger.warning("No property updates to perform.")
        conn.close()
        return

    # 4. Perform bulk updates
    logger.info("Executing bulk update on dc_properties...")
    with conn.cursor() as cur:
        # Use execute_values for high performance updates
        from psycopg2.extras import execute_values
        query = """
            UPDATE dc_properties
            SET year_built = u.year_built,
                units_res = u.units_res,
                units_total = u.units_total,
                updated_at = NOW()
            FROM (VALUES %s) AS u(year_built, units_res, units_total, bbl)
            WHERE dc_properties.bbl = u.bbl
        """
        # Execute in chunks of 5000
        chunk_size = 5000
        for i in range(0, len(updates), chunk_size):
            chunk = updates[i:i+chunk_size]
            execute_values(cur, query, chunk)
            conn.commit()
            logger.info(f"  Updated {min(i+chunk_size, len(updates))} properties...")
            
    logger.info("Enrichment complete!")
    conn.close()

if __name__ == "__main__":
    main()
