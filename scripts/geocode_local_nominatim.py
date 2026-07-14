import os
import time
import requests
import psycopg2
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from api.geocoding_utils import is_geocode_match_credible, is_valid_coordinate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("local_nominatim_geocoder")

DATABASE_URL = os.environ.get("DATABASE_URL")
NOMINATIM_LOCAL_URL = "http://ctdata_nominatim:8080/search"

# We can blast the local nominatim instance pretty heavily
MAX_WORKERS = 20
BATCH_SIZE = 1000
GEOCODE_FAILED_MARKER = "GEOCODE_FAILED"

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def geocode_local(prop):
    prop_id, location, city, zip_code = prop
    
    if not location: return (prop_id, None, None, None)
    
    loc = str(location).strip().upper()
    if loc.startswith('0 ') or loc.startswith('00 '):
        return (prop_id, None, None, None)
    
    # Handle ranged
    import re
    range_match = re.match(r'^(\d+)-\d+[^\s]*\s+(.*)', loc)
    if range_match: loc = f"{range_match.group(1)} {range_match.group(2)}"
    
    clean_zip = ""
    if zip_code:
        try:
            clean_zip = str(int(float(str(zip_code).strip()))).zfill(5)
        except:
            clean_zip = str(zip_code).strip()
            
    full_address = f"{loc}, {city or ''}, CT {clean_zip}".strip()
    
    try:
        params = {"q": full_address, "format": "json", "limit": 1}
        resp = requests.get(NOMINATIM_LOCAL_URL, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if data:
            lat, lon = float(data[0]['lat']), float(data[0]['lon'])
            norm = data[0].get('display_name')
            if is_valid_coordinate(lat, lon, "CT") and is_geocode_match_credible(full_address, norm):
                return (prop_id, lat, lon, norm)
            logger.warning(f"Rejected geocode for property {prop_id}: {lat}, {lon}; matched={norm!r}; input={full_address!r}")
            
        # Fallback without zip
        addr_no_zip = re.sub(r'\s\d{5}$', '', full_address)
        if addr_no_zip != full_address:
            params = {"q": addr_no_zip, "format": "json", "limit": 1}
            resp = requests.get(NOMINATIM_LOCAL_URL, params=params, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if data:
                lat, lon = float(data[0]['lat']), float(data[0]['lon'])
                norm = data[0].get('display_name')
                if is_valid_coordinate(lat, lon, "CT") and is_geocode_match_credible(addr_no_zip, norm):
                    return (prop_id, lat, lon, norm)
                logger.warning(f"Rejected fallback geocode for property {prop_id}: {lat}, {lon}; matched={norm!r}; input={addr_no_zip!r}")
                
        return (prop_id, None, None, None)
    except Exception as e:
        logger.warning(f"Error for {full_address}: {e}")
        return (prop_id, None, None, None)

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Bulk geocode CT properties with local Nominatim")
    parser.add_argument("--max-batches", type=int, default=None, help="Process this many batches, then exit")
    parser.add_argument("--reprocess-failures", action="store_true", help="Retry GEOCODE_FAILED rows")
    args = parser.parse_args()

    conn = get_db_connection()
    cur = conn.cursor()
    
    marker_filter = "" if args.reprocess_failures else "AND COALESCE(normalized_address, '') <> %s"
    count_params = () if args.reprocess_failures else (GEOCODE_FAILED_MARKER,)
    cur.execute(f"""
        SELECT count(*)
        FROM properties
        WHERE (latitude IS NULL OR longitude IS NULL OR latitude = 0 OR latitude = -1)
        AND (source IS NULL OR source != 'NYS_OPEN_DATA')
        AND location IS NOT NULL AND location != ''
        {marker_filter}
    """, count_params)
    total = cur.fetchone()[0]
    logger.info(f"Geocoding {total} properties with local Nominatim")
    
    processed = 0
    batches = 0
    while True:
        if args.max_batches is not None and batches >= args.max_batches:
            logger.info(f"Reached max_batches={args.max_batches}. Exiting.")
            break

        params = () if args.reprocess_failures else (GEOCODE_FAILED_MARKER,)
        cur.execute(f"""
            SELECT id, location, property_city, property_zip 
            FROM properties 
            WHERE (latitude IS NULL OR longitude IS NULL OR latitude = 0 OR latitude = -1)
            AND (source IS NULL OR source != 'NYS_OPEN_DATA')
            AND location IS NOT NULL AND location != ''
            {marker_filter}
            ORDER BY id DESC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """, (*params, BATCH_SIZE))
        rows = cur.fetchall()
        
        if not rows: break
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(geocode_local, row) for row in rows]
            for future in as_completed(futures):
                results.append(future.result())
                
        success = 0
        from psycopg2.extras import execute_batch
        
        update_success = []
        update_fail = []
        
        for pid, lat, lon, norm in results:
            if is_valid_coordinate(lat, lon, "CT"):
                update_success.append((lat, lon, norm, pid))
                success += 1
            else:
                update_fail.append((pid,))
                
        if update_success:
            execute_batch(cur, "UPDATE properties SET latitude = %s, longitude = %s, normalized_address = %s WHERE id = %s", update_success)
        
        if update_fail:
            execute_batch(cur, "UPDATE properties SET latitude = NULL, longitude = NULL, normalized_address = %s WHERE id = %s", [(GEOCODE_FAILED_MARKER, pid) for (pid,) in update_fail])

        conn.commit()
        processed += len(rows)
        batches += 1
        logger.info(f"Batch ({len(rows)}): {success} successful. Total processed: {processed}/{total}")

    conn.close()
    logger.info("Local Nominatim bulk geocoding complete.")

if __name__ == '__main__':
    main()
