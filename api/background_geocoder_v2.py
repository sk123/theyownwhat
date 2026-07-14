
import os
import time
import requests
import psycopg2
from psycopg2 import pool
import logging
import signal
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the parent directory to sys.path so we can import 'api.geocoding_utils'
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from api.geocoding_utils import geocode_census, geocode_nominatim, is_geocode_match_credible, is_valid_coordinate

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("background-geocoder-v2")

DATABASE_URL = os.environ.get("DATABASE_URL")
BATCH_SIZE = 500
MAX_WORKERS = 10 # Faster for Census
GEOCODE_FAILED_MARKER = "GEOCODE_FAILED"

stop_signal = False

def handle_sigterm(*args):
    global stop_signal
    stop_signal = True
    logger.info("SIGTERM received. Shutting down gracefully...")

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"DB Connection failed: {e}")
        return None

def clean_address_for_geocoding(location, city, zip_code):
    """
    Applies logic to handle problematic address patterns:
    1. Strips leading '0 ' (e.g. '0 MELBA ST' -> 'MELBA ST')
    2. Handles ranges (e.g. '1-15 EDGEWATER' -> '1 EDGEWATER')
    3. Normalizes zip codes (removes '.0')
    """
    if not location:
        return None

    loc = str(location).strip().upper()
    
    # Leading-zero municipal placeholders do not identify a real street number.
    if re.match(r'^0+\s+', loc):
        return None
    
    # 2. Handle range addresses like '1-15 EDGEWATER' or '1-15 A EDGEWATER'
    # We take the first number in the range
    # Regex: Start of string, digits, hyphen, digits, (any non-space chars), space, rest of string
    range_match = re.match(r'^(\d+)-\d+[^\s]*\s+(.*)', loc)
    if range_match:
        loc = f"{range_match.group(1)} {range_match.group(2)}"

    # 3. Clean zip code
    clean_zip = ""
    if zip_code:
        try:
            clean_zip = str(int(float(str(zip_code).strip())))
            if len(clean_zip) < 5:
                clean_zip = clean_zip.zfill(5)
        except:
            clean_zip = str(zip_code).strip()
            
    DEFAULT_STATE = os.environ.get("DEFAULT_STATE", "CT")
    full_address = f"{loc}, {city or ''}, {DEFAULT_STATE} {clean_zip}".strip()
    return full_address

def process_property(prop):
    if stop_signal: return None
    
    prop_id, location, city, zip_code = prop
    
    full_address = clean_address_for_geocoding(location, city, zip_code)
    if not full_address:
        return (prop_id, None, None, None)

    def usable(candidate_lat, candidate_lon, candidate_norm, input_address):
        return (
            is_valid_coordinate(candidate_lat, candidate_lon, "CT")
            and is_geocode_match_credible(input_address, candidate_norm)
        )
    
    # Try Census First
    lat, lon, norm = geocode_census(full_address)
    if lat and lon and not usable(lat, lon, norm, full_address):
        logger.info(f"Rejected Census match for property {prop_id}; trying Nominatim. matched={norm!r}; input={full_address!r}")
        lat, lon, norm = None, None, None
    
    # Fallback to Nominatim
    if not lat:
        lat, lon, norm = geocode_nominatim(full_address)
        if lat and lon and not usable(lat, lon, norm, full_address):
            logger.info(f"Rejected Nominatim match for property {prop_id}; trying without ZIP. matched={norm!r}; input={full_address!r}")
            lat, lon, norm = None, None, None
    
    # Second Fallback: Try without zip if it failed
    if not lat:
        addr_no_zip = re.sub(r'\s\d{5}$', '', full_address)
        if addr_no_zip != full_address:
            lat, lon, norm = geocode_nominatim(addr_no_zip)
            if lat and lon and not usable(lat, lon, norm, addr_no_zip):
                logger.info(f"Rejected no-ZIP Nominatim match for property {prop_id}. matched={norm!r}; input={addr_no_zip!r}")
                lat, lon, norm = None, None, None
        
    if usable(lat, lon, norm, full_address):
        return (prop_id, lat, lon, norm)

    if lat or lon:
        logger.warning(f"Rejected geocode for property {prop_id}: {lat}, {lon}; matched={norm!r}; input={full_address!r}")

    return (prop_id, None, None, None)

def run_geocoder(reprocess_failures=False, max_batches=None):
    logger.info(f"Starting Geocoder V2 (Reprocess={reprocess_failures})...")
    
    conn = get_db_connection()
    if not conn: return

    batches_processed = 0

    while not stop_signal:
        if max_batches is not None and batches_processed >= max_batches:
            logger.info(f"Reached max_batches={max_batches}. Exiting.")
            break

        try:
            with conn.cursor() as cur:
                # Select based on mode
                if reprocess_failures:
                    # Target old fake-coordinate failure markers.
                    where_clause = """
                        (
                            (latitude = 0 AND longitude = 0)
                            OR (latitude = -1 AND longitude = -1)
                            OR normalized_address = %s
                        )
                    """
                    params = (GEOCODE_FAILED_MARKER,)
                else:
                    # Target NULLs (unprocessed), but skip already attempted failures.
                    where_clause = """
                        (latitude IS NULL OR longitude IS NULL)
                        AND COALESCE(normalized_address, '') <> %s
                    """
                    params = (GEOCODE_FAILED_MARKER,)
                
                cur.execute(f"""
                    SELECT id, location, property_city, property_zip 
                    FROM properties 
                    WHERE {where_clause} 
                    AND (source IS NULL OR source != 'NYS_OPEN_DATA')
                    AND location IS NOT NULL AND location != ''
                    ORDER BY id DESC
                    LIMIT {BATCH_SIZE}
                    FOR UPDATE SKIP LOCKED
                """, params)
                rows = cur.fetchall()
                
                if not rows:
                    if reprocess_failures:
                        logger.info("No more failures to reprocess. Switching to normal mode...")
                        reprocess_failures = False
                        continue
                    else:
                        logger.info("No properties pending geocoding. Sleeping 30s...")
                        time.sleep(30)
                        continue
                
                logger.info(f"Processing batch of {len(rows)} properties...")
                
                results = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(process_property, row) for row in rows]
                    for i, future in enumerate(as_completed(futures)):
                        try:
                            res = future.result()
                            if res:
                                results.append(res)
                            if i % 50 == 0 and i > 0:
                                logger.info(f"  -> {i}/{len(rows)} processed in batch")
                        except Exception as e:
                            logger.error(f"Worker Error: {e}")

                # Batch Update
                success_count = 0
                for pid, lat, lon, norm_addr in results:
                    if is_valid_coordinate(lat, lon, "CT"):
                        cur.execute("""
                            UPDATE properties 
                            SET latitude = %s, longitude = %s, normalized_address = %s
                            WHERE id = %s
                        """, (lat, lon, norm_addr, pid))
                        success_count += 1
                    else:
                        # Audit marker only. Coordinates stay NULL so map code never sees fake points.
                        cur.execute("""
                            UPDATE properties 
                            SET latitude = NULL, longitude = NULL, normalized_address = %s
                            WHERE id = %s
                        """, (GEOCODE_FAILED_MARKER, pid))
                
                conn.commit()
                batches_processed += 1
                logger.info(f"Batch Complete: {success_count}/{len(rows)} resolved.")
                
        except Exception as e:
            logger.error(f"Loop error: {e}")
            if conn: conn.rollback()
            time.sleep(10)
            if isinstance(e, psycopg2.OperationalError):
                conn = get_db_connection()

    if conn: conn.close()
    logger.info("Exiting...")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--reprocess", action="store_true", help="Reprocess known failures (old fake coordinates or GEOCODE_FAILED)")
    parser.add_argument("--max-batches", type=int, default=None, help="Process this many batches, then exit")
    parser.add_argument("--once", action="store_true", help="Process one batch, then exit")
    args = parser.parse_args()
    
    run_geocoder(reprocess_failures=args.reprocess, max_batches=1 if args.once else args.max_batches)
