import os
import time
import requests
import psycopg2
from psycopg2 import pool
import logging
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("background-geocoder")

DATABASE_URL = os.environ.get("DATABASE_URL")
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
USER_AGENT = "TheyOwnWhatApp/1.0"
RATE_LIMIT_DELAY = 1.1  # Seconds (Nominatim requires 1 sec)
BATCH_SIZE = 500
MAX_WORKERS = 5

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


from api.geocoding_utils import geocode_census, geocode_nominatim

def process_property(prop):
    if stop_signal: return None
    
    prop_id, location, city, zip_code = prop
    full_address = f"{location}, {city or ''}, CT {zip_code or ''}".strip()
    
    # Try Census First
    lat, lon = geocode_census(full_address)
    
    # Fallback to Nominatim
    if not lat:
        lat, lon = geocode_nominatim(full_address)
        
    return (prop_id, lat, lon)


def main():
    logger.info(f"Starting Optimized Background Geocoder (Workers: {MAX_WORKERS})...")
    
    conn = None
    while not conn and not stop_signal:
        conn = get_db_connection()
        if not conn:
            time.sleep(5)
    
    if stop_signal: return

    logger.info("Connected to DB.")
    
    while not stop_signal:
        try:
            with conn.cursor() as cur:
                # Batch Fetch
                cur.execute(f"""
                    SELECT id, location, property_city, property_zip 
                    FROM properties 
                    WHERE latitude IS NULL AND location IS NOT NULL AND location != ''
                    ORDER BY id DESC
                    LIMIT {BATCH_SIZE}
                    FOR UPDATE SKIP LOCKED
                """)
                rows = cur.fetchall()
                
                if not rows:
                    logger.info("No properties pending geocoding. Sleeping 10s...")
                    time.sleep(10)
                    continue
                
                logger.info(f"Processing batch of {len(rows)} properties...")
                
                results = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(process_property, row) for row in rows]
                    
                    for future in as_completed(futures):
                        try:
                            res = future.result()
                            if res:
                                results.append(res)
                        except Exception as e:
                            logger.error(f"Worker Error: {e}")

                # Batch Update
                success_count = 0
                for pid, lat, lon in results:
                    if lat and lon:
                        cur.execute("""
                            UPDATE properties 
                            SET latitude = %s, longitude = %s 
                            WHERE id = %s
                        """, (lat, lon, pid))
                        success_count += 1
                    else:
                        # Fail marker
                        cur.execute("""
                            UPDATE properties 
                            SET latitude = 0, longitude = 0 
                            WHERE id = %s
                        """, (pid,))
                
                conn.commit()
                logger.info(f"Batch Complete: {success_count}/{len(rows)} resolved.")
                
        except Exception as e:
            logger.error(f"Loop error: {e}")
            if conn:
                conn.rollback()
            time.sleep(5)
            if isinstance(e, psycopg2.OperationalError):
                conn = get_db_connection()

    if conn: conn.close()
    logger.info("Exiting...")

if __name__ == "__main__":
    main()
