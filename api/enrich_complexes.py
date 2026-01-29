import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def get_complexes(min_units=5):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT location as street_address, property_city as city, COUNT(*) as units 
                FROM properties 
                GROUP BY location, property_city 
                HAVING COUNT(*) >= %s
                ORDER BY units DESC
            """, (min_units,))
            return cur.fetchall()
    finally:
        conn.close()

def save_management_info(street_address, city, management_name, official_url, phone=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO complex_management (street_address, city, management_name, official_url, phone, last_updated)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (street_address, city) DO UPDATE SET
                    management_name = EXCLUDED.management_name,
                    official_url = EXCLUDED.official_url,
                    phone = COALESCE(EXCLUDED.phone, complex_management.phone),
                    last_updated = CURRENT_TIMESTAMP
            """, (street_address, city, management_name, official_url, phone))
            conn.commit()
    finally:
        conn.close()

import concurrent.futures

def enrich_complex(comp):
    street_address = comp['street_address']
    city = comp['city']
    query = f"{street_address} {city} CT property management leasing official website"
    logger.info(f"Processing: {street_address}, {city}")
    # In the actual agentic flow, we use the search_web tool externally.
    # This script serves as the 'logic' and 'persistence' hook.
    return comp

def run_enrichment(min_units=10, max_workers=5):
    complexes = get_complexes(min_units=min_units)
    logger.info(f"Enriching {len(complexes)} complexes...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(enrich_complex, complexes)

if __name__ == "__main__":
    run_enrichment()
