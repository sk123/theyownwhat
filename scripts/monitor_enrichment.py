import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def monitor_enrichment():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Focus on key municipalities
            target_cities = ['HARTFORD', 'BRIDGEPORT', 'NEW HAVEN', 'STAMFORD', 'NEWTOWN', 'NEW BRITAIN']
            
            logger.info("=== Property Enrichment Status Report ===")
            logger.info(f"{'City':<15} | {'Total':<7} | {'Photos':<7} | {'CAMA':<7} | {'Units %':<7}")
            logger.info("-" * 60)
            
            for city in target_cities:
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(building_photo) FILTER (WHERE building_photo IS NOT NULL AND building_photo != '') as with_photos,
                        COUNT(cama_site_link) FILTER (WHERE cama_site_link IS NOT NULL AND cama_site_link != '') as with_cama,
                        COUNT(number_of_units) FILTER (WHERE number_of_units IS NOT NULL) as with_units
                    FROM properties 
                    WHERE property_city ILIKE %s
                """, (city,))
                row = cur.fetchone()
                
                total = row['total']
                if total == 0:
                    logger.info(f"{city:<15} | No properties found")
                    continue
                    
                photo_pct = (row['with_photos'] / total) * 100
                cama_pct = (row['with_cama'] / total) * 100
                unit_pct = (row['with_units'] / total) * 100
                
                logger.info(f"{city:<15} | {total:<7} | {row['with_photos']:<7} | {row['with_cama']:<7} | {unit_pct:>6.1f}%")
                
            logger.info("-" * 60)
            
    finally:
        conn.close()

if __name__ == "__main__":
    monitor_enrichment()
