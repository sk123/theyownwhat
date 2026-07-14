
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    db_url = os.environ.get("DATABASE_URL")
    return psycopg2.connect(db_url)

def debug_props():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Yankee Gas Properties Principal IDs
            logger.info("Fetching Yankee Property Principals...")
            cur.execute("SELECT DISTINCT principal_id FROM properties WHERE owner ILIKE '%YANKEE GAS%' AND principal_id IS NOT NULL")
            y_pids = {r['principal_id'] for r in cur.fetchall()}
            
            # 2. Bridgeport Hospital Properties Principal IDs
            logger.info("Fetching Bridgeport Property Principals...")
            cur.execute("SELECT DISTINCT principal_id FROM properties WHERE owner ILIKE '%BRIDGEPORT HOSPITAL%' AND principal_id IS NOT NULL")
            b_pids = {r['principal_id'] for r in cur.fetchall()}
            
            logger.info(f"Yankee Props have {len(y_pids)} principals. Bridgeport Props have {len(b_pids)} principals.")
            
            # 3. Intersect
            common = y_pids.intersection(b_pids)
            if common:
                logger.info(f"⚠️ FOUND {len(common)} COMMON PRINCIPALS ON PROPERTIES!")
                cur.execute("SELECT id, name, name_c FROM principals WHERE id IN %s", (tuple(common),))
                for r in cur.fetchall():
                    logger.info(f"  - BRIDGE PRINCIPAL: {r['name_c'] or r['name']} (ID: {r['id']})")
            else:
                logger.info("No common principals found on properties.")

            # 4. Check Ahron Rudich Unit Counts
            logger.info("--- Checking Ahron Rudich Data ---")
            cur.execute("SELECT id FROM principals WHERE name_c ILIKE '%AHRON RUDICH%'")
            ar_rows = cur.fetchall()
            if not ar_rows:
                logger.warning("Ahron Rudich Principal NOT FOUND.")
            else:
                ar_id = ar_rows[0]['id']
                logger.info(f"Ahron Rudich ID: {ar_id}")
                cur.execute("""
                    SELECT count(*) as count, sum(number_of_units) as total_units, 
                           sum(case when number_of_units = 0 then 1 else 0 end) as zero_units
                    FROM properties 
                    WHERE principal_id = %s OR principal_id = %s
                """, (ar_id, str(ar_id))) # Try integer and string just in case
                stats = cur.fetchone()
                logger.info(f"Ahron Rudich Props: {stats['count']}, Total Units: {stats['total_units']}, Zero Units: {stats['zero_units']}")
                
                # Check a few examples
                cur.execute("""
                    SELECT id, property_city, address_number, street_name, number_of_units, property_type
                    FROM properties
                    WHERE principal_id = %s OR principal_id = %s
                    LIMIT 5
                """, (ar_id, str(ar_id)))
                for r in cur.fetchall():
                    logger.info(f"  - Prop: {r['address_number']} {r['street_name']}, {r['property_city']} - Units: {r['number_of_units']} (Type: {r['property_type']})")

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    debug_props()
