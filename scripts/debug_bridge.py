
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    db_url = os.environ.get("DATABASE_URL")
    return psycopg2.connect(db_url)

def find_bridge():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Get Business IDs for both
            logger.info("Fetching Business IDs...")
            cur.execute("SELECT id, name FROM businesses WHERE name ILIKE '%YANKEE GAS%'")
            yankee_bids = {r['id']: r['name'] for r in cur.fetchall()}
            
            cur.execute("SELECT id, name FROM businesses WHERE name ILIKE '%BRIDGEPORT HOSPITAL%'")
            bh_bids = {r['id']: r['name'] for r in cur.fetchall()}
            
            logger.info(f"Found {len(yankee_bids)} Yankee businesses and {len(bh_bids)} Bridgeport Hospital businesses.")
            
            if not yankee_bids or not bh_bids:
                logger.warning("One or both groups have no businesses found.")
                # Try principals
            
            # 2. Get Principals for both groups via links
            logger.info("Fetching Principals via links...")
            
            y_pids = set()
            if yankee_bids:
                cur.execute("SELECT principal_id FROM principal_business_links WHERE business_id IN %s", (tuple(yankee_bids.keys()),))
                y_pids = {r['principal_id'] for r in cur.fetchall()}
                
            b_pids = set()
            if bh_bids:
                cur.execute("SELECT principal_id FROM principal_business_links WHERE business_id IN %s", (tuple(bh_bids.keys()),))
                b_pids = {r['principal_id'] for r in cur.fetchall()}
                
            logger.info(f"Yankee has {len(y_pids)} principals. Bridgeport has {len(b_pids)} principals.")
            
            # 3. Intersect
            common_pids = y_pids.intersection(b_pids)
            if common_pids:
                logger.info(f"⚠️ FOUND {len(common_pids)} COMMON PRINCIPALS!")
                cur.execute("SELECT id, name_c FROM principals WHERE id IN %s", (tuple(common_pids),))
                for r in cur.fetchall():
                    logger.info(f"  - BRIDGE PRINCIPAL: {r['name_c']} (ID: {r['id']})")
            else:
                logger.info("No common principals found via business links.")
                
            # 4. Check for Shared Business (if multiple variants)
            # Unlikely, but maybe they share a parent company?
            
            # 5. Check Property-level links (direct ownership)
            # This covers cases where Principal owns Property A (Yankee) and Property B (Bridgeport)
            # But the previous step covered "Principals linked to Businesses".
            # What if "Yankee Gas" IS A PRINCIPAL?
            
            logger.info("Checking if they exist as Principals...")
            cur.execute("SELECT id, name_c FROM principals WHERE name_c ILIKE '%YANKEE GAS%'")
            yankee_prins = {r['id']: r['name_c'] for r in cur.fetchall()}
            
            cur.execute("SELECT id, name_c FROM principals WHERE name_c ILIKE '%BRIDGEPORT HOSPITAL%'")
            bh_prins = {r['id']: r['name_c'] for r in cur.fetchall()}
            
            logger.info(f"Found {len(yankee_prins)} Yankee principals and {len(bh_prins)} Bridgeport principals.")
            
            # Check if any principal in (yankee_prins) is also in (bh_prins)? No, names are different.
            
            # Check if any property has Yankee Principal AND Bridgeport Principal? (Co-owners)
            # Or Yankee Business AND Bridgeport Principal?
            
            # 6. Check properties with mixed ownership
            logger.info("Checking mixed ownership on properties...")
            # This is hard to do without full scan.
            # But if the network merges, it's usually via a specific node.
            
            pass

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    find_bridge()
