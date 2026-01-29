
import os
import time
import psycopg2
from psycopg2.extras import execute_values
import logging
import sys

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from shared_utils import normalize_business_name

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

def link_properties():
    conn = get_db_connection()
    try:
        logger.info("loading Businesses...")
        b_map = {} # NormName -> ID
        b_map_nospace = {} # NormNameNoSpace -> ID
        
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM businesses")
            for row in cur:
                norm = normalize_business_name(row[1])
                if norm:
                    b_map[norm] = row[0]
                    # Also populate nospace map (careful of collisions, heuristic: last write wins or ignore?)
                    # Heuristic: First one wins? Or keep all?
                    # Let's simple overwrite.
                    ns = norm.replace(" ", "")
                    if ns:
                        b_map_nospace[ns] = row[0]

        logger.info(f"Loaded {len(b_map)} businesses. (Nospace variant: {len(b_map_nospace)} keys)")
        
        logger.info("Fetching Properties...")
        # We fetch all properties to normalize owner and try to link
        # Optimization: Only fetch columns we need.
        # We will update in batches.
        
        read_cursor = conn.cursor()
        read_cursor.execute("SELECT id, owner FROM properties WHERE owner IS NOT NULL")
        all_rows = read_cursor.fetchall()
        read_cursor.close()
        
        logger.info(f"Fetched {len(all_rows)} properties. Processing linkage...")
        
        write_cursor = conn.cursor()
        
        updates = [] 
        
        count = 0
        match_count = 0
        
        # Batch size for updates
        BATCH_SIZE = 10000
        
        for i in range(0, len(all_rows), BATCH_SIZE):
            batch_rows = all_rows[i:i+BATCH_SIZE]
            
            batch_updates = []
            for r in batch_rows:
                pid = r[0]
                owner = r[1]
                norm = normalize_business_name(owner)
                
                bid = b_map.get(norm)
                if not bid and norm:
                    # Try fallback
                    bid = b_map_nospace.get(norm.replace(" ", ""))
                
                if bid:
                    match_count += 1
                
                batch_updates.append((norm, bid, pid))
            
            # Execute Update using write_cursor
            execute_values(write_cursor,
                """UPDATE properties AS p 
                   SET owner_norm = v.owner_norm, business_id = v.business_id 
                   FROM (VALUES %s) AS v(owner_norm, business_id, id) 
                   WHERE p.id = v.id""",
                batch_updates
            )
            conn.commit()
            
            count += len(batch_rows)
            if count % 100000 == 0:
                logger.info(f"Processed {count} properties. Linked {match_count} to businesses.")
                
        logger.info(f"Complete. Processed {count} properties. Total Linked: {match_count}.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    link_properties()
