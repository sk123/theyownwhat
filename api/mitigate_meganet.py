
import os
import sys
import time
import psycopg2
import logging
from safe_network_refresh import run_refresh
from find_path_network4 import find_path_network4

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def wait_for_deduplication():
    """Waits for deduplicate_principals.py to finish by checking pg_stat_activity."""
    logger.info("⏳ Waiting for deduplicate_principals.py to finish...")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        while True:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%deduplicate_principals.py%' AND state != 'idle'")
                count = cur.fetchone()[0]
                
                # Also check for the INSERT query specifically
                cur.execute("SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%INSERT INTO principal_business_links%' AND state = 'active'")
                insert_count = cur.fetchone()[0]
                
                if count == 0 and insert_count == 0:
                    logger.info("✅ Deduplication appears to be complete (no active queries).")
                    break
                
                logger.info(f"   - Deduplication still running (ProcCount={count}, InsertCount={insert_count})...")
            time.sleep(10)
    finally:
        conn.close()

def main():
    logger.info("🚀 Starting Meganet Mitigation Orchestration")
    
    # 1. Wait for current process
    wait_for_deduplication()
    
    # 2. Run Refresh (Full)
    logger.info("🔄 Running Full Network Refresh...")
    success = run_refresh(dry_run=False, skip_linking=False)
    if not success:
        logger.error("❌ Network Refresh Failed!")
        sys.exit(1)
        
    # 3. Verify
    logger.info("🕵️ Verifying Fix with Path Analysis...")
    # Note: IDs might have changed due to deduplication re-run.
    # We need to find the new IDs for "Menachem Gurevitch" and "Alex Vigliotti".
    
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT principal_id FROM unique_principals WHERE name_normalized = 'MENACHEM GUREVITCH' LIMIT 1")
            res = cur.fetchone()
            g_id = res[0] if res else None
            
            cur.execute("SELECT principal_id FROM unique_principals WHERE name_normalized = 'ALEX VIGLIOTTI' LIMIT 1")
            res = cur.fetchone()
            v_id = res[0] if res else None
            
            if not g_id or not v_id:
                logger.error(f"❌ Could not find principals! Gurevitch={g_id}, Vigliotti={v_id}")
                return
            
            logger.info(f"   - Gurevitch ID: {g_id}")
            logger.info(f"   - Vigliotti ID: {v_id}")
            
            # Run pathfinding
            find_path_network4(g_id=g_id, v_id=v_id)
            
    finally:
        conn.close()

if __name__ == "__main__":
    main()
