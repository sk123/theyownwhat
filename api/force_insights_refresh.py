import os
import sys
import logging
from contextlib import contextmanager

# Add parent directory to path to allow imports from api
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.db import init_db_pool, db_pool
from api.main import _update_insights_cache_sync

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def force_refresh():
    logger.info("🔌 Initializing DB pool...")
    init_db_pool()
    
    # Access the global pool directly after init
    from api.db import db_pool
    
    if db_pool is None:
        logger.error("Failed to initialize DB pool.")
        return

    conn = None
    try:
        conn = db_pool.getconn()
        logger.info("🗑️ Clearing insights cache (truncating kv_cache)...")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE kv_cache")
        conn.commit()
        logger.info("✅ Cache cleared.")
        
        # Put connection back
        db_pool.putconn(conn)
        conn = None
        
        logger.info("🔄 Triggering insight generation...")
        # _update_insights_cache_sync handles getting its own connection from the pool
        _update_insights_cache_sync()
        logger.info("✅ Insight generation complete.")
        
    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        if conn: db_pool.putconn(conn)

if __name__ == "__main__":
    force_refresh()
