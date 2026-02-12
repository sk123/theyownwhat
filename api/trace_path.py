
import psycopg2
import os
import sys
import logging
from collections import deque, defaultdict
from shared_utils import get_email_match_key

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
START_NODE = ('principal', 25129) # Gurevitch
END_NODE = ('principal', 18576)   # Carlos Mouta

# Cache for email keys to avoid re-querying
email_key_cache = {}

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def trace_path_dynamic():
    conn = get_db_connection()
    
    # Load Hub Principals (Match network_builder.py)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT principal_id FROM principal_business_links
            GROUP BY principal_id HAVING COUNT(*) > 250
        """)
        hub_principals = {row[0] for row in cur.fetchall()}
        logger.info(f"Loaded {len(hub_principals)} Hub Principals (>250 links).")

    email_rules = {}
    queue = deque([(START_NODE, [START_NODE])])
    visited = {START_NODE}
    
    logger.info(f"Starting Dynamic BFS (P-B ONLY + HUB FILTERING) from {START_NODE} to {END_NODE}...")
    
    while queue:
        curr, path = queue.popleft()
        node_type, node_id = curr
        
        if curr == END_NODE:
            # ... (keep found path logic)
            print("\n✅ FOUND PATH:")
            for n in path:
                with conn.cursor() as cur:
                    if n[0] == 'principal':
                        cur.execute("SELECT name, name_normalized FROM unique_principals WHERE principal_id = %s", (n[1],))
                    else:
                        cur.execute("SELECT name, business_email_address FROM businesses WHERE id = %s", (n[1],))
                    res = cur.fetchone()
                print(f"  -> {n}: {res}")
            return

        if len(path) > 15: continue # Increased depth for P-B only trace

        neighbors = set()
        
        with conn.cursor() as cur:
            if node_type == 'principal':
                # P-B Links: Find businesses owned by this principal
                cur.execute("SELECT business_id FROM principal_business_links WHERE principal_id = %s", (node_id,))
                for row in cur:
                    neighbors.add(('business', row[0])) # business_id is str
                    
            elif node_type == 'business':
                # P-B Links: Find principals owning this business
                cur.execute("SELECT principal_id FROM principal_business_links WHERE business_id = %s", (node_id,))
                for row in cur:
                    pid = row[0]
                    if pid not in hub_principals:
                        neighbors.add(('principal', pid)) # principal_id is int
                
                # Shared Email: (DISABLED FOR P-B ONLY TRACE)
                pass

        for n in neighbors:
            if n not in visited:
                visited.add(n)
                queue.append((n, path + [n]))
        
        if len(visited) % 100 == 0:
            logger.info(f"Visited {len(visited)} nodes. Queue size: {len(queue)}")
            
    print("❌ No path found via dynamic BFS.")

if __name__ == "__main__":
    trace_path_dynamic()
