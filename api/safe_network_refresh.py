
import os
import sys
import psycopg2
import logging
from psycopg2.extras import RealDictCursor

# Add current directory to path so we can import discover_networks
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from discover_networks import get_db_connection, build_graph_from_owners, discover_networks, store_networks, update_network_statistics, setup_network_schema, build_address_edges

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def setup_shadow_tables(conn):
    """Creates shadow tables for safe writing."""
    logger.info("Setting up shadow tables...")
    with conn.cursor() as cursor:
        # Create shadow tables with identical schema to live tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS networks_shadow (
                id SERIAL PRIMARY KEY, primary_name TEXT, total_properties INTEGER DEFAULT 0,
                total_assessed_value NUMERIC DEFAULT 0, business_count INTEGER DEFAULT 0,
                principal_count INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                network_size TEXT, updated_at TIMESTAMP
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entity_networks_shadow (
                network_id INTEGER,
                entity_type TEXT NOT NULL CHECK (entity_type IN ('business', 'principal')),
                entity_id TEXT NOT NULL, entity_name TEXT NOT NULL, normalized_name TEXT,
                PRIMARY KEY (network_id, entity_type, entity_id)
            );
        """)
        # Create indexes on shadow tables for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entity_networks_shadow_network ON entity_networks_shadow(network_id);")
        # Truncate to ensure they are empty for the new run
        cursor.execute("TRUNCATE networks_shadow, entity_networks_shadow RESTART IDENTITY;")
    conn.commit()
    logger.info("Shadow tables ready.")

def validate_shadow_data(conn):
    """
    Compares shadow tables against live tables to ensure data integrity.
    Returns True if safe to swap, False otherwise.
    """
    logger.info("Validating shadow data against live data...")
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # 1. Check Total Counts
        cursor.execute("SELECT COUNT(*) as count FROM networks")
        live_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM networks_shadow")
        shadow_count = cursor.fetchone()['count']
        
        if live_count > 0:
            diff_percent = abs(live_count - shadow_count) / live_count
            if diff_percent > 0.2:
                logger.error(f"❌ VALIDATION FAILED: Network count mismatch too high! Live: {live_count}, Shadow: {shadow_count} (Diff: {diff_percent:.1%})")
                return False
        
        logger.info(f"✅ Count validation passed: Live={live_count}, Shadow={shadow_count}")

        # 2. Check Top 10 Consistency (Optional but recommended)
        # We can check if the largest network in shadow is roughly same size as live
        return True

def atomic_swap(conn):
    """Perform the table swap in a single transaction."""
    logger.info("Performing atomic table swap...")
    with conn.cursor() as cursor:
        # Rename Live -> Old
        cursor.execute("ALTER TABLE networks RENAME TO networks_old;")
        cursor.execute("ALTER TABLE entity_networks RENAME TO entity_networks_old;")
        
        # Rename Shadow -> Live
        cursor.execute("ALTER TABLE networks_shadow RENAME TO networks;")
        cursor.execute("ALTER TABLE entity_networks_shadow RENAME TO entity_networks;")
        
        # Drop Old (Or keep for backup? Let's drop for now to save space, or rename carefully)
        cursor.execute("DROP TABLE networks_old CASCADE;")
        cursor.execute("DROP TABLE entity_networks_old CASCADE;")
        
        # Rename indexes/constraints if necessary? 
        # Postgres constraints usually follow the table rename, but index names stay same.
        # We might need to rename indexes to avoid conflicts if we re-run.
        # Ideally we'd drop the old ones.
        
    conn.commit()
    logger.info("✅ Atomic swap complete. Live site is now serving new data.")

def run_refresh(depth=4, dry_run=False):
    """
    Executes the safe network refresh process.
    1. Setup Shadow Tables
    2. Build Graph & Discover Networks
    3. Store in Shadow Tables
    4. Validate
    5. atomic_swap (if not dry_run)
    """
    logger.info(f"🚀 Starting Network Refresh (Depth={depth}, DryRun={dry_run})")
    conn = None
    try:
        conn = get_db_connection()
        
        # 1. Setup Shadow Tables
        setup_shadow_tables(conn)
        
        # 2. Build Graph (Standard Logic)
        graph, entity_info = build_graph_from_owners(conn)
        
        # Shared Address Linking
        address_edges = build_address_edges(conn, graph)
        for u, v in address_edges:
            graph[u].add(v)
            graph[v].add(u)
            
        # Network Discovery (PASS DEPTH ARG)
        from discover_networks import discover_networks_depth_limited
        networks = discover_networks_depth_limited(graph, max_depth=depth)
        
        # 3. Store in Shadow Tables
        if networks:
            store_networks(conn, networks, entity_info, networks_table='networks_shadow', entity_networks_table='entity_networks_shadow')
        
        # 4. Validate
        if validate_shadow_data(conn):
            if dry_run:
                logger.info("Dry run complete. Tables NOT swapped.")
            else:
                # 5. Swap
                atomic_swap(conn)
                
                # 6. Update Stats (on the new live table)
                update_network_statistics(conn)
            return True
        else:
            logger.error("🛑 Aborting refresh due to validation failure. Live data remains untouched.")
            return False
            
    except Exception as e:
        logger.error(f"Refresh failed: {e}")
        if conn: conn.rollback()
        return False
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Safe Network Refresh")
    parser.add_argument('--depth', type=int, default=4, help="Max recursion depth for network discovery (default: 4)")
    parser.add_argument('--dry-run', action='store_true', help="Run without swapping tables")
    args = parser.parse_args()

    run_refresh(depth=args.depth, dry_run=args.dry_run)
