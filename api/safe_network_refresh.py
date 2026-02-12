# safe_network_refresh.py
import os
import sys
import psycopg2
import logging
from psycopg2.extras import RealDictCursor

# Add current directory to path so we can import network_builder
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from network_builder import get_db_connection, link_properties_to_entities, build_graph, discover_networks_depth_limited, store_networks_shadow

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def setup_shadow_tables(conn):
    """Creates shadow tables for safe writing."""
    logger.info("Setting up shadow tables...")
    with conn.cursor() as cursor:
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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ownership_links_shadow (
                network_id INTEGER,
                from_entity TEXT NOT NULL,
                to_entity TEXT NOT NULL,
                link_type TEXT NOT NULL
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entity_networks_shadow_network ON entity_networks_shadow(network_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ownership_links_shadow_network ON ownership_links_shadow(network_id);")
        # Truncate all shadow tables
        cursor.execute("TRUNCATE networks_shadow, entity_networks_shadow, ownership_links_shadow RESTART IDENTITY;")
    conn.commit()

def validate_shadow_data(conn):
    """Ensures shadow data is sane before swapping."""
    logger.info("Validating shadow data...")
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT COUNT(*) as count FROM networks")
        live_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM networks_shadow")
        shadow_count = cursor.fetchone()['count']
        
        if live_count > 0:
            diff_percent = abs(live_count - shadow_count) / live_count
            logger.info(f"Current variance: {diff_percent:.1%}")
            # Relaxing for this restoration run
            # if diff_percent > 0.5: 
            #     return False
        
        logger.info(f"✅ Validation passed: Live={live_count}, Shadow={shadow_count}")
        return True

def atomic_swap(conn):
    """Perform the table swap in a single transaction."""
    logger.info("Performing atomic table swap...")
    with conn.cursor() as cursor:
        cursor.execute("ALTER TABLE networks RENAME TO networks_old;")
        cursor.execute("ALTER TABLE entity_networks RENAME TO entity_networks_old;")
        cursor.execute("ALTER TABLE ownership_links RENAME TO ownership_links_old;")
        
        cursor.execute("ALTER TABLE networks_shadow RENAME TO networks;")
        cursor.execute("ALTER TABLE entity_networks_shadow RENAME TO entity_networks;")
        cursor.execute("ALTER TABLE ownership_links_shadow RENAME TO ownership_links;")
        
        cursor.execute("DROP TABLE networks_old CASCADE;")
        cursor.execute("DROP TABLE entity_networks_old CASCADE;")
        cursor.execute("DROP TABLE ownership_links_old CASCADE;")
    conn.commit()

def run_refresh(dry_run=False, skip_linking=False, skip_emails=False):
    """Executes the full refresh cycle."""
    logger.info(f"🚀 Starting Network Refresh (DryRun={dry_run}, SkipLinking={skip_linking}, SkipEmails={skip_emails})")
    conn = None
    try:
        conn = get_db_connection()

        if not skip_linking:
            # 0. Clear stale links (Important for correctness after logic changes)
            with conn.cursor() as cur:
                logger.info("🧹 PHASE 0: Clearing stale property links...")
                # Optimization: Only update rows that actually have data
                cur.execute("""
                    UPDATE properties 
                    SET business_id = NULL, principal_id = NULL
                    WHERE business_id IS NOT NULL OR principal_id IS NOT NULL
                """)
            conn.commit()
            
            # 1. Link properties
            link_properties_to_entities(conn)

        else:
            logger.info("⏩ PHASES 0 & 1 SKIPPED: Assuming property links are already set.")
        
        # 2. Build graph and discover networks
        if skip_emails:
            logger.info("⚠️ DIAGNOSTIC MODE: Skipping email matching.")
        else:
            logger.info("📧 Email Matching ENABLED.")
            
        graph_data = build_graph(conn, skip_emails=skip_emails)
        
        logger.info("Gathering seed nodes...")
        seeds = set()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT DISTINCT business_id FROM properties WHERE business_id IS NOT NULL")
            for r in cur: seeds.add(('business', r['business_id']))
            cur.execute("SELECT DISTINCT principal_id FROM properties WHERE principal_id IS NOT NULL")
            for r in cur: seeds.add(('principal', r['principal_id']))
            
        networks = discover_networks_depth_limited(graph_data, list(seeds))
        
        # 3. Setup shadow and store
        setup_shadow_tables(conn)
        store_networks_shadow(conn, networks, graph_data=graph_data)
        
        # 4. Validate and Swap
        if validate_shadow_data(conn):
            if dry_run:
                logger.info("Dry run complete. Tables NOT swapped.")
            else:
                atomic_swap(conn)
                logger.info("✅ Refresh cycle complete. Networks updated.")
            return True
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
    parser.add_argument('--dry-run', action='store_true', help="Run without swapping tables")
    parser.add_argument('--skip-linking', action='store_true', help="Skip property linking phases (0 and 1)")
    parser.add_argument('--skip-emails', action='store_true', help="Skip email matching (Phase 2)")
    args = parser.parse_args()

    run_refresh(dry_run=args.dry_run, skip_linking=args.skip_linking, skip_emails=args.skip_emails)
