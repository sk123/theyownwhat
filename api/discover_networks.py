# discover_networks.py
import os
import re
import time
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from collections import deque, defaultdict
from itertools import combinations
import argparse
import logging
import sys
from typing import List, Set

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared_utils import normalize_business_name, normalize_person_name

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")


# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("✅ Database connection successful.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

# --- NEW HELPER: SQL Normalization Function ---
def get_norm_sql(col_name):
    """Returns a string of SQL operations to normalize a name column."""
    norm_ops = f"UPPER({col_name})"
    norm_ops = f"REPLACE({norm_ops}, '&', 'AND')"
    norm_ops = f"REGEXP_REPLACE({norm_ops}, '[.,''`\"]', '', 'g')" # Remove punctuation
    norm_ops = f"REGEXP_REPLACE({norm_ops}, '[^A-Z0-9\s-]', '', 'g')" # Remove special chars (keep hyphen)
    norm_ops = f"TRIM(REGEXP_REPLACE({norm_ops}, '\s+', ' ', 'g'))" # Collapse whitespace
    return norm_ops

# --- Helper Functions (Email Logic) ---
def load_email_rules(conn):
    """Loads all EMAIL rules from the database into memory."""
    logger.info("Loading email matching rules from database...")
    email_rules = {}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT domain, match_type FROM email_match_rules")
            for row in cursor.fetchall():
                email_rules[row['domain']] = row['match_type']
            logger.info(f"Loaded {len(email_rules)} email rules (public, registrar, and custom).")
    except psycopg2.Error as e:
        logger.warning(f"Could not load email rules (table might not exist): {e}")
    return email_rules

def get_email_match_key(email: str, email_rules: dict) -> str | None:
    """Classifies an email and returns a matching key based on the 3-category logic."""
    if not email or '@' not in email: return None
    email = email.lower().strip()
    try:
        _, domain = email.split('@', 1)
    except ValueError:
        return None
    rule = email_rules.get(domain)
    if rule == 'registrar': return None
    if rule == 'custom': return domain
    return email # Default: Match full email for public/unknown

# --- Schema Setup (Unchanged) ---
def setup_network_schema(conn):
    """Creates tables for networks/entities and ensures properties table is correct."""
    logger.info("Setting up network schema...")
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS networks (
                id SERIAL PRIMARY KEY, primary_name TEXT, total_properties INTEGER DEFAULT 0,
                total_assessed_value NUMERIC DEFAULT 0, business_count INTEGER DEFAULT 0,
                principal_count INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                network_size TEXT, updated_at TIMESTAMP
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entity_networks (
                network_id INTEGER REFERENCES networks(id) ON DELETE CASCADE,
                entity_type TEXT NOT NULL CHECK (entity_type IN ('business', 'principal')),
                entity_id TEXT NOT NULL, entity_name TEXT NOT NULL, normalized_name TEXT,
                PRIMARY KEY (network_id, entity_type, entity_id)
            );
        """)
        logger.info("Ensuring 'properties' table schema is correct...")
        cursor.execute("ALTER TABLE properties DROP CONSTRAINT IF EXISTS fk_properties_networks;")
        cursor.execute("ALTER TABLE properties DROP COLUMN IF EXISTS network_id;")
        cursor.execute("ALTER TABLE properties ADD COLUMN IF NOT EXISTS business_id TEXT;")
        cursor.execute("ALTER TABLE properties ADD COLUMN IF NOT EXISTS principal_id TEXT;")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_business_id ON properties(business_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_principal_id ON properties(principal_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entity_networks_network ON entity_networks(network_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entity_networks_normalized ON entity_networks(normalized_name);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_owner_upper ON properties(UPPER(owner));")
    conn.commit()
    logger.info("Schema setup and migration complete.")

# --- NEW STEP 1: Link Properties (Standalone) ---
def link_properties_standalone(conn):
    """
    Robustly matches properties directly to businesses and principals
    WITHOUT relying on any pre-built network data.
    """
    logger.info("STEP 1: Robustly linking properties directly to entities...")
    with conn.cursor() as cursor:
        
        # Clear any old links first
        logger.info("Clearing old property links...")
        cursor.execute("UPDATE properties SET business_id = NULL, principal_id = NULL;")
        conn.commit()

        # Link to Businesses
        logger.info("Linking properties to Businesses...")
        biz_norm_sql = get_norm_sql('b.name')
        prop_norm_sql = get_norm_sql('p.owner')
        cursor.execute(f"""
            UPDATE properties p
            SET business_id = b.id
            FROM businesses b
            WHERE {prop_norm_sql} = {biz_norm_sql}
                AND p.owner IS NOT NULL AND b.name IS NOT NULL;
        """)
        biz_linked_count = cursor.rowcount
        conn.commit()
        logger.info(f"✅ Linked {biz_linked_count:,} properties to businesses.")

        # Link to Principals
        logger.info("Linking properties to Principals (where no business was matched)...")
        prin_norm_sql = get_norm_sql('pr.name_c')
        prop_norm_sql_prin = get_norm_sql('p.owner')

        cursor.execute(f"""
            WITH norm_principals AS (
                SELECT DISTINCT ON ({prin_norm_sql}) 
                       {prin_norm_sql} AS norm_name
                FROM principals pr
                WHERE pr.name_c IS NOT NULL AND pr.name_c != ''
            )
            UPDATE properties p
            SET principal_id = np.norm_name -- Store the NORMALIZED name as the ID
            FROM norm_principals np
            WHERE {prop_norm_sql_prin} = np.norm_name
                AND p.business_id IS NULL; -- Only link if a business didn't already match
        """)
        prin_linked_count = cursor.rowcount
        conn.commit()
        logger.info(f"✅ Linked {prin_linked_count:,} properties to principals.")
        
        total = biz_linked_count + prin_linked_count
        logger.info(f"🎉 Property Linking Complete. Total linked: {total:,}")
        return total

# --- NEW STEP 2: Build Graph from Property Owners ---
def build_graph_from_owners(conn):
    """
    Builds a graph starting ONLY from entities that own property, then expands.
    """
    logger.info("STEP 2: Building graph starting from property-owning entities...")
    email_rules = load_email_rules(conn)
    graph = defaultdict(set)
    entity_info = {} # Stores display data: { key: {name, id} }
    email_key_map = defaultdict(set)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # 1. Get our "seed lists" of all entities that own property
        cursor.execute("SELECT DISTINCT business_id FROM properties WHERE business_id IS NOT NULL")
        seed_biz_ids = {row['business_id'] for row in cursor.fetchall()}
        
        cursor.execute("SELECT DISTINCT principal_id FROM properties WHERE principal_id IS NOT NULL")
        # principal_id *is* the normalized name
        seed_prin_names = {row['principal_id'] for row in cursor.fetchall()}

        logger.info(f"Found {len(seed_biz_ids)} property-owning businesses and {len(seed_prin_names)} property-owning principals.")

        # 2. Add all these seed entities to our graph/info maps
        # Add seed businesses
        if seed_biz_ids:
            cursor.execute("SELECT id, name, business_email_address FROM businesses WHERE id = ANY(%s)", (list(seed_biz_ids),))
            for b in cursor.fetchall():
                biz_key = ('business', b['id'])
                if biz_key not in entity_info:
                    entity_info[biz_key] = {'name': b['name'], 'id': b['id']}
                email_key = get_email_match_key(b['business_email_address'], email_rules)
                if email_key:
                    email_key_map[email_key].add(biz_key)

        # Add seed principals (get their raw name for display)
        if seed_prin_names:
            prin_norm_col_sql = get_norm_sql('name_c')
            cursor.execute(f"""
                WITH norm_prins AS (
                    SELECT name_c, {prin_norm_col_sql} as norm_name
                    FROM principals WHERE name_c IS NOT NULL AND name_c != ''
                )
                SELECT DISTINCT ON (np.norm_name) np.norm_name, np.name_c AS raw_name
                FROM norm_prins np
                WHERE np.norm_name = ANY(%s)
            """, (list(seed_prin_names),))
            for p in cursor.fetchall():
                prin_key = ('principal', p['norm_name'])
                if prin_key not in entity_info:
                    entity_info[prin_key] = {'name': p['raw_name'], 'id': p['norm_name']}
        
        logger.info(f"Graph starting with {len(entity_info)} seed nodes.")

        # 3. Now, build connections starting ONLY from our seed businesses
        logger.info("Expanding graph: Finding principals linked to seed businesses...")
        if not seed_biz_ids:
            logger.info("No seed businesses, skipping principal expansion.")
            return graph, entity_info
            
        cursor.execute("""
            SELECT business_id, name_c
            FROM principals 
            WHERE business_id = ANY(%s)
            AND name_c IS NOT NULL AND name_c != ''
        """, (list(seed_biz_ids),))
        
        principal_links = cursor.fetchall()
        logger.info(f"Found {len(principal_links)} principal links from seed businesses.")

        cursor.execute("SELECT normalized_name FROM principal_ignore_list")
        principal_ignore_set = {row['normalized_name'] for row in cursor.fetchall()}
        logger.info(f"Loaded {len(principal_ignore_set)} agent principals to ignore.")
        ignored_links = 0
        
        for link in principal_links:
            biz_key = ('business', link['business_id'])
            raw_prin_name = link['name_c']
            norm_prin_name = normalize_person_name(raw_prin_name)

            if not norm_prin_name or norm_prin_name in principal_ignore_set:
                if norm_prin_name in principal_ignore_set: ignored_links += 1
                continue

            prin_key = ('principal', norm_prin_name)
            graph[biz_key].add(prin_key)
            graph[prin_key].add(biz_key)

            if prin_key not in entity_info:
                entity_info[prin_key] = {'name': raw_prin_name, 'id': norm_prin_name}
        
        logger.info(f"Filtered {ignored_links} agent links. Added principal edges to graph.")
        
        # 4. Email Linking
        logger.info("Expanding graph: Linking entities by shared email keys...")
        email_edge_count = 0
        for email_key, entities_set in email_key_map.items():
            relevant_entities = {e for e in entities_set if e in entity_info}
            if len(relevant_entities) > 1:
                for entity_a, entity_b in combinations(relevant_entities, 2):
                    graph[entity_a].add(entity_b)
                    graph[entity_b].add(entity_a)
                    email_edge_count += 1
        logger.info(f"Added {email_edge_count} email edges between property-owning entities.")

    total_edges = sum(len(v) for v in graph.values()) // 2
    logger.info(f"✅ Property-Owner graph built: {len(graph)} entities, {total_edges} edges.")
    return graph, entity_info

# --- DISCOVERY & STORAGE ---
def discover_networks(graph):
    logger.info("STEP 3: Discovering connected components (BFS)...")
    visited_globally = set()
    networks = []
    for start_node in graph:
        if start_node in visited_globally: continue
        network_component = set(); queue = deque([start_node])
        visited_globally.add(start_node); network_component.add(start_node)
        while queue:
            current_node = queue.popleft()
            for neighbor in graph.get(current_node, []):
                if neighbor not in visited_globally:
                    visited_globally.add(neighbor); network_component.add(neighbor)
                    queue.append(neighbor)
        if len(network_component) > 1: networks.append(list(network_component))
    networks.sort(key=len, reverse=True)
    logger.info(f"Discovered {len(networks)} distinct networks (size > 1).")
    return networks

def store_networks(conn, networks, entity_info):
    logger.info("STEP 4: Storing networks in database...")
    with conn.cursor() as cursor:
        # --- FIXED: Use explicit TRUNCATE instead of CASCADE ---
        logger.info("Clearing old network and insight data...")
        cursor.execute("TRUNCATE cached_insights, entity_networks, networks RESTART IDENTITY;")
        conn.commit()

        entity_data_to_copy = []
        for network in networks:
            businesses = [entity_info.get(n) for n in network if n[0] == 'business' and entity_info.get(n)]
            primary_name = businesses[0]['name'] if businesses else entity_info.get(network[0], {}).get('name', 'Unknown')
            
            cursor.execute("INSERT INTO networks (primary_name) VALUES (%s) RETURNING id", (primary_name,))
            network_id = cursor.fetchone()[0]
            
            for entity_type, entity_key in network:
                info = entity_info.get((entity_type, entity_key))
                if not info: continue
                
                name = info.get('name')
                db_id = info.get('id')
                normalized = normalize_business_name(name) if entity_type == 'business' else normalize_person_name(name)
                entity_data_to_copy.append((network_id, entity_type, db_id, name, normalized))
        
        logger.info(f"Preparing to bulk-insert {len(entity_data_to_copy):,} entity-network links...")
        execute_values(cursor, 
            "INSERT INTO entity_networks (network_id, entity_type, entity_id, entity_name, normalized_name) VALUES %s",
            entity_data_to_copy)
    conn.commit()
    logger.info(f"✅ Successfully stored {len(networks)} new networks.")

# --- Update Statistics ---
def update_network_statistics(conn):
    logger.info("STEP 5: Final network statistics...")
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        logger.info("--- Top 10 Property-Centric Networks by Entity Count ---")
        cursor.execute("SELECT primary_name, (SELECT COUNT(*) FROM entity_networks WHERE network_id=n.id) as entity_count FROM networks n ORDER BY entity_count DESC LIMIT 10;")
        results = cursor.fetchall()
        if not results: logger.info("No networks were found.")
        else:
            for row in results: logger.info(f"  - {row['primary_name']}: {row['entity_count']} entities")
    conn.commit()
    logger.info("Statistics update complete.")

# --- Main Process Flow ---
def main():
    parser = argparse.ArgumentParser(description="Discover and store ownership networks")
    parser.add_argument('--force', action='store_true', help="Force re-linking of properties and re-build all networks.")
    args = parser.parse_args()
    
    conn = None
    try:
        conn = get_db_connection()
        setup_network_schema(conn)
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM properties WHERE business_id IS NOT NULL OR principal_id IS NOT NULL LIMIT 1;")
            linking_done = cursor.fetchone() is not None

        if not linking_done or args.force:
            link_properties_standalone(conn)
        else:
            logger.info("STEP 1: Properties already linked. Skipping linking step. (Use --force to re-link).")

        graph, entity_info = build_graph_from_owners(conn)
        networks = discover_networks(graph)
        
        if networks:
            store_networks(conn, networks, entity_info)
        
        update_network_statistics(conn)
        
        logger.info("🎉 Network discovery and update complete! Graph is now property-centric.")
        
    except psycopg2.Error as e:
        logger.error(f"A database error occurred: {e}", exc_info=False) # Set exc_info to False for cleaner output on known issues
        if conn: conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()