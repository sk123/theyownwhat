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

from shared_utils import normalize_business_name, normalize_person_name, normalize_mailing_address

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
    norm_ops = f"REGEXP_REPLACE({norm_ops}, '[^A-Z0-9\\s-]', '', 'g')" # Remove special chars (keep hyphen)
    norm_ops = f"TRIM(REGEXP_REPLACE({norm_ops}, '\\s+', ' ', 'g'))" # Collapse whitespace
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
        conn.rollback() # Reset transaction
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
        
        # Ensure helper tables exist
        cursor.execute("CREATE TABLE IF NOT EXISTS email_match_rules (domain TEXT PRIMARY KEY, match_type TEXT);")
        cursor.execute("CREATE TABLE IF NOT EXISTS principal_ignore_list (normalized_name TEXT PRIMARY KEY);")
    
    conn.commit()
    logger.info("Schema setup and migration complete.")

# --- NEW STEP 1: Link Properties (Standalone) ---
def flip_name(name):
    """Simple flip for LAST FIRST <-> FIRST LAST."""
    if not name: return None
    parts = name.split()
    if len(parts) == 2:
        return f"{parts[1]} {parts[0]}"
    return None

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

        # A. Link to Businesses (Check both owner and co_owner)
        logger.info("Linking properties to Businesses via owner_norm and co_owner_norm...")
        
        # Pass 1: Primary Owner -> Business Name
        cursor.execute("""
            UPDATE properties p
            SET business_id = b.id
            FROM businesses b
            WHERE p.owner_norm = b.name_norm
              AND p.owner_norm IS NOT NULL AND b.name_norm IS NOT NULL;
        """)
        biz_linked_count = cursor.rowcount
        
        # Pass 2: Co-Owner -> Business Name (only if not already linked)
        cursor.execute("""
            UPDATE properties p
            SET business_id = b.id
            FROM businesses b
            WHERE p.business_id IS NULL 
              AND p.co_owner_norm = b.name_norm
              AND p.co_owner_norm IS NOT NULL AND b.name_norm IS NOT NULL;
        """)
        biz_linked_count += cursor.rowcount
        conn.commit()
        logger.info(f"✅ Linked {biz_linked_count:,} properties to businesses.")

        # B. Link to Principals (Check both owner/co_owner and handle permutations)
        logger.info("Linking properties to Principals (where no business was matched)...")
        
        # 1. Prepare search keys (including flipped names for LAST FIRST matching)
        cursor.execute("SELECT DISTINCT name_c_norm FROM principals WHERE name_c_norm IS NOT NULL AND name_c_norm != ''")
        principals = [r[0] for r in cursor.fetchall()]
        
        output = StringIO()
        for p in principals:
            output.write(f"{p}\t{p}\n")
            flipped = flip_name(p)
            if flipped:
                output.write(f"{flipped}\t{p}\n")
        output.seek(0)
        
        cursor.execute("CREATE TEMP TABLE tmp_prin_search (search_key TEXT, canonical_name TEXT)")
        cursor.copy_from(output, 'tmp_prin_search', columns=('search_key', 'canonical_name'))
        cursor.execute("CREATE INDEX idx_tmp_prin_search ON tmp_prin_search(search_key)")
        
        # Pass 1: owner_norm -> Principal Search Key
        cursor.execute("""
            UPDATE properties p
            SET principal_id = ts.canonical_name
            FROM tmp_prin_search ts
            WHERE p.business_id IS NULL
              AND p.owner_norm = ts.search_key;
        """)
        prin_linked_count = cursor.rowcount
        
        # Pass 2: co_owner_norm -> Principal Search Key
        cursor.execute("""
            UPDATE properties p
            SET principal_id = ts.canonical_name
            FROM tmp_prin_search ts
            WHERE p.business_id IS NULL AND p.principal_id IS NULL
              AND p.co_owner_norm = ts.search_key;
        """)
        prin_linked_count += cursor.rowcount
        
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
            prin_norm_col_sql = get_norm_sql('name_c') # Fallback to on-the-fly normalization
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

def build_address_edges(conn, existing_graph):
    """
    Finds businesses that share the same MAILING address.
    Returns a list of edge tuples: [ (('business', id_A), ('business', id_B)), ... ]
    """
    logger.info("STEP 2.5: detecting hidden networks via shared mailing addresses...")
    
    # 1. Get all businesses currently in our graph (property owners & their connections)
    # We only care about linking entities that are ALREADY relevant (i.e. own property or related to one)
    relevant_biz_ids = {node[1] for node in existing_graph if node[0] == 'business'}
    
    if not relevant_biz_ids:
        return []

    from shared_utils import normalize_mailing_address

    edges = []
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # Fetch addresses for relevant businesses
        cursor.execute(f"""
            SELECT id, mail_address, business_address 
            FROM businesses 
            WHERE id = ANY(%s) 
            AND (mail_address IS NOT NULL OR business_address IS NOT NULL)
        """, (list(relevant_biz_ids),))
        
        # Group businesses by normalized address
        addr_map = defaultdict(list)
        
        # JUNK LIST: Common placeholders that shouldn't link businesses
        JUNK_ADDRS = {
            'NO INFORMATION PROVIDED', 'NONE', 'UNKNOWN', '.', '',
            'NOT PROVIDED', 'N/A', 'NULL', 'CONNECTICUT', 'CT', 'USA',
            '2389 MAIN STREET #100 GLASTONBURY COURT UNITED STATES 06033',
            '2389 MAIN STREET GLASTONBURY COURT UNITED STATES 06033',
            '2389 MAIN ST STE 100 GLASTONBURY CT UNITED STATES 06033',
            'C T CORP SYSTEMS 799 MAIN STREET HARTFORD COURT 06103'
        }
        
        for row in cursor.fetchall():
            # Prioritize mailing address, fall back to business address
            raw = row['mail_address'] or row['business_address']
            norm = normalize_mailing_address(raw)
            # Filter out junk and extremely short strings
            if norm and norm not in JUNK_ADDRS and len(norm) > 10: 
                addr_map[norm].append(row['id'])

        # Create edges for clusters
        ignored_clusters = 0
        linked_clusters = 0
        
        for addr, biz_ids in addr_map.items():
            if len(biz_ids) < 2:
                continue
                
            # SAFETY VALVE: Ignore addresses shared by too many businesses (Registered Agents, Lawyers)
            # Raised threshold to 100 to capture Gurevitch's PO Box (58 units)
            # while still filtering out massive agent hubs.
            if len(biz_ids) > 100: 
                ignored_clusters += 1
                logger.info(f"Ignoring high-freq address (potential agent): {addr} ({len(biz_ids)} businesses)")
                continue

            linked_clusters += 1
            # Link all businesses in this cluster to each other
            for b1, b2 in combinations(biz_ids, 2):
                edges.append((('business', b1), ('business', b2)))

    logger.info(f"Found {len(edges)} hidden edges across {linked_clusters} shared addresses (Ignored {ignored_clusters} high-freq addresses).")
    return edges

# --- DISCOVERY & STORAGE ---
def discover_networks_depth_limited(graph, max_depth=3):
    """
    Discovers networks using DEPTH-LIMITED BFS from each property-owning entity.
    This prevents distant/weak connections from merging unrelated portfolios.
    
    max_depth=3 means:
    - Level 0: Starting entity
    - Level 1: Businesses they're in / Principals they're connected to
    - Level 2: Principals of those businesses / Businesses of those principals
    - Level 3: Businesses of level-2 principals
    """
    logger.info(f"STEP 3: Discovering networks with depth-limited BFS (max_depth={max_depth})...")
    
    # Track which entities have been used as starting points
    used_as_seed = set()
    networks = []
    
    # Only start from property-owning entities (our seed nodes)
    # Sort for deterministic results
    seed_nodes = sorted(graph.keys())
    
    for start_node in seed_nodes:
        if start_node in used_as_seed:
            continue
            
        # BFS with depth tracking
        network_component = set()
        queue = deque([(start_node, 0)])  # (node, depth)
        visited_local = {start_node}
        network_component.add(start_node)
        
        while queue:
            current_node, depth = queue.popleft()
            
            # Don't expand beyond max_depth
            if depth >= max_depth:
                continue
                
            for neighbor in graph.get(current_node, []):
                if neighbor not in visited_local:
                    visited_local.add(neighbor)
                    network_component.add(neighbor)
                    queue.append((neighbor, depth + 1))
        
        # Mark all entities in this network as "used"
        used_as_seed.update(network_component)
        
        if len(network_component) > 1:
            networks.append(list(network_component))
    
    networks.sort(key=len, reverse=True)
    logger.info(f"Discovered {len(networks)} distinct networks (size > 1).")
    return networks

def discover_networks(graph):
    """Wrapper to maintain compatibility"""
    return discover_networks_depth_limited(graph, max_depth=3)

def store_networks(conn, networks, entity_info, networks_table='networks', entity_networks_table='entity_networks', should_truncate=True):
    logger.info(f"STEP 4: Storing networks in database (Target: {networks_table})...")
    with conn.cursor() as cursor:
        if should_truncate:
             logger.info(f"Clearing old network data from {networks_table}...")
             # Only restart identity for the networks table
             cursor.execute(f"TRUNCATE {entity_networks_table}, {networks_table} RESTART IDENTITY;")
        
        entity_data_to_copy = []
        for network in networks:
            businesses = [entity_info.get(n) for n in network if n[0] == 'business' and entity_info.get(n)]
            primary_name = businesses[0]['name'] if businesses else entity_info.get(network[0], {}).get('name', 'Unknown')
            
            cursor.execute(f"INSERT INTO {networks_table} (primary_name) VALUES (%s) RETURNING id", (primary_name,))
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
            f"INSERT INTO {entity_networks_table} (network_id, entity_type, entity_id, entity_name, normalized_name) VALUES %s",
            entity_data_to_copy)
    conn.commit()
    logger.info(f"✅ Successfully stored {len(networks)} new networks in {networks_table}.")

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
        
        # --- NEW: Shared Address Linking ---
        # Determine which businesses (nodes in our graph) share a physical/mailing address
        address_edges = build_address_edges(conn, graph)
        for u, v in address_edges:
            graph[u].add(v)
            graph[v].add(u)
        # -----------------------------------

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