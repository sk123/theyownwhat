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
from io import StringIO
from typing import List, Set

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared_utils import normalize_business_name, normalize_person_name, normalize_mailing_address, canonicalize_person_name, get_name_variations

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
    norm_ops = f"UPPER(TRIM({col_name}))"
    # Strip trailing noise (& and /)
    norm_ops = f"REGEXP_REPLACE({norm_ops}, '[&/]\\s*$', '', 'g')"
    # Replace inward joint markers with space (to match Python split/space logic)
    norm_ops = f"REPLACE({norm_ops}, ' & ', ' ')"
    norm_ops = f"REPLACE({norm_ops}, ' / ', ' ')"
    norm_ops = f"REPLACE({norm_ops}, ' AND ', ' ')"
    norm_ops = f"REGEXP_REPLACE({norm_ops}, '[.,''`\"]', '', 'g')" # Remove punctuation
    norm_ops = f"REGEXP_REPLACE({norm_ops}, '[^A-Z0-9\\s-]', '', 'g')" # Remove special chars
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
    if rule == 'public': return email # Match full email for public providers
    return domain # Default: Group by domain for private/custom providers

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
        # Check if we need to drop legacy constraint/column
        cursor.execute("SELECT 1 FROM pg_constraint WHERE conname = 'fk_properties_networks'")
        if cursor.fetchone():
            cursor.execute("ALTER TABLE properties DROP CONSTRAINT IF EXISTS fk_properties_networks;")
            
        cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='properties' AND column_name='network_id'")
        if cursor.fetchone():
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
# Removed flip_name, using canonicalize_person_name instead.

def link_properties_standalone(conn):
    """
    Robustly matches properties directly to businesses and principals
    WITHOUT relying on any pre-built network data.
    """
    logger.info("STEP 1: Robustly linking properties directly to entities...")
    with conn.cursor() as cursor:
        
        # Clear any old links first
        logger.info("Clearing old property links and re-normalizing owner fields...")
        cursor.execute("UPDATE properties SET business_id = NULL, principal_id = NULL;")
        
        # Re-normalize owner fields using updated SQL logic
        owner_norm_sql = get_norm_sql('owner')
        co_owner_norm_sql = get_norm_sql('co_owner')
        cursor.execute(f"UPDATE properties SET owner_norm = {owner_norm_sql}, co_owner_norm = {co_owner_norm_sql};")
        
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
        
        # Pass 3: Suffix-blind Business Match (Primary Owner)
        # We strip common suffixes to handle "LLC" vs no "LLC" issues
        suffix_regex = r'\s+(LLC|INC|LTD|CORP|CO|COMPANY|LIMITED|GROUP|HOLDINGS|LLP|LP|L L C|L L P|L P)$'
        cursor.execute(f"""
            UPDATE properties p
            SET business_id = b.id
            FROM businesses b
            WHERE p.business_id IS NULL
              AND REGEXP_REPLACE(p.owner_norm, %s, '', 'g') = REGEXP_REPLACE(b.name_norm, %s, '', 'g')
              AND p.owner_norm IS NOT NULL AND b.name_norm IS NOT NULL;
        """, (suffix_regex, suffix_regex))
        biz_linked_count += cursor.rowcount

        conn.commit()
        logger.info(f"✅ Linked {biz_linked_count:,} properties to businesses.")

        # B. Link to Principals (Check both owner/co_owner and handle permutations)
        logger.info("Linking properties to Principals (where no business was matched)...")
        
        # 1. Prepare search keys: All variations mapping to a canonical ID
        cursor.execute("SELECT DISTINCT name_c FROM principals WHERE name_c IS NOT NULL AND name_c != ''")
        principals_raw = [r[0] for r in cursor.fetchall()]
        
        output = StringIO()
        seen_pairs = set()
        for p_raw in principals_raw:
            canon = canonicalize_person_name(p_raw)
            if not canon: continue
            
            # Get all variations for this principal (handles &, reversals, etc.)
            variations = get_name_variations(p_raw, 'principal')
            sanitized_canon = canon.replace('\\', '').replace('\t', ' ')
            
            for v in variations:
                sanitized_v = v.replace('\\', '').replace('\t', ' ')
                pair = (sanitized_v, sanitized_canon)
                if pair not in seen_pairs:
                    output.write(f"{sanitized_v}\t{sanitized_canon}\n")
                    seen_pairs.add(pair)
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
        
        # --- NEW: Fallback Address Linking ---
        # Link orphans (no business/principal) if they share a mailing address with a business
        # BUT be careful to avoid mega-networks (exclude JUNK and High-Frequency addresses)
        try:
             link_orphans_by_address(conn)
        except Exception as e:
             logger.error(f"Fallback address linking failed: {e}")

        conn.commit()
        return total

def link_orphans_by_address(conn):
    """
    Fallback linking: If a property is unlinked, but shares a mailing address 
    with a legitimate business, link it to that business.
    Safeguards: 
    1. Exclude JUNK_ADDRS
    2. Exclude addresses shared by too many businesses (>50) to avoid agent mega-nets.
    """
    logger.info("STEP 1.5: Fallback Linking - Matching orphans by address...")
    
    # 1. Reuse JUNK_ADDRS logic (defined in build_graph usually, duplicating here for safety scope)
    JUNK_ADDRS = {
        '2389 MAIN STREET GLASTONBURY COURT UNITED STATES 06033',
        '2389 MAIN ST STE 100 GLASTONBURY CT UNITED STATES 06033',
        'C T CORP SYSTEMS 799 MAIN STREET HARTFORD COURT 06103',
        '10 MAIN STREET BRISTOL CT UNITED STATES 06010',
        '10 MAIN ST BRISTOL CT UNITED STATES 06010', 
        '400 MIDDLE ST BRISTOL CT 06010'
    }

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # 1. Build map of Normalized Address -> Business ID
        # Only use businesses that have a valid address!
        logger.info("Building verified business address map...")
        cursor.execute("SELECT id, mail_address, business_address FROM businesses")
        
        addr_to_biz = defaultdict(list)
        
        for row in cursor.fetchall():
            # Try mailing first, then business
            raw = row['mail_address'] or row['business_address']
            norm = normalize_mailing_address(raw)
            
            if norm and len(norm) > 10 and norm not in JUNK_ADDRS:
                addr_to_biz[norm].append(row['id'])

        # 2. Filter out high-frequency addresses (Agents/Lawyers) -> potential mega-nets
        # Threshold: 500. (User requested increase to support big landlords).
        # Known agents like '10 Main St' are handled by JUNK_ADDRS.
        valid_addr_map = {}
        ignored_count = 0
        
        for addr, biz_ids in addr_to_biz.items():
            if len(biz_ids) > 500:
                ignored_count += 1
                continue
            # Linking rule: Link to the first business found at this address
            # (In a perfect world, they are all the same network anyway)
            valid_addr_map[addr] = biz_ids[0]
            
        logger.info(f"indexed {len(valid_addr_map)} valid business addresses (Ignored {ignored_count} high-freq addresses).")

        # 3. Fetch Orphans and Link
        logger.info("Scanning unlinked properties...")
        cursor.execute("SELECT id, mailing_address FROM properties WHERE business_id IS NULL AND principal_id IS NULL AND mailing_address IS NOT NULL")
        orphans = cursor.fetchall()
        
        linked_count = 0
        updates = []
        
        for p in orphans:
            norm_p = normalize_mailing_address(p['mailing_address'])
            if norm_p in valid_addr_map:
                biz_id = valid_addr_map[norm_p]
                updates.append((biz_id, p['id']))
                linked_count += 1
        
        if updates:
            logger.info(f"Linking {len(updates)} orphaned properties via address match...")
            execute_values(cursor, "UPDATE properties SET business_id = data.b_id FROM (VALUES %s) AS data (b_id, p_id) WHERE id = data.p_id", updates)
            
        logger.info(f"✅ Fallback Address Linking Complete. Linked {linked_count} properties.")

# --- NEW STEP 2: Build Global Graph ---
def build_graph(conn):
    """
    Builds a global graph of all business-principal links and shared emails.
    Discovery will later start from property-owning entities.
    """
    logger.info("STEP 2: Building global entity graph...")
    email_rules = load_email_rules(conn)
    graph = defaultdict(set)
    entity_info = {} # Stores display data: { key: {name, id} }
    email_key_map = defaultdict(set)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # 1. Load all Businesses and their Emails
        logger.info("Loading all businesses and email keys...")
        cursor.execute("SELECT id, name, business_email_address FROM businesses")
        for b in cursor.fetchall():
            biz_key = ('business', b['id'])
            entity_info[biz_key] = {'name': b['name'], 'id': b['id']}
            email_key = get_email_match_key(b['business_email_address'], email_rules)
            if email_key:
                email_key_map[email_key].add(biz_key)

        # 2. Load all Principals and their Links
        logger.info("Loading all principal links...")
        cursor.execute("SELECT normalized_name FROM principal_ignore_list")
        principal_ignore_set = {row['normalized_name'] for row in cursor.fetchall()}
        
        cursor.execute("SELECT business_id, name_c FROM principals WHERE name_c IS NOT NULL AND name_c != ''")
        for link in cursor:
            raw_prin_name = link['name_c']
            norm_prin_name = normalize_person_name(raw_prin_name)
            canon_prin_name = canonicalize_person_name(raw_prin_name)

            if not norm_prin_name or norm_prin_name in principal_ignore_set:
                continue

            biz_key = ('business', link['business_id'])
            prin_key = ('principal', canon_prin_name)
            
            if prin_key not in entity_info:
                entity_info[prin_key] = {'name': raw_prin_name, 'id': canon_prin_name}
            
            graph[biz_key].add(prin_key)
            graph[prin_key].add(biz_key)
        
        # --- MANUAL MERGES (Hardcoded Fixes) ---
        # Force merge 'YEHUDA GUREVITCH' and 'MENACHEM GUREVITCH' to fix fragmentation
        menachem_key = ('principal', canonicalize_person_name('MENACHEM GUREVITCH'))
        yehuda_key = ('principal', canonicalize_person_name('YEHUDA GUREVITCH'))
        
        # Ensure nodes exist in graph so they can be discovered
        if menachem_key not in entity_info: 
             entity_info[menachem_key] = {'name': 'MENACHEM GUREVITCH', 'id': menachem_key[1]}
        if yehuda_key not in entity_info:
             entity_info[yehuda_key] = {'name': 'YEHUDA GUREVITCH', 'id': yehuda_key[1]}

        graph[menachem_key].add(yehuda_key)
        graph[yehuda_key].add(menachem_key)
        logger.info(f"✅ Manually linked {menachem_key} and {yehuda_key}")
        
        # 3. Load Property Owners (Seed Info)
        logger.info("Identifying property-owning seed nodes...")
        cursor.execute("SELECT DISTINCT business_id FROM properties WHERE business_id IS NOT NULL")
        seed_biz_ids = {row['business_id'] for row in cursor.fetchall()}
        
        cursor.execute("SELECT DISTINCT principal_id FROM properties WHERE principal_id IS NOT NULL")
        seed_prin_names = {row['principal_id'] for row in cursor.fetchall()}
        
        seed_nodes = set()
        for bid in seed_biz_ids: seed_nodes.add(('business', bid))
        for pname in seed_prin_names: seed_nodes.add(('principal', pname))
        
        # 4. Email Linking (Global)
        logger.info("Adding global email edges...")
        email_edge_count = 0
        for email_key, entities_set in email_key_map.items():
            if len(entities_set) > 1:
                # To prevent massive cliques (e.g. 1000 businesses sharing one email), 
                # we only link if the set is manageable, or we use a virtual "email node".
                # For simplicity, we'll cap it or just link them all if they are < 100.
                # Increase limit for private domains to 5000 (enough for Gurevitch/Mandy)
                if len(entities_set) > 5000:
                    continue
                for entity_a, entity_b in combinations(entities_set, 2):
                    graph[entity_a].add(entity_b)
                    graph[entity_b].add(entity_a)
                    email_edge_count += 1
        logger.info(f"Added {email_edge_count:,} email edges.")

        # 5. Manual Hardcoded Links (Fixes user-reported fragmentation)
        # Force merge 'YEHUDA GUREVITCH' and 'MENACHEM GUREVITCH'
        try:
             # Normalized Names
             gurevitch_1 = ('principal', 'YEHUDA GUREVITCH')
             gurevitch_2 = ('principal', 'MENACHEM GUREVITCH')
             
             # Initialize if missing
             if gurevitch_1 not in graph: graph[gurevitch_1] = set()
             if gurevitch_2 not in graph: graph[gurevitch_2] = set()
             if gurevitch_1 not in entity_info: entity_info[gurevitch_1] = {'name': 'YEHUDA GUREVITCH', 'id': 'YEHUDA GUREVITCH'}
             if gurevitch_2 not in entity_info: entity_info[gurevitch_2] = {'name': 'MENACHEM GUREVITCH', 'id': 'MENACHEM GUREVITCH'}

             graph[gurevitch_1].add(gurevitch_2)
             graph[gurevitch_2].add(gurevitch_1)
             logger.info("✅ Manually linked 'YEHUDA GUREVITCH' and 'MENACHEM GUREVITCH'.")
        except Exception as e:
             logger.warning(f"Could not apply manual link for Gurevitch: {e}")

    total_edges = sum(len(v) for v in graph.values()) // 2
    logger.info(f"✅ Global graph built: {len(graph):,} entities, {total_edges:,} edges.")
    return graph, entity_info, seed_nodes

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
            'C T CORP SYSTEMS 799 MAIN STREET HARTFORD COURT 06103',
            '10 MAIN STREET BRISTOL CT UNITED STATES 06010',
            '10 MAIN ST BRISTOL CT UNITED STATES 06010', 
            '400 MIDDLE ST BRISTOL CT 06010'
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
                
            # Lowered threshold to 500 for safety against "meganets"
            if len(biz_ids) > 500: 
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
def discover_networks(graph, seed_nodes):
    """
    Discovers all connected components in the graph that contain at least one seed node.
    Uses a global visited set to find true disjoint components.
    """
    logger.info(f"STEP 3: Discovering networks starting from {len(seed_nodes)} seeds...")
    
    global_visited = set()
    networks = []
    
    # Sort for deterministic results
    seed_nodes = sorted(list(seed_nodes))
    
    for start_node in seed_nodes:
        if start_node in global_visited:
            continue
            
        # BFS with DEPTH LIMIT (to prevent runaway meganetworks)
        network_component = set()
        queue = deque([(start_node, 0)])
        global_visited.add(start_node)
        network_component.add(start_node)
        
        while queue:
            current_node, depth = queue.popleft()
            if depth >= 4:
                continue
                
            for neighbor in graph.get(current_node, []):
                if neighbor not in global_visited:
                    global_visited.add(neighbor)
                    network_component.add(neighbor)
                    queue.append((neighbor, depth + 1))
        
        if len(network_component) > 1:
            networks.append(list(network_component))
            
    networks.sort(key=len, reverse=True)
    logger.info(f"✅ Discovered {len(networks)} disjoint networks containing properties.")
    return networks


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
    lock_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'maintenance.lock')
    try:
        # Create lock file
        with open(lock_path, 'w') as f:
            f.write(str(time.time()))
        
        conn = get_db_connection()
        # skip setup_network_schema(conn) to avoid AccessExclusiveLocks hanging the refresh
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM properties WHERE business_id IS NOT NULL OR principal_id IS NOT NULL LIMIT 1;")
            linking_done = cursor.fetchone() is not None

        if not linking_done or args.force:
            link_properties_standalone(conn)
        else:
            logger.info("STEP 1: Properties already linked. Skipping linking step. (Use --force to re-link).")

        graph, entity_info, seed_nodes = build_graph(conn)
        
        # --- NEW: Shared Address Linking ---
        # Determine which businesses (nodes in our graph) share a physical/mailing address
        address_edges = build_address_edges(conn, graph)
        for u, v in address_edges:
            graph[u].add(v)
            graph[v].add(u)
        # -----------------------------------

        networks = discover_networks(graph, seed_nodes)
        
        if networks:
            store_networks(conn, networks, entity_info)
        
        update_network_statistics(conn)
        
        logger.info("🎉 Network discovery and update complete! Graph is now property-centric.")
        
    except psycopg2.Error as e:
        logger.error(f"A database error occurred: {e}", exc_info=False) 
        if conn: conn.rollback()
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        if conn: conn.rollback()
        sys.exit(1)
    finally:
        if conn: conn.close()
        # Remove lock file
        if os.path.exists(lock_path):
            os.remove(lock_path)

if __name__ == "__main__":
    main()