# network_builder.py
import os
import re
import time
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from collections import deque, defaultdict
import logging
import sys

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared_utils import (
    normalize_business_name, 
    normalize_person_name, 
    normalize_mailing_address, 
    canonicalize_person_name, 
    get_name_variations,
    get_email_match_key
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
MAX_DEPTH = 5 # Reverted to 5 to ensure full network capture (e.g. Gurevitch)

# --- Database Connection ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

# --- Logic 1: Property Linking ---
def link_properties_to_entities(conn):
    """Maps properties to business_id or principal_id using robust name matching."""
    logger.info("🔗 PHASE 1: Linking properties to entities...")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        b_map = {}
        cur.execute("SELECT id, name_norm FROM businesses WHERE name_norm IS NOT NULL")
        for row in cur: b_map[row['name_norm']] = row['id']
        
        p_map = {}
        cur.execute("SELECT principal_id, name_normalized FROM unique_principals")
        for row in cur:
            # We map BOTH the raw normalized name AND the word-sorted canonical version
            p_map[row['name_normalized']] = row['principal_id']
            canon = canonicalize_person_name(row['name_normalized'])
            if canon: p_map[canon] = row['principal_id']
            
        cur.execute("SELECT id, owner, co_owner, owner_norm, co_owner_norm FROM properties")
        updates = []
        linked_count = 0
        for row in cur:
            # Check businesses first
            bid = b_map.get(row['owner_norm']) or b_map.get(row['co_owner_norm'])
            
            # Check principals with canonical fallback (handles "Last First" vs "First Last")
            pid = p_map.get(row['owner_norm']) or p_map.get(row['co_owner_norm'])
            if not pid:
                p_canon_owner = canonicalize_person_name(row['owner_norm'])
                p_canon_co = canonicalize_person_name(row['co_owner_norm'])
                pid = p_map.get(p_canon_owner) or p_map.get(p_canon_co)
                
            if bid or pid:
                updates.append((bid, pid, row['id']))
                linked_count += 1
            if len(updates) >= 10000:
                with conn.cursor() as write_cur:
                    execute_values(write_cur, "UPDATE properties SET business_id = v.bid, principal_id = v.pid FROM (VALUES %s) AS v(bid, pid, id) WHERE properties.id = v.id", updates)
                conn.commit()
                updates = []
        if updates:
            with conn.cursor() as write_cur:
                execute_values(write_cur, "UPDATE properties SET business_id = v.bid, principal_id = v.pid FROM (VALUES %s) AS v(bid, pid, id) WHERE properties.id = v.id", updates)
            conn.commit()
    logger.info(f"✅ PHASE 1 COMPLETE: Linked {linked_count:,} properties.")

# --- Logic 2: Building the Graph (Adjacency List) ---
def build_graph(conn):
    """Builds an adjacency list of entities."""
    logger.info("🕸️ PHASE 2: Building entity graph...")
    graph = defaultdict(set)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # A. Principal-Business Links (Filter common names)
        cur.execute("SELECT normalized_name FROM principal_ignore_list")
        ignore_names = {row['normalized_name'] for row in cur}
        
        cur.execute("""
            SELECT pbl.business_id, pbl.principal_id, up.name_normalized
            FROM principal_business_links pbl
            JOIN unique_principals up ON up.principal_id = pbl.principal_id
        """)
        for row in cur:
            if row['name_normalized'] in ignore_names: continue
            u, v = ('business', row['business_id']), ('principal', row['principal_id'])
            graph[u].add(v); graph[v].add(u)

        # B. Shared Emails (Exclude registrars)
        cur.execute("SELECT domain, match_type FROM email_match_rules")
        email_rules = {row['domain']: row['match_type'] for row in cur}
        cur.execute("SELECT id, business_email_address FROM businesses WHERE business_email_address IS NOT NULL")
        email_map = defaultdict(list)
        for row in cur:
            key = get_email_match_key(row['business_email_address'], email_rules)
            if key: email_map[key].append(('business', row['id']))
        for entities in email_map.values():
            if 1 < len(entities) < 50: # Tightened threshold for email clusters (Was 100)
                for i in range(len(entities)):
                    for j in range(i+1, len(entities)):
                        graph[entities[i]].add(entities[j]); graph[entities[j]].add(entities[i])

        # C. Shared Mailing Addresses (Exclude agents/hubs)
        AGENT_REGEX = re.compile(r'ONE COMMERCIAL PLAZA|CORPORATION SYSTEM|C T CORP|591 W PUTNAM|INCFILE|REGISTERED AGENT|PO BOX|P\.O\. BOX|2389 MAIN ST|2389 MAIN STREET', re.I)
        cur.execute("SELECT id, mail_address, business_address FROM businesses")
        addr_map = defaultdict(list)
        for row in cur:
            raw = row['mail_address'] or row['business_address']
            norm = normalize_mailing_address(raw)
            if norm and len(norm) > 10 and not AGENT_REGEX.search(norm):
                addr_map[norm].append(('business', row['id']))
        for entities in addr_map.values():
            if 1 < len(entities) < 250: # Threshold for address clusters (Preserves Gurevitch @ 208, cuts Glastonbury Hub @ 500+)
                for i in range(len(entities)):
                    for j in range(i+1, len(entities)):
                        graph[entities[i]].add(entities[j]); graph[entities[j]].add(entities[i])

    logger.info(f"✅ PHASE 2 COMPLETE: Graph built with {len(graph):,} nodes.")
    return graph

# --- Logic 3: Depth-Limited Discovery ---
def discover_networks_depth_limited(graph, seed_nodes):
    """Discovers networks using BFS with a depth limit."""
    logger.info("🔍 PHASE 3: Discovering networks (Depth-Limited BFS)...")
    discovered_networks = []
    seen_globally = set()

    for seed in seed_nodes:
        if seed in seen_globally: continue
        
        # BFS
        queue = deque([(seed, 0)])
        current_network = {seed}
        inner_seen = {seed}
        
        while queue:
            node, depth = queue.popleft()
            if depth >= MAX_DEPTH: continue
            
            for neighbor in graph.get(node, []):
                if neighbor not in inner_seen:
                    inner_seen.add(neighbor)
                    current_network.add(neighbor)
                    queue.append((neighbor, depth + 1))
        
        discovered_networks.append(list(current_network))
        seen_globally.update(current_network)
        
    logger.info(f"✅ PHASE 3 COMPLETE: Found {len(discovered_networks):,} distinct networks.")
    return discovered_networks

# --- Logic 4: Storage ---
def store_networks_shadow(conn, networks):
    """Stores discovered networks into shadow tables with property-weighted human naming."""
    logger.info("💾 PHASE 4: Storing to shadow tables...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE networks_shadow, entity_networks_shadow RESTART IDENTITY;")
    
    # Metadata loading
    biz_names = {}; prin_names = {}
    biz_props = defaultdict(int); prin_props = defaultdict(int)
    biz_to_prins = defaultdict(list)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        logger.info("  - Loading names...")
        cur.execute("SELECT id, name FROM businesses"); biz_names = {r['id']: r['name'] for r in cur}
        cur.execute("SELECT principal_id, representative_name_c FROM unique_principals"); prin_names = {r['principal_id']: r['representative_name_c'] for r in cur}
        
        logger.info("  - Loading property weights...")
        cur.execute("SELECT business_id, COUNT(*) as count FROM properties WHERE business_id IS NOT NULL GROUP BY business_id")
        for r in cur: biz_props[r['business_id']] = r['count']
        
        cur.execute("SELECT principal_id, COUNT(*) as count FROM properties WHERE principal_id IS NOT NULL GROUP BY principal_id")
        for r in cur: 
            try:
                pid = int(r['principal_id'])
                prin_props[pid] = r['count']
            except (ValueError, TypeError):
                continue
                
        logger.info("  - Loading principal-business links...")
        cur.execute("SELECT principal_id, business_id FROM principal_business_links")
        for r in cur:
            try:
                pid = int(r['principal_id'])
                biz_to_prins[r['business_id']].append(pid)
            except (ValueError, TypeError):
                continue

    def is_human_like(name):
        if not name: return False
        name_upper = name.upper()
        # Heuristic: If it has biz suffixes or common buzzwords, it's probably not a "key human principal"
        for suffix in [' LLC', ' INC', ' CORP', ' LTD', ' LP ', ' LLP', ' TRUST', ' ESTATE', ' ASSOC', 'HOLDING', 'PROPERT', 'REALTY', 'MANAGEMENT', 'PARTNERS', 'VENTURES', 'DEVELOPMENT', 'INVESTMENT', 'SERVIC']:
            if suffix in name_upper: return False
        return True

    entity_links = []
    for nid, group in enumerate(networks, 1):
        # 1. Identify members
        principals = []
        for n in group:
            if n[0] == 'principal':
                try: principals.append(int(n[1]))
                except: pass
        businesses = [n[1] for n in group if n[0] == 'business']
        
        # 2. NAMING LOGIC: Property-Weighted Human Priority
        # Exclude known agents from the ignore list
        with conn.cursor() as cur:
            cur.execute("SELECT normalized_name FROM principal_ignore_list")
            ignore_names_set = {r[0] for r in cur}

        p_weights = defaultdict(int)
        for pid in principals:
            p_weights[pid] += prin_props.get(pid, 0)
            
        # Add property counts from businesses owned by these principals (within this network)
        for bid in businesses:
            b_count = biz_props.get(bid, 0)
            if b_count > 0:
                for pid in biz_to_prins.get(bid, []):
                    if pid in p_weights: # Only count if the principal is actually in this network
                        p_weights[pid] += b_count
            
        # Sort principals: First by human-likeness, then by property influence
        # CRITICAL: Exclude agents from being the head name
        sorted_ps = sorted(
            [p for p in principals if normalize_person_name(prin_names.get(p)) not in ignore_names_set], 
            key=lambda p: (is_human_like(prin_names.get(p)), p_weights.get(p, 0)), 
            reverse=True
        )
        
        primary_name = None
        if sorted_ps:
            primary_name = prin_names.get(sorted_ps[0])
            
        if not primary_name:
            # Fallback to top business owner
            # We sort businesses by their property count within the network
            sorted_bs = sorted(businesses, key=lambda b: biz_props.get(b, 0), reverse=True)
            if sorted_bs:
                primary_name = biz_names.get(sorted_bs[0])
            else:
                primary_name = "Unknown Network"

        # 3. Store network record
        with conn.cursor() as cur:
            cur.execute("INSERT INTO networks_shadow (primary_name, business_count, principal_count) VALUES (%s, %s, %s) RETURNING id",
                        (primary_name, len(businesses), len(principals)))
            net_id = cur.fetchone()[0]
            
        for node_type, node_id in group:
            name = biz_names.get(node_id) if node_type == 'business' else prin_names.get(node_id)
            if not name: continue
            norm = normalize_business_name(name) if node_type == 'business' else normalize_person_name(name)
            entity_links.append((net_id, node_type, node_id, name, norm))

        if len(entity_links) >= 10000:
            with conn.cursor() as cur:
                execute_values(cur, "INSERT INTO entity_networks_shadow (network_id, entity_type, entity_id, entity_name, normalized_name) VALUES %s", entity_links)
            conn.commit(); entity_links = []

    if entity_links:
        with conn.cursor() as cur:
            execute_values(cur, "INSERT INTO entity_networks_shadow (network_id, entity_type, entity_id, entity_name, normalized_name) VALUES %s", entity_links)
        conn.commit()
    logger.info("✅ PHASE 4 COMPLETE.")

def run_full_rebuild():
    conn = get_db_connection()
    try:
        # 0. Clear stale links (to remove old name-based IDs)
        logger.info("🧹 PHASE 0: Clearing stale property links...")
        with conn.cursor() as cur:
            cur.execute("UPDATE properties SET business_id = NULL, principal_id = NULL")
        conn.commit()

        link_properties_to_entities(conn)
        graph = build_graph(conn)
        
        # Seed nodes: Entities that own properties
        logger.info("Gathering seed nodes...")
        seeds = set()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT DISTINCT business_id FROM properties WHERE business_id IS NOT NULL")
            for r in cur: seeds.add(('business', r['business_id']))
            cur.execute("SELECT DISTINCT principal_id FROM properties WHERE principal_id IS NOT NULL")
            for r in cur: 
                try:
                    pid = int(r['principal_id'])
                    seeds.add(('principal', pid))
                except (ValueError, TypeError):
                    continue
            
        networks = discover_networks_depth_limited(graph, list(seeds))
        store_networks_shadow(conn, networks)
    finally:
        conn.close()

if __name__ == "__main__":
    run_full_rebuild()
