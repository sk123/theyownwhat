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
    canonicalize_business_name,
    get_name_variations,
    get_email_match_key
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
MAX_DEPTH = 4 # Increased to 4 per user request

# Placeholder/Generic names to skip during linking to prevent bad merges
SKIP_NAMES = {
    'CURRENT OWNER', 'UNKNOWN OWNER', 'OCCUPANT', 'OWNER', 
    'UNKNOWN', 'CT', 'CONNECTICUT', 'THE', 'INC', 'LLC', 'CORP',
    'USA', 'UNITED STATES', 'NO NAME', 'N/A', 'NA', 'NONE',
    'NO INFORMATION PROVIDED', 'NOT PROVIDED', 'VACANT', 'NULL',
    'NOT AVAILABLE', '[UNKNOWN]', 'CURRENT COMPANY OWNER', 'CURRENT COMPANY-OWNER',
    'SV', 'SURVIVORSHIP', 'JT', 'TIC', 'TC', 'ET AL', 'LII', 'ETAL'
}

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
        # 1. Load Business Map
        b_map = {}
        b_canon_map = {}
        cur.execute("SELECT id, name FROM businesses")
        logger.info("  - Loading business names into memory...")
        b_count = 0
        for row in cur: 
            b_count += 1
            if b_count % 100000 == 0:
                logger.info(f"    Loaded {b_count:,} businesses...")
            norm = normalize_business_name(row['name'])
            if norm: b_map[norm] = row['id']
            
            # Add canonical version as secondary match key
            bc = canonicalize_business_name(row['name'])
            if bc and bc not in b_canon_map: # Prefer earlier (better) names if collision
                b_canon_map[bc] = row['id']
        
        logger.info(f"  - Business map loaded ({len(b_map):,} unique normal, {len(b_canon_map):,} unique canon).")

        # 2. Load Principal Map
        p_map = {}
        cur.execute("SELECT principal_id, name_normalized FROM unique_principals")
        logger.info("  - Loading principal names into memory...")
        p_count = 0
        for row in cur:
            p_count += 1
            if p_count % 100000 == 0:
                logger.info(f"    Loaded {p_count:,} principals...")
            # unique_principals.name_normalized is already fairly clean, but we canonicalize it
            p_map[row['name_normalized']] = row['principal_id']
            canon = canonicalize_person_name(row['name_normalized'])
            if canon: p_map[canon] = row['principal_id']
        
        logger.info(f"  - Principal map loaded ({len(p_map):,} unique names).")
            
        # Use a separate connection for the server-side cursor to avoid closure on commit
        read_conn = get_db_connection()
        try:
            cursor_name = f"prop_linker_{int(time.time())}"
            with read_conn.cursor(name=cursor_name, cursor_factory=RealDictCursor) as sc:
                sc.execute("SELECT id, owner, co_owner FROM properties")
                updates = []
                linked_count = 0
                count = 0
                
                # Fetch in chunks from the server-side cursor
                while True:
                    rows = sc.fetchmany(5000)
                    if not rows: break
                    
                    for row in rows:
                        count += 1
                        if count % 100000 == 0:
                            logger.info(f"    Processed {count:,} properties...")

                        oname = row['owner'] or ''
                        cname = row['co_owner'] or ''
                        
                        onorm = normalize_business_name(oname)
                        cnorm = normalize_business_name(cname)
                        
                        # Pass 1: Primary Business matches
                        bid = None
                        if onorm not in SKIP_NAMES:
                            bid = b_map.get(onorm)
                        if not bid and cnorm not in SKIP_NAMES:
                            bid = b_map.get(cnorm)
                        
                        # Pass 2: Canonical Business matches (Suffix-stripped)
                        if not bid:
                            if onorm not in SKIP_NAMES:
                                bc_owner = canonicalize_business_name(oname)
                                bid = b_canon_map.get(bc_owner)
                            if not bid and cnorm not in SKIP_NAMES:
                                bc_co = canonicalize_business_name(cname)
                                bid = b_canon_map.get(bc_co)
                        
                        # Pass 3: Principal matches (with LAST FIRST ↔ FIRST LAST reversal)
                        pid = None
                        if not bid: # Only check principal if no business found
                            for raw_name in (oname, cname):
                                if pid:
                                    break
                                name_clean = normalize_person_name(raw_name)
                                if not name_clean:
                                    continue
                                # Direct match
                                pid = p_map.get(name_clean)
                                # Canonical match
                                if not pid:
                                    pid = p_map.get(canonicalize_person_name(name_clean))
                                # LAST FIRST → FIRST LAST reversal
                                if not pid:
                                    parts = name_clean.split()
                                    if len(parts) >= 2:
                                        # Try "LAST FIRST" → "FIRST LAST"
                                        reversed_name = f"{parts[-1]} {' '.join(parts[:-1])}"
                                        pid = p_map.get(reversed_name)
                                        if not pid:
                                            pid = p_map.get(canonicalize_person_name(reversed_name))
                                        # Try "FIRST LAST" → "LAST FIRST"  
                                        if not pid:
                                            reversed_name2 = f"{' '.join(parts[1:])} {parts[0]}"
                                            pid = p_map.get(reversed_name2)
                                            if not pid:
                                                pid = p_map.get(canonicalize_person_name(reversed_name2))

                        if bid or pid:
                            updates.append((bid, pid, row['id']))
                            linked_count += 1
                            
                        if len(updates) >= 5000:
                            with conn.cursor() as up_cur: # Use the main connection for updates
                                execute_values(up_cur, """
                                    UPDATE properties AS p
                                    SET business_id = v.bid, principal_id = v.pid
                                    FROM (VALUES %s) AS v(bid, pid, id)
                                    WHERE p.id = v.id
                                """, updates)
                            conn.commit()
                            updates = []
                    
                if updates:
                    with conn.cursor() as up_cur:
                        execute_values(up_cur, """
                            UPDATE properties AS p
                            SET business_id = v.bid, principal_id = v.pid
                            FROM (VALUES %s) AS v(bid, pid, id)
                            WHERE p.id = v.id
                        """, updates)
                    conn.commit()
        finally:
            read_conn.close()
            
    logger.info(f"  - Linked {linked_count:,} out of {count:,} properties.")

# --- Logic 2: Graph Building ---
def build_graph(conn, skip_emails=False):
    """Builds the entity graph using shared attributes."""
    logger.info("🕸️ PHASE 2: Building graph (Integers)...")
    
    # Start IDs:
    # 0 = reserved
    # Positive ints only
    node_to_int = {}
    int_to_node = [] # Index is the int ID
    
    def get_id(node_tuple):
        if node_tuple not in node_to_int:
            node_to_int[node_tuple] = len(int_to_node)
            int_to_node.append(node_tuple)
        return node_to_int[node_tuple]

    # Use defaultdict(list) for adjacency list as it is lighter than set (handle dupes later or ignore)
    # Using arrays would be even better but lists are standard python.
    graph = defaultdict(set) # Using set to avoid edges duplication during build

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT normalized_name FROM principal_ignore_list")
        ignore_names = {row['normalized_name'] for row in cur}

        # Institutional/Service regex to catch pseudo-hubs
        INSTITUTIONAL_PATTERN = re.compile(
            r'HOUSING AUTHORITY|UNIVERSITY|STATE OF |CONNECTICUT STATE|TOWN OF |CITY OF |REDEVELOPMENT|'
            r'BOARD OF EDUCATION|MUNICIPAL|FEDERAL|DEPARTMENT OF|AUTHORITY|'
            r'CORPORATION SERVICE COMPANY|CT CORPORATION|C T CORPORATION|CSC ENTITY|'
            r'NORTHWEST REGISTERED AGENT|COGENCY GLOBAL|VCORP SERVICES|WOLTERS KLUWER|'
            r'NATIONAL REGISTERED AGENTS|REGISTERED AGENT|'
            # Utility companies — prevent mega-network merging through shared agents
            r'WATER COMPANY|LIGHT AND POWER|GAS SERVICES|ELECTRIC COMPANY|POWER COMPANY|'
            r'WATER UTILITY|SEWER |AQUARION|EVERSOURCE|UNITED ILLUMINATING|'
            r'YANKEE GAS|SOUTHERN CONN.* GAS|AVANGRID|BERKSHIRE GAS|CONNECTICUT NATURAL GAS', 
            re.IGNORECASE
        )
        # Whitelist patterns to protect known large legitimate networks from anti-hub filters
        WHITELIST_PATTERN = re.compile(r'GUREVITCH|MANDY|NETZ|ACME', re.IGNORECASE)

        SERVICE_PATTERN = re.compile(r'SERVICES|AGENT|PROPERTY MGMT', re.IGNORECASE)

        # --- LOAD PROPERTY-OWNING ENTITIES FIRST (needed for hub detection) ---
        logger.info("  - Loading Property-Owning Business List...")
        cur.execute("SELECT DISTINCT business_id FROM properties WHERE business_id IS NOT NULL")
        owning_business_ids = {row['business_id'] for row in cur}
        logger.info(f"    Found {len(owning_business_ids):,} businesses with properties.")

        # principals with direct property ownership should also be valid seeds/nodes
        cur.execute("SELECT DISTINCT principal_id FROM properties WHERE principal_id IS NOT NULL")
        owning_principal_ids = set()
        for r in cur:
            try: owning_principal_ids.add(int(r['principal_id']))
            except: pass
        logger.info(f"    Found {len(owning_principal_ids):,} principals with direct properties.")

        # No degree-based hub caps — rely on name-based institutional/service filters only.
        # The property-owning-only filter on line 293 naturally limits the graph.
        hub_principals = set()
        hub_businesses = set()
        logger.info("  - Hub detection: NO degree caps (name-based filters only).")
        
        # A. Principal-Business Links
        logger.info("  - Loading Principal-Business Links (Filtered to Owners)...")
        cur.execute("""
            SELECT pbl.business_id, pbl.principal_id, up.name_normalized as p_name, b.name as b_name
            FROM principal_business_links pbl
            JOIN unique_principals up ON up.principal_id = pbl.principal_id
            JOIN businesses b ON b.id = pbl.business_id
        """)
        
        count = 0
        for row in cur:
            bid = row['business_id']
            pid = row['principal_id']

            # --- FILTER: Must be an owning business ---
            if bid not in owning_business_ids: 
                continue

            if row['p_name'] in ignore_names: continue
            if pid in hub_principals: continue
            if bid in hub_businesses: continue
            
            # Extra safety: name-based filter for institutional entities that might have low degree
            if INSTITUTIONAL_PATTERN.search(row['p_name'] or ""):
                continue
            if INSTITUTIONAL_PATTERN.search(row['b_name'] or ""):
                continue
            
            u_node = ('business', bid)
            v_node = ('principal', pid)
            
            u = get_id(u_node)
            v = get_id(v_node)
            
            graph[u].add(v)
            graph[v].add(u)
            count += 1
            if count % 100000 == 0:
                logger.debug(f"    Loaded {count:,} links...")

        # B. Shared Emails (Filtered)
        if skip_emails:
            logger.info("  - SKIPPING Shared Emails (Diagnostic Mode).")
            return graph, node_to_int, int_to_node

        logger.info("  - Loading Shared Emails...")
        # Force a fresh cursor for rules to ensure no state leakage
        with conn.cursor(cursor_factory=RealDictCursor) as rule_cur:
            rule_cur.execute("SELECT domain, match_type FROM email_match_rules")
            email_rules = {row['domain'].lower().strip(): row['match_type'] for row in rule_cur}
        
        # Hardcoded fail-safe blacklist for known mega-registrars
        BLACKLIST_DOMAINS = {
            'cscinfo.com', 'cscglobal.com', 'wolterskluwer.com', 'incfile.com', 
            'northwestregisteredagent.com', 'cogencyglobal.com', 'registeredagentsinc.com',
            'corporatedocfiling.com', 'corpcreations.com', 'unitedagentgroup.com',
            'primecorporateservices.com', 'starwood.com', 'rohuer.com', 'cncinfo.com',
            'ct.gov' # Institutional bypass
        }
        for d in BLACKLIST_DOMAINS:
            email_rules[d] = 'registrar'

        logger.info(f"  - Loaded {len(email_rules)} email rules.")

        cur.execute("SELECT id, business_email_address FROM businesses WHERE business_email_address IS NOT NULL")
        email_map = defaultdict(list)
        for row in cur:
            # --- FILTER: Must be an owning business ---
            if row['id'] not in owning_business_ids:
                continue

            key = get_email_match_key(row['business_email_address'], email_rules)
            if key: email_map[key].append(row['id']) # Store string ID directly

        for biz_ids in email_map.values():
            if len(biz_ids) > 1: # No upper cap on email group size
                # Convert all to ints first
                int_ids = [get_id(('business', bid)) for bid in biz_ids]
                for i in range(len(int_ids)):
                    for j in range(i+1, len(int_ids)):
                        u, v = int_ids[i], int_ids[j]
                        graph[u].add(v); graph[v].add(u)

        # C. Shared Mailing Addresses
        # DISABLED per user request (2026-02-09) to prevent meganetworks.
        # Addresses should only be used for verification, not graph building.
        # logger.info("  - Loading Shared Addresses...")
        # AGENT_REGEX = re.compile(r'ONE COMMERCIAL PLAZA|CORPORATION SYSTEM|C T CORP|591 W PUTNAM|INCFILE|REGISTERED AGENT|PO BOX|P\.O\. BOX|2389 MAIN ST|2389 MAIN STREET|100 PEARL ST|100 PEARL STREET|280 TRUMBULL ST|280 TRUMBULL STREET|303 CONGRESS ST|303 CONGRESS STREET|124 LASALLE RD|124 LASALLE ROAD|399 WHALLEY|565 ELLSWORTH|80 RED SCHOOLHOUSE|CORPORATION SERVICE|CT CORP', re.I)
        # cur.execute("SELECT id, mail_address, business_address FROM businesses")
        # addr_map = defaultdict(list)
        # for row in cur:
        #     raw = row['mail_address'] or row['business_address']
        #     norm = normalize_mailing_address(raw)
        #     if norm and len(norm) > 10 and not AGENT_REGEX.search(norm):
        #         addr_map[norm].append(row['id'])

        # for biz_ids in addr_map.values():
        #     if 1 < len(biz_ids) < 250:
        #         int_ids = [get_id(('business', bid)) for bid in biz_ids]
        #         for i in range(len(int_ids)):
        #             for j in range(i+1, len(int_ids)):
        #                 u, v = int_ids[i], int_ids[j]
        #                 graph[u].add(v); graph[v].add(u)

    # Convert sets to tuples for memory efficiency (sets have high overhead)
    # We use a dict instead of defaultdict for the final object to prevent accidental growth
    final_graph = {k: tuple(v) for k, v in graph.items()}
    del graph # Free the sets immediately
    
    logger.info(f"✅ PHASE 2 COMPLETE: Graph built with {len(final_graph):,} nodes and {len(int_to_node):,} entities.")
    return final_graph, node_to_int, int_to_node

# --- Logic 3: Depth-Limited Discovery ---
def discover_networks_depth_limited(graph_data, seed_nodes_raw):
    """Discovers networks using Iterative BFS on INTEGER graph."""
    graph, node_to_int, int_to_node = graph_data
    
    logger.info("🔍 PHASE 3: Discovering networks (Iterative BFS + DSU Merging on Integers)...")
    
    # Filter seed nodes to those present in our graph
    # (Seeds might be filtered out by hub rules, etc)
    seed_ints = []
    for s in seed_nodes_raw:
        if s in node_to_int:
            seed_ints.append(node_to_int[s])
            
    logger.info(f"  - Valid seeds count: {len(seed_ints):,} / {len(seed_nodes_raw):,}")
    
    # Initialize DSU for merging seed components
    # Map: Seed_Index -> Parent_Seed_Index
    parent = list(range(len(seed_ints)))

    def find(i):
        if parent[i] == i:
            return i
        root = i
        while parent[root] != root:
            root = parent[root]
        curr = i
        while curr != root:
            next_val = parent[curr]
            parent[curr] = root
            curr = next_val
        return root

    def union(i, j):
        root_i = find(i)
        root_j = find(j)
        if root_i != root_j:
            parent[root_i] = root_j

    # Map: Node_Int -> Seed_Index (owner)
    node_owner = {}
    
    processed_count = 0
    total_seeds = len(seed_ints)
    
    for idx, seed_id in enumerate(seed_ints):
        if idx % 10000 == 0:
            logger.info(f"  - Processing seed {idx:,}/{total_seeds:,}...")

        if seed_id in node_owner:
            continue
            
        # Start new component
        node_owner[seed_id] = idx
        queue = deque([(seed_id, 0)])
        
        while queue:
            node, depth = queue.popleft()
            processed_count += 1
            
            if depth >= MAX_DEPTH: continue
            
            for neighbor in graph.get(node, []):
                if neighbor not in node_owner:
                    node_owner[neighbor] = idx
                    queue.append((neighbor, depth + 1))
                else:
                    # Collision
                    owner_idx = node_owner[neighbor]
                    if find(idx) != find(owner_idx):
                        union(idx, owner_idx)
                        
    # Construct discovered_networks list
    logger.info("  - Compiling network components...")
    components_int = defaultdict(list)
    
    for node_id, owner_idx in node_owner.items():
        root = find(owner_idx)
        components_int[root].append(node_id)
        
    # Convert ints back to node tuples
    discovered_networks = []
    for comp_ints in components_int.values():
        network = [int_to_node[i] for i in comp_ints]
        discovered_networks.append(network)

    logger.info(f"✅ PHASE 3 COMPLETE: Found {len(discovered_networks):,} distinct networks.")
    return discovered_networks

# --- Logic 4: Storage ---
def store_networks_shadow(conn, networks, graph_data=None):
    """Stores discovered networks into shadow tables with property-weighted human naming."""
    logger.info("💾 PHASE 4: Storing to shadow tables...")
    with conn.cursor() as cur:
        # ownership_links_shadow is truncated in setup_shadow_tables in safe_network_refresh, 
        # but if running standalone it might need truncation. 
        # Safe to truncate here if we assume this is the only writer.
        cur.execute("TRUNCATE networks_shadow, entity_networks_shadow, ownership_links_shadow RESTART IDENTITY;")
    
    # Metadata loading
    biz_names = {}; prin_names = {}
    biz_props = defaultdict(int); prin_props = defaultdict(int)
    biz_values = defaultdict(float); prin_values = defaultdict(float)
    biz_to_prins = defaultdict(list)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        logger.info("  - Loading names...")
        cur.execute("SELECT id, name FROM businesses"); biz_names = {r['id']: r['name'] for r in cur}
        cur.execute("SELECT principal_id, representative_name_c FROM unique_principals"); prin_names = {r['principal_id']: r['representative_name_c'] for r in cur}
        
        logger.info("  - Loading property weights...")
        cur.execute("SELECT business_id, COUNT(*) as count, SUM(COALESCE(assessed_value, 0)) as val FROM properties WHERE business_id IS NOT NULL GROUP BY business_id")
        for r in cur: 
            biz_props[r['business_id']] = r['count']
            biz_values[r['business_id']] = float(r['val'])
        
        cur.execute("SELECT principal_id, COUNT(*) as count, SUM(COALESCE(assessed_value, 0)) as val FROM properties WHERE principal_id IS NOT NULL GROUP BY principal_id")
        for r in cur: 
            try:
                pid = int(r['principal_id'])
                prin_props[pid] = r['count']
                prin_values[pid] = float(r['val'])
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
        for suffix in [' LLC', ' INC', ' CORP', ' LTD', ' LP ', ' LLP', ' TRUST', ' ESTATE', ' ASSOC', 'HOLDING', 'PROPERT', 'REALTY', 'MANAGEMENT', 'PARTNERS', 'VENTURES', 'DEVELOPMENT', 'INVESTMENT', 'SERVIC', 'YANKEE', 'POWER', 'LIGHT', ' GAS ', 'ELECTRIC', 'UTILITY']:
            if suffix in name_upper: return False
        return True

    owning_principals = set(prin_props.keys())
    owning_businesses = set(biz_props.keys())

    entity_links = []
    
    # Prepare for edge storage
    edges_to_store = []
    graph_map = None
    node_to_int = None
    if graph_data:
        graph_map, node_to_int, _ = graph_data
        logger.info("  - Graph data provided. Will store edges.")

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
            
        # Header candidates MUST be active owners (direct or indirect)
        active_ps = [p for p in principals if p_weights.get(p, 0) > 0 and normalize_person_name(prin_names.get(p)) not in ignore_names_set]
        
        # Sort active principals: First by human-likeness, then by property influence
        sorted_ps = sorted(
            active_ps, 
            key=lambda p: (is_human_like(prin_names.get(p)), p_weights.get(p, 0)), 
            reverse=True
        )
        
        primary_name = None
        if sorted_ps:
            primary_name = prin_names.get(sorted_ps[0])
            
        if not primary_name:
            # Fallback to top active business owner
            active_bs = [b for b in businesses if b in owning_businesses]
            sorted_bs = sorted(active_bs, key=lambda b: biz_props.get(b, 0), reverse=True)
            if sorted_bs:
                primary_name = biz_names.get(sorted_bs[0])
            else:
                primary_name = "Unknown Network"
        
        # User Feedback Fix: Force human name if available in network even if business has more direct props
        # (This is already handled by sorted_ps priority if they have ANY weighted props)

        # 3. Store network record
        # Visible counts only include active owners
        visible_p_count = len([p for p in principals if p in owning_principals])
        visible_b_count = len([b for b in businesses if b in owning_businesses])
        
        # Calculate totals across the entire network (not just visible in sidebar if we want full count)
        net_total_props = sum(prin_props.get(p, 0) for p in principals) + sum(biz_props.get(b, 0) for b in businesses)
        net_total_value = sum(prin_values.get(p, 0) for p in principals) + sum(biz_values.get(b, 0) for b in businesses)

        with conn.cursor() as cur:
            cur.execute("INSERT INTO networks_shadow (primary_name, business_count, principal_count, total_properties, total_assessed_value) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                        (primary_name, visible_b_count, visible_p_count, net_total_props, net_total_value))
            net_id = cur.fetchone()[0]
            
        # Identify principals who are connected to owning businesses (indirect ownership)
        principals_via_businesses = set()
        for bid in businesses:
            if bid in owning_businesses:
                for pid in biz_to_prins.get(bid, []):
                    principals_via_businesses.add(pid)
        
        # STORE NODES
        for node_type, node_id in group:
            # RESTRICTED: Only insert owning entities into the link table (sidebar)
            if node_type == 'business' and node_id not in owning_businesses:
                continue
            if node_type == 'principal':
                try:
                    pid = int(node_id)
                    # Include if directly owns property OR indirectly via business
                    if pid not in owning_principals and pid not in principals_via_businesses:
                        continue
                except: continue

            name = biz_names.get(node_id) if node_type == 'business' else prin_names.get(node_id)
            if not name: continue
            norm = normalize_business_name(name) if node_type == 'business' else normalize_person_name(name)
            entity_links.append((net_id, node_type, node_id, name, norm))

        # STORE EDGES (Optimization: Only if graph data provided)
        if graph_map:
            # Map node tuple to entity name for storage
            # We want to store edges between ANY nodes in the network, not just "visible" ones, 
            # because the invisible ones (e.g. non-owning businesses) might be the bridge.
            
            # 1. Get Int IDs for all nodes in this network group
            group_ints = []
            node_map = {} # Int -> Name
            
            for node in group:
                if node in node_to_int:
                    idx = node_to_int[node]
                    group_ints.append(idx)
                    
                    # Resolve name
                    n_name = None
                    if node[0] == 'business': n_name = biz_names.get(node[1])
                    else: n_name = prin_names.get(node[1]) # ID is already int or string matching key
                    
                    if n_name: node_map[idx] = n_name

            # 2. Check edges between them
            # We iterate all pairs or check adjacency. Adjacency is O(E).
            for u_idx in group_ints:
                u_name = node_map.get(u_idx)
                if not u_name: continue
                
                # Check neighbors in graph
                neighbors = graph_map.get(u_idx, [])
                for v_idx in neighbors:
                    # Undirected graph, we will see (u,v) and (v,u). 
                    # Store both or canonicalize? App usually expects directed link rows or handles duplication.
                    # We'll store (u,v) where u < v to save space, OR store all for easiest querying.
                    if v_idx > u_idx and v_idx in node_map: # Check if neighbor is in THIS network (it should be)
                        v_name = node_map.get(v_idx)
                        
                        # Infer type
                        # If one is Principal and other Business -> 'principal_link'
                        # If Business and Business -> 'shared_contact'
                        # If Principal and Principal -> 'shared_contact' (rare)
                        
                        # We need types from the node tuples, but we only have names in node_map.
                        # Recover types from group list is slow.
                        # Let's assume P-B if names are different types... wait names don't have types.
                        # We can use the graph_data int_to_node to look up types.
                        
                        u_node = graph_data[2][u_idx]
                        v_node = graph_data[2][v_idx]
                        
                        l_type = 'link'
                        if u_node[0] != v_node[0]: l_type = 'principal_link'
                        elif u_node[0] == 'business': l_type = 'shared_contact'
                        
                        edges_to_store.append((net_id, u_name, v_name, l_type))

        if len(entity_links) >= 10000:
            with conn.cursor() as cur:
                execute_values(cur, "INSERT INTO entity_networks_shadow (network_id, entity_type, entity_id, entity_name, normalized_name) VALUES %s", entity_links)
            conn.commit(); entity_links = []
            
        if len(edges_to_store) >= 10000:
            with conn.cursor() as cur:
                execute_values(cur, "INSERT INTO ownership_links_shadow (network_id, from_entity, to_entity, link_type) VALUES %s", edges_to_store)
            conn.commit(); edges_to_store = []

    if entity_links:
        with conn.cursor() as cur:
            execute_values(cur, "INSERT INTO entity_networks_shadow (network_id, entity_type, entity_id, entity_name, normalized_name) VALUES %s", entity_links)
        conn.commit()
        
    if edges_to_store:
        with conn.cursor() as cur:
            execute_values(cur, "INSERT INTO ownership_links_shadow (network_id, from_entity, to_entity, link_type) VALUES %s", edges_to_store)
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
        graph, node_to_int, int_to_node = build_graph(conn)
        
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
            
        graph_data = (graph, node_to_int, int_to_node)
        networks = discover_networks_depth_limited(graph_data, list(seeds))
        store_networks_shadow(conn, networks, graph_data=graph_data)
    finally:
        conn.close()

if __name__ == "__main__":
    run_full_rebuild()
