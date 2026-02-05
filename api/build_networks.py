import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from collections import defaultdict
import logging
import sys

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from shared_utils import normalize_business_name, normalize_person_name, get_email_match_key

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

class UnionFind:
    def __init__(self):
        self.parent = {}
    
    def find(self, i):
        if i not in self.parent:
            self.parent[i] = i
        if self.parent[i] != i:
            self.parent[i] = self.find(self.parent[i])
        return self.parent[i]
    
    def union(self, i, j):
        root_i = self.find(i)
        root_j = self.find(j)
        if root_i != root_j:
            self.parent[root_i] = root_j
            return True
        return False

    def get_components(self):
        components = defaultdict(list)
        for node in self.parent:
            root = self.find(node)
            components[root].append(node)
        return components

def setup_schema(conn):
    logger.info("Setting up network schema...")
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS entity_networks CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS networks CASCADE;")
        
        cursor.execute("""
            CREATE TABLE networks (
                id SERIAL PRIMARY KEY, 
                primary_name TEXT, 
                business_count INTEGER DEFAULT 0,
                principal_count INTEGER DEFAULT 0, 
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("""
            CREATE TABLE entity_networks (
                network_id INTEGER REFERENCES networks(id) ON DELETE CASCADE,
                entity_type TEXT NOT NULL CHECK (entity_type IN ('business', 'principal')),
                entity_id TEXT NOT NULL, 
                entity_name TEXT, 
                normalized_name TEXT,
                PRIMARY KEY (network_id, entity_type, entity_id)
            );
        """)
        cursor.execute("CREATE INDEX idx_entity_networks_lookup ON entity_networks(entity_type, entity_id);")
        conn.commit()

def load_data_and_build_graph(conn):
    uf = UnionFind()
    
    email_rules = {
        # Webmail & Common Providers (Match full email)
        'gmail.com': 'public', 'yahoo.com': 'public', 'hotmail.com': 'public', 'outlook.com': 'public', 'aol.com': 'public',
        'live.com': 'public', 'msn.com': 'public', 'rocketmail.com': 'public', 'protonmail.com': 'public', 'proton.me': 'public',
        'zoho.com': 'public', 'yandex.com': 'public', 'gmx.com': 'public', 'mail.com': 'public', 'inbox.com': 'public',
        'fastmail.com': 'public', 'fastmail.fm': 'public', 'hushmail.com': 'public',
        
        # Apple / Cloud (Match full email)
        'icloud.com': 'public', 'me.com': 'public', 'mac.com': 'public',
        
        # ISPs (Match full email)
        'comcast.net': 'public', 'verizon.net': 'public', 'att.net': 'public', 'sbcglobal.net': 'public', 'cox.net': 'public',
        'snet.net': 'public', 'snet.com': 'public', 'optonline.net': 'public', 'optonline.com': 'public', 'charter.net': 'public',
        'frontier.com': 'public', 'frontiernet.net': 'public', 'earthlink.net': 'public', 'juno.com': 'public', 'netzero.net': 'public',
        'mindspring.com': 'public', 'roadrunner.com': 'public', 'rr.com': 'public', 'centurylink.net': 'public',
        'windstream.net': 'public', 'cablevision.com': 'public', 'bell.net': 'public', 'shaw.ca': 'public',
        'sympatico.ca': 'public', 'rogers.com': 'public', 'telus.net': 'public',
        
        # Education (Match full email)
        'yale.edu': 'public', 'aya.yale.edu': 'public', 'uchc.edu': 'public', 'uconn.edu': 'public',
        
        # Registrars & Agents (Ignore entirely)
        'cscinfo.com': 'registrar', 
        'incfile.com': 'registrar', 
        'cscglobal.com': 'registrar', 
        'northwestregisteredagent.com': 'registrar',
        'wolterskluwer.com': 'registrar',
        'cogencyglobal.com': 'registrar',
        'legalzoom.com': 'registrar',
        'registeredagentsinc.com': 'registrar',
        'registeredagentinc.com': 'registrar',
        'regesteredinc.com': 'registrar', # User noted typo variation
        'zenbusiness.com': 'registrar',
        'ct.gov': 'registrar', # Department of State / Govt agents
    }
    
    known_connectors = {
        'SUNRUN INC', 'SUNRUN INC.', 'VIVINT SOLAR', 'SOLARCITY', 'POSIGEN', 'UNKNOWN', 'CURRENT OWNER', 'OWNER'
    }
    
    email_map = {}
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        logger.info("Filtering Businesses: Loading only active property owners...")
        cur.execute("""
            SELECT b.id, b.name, b.business_email_address 
            FROM businesses b
            JOIN (SELECT DISTINCT business_id FROM properties WHERE business_id IS NOT NULL) p 
              ON p.business_id = b.id
            WHERE b.id IS NOT NULL 
              AND UPPER(b.name) NOT IN ('UNKNOWN', 'CURRENT OWNER', 'OWNER')
        """)
        
        count = 0
        for row in cur:
            bid = row['id']
            email = row['business_email_address']
            domain = email.split('@')[1].lower() if email and '@' in email else None
            
            uf.find(bid)
            
            key = get_email_match_key(email, email_rules)
            if key:
                if key in email_map:
                    original_bid = email_map[key]
                    if uf.union(bid, original_bid):
                        # Only log if it's a custom domain merge (not a full public email)
                        if domain and key == domain:
                            logger.info(f"🔗 Merged business {bid} ('{row['name']}') into network via custom domain: @{key}")
                else:
                    email_map[key] = bid
                    
            count += 1
            if count % 100000 == 0:
                logger.info(f"Processed {count} businesses...")
                
    logger.info(f"Processed {count} businesses. Found {len(email_map)} unique email grouping keys.")

    # 2. Loading Unique Principals (Deduplicated) and grouping by principal_id
    logger.info("Loading deduplicated Principals and grouping by principal_id...")
    
    exclude_list = "', '".join(known_connectors)
    principal_map = {}
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT up.principal_id, pbl.business_id, up.name_normalized
            FROM unique_principals up
            JOIN principal_business_links pbl ON pbl.principal_id = up.principal_id
            JOIN (
                SELECT DISTINCT b.id
                FROM businesses b
                JOIN properties prop ON prop.business_id = b.id
            ) ct_b ON ct_b.id = pbl.business_id
            WHERE up.name_normalized NOT IN ('{exclude_list}')
        """)
        
        count = 0
        for row in cur:
            principal_id = row['principal_id']
            bid = row['business_id']
            
            uf.find(bid)
            
            if principal_id in principal_map:
                uf.union(bid, principal_map[principal_id])
            else:
                principal_map[principal_id] = bid
            
            count += 1
            if count % 100000 == 0:
                logger.info(f"Processed {count} principal links...")

    logger.info(f"Processed {count} principal links. Found {len(principal_map)} unique principals linking businesses.")
    return uf

def save_networks(conn, uf):
    """
    Groups businesses by network (from UnionFind) and saves to database.
    """
    logger.info("Grouping components and saving to database...")
    components = uf.get_components()
    logger.info(f"Found {len(components)} unique networks (connected components).")
    
    # Load Business Names for metadata
    logger.info("Loading business names for metadata...")
    b_names = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM businesses")
        for r in cur:
            b_names[r[0]] = r[1]

    # Load Principal Names and their frequency in each network to pick a good primary name
    logger.info("Picking representative names for networks...")
    p_names = {} # principal_id -> name
    biz_to_prins = defaultdict(list) # bid -> [principal_id]
    with conn.cursor() as cur:
        # Use representative_name_c if available, else name_normalized
        cur.execute("SELECT principal_id, COALESCE(representative_name_c, name_normalized) FROM unique_principals")
        for r in cur:
            p_names[r[0]] = r[1]
        cur.execute("SELECT business_id, principal_id FROM principal_business_links")
        for r in cur:
            biz_to_prins[r[0]].append(r[1])

    network_rows = []
    component_bids = []
    
    for root, bids in components.items():
        # Count occurrences of each principal across all businesses in this component
        p_counts = defaultdict(int)
        b_counts = defaultdict(int)
        for bid in bids:
            b_name = b_names.get(bid)
            if b_name:
                b_counts[b_name] += 1
            for pid in biz_to_prins.get(bid, []):
                p_counts[pid] += 1
        
        primary = "Unknown Network"
        if p_counts:
            # 1. Separate principals into "Corporate" vs "Human" (heuristic)
            def is_corporate(name):
                # Common corporate suffixes/keywords
                keywords = ['LLC', 'INC', 'CORP', 'LTD', 'REALTY', 'MANAGEMENT', 'PROPERTIES', 'GROUP', 'HOLDINGS', 'ASSOCIATES', 'PARTNERS', 'TRUST', 'ESTATE', 'HOUSING', 'APTS', 'APARTMENTS', 'CONDO', 'CONDOMINIUM']
                upper = name.upper()
                # Check suffix or presence of keywords
                parts = upper.split()
                if not parts: return False
                if parts[-1].replace('.', '') in keywords: return True
                for k in keywords:
                    if f" {k} " in f" {upper} " or f" {k}," in f" {upper} ": return True
                return False

            human_candidates = []
            corporate_candidates = []
            
            for pid, count in p_counts.items():
                name = p_names.get(pid, "Unknown")
                if is_corporate(name):
                    corporate_candidates.append((pid, count, name))
                else:
                    human_candidates.append((pid, count, name))
            
            # Sort by count (desc)
            human_candidates.sort(key=lambda x: x[1], reverse=True)
            corporate_candidates.sort(key=lambda x: x[1], reverse=True)
            
            if human_candidates:
                # We have humans!
                # If we have 2 significant humans, combine them.
                # Significant = top 2.
                if len(human_candidates) >= 2:
                    p1 = human_candidates[0]
                    p2 = human_candidates[1]
                    # Only combine if the second one is somewhat significant? (e.g. > 20% of first?)
                    # User request: "Zvi Horowitz and Samuel Pollack".
                    # Let's just always combine top 2 if available to be safe/inclusive for partners.
                    primary = f"{p1[2]} & {p2[2]}"
                else:
                    primary = human_candidates[0][2]
            elif corporate_candidates:
                # Fallback to top corporate principal
                primary = corporate_candidates[0][2]
            else:
                # Should not happen if p_counts is non-empty
                best_pid = max(p_counts.items(), key=lambda x: x[1])[0]
                primary = p_names.get(best_pid)
        
        # Fallback to business name if no network name determined yet
        if not primary or primary == "Unknown Network":
            if b_counts:
                primary = max(b_counts.items(), key=lambda x: x[1])[0]
            elif bids:
                primary = b_names.get(bids[0], "Unknown Network")
            
        network_rows.append((primary, len(bids)))
        component_bids.append(bids)
        
    # Insert Networks and get IDs
    logger.info("Inserting network headers...")
    network_ids = []
    with conn.cursor() as cur:
        for row in network_rows:
            cur.execute("INSERT INTO networks (primary_name, business_count) VALUES (%s, %s) RETURNING id", row)
            network_ids.append(cur.fetchone()[0])
    
    logger.info(f"Inserted {len(network_ids)} network headers.")
    
    logger.info("Inserting entity_networks (Businesses)...")
    entity_rows = []
    for i, nid in enumerate(network_ids):
        bids = component_bids[i]
        for bid in bids:
            name = b_names.get(bid, "Unknown")
            entity_rows.append((nid, 'business', bid, name, normalize_business_name(name)))
            
    with conn.cursor() as cursor:
        execute_values(cursor, 
            """
            INSERT INTO entity_networks (network_id, entity_type, entity_id, entity_name, normalized_name) 
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            entity_rows, page_size=5000
        )
    
    logger.info("Inserting entity_networks (Principals from unique_principals)...")
    with conn.cursor() as cursor:
        # Batch 1: ID-based indexing
        cursor.execute("""
            INSERT INTO entity_networks (network_id, entity_type, entity_id, entity_name, normalized_name)
            SELECT DISTINCT ON (en.network_id, up.principal_id)
                en.network_id,
                'principal',
                CAST(up.principal_id AS TEXT),
                up.representative_name_c,
                up.name_normalized
            FROM unique_principals up
            JOIN principal_business_links pbl ON pbl.principal_id = up.principal_id
            JOIN entity_networks en ON en.entity_id = pbl.business_id
            WHERE en.entity_type = 'business'
            ON CONFLICT DO NOTHING;
        """)
        
        # Batch 2: Name-based indexing (fallback for API)
        cursor.execute("""
            INSERT INTO entity_networks (network_id, entity_type, entity_id, entity_name, normalized_name)
            SELECT DISTINCT ON (en.network_id, up.name_normalized)
                en.network_id,
                'principal',
                up.name_normalized,
                up.representative_name_c,
                up.name_normalized
            FROM unique_principals up
            JOIN principal_business_links pbl ON pbl.principal_id = up.principal_id
            JOIN entity_networks en ON en.entity_id = pbl.business_id
            WHERE en.entity_type = 'business'
            ON CONFLICT DO NOTHING;
        """)
    
    # Update Principal Counts
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE networks n
            SET principal_count = sub.cnt
            FROM (
                SELECT network_id, COUNT(*) as cnt
                FROM entity_networks
                WHERE entity_type = 'principal'
                GROUP BY network_id
            ) sub
            WHERE n.id = sub.network_id;
        """)
        
    conn.commit()
    logger.info("✅ Network Discovery Complete.")


LOCK_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'maintenance.lock')

def create_lock_file():
    try:
        with open(LOCK_FILE_PATH, 'w') as f:
            f.write(str(time.time()))
        logger.info(f"🔒 Created lock file at {LOCK_FILE_PATH}")
    except Exception as e:
        logger.error(f"Failed to create lock file: {e}")

def remove_lock_file():
    try:
        if os.path.exists(LOCK_FILE_PATH):
            os.remove(LOCK_FILE_PATH)
            logger.info(f"🔓 Removed lock file at {LOCK_FILE_PATH}")
    except Exception as e:
        logger.error(f"Failed to remove lock file: {e}")

def main():
    logger.info("🚀 Starting Network Discovery (Recursive/Connected Components)...")
    create_lock_file()
    conn = get_db_connection()
    try:
        setup_schema(conn)
        uf = load_data_and_build_graph(conn)
        save_networks(conn, uf)
        logger.info("✅ Network Discovery Complete.")
    except Exception as e:
        logger.exception("❌ Network Discovery Failed")
        raise e
    finally:
        conn.close()
        remove_lock_file()

if __name__ == "__main__":
    main()

