import os
import sys
import time
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import logging
import re

# Add the current directory to Python path for shared_utils if needed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared_utils import normalize_person_name, canonicalize_person_name

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database with retries."""
    retries = 10
    while retries > 0:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            logger.info("✅ Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"⏳ Database not ready, retrying... ({retries} attempts left). Error: {e}")
            retries -= 1
            time.sleep(5)
    raise Exception("❌ Could not connect to the database after multiple retries.")


def normalize_email(email):
    """Normalize an email for deduplication: lowercase, strip whitespace."""
    if not email or not isinstance(email, str):
        return None
    email = email.strip().lower()
    if '@' not in email:
        return None
    return email


def normalize_name(name_c):
    """
    DEPRECATED: Use shared_utils.normalize_person_name instead.
    Keeping for compatibility during transition if needed, but 
    deduplicate_principals now uses canonicalize_person_name.
    """
    return normalize_person_name(name_c)


def create_schema(conn):
    """Create the unique_principals and principal_business_links tables."""
    logger.info("🚀 Creating unique_principals schema...")
    
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS principal_business_links CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS unique_principals CASCADE;")
        
        cursor.execute("""
            CREATE TABLE unique_principals (
                principal_id SERIAL PRIMARY KEY,
                name_normalized TEXT NOT NULL,
                email_normalized TEXT,
                representative_name_c TEXT,
                business_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        cursor.execute("CREATE INDEX idx_unique_principals_name ON unique_principals(name_normalized);")
        cursor.execute("CREATE INDEX idx_unique_principals_email ON unique_principals(email_normalized);")
        
        cursor.execute("""
            CREATE TABLE principal_business_links (
                principal_id INTEGER REFERENCES unique_principals(principal_id) ON DELETE CASCADE,
                business_id TEXT REFERENCES businesses(id) ON DELETE CASCADE,
                PRIMARY KEY (principal_id, business_id)
            );
        """)
        
        cursor.execute("CREATE INDEX idx_pbl_business ON principal_business_links(business_id);")
        cursor.execute("CREATE INDEX idx_pbl_principal ON principal_business_links(principal_id);")
        
    conn.commit()
    logger.info("✅ Schema created successfully.")


def deduplicate_principals(conn):
    """Deduplicate principals by grouping on normalized name."""
    logger.info("🚀 Starting principal deduplication...")
    
    # Step 1: Load all principals with their business emails
    logger.info("Loading principals from database...")
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT 
                p.id as principal_raw_id,
                p.business_id,
                p.name_c,
                b.business_email_address
            FROM principals p
            LEFT JOIN businesses b ON b.id = p.business_id
            WHERE p.name_c IS NOT NULL 
              AND TRIM(p.name_c) != ''
              AND UPPER(TRIM(p.name_c)) NOT IN (
                  'UNKNOWN', 'UNKNOWN OWNER', 'CURRENT OWNER', 'OWNER', 'OCCUPANT', 
                  'CT', 'CONNECTICUT', 'THE', 'INC', 'LLC', 'CORP',
                  'USA', 'UNITED STATES', 'NO NAME', 'N/A', 'NA', 'NONE', 
                  'NO INFORMATION PROVIDED', 'NOT PROVIDED', 
                  'VACANT', 'NULL', 'NOT AVAILABLE', '[UNKNOWN]', 
                  'CURRENT COMPANY OWNER', 'CURRENT COMPANY-OWNER',
                  'SV', 'SURVIVORSHIP', 'JT', 'TIC', 'TC', 'ET AL', 'ETAL', 'LII',
                  'TRUSTEE', 'EXECUTOR', 'ADMINISTRATOR'
              )
        """)
        principals = cursor.fetchall()
    
    logger.info(f"Loaded {len(principals):,} principal records.")
    
    # Step 2: Grouping in-memory with Common Name Guard
    logger.info("Grouping in-memory with Common Name Guard...")
    
    # Heuristic for common names that shouldn't be greedily merged
    COMMON_SURNAMES = {
        'SMITH', 'JOHNSON', 'WILLIAMS', 'BROWN', 'JONES', 'GARCIA', 'MILLER', 'DAVIS', 
        'RODRIGUEZ', 'MARTINEZ', 'HERNANDEZ', 'LOPEZ', 'GONZALEZ', 'WILSON', 'ANDERSON', 
        'THOMAS', 'TAYLOR', 'MOORE', 'JACKSON', 'MARTIN', 'LEE', 'Perez', 'Thompson', 
        'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson'
    }
    
    def is_common_name(name_norm):
        parts = name_norm.split()
        if not parts: return False
        # If it's a 2-word name where one part is a common surname
        if len(parts) <= 2:
            return any(p in COMMON_SURNAMES for p in parts)
        return False

    groups = {}  # group_key -> {repr_name, emails: set, biz_ids: set, norm_name}
    
    for p in principals:
        norm = canonicalize_person_name(p['name_c'])
        if not norm: continue
        
        email = normalize_email(p['business_email_address'])
        
        # Determine grouping key
        if is_common_name(norm):
            if email:
                # Merge common names ONLY if they share an email
                group_key = f"{norm}_em_{email}"
            else:
                # Split common names by business if no email (fallback to separate identities)
                group_key = f"{norm}_biz_{p['business_id']}"
        else:
            # Standard name-based merge for less common names - Unify regardless of email!
            group_key = norm
        
        if group_key not in groups:
            groups[group_key] = {
                'norm_name': norm,
                'repr_name': p['name_c'],
                'emails': set(),
                'biz_ids': set()
            }
        
        if email: groups[group_key]['emails'].add(email)
        if p['business_id']: groups[group_key]['biz_ids'].add(p['business_id'])
    
    logger.info(f"Identified {len(groups):,} unique principal clusters.")
    
    # Step 3: Fast Insertion of unique_principals
    logger.info("Inserting unique_principals in bulk...")
    up_rows = []
    # Note: Multiple clusters might share the same 'norm_name' (if common names were split)
    # We'll insert them as separate unique_principal records.
    for gkey, data in groups.items():
        email = list(data['emails'])[0] if data['emails'] else None
        up_rows.append((data['norm_name'], email, data['repr_name'], len(data['biz_ids'])))
    
    with conn.cursor() as cursor:
        execute_values(cursor, 
            "INSERT INTO unique_principals (name_normalized, email_normalized, representative_name_c, business_count) VALUES %s",
            up_rows, page_size=10000)
    conn.commit()
    
    # Step 4: Map cluster data to generated principal_id
    # Since we have multiple records with same norm_name (for common names), 
    # we need to be careful with mapping.
    # Actually, we can just fetch the IDs in the same order if we are careful, 
    # but it's safer to use a Join or update-style mapping.
    
    # Let's do a more robust mapping by storing the group_key temporarily or using the row order.
    # Better: Add a temp column to unique_principals for the group_key
    with conn.cursor() as cursor:
        cursor.execute("ALTER TABLE unique_principals ADD COLUMN IF NOT EXISTS _temp_gkey TEXT;")
        execute_values(cursor, 
            "UPDATE unique_principals SET _temp_gkey = v.gkey FROM (VALUES %s) AS v(norm, email, repr, bcnt, gkey) WHERE name_normalized = v.norm AND (email_normalized = v.email OR (email_normalized IS NULL AND v.email IS NULL)) AND representative_name_c = v.repr",
            [(r[0], r[1], r[2], r[3], k) for k, r in zip(groups.keys(), up_rows)], page_size=5000)
        # Wait, that's slow. Let's just re-create the table with the key.
    
    # REVISED STEP 3 & 4: Use a clean re-creation
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS principal_business_links CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS unique_principals CASCADE;")
        cursor.execute("""
            CREATE TABLE unique_principals (
                principal_id SERIAL PRIMARY KEY,
                name_normalized TEXT NOT NULL,
                email_normalized TEXT,
                representative_name_c TEXT,
                business_count INTEGER DEFAULT 0,
                group_key TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        up_rows_with_key = []
        for gkey, data in groups.items():
            email = list(data['emails'])[0] if data['emails'] else None
            up_rows_with_key.append((data['norm_name'], email, data['repr_name'], len(data['biz_ids']), gkey))
            
        execute_values(cursor, 
            "INSERT INTO unique_principals (name_normalized, email_normalized, representative_name_c, business_count, group_key) VALUES %s",
            up_rows_with_key, page_size=10000)
        
        cursor.execute("CREATE INDEX idx_up_gkey ON unique_principals(group_key);")
        cursor.execute("""
            CREATE TABLE principal_business_links (
                principal_id INTEGER REFERENCES unique_principals(principal_id) ON DELETE CASCADE,
                business_id TEXT REFERENCES businesses(id) ON DELETE CASCADE,
                PRIMARY KEY (principal_id, business_id)
            );
        """)
    conn.commit()

    # Step 5: Fast Insertion of principal_business_links
    logger.info("Inserting principal_business_links in bulk...")
    
    # Get ID mapping based on group_key
    gkey_to_id = {}
    with conn.cursor() as cursor:
        cursor.execute("SELECT principal_id, group_key FROM unique_principals")
        for pid, gkey in cursor:
            gkey_to_id[gkey] = pid
            
    link_rows = []
    for gkey, data in groups.items():
        pid = gkey_to_id.get(gkey)
        if not pid: continue
        for bid in data['biz_ids']:
            link_rows.append((pid, bid))
    
    # We must ensure businesses exist (or skip the check if we trust the source)
    # The previous script had a check; let's do it faster by filtering biz_ids against businesses table
    logger.info("Filtering valid business IDs...")
    valid_bids = set()
    with conn.cursor() as cursor:
        cursor.execute("SELECT id FROM businesses")
        for (bid,) in cursor:
            valid_bids.add(bid)
    
    final_links = [l for l in link_rows if l[1] in valid_bids]
    logger.info(f"Filtered {len(link_rows) - len(final_links):,} invalid business links.")
    
    with conn.cursor() as cursor:
        execute_values(cursor,
            "INSERT INTO principal_business_links (principal_id, business_id) VALUES %s ON CONFLICT DO NOTHING",
            final_links, page_size=10000)
    conn.commit()
    
    logger.info("✅ Deduplication complete.")

def main():
    if not DATABASE_URL:
        logger.error("❌ Error: DATABASE_URL not set.")
        sys.exit(1)
    
    conn = None
    try:
        conn = get_db_connection()
        create_schema(conn)
        deduplicate_principals(conn)
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        if conn: conn.rollback()
        sys.exit(1)
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()
