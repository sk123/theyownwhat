import os
import sys
import time
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import logging

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
    Normalize a principal name:
    - Uppercase and trim
    - Remove punctuation
    - Handle common typos/variations
    - Remove suffixes (Jr, Sr, III, etc)
    - Remove middle initials (single letters)
    - Keep exact first + last name
    """
    if not name_c:
        return ''
    
    # Uppercase, trim, remove punctuation
    name = name_c.strip().upper().replace(',', ' ').replace('.', ' ')
    
    # Handle common typos/variations
    name = name.replace('GUREVITOH', 'GUREVITCH').replace('MANACHEM', 'MENACHEM').replace('MENACHERM', 'MENACHEM').replace('MENAHEM', 'MENACHEM').replace('GURAVITCH', 'GUREVITCH')
    
    parts = [part.strip() for part in name.split() if part.strip()]
    
    if not parts:
        return ''
    
    # Common suffixes to remove
    suffixes = {'JR', 'SR', 'II', 'III', 'IV', 'V', 'ESQ', 'PHD', 'MD', 'DDS', 'DMD', 'DVM'}
    
    # Remove suffixes from end
    while parts and parts[-1] in suffixes:
        parts.pop()
    
    if not parts:
        return ''
    
    # Remove middle initials (single letters between first and last)
    if len(parts) >= 3:
        first = parts[0]
        last = parts[-1]
        # Keep only middle parts that are >1 letter
        middle = [p for p in parts[1:-1] if len(p) > 1]
        parts = [first] + middle + [last]
    
    return ' '.join(parts)


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
              AND UPPER(p.name_c) NOT IN ('UNKNOWN', 'CURRENT OWNER', 'OWNER')
        """)
        principals = cursor.fetchall()
    
    logger.info(f"Loaded {len(principals):,} principal records.")
    
    # Step 2: Grouping in-memory
    logger.info("Grouping in-memory...")
    groups = {}  # normalized_name -> {repr_name, emails: set, biz_ids: set}
    
    for p in principals:
        norm = normalize_name(p['name_c'])
        if not norm: continue
        
        if norm not in groups:
            groups[norm] = {
                'repr_name': p['name_c'],
                'emails': set(),
                'biz_ids': set()
            }
        
        email = normalize_email(p['business_email_address'])
        if email: groups[norm]['emails'].add(email)
        if p['business_id']: groups[norm]['biz_ids'].add(p['business_id'])
    
    logger.info(f"Identified {len(groups):,} unique principals.")
    
    # Step 3: Fast Insertion of unique_principals
    logger.info("Inserting unique_principals in bulk...")
    up_rows = []
    for norm, data in groups.items():
        email = list(data['emails'])[0] if data['emails'] else None
        up_rows.append((norm, email, data['repr_name'], len(data['biz_ids'])))
    
    with conn.cursor() as cursor:
        execute_values(cursor, 
            "INSERT INTO unique_principals (name_normalized, email_normalized, representative_name_c, business_count) VALUES %s",
            up_rows, page_size=10000)
    conn.commit()
    
    # Step 4: Map name back to principal_id for links
    logger.info("Mapping names to generated principal_ids...")
    name_to_id = {}
    with conn.cursor() as cursor:
        cursor.execute("SELECT principal_id, name_normalized FROM unique_principals")
        for pid, norm in cursor:
            name_to_id[norm] = pid
    
    # Step 5: Fast Insertion of principal_business_links
    logger.info("Inserting principal_business_links in bulk...")
    link_rows = []
    for norm, data in groups.items():
        pid = name_to_id.get(norm)
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
