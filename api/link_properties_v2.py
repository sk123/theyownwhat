import os
import time
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import logging
import sys
import re

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from shared_utils import normalize_business_name, normalize_person_name, get_name_variations, normalize_mailing_address

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

def link_properties():
    conn = get_db_connection()
    try:
        # 1. Load Businesses and their Variations
        logger.info("Loading Businesses and generating variations...")
        b_map = {} # NormName -> business_id
        addr_to_bid = {} # NormAddr -> business_id
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, name, business_address, mail_address FROM businesses")
            for row in cur:
                bid = row['id']
                # Name variations
                for var in get_name_variations(row['name'], 'business'):
                    if var not in b_map: b_map[var] = bid
                
                # Address matching (Normalize addresses)
                # We use both business and mailing address for the business entity
                addresses = []
                if row['business_address']: addresses.append(row['business_address'])
                if row['mail_address']: addresses.append(row['mail_address'])
                
                for addr in addresses:
                    norm_addr = normalize_mailing_address(addr)
                    if norm_addr:
                        if norm_addr not in addr_to_bid:
                            addr_to_bid[norm_addr] = bid

        # 2. Load Unique Principals and their Variations
        logger.info("Loading Unique Principals and generating variations...")
        p_map = {} # NormName -> principal_id
        addr_to_pid = {} # NormAddr -> principal_id
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Join with raw principals to get addresses
            cur.execute("""
                SELECT up.principal_id, up.name_normalized, up.representative_name_c, p.address
                FROM unique_principals up
                LEFT JOIN principal_business_links pbl ON pbl.principal_id = up.principal_id
                LEFT JOIN principals p ON p.business_id = pbl.business_id AND 
                     (UPPER(p.name_c) = UPPER(up.representative_name_c) OR UPPER(p.name_c) = UPPER(up.name_normalized))
            """)
            for row in cur:
                pid = row['principal_id']
                # Name variations
                for var in get_name_variations(row['representative_name_c'], 'principal'):
                    if var not in p_map: p_map[var] = pid
                if row['name_normalized'] not in p_map:
                    p_map[row['name_normalized']] = pid
                
                # Address matching
                if row['address']:
                    norm_addr = normalize_mailing_address(row['address'])
                    if norm_addr and norm_addr not in addr_to_pid:
                        addr_to_pid[norm_addr] = pid

        logger.info(f"Loaded mappings: {len(b_map)} biz names, {len(p_map)} prin names, {len(addr_to_bid)} biz addrs, {len(addr_to_pid)} prin addrs.")
        
        # 3. Process Properties
        logger.info("Fetching Properties...")
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, owner, co_owner, location, normalized_address, mailing_address FROM properties WHERE owner IS NOT NULL")
            all_rows = cur.fetchall()
        
        logger.info(f"Fetched {len(all_rows)} properties. processing linkage...")
        
        batch_updates = []
        b_name_links = 0
        p_name_links = 0
        loc_links = 0
        mail_links = 0
        
        BATCH_SIZE = 10000
        total = len(all_rows)
        
        for i, row in enumerate(all_rows):
            prop_id = row['id']
            owner = row['owner']
            co_owner = row['co_owner']
            loc = row['location']
            norm_loc_addr = row['normalized_address'] # From geocoder if available
            mail_addr = row['mailing_address']
            
            bid = None
            pid = None
            owner_norm = normalize_business_name(owner)
            
            # --- PHASE A: Name Match ---
            if owner_norm:
                bid = b_map.get(owner_norm)
                pid = p_map.get(owner_norm)
                if bid: b_name_links += 1
                if pid: p_name_links += 1

            # --- PHASE B: Address Match (Fallbacks) ---
            if not bid and not pid:
                # 1. Location Match
                addresses_to_check = []
                if loc: addresses_to_check.append(loc)
                if norm_loc_addr: addresses_to_check.append(norm_loc_addr)
                
                for addr in addresses_to_check:
                    n_addr = normalize_mailing_address(addr)
                    if n_addr:
                        bid = addr_to_bid.get(n_addr)
                        pid = addr_to_pid.get(n_addr)
                        if bid or pid:
                            loc_links += 1
                            break
                            
            if not bid and not pid:
                # 2. Mailing Address Match
                if mail_addr:
                    n_mail = normalize_mailing_address(mail_addr)
                    if n_mail:
                        bid = addr_to_bid.get(n_mail)
                        pid = addr_to_pid.get(n_mail)
                        if bid or pid:
                            mail_links += 1

            if bid or pid:
                batch_updates.append((owner_norm, bid, pid, prop_id))
            
            if len(batch_updates) >= BATCH_SIZE:
                with conn.cursor() as write_cursor:
                    execute_values(write_cursor,
                        """UPDATE properties AS p 
                           SET owner_norm = v.owner_norm, business_id = v.business_id, principal_id = v.principal_id
                           FROM (VALUES %s) AS v(owner_norm, business_id, principal_id, id) 
                           WHERE p.id = v.id""",
                        batch_updates
                    )
                conn.commit()
                batch_updates = []
                logger.info(f"Processed {i+1}/{total}... Links: {b_name_links} BizName, {p_name_links} PrinName, {loc_links} Loc, {mail_links} Mail")

        # Final batch
        if batch_updates:
            with conn.cursor() as write_cursor:
                execute_values(write_cursor,
                    """UPDATE properties AS p 
                       SET owner_norm = v.owner_norm, business_id = v.business_id, principal_id = v.principal_id
                       FROM (VALUES %s) AS v(owner_norm, business_id, principal_id, id) 
                       WHERE p.id = v.id""",
                    batch_updates
                )
            conn.commit()
                
        logger.info(f"✅ Complete.")
        logger.info(f"   Name Matches: {b_name_links} Biz, {p_name_links} Prin")
        logger.info(f"   Address Matches: {loc_links} Location, {mail_links} Mailing")
        
    finally:
        conn.close()

if __name__ == "__main__":
    link_properties()
