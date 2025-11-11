# evaluate_networks.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import sys
import argparse
from collections import Counter

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("✅ Database connection successful.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

def load_email_rules(conn):
    """Loads all email rules from the database into a single dict."""
    logger.info("Loading email matching rules from database...")
    email_rules = {}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT domain, match_type FROM email_match_rules")
            for row in cursor.fetchall():
                email_rules[row['domain']] = row['match_type']
            logger.info(f"Loaded {len(email_rules)} email rules.")
    except psycopg2.Error as e:
        logger.warning(f"Could not load email rules (table might not exist): {e}")
    return email_rules

def get_network_id(conn, specified_id):
    """Gets a network ID, either the one specified or the largest one."""
    if specified_id:
        logger.info(f"Analyzing specified network ID: {specified_id}")
        return specified_id
        
    logger.info("No network ID specified. Finding the largest network...")
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT id, primary_name, business_count, principal_count 
            FROM networks
            ORDER BY (business_count + principal_count) DESC
            LIMIT 1;
        """)
        result = cursor.fetchone()
        if not result:
            logger.error("❌ No networks found in the database. Run discover_networks.py first.")
            sys.exit(1)
            
        logger.info(f"Found largest network: '{result['primary_name']}' (ID: {result['id']})")
        logger.info(f"Contains: {result['business_count']:,} businesses, {result['principal_count']:,} principals")
        return result['id']

def analyze_principals(conn, network_id):
    """Finds the most-connected principals within a given network."""
    logger.info(f"\n--- Analysis 1: Top 100 Most-Connected Principals in Network {network_id} ---")
    logger.info("This query finds all principals in this network and counts their TOTAL business links (globally).")
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        query = """
            SELECT 
                p.name_c AS principal_name, 
                COUNT(p.business_id) AS global_link_count
            FROM principals p
            JOIN entity_networks en ON p.name_c = en.entity_id
            WHERE 
                en.network_id = %s 
                AND en.entity_type = 'principal'
            GROUP BY p.name_c
            ORDER BY global_link_count DESC
            LIMIT 100;
        """
        try:
            cursor.execute(query, (network_id,))
            results = cursor.fetchall()
            if not results:
                logger.warning("No principals found for this network.")
                return

            for row in results:
                logger.info(f"  - {row['principal_name']}: {row['global_link_count']:,} global business links")
        
        except psycopg2.Error as e:
            logger.error(f"Error querying principals: {e}")

def analyze_email_domains(conn, network_id):
    """Finds the most common CUSTOM email domains within a given network."""
    logger.info(f"\n--- Analysis 2: Top 1000 Custom Email Domains in Network {network_id} ---")
    logger.info("Loading email rules to filter public/registrar domains...")
    email_rules = load_email_rules(conn)
    
    COMMON_PUBLIC_DOMAINS = {
        'gmail.com', 'yahoo.com', 'hotmail.com', 'aol.com', 'outlook.com', 
        'msn.com', 'comcast.net', 'sbcglobal.net', 'att.net', 'live.com', 'icloud.com'
    }

    custom_domain_counter = Counter()
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        logger.info("Fetching all business emails for this network...")
        
        query = """
            SELECT b.business_email_address
            FROM businesses b
            JOIN entity_networks en ON b.id::text = en.entity_id
            WHERE 
                en.network_id = %s 
                AND en.entity_type = 'business'
                AND b.business_email_address IS NOT NULL
                AND b.business_email_address LIKE '%%@%%'; 
        """
        try:
            cursor.execute(query, (network_id,))
            rows = cursor.fetchall()
            logger.info(f"Found {len(rows)} emails to analyze...")

            for row in rows:
                email = row['business_email_address'].lower()
                try:
                    _, domain = email.split('@', 1)
                except ValueError:
                    continue 

                rule = email_rules.get(domain)
                
                if rule == 'registrar':
                    continue
                if rule == 'public' or domain in COMMON_PUBLIC_DOMAINS:
                    continue
                if domain.endswith('.edu') or domain.endswith('.gov'):
                    continue

                custom_domain_counter[domain] += 1
            
            logger.info("\n--- Top 100 Most Common CUSTOM Domains ---")
            if not custom_domain_counter:
                logger.info("No custom domains found in this network.")
                return

            for domain, count in custom_domain_counter.most_common(100):
                logger.info(f"  - {domain}: {count:,} occurrences")

        except psycopg2.Error as e:
            logger.error(f"Error querying business emails: {e}")

def main():
    parser = argparse.ArgumentParser(description="Evaluate a network to find supernodes.")
    parser.add_argument("network_id", type=int, nargs='?', default=None, 
                        help="Optional: The specific network ID to analyze. Defaults to the largest network.")
    args = parser.parse_args()

    if not DATABASE_URL:
        logger.error("❌ Error: DATABASE_URL environment variable is not set.")
        sys.exit(1)

    conn = None
    try:
        conn = get_db_connection()
        network_id = get_network_id(conn, args.network_id)
        
        analyze_principals(conn, network_id)
        analyze_email_domains(conn, network_id)
        
        logger.info("\n✅ Evaluation complete.")

    except Exception as e:
        logger.error(f"❌ A critical error occurred: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("🔌 Database connection closed.")

if __name__ == "__main__":
    main()