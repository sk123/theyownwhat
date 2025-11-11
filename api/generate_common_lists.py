import os
import psycopg2
import logging
import sys

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

def get_top_email_domains(conn):
    """
    Queries the database for the 100 most common email domains
    found in the businesses.business_email_address column.
    """
    logger.info("Querying for Top 100 most common email domains...")
    results = []
    
    query = """
        SELECT 
            LOWER(SUBSTRING(business_email_address FROM '@(.*)$')) AS email_domain,
            COUNT(*) AS domain_count
        FROM 
            businesses
        WHERE 
            business_email_address IS NOT NULL 
            AND business_email_address LIKE '%@%'  -- Must contain @ to be valid
        GROUP BY 
            email_domain
        HAVING 
            LOWER(SUBSTRING(business_email_address FROM '@(.*)$')) IS NOT NULL
            AND LOWER(SUBSTRING(business_email_address FROM '@(.*)$')) != ''
        ORDER BY 
            domain_count DESC
        LIMIT 100;
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        logger.info(f"✅ Found {len(results)} common domains.")
    except Exception as e:
        logger.error(f"❌ Error querying email domains: {e}")
        
    return results

def get_top_street_addresses(conn):
    """
    Queries the database for the 100 most common street addresses
    found in the businesses.business_address column.
    This helps identify registered agent addresses.
    """
    logger.info("Querying for Top 100 most common street addresses...")
    results = []
    
    query = """
        SELECT 
            UPPER(TRIM(business_address)) AS normalized_address,
            COUNT(*) AS address_count
        FROM 
            businesses
        WHERE 
            business_address IS NOT NULL 
            AND business_address != ''
        GROUP BY 
            normalized_address
        ORDER BY 
            address_count DESC
        LIMIT 100;
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        logger.info(f"✅ Found {len(results)} common addresses.")
    except Exception as e:
        logger.error(f"❌ Error querying street addresses: {e}")
        
    return results

def get_top_co_addresses(conn):
    """
    Queries for the 100 most common 'c/o' (care of) addresses and its variations
    to help identify third-party agents like law firms or accountants.
    """
    logger.info("Querying for Top 100 most common 'c/o' and 'care of' addresses...")
    results = []
    
    query = """
        SELECT 
            UPPER(TRIM(business_address)) AS normalized_co_address,
            COUNT(*) AS address_count
        FROM 
            businesses
        WHERE 
            business_address IS NOT NULL 
            AND (
                UPPER(TRIM(business_address)) LIKE 'C/O %' OR
                UPPER(TRIM(business_address)) LIKE 'CARE OF %' OR
                UPPER(TRIM(business_address)) LIKE 'C.O. %' OR
                UPPER(TRIM(business_address)) LIKE 'C O %'
            )
        GROUP BY 
            normalized_co_address
        ORDER BY 
            address_count DESC
        LIMIT 100;
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        logger.info(f"✅ Found {len(results)} common 'c/o' variant addresses.")
    except Exception as e:
        logger.error(f"❌ Error querying 'c/o' variant addresses: {e}")
        
    return results

def main():
    if not DATABASE_URL:
        logger.error("❌ Error: DATABASE_URL environment variable is not set.")
        sys.exit(1)

    conn = None
    try:
        conn = get_db_connection()
        
        # --- Get and Print Top Domains ---
        domains = get_top_email_domains(conn)
        print("\n" + "="*80)
        print(f"📊 TOP {len(domains)} EMAIL DOMAINS")
        print("="*80)
        if domains:
            print(f"{'Count':<10} | {'Domain':<60}")
            print("-"*80)
            for domain, count in domains:
                print(f"{count:<10} | {domain}")
        else:
            print("No email domains found or query failed.")
        
        # --- Get and Print Top Addresses ---
        addresses = get_top_street_addresses(conn)
        print("\n" + "="*80)
        print(f"📊 TOP {len(addresses)} BUSINESS ADDRESSES (Potential Registrars)")
        print("="*80)
        if addresses:
            print(f"{'Count':<10} | {'Address':<100}")
            print("-"*80)
            for address, count in addresses:
                print(f"{count:<10} | {address}")
        else:
            print("No addresses found or query failed.")
            
        # --- Get and Print Top C/O Addresses ---
        co_addresses = get_top_co_addresses(conn)
        print("\n" + "="*80)
        print(f"📊 TOP {len(co_addresses)} 'C/O' VARIANT ADDRESSES (Potential Law Firms/Accountants)")
        print("="*80)
        if co_addresses:
            print(f"{'Count':<10} | {'Address':<100}")
            print("-"*80)
            for address, count in co_addresses:
                print(f"{count:<10} | {address}")
        else:
            print("No 'c/o' addresses found or query failed.")
            
        print("\n" + "="*80)
        logger.info("Review these lists to create your exclusion/provider lists.")

    except Exception as e:
        logger.error(f"❌ A critical error occurred in main process: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("🔌 Database connection closed.")

if __name__ == "__main__":
    main()