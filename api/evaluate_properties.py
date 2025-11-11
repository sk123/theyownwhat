import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor  # Keep RealDictCursor import for consistency, though we use default tuple cursor

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")

# --- Database Connection (Copied from diagnose_properties.py) ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("✅ Database connection successful.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Could not connect to the database. Ensure DATABASE_URL is set correctly.", file=sys.stderr)
        print(f"Error details: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


def check_property_identifiers(conn):
    """
    Check if the properties table has unique identifiers that match
    the URL patterns for propertyrecordcards.com and Hartford assessor.
    
    NOTE: This script assumes the database is PostgreSQL.
    """
    
    # Use a standard tuple cursor to match original script's logic (e.g., table[0], col[0])
    with conn.cursor() as cursor:
    
        # First, let's see what tables exist in the public schema (PostgreSQL syntax)
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
        tables_fetched = cursor.fetchall()
        # Flatten the list of tuples
        tables = [table[0] for table in tables_fetched]
        
        print("Tables in public schema:")
        for table in tables:
            print(f"  - {table}")
        
        # Check if properties table exists
        if 'properties' not in tables:
            print("\n❌ No 'properties' table found in database")
            return
        
        # Get column information for properties table (PostgreSQL syntax)
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'properties' AND table_schema = 'public';
        """)
        columns_fetched = cursor.fetchall()
        # Flatten the list of tuples to get column names
        column_names = [col[0] for col in columns_fetched]
        
        print("\n\nColumns in 'properties' table:")
        for col in column_names:
            print(f"  - {col}")
        
        # Check for potential unique ID fields
        print("\n\n=== Checking for Property Record Cards Unique ID ===")
        potential_unique_id_fields = [
            'unique_id', 'uniqueid', 'unique_id_number', 'property_id', 
            'parcel_id', 'account_number', 'assessment_id', 'tax_id', 'id', 'serial_number'
        ]
        
        found_unique_id = None
        for field in potential_unique_id_fields:
            if field in column_names:
                print(f"✓ Found potential unique ID field: '{field}'")
                found_unique_id = field
                break
        
        if not found_unique_id:
            print("❌ No obvious unique ID field found for propertyrecordcards.com")
            print("   (Looking for fields like: unique_id, uniqueid, property_id, etc.)")
        
        # Check for Hartford Account Number
        print("\n=== Checking for Hartford Account Number ===")
        potential_account_fields = [
            'account_number', 'account_num', 'account', 'tax_account',
            'assessor_account', 'property_account','serial_number'
        ]
        
        found_account = None
        for field in potential_account_fields:
            if field in column_names:
                print(f"✓ Found potential account number field: '{field}'")
                found_account = field
                break
        
        if not found_account:
            print("❌ No obvious account number field found for Hartford")
            print("   (Looking for fields like: account_number, account, etc.)")
        
        # Check for town/municipality field
        print("\n=== Checking for Town/Municipality Field ===")
        potential_town_fields = [
            'town', 'municipality', 'city', 'town_code', 'towncode',
            'muni_code', 'location', 'district', 'property_city' # Added property_city
        ]
        
        found_town = None
        for field in potential_town_fields:
            if field in column_names:
                print(f"✓ Found potential town field: '{field}'")
                found_town = field
                break
        
        if not found_town:
            print("❌ No obvious town/municipality field found")
            print("   (Looking for fields like: town, municipality, property_city, etc.)")
        
        # Sample some data to check format
        print("\n\n=== Sample Data Analysis ===")
        
        if found_unique_id:
            # Note: Using f-string for column name is safe here as it's derived from our own logic, not user input.
            cursor.execute(f"SELECT DISTINCT {found_unique_id} FROM properties LIMIT 5")
            samples = cursor.fetchall()
            print(f"\nSample values from '{found_unique_id}':")
            for sample in samples:
                if sample[0]:
                    print(f"  - {sample[0]}")
                    # Check if format matches expected pattern (8 digits for propertyrecordcards)
                    if str(sample[0]).isdigit() and len(str(sample[0])) == 8:
                        print(f"    ✓ Matches expected 8-digit format for propertyrecordcards.com")
                    else:
                        print(f"    ⚠ Format may need adjustment (expected 8 digits)")
        
        if found_account:
            cursor.execute(f"SELECT DISTINCT {found_account} FROM properties LIMIT 5")
            samples = cursor.fetchall()
            print(f"\nSample values from '{found_account}':")
            for sample in samples:
                if sample[0]:
                    print(f"  - {sample[0]}")
                    # Check if format matches Hartford pattern (5-6 digits typically)
                    if str(sample[0]).isdigit() and 4 <= len(str(sample[0])) <= 6:
                        print(f"    ✓ Could match Hartford account number format")
        
        if found_town:
            cursor.execute(f"SELECT DISTINCT {found_town} FROM properties LIMIT 10")
            samples = cursor.fetchall()
            print(f"\nSample values from '{found_town}':")
            for sample in samples:
                if sample[0]:
                    print(f"  - {sample[0]}")
        
        # Check if we have specific towns
        print("\n\n=== Checking for Specific Towns ===")
        if found_town:
            for town_name in ['Hartford', 'Ashford', 'Ansonia']:
                # Use %s placeholder for PostgreSQL
                cursor.execute(f"SELECT COUNT(*) FROM properties WHERE LOWER({found_town}) LIKE %s", 
                             (f'%{town_name.lower()}%',))
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"✓ Found {count} properties in {town_name}")
                else:
                    print(f"❌ No properties found for {town_name}")
        
        # Summary and recommendations
        print("\n\n=== SUMMARY & RECOMMENDATIONS ===")
        
        if found_unique_id and found_town:
            print("✅ Database appears to have necessary fields for propertyrecordcards.com URLs")
            print(f"   - Use '{found_unique_id}' for uniqueid parameter")
            print(f"   - Use '{found_town}' to determine towncode")
            print("   - You'll need a mapping of town names to town codes")
        else:
            print("⚠️  Database may be missing required fields for propertyrecordcards.com")
            if not found_unique_id:
                print("   - Need a unique ID field that matches property record system")
            if not found_town:
                print("   - Need a town/municipality field to determine town codes")
        
        if found_account:
            print("\n✅ Database may support Hartford assessor URLs")
            print(f"   - Use '{found_account}' for AccountNumber parameter")
        else:
            print("\n⚠️  Database may not have Hartford account numbers")
    
    # Connection is closed by the 'main' function's finally block


if __name__ == "__main__":
    # Main execution logic modeled after diagnose_properties.py
    if not DATABASE_URL:
        print("❌ Error: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)
        
    conn = None
    try:
        conn = get_db_connection()
        # Run the check using the established connection
        check_property_identifiers(conn)

    except Exception as e:
        print(f"❌ An unexpected error occurred during execution: {e}", file=sys.stderr)
    
    finally:
        if conn:
            conn.close()
            print("\nINFO: Database connection closed.")

    
    print("\n\n" + "="*60)
    print("NEXT STEPS:")
    print("="*60)
    print("""
1. If unique IDs are present but in wrong format:
   - May need to pad with zeros (e.g., '159500' -> '00159500')
   - May need to strip prefixes or suffixes
   
2. Create town code mapping dictionary:
   town_codes = {
       'Ashford': '003',
       'Ansonia': '002',
       # ... add more as needed
   }
   
3. For Hartford properties:
   - If account numbers don't match, may need separate lookup table
   - Consider scraping from search results if direct match not possible
   
4. Test with known examples:
   - Ashford: uniqueid=00159500 should exist
   - Ansonia: uniqueid=00009850 should exist
   - Hartford: AccountNumber=16303 should exist
""")