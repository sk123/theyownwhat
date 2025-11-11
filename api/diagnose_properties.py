import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import argparse

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")

# --- Database Connection ---
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

def search_owner_properties(conn, owner_search):
    """Searches for properties where owner name contains the search parameter."""
    print(f"\n--- Properties for Owner Search: '{owner_search}' ---")
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            # Search for owners containing the search term (case-insensitive)
            cursor.execute("""
                SELECT owner, location
                FROM properties 
                WHERE owner ILIKE %s
                ORDER BY owner, location;
            """, (f'%{owner_search}%',))
            
            matching_properties = cursor.fetchall()
            
            if matching_properties:
                print(f"Found {len(matching_properties)} properties matching owner search '{owner_search}':")
                print()
                
                for prop in matching_properties:
                    owner_name = prop['owner'] or 'N/A'
                    address = prop['location'] or 'N/A'
                    print(f"   Owner: {owner_name}")
                    print(f"   Address: {address}")
                    print()
                    
            else:
                print(f"No properties found for owner search term: '{owner_search}'")
                
        except psycopg2.Error as e:
            print(f"\n❌ A database error occurred during owner search: {e}", file=sys.stderr)

# ADDED THIS FUNCTION
def search_address_properties(conn, address_search):
    """Searches for properties where location contains the search parameter."""
    print(f"\n--- Properties for Address Search: '{address_search}' ---")
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            # Search for locations containing the search term (case-insensitive)
            cursor.execute("""
                SELECT owner, location
                FROM properties 
                WHERE location ILIKE %s
                ORDER BY location, owner;
            """, (f'%{address_search}%',))
            
            matching_properties = cursor.fetchall()
            
            if matching_properties:
                print(f"Found {len(matching_properties)} properties matching address search '{address_search}':")
                print()
                
                for prop in matching_properties:
                    owner_name = prop['owner'] or 'N/A'
                    address = prop['location'] or 'N/A'
                    print(f"   Owner: {owner_name}")
                    print(f"   Address: {address}")
                    print()
                    
            else:
                print(f"No properties found for address search term: '{address_search}'")
                
        except psycopg2.Error as e:
            print(f"\n❌ A database error occurred during address search: {e}", file=sys.stderr)


def search_city_properties(conn, city_search):
    """Searches for 5 sample properties within a specific city."""
    print(f"\n--- 5 Sample Properties for City: '{city_search}' ---")
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            # Search for properties in the city (case-insensitive), order by value to get an interesting sample
            cursor.execute("""
                SELECT owner, location, account_number, serial_number, assessed_value
                FROM properties 
                WHERE property_city ILIKE %s
                ORDER BY assessed_value DESC NULLS LAST
                LIMIT 5;
            """, (city_search,))
            
            sample_properties = cursor.fetchall()
            
            if sample_properties:
                print(f"Found {len(sample_properties)} sample properties for city '{city_search}':")
                for i, prop in enumerate(sample_properties, 1):
                    print(f"\n   - Sample {i}:")
                    print(f"     Owner: {prop.get('owner', 'N/A')}")
                    print(f"     Location: {prop.get('location', 'N/A')}")
                    print(f"     Account #: {prop.get('account_number', 'N/A')}")
                    print(f"     Serial #: {prop.get('serial_number', 'N/A')}")
                    print(f"     Assessed Value: {prop.get('assessed_value', 'N/A')}")
            else:
                print(f"No properties found for city: '{city_search}'")
                
        except psycopg2.Error as e:
            print(f"\n❌ A database error occurred during city search: {e}", file=sys.stderr)

def run_diagnostics(conn, owner_search=None, address_search=None, city_search=None):
    """Runs a series of checks on the properties table and prints a report."""
    print("\n--- Properties Table Health Report ---")
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            # 1. Check total row count
            cursor.execute("SELECT COUNT(*) AS total FROM properties;")
            total_rows = cursor.fetchone()['total']
            print(f"1. Total Rows: {total_rows:,}")
            if total_rows == 0:
                print("WARNING: The properties table is empty. Please run the import script.")
                return

            # 2. Check for missing owner names
            cursor.execute("SELECT COUNT(*) AS missing FROM properties WHERE owner IS NULL OR owner = '';")
            missing_owners = cursor.fetchone()['missing']
            percentage_missing = (missing_owners / total_rows) * 100 if total_rows > 0 else 0
            print(f"\n2. Rows with Missing Owner Name ('owner'):")
            print(f"   - Count: {missing_owners:,} ({percentage_missing:.2f}%)")
            if percentage_missing > 5:
                print("   WARNING: A significant number of properties are missing an owner.")

            # 3. Check for missing location/address
            cursor.execute("SELECT COUNT(*) AS missing FROM properties WHERE location IS NULL OR location = '';")
            missing_locations = cursor.fetchone()['missing']
            percentage_loc_missing = (missing_locations / total_rows) * 100 if total_rows > 0 else 0
            print(f"\n3. Rows with Missing Location ('location'):")
            print(f"   - Count: {missing_locations:,} ({percentage_loc_missing:.2f}%)")
            if percentage_loc_missing > 5:
                print("   WARNING: A significant number of properties are missing a location.")

            # 4. Check for properties without an assessed value
            cursor.execute("SELECT COUNT(*) AS no_value FROM properties WHERE assessed_value IS NULL OR assessed_value <= 0;")
            no_value_count = cursor.fetchone()['no_value']
            percentage_no_value = (no_value_count / total_rows) * 100 if total_rows > 0 else 0
            print(f"\n4. Rows with Missing or Zero Assessed Value ('assessed_value'):")
            print(f"   - Count: {no_value_count:,} ({percentage_no_value:.2f}%)")

            # 5. Top 10 most frequent owners (potential data quality issues or large landlords)
            print("\n5. Top 10 Most Frequent Owners:")
            cursor.execute("""
                SELECT owner, COUNT(*) as count
                FROM properties
                WHERE owner IS NOT NULL AND owner != ''
                GROUP BY owner
                ORDER BY count DESC
                LIMIT 10;
            """)
            top_owners = cursor.fetchall()
            if top_owners:
                for row in top_owners:
                    print(f"   - \"{row['owner']}\" owns {row['count']:,} properties")
            else:
                print("   - No owners found.")

            # 6. Top 10 Cities by Property Count:
            print("\n6. Top 10 Cities by Property Count:")
            cursor.execute("""
                SELECT property_city, COUNT(*) as count
                FROM properties
                WHERE property_city IS NOT NULL AND property_city != ''
                GROUP BY property_city
                ORDER BY count DESC
                LIMIT 10;
            """)
            top_cities = cursor.fetchall()
            if top_cities:
                for row in top_cities:
                    print(f"   - \"{row['property_city']}\" has {row['count']:,} properties")
            else:
                print("   - No city data found.")

            # 7. Top 10 Cities by CAMA Link Count (with Sample URL):
            print("\n7. Top 10 Cities by CAMA Link Count (with Sample URL):")
            cursor.execute("""
                SELECT
                    property_city,
                    COUNT(*) AS link_count,
                    MAX(cama_site_link) AS sample_url 
                FROM
                    properties
                WHERE
                    cama_site_link LIKE 'http%'
                GROUP BY
                    property_city
                ORDER BY
                    link_count DESC
                LIMIT 10;
            """)
            top_cama_cities = cursor.fetchall()
            if top_cama_cities:
                for row in top_cama_cities:
                    print(f"   - \"{row['property_city']}\": {row['link_count']:,} properties with links")
                    print(f"     Sample URL: {row['sample_url']}")
            else:
                print("   - No properties with CAMA links found.")


            # 8. Sample of 5 records (MODIFIED)
            print("\n8. Sample of 5 records from the table:")
            cursor.execute("""
                SELECT owner, location, property_city, assessed_value, sale_date, 
                       cama_site_link, account_number, serial_number 
                FROM properties LIMIT 5;
            """)
            sample_records = cursor.fetchall()
            if sample_records:
                for i, record in enumerate(sample_records, 1):
                    print(f"   - Record {i}:")
                    print(f"     Owner: {record.get('owner', 'N/A')}")
                    print(f"     Location: {record.get('location', 'N/A')}")
                    print(f"     City: {record.get('property_city', 'N/A')}")
                    print(f"     Account #: {record.get('account_number', 'N/A')}")
                    print(f"     Serial #: {record.get('serial_number', 'N/A')}")
                    print(f"     Assessed Value: {record.get('assessed_value', 'N/A')}")
                    print(f"     Sale Date: {record.get('sale_date', 'N/A')}")
                    print(f"     CAMA Link: {record.get('cama_site_link', 'N/A')}")
            else:
                print("   - No records to sample.")

        except psycopg2.Error as e:
            print(f"\n❌ A database error occurred: {e}", file=sys.stderr)

    print("\n--- End of Report ---")

    # If owner search parameter is provided, run the owner search
    if owner_search:
        search_owner_properties(conn, owner_search)
    
    # If address search parameter is provided, run the address search
    if address_search:
        search_address_properties(conn, address_search)

    # --- NEW BLOCK ---
    # If city search parameter is provided, run the city search
    if city_search:
        search_city_properties(conn, city_search)
    # --- END NEW BLOCK ---

def main():
    """Main execution function."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Diagnose properties table and optionally search for properties by owner and/or address')
    parser.add_argument('--owner', '-o', type=str, help='Search for properties where owner name contains this value')
    parser.add_argument('--address', '-a', type=str, help='Search for properties where address contains this value')
    parser.add_argument('--city', '-c', type=str, help='Show 5 sample properties for a specific city') # <-- ADDED
    args = parser.parse_args()

    if not DATABASE_URL:
        print("❌ Error: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)
        
    conn = None
    try:
        conn = get_db_connection()
        # Pass the new arg to the function call
        run_diagnostics(conn, args.owner, args.address, args.city) # <-- UPDATED
    finally:
        if conn:
            conn.close()
            print("\nINFO: Database connection closed.")

if __name__ == "__main__":
    main()