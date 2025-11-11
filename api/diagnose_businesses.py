import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import argparse
import re

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
# Assumes the script is run from the root directory where the 'data' folder is
BUSINESSES_CSV_PATH = '/app/data/businesses.csv'

# Regex to find and split unit information from a street address
# It looks for the start (Group 1: Street) and an optional unit part (Group 2: Unit Num)
UNIT_REGEX = re.compile(r'^(.*?)(?:\s+(?:SUITE|STE|UNIT|APT|APARTMENT|#)\s*(.*))?$', re.IGNORECASE)

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

def analyze_csv_header(file_path):
    """Reads the header of the businesses.csv file and reports its structure."""
    print(f"\n--- Analyzing CSV Structure: {file_path} ---")
    try:
        df = pd.read_csv(file_path, nrows=0) # Read only the header
        print("✅ CSV file found. Column headers are:")
        for col in df.columns:
            print(f"   - '{col}'")
        return df.columns
    except FileNotFoundError:
        print(f"❌ ERROR: The file '{file_path}' was not found.")
        return None
    except Exception as e:
        print(f"❌ ERROR: Could not read the CSV file. Details: {e}")
        return None

def run_diagnostics(conn):
    """Runs a series of checks on the businesses table and prints a report."""
    print("\n--- Database Table Health Report: 'businesses' ---")
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            # 1. Check total row count
            cursor.execute("SELECT COUNT(*) AS total FROM businesses;")
            total_rows = cursor.fetchone()['total']
            print(f"1. Total Rows: {total_rows:,}")
            if total_rows == 0:
                print("   WARNING: The businesses table is empty. Please run the import script.")
                return

            # 2. Key Field Population Analysis
            print("\n2. Key Field Population:")
            key_fields = ['name', 'id', 'business_address', 'business_city', 'business_state', 'principal_name']
            for field in key_fields:
                cursor.execute(f"SELECT COUNT(*) AS missing FROM businesses WHERE {field} IS NULL OR {field} = '';")
                missing_count = cursor.fetchone()['missing']
                percentage_missing = (missing_count / total_rows) * 100
                status = "✅" if percentage_missing < 5 else "⚠️" if percentage_missing < 50 else "❌"
                print(f"   {status} Missing '{field}': {missing_count:,} ({percentage_missing:.2f}%)")

            # 3. Business Status Distribution
            print("\n3. Business Status Distribution:")
            cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM businesses
                GROUP BY status
                ORDER BY count DESC;
            """)
            statuses = cursor.fetchall()
            if statuses:
                for row in statuses:
                    print(f"   - {row['status'] or 'N/A'}: {row['count']:,} records")
            else:
                print("   - No status information found.")

            # 4. Sample of 5 records (with more relevant fields)
            print("\n4. Sample of 5 records from the table:")
            cursor.execute("SELECT id, name, status, principal_name, business_address, business_city, business_state FROM businesses WHERE name IS NOT NULL LIMIT 5;")
            sample_records = cursor.fetchall()
            if sample_records:
                for i, record in enumerate(sample_records, 1):
                    print(f"   - Record {i}:")
                    for key, value in record.items():
                        print(f"     {key.replace('_', ' ').title()}: {value or 'N/A'}")
            else:
                print("   - No records to sample.")

        except psycopg2.ProgrammingError as e:
            if "does not exist" in str(e):
                print("\n❌ CRITICAL: The 'businesses' table does not exist. Please run the import script first.")
            else:
                print(f"\n❌ A database error occurred: {e}", file=sys.stderr)
        except psycopg2.Error as e:
            print(f"\n❌ A database error occurred: {e}", file=sys.stderr)

    print("\n--- End of Report ---")

def search_business_by_name(conn, name):
    """Searches for businesses by name and prints their parsed address components."""
    print(f"\n--- Searching for businesses matching: '{name}' ---")
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            # --- MODIFIED: Added business_email_address to the query ---
            query = "SELECT name, business_address, business_email_address FROM businesses WHERE UPPER(name) LIKE %s ORDER BY name;"
            params = (f'%{name.upper()}%',)
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                print(f"No businesses found matching '{name}'.")
                return

            print(f"Found {len(results)} matching businesses:")
            
            for record in results:
                b_name = record.get('name')
                b_addr_full = record.get('business_address')
                # --- MODIFIED: Get the email from the record ---
                b_email = record.get('business_email_address') or 'N/A'
                
                street = b_addr_full or 'N/A'
                unit = 'N/A'
                
                if b_addr_full:
                    match = UNIT_REGEX.match(b_addr_full.strip())
                    if match and match.group(2) is not None: # Group 2 is the unit part
                        street = match.group(1).strip().rstrip(',') # Clean up street
                        unit = match.group(2).strip()
                    elif match: # Match, but no group 2 (e.g., just the street)
                        street = match.group(1).strip().rstrip(',')
                    else: # No regex match at all (unlikely)
                        street = b_addr_full
                        
                print("-" * 20)
                print(f"  (1) Business Name:  {b_name}")
                print(f"  (2) Street Address: {street}")
                print(f"  (3) Business Unit:  {unit}")
                # --- MODIFIED: Added new print line ---
                print(f"  (4) Business Email: {b_email}")
                
        except psycopg2.Error as e:
            print(f"\n❌ A database error occurred during search: {e}", file=sys.stderr)

def main():
    """Main execution function: either runs diagnostics or a specific name search."""
    parser = argparse.ArgumentParser(description="Run diagnostics or search for businesses in the 'businesses' table.")
    parser.add_argument('--name', type=str, help='Business name to search for (case-insensitive LIKE search).')
    
    args = parser.parse_args()

    if not DATABASE_URL:
        print("❌ Error: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    conn = None
    try:
        conn = get_db_connection()
        
        if args.name:
            # Run the new search function
            search_business_by_name(conn, args.name)
        else:
            # Run the original full diagnostics
            analyze_csv_header(BUSINESSES_CSV_PATH)
            run_diagnostics(conn)
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed.")

if __name__ == "__main__":
    main()