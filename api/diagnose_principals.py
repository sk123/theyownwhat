import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
DATABASE_URL = os.environ.get("DATABASE_URL")

def run_diagnostics():
    """Connects to the database and runs a series of checks on the principals table."""
    if not DATABASE_URL:
        logging.error("DATABASE_URL environment variable not set. Cannot connect to the database.")
        return

    conn = None
    try:
        logging.info("Connecting to the database...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        logging.info("Connection successful. Running diagnostics...\n")

        # --- Check 1: Total Count ---
        cursor.execute("SELECT COUNT(*) AS total FROM principals;")
        total_rows = cursor.fetchone()['total']
        print(f"--- Principals Table Health Report ---")
        print(f"1. Total Rows: {total_rows:,}")
        if total_rows == 0:
            logging.warning("Warning: The principals table is empty.")
            return

        # --- Check 2: Null or Empty Names ---
        cursor.execute("SELECT COUNT(*) AS missing FROM principals WHERE name_c IS NULL OR name_c = '';")
        missing_names = cursor.fetchone()['missing']
        percentage_missing = (missing_names / total_rows) * 100 if total_rows > 0 else 0
        print(f"\n2. Rows with Missing Principal Name ('name_c'):")
        print(f"   - Count: {missing_names:,} ({percentage_missing:.2f}%)")
        if percentage_missing > 5:
            logging.warning("Warning: A significant percentage of principals are missing a name.")

        # --- Check 3: Unlinked Principals ---
        cursor.execute("SELECT COUNT(*) AS unlinked FROM principals WHERE business_id IS NULL OR business_id = '';")
        unlinked_principals = cursor.fetchone()['unlinked']
        percentage_unlinked = (unlinked_principals / total_rows) * 100 if total_rows > 0 else 0
        print(f"\n3. Rows not linked to a Business ('business_id'):")
        print(f"   - Count: {unlinked_principals:,} ({percentage_unlinked:.2f}%)")
        if percentage_unlinked > 5:
            logging.warning("Warning: A significant percentage of principals are not linked to any business.")

        # --- Check 4: Top 10 Most Frequent Principals ---
        print("\n4. Top 10 Most Frequent Principal Names (potential registered agents):")
        cursor.execute("""
            SELECT name_c, COUNT(*) AS count 
            FROM principals 
            WHERE name_c IS NOT NULL AND name_c != '' 
            GROUP BY name_c 
            ORDER BY count DESC 
            LIMIT 10;
        """)
        top_principals = cursor.fetchall()
        if top_principals:
            for row in top_principals:
                print(f"   - {row['name_c']}: {row['count']:,} times")
        else:
            print("   - No principals found.")
            
        # --- Check 5: Sample Records ---
        print("\n5. Sample of 5 records from the table:")
        cursor.execute("SELECT id, name_c, address, business_id FROM principals LIMIT 5;")
        sample_data = cursor.fetchall()
        if sample_data:
            for i, row in enumerate(sample_data):
                print(f"   - Record {i+1}:")
                print(f"     ID: {row['id']}")
                print(f"     Name: {row['name_c']}")
                print(f"     Address: {row['address']}")
                print(f"     Business ID: {row['business_id']}")
        else:
            print("   - No records to sample.")

        print("\n--- End of Report ---")

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("\nDatabase connection closed.")

if __name__ == "__main__":
    run_diagnostics()
