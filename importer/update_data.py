import os
import time
import pandas as pd
import psycopg2
from psycopg2 import sql
import sys
import argparse
from io import StringIO

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
# Define sources, mirroring import_data.py
DATA_SOURCES = {
    "businesses": "businesses.csv",
    "principals": "principals.csv",
    "properties": "new_parcels.csv",
}


def get_db_connection():
    """Establishes a connection to the PostgreSQL database with retries."""
    retries = 10
    while retries > 0:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            print("✅ Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"⏳ Database not ready, retrying... ({retries} attempts left). Error: {e}")
            retries -= 1
            time.sleep(5)
    raise Exception("❌ Could not connect to the database after multiple retries.")

# --- Helper Function (from import_data.py) ---
def construct_principal_name(row):
    """Helper to construct principal name, mirroring import script logic."""
    # Check for 'name_c' (mapped from 'Principal Name') first.
    if 'name_c' in row and pd.notna(row['name_c']) and str(row['name_c']).strip():
        return str(row['name_c']).strip()
    # If blank, construct from parts.
    parts = [row.get('firstname', ''), row.get('middlename', ''), row.get('lastname', ''), row.get('suffix', '')]
    return ' '.join(str(p) for p in parts if pd.notna(p) and str(p).strip()).strip()


# =============================================================================
# SCHEMA UPDATE FUNCTIONS
# =============================================================================

def add_property_columns(cursor):
    """Adds new columns to the PROPERTIES table if they don't already exist."""
    print("🚀 Ensuring database schema includes new 'properties' columns...")
    
    new_columns = [
        ("link", "TEXT"), ("account_number", "TEXT"), ("gis_tag", "TEXT"),
        ("map", "TEXT"), ("map_cut", "TEXT"), ("block", "TEXT"),
        ("block_cut", "TEXT"), ("lot", "TEXT"), ("lot_cut", "TEXT"),
        ("unit", "TEXT"), ("unit_cut", "TEXT"), ("property_zip", "TEXT"),
        ("property_county", "TEXT"), ("street_name", "TEXT"), ("address_number", "TEXT"),
        ("address_prefix", "TEXT"), ("address_suffix", "TEXT"), ("cama_site_link", "TEXT"),
        ("building_photo", "TEXT"), ("number_of_units", "NUMERIC")
    ]
    commands = [
        sql.SQL("ALTER TABLE properties ADD COLUMN IF NOT EXISTS {} {};")
            .format(sql.Identifier(col_name), sql.SQL(col_type))
        for col_name, col_type in new_columns
    ]
    
    try:
        for command in commands:
            cursor.execute(command)
        print("✅ Schema check complete for 'properties'.")
    except psycopg2.Error as e:
        print(f"❌ Error altering 'properties' schema: {e}")
        raise

def add_business_columns(cursor):
    """Adds new columns to the BUSINESSES table if they don't already exist."""
    print("🚀 Ensuring database schema includes new 'businesses' columns...")
    
    # Note: business_address, city, state, and zip already exist from import_data.py
    new_columns = [
        ("business_unit", "TEXT"),
        ("business_country", "TEXT"),
        ("business_email_address", "TEXT")
    ]
    commands = [
        sql.SQL("ALTER TABLE businesses ADD COLUMN IF NOT EXISTS {} {};")
            .format(sql.Identifier(col_name), sql.SQL(col_type))
        for col_name, col_type in new_columns
    ]
    
    try:
        for command in commands:
            cursor.execute(command)
        print("✅ Schema check complete for 'businesses'.")
    except psycopg2.Error as e:
        print(f"❌ Error altering 'businesses' schema: {e}")
        raise

def add_principal_columns(cursor):
    """Adds new columns to the PRINCIPALS table if they don't already exist."""
    print("🚀 Ensuring database schema includes new 'principals' columns...")
    
    # Note: 'address' (from Business Street Address 1), city, state, and zip exist.
    new_columns = [
        ("business_street_address_2", "TEXT"),
        ("business_street_address_3", "TEXT"),
        ("name_c_norm", "TEXT")
    ]
    commands = [
        sql.SQL("ALTER TABLE principals ADD COLUMN IF NOT EXISTS {} {};")
            .format(sql.Identifier(col_name), sql.SQL(col_type))
        for col_name, col_type in new_columns
    ]
    
    try:
        for command in commands:
            cursor.execute(command)
        print("✅ Schema check complete for 'principals'.")
    except psycopg2.Error as e:
        print(f"❌ Error altering 'principals' schema: {e}")
        raise

# =============================================================================
# DATA UPDATE FUNCTIONS
# =============================================================================

def run_properties_update(conn, file_path, column_map):
    """
    Updates the PROPERTIES table from its CSV.
    Conditionally updates owner fields to protect existing data.
    """
    print(f"🚚 Starting data update for 'properties' from '{file_path}'...")

    staging_table_name = "staging_properties_update"
    # Staging table must include join key, conditional fields, and new fields
    staging_cols_with_types = [
        ("serial_number", "TEXT"), ("owner", "TEXT"), ("co_owner", "TEXT"),
        ("owner_norm", "TEXT"), ("co_owner_norm", "TEXT"),
        ("link", "TEXT"), ("account_number", "TEXT"), ("gis_tag", "TEXT"),
        ("map", "TEXT"), ("map_cut", "TEXT"), ("block", "TEXT"),
        ("block_cut", "TEXT"), ("lot", "TEXT"), ("lot_cut", "TEXT"),
        ("unit", "TEXT"), ("unit_cut", "TEXT"), ("property_zip", "TEXT"),
        ("property_county", "TEXT"), ("street_name", "TEXT"), ("address_number", "TEXT"),
        ("address_prefix", "TEXT"), ("address_suffix", "TEXT"), ("cama_site_link", "TEXT"),
        ("building_photo", "TEXT"), ("number_of_units", "NUMERIC")
    ]
    staging_col_names = [col[0] for col in staging_cols_with_types]

    # SQL to create the temp table
    create_temp_table_sql = sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT PRESERVE ROWS;").format(
        sql.Identifier(staging_table_name),
        sql.SQL(", ").join(
            sql.Identifier(name) + sql.SQL(" ") + sql.SQL(dtype) for name, dtype in staging_cols_with_types
        )
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_temp_table_sql)
            print(f"✅ Created temp table '{staging_table_name}' for processing.")

        total_size = os.path.getsize(file_path)
        print(f"Processing file ({total_size / (1024*1024):.2f} MB)...")

        df_chunk_iter = pd.read_csv(file_path, chunksize=25000, low_memory=False, encoding='utf-8', on_bad_lines='warn')
        
        total_rows = 0
        for chunk in df_chunk_iter:
            chunk.columns = [c.strip().lower().replace(' ', '_').replace('-', '_') for c in chunk.columns]
            chunk.rename(columns=column_map, inplace=True)

            if 'number_of_units' in chunk.columns:
                chunk['number_of_units'] = pd.to_numeric(chunk['number_of_units'], errors='coerce')

            # Create normalized owner fields
            if 'owner' in chunk.columns:
                chunk['owner_norm'] = chunk['owner'].astype(str).str.strip().str.upper().replace(r'[^a-zA-Z0-9 ]', '', regex=True).replace(r'\s+', ' ', regex=True)
            if 'co_owner' in chunk.columns:
                chunk['co_owner_norm'] = chunk['co_owner'].astype(str).str.strip().str.upper().replace(r'[^a-zA-Z0-9 ]', '', regex=True).replace(r'\s+', ' ', regex=True)

            final_chunk = chunk.reindex(columns=staging_col_names)

            buffer = StringIO()
            final_chunk.to_csv(buffer, index=False, header=False, sep='\t', quotechar='"', na_rep='\\N')
            buffer.seek(0)
            
            with conn.cursor() as c:
                c.execute(sql.SQL("TRUNCATE TABLE {};").format(sql.Identifier(staging_table_name)))
                c.copy_expert(
                    sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL '\\N')")
                    .format(sql.Identifier(staging_table_name), sql.SQL(', ').join(map(sql.Identifier, staging_col_names))),
                    buffer
                )

                # 1. UPDATE ALL NEW FIELDS
                c.execute("""
                UPDATE properties p SET
                    link = s.link, account_number = s.account_number, gis_tag = s.gis_tag,
                    map = s.map, map_cut = s.map_cut, block = s.block, block_cut = s.block_cut,
                    lot = s.lot, lot_cut = s.lot_cut, unit = s.unit, unit_cut = s.unit_cut,
                    property_zip = s.property_zip, property_county = s.property_county,
                    street_name = s.street_name, address_number = s.address_number,
                    address_prefix = s.address_prefix, address_suffix = s.address_suffix,
                    cama_site_link = s.cama_site_link, building_photo = s.building_photo,
                    number_of_units = s.number_of_units
                FROM staging_properties_update s
                WHERE p.serial_number = s.serial_number;
                """)
                
                # 2. CONDITIONALLY UPDATE OWNER FIELDS
                c.execute("""
                UPDATE properties p SET
                    owner = s.owner,
                    co_owner = s.co_owner,
                    owner_norm = s.owner_norm,
                    co_owner_norm = s.co_owner_norm
                FROM staging_properties_update s
                WHERE p.serial_number = s.serial_number
                  AND s.owner IS NOT NULL
                  AND s.owner != 'Current Owner';
                """)
            
            conn.commit()
            total_rows += len(chunk)
            sys.stdout.write(f'\rUpdating... processed {total_rows:,} source rows.')
            sys.stdout.flush()

        print(f"\n✅ Successfully processed {total_rows:,} rows from CSV to update 'properties'.")

    except Exception as e:
        print(f"\n❌ An error occurred during the 'properties' update process: {e}")
        conn.rollback()
        raise e


def run_businesses_update(conn, file_path, column_map):
    """Updates the BUSINESSES table from its CSV based on Business ID."""
    print(f"🚚 Starting data update for 'businesses' from '{file_path}'...")

    staging_table_name = "staging_businesses_update"
    # Staging table: JOIN KEY (id) + all new/updated columns
    staging_cols_with_types = [
        ("id", "TEXT"), # Join Key
        ("business_address", "TEXT"),
        ("business_unit", "TEXT"),
        ("business_city", "TEXT"),
        ("business_country", "TEXT"),
        ("business_zip", "TEXT"),
        ("business_state", "TEXT"),
        ("business_email_address", "TEXT")
    ]
    staging_col_names = [col[0] for col in staging_cols_with_types]

    create_temp_table_sql = sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT PRESERVE ROWS;").format(
        sql.Identifier(staging_table_name),
        sql.SQL(", ").join(
            sql.Identifier(name) + sql.SQL(" ") + sql.SQL(dtype) for name, dtype in staging_cols_with_types
        )
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_temp_table_sql)
            print(f"✅ Created temp table '{staging_table_name}' for processing.")

        total_size = os.path.getsize(file_path)
        print(f"Processing file ({total_size / (1024*1024):.2f} MB)...")

        df_chunk_iter = pd.read_csv(file_path, chunksize=25000, low_memory=False, encoding='utf-8', on_bad_lines='warn')
        
        total_rows = 0
        for chunk in df_chunk_iter:
            chunk.columns = [c.strip().lower().replace(' ', '_').replace('-', '_') for c in chunk.columns]
            chunk.rename(columns=column_map, inplace=True)
            
            # Filter chunk to only columns we care about
            final_chunk = chunk.reindex(columns=staging_col_names)

            buffer = StringIO()
            final_chunk.to_csv(buffer, index=False, header=False, sep='\t', quotechar='"', na_rep='\\N')
            buffer.seek(0)
            
            with conn.cursor() as c:
                c.execute(sql.SQL("TRUNCATE TABLE {};").format(sql.Identifier(staging_table_name)))
                c.copy_expert(
                    sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL '\\N')")
                    .format(sql.Identifier(staging_table_name), sql.SQL(', ').join(map(sql.Identifier, staging_col_names))),
                    buffer
                )

                # Run simple UPDATE...FROM on the primary key
                c.execute("""
                UPDATE businesses b SET
                    business_address = s.business_address,
                    business_unit = s.business_unit,
                    business_city = s.business_city,
                    business_country = s.business_country,
                    business_zip = s.business_zip,
                    business_state = s.business_state,
                    business_email_address = s.business_email_address
                FROM staging_businesses_update s
                WHERE b.id = s.id;
                """)
            
            conn.commit()
            total_rows += len(chunk)
            sys.stdout.write(f'\rUpdating... processed {total_rows:,} source rows.')
            sys.stdout.flush()

        print(f"\n✅ Successfully processed {total_rows:,} rows from CSV to update 'businesses'.")

    except Exception as e:
        print(f"\n❌ An error occurred during the 'businesses' update process: {e}")
        conn.rollback()
        raise e

def run_principals_update(conn, file_path, column_map):
    """
    Updates the PRINCIPALS table from its CSV.
    Uses a composite key of (business_id, name_c) to join.
    """
    print(f"🚚 Starting data update for 'principals' from '{file_path}'...")

    staging_table_name = "staging_principals_update"
    # Staging table: JOIN KEYS (business_id, name_c) + all new/updated columns
    staging_cols_with_types = [
        ("business_id", "TEXT"), # Join Key 1
        ("name_c", "TEXT"),      # Join Key 2
        ("address", "TEXT"),
        ("business_street_address_2", "TEXT"),
        ("business_street_address_3", "TEXT"),
        ("city", "TEXT"),
        ("state", "TEXT"),
        ("zip", "TEXT"),
        ("name_c_norm", "TEXT")
    ]
    staging_col_names = [col[0] for col in staging_cols_with_types]

    create_temp_table_sql = sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT PRESERVE ROWS;").format(
        sql.Identifier(staging_table_name),
        sql.SQL(", ").join(
            sql.Identifier(name) + sql.SQL(" ") + sql.SQL(dtype) for name, dtype in staging_cols_with_types
        )
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_temp_table_sql)
            print(f"✅ Created temp table '{staging_table_name}' for processing.")

        total_size = os.path.getsize(file_path)
        print(f"Processing file ({total_size / (1024*1024):.2f} MB)...")

        df_chunk_iter = pd.read_csv(file_path, chunksize=25000, low_memory=False, encoding='utf-8', on_bad_lines='warn')
        
        total_rows = 0
        for chunk in df_chunk_iter:
            chunk.columns = [c.strip().lower().replace(' ', '_').replace('-', '_') for c in chunk.columns]
            chunk.rename(columns=column_map, inplace=True)

            # CRITICAL: We must build the 'name_c' key exactly as import_data.py does
            # to ensure our composite join key matches the data already in the database.
            chunk['name_c'] = chunk.apply(construct_principal_name, axis=1)
            
            # Drop rows where our composite key is null, they can't be joined
            chunk.dropna(subset=['business_id', 'name_c'], inplace=True)
            chunk = chunk[chunk['name_c'] != '']
            
            # Create normalized name field
            # Define normalization helper
            def clean_name(x):
                if not isinstance(x, str): return ''
                n = x.strip().upper()
                n = re.sub(r'[^A-Z0-9 ]', '', n)
                n = re.sub(r'\s+', ' ', n).strip()
                # Typo fixes
                n = n.replace('GUREVITOH', 'GUREVITCH').replace('MANACHEM', 'MENACHEM').replace('MENACHERM', 'MENACHEM').replace('MENAHEM', 'MENACHEM').replace('GURAVITCH', 'GUREVITCH')
                # Middle initial strip
                parts = n.split()
                if len(parts) >= 3:
                     if len(parts[0]) > 1 and len(parts[-1]) > 1:
                         mid = [p for p in parts[1:-1] if len(p) > 1]
                         n = " ".join([parts[0]] + mid + [parts[-1]])
                return n

            chunk['name_c_norm'] = chunk['name_c'].apply(clean_name)

            # Filter chunk to only columns we care about
            final_chunk = chunk.reindex(columns=staging_col_names)

            buffer = StringIO()
            final_chunk.to_csv(buffer, index=False, header=False, sep='\t', quotechar='"', na_rep='\\N')
            buffer.seek(0)
            
            with conn.cursor() as c:
                c.execute(sql.SQL("TRUNCATE TABLE {};").format(sql.Identifier(staging_table_name)))
                c.copy_expert(
                    sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL '\\N')")
                    .format(sql.Identifier(staging_table_name), sql.SQL(', ').join(map(sql.Identifier, staging_col_names))),
                    buffer
                )

                # Run UPDATE...FROM on the composite natural key
                c.execute("""
                UPDATE principals p SET
                    address = s.address,
                    business_street_address_2 = s.business_street_address_2,
                    business_street_address_3 = s.business_street_address_3,
                    city = s.city,
                    state = s.state,
                    zip = s.zip,
                    name_c_norm = s.name_c_norm
                FROM staging_principals_update s
                WHERE p.business_id = s.business_id 
                  AND p.name_c = s.name_c;
                """)
            
            conn.commit()
            total_rows += len(chunk)
            sys.stdout.write(f'\rUpdating... processed {total_rows:,} source rows.')
            sys.stdout.flush()

        print(f"\n✅ Successfully processed {total_rows:,} rows from CSV to update 'principals'.")

    except Exception as e:
        print(f"\n❌ An error occurred during the 'principals' update process: {e}")
        conn.rollback()
        raise e


# =============================================================================
# INDEX CREATION
# =============================================================================

def create_new_indices(cursor, table_name):
    """Creates performance-enhancing indexes on newly added/updated columns."""
    print(f"🚀 Creating performance indexes for '{table_name}'...")
    try:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        print("✅ pg_trgm extension enabled.")
        
        if table_name == 'properties':
            print("⏳ Indexing properties (street_name, account_number, gis_tag)...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_street_name_gin ON properties USING gin (street_name gin_trgm_ops);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_account_num_gin ON properties USING gin (account_number gin_trgm_ops);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_gis_tag_gin ON properties USING gin (gis_tag gin_trgm_ops);")
            print("⏳ Indexing properties (number_of_units)...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_num_units ON properties (number_of_units);")
        
        elif table_name == 'businesses':
            print("⏳ Indexing businesses (business_address, business_email_address)...")
            # These columns are heavily searched
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_businesses_address_gin ON businesses USING gin (business_address gin_trgm_ops);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_businesses_email_gin ON businesses USING gin (business_email_address gin_trgm_ops);")

        elif table_name == 'principals':
            print("⏳ Indexing principals (business_id, name_c) composite key...")
            # CRITICAL: This index is required for the UPDATE query to be fast.
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_principals_bizid_name_composite ON principals (business_id, name_c);")
            print("⏳ Indexing principals (address)...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_principals_address_gin ON principals USING gin (address gin_trgm_ops);")
            print("⏳ Indexing principals (name_c_norm)...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_principals_name_c_norm ON principals (name_c_norm);")

        print(f"✅ All new performance indexes for '{table_name}' created successfully.")
    
    except psycopg2.Error as e:
        print(f"❌ An error occurred during index creation: {e}")
        raise


# =============================================================================
# MAIN EXECUTION
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Data update script for the property network database.")
    parser.add_argument(
        'table_name', 
        choices=['properties', 'businesses', 'principals'], 
        help="The name of the table to update."
    )
    args = parser.parse_args()
    
    target_table = args.table_name
    csv_file = DATA_SOURCES[target_table]
    
    # Try container path first, then local path
    csv_path = f'/app/data/{csv_file}'
    if not os.path.exists(csv_path):
        # Fallback to local relative path
        possible_paths = [
            os.path.join(os.getcwd(), 'data', csv_file),
            os.path.join(os.path.dirname(__file__), '..', 'data', csv_file),
            os.path.join(os.getcwd(), 'tow3', 'data', csv_file)
        ]
        for p in possible_paths:
            if os.path.exists(p):
                csv_path = p
                break
    
    print(f"Using data file: {csv_path}")

    if not DATABASE_URL:

        print("❌ Error: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)
        
    if not os.path.exists(csv_path):
        print(f"❌ Error: Source file '{csv_path}' not found. Cannot perform update.", file=sys.stderr)
        sys.exit(1)

    # --- Define Column Maps for each update process ---

    property_update_cols = {
        'pid': 'serial_number', 'owner': 'owner', 'co_owner': 'co_owner', 'link': 'link',
        'account_number': 'account_number', 'gis_tag': 'gis_tag', 'map': 'map', 'map_cut': 'map_cut',
        'block': 'block', 'block_cut': 'block_cut', 'lot': 'lot', 'lot_cut': 'lot_cut',
        'unit': 'unit', 'unit_cut': 'unit_cut', 'property_zip': 'property_zip',
        'property_county': 'property_county', 'street_name': 'street_name',
        'address_number': 'address_number', 'address_prefix': 'address_prefix',
        'address_suffix': 'address_suffix', 'cama_site_link': 'cama_site_link',
        'building_photo': 'building_photo', 'number_of_units': 'number_of_units'
    }
    
    business_update_cols = {
        'id': 'id', # Join Key
        'business_street': 'business_address', # Map CSV 'Business_Street' to DB 'business_address'
        'business_unit': 'business_unit',
        'business_city': 'business_city',
        'business_country': 'business_country',
        'business_zip': 'business_zip',
        'business_state': 'business_state',
        'business_email_address': 'business_email_address'
    }

    principal_update_cols = {
        'unique_key': 'business_id',   # Join Key 1
        'principal_name': 'name_c',    # Join Key 2 (raw value, processed by helper)
        'first_name': 'firstname',     # Needed for helper
        'middle_name': 'middlename',   # Needed for helper
        'last_name': 'lastname',     # Needed for helper
        'suffix': 'suffix',            # Needed for helper
        'business_street_address_1': 'address', # Update Target
        'business_street_address_2': 'business_street_address_2', # Update Target
        'business_street_address_3': 'business_street_address_3', # Update Target
        'business_city': 'city',       # Update Target
        'business_state': 'state',     # Update Target
        'business_zip_code': 'zip'     # Update Target
    }

    conn = None
    try:
        conn = get_db_connection()
        
        with conn.cursor() as cursor:
            # 1. Add new columns
            if target_table == 'properties':
                add_property_columns(cursor)
            elif target_table == 'businesses':
                add_business_columns(cursor)
            elif target_table == 'principals':
                add_principal_columns(cursor)
            conn.commit()

            # 2. Run the update data process
            if target_table == 'properties':
                run_properties_update(conn, csv_path, property_update_cols)
            elif target_table == 'businesses':
                run_businesses_update(conn, csv_path, business_update_cols)
            elif target_table == 'principals':
                run_principals_update(conn, csv_path, principal_update_cols)

            # 3. Create new indexes for this table
            create_new_indices(cursor, target_table)
            conn.commit()

    except Exception as e:
        print(f"\n❌ A critical error occurred in the main process: {e}")
        if conn:
            conn.rollback() # Rollback any failed transactions
    finally:
        if conn:
            conn.close()
            print("🔌 Database connection closed.")
        print(f"\n🎉 Data update process complete for '{target_table}'.")

if __name__ == "__main__":
    main()