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
DATA_SOURCES = {
    "businesses": "businesses.csv",
    "principals": "principals.csv",
    "properties": "new_parcels.csv", 
}

# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database with retries."""
    retries = 10
    while retries > 0:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            print("✅ Database connection successful.")
            return conn
        except psycopg2.OperationalError:
            print(f"⏳ Database not ready, retrying... ({retries} attempts left)")
            retries -= 1
            time.sleep(5)
    raise Exception("❌ Could not connect to the database after multiple retries.")

# --- Schema Creation ---
def create_schema(cursor):
    """Creates the necessary tables in the database if they don't exist."""
    print("🚀 Creating database schema...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS businesses (
        id TEXT PRIMARY KEY, name TEXT, status TEXT, date_of_formation DATE, business_type TEXT,
        nature_of_business TEXT, principal_name TEXT, principal_address TEXT, business_address TEXT,
        business_city TEXT, business_state TEXT, business_zip TEXT, mail_address TEXT, mail_city TEXT,
        mail_state TEXT, mail_zip TEXT, location TEXT
    );
    CREATE TABLE IF NOT EXISTS principals (
        id SERIAL PRIMARY KEY, business_id TEXT, name_c TEXT, firstname TEXT, middlename TEXT,
        lastname TEXT, suffix TEXT, address TEXT, city TEXT, state TEXT, zip TEXT, title TEXT
    );
    CREATE TABLE IF NOT EXISTS properties (
        id SERIAL PRIMARY KEY, serial_number TEXT, list_year INTEGER, property_city TEXT, owner TEXT,
        co_owner TEXT, location TEXT, property_type TEXT, living_area NUMERIC, year_built INTEGER,
        acres NUMERIC, zone TEXT, assessed_value NUMERIC, appraised_value NUMERIC,
        sale_amount NUMERIC, sale_date DATE,
        link TEXT,
        account_number TEXT,
        gis_tag TEXT,
        map TEXT,
        map_cut TEXT,
        block TEXT,
        block_cut TEXT,
        lot TEXT,
        lot_cut TEXT,
        unit TEXT,
        unit_cut TEXT,
        property_zip TEXT,
        property_county TEXT,
        street_name TEXT,
        address_number TEXT,
        address_prefix TEXT,
        address_suffix TEXT,
        cama_site_link TEXT,
        building_photo TEXT,
        number_of_units NUMERIC,
        latitude NUMERIC,
        longitude NUMERIC,
        mailing_address TEXT,
        mailing_city TEXT,
        mailing_state TEXT,
        mailing_zip TEXT
    );
    """)
    print("✅ Schema created successfully.")

# --- Generic CSV Importer ---
def import_csv_data(conn, table_name, file_name, column_map):
    """Imports data from a local CSV file with flexible column mapping."""
    print(f"🚚 Starting local import for '{table_name}' from '{file_name}'...")
    file_path = f'/app/data/{file_name}'
    if not os.path.exists(file_path):
        file_path = f'../data/{file_name}' # Fallback for local run
    
    try:
        total_size = os.path.getsize(file_path)
        print(f"Processing file ({total_size / (1024*1024):.2f} MB)...")

        chunk_iter = pd.read_csv(file_path, chunksize=50000, low_memory=False, encoding='utf-8', on_bad_lines='warn', quotechar='"', doublequote=True)
        
        total_rows = 0
        processed_size = 0

        for chunk in chunk_iter:
            chunk.columns = [c.strip().lower().replace(' ', '_').replace('-', '_') for c in chunk.columns]
            
            # --- FIX: Drop columns that will be overwritten by rename if they already exist ---
            for src, dst in column_map.items():
                if src != dst and dst in chunk.columns and src in chunk.columns:
                    chunk.drop(columns=[dst], inplace=True)
            
            # --- Apply column mapping ---
            chunk.rename(columns=column_map, inplace=True)

            # --- Data Type Coercion & Cleaning ---
            if table_name == 'businesses':
                if 'date_of_formation' in chunk.columns:
                    chunk['date_of_formation'] = pd.to_datetime(chunk['date_of_formation'], errors='coerce').dt.date

            if table_name == 'principals':
                def construct_name(row):
                    if 'name_c' in row and pd.notna(row['name_c']) and str(row['name_c']).strip():
                        return str(row['name_c']).strip()
                    parts = [row.get('firstname', ''), row.get('middlename', ''), row.get('lastname', ''), row.get('suffix', '')]
                    return ' '.join(str(p) for p in parts if pd.notna(p) and str(p).strip()).strip()

                chunk['name_c'] = chunk.apply(construct_name, axis=1)
                chunk.dropna(subset=['name_c', 'business_id'], inplace=True)
                chunk = chunk[chunk['name_c'] != '']
            
            if table_name == 'properties':
                if 'sale_date' in chunk.columns:
                    chunk['sale_date'] = pd.to_datetime(chunk['sale_date'], errors='coerce').dt.date
                # FIX: Coerce year columns to nullable integers to handle "YYYY.0" format
                if 'year_built' in chunk.columns:
                    chunk['year_built'] = pd.to_numeric(chunk['year_built'], errors='coerce').astype('Int64')
                if 'list_year' in chunk.columns:
                    chunk['list_year'] = pd.to_numeric(chunk['list_year'], errors='coerce').astype('Int64')
                
                # --- NEWLY ADDED ---
                if 'number_of_units' in chunk.columns:
                    chunk['number_of_units'] = pd.to_numeric(chunk['number_of_units'], errors='coerce')
                
                # --- ROBUSTNESS: Infer "Unit" from Location if missing ---
                # This mirrors the logic in update_vision_data.py and fix_units_context_aware.py
                if 'location' in chunk.columns and (( 'unit' not in chunk.columns ) or (chunk['unit'].isnull().any())):
                     if 'unit' not in chunk.columns:
                         chunk['unit'] = None
                     
                     # Define inference function
                     import re
                     def infer_unit(row):
                         if pd.notna(row['unit']) and str(row['unit']).strip():
                             return row['unit'] # Keep existing
                         
                         loc = str(row['location']).strip().upper()
                         # Regex: Space followed by optional "Unit", then (Single Uppercase Letter OR 1-4 Alphanumeric chars) at end of string
                         # We try to catch "Unit 11B" or "Unit 7" or just " 7"
                         m = re.search(r'(?:\sUNIT\s+|\s)([A-Z\d]{1,4})$', loc)
                         if m:
                             return m.group(1)
                         return None

                     # Apply to missing rows only to save time? Or apply to column. 
                     # Vectorized application is hard with regex group extraction in pandas without .str accessor which might be slow on huge chunks.
                     # Using apply is fine for 50k chunks.
                     chunk['unit'] = chunk.apply(infer_unit, axis=1)
                # -------------------

            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            db_cols_set = {row[0] for row in cursor.fetchall()}
            cursor.close()
            
            final_columns = [col for col in chunk.columns if col in db_cols_set]
            if not final_columns:
                print(f"⚠️ No matching columns found for this chunk in {file_name}. Skipping.")
                continue

            buffer = StringIO()
            chunk[final_columns].to_csv(buffer, index=False, header=False, sep='\t', quotechar='"', na_rep='\\N')
            buffer.seek(0)
            
            with conn.cursor() as c:
                c.copy_expert(
                    sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL '\\N')")
                    .format(sql.Identifier(table_name), sql.SQL(', ').join(map(sql.Identifier, final_columns))),
                    buffer
                )
            conn.commit()

            processed_size += chunk.memory_usage(index=True).sum()
            total_rows += len(chunk)
            progress = (processed_size / total_size) * 100 if total_size > 0 else 0
            sys.stdout.write(f'\rImporting... {progress:.1f}% complete ({total_rows:,} rows processed)')
            sys.stdout.flush()

        print(f"\n✅ Successfully imported {total_rows:,} rows into '{table_name}'.")

    except FileNotFoundError:
        print(f"\n❌ Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"\n❌ An error occurred during the import for {table_name}: {e}")
        conn.rollback()
        raise e

def create_indices(conn):
    """Creates performance-enhancing trigram indexes."""
    print("🚀 Creating performance indexes... (This may take several minutes)")
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            print("✅ pg_trgm extension enabled.")
            
            print("⏳ Indexing businesses name...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_businesses_name_gin ON businesses USING gin (name gin_trgm_ops);")
            
            print("⏳ Indexing principals name...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_principals_name_c_gin ON principals USING gin (name_c gin_trgm_ops);")
            
            print("⏳ Indexing properties owner...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_owner_gin ON properties USING gin (owner gin_trgm_ops);")
            
            print("⏳ Indexing properties location...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_location_gin ON properties USING gin (location gin_trgm_ops);")

            conn.commit()
            print("✅ All performance indexes created successfully.")
    except psycopg2.Error as e:
        print(f"❌ An error occurred during index creation: {e}")
        conn.rollback()

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Data import script for the property network explorer.")
    parser.add_argument('--force', nargs='*', help="List of tables to truncate and re-import (e.g., 'businesses principals').")
    args = parser.parse_args()
    
    force_tables = args.force if args.force is not None else []

    conn = None
    try:
        print("DEBUG: DATA_SOURCES:", DATA_SOURCES)
        conn = get_db_connection()
        with conn.cursor() as cursor:
            create_schema(cursor)
            conn.commit()
            
            business_cols = {
                'id': 'id', 'name': 'name', 'status': 'status', 
                'date_of_formation': 'date_of_formation',
                'date_of_organization_meeting': 'date_of_formation',
                'business_type': 'business_type', 'nature_of_business': 'nature_of_business', 
                'principal_name': 'principal_name', 'agent_name': 'principal_name',
                'agent': 'principal_name', 'registered_agent': 'principal_name',
                'registered_agent_name': 'principal_name',
                'principal_address': 'principal_address', 'agent_address': 'principal_address',
                'agent_street_address': 'principal_address',
                'business_address': 'business_address',
                'business_street_address_1': 'business_address',
                'street_address': 'business_address',
                'address1': 'business_address',
                'address_line_1': 'business_address',
                'street': 'business_address',
                'business_street': 'business_address',
                'business_city': 'business_city', 'business_state': 'business_state', 
                'business_zip': 'business_zip', 'mailing_address': 'mail_address', 
                'mail_city': 'mail_city', 'mail_state': 'mail_state', 'mail_zip': 'mail_zip'
            }
            
            principal_cols = {
                'unique_key': 'business_id',
                'principal_name': 'name_c',
                'first_name': 'firstname',
                'middle_name': 'middlename',
                'last_name': 'lastname',
                'suffix': 'suffix',
                'business_street_address_1': 'address',
                'business_city': 'city',
                'business_state': 'state',
                'business_zip_code': 'zip',
                'designation': 'title'
            }
            
            # --- UPDATED property_cols to include ALL fields ---
            property_cols = {
                'pid': 'serial_number', 
                'list_year': 'list_year',
                'property_city': 'property_city',
                'owner': 'owner', 
                'co_owner': 'co_owner', 
                'location_cama': 'location', # FIX: Use CAMA location, it's more accurate
                'style_desc': 'property_type', 
                'living_area': 'living_area', 
                'ayb': 'year_built', 
                'land_acres': 'acres', 
                'zone': 'zone', 
                'assessed_total': 'assessed_value',
                'appraised_total': 'appraised_value',
                'sale_price': 'sale_amount', 
                'sale_date': 'sale_date',
                'link': 'link',
                'account_number': 'account_number',
                'gis_tag': 'gis_tag',
                'map': 'map',
                'map_cut': 'map_cut',
                'block': 'block',
                'block_cut': 'block_cut',
                'lot': 'lot',
                'lot_cut': 'lot_cut',
                'unit': 'unit',
                'unit_cut': 'unit_cut',
                'property_zip': 'property_zip',
                'property_county': 'property_county',
                'street_name': 'street_name',
                'address_number': 'address_number',
                'address_prefix': 'address_prefix',
                'address_suffix': 'address_suffix',
                'cama_site_link': 'cama_site_link',
                'building_photo': 'building_photo',
                'number_of_units': 'number_of_units',
                'latitude': 'latitude',
                'longitude': 'longitude',
                'mailing_address': 'mailing_address',
                'mailing_city': 'mailing_city',
                'mailing_state': 'mailing_state',
                'mailing_zip': 'mailing_zip'
            }
            
            all_mappings = {}
            if "businesses" in DATA_SOURCES:
                all_mappings["businesses"] = (DATA_SOURCES["businesses"], business_cols)
            if "principals" in DATA_SOURCES:
                all_mappings["principals"] = (DATA_SOURCES["principals"], principal_cols)
            if "properties" in DATA_SOURCES:
                all_mappings["properties"] = (DATA_SOURCES["properties"], property_cols)

            for table_name, (file_name, col_map) in all_mappings.items():
                if table_name in force_tables:
                    print(f"⚠️  --force specified. Truncating table '{table_name}'...")
                    cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
                    conn.commit()
                    import_csv_data(conn, table_name, file_name, col_map)
                else:
                    cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1;")
                    if cursor.fetchone():
                        print(f"✅ Table '{table_name}' already contains data. Skipping import.")
                    else:
                        import_csv_data(conn, table_name, file_name, col_map)
        
        create_indices(conn)

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\n❌ A critical error occurred in the main process: {e}")
    finally:
        if conn:
            conn.close()
            print("🔌 Database connection closed.")
        print("\n🎉 Data import process complete!")

if __name__ == "__main__":
    main()