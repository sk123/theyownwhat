import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import re

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
DATA_FILE = "/app/data/Active and Inconclusive Properties.xlsx"
# Fallback for local testing if file exists there
if not os.path.exists(DATA_FILE):
    DATA_FILE = "data/Active and Inconclusive Properties.xlsx"

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def normalize_address(addr):
    if not addr: return ""
    addr = str(addr).upper().strip()
    addr = re.sub(r'\s+', ' ', addr)
    addr = re.sub(r'[^\w\s]', '', addr)
    return addr

def import_nhpd():
    if not os.path.exists(DATA_FILE):
        print(f"❌ NHPD data file not found: {DATA_FILE}")
        return

    print(f"📖 Loading NHPD data from {DATA_FILE}...")
    df = pd.read_excel(DATA_FILE)
    
    # Filter for CT if not already done
    if 'State' in df.columns:
        df = df[df['State'] == 'CT'].copy()
    
    print(f"✅ Found {len(df)} properties in CT.")

    conn = get_db_connection()
    cur = conn.cursor()

    # We'll use a dictionary to cache property matches to avoid repeated queries
    match_cache = {}

    processed_count = 0
    matched_count = 0
    
    # Define the subsidy prefixes to look for
    subsidy_prefixes = [
        'S8_1', 'S8_2', 'S202_1', 'S202_2', 'S236_1', 'S236_2',
        'FHA_1', 'FHA_2', 'LIHTC_1', 'LIHTC_2', 'RHS515_1', 'RHS515_2',
        'RHS538_1', 'RHS538_2', 'HOME_1', 'HOME_2', 'PH_1', 'PH_2',
        'State_1', 'State_2', 'Pbv_1', 'Pbv_2', 'Mr_1', 'Mr_2', 'NHTF_1', 'NHTF_2'
    ]

    for _, row in df.iterrows():
        processed_count += 1
        nhpd_id = row.get('NHPDPropertyID')
        prop_name = row.get('PropertyName')
        addr = row.get('PropertyAddress')
        city = row.get('City')
        lat = row.get('Latitude')
        lon = row.get('Longitude')
        total_units = row.get('TotalUnits')
        owner = row.get('Owner')
        manager = row.get('ManagerName')

        # Try to find a matching property in our database
        # Strategy 1: Lat/Long proximity
        target_property_id = None
        
        if pd.notna(lat) and pd.notna(lon):
            # Find closest property within ~50 meters (roughly 0.0005 degrees)
            cur.execute("""
                SELECT id, location, property_city, 
                       (point(longitude, latitude) <-> point(%s, %s)) as distance
                FROM properties
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                AND latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
                ORDER BY distance ASC
                LIMIT 1
            """, (lon, lat, lat-0.0005, lat+0.0005, lon-0.0005, lon+0.0005))
            
            match = cur.fetchone()
            if match:
                target_property_id = match[0]
                matched_count += 1

        # Strategy 2: Address Match (if lat/long failed)
        if not target_property_id and addr:
            norm_addr = normalize_address(addr)
            # This is slow for a large DB, but we only do it for misses
            cur.execute("""
                SELECT id FROM properties 
                WHERE (location ILIKE %s OR street_name ILIKE %s) 
                AND property_city ILIKE %s 
                LIMIT 1
            """, (f"%{addr}%", f"%{addr}%", city))
            match = cur.fetchone()
            if match:
                target_property_id = match[0]
                matched_count += 1

        if target_property_id:
            # Sanitize None/NaN values for DB insertion
            safe_prop_name = str(prop_name) if pd.notna(prop_name) and str(prop_name).strip() else None
            
            # Management company must be string or None
            safe_manager = str(manager) if pd.notna(manager) and str(manager).lower() != 'nan' else None
            
            # Units must be int or None
            try:
                safe_units = int(total_units) if pd.notna(total_units) else None
            except:
                safe_units = None

            # Update property metadata
            cur.execute("""
                UPDATE properties 
                SET nhpd_id = %s, 
                    complex_name = COALESCE(complex_name, %s),
                    management_company = COALESCE(management_company, %s),
                    number_of_units = COALESCE(number_of_units, %s)
                WHERE id = %s
            """, (nhpd_id, safe_prop_name, safe_manager, safe_units, target_property_id))

            # Delete old subsidies for this property to avoid duplicates on re-import
            cur.execute("DELETE FROM property_subsidies WHERE property_id = %s", (target_property_id,))

            # Process subsidies
            for prefix in subsidy_prefixes:
                prog_name_col = f"{prefix}_ProgramName"
                end_date_col = f"{prefix}_EndDate"
                units_col = f"{prefix}_AssistedUnits"
                
                # Some prefixes might not have all columns (e.g. Pbv doesn't have ID in the list?)
                # Check if columns exist in the row
                if prog_name_col in row and pd.notna(row[prog_name_col]):
                    prog_name = row[prog_name_col]
                    expiry_date = row.get(end_date_col)
                    units = row.get(units_col)
                    
                    if pd.isna(expiry_date): expiry_date = None
                    if pd.isna(units): units = 0
                    
                    # Convert expiry_date to string if it's a timestamp
                    if isinstance(expiry_date, pd.Timestamp):
                        expiry_date = expiry_date.date()

                    cur.execute("""
                        INSERT INTO property_subsidies 
                        (property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (target_property_id, prog_name, prefix.split('_')[0], int(units), expiry_date, f"https://nhpd.preservationdatabase.org/Property/{nhpd_id}"))

        if processed_count % 100 == 0:
            print(f"⏳ Processed {processed_count}/{len(df)} properties... ({matched_count} matched)")
            conn.commit()

    conn.commit()
    cur.close()
    conn.close()
    
    print(f"🎉 Import complete! Processed: {processed_count}, Matched: {matched_count}")

if __name__ == "__main__":
    import_nhpd()
