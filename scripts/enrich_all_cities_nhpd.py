#!/usr/bin/env python3
import os
import re
import sys
import logging
from collections import defaultdict
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
DATA_FILE = "data/Active and Inconclusive Properties.xlsx"

# Mapping from city to its corresponding State in the NHPD database
CITY_STATES = {
    "nyc": "NY",
    "dc": "DC",
    "baltimore": "MD",
    "boston": "MA",
    "detroit": "MI",
    "philadelphia": "PA",
    "chicago": "IL",
    "miami": "FL"
}

# Prefix list of NHPD subsidy program names to check
SUBSIDY_PREFIXES = [
    'S8_1', 'S8_2', 'S202_1', 'S202_2', 'S236_1', 'S236_2',
    'FHA_1', 'FHA_2', 'LIHTC_1', 'LIHTC_2', 'RHS515_1', 'RHS515_2',
    'RHS538_1', 'RHS538_2', 'HOME_1', 'HOME_2', 'PH_1', 'PH_2',
    'State_1', 'State_2', 'Pbv_1', 'Pbv_2', 'Mr_1', 'Mr_2', 'NHTF_1', 'NHTF_2'
]

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def normalize_address(addr):
    if not addr:
        return ""
    addr = str(addr).upper().strip()
    addr = re.sub(r'\s+', ' ', addr)
    addr = re.sub(r'[^\w\s]', '', addr)
    return addr

def extract_programs_for_row(row):
    """Extracts a comma-separated string of unique program names for active subsidies in the row."""
    programs = []
    for prefix in SUBSIDY_PREFIXES:
        col = f"{prefix}_ProgramName"
        if col in row and pd.notna(row[col]) and str(row[col]).strip():
            programs.append(str(row[col]).strip())
    return ", ".join(sorted(list(set(programs)))) if programs else "Unknown Program"

def enrich_all_cities():
    if not os.path.exists(DATA_FILE):
        logger.error(f"NHPD data file not found: {DATA_FILE}")
        return

    logger.info(f"Reading NHPD dataset from {DATA_FILE}...")
    try:
        nhpd_df = pd.read_excel(DATA_FILE)
    except Exception as e:
        logger.error(f"Failed to read NHPD spreadsheet: {e}")
        return

    conn = get_db_connection()
    try:
        for city, state_code in CITY_STATES.items():
            logger.info(f"=== Enriching {city.upper()} with NHPD data ===")
            
            # Filter NHPD records for this state
            state_df = nhpd_df[nhpd_df['State'] == state_code].copy()
            logger.info(f"Found {len(state_df)} NHPD records in state {state_code}")
            if state_df.empty:
                continue

            # Build coordinate grid lookup (O(N) search index)
            nhpd_grid = defaultdict(list)
            for _, row in state_df.iterrows():
                lat = row.get('Latitude')
                lon = row.get('Longitude')
                if pd.notna(lat) and pd.notna(lon):
                    grid_key = (round(lat, 3), round(lon, 3))
                    nhpd_grid[grid_key].append(row)

            # Build address mapping lookup
            nhpd_addr_map = {}
            for _, row in state_df.iterrows():
                addr = row.get('PropertyAddress')
                if pd.notna(addr):
                    nhpd_addr_map[normalize_address(addr)] = row

            # Fetch properties for this city
            with conn.cursor() as cur:
                cur.execute(f"SELECT bbl, address, latitude, longitude FROM {city}_properties")
                properties = cur.fetchall()

            logger.info(f"Loaded {len(properties)} properties from database for {city}")
            if not properties:
                continue

            matches = []
            for bbl, address, latitude, longitude in properties:
                matched_row = None
                
                # 1. Coordinate Proximity Matching (within ~50 meters)
                if latitude is not None and longitude is not None:
                    lat_val = float(latitude)
                    lon_val = float(longitude)
                    best_dist = 0.0005 # ~50m
                    
                    # Search neighboring grid cells
                    for d_lat in [-0.001, 0, 0.001]:
                        for d_lon in [-0.001, 0, 0.001]:
                            grid_key = (round(lat_val + d_lat, 3), round(lon_val + d_lon, 3))
                            for nhpd_row in nhpd_grid.get(grid_key, []):
                                n_lat = float(nhpd_row['Latitude'])
                                n_lon = float(nhpd_row['Longitude'])
                                dist = ((lat_val - n_lat)**2 + (lon_val - n_lon)**2)**0.5
                                if dist < best_dist:
                                    best_dist = dist
                                    matched_row = nhpd_row

                # 2. Address string matching fallback
                if matched_row is None and address:
                    norm_addr = normalize_address(address)
                    matched_row = nhpd_addr_map.get(norm_addr)

                # If matched, compile the stats record update
                if matched_row is not None:
                    prog_str = extract_programs_for_row(matched_row)
                    
                    # Determine expiration date
                    exp_date = matched_row.get('LatestEndDate')
                    if pd.isna(exp_date):
                        exp_date = matched_row.get('EarliestEndDate')
                    
                    if pd.notna(exp_date):
                        if isinstance(exp_date, pd.Timestamp):
                            exp_date = exp_date.date()
                    else:
                        exp_date = None

                    matches.append((
                        bbl,
                        True,
                        prog_str,
                        exp_date
                    ))

            logger.info(f"Matched {len(matches)} properties to NHPD records in {city}")
            if not matches:
                continue

            # Upsert into city_bbl_stats table
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {city}_bbl_stats (
                        bbl,
                        nhpd_subsidy,
                        nhpd_program,
                        nhpd_expiration,
                        updated_at
                    )
                    VALUES %s
                    ON CONFLICT (bbl) DO UPDATE SET
                        nhpd_subsidy = EXCLUDED.nhpd_subsidy,
                        nhpd_program = EXCLUDED.nhpd_program,
                        nhpd_expiration = EXCLUDED.nhpd_expiration,
                        updated_at = NOW()
                    """,
                    matches,
                    template="(%s, %s, %s, %s, NOW())"
                )
            conn.commit()
            logger.info(f"Successfully updated {len(matches)} stats records for {city}")

    finally:
        conn.close()

if __name__ == "__main__":
    enrich_all_cities()
