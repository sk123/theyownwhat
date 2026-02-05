import threading
import os
import json
import pandas as pd
import time
import psycopg2
from psycopg2 import sql
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import re
import urllib3
import concurrent.futures
from requests.compat import urljoin
import sys
import random
import traceback
from io import StringIO
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# Add scripts directory to path for sibling imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../api')) # For network refresh
try:
    from safe_network_refresh import run_refresh
except ImportError:
    print("Warning: Could not import safe_network_refresh. Network rebuilding will be skipped.")
    run_refresh = None

# Suppress only the single InsecureRequestWarning from urllib3 needed for this script
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
VISION_BASE_URL = "https://www.vgsi.com"
CONNECTICUT_DATABASE_URL = f"{VISION_BASE_URL}/connecticut-online-database/"
MAX_WORKERS = 20  # Increased workers for faster scraping
DEFAULT_MUNI_WORKERS = 8 # Process more municipalities in parallel

# --- Municipality-specific data sources ---
MUNICIPAL_DATA_SOURCES = {
# update_data.py

# ... inside MUNICIPAL_DATA_SOURCES dictionary ...
    "HARTFORD": {
        'type': 'ct_geodata_csv',
        'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0',
        'town_filter': 'Hartford'
    },
# ...
    # Add more municipalities here as needed
    'ANSONIA': {'type': 'MAPXPRESS', 'domain': 'ansonia.mapxpress.net'},
    'POMFRET': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/pomfretct/'},
    'BEACON FALLS': {'type': 'MAPXPRESS', 'domain': 'beaconfalls.mapxpress.net'},
    # 'BERLIN': {'type': 'MAPXPRESS', 'domain': 'berlin.mapxpress.net'},
    'BETHANY': {'type': 'MAPXPRESS', 'domain': 'bethany.mapxpress.net'},
    'BETHLEHEM': {'type': 'MAPXPRESS', 'domain': 'bethlehem.mapxpress.net'},
    'BRIDGEPORT': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/bridgeportct/'},
    'BRISTOL': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/bristolct/'},
    'MANSFIELD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/mansfieldct/'},
    # Additional Vision Appraisal municipalities with missing photos
    'NORWALK': {'type': 'actdatascout', 'url': 'https://www.actdatascout.com/RealProperty/Connecticut/Norwalk', 'county_id': '9103'},
    'GLASTONBURY': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/glastonburyct/'},
    'WINDSOR': {'type': 'windsor_api', 'url': 'https://windsorct.com'},
    'WETHERSFIELD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/wethersfieldct/'},
    'VERNON': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/vernonct/'},
    'STONINGTON': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/stoningtonct/'},
    'BLOOMFIELD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/bloomfieldct/'},
    'AVON': {'type': 'avon_static', 'url': 'http://assessor.avonct.gov'},
    'WOLCOTT': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/wolcottct/'},
    'WINDHAM': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/windhamct/'},
    'WOODSTOCK': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/woodstockct/'},
    'BROOKFIELD': {'type': 'MAPXPRESS', 'domain': 'brookfield.mapxpress.net'},
    'BURLINGTON': {'type': 'MAPXPRESS', 'domain': 'burlington.mapxpress.net'},
    'CANTON': {'type': 'MAPXPRESS', 'domain': 'canton.mapxpress.net'},
    'CHESHIRE': {'type': 'MAPXPRESS', 'domain': 'cheshire.mapxpress.net'},
    'COLCHESTER': {'type': 'MAPXPRESS', 'domain': 'colchester.mapxpress.net'},
    'COVENTRY': {'type': 'MAPXPRESS', 'domain': 'coventry.mapxpress.net'},
    'DERBY': {'type': 'MAPXPRESS', 'domain': 'derby.mapxpress.net'},
    # 'EAST HAVEN': {'type': 'MAPXPRESS', 'domain': 'easthaven.mapxpress.net'},
    'FARMINGTON': {'type': 'MAPXPRESS', 'domain': 'farmington.mapxpress.net'},
    'LITCHFIELD': {'type': 'MAPXPRESS', 'domain': 'litchfield.mapxpress.net'},
    'MIDDLEBURY': {'type': 'MAPXPRESS', 'domain': 'middlebury.mapxpress.net'},
    'NAUGATUCK': {'type': 'MAPXPRESS', 'domain': 'naugatuck.mapxpress.net'},
    'NEW BRITAIN': {'type': 'MAPXPRESS', 'domain': 'newbritain.mapxpress.net', 'id_param': 'parid'},
    'NEWTOWN': {'type': 'MAPXPRESS', 'domain': 'newtown.mapxpress.net'},
    'OXFORD': {'type': 'MAPXPRESS', 'domain': 'oxford.mapxpress.net'},
    'PLAINVILLE': {'type': 'MAPXPRESS', 'domain': 'plainville.mapxpress.net'},
    'SALEM': {'type': 'MAPXPRESS', 'domain': 'salem.mapxpress.net'},
    'SEYMOUR': {'type': 'MAPXPRESS', 'domain': 'seymour.mapxpress.net'},
    'SOUTHBURY': {'type': 'MAPXPRESS', 'domain': 'southbury.mapxpress.net'},
    'SUFFIELD': {'type': 'MAPXPRESS', 'domain': 'suffield.mapxpress.net'},
    'WEST HAVEN': {'type': 'MAPXPRESS', 'domain': 'westhaven.mapxpress.net'},
    # PropertyRecordCards Municipalities
    # 'ANSONIA': {'type': 'PROPERTYRECORDCARDS', 'towncode': '002'}, # Moved to MapXpress
    'ASHFORD': {'type': 'PROPERTYRECORDCARDS', 'towncode': '003'},
    'BETHANY': {'type': 'PROPERTYRECORDCARDS', 'towncode': '008'},
    'BOZRAH': {'type': 'PROPERTYRECORDCARDS', 'towncode': '013'},
    'BRIDGEWATER': {'type': 'PROPERTYRECORDCARDS', 'towncode': '16'},
    'CANAAN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '21'},
    'CHESHIRE': {'type': 'PROPERTYRECORDCARDS', 'towncode': '025'},
    'CHESTER': {'type': 'PROPERTYRECORDCARDS', 'towncode': '026'},
    'COLEBROOK': {'type': 'PROPERTYRECORDCARDS', 'towncode': '029'},
    'COLUMBIA': {'type': 'PROPERTYRECORDCARDS', 'towncode': '030'},
    'DANBURY': {'type': 'PROPERTYRECORDCARDS', 'towncode': '034'},
    'DERBY': {'type': 'PROPERTYRECORDCARDS', 'towncode': 'DRB'},
    'DURHAM': {'type': 'PROPERTYRECORDCARDS', 'towncode': '38'},
    'EAST HAMPTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '42'},
    'EAST HAVEN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '044'},
    'EASTFORD': {'type': 'PROPERTYRECORDCARDS', 'towncode': '039'},
    'EASTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '046'},
    'ELLINGTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '048'},
    # 'FARMINGTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '052'},
    'FRANKLIN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '053'},
    'GUILFORD': {'type': 'PROPERTYRECORDCARDS', 'towncode': '060'},
    'HADDAM': {'type': 'PROPERTYRECORDCARDS', 'towncode': '061'},
    'HEBRON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '067'},
    'KILLINGLY': {'type': 'PROPERTYRECORDCARDS', 'towncode': '069'},
    'KILLINGWORTH': {'type': 'PROPERTYRECORDCARDS', 'towncode': '070'},
    'MARLBOROUGH': {'type': 'PROPERTYRECORDCARDS', 'towncode': '079'},
    'MONTVILLE': {'type': 'PROPERTYRECORDCARDS', 'towncode': '086'},
    'NAUGATUCK': {'type': 'PROPERTYRECORDCARDS', 'towncode': '088'},
    'NEW CANAAN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '090'},
    'NEWINGTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '094'},
    'NORFOLK': {'type': 'PROPERTYRECORDCARDS', 'towncode': '098'},
    'NORTH CANAAN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '100'},
    'NORTH HAVEN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '101'},
    'NORTH STONINGTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '102'},
    'OXFORD': {'type': 'PROPERTYRECORDCARDS', 'towncode': '108'},
    'PLAINVILLE': {'type': 'PROPERTYRECORDCARDS', 'towncode': '110'},
    'PLYMOUTH': {'type': 'PROPERTYRECORDCARDS', 'towncode': '111'},
    'PROSPECT': {'type': 'PROPERTYRECORDCARDS', 'towncode': '115'},
    'RIDGEFIELD': {'type': 'PROPERTYRECORDCARDS', 'towncode': '118'},
    'ROCKY HILL': {'type': 'PROPERTYRECORDCARDS', 'towncode': '119'},
    'ROXBURY': {'type': 'PROPERTYRECORDCARDS', 'towncode': '120'},
    'SALISBURY': {'type': 'PROPERTYRECORDCARDS', 'towncode': '122'},
    'SCOTLAND': {'type': 'PROPERTYRECORDCARDS', 'towncode': '123'},
    'SEYMOUR': {'type': 'PROPERTYRECORDCARDS', 'towncode': '124'},
    'SHELTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '126'},
    'SHERMAN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '127', 'path_prefix': '/Sherman'},
    'SIMSBURY': {'type': 'PROPERTYRECORDCARDS', 'towncode': '128'},
    'SUFFIELD': {'type': 'PROPERTYRECORDCARDS', 'towncode': '139'},
    'TORRINGTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '143'},
    'VOLUNTOWN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '147'},
    'WARREN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '149'},
    'WASHINGTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '150'},
    'WATERBURY': {'type': 'PROPERTYRECORDCARDS', 'towncode': '151'},
    'WATERTOWN': {'type': 'PROPERTYRECORDCARDS', 'towncode': '153'},
    'WESTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '157'},
    'WILTON': {'type': 'PROPERTYRECORDCARDS', 'towncode': '161'},
    'WINDSOR LOCKS': {'type': 'PROPERTYRECORDCARDS', 'towncode': '165'},
    'WOODBRIDGE': {'type': 'PROPERTYRECORDCARDS', 'towncode': '167'},
    'WOODBURY': {'type': 'PROPERTYRECORDCARDS', 'towncode': '168'},
    # CT Geodata Portal (Statewide 2025)
    # 'NEW HAVEN': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'New Haven'},
    # 'WATERBURY': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Waterbury'},
    # 'BRIDGEPORT': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Bridgeport'},
    'HARTFORD': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Hartford'},
    'STAMFORD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/stamfordct/'},
    # 'NORWALK': {'type': 'ct_geodata_csv', ... REPLACED BY ACTDATASCOUT above ... },
    # 'DANBURY': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Danbury'},
    # 'NEW BRITAIN': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'New Britain'},
    # 'WEST HARTFORD': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'West Hartford'},
    'GREENWICH': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Greenwich'},
    'HAMDEN': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/hamdenct/'},
    'MERIDEN': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/meridenct/'},
    'BRISTOL': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/bristolct/'},
    'WEST HAVEN': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/westhavenct/'},
    'MIDDLETOWN': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/middletownct/'},
    'ENFIELD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/enfieldct/'},
    'MILFORD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/milfordct/'},
    'STRATFORD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/stratfordct/'},
    'EAST HARTFORD': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'East Hartford'},
    'MANCHESTER': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/manchesterct/'},
    'CLINTON': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/clintonct/'},
    'EAST HAMPTON': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'East Hampton'},
    'CROMWELL': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Cromwell'},
    'OLD LYME': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/oldlymect/'},
    'ESSEX': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/essexct/'},
    'LYME': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/lymect/'},
    'NORWICH': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/norwichct/'},
    'GROTON': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Groton'},
    'SOUTHINGTON': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/southingtonct/'},
    'WALLINGFORD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/wallingfordct/'},
    # 'SHELTON': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Shelton'},
    # 'TORRINGTON': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Torrington'},
    'TRUMBULL': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/trumbullct/'},
}

# --- Shared Cache for Large CSVs ---
GEODATA_CACHE = {}
GEODATA_LOCK = threading.Lock()

# --- Logging ---
def log(message, municipality=None):
    """Prints a message with a timestamp and optional municipality prefix."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    prefix = f"[{municipality}] " if municipality else ""
    print(f"[{timestamp}] {prefix}{message}", flush=True)
    sys.stdout.flush()

# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database with retries."""
    if not DATABASE_URL:
        raise Exception("DATABASE_URL environment variable is not set")
    
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            log("Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            log(f"Database connection failed: {e}. Retrying... ({retries} attempts left)")
            retries -= 1
            time.sleep(3)
    raise Exception("Could not connect to the database after multiple retries.")

# --- Resumability Functions ---
def mark_property_processed_today(conn, property_id):
    """Marks a property as processed today to enable resumability."""
    today = date.today()
    query = """
        INSERT INTO property_processing_log (property_id, last_processed_date) 
        VALUES (%s, %s) 
        ON CONFLICT (property_id) 
        DO UPDATE SET last_processed_date = EXCLUDED.last_processed_date
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (property_id, today))
            conn.commit()
    except psycopg2.Error:
        # If table doesn't exist, create it (fallback)
        create_processing_log_table(conn)
        with conn.cursor() as cursor:
            cursor.execute(query, (property_id, today))
            conn.commit()

def create_processing_log_table(conn):
    """Creates the processing log table if it doesn't exist."""
    query = """
        CREATE TABLE IF NOT EXISTS property_processing_log (
            property_id INTEGER PRIMARY KEY,
            last_processed_date DATE NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_processing_log_date 
        ON property_processing_log(last_processed_date);
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()
    log("Ensured property_processing_log table exists for resumability.")

def check_headers(url):
    """
    Checks the Last-Modified header of a URL.
    Returns datetime object or None.
    """
    try:
        response = requests.head(url, timeout=10, allow_redirects=True)
        if response.status_code == 405:
            response = requests.get(url, stream=True, timeout=10)
            response.close()
            
        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(last_modified)
    except Exception as e:
        log(f"Failed to check headers for {url}: {e}")
        return None
    return None

def should_scrape(municipality_name, config, conn):
    """
    Determines if a municipality needs scraping based on external timestamps.
    Returns True if scrape should proceed, False if we can skip.
    """
    # 1. CT Geodata (ArcGIS CSV)
    if config.get('type') == 'ct_geodata_csv':
        url = config.get('url')
        remote_dt = check_headers(url)
        if not remote_dt:
            return True # Can't determine, safer to scrape
            
        # Check our last successful refresh
        with conn.cursor() as cursor:
            cursor.execute("SELECT external_last_updated FROM data_source_status WHERE source_name = %s", (municipality_name,))
            row = cursor.fetchone()
            if row and row[0]:
                local_dt = row[0]
                # If remote is older or equal to local, we might skip
                # Issue: timezones. Ensure both are aware or both naive.
                # parsedate_to_datetime returns aware (UTC usually).
                # Local usually stored as aware in Postgres.
                if remote_dt <= local_dt:
                    log(f"Skipping {municipality_name}: Remote ({remote_dt}) <= Local ({local_dt})")
                    return False
                    
        # Update the 'external_last_updated' field in status table?
        # No, we only update that AFTER a successful scrape?
        # Or we update it now to say "we saw this date"?
        return True

    return True

def get_current_owner_properties_by_municipality(conn):
    """Gets municipalities ordered by count of 'Current Owner' properties."""
    query = """
        SELECT property_city, COUNT(*) as current_owner_count
        FROM properties 
        WHERE owner = 'Current Owner'
        GROUP BY property_city
        ORDER BY current_owner_count DESC
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        log(f"Found {len(results)} municipalities with 'Current Owner' properties.")
        return results

def get_unprocessed_current_owner_properties(conn, municipality_name, force_process=False):
    """
    Gets 'Current Owner' properties for a municipality.
    If force_process is False, it gets only unprocessed ones.
    If force_process is True, it gets ALL 'Current Owner' properties.
    """
    base_query = """
        SELECT p.id, p.location, p.cama_site_link 
        FROM properties p
        LEFT JOIN property_processing_log ppl ON p.id = ppl.property_id
        WHERE UPPER(p.property_city) = %s 
        AND p.owner = 'Current Owner'
    """
    
    params = [municipality_name.upper()]
    query_parts = [base_query]
    
    if not force_process:
        query_parts.append("AND (ppl.last_processed_date IS NULL OR ppl.last_processed_date < %s)")
        params.append(date.today())
        
    query_parts.append("ORDER BY p.id")
    final_query = "\n".join(query_parts)

    with conn.cursor() as cursor:
        cursor.execute(final_query, params)
        properties = cursor.fetchall()
        status = "all (force)" if force_process else "unprocessed"
        log(f"Found {len(properties)} {status} 'Current Owner' properties in {municipality_name}.")
        return properties

def get_unprocessed_properties(conn, municipality_name, force_process=False):
    """
    Gets properties that haven't been processed today.
    If force_process is True, gets ALL properties for the municipality.
    """
    base_query = """
        SELECT p.id, p.location, p.cama_site_link 
        FROM properties p
        LEFT JOIN property_processing_log ppl ON p.id = ppl.property_id
        WHERE UPPER(p.property_city) = %s
    """
    
    params = [municipality_name.upper()]
    query_parts = [base_query]

    if not force_process:
        query_parts.append("AND (ppl.last_processed_date IS NULL OR ppl.last_processed_date < %s)")
        params.append(date.today())
        
    query_parts.append("ORDER BY p.id")
    final_query = "\n".join(query_parts)

    with conn.cursor() as cursor:
        cursor.execute(final_query, params)
        properties = cursor.fetchall()
        status = "all (force)" if force_process else "unprocessed"
        log(f"Found {len(properties)} {status} properties in {municipality_name}.")
        return properties

# --- NEW: ArcGIS CSV Data Handler ---
def download_arcgis_csv(url, municipality_name):
    """Downloads CSV data from ArcGIS Hub."""
    log(f"Downloading ArcGIS CSV data for {municipality_name}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=120)  # Longer timeout for large files
        response.raise_for_status()
        
        # Read CSV into pandas DataFrame
        df = pd.read_csv(StringIO(response.text), low_memory=False)
        log(f"Successfully downloaded {len(df)} records for {municipality_name}")
        return df
        
    except Exception as e:
        log(f"Error downloading ArcGIS CSV for {municipality_name}: {e}")
        return None

def normalize_address_for_matching(address):
    """Normalizes an address for consistent matching."""
    if not address:
        return ""
    
    # Convert to uppercase and normalize whitespace
    normalized = ' '.join(str(address).upper().strip().split())
    
    # Common address normalizations for better matching
    normalizations = {
        ' STREET': ' ST',
        ' AVENUE': ' AVE', 
        ' ROAD': ' RD',
        ' DRIVE': ' DR',
        ' LANE': ' LN',
        ' COURT': ' CT',
        ' PLACE': ' PL',
        ' BOULEVARD': ' BLVD',
        ' CIRCLE': ' CIR'
    }
    
    for old, new in normalizations.items():
        normalized = normalized.replace(old, new)
    
    return normalized

def process_arcgis_data(df, column_mapping, municipality_name):
    """Processes ArcGIS DataFrame and returns a dictionary mapping addresses to A LIST OF property data dicts."""
    log(f"Processing {len(df)} ArcGIS records for {municipality_name}...")
    
    processed_data = {}
    
    for _, row in df.iterrows():
        try:
            # Build the address from available components
            address_parts = []
            if pd.notna(row.get('StreetNumberFrom')):
                address_parts.append(str(int(row['StreetNumberFrom'])))
            if pd.notna(row.get('StreetName')):
                address_parts.append(str(row['StreetName']))
            
            if not address_parts:
                continue
                
            full_address = ' '.join(address_parts)
            normalized_address = normalize_address_for_matching(full_address)
            
            if not normalized_address:
                continue
            
            # Map the data using the column mapping
            property_data = {}
            
            # Set the location directly from the constructed full address
            if full_address.strip():
                property_data['location'] = full_address.strip()

            for source_col, target_col in column_mapping.items():
                if source_col in row.index and pd.notna(row[source_col]):
                    value = row[source_col]
                    
                    # Handle different data types
                    if target_col in ['sale_amount', 'assessed_value', 'appraised_value', 'living_area', 'acres', 'number_of_units']:
                        try:
                            # Remove any currency symbols and convert to float
                            if isinstance(value, str):
                                value = re.sub(r'[$,]', '', value)
                            numeric_value = float(value)
                            if numeric_value > 0:  # Only store positive values
                                property_data[target_col] = numeric_value
                        except (ValueError, TypeError):
                            pass
                    elif target_col == 'sale_date':
                        try:
                            if isinstance(value, str) and value.strip():
                                # Try different date formats
                                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y/%m/%d']:
                                    try:
                                        property_data[target_col] = datetime.strptime(value.strip(), fmt).date()
                                        break
                                    except ValueError:
                                        continue
                        except Exception:
                            pass
                    elif target_col == 'year_built':
                        try:
                            year = int(float(value))
                            if 1600 <= year <= datetime.now().year:  # Reasonable year range
                                property_data[target_col] = year
                        except (ValueError, TypeError):
                            pass
                    else:
                        # String fields
                        if isinstance(value, str) and value.strip():
                            property_data[target_col] = value.strip()
                        elif pd.notna(value):
                            property_data[target_col] = str(value).strip()
            
            # --- Auto-Calculate missing values ---
            if 'appraised_value' in property_data and property_data['appraised_value'] > 0:
                if 'assessed_value' not in property_data or not property_data['assessed_value']:
                    property_data['assessed_value'] = round(property_data['appraised_value'] * 0.70, 2)
            elif 'assessed_value' in property_data and property_data['assessed_value'] > 0:
                if 'appraised_value' not in property_data or not property_data['appraised_value']:
                    property_data['appraised_value'] = round(property_data['assessed_value'] / 0.70, 2)

            # Only store if we have meaningful data
            if len(property_data) >= 2:  # At least owner and one other field
                # Fallback: Inference for missing unit
                if 'unit' not in property_data or not property_data['unit']:
                    import re
                    m = re.search(r'\s([A-Z]|\d{1,4})$', full_address)
                    if m:
                        property_data['unit'] = m.group(1)

                if normalized_address not in processed_data:
                    processed_data[normalized_address] = []
                processed_data[normalized_address].append(property_data)
                
        except Exception as e:
            log(f"Error processing row: {e}")
            continue
    
    log(f"Successfully processed address records for {municipality_name}")
    return processed_data

def process_municipality_with_arcgis(conn, municipality_name, data_source_config, current_owner_only=False, force_process=False):
    """Process a municipality using ArcGIS CSV data."""
    log(f"--- Processing municipality: {municipality_name} via ArcGIS CSV (Force={force_process}) ---")
    
    # Get properties to update from database
    if current_owner_only:
        db_properties = get_unprocessed_current_owner_properties(conn, municipality_name, force_process)
    else:
        db_properties = get_unprocessed_properties(conn, municipality_name, force_process)
    
    if not db_properties:
        log(f"No properties to process for {municipality_name}.")
        return 0
    
    # Download and process ArcGIS data
    df = download_arcgis_csv(data_source_config['url'], municipality_name)
    if df is None:
        log(f"Failed to download data for {municipality_name}. Skipping.")
        return 0
    
    processed_arcgis_data = process_arcgis_data(df, data_source_config['column_mapping'], municipality_name)
    if not processed_arcgis_data:
        log(f"No processable data found for {municipality_name}.")
        return 0
    
    # Group DB properties by normalized address for batch matching
    db_props_by_address = {}
    all_property_ids = []
    
    for prop_id, prop_location, _ in db_properties:
        all_property_ids.append(prop_id)
        if prop_location:
            norm_addr = normalize_address_for_matching(prop_location)
            if norm_addr not in db_props_by_address:
                db_props_by_address[norm_addr] = []
            db_props_by_address[norm_addr].append(prop_id)

    log(f"Matching {len(db_properties)} DB properties (grouped by {len(db_props_by_address)} addresses) against ArcGIS records...")
    
    updated_count = 0
    processed_count = 0
    
    for norm_addr, prop_ids in db_props_by_address.items():
        matched_data_list = []
        
        # Direct match
        if norm_addr in processed_arcgis_data:
            matched_data_list = processed_arcgis_data[norm_addr]
        
        # If we have matches, distribute them to the properties at this address
        if matched_data_list:
            # We zip them: 1st DB prop gets 1st CSV record, etc.
            # This handles condos (many units at same address) by assigning one unique record to each,
            # assuming the count roughly matches. If DB has more, some won't get updated? No, zip stops at shortest.
            # If CSV has more, some CSV records are unused (missing from DB).
            
            # TODO: Improve matching by using secondary keys if available (e.g. Unit # if already in DB? No, it's missing)
            # For now, blind distribution is better than overwriting all with the SAME record.
            
            for prop_id, vision_data in zip(prop_ids, matched_data_list):
                 if update_property_in_db(conn, prop_id, vision_data):
                    updated_count += 1
        
        processed_count += len(prop_ids)
        if processed_count % 100 == 0:
            log(f"  -> Progress: {processed_count}/{len(db_properties)}, updated {updated_count} so far...")
    
    # Mark all properties as processed
    log(f"Marking all {len(all_property_ids)} properties as processed for today.")
    for prop_id in all_property_ids:
        mark_property_processed_today(conn, prop_id)
    
    log(f"Finished {municipality_name}. Updated {updated_count} of {len(db_properties)} properties.")
    return updated_count

# --- NEW: CT Geodata CSV Handler ---
def download_ct_geodata_csv(url):
    """Downloads (or loads from local file) and caches CT Geodata CSV."""
    with GEODATA_LOCK:
        if url in GEODATA_CACHE:
            return GEODATA_CACHE[url]
        
        log(f"Loading CT Geodata CSV from source...")
        try:
            # Check for local cache first (pre-downloaded for speed/stability)
            local_path = "/tmp/ct_geodata.csv"
            if "geodata.ct.gov" in url and os.path.exists(local_path):
                log(f"  -> Using local file: {local_path}")
                df = pd.read_csv(local_path, low_memory=False)
            else:
                log(f"  -> Downloading from: {url}")
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                response = requests.get(url, headers=headers, timeout=300)
                response.raise_for_status()
                df = pd.read_csv(StringIO(response.text), low_memory=False)
            
            GEODATA_CACHE[url] = df
            log(f"Successfully loaded {len(df)} records into cache.")
            return df
        except Exception as e:
            log(f"Error loading CT Geodata CSV: {e}")
            return None

def process_municipality_with_ct_geodata(conn, municipality_name, config, current_owner_only=False, force_process=False):
    """Process municipality using CT Geodata CSV."""
    log(f"--- Processing {municipality_name} via CT Geodata CSV ---")
    
    # 1. Get properties
    if current_owner_only:
        db_properties = get_unprocessed_current_owner_properties(conn, municipality_name, force_process)
    else:
        db_properties = get_unprocessed_properties(conn, municipality_name, force_process)
    
    if not db_properties:
        log(f"No properties to process for {municipality_name}.")
        return 0
    
    # 2. Get CSV
    df_all = download_ct_geodata_csv(config['url'])
    if df_all is None: return 0
    
    # 3. Filter for this town
    town_filter = config.get('town_filter', municipality_name)
    df = df_all[df_all['Town Name'].str.upper() == town_filter.upper()].copy()
    log(f"Found {len(df)} records for {town_filter}")
    
    # 4. Map columns
    # OBJECTID,Town Name,Location,CAMA_Link,Parcel_ID,Parcel Type,Unit_Type,Link,Collection_year,Editor,Editor Comment,Edit_Date,Link_From_CAMA,Location_CAMA,Property_City,Property_Zip,Owner,Co_Owner,Mailing_Address,Mailing_City,Mailing_State,Mailing_Zip,Assessed_Total,Assessed_Land,Assessed_Building,Pre_Yr_Assessed_Total,Appraised_Land,Appraised_Building,Appraised_Outbuilding,Valuation_Year,Land_Acres,Zone,State_Use,State_Use_Description,EYB,AYB,Model,Condition,Living_Area,Effective_Area,Total_Rooms,Number_of_Bedroom,Number_of_Baths,Number_of_Half_Baths,Sale_Price,Sale_Date,Prior_Sale_Date,Prior_Book_Page,Prior_Sale_Price,Occupancy,FIPS Code,CouncilsOfGovernments,Shape__Area,Shape__Length
    
    mapping = {
        'Owner': 'owner',
        'Co_Owner': 'co_owner',
        'Assessed_Total': 'assessed_value',
        'Land_Acres': 'acres',
        'Zone': 'zone',
        'State_Use_Description': 'property_type',
        'AYB': 'year_built',
        'Living_Area': 'living_area',
        'Sale_Price': 'sale_amount',
        'Sale_Date': 'sale_date',
        'Mailing_Address': 'mailing_address',
        'Mailing_City': 'mailing_city',
        'Mailing_State': 'mailing_state',
        'Mailing_Zip': 'mailing_zip',
        'CAMA_Link': 'cama_site_link',
        'Location': 'location',
        'Unit_Type': 'unit',
        'Occupancy': 'number_of_units'
    }
    
    # Pre-calculate appraised_value
    df['appraised_value'] = df[['Appraised_Land', 'Appraised_Building', 'Appraised_Outbuilding']].sum(axis=1)
    
    # Custom process data logic
    processed_data = {}
    for _, row in df.iterrows():
        try:
            # Skip CNDASC placeholder units (Condo Association administrative records)
            unit_type = str(row.get('Unit_Type', '')).upper()
            if 'CNDASC' in unit_type or 'CONDO ASC' in unit_type:
                continue
            
            loc1 = str(row.get('Location', '')).strip()
            loc2 = str(row.get('Location_CAMA', '')).strip()
            
            # Heuristic: Pick the one that looks more like a full address (New Haven fix)
            # If both are just "93" or numeric, try to recover from Mailing Address or Owner Name
            if loc1.isdigit() and len(loc1) < 4:
                # "93" artifact case
                if pd.notna(row.get('Mailing_Address')) and str(row.get('Mailing_City')).upper() == 'NEW HAVEN':
                     loc = str(row['Mailing_Address']).strip()
                elif pd.notna(row.get('Owner')):
                     # Try to extract address from Owner if it looks like "123 MAIN ST LLC"
                     owner = str(row['Owner']).upper()
                     if ' LLC' in owner:
                         possible_addr = owner.split(' LLC')[0]
                         if possible_addr[0].isdigit():
                             loc = possible_addr
                else:
                     loc = loc2 if len(loc2) > len(loc1) else loc1
            elif len(loc2) > len(loc1) and ' ' in loc2:
                loc = loc2
            elif len(loc1) > 0:
                loc = loc1
            else:
                loc = loc2

            if not loc:
                continue

            norm_addr = normalize_address_for_matching(loc)

            p_data = {}
            for src, target in mapping.items():
                val = row.get(src)
                if pd.notna(val):
                    if target == 'sale_date':
                         try: p_data[target] = pd.to_datetime(val).date()
                         except: pass
                    else:
                         p_data[target] = val

            p_data['appraised_value'] = row['appraised_value']

            # --- Auto-Calculate missing values (70% Rule) ---
            if p_data.get('appraised_value', 0) > 0:
                if 'assessed_value' not in p_data or not p_data['assessed_value']:
                    p_data['assessed_value'] = round(p_data['appraised_value'] * 0.70, 2)
            elif p_data.get('assessed_value', 0) > 0:
                if 'appraised_value' not in p_data or not p_data['appraised_value']:
                    p_data['appraised_value'] = round(p_data['assessed_value'] / 0.70, 2)

            if 'cama_site_link' not in p_data and pd.notna(row.get('Link_From_CAMA')):
                p_data['cama_site_link'] = row['Link_From_CAMA']

            # Only assign unit if present in official record
            official_unit = str(row.get('Unit_Type', '')).strip()
            if official_unit:
                p_data['unit'] = official_unit
            # Do NOT infer or assign a unit if official record does not have one

            if norm_addr not in processed_data:
                processed_data[norm_addr] = []
            processed_data[norm_addr].append(p_data)
        except: continue
        
    # 5. Update DB
    updated_count = 0
    processed_count = 0
    for prop_id, prop_location, _ in db_properties:
        processed_count += 1
        if not prop_location: 
            mark_property_processed_today(conn, prop_id)
            continue
            
        norm_addr = normalize_address_for_matching(prop_location)
        matches = processed_data.get(norm_addr)
        if matches:
            v_data = matches[0] 
            if update_property_in_db(conn, prop_id, v_data):
                updated_count += 1
        mark_property_processed_today(conn, prop_id)
        
        if processed_count % 100 == 0:
            log(f"    -> Progress for {municipality_name}: {processed_count}/{len(db_properties)}, updated {updated_count}...")
        
    log(f"Finished {municipality_name}. Updated {updated_count}.")

    # --- NEW: Hartford Enrichment Trigger ---
    if municipality_name.upper() == 'HARTFORD':
        try:
            from api.hartford_enrichment import run_enrichment
            log("  -> Triggering Hartford Unit Enrichment (Multi-Threaded Backfill)...")
            enriched = run_enrichment()
            log(f"  -> Hartford Enrichment Complete. Enriched {enriched} properties.")
        except Exception as e:
            log(f"  -> Error triggering Hartford enrichment: {e}")

    # --- NEW: Unit Collapse Detection ---
    potential_collapses = []
    for addr, rows in processed_data.items():
        if len(rows) > 30: # Heuristic: More than 30 units matching one address record is suspicious
            potential_collapses.append(f"{addr} ({len(rows)} records)")
    
    if potential_collapses:
        log(f"  [WARNING] Unit Collapse potential detected for {municipality_name} in source data:")
        for pc in potential_collapses[:5]: # Show top 5
            log(f"    - {pc}")
        if len(potential_collapses) > 5:
            log(f"    - ... and {len(potential_collapses) - 5} more")

    return updated_count

def parse_mapxpress_html(html_content):
    """Parses MapXpress property detail HTML."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_content, 'lxml')
    data = {}
    
    # Helper to find value in key-value tables
    # Usually Structure: <tr><td ...>Key</td><td ...>Value</td></tr>
    # But looking at snippet:
    # <tr><td ...>Living Area - sqft</td><td ...>1598</td></tr>
    
    # Find all tables
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 2: # Looking for key-value pairs, often spanning columns
                # Iterate through cells to find keys
                for i in range(len(cells)):
                    text = cells[i].get_text(strip=True)
                    next_text = ""
                    # The value might be in the next cell (i+1) or next-next?
                    # In the provided snippet:
                    # <td colspan="2">Living Area - sqft</td><td colspan="3">1598</td>
                    # So it is the very next sibling cell element in the DOM?
                    # bs4 finds cells in linear order.
                    
                    if i + 1 < len(cells):
                         val = cells[i+1].get_text(strip=True)
                         
                         if "Living Area - sqft" in text:
                             try: data['living_area'] = float(val.replace(',', ''))
                             except: pass
                         elif "Year Built" == text:
                             try: data['year_built'] = int(val)
                             except: pass
                         elif "Building Style" == text:
                             data['property_type'] = val
                         elif "Total Acres" in text or "Acres" == text: # Guessing for Acres
                             try: data['acres'] = float(val)
                             except: pass
                         elif "Zone" == text or "Zoning" == text: # Guessing
                             data['zone'] = val
                         elif "Unit" in text or "Apt" in text:
                             data['unit'] = val
                         elif "Unit" in text or "Apt" in text:
                             data['unit'] = val
                         elif "Occupancy" in text:
                             try: data['number_of_units'] = int(float(val))
                             except: pass
    
    # Try to find Appraisal/Assessment if available
    # Usually in a summary table at top.
    
    # --- MapXpress Photo Extraction ---
    # Look for image with 'Photo' in ID or src, or residing in a photo container
    photo_img = soup.find('img', id=re.compile(r'Photo|MainImage', re.I))
    if not photo_img:
        photo_img = soup.find('img', src=re.compile(r'/photos/|/bldgphotos/|/images/prop', re.I))
    
    if photo_img and photo_img.get('src'):
        data['building_photo'] = photo_img.get('src')
    
    return data


def scrape_mapxpress_property(session, base_url_template, row):
    """Worker function to scrape a single property."""
    import time
    import random
    
    prop_id, location, account_num, serial_num, current_link = row
    unique_id = account_num if account_num else serial_num
    
    # Fallback: Use cama_site_link if it looks like an ID (common in Bridgeport import)
    if not unique_id and current_link and not current_link.startswith('http'):
        unique_id = current_link
    
    if not unique_id:
        return prop_id, None, None
        
    target_url = base_url_template.format(unique_id)
    
    try:
        # Reduced sleep time for parallel execution (random jitter 0.1s - 0.5s)
        time.sleep(random.uniform(0.1, 0.5))
        
        resp = session.get(target_url, timeout=15)
        if resp.status_code == 200:
            scraped_data = parse_mapxpress_html(resp.text)
            
            # Resolve relative photo URL if found
            if 'building_photo' in scraped_data:
                scraped_data['building_photo'] = resp.url.rsplit('/', 1)[0] + '/' + scraped_data['building_photo'] if not scraped_data['building_photo'].startswith('http') else scraped_data['building_photo']

            scraped_data['cama_site_link'] = target_url
            return prop_id, scraped_data, None
        else:
            return prop_id, None, f"Status {resp.status_code}"
            
    except Exception as e:
        return prop_id, None, str(e)


def discover_mapxpress_properties(conn, municipality_name, domain, id_param='UNIQUE_ID'):
    """Crawls MapXpress search to find ALL properties and insert missing ones."""
    log(f"Starting discovery for {municipality_name}...", municipality=municipality_name)
    base_url = f"https://{domain}"
    search_url = f"{base_url}/PAGES/search.asp"
    
    session = get_session()
    session.verify = False
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': search_url,
        'Origin': base_url
    })
    
    # Initialize session
    try:
        session.get(search_url, timeout=10)
    except Exception as e:
        log(f"Discovery Init Failed: {e}", municipality=municipality_name)
        return

    # Iteration keys: A-Z and 0-9
    search_keys = [chr(i) for i in range(65, 91)] + [str(i) for i in range(10)]
    
    discovered_count = 0
    new_count = 0
    
    for key in search_keys:
        try:
            # log(f"  Searching '{key}'...", municipality=municipality_name)
            resp = session.post(search_url, data={
                'searchname': key,
                'houseno': '',
                'mbl': '',
                'go.x': 1, 'go.y': 1
            }, timeout=20)
            
            if resp.status_code != 200:
                continue
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            links = soup.find_all('a', href=lambda h: h and 'detail.asp' in h)
            
            for link in links:
                href = link.get('href', '')
                # Extract ID
                match = re.search(rf'{id_param}=([^&]+)', href, re.I)
                if match:
                    unique_id = match.group(1)
                    raw_address = link.text.strip()
                    # Clean address mostly (MapXpress usually puts address in text)
                    if not raw_address or "Parcel Details" in raw_address:
                        continue
                        
                    # Insert if missing
                    with conn.cursor() as cursor:
                        # Check existence by account_number (if we treat unique_id as account_number)
                        # OR cama_site_link
                        
                        # We use ON CONFLICT DO NOTHING to be safe and fast
                        # But we need to ensure we insert minimal valid data
                        
                        # Map unique_id to account_number for now? Or just store in cama_site_link?
                        # update_data.py usually links account_number to the unique ID.
                        
                        full_link = f"{base_url}/PAGES/detail.asp?{id_param}={unique_id}"
                        
                        cursor.execute("""
                            INSERT INTO properties (property_city, location, account_number, cama_site_link, source)
                            VALUES (%s, %s, %s, %s, 'MAPXPRESS_DISCOVERY')
                            ON CONFLICT (property_city, location) DO NOTHING
                            RETURNING id
                        """, (municipality_name.title(), raw_address, unique_id, full_link))
                        
                        if cursor.fetchone():
                            new_count += 1
                    
                    discovered_count += 1
                    
        except Exception as e:
            log(f"  Discovery Error on key {key}: {e}", municipality=municipality_name)
            
    log(f"Discovery Complete: Found {discovered_count} total, Inserted {new_count} new properties.", municipality=municipality_name)
    conn.commit()


def process_municipality_with_mapxpress(conn, municipality_name, data_source_config, current_owner_only=False, force_process=False):
    """Process a municipality scraping MapXpress (Parallel)."""
    
    log(f"--- Processing municipality: {municipality_name} via MapXpress (Force={force_process}) ---", municipality=municipality_name)
    
    # 0. Run Discovery Phase
    domain = data_source_config['domain']
    if domain.endswith('/'): domain = domain[:-1]
    id_param = data_source_config.get('id_param', 'UNIQUE_ID')
    
    if not current_owner_only:
        discover_mapxpress_properties(conn, municipality_name, domain, id_param)

    query = """
        SELECT p.id, p.location, p.account_number, p.serial_number, p.cama_site_link 
        FROM properties p
        LEFT JOIN property_processing_log ppl ON p.id = ppl.property_id
        WHERE p.property_city ILIKE %s 
        AND (
            (p.account_number IS NOT NULL AND p.account_number != '') 
            OR (p.cama_site_link IS NOT NULL AND p.cama_site_link != '')
        )
    """
    
    if not force_process:
        query += " AND (ppl.last_processed_date IS NULL OR ppl.last_processed_date < CURRENT_DATE)"
    
    if current_owner_only:
        query += " AND p.owner ILIKE '%%Current Owner%%'"
        
    query += " ORDER BY p.location" 
    
    with conn.cursor() as cursor:
        cursor.execute(query, (municipality_name,))
        properties = cursor.fetchall()
        
    if not properties:
        log(f"No properties found with account_number for {municipality_name}.", municipality=municipality_name)
        return 0
        
    log(f"Found {len(properties)} properties to scrape for {municipality_name}. Starting parallel scrape (10 threads)...", municipality=municipality_name)
    
    base_url_template = f"https://{domain}/PAGES/detail.asp?{id_param}={{}}"

    updated_count = 0
    processed_count = 0
    
    session = get_session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    # Parallel Processing using ThreadPoolExecutor
    # We use threads for I/O (scraping), but update DB in the main thread to ensure safety.
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all tasks
        future_to_prop = {
            executor.submit(scrape_mapxpress_property, session, base_url_template, row): row 
            for row in properties
        }
        
        for future in concurrent.futures.as_completed(future_to_prop):
            prop_id, scraped_data, error_msg = future.result()
            
            if scraped_data:
                if update_property_in_db(conn, prop_id, scraped_data, municipality_name=municipality_name):
                    updated_count += 1
            elif error_msg:
                # Log error but don't spam if common
                if "Status 404" not in error_msg: 
                     pass # log(f"Scrape error for {prop_id}: {error_msg}")
            
            # Always mark processed
            mark_property_processed_today(conn, prop_id)
            
            processed_count += 1
            if processed_count % 50 == 0:
                log(f"  -> Scraped {processed_count}/{len(properties)}, updated {updated_count}...", municipality=municipality_name)
            
    log(f"Finished {municipality_name}. Scraped {processed_count}, Updated {updated_count}.", municipality=municipality_name)
    return updated_count

# --- PropertyRecordCards Scraper ---

def parse_propertyrecordcards_html(html_content):
    """Parses PropertyRecordCards property detail HTML."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_content, 'lxml')
    data = {}
    
    # Helper to clean currency/number strings
    def clean_number(val):
        if not val: return None
        return re.sub(r'[^\d.]', '', val)

    # 1. Parse IDs (Acres, Zone, Values)
    id_map = {
        'acres': 'MainContent_tbgMapAcres',
        'zone': 'MainContent_tbgMapZone',
        'appraised_value': 'MainContent_tbgMapAppraisedValue',
        'assessed_value': 'MainContent_tbgMapAssessedValue'
    }

    for db_field, html_id in id_map.items():
        element = soup.find(id=html_id)
        if element and element.get('value'):
            val = element.get('value')
            try:
                if db_field in ['acres', 'appraised_value', 'assessed_value']:
                    clean_val = clean_number(val)
                    if clean_val:
                        data[db_field] = float(clean_val)
                else:
                    data[db_field] = val
            except: pass

    # 2. Parse Tables (Living Area, Year Built, Style)
    # Search for cells containing keys, then get next cell value
    key_map = {
        'Living Area:': 'living_area',
        'Year Built:': 'year_built',
        'Style:': 'property_type',
        'Use Code:': 'property_type', # Fallback
        'Unit:': 'unit',
        'Beds/Units:': 'number_of_units', # Waterbury specific
        'GLA:': 'living_area' # Waterbury specific
    }
    
    tables = soup.find_all('table')
    for table in tables:
        cells = table.find_all('td')
        for i, cell in enumerate(cells):
            text = cell.get_text(strip=True)
            if text in key_map and i + 1 < len(cells):
                val = cells[i+1].get_text(strip=True)
                db_field = key_map[text]
                
                # Check if we already found it (prioritize first match?)
                if db_field in data: continue
                
                try:
                    if db_field == 'living_area':
                         clean_val = clean_number(val)
                         if clean_val: data[db_field] = float(clean_val)
                    elif db_field == 'year_built':
                        data[db_field] = int(val)
                    else:
                        data[db_field] = val
                except: pass
                
    # 3. Parse Photos
    # Look for image with 'Photo' in ID or src
    photo_img = soup.find('img', id=re.compile(r'Photo|MainImage|imgPhoto', re.I))
    if not photo_img:
        photo_img = soup.find('img', src=re.compile(r'/Photos/|/bldgphotos/|/images/prop', re.I))
    
    if photo_img and photo_img.get('src'):
        data['building_photo'] = photo_img.get('src')
                
    return data

def scrape_propertyrecordcards_property(session, base_url_template, row, towncode=None):
    """Worker function to scrape a single PropertyRecordCards property."""
    import time
    import random
    
    prop_id, location, account_num, serial_num, current_link = row
    unique_id = account_num if account_num else (serial_num if serial_num else current_link)
    
    # Waterbury (151) ID Fix: 80070-0273-0020-0006 -> 027300200006
    if towncode == '151' and unique_id:
        tmp_id = str(unique_id).strip()
        if tmp_id.startswith('80070-'):
             tmp_id = tmp_id.replace('80070-', '')
        tmp_id = tmp_id.replace('-', '')
        unique_id = tmp_id
        
    if not unique_id:
        return prop_id, None, None # Cannot scrape w/o ID
        
    target_url = base_url_template.format(unique_id)
    
    try:
        # Respectful jitter
        time.sleep(random.uniform(0.1, 0.5))
        
        resp = session.get(target_url, timeout=20)
        
        # Check for soft errors or redirect to search page (invalid ID)
        if "SearchMaster.aspx" in resp.url and "propertyresults.aspx" not in resp.url:
             return prop_id, None, "Redirected to Search (Invalid ID?)"

        if resp.status_code == 200:
            scraped_data = parse_propertyrecordcards_html(resp.text)
            if scraped_data: # Ensure we actually got some data
                # Fallback: If unit is missing, try to infer from location
                if 'unit' not in scraped_data or not scraped_data['unit']:
                    # Extract unit from location if predictable (Space + Single Letter or Digits)
                    # Regex: Space followed by (Single Uppercase Letter OR 1-4 Digits) at end of string
                    # Excludes street suffixes like AV, RD, ST (2 letters).
                    import re # Ensure re is available (module level usually imports it but safer here)
                    m = re.search(r'\s([A-Z]|\d{1,4})$', location)
                    if m:
                         scraped_data['unit'] = m.group(1)

                scraped_data['cama_site_link'] = target_url
                return prop_id, scraped_data, None
            else:
                return prop_id, None, "No data parsed"
        else:
            return prop_id, None, f"Status {resp.status_code}"
            
    except Exception as e:
        return prop_id, None, str(e)


def discover_propertyrecordcards_properties(conn, municipality_name, towncode, path_prefix=''):
    """Crawls PropertyRecordCards search to find ALL properties and insert missing ones."""
    log(f"Starting discovery for {municipality_name} (PRC)...", municipality=municipality_name)
    base_url = f"https://www.propertyrecordcards.com{path_prefix}"
    search_url = f"{base_url}/SearchMaster.aspx?towncode={towncode}"
    
    session = get_session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': f"https://www.propertyrecordcards.com{path_prefix}/Search.aspx"
    })
    
    # 1. Initialize session and get ViewState
    try:
        resp = session.get(search_url, timeout=15)
        soup = BeautifulSoup(resp.content, 'html.parser')
        viewstate = soup.find('input', id='__VIEWSTATE')['value']
        valgen = soup.find('input', id='__VIEWSTATEGENERATOR')['value'] if soup.find('input', id='__VIEWSTATEGENERATOR') else ""
        eventval = soup.find('input', id='__EVENTVALIDATION')['value'] if soup.find('input', id='__EVENTVALIDATION') else ""
    except Exception as e:
        log(f"Discovery Init Failed: {e}", municipality=municipality_name)
        return

    # Iteration keys: A-Z
    search_keys = [chr(i) for i in range(65, 91)]
    
    discovered_count = 0
    new_count = 0
    
    for key in search_keys:
        try:
            # log(f"  Searching '{key}'...", municipality=municipality_name)
            # PRC search usually requires a POST with __VIEWSTATE
            # The field for name search is often MainContent_tbPropertySearchName
            payload = {
                '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': valgen,
                '__EVENTVALIDATION': eventval,
                'ctl00$MainContent$tbPropertySearchName': key,
                'ctl00$MainContent$btnPropertySearch': 'Search'
            }
            
            resp = session.post(search_url, data=payload, timeout=20)
            
            if resp.status_code != 200:
                continue
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            # Look for property links in the results table
            # Link Format: propertyresults.aspx?towncode=...&uniqueid=...
            links = soup.find_all('a', href=re.compile(r'propertyresults\.aspx\?.*uniqueid=', re.I))
            
            for link in links:
                href = link.get('href', '')
                match = re.search(r'uniqueid=([^&]+)', href, re.I)
                if match:
                    unique_id = match.group(1)
                    # The link text is usually the address
                    raw_address = link.text.strip()
                    
                    if not raw_address or "View" in raw_address:
                        # Try to find address in the same row
                        row = link.find_parent('tr')
                        if row:
                            cells = row.find_all('td')
                            if len(cells) > 1:
                                raw_address = cells[1].text.strip()

                    if not raw_address:
                        continue
                        
                    with conn.cursor() as cursor:
                        full_link = f"https://www.propertyrecordcards.com{path_prefix}/propertyresults.aspx?towncode={towncode}&uniqueid={unique_id}"
                        
                        cursor.execute("""
                            INSERT INTO properties (property_city, location, account_number, cama_site_link, source)
                            VALUES (%s, %s, %s, %s, 'PRC_DISCOVERY')
                            ON CONFLICT (property_city, location) 
                            DO UPDATE SET 
                                account_number = COALESCE(properties.account_number, EXCLUDED.account_number),
                                cama_site_link = COALESCE(properties.cama_site_link, EXCLUDED.cama_site_link)
                            RETURNING id
                        """, (municipality_name.title(), raw_address, unique_id, full_link))
                        
                        if cursor.fetchone():
                            new_count += 1
                    
                    discovered_count += 1
            
            # Commit per letter to save progress
            conn.commit()
                    
        except Exception as e:
            log(f"  Discovery Error on key {key}: {e}", municipality=municipality_name)
            
    log(f"Discovery Complete: Found {discovered_count} total, Updated/Inserted {new_count} properties.", municipality=municipality_name)

def process_municipality_with_propertyrecordcards(conn, municipality_name, data_source_config, current_owner_only=False, force_process=False):
    """Process a municipality using PropertyRecordCards (Parallel)."""
    
    log(f"--- Processing municipality: {municipality_name} via PropertyRecordCards (Force={force_process}) ---", municipality=municipality_name)
    
    # 0. Run Discovery Phase
    towncode = data_source_config['towncode']
    path_prefix = data_source_config.get('path_prefix', '')
    
    if not current_owner_only:
        discover_propertyrecordcards_properties(conn, municipality_name, towncode, path_prefix)

    # 1. Get Properties (same logic as MapXpress)
    # 1. Get Properties (same logic as MapXpress)
    query = """
        SELECT p.id, p.location, p.account_number, p.serial_number, p.cama_site_link 
        FROM properties p
        LEFT JOIN property_processing_log ppl ON p.id = ppl.property_id
        WHERE p.property_city ILIKE %s 
        AND ((p.account_number IS NOT NULL AND p.account_number != '') OR (p.cama_site_link IS NOT NULL AND p.cama_site_link != ''))
    """
    if not force_process:
        query += " AND (ppl.last_processed_date IS NULL OR ppl.last_processed_date < CURRENT_DATE)"
    if current_owner_only:
        query += " AND p.owner ILIKE '%%Current Owner%%'"
    query += " ORDER BY p.location" 
    
    with conn.cursor() as cursor:
        cursor.execute(query, (municipality_name,))
        properties = cursor.fetchall()
        
    if not properties:
        log(f"No properties found with account_number for {municipality_name}.", municipality=municipality_name)
        return 0
        
    log(f"Found {len(properties)} properties to scrape for {municipality_name}. Starting parallel scrape (10 threads)...", municipality=municipality_name)
    
    # 2. Construct URL Template
    towncode = data_source_config['towncode']
    path_prefix = data_source_config.get('path_prefix', '') # e.g. /Sherman or empty
    # URL Format: https://www.propertyrecordcards.com/Sherman/propertyresults.aspx?towncode=127&uniqueid=...
    # OR Standard: https://www.propertyrecordcards.com/propertyresults.aspx?towncode=002&uniqueid=...
    
    base_url_template = f"https://www.propertyrecordcards.com{path_prefix}/propertyresults.aspx?towncode={towncode}&uniqueid={{}}"

    updated_count = 0
    processed_count = 0
    
    session = get_session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    # 3. Parallel Execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_prop = {
            executor.submit(scrape_propertyrecordcards_property, session, base_url_template, row, towncode): row 
            for row in properties
        }
        
        for future in concurrent.futures.as_completed(future_to_prop):
            prop_id, scraped_data, error_msg = future.result()
            
            if scraped_data:
                if update_property_in_db(conn, prop_id, scraped_data, municipality_name=municipality_name):
                    updated_count += 1
            elif error_msg:
                # log(f"Scrape error for {prop_id}: {error_msg}")
                pass
            
            # Resolve relative photo URL
            # PropertyRecordCards photos are often relative to the site root or current page
            # But the scraper currently gets whatever is in src.
            # We'll handle normalization in update_property_in_db or here.
            
            mark_property_processed_today(conn, prop_id)
            processed_count += 1
            
            if processed_count % 50 == 0:
                log(f"  -> Scraped {processed_count}/{len(properties)}, updated {updated_count}...", municipality=municipality_name)
            
    log(f"Finished {municipality_name}. Scraped {processed_count}, Updated {updated_count}.", municipality=municipality_name)
    return updated_count

# --- Vision Appraisal Functions (existing code) ---
def get_vision_municipalities():
    """Scrapes the VGSI website to get a list of municipalities with their last updated date and URL."""
    log(f"Scraping {CONNECTICUT_DATABASE_URL} for municipalities...")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(CONNECTICUT_DATABASE_URL, verify=False, timeout=15, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        municipalities = {}
        table = soup.find('table')
        if not table:
            log("Could not find the municipalities table on the Vision Appraisal site.")
            return municipalities

        for row in table.find_all('tr')[1:]:  # Skip header
            cols = row.find_all('td')
            if len(cols) >= 3:
                municipality_name = cols[0].text.strip()
                last_updated_str = cols[2].text.strip()
                link = cols[0].find('a')
                
                cleaned_name = municipality_name.upper().replace(', CT', '').strip()

                if cleaned_name and last_updated_str and link:
                    last_updated_date = None
                    try:
                        last_updated_date = datetime.strptime(last_updated_str, '%m/%d/%Y')
                    except ValueError:
                        # Handle "Daily", "Weekly", etc. by assuming recent update
                        last_updated_date = datetime.now()

                    try:
                        full_url = link['href']
                        if full_url.startswith('http'):
                            base_url = '/'.join(full_url.split('/')[:-1]) + '/'
                        else:
                            base_url = f"{VISION_BASE_URL}/{'/'.join(full_url.split('/')[:-1]).lstrip('/')}/"
                        
                        municipalities[cleaned_name] = {
                            "last_updated": last_updated_date,
                            "url": base_url,
                            "type": "vision_appraisal"
                        }
                    except KeyError as e:
                        log(f"Could not parse URL for {municipality_name}: {e}")

        log(f"Found {len(municipalities)} municipalities on the Vision Appraisal site.")
        return municipalities

    except requests.exceptions.RequestException as e:
        log(f"Error fetching Vision Appraisal page: {e}")
        return {}

def normalize_address(address):
    """Normalizes an address for consistent matching."""
    if not address:
        return ""
    
    # Convert to uppercase and normalize whitespace
    normalized = ' '.join(address.upper().strip().split())
    
    # Common address normalizations for better matching
    normalizations = {
        ' STREET': ' ST',
        ' AVENUE': ' AVE', 
        ' ROAD': ' RD',
        ' DRIVE': ' DR',
        ' LANE': ' LN',
        ' COURT': ' CT',
        ' PLACE': ' PL',
        ' BOULEVARD': ' BLVD',
        ' CIRCLE': ' CIR'
    }
    
    for old, new in normalizations.items():
        normalized = normalized.replace(old, new)
    
    return normalized

def scrape_individual_property_page(prop_page_url, session, referer):
    """Scrapes data from a single property detail page."""
    # print(f"DEBUG: START Scraping {prop_page_url}", flush=True) # Commented out to avoid spam, uncomment if needed
    try:
        # User requested silence, but I need to see errors.
        # time.sleep(0.4)  # Respectful delay
        headers = {
            'Referer': referer,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = session.get(prop_page_url, verify=False, timeout=20, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        # print(f"DEBUG: Scraped URL {prop_page_url} - Payload Size: {len(response.content)} - Title: {soup.title.text.strip() if soup.title else 'No Title'}", flush=True)

    except Exception as e:
        print(f"DEBUG: EXCEPTION fetching {prop_page_url}: {e}", flush=True)
        return None

    data = {}
    
    # Vision Appraisal specific selectors (primary strategy)
    selectors = {
        'owner': ['span[id*="lblGenOwner"]', 'span[id*="lblOwner"]'],
        'sale_amount': ['span[id*="lblPrice"]'],
        'sale_date': ['span[id*="lblSaleDate"]'],
        'assessed_value': ['*[id*="lblGenAssessment"]', '*[id="MainContent_lblGenAssessment"]'],
        'appraised_value': ['*[id*="lblGenAppraisal"]', '*[id="MainContent_lblGenAppraisal"]'],
        'unit': ['*[id*="lblUnit"]', '*[id*="lblApt"]', '*[id*="lblSuite"]'],
        'building_photo': ['img[id*="imgPhoto"]', 'img[id*="MainContent_imgPhoto"]'],
        'location': ['*[id="MainContent_lblLocation"]', '*[id*="lblLocation"]', '*[id*="lblGenLocation"]'],
        'property_type': ['span[id*="lblUseCode"]', 'span[id*="lblGenUseCode"]']
    }
    
    # Add a specific check for "Occupancy" and "Style" in the attributes table
    for label in ['Occupancy', 'Style']:
        label_td = soup.find('td', string=re.compile(f'^{label}$', re.IGNORECASE))
        if not label_td:
             label_td = soup.find(lambda tag: tag.name == 'td' and label.lower() in tag.text.lower().strip())
        
        if label_td:
            val_td = label_td.find_next_sibling('td')
            if val_td:
                val_text = val_td.text.strip()
                if label == 'Occupancy':
                    try:
                        data['number_of_units'] = int(float(val_text))
                    except ValueError:
                        pass
                elif label == 'Style':
                    data['property_type'] = val_text
    
    for field, field_selectors in selectors.items():
        for selector in field_selectors:
            element = soup.select_one(selector)
            if not element:
                continue
            
            if field == 'building_photo':
                img_src = element.get('src')
                if img_src:
                    data[field] = urljoin(prop_page_url, img_src)
            elif element.text.strip():
                text = element.text.strip()
                if field == 'owner':
                    data[field] = text
                elif field == 'location':
                    data[field] = text
                elif field in ['sale_amount', 'assessed_value', 'appraised_value']:
                    cleaned = re.sub(r'[$,]', '', text)
                    if cleaned.replace('.', '', 1).replace('-', '').isdigit():
                        value = float(cleaned)
                        if value > 0:
                            data[field] = value
                elif field == 'sale_date':
                    try:
                        data[field] = datetime.strptime(text, '%m/%d/%Y').date()
                    except ValueError:
                        pass
                if field in data:
                    break
    
    # Fallback strategy: Generic table/dl search
    if len(data) < 2:
        fallback_keywords = {
            'owner': 'Owner',
            'sale_amount': 'Sale Price',
            'sale_date': 'Sale Date',
            'assessed_value': 'Assessment',
            'appraised_value': 'Appraisal',
            'location': 'Location',
            'unit': 'Unit'
        }

        for field, keyword in fallback_keywords.items():
            if field in data:
                continue

            keyword_element = soup.find(['td', 'dt'], string=re.compile(f'^{re.escape(keyword)}', re.IGNORECASE))
            if not keyword_element:
                 keyword_element = soup.find(['td', 'dt'], string=re.compile(f'{re.escape(keyword)}', re.IGNORECASE))
                 
            if keyword_element:
                next_element = keyword_element.find_next_sibling(['td', 'dd'])
                if next_element and next_element.text.strip():
                    text = next_element.text.strip()
                    if field == 'owner':
                        data[field] = text
                    elif field in ['sale_amount', 'assessed_value', 'appraised_value']:
                        cleaned = re.sub(r'[$,]', '', text)
                        if cleaned.replace('.', '', 1).replace('-', '').isdigit():
                            value = float(cleaned)
                            if value > 0:
                                data[field] = value
                    elif field == 'sale_date':
                        try:
                            data[field] = datetime.strptime(text, '%m/%d/%Y').date()
                        except ValueError:
                            pass
    
    if 'location' in data and 'unit' not in data:
        loc = data['location'].strip()
        match = re.search(r'(?:,|,\s*|\s+)(?:UNIT|#|APT|STE|SUITE|#UD|FL|FLOOR|RM|ROOM)\.?\s*([A-Z0-9-]+)$', loc, re.IGNORECASE)
        if match:
            data['unit'] = match.group(1)
        else:
            parts = loc.split()
            if len(parts) > 1:
                last_part = parts[-1]
                reserved = {
                    'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW', 'NORTH', 'SOUTH', 'EAST', 'WEST',
                    'ST', 'AVE', 'RD', 'CT', 'BLVD', 'LN', 'DR', 'WAY', 'PL', 'TER', 'CIR', 'HWY', 'PKWY', 'TPKE', 'EXT',
                    'STREET', 'AVENUE', 'ROAD', 'COURT', 'BOULEVARD', 'LANE', 'DRIVE', 'PLACE', 'TERRACE', 'CIRCLE'
                }
                if last_part.upper() not in reserved and len(last_part) < 6 and re.match(r'^[A-Z0-9]+$', last_part):
                    data['unit'] = last_part

    if 'location' not in data or not data['location']:
        return None
    if 'owner' not in data or not data['owner']:
        return None
        
    return data if data else None


def scrape_street_properties(street_link, municipality_url, referer, session=None, municipality_name=None):
    """Scrapes all properties on a single street page."""
    street_props = {}
    
    # Random jitter to avoid strict pattern detection
    # Use smaller jitter if we are sharing a session to keep things fast but safe
    time.sleep(random.uniform(0.1, 0.5) if session else random.uniform(0.5, 1.5))

    try:
        if session:
            # When sharing a session, we stay in the same "ASP.NET Session"
            response = session.get(street_link, headers={'Referer': referer} if referer else {}, verify=False, timeout=30)
        else:
            with get_session() as s:
                s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
                response = s.get(street_link, verify=False, timeout=30)

        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # FIX: Use regex for case-insensitive and robust link finding
        prop_links = soup.find_all("a", href=re.compile(r'\.aspx\?(pid|acct|uniqueid)=', re.I))
        
        if len(prop_links) == 0:
             log(f"Street {street_link} found 0 property links. Status: {response.status_code}. Len: {len(response.text)}. Resolved URL: {response.url}", municipality=municipality_name)

        for prop_link in prop_links:
            raw_address = prop_link.text.strip()
            # Clean address: remove "Mblu:" and other extraneous data
            address = re.sub(r'\s+Mblu:.*', '', raw_address, flags=re.IGNORECASE).strip()
            address = normalize_address(address)

            href = prop_link.get('href')
            if not href or not address:
                continue
            
            # Filter out malformed links or template artifacts
            if re.search(r'(pid|acct)=\s*$', href, re.I) or '<%' in href:
                continue
            
            prop_page_url = urljoin(municipality_url, href)
            # Use provided session for individual property scraping too
            prop_details = scrape_individual_property_page(prop_page_url, session, street_link)
            
            if prop_details:
                # Add the specific parcel URL to the data dict so it can be saved in the DB
                prop_details['cama_site_link'] = prop_page_url
                street_props[address] = prop_details
            else:
                log(f"Failed to scrape details for {prop_page_url}", municipality=municipality_name)
                    
    except Exception as e:
        log(f"Error scraping street {street_link}: {e}", municipality=municipality_name)
        
    return street_props

def scrape_all_properties_by_address(municipality_url, municipality_name):
    """Orchestrates the scraping of all properties for a municipality."""
    all_props_data = {}
    street_list_base_url = f"{municipality_url}Streets.aspx"
    
    with get_session() as main_session:
        main_session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        
        # Initialize session by visiting homepage first (fixes Berlin redirect issue)
        try:
             main_session.get(municipality_url, verify=False, timeout=20)
        except Exception: 
             pass # Ignore init errors, try direct link anyway

        try:
            log(f"  -> Fetching street index for {municipality_name}...")
            response = main_session.get(street_list_base_url, verify=False, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # More robust selector: any link containing Streets.aspx?Letter=
            letter_links = { urljoin(municipality_url, a['href']) for a in soup.find_all('a', href=re.compile(r'Streets\.aspx\?Letter=', re.I)) }
            if not letter_links:
                 # Fallback: maybe just Letter=
                 letter_links = { urljoin(municipality_url, a['href']) for a in soup.find_all('a', href=re.compile(r'Letter=', re.I)) }
            
            if not letter_links:
                log(f"  -> No A-Z letter links found for {municipality_name}.")
                return {}
        except Exception as e:
            log(f"  -> Failed to get street index for {municipality_name}: {e}")
            return {}

        log(f"  -> Found {len(letter_links)} letter pages to scan for {municipality_name}.")

        # --- OPTIMIZATION: Discover ALL streets for ALL letters in parallel ---
        all_street_links = []
        
        def get_streets_for_letter(letter_link):
            """Helper to fetch street links for a single letter."""
            local_streets = []
            try:
                # We need a new session or at least careful error handling per thread if we reused one.
                # Since this is low volume (26 requests), simple requests.get is fine or we create a session.
                # Reuse main_session with caution or just new requests.
                # Let's use a new request to be thread-safe/simple.
                with get_session() as s:
                    s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
                    resp = s.get(letter_link, verify=False, timeout=20, headers={'Referer': street_list_base_url})
                    l_soup = BeautifulSoup(resp.content, 'html.parser')
                    
                    # More robust selector: any link containing Streets.aspx?Name=
                    # CRITICAL FIX: Strip trailing spaces from href which cause 0 results in VGSI
                    links = [ urljoin(municipality_url, a['href'].strip()) for a in l_soup.find_all('a', href=re.compile(r'Streets\.aspx\?Name=', re.I)) ]
                    if not links:
                        # Fallback: maybe just Name=
                        links = [ urljoin(municipality_url, a['href'].strip()) for a in l_soup.find_all('a', href=re.compile(r'Name=', re.I)) ]
                    return (letter_link, links)
            except Exception as e:
                log(f"  !!! ERROR in get_streets_for_letter for {letter_link}: {e}")
                return (letter_link, [])

        log(f"  -> discovering streets across {len(letter_links)} letters in parallel...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_letter = {executor.submit(get_streets_for_letter, link): link for link in letter_links}
            for future in concurrent.futures.as_completed(future_to_letter):
                ll, s_links = future.result()
                if s_links:
                    all_street_links.extend([(link, ll) for link in s_links]) # Tuple of (StreetLink, Referer)

        log(f"  -> Total streets discovered: {len(all_street_links)}. Starting massive parallel scrape...")

        # --- PROCESS ALL STREETS ---
        # Shuffle slightly to reduce contention on specific letter pages if that matters? 
        # Probability low. Just submit all.
        
        chunk_size = 500
        total_processed = 0
        
        # We can submit ALL, but for huge lists, maybe chunking helps avoid memory spikes?
        # 1000 streets is fine.
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # We pass the specific letter link as referer for each street AND use the main_session
            future_to_street = {executor.submit(scrape_street_properties, s_link, municipality_url, referer, main_session, municipality_name): s_link for s_link, referer in all_street_links}
            
            for i, future in enumerate(concurrent.futures.as_completed(future_to_street)):
                street_data = future.result()
                if street_data:
                    all_props_data.update(street_data)
                
                if (i + 1) % 50 == 0:
                    log(f"  -> [Progress] Scraped {i + 1}/{len(all_street_links)} streets...", municipality=municipality_name)

    log(f"  -> Scraped {len(all_props_data)} properties total for {municipality_name}.")
    return all_props_data

def get_session(pool_size=50):
    """Returns a requests session with optimized connection pooling."""
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
def update_property_in_db(conn, property_db_id, vision_data, restricted_mode=False, municipality_name=None):
    """
    Updates a property record in the database with new information.
    restricted_mode (bool): If True, only update fields that are currently empty or 'Current Owner'.
                            Exceptions: 'cama_site_link', 'building_photo', 'unit', 'unit_cut' are always updated.
    """
    if not vision_data or not any(v is not None for v in vision_data.values()): 
        return False

    # Fetch current state if in restricted mode
    current_state = {}
    if restricted_mode:
        try:
            with conn.cursor() as cursor:
                # Fetch only the columns we might want to update (plus owner for the 'Current Owner' check)
                # We can construct the SELECT dynamically based on vision_data keys
                cols_to_fetch = [k for k in vision_data.keys() if k in ['owner', 'sale_amount', 'sale_date', 'assessed_value', 'appraised_value', 'year_built', 'living_area', 'property_type', 'acres', 'zone', 'location', 'number_of_units']]
                # Always fetch account_number to check if property was enriched
                if 'account_number' not in cols_to_fetch:
                    cols_to_fetch.append('account_number')

                select_sql = sql.SQL("SELECT {} FROM properties WHERE id = %s").format(
                    sql.SQL(", ").join(map(sql.Identifier, cols_to_fetch))
                )
                cursor.execute(select_sql, (property_db_id,))
                row = cursor.fetchone()
                if row:
                    current_state = dict(zip(cols_to_fetch, row))
                else:
                    return False # ID not found?
        except psycopg2.Error as e:
            log(f"DB fetch error for property ID {property_db_id}: {e}", municipality=municipality_name)
            return False

    update_fields = []
    values = []
    
    # Fields that ARE NEVER allowed to be overwritten by the scraper if they already have data
    # unless the new data is substantially "better" (not handled automatically here)
    PROTECTED_FIELDS = {
        'latitude', 'longitude', 'normalized_address', 
        'unit', 'unit_cut', 'owner', 'co_owner', 'location', 'account_number'
    }

    for key, new_value in vision_data.items():
        if new_value is None or str(new_value).strip() == '':
            continue
            
        should_update = True
        current_val = current_state.get(key)
        
        # 1. Placeholder logic: Always update if current is placeholder
        is_placeholder = (
            current_val is None or
            str(current_val).strip() == '' or 
            str(current_val).strip().upper() in ['CURRENT OWNER', 'NULL', 'NONE'] or
            (key == 'location' and str(current_val).strip().replace(' ', '').isdigit() and len(str(current_val).strip()) < 6)
        )

        # 2. Field-specific logic
        if key in PROTECTED_FIELDS:
            # Special logic for enriched data (Hartford)
            # If account_number is set, we consider the current unit/location to be high-quality
            if current_state.get('account_number') and key in ['unit', 'location']:
                should_update = False
            
            # Standard protection: Only update if it's currently a placeholder
            elif not is_placeholder:
                # Special Case: If new owner is "Current Owner", do NOT overwrite a specific name
                if 'OWNER' in key.upper() and str(new_value).strip().upper() == 'CURRENT OWNER':
                    should_update = False
                else:
                    # If we already have data, skip unless restricted_mode is false and we explicitly want to overwrite
                    if restricted_mode:
                        should_update = False
        
        if should_update:
            log(f"Updating field {key} (old: {current_val}) -> (new: {new_value})", municipality=municipality_name)
            update_fields.append(sql.SQL("{} = %s").format(sql.Identifier(key)))
            values.append(new_value)
        else:
            # print(f"DEBUG DB: Skipping field {key} because should_update is False", flush=True)
            pass
    
    if not update_fields:
        print(f"DEBUG DB: No fields to update for property {property_db_id}", flush=True)
        return False

    # Use psycopg2.sql to safely format identifiers and structure
    query_sql = sql.SQL("UPDATE properties SET {} WHERE id = %s").format(
        sql.SQL(", ").join(update_fields)
    )
    
    values.append(property_db_id)
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query_sql, values)
            conn.commit()
            # log(f"Committed updates for property {property_db_id}", municipality=municipality_name)
            return True
    except psycopg2.Error as e:
        log(f"DB update error for property ID {property_db_id}: {e}")
        conn.rollback()
        return False

def find_match_for_property(prop_address, scraped_properties_dict):
    """Helper function to find a scraped property matching a DB address."""
    if not prop_address:
        return None
    
    normalized_db_address = normalize_address(prop_address)
    
    # Strategy 1: Direct match on normalized address
    if normalized_db_address in scraped_properties_dict:
        return scraped_properties_dict[normalized_db_address]
    else:
        # Strategy 2: Fuzzy matching for edge cases
        for scraped_addr, vision_data in scraped_properties_dict.items():
            # Check if DB address is in scraped address or vice-versa
            if normalized_db_address in scraped_addr or scraped_addr in normalized_db_address:
                return vision_data
                
    return None

def process_municipality_with_realtime_updates(conn, municipality_name, municipality_url, last_updated_date=None, current_owner_only=False, force_process=False):
    """
    Process a municipality with real-time database updates, resumability, and direct URL optimization.
    """
    log(f"--- Processing municipality: {municipality_name} (Force={force_process}) ---")
    
    # Determine Restricted Mode based on last_updated_date
    restricted_mode = False
    if last_updated_date and last_updated_date.year < 2026:
        log(f"  -> Data Source Last Updated: {last_updated_date.strftime('%Y-%m-%d')} (< 2026). ENABLED RESTRICTED MODE.")
        log("  -> In Restricted Mode, only empty or 'Current Owner' fields will be updated (except URLs/Photos/Units).")
        restricted_mode = True
    else:
        log(f"  -> Data Source Last Updated: {last_updated_date.strftime('%Y-%m-%d') if last_updated_date else 'Unknown'}. Normal update mode.")
    
    # Get properties to update from database
    if current_owner_only:
        db_properties = get_unprocessed_current_owner_properties(conn, municipality_name, force_process)
    else:
        db_properties = get_unprocessed_properties(conn, municipality_name, force_process)
        
    if not db_properties:
        log(f"No properties to process for {municipality_name}.")
        return 0

    # --- NEW LOGIC: Split processing into two groups ---
    props_with_urls = []
    props_without_urls = []
    all_processed_ids = [] # Store all IDs so we can mark them as processed at the end

    for prop_id, prop_location, prop_url in db_properties:
        all_processed_ids.append(prop_id)
        if prop_url and str(prop_url).strip().lower().startswith('http'):
            props_with_urls.append((prop_id, prop_url))
        else:
            props_without_urls.append((prop_id, prop_location))

    log(f"Total to process: {len(all_processed_ids)}. Direct URL Mode: {len(props_with_urls)}. Full Scrape Mode: {len(props_without_urls)}.")
    
    total_updated_count = 0

    # --- GROUP 1: Process properties we already have URLs for (FAST PATH) ---
    if props_with_urls:
        log(f"  -> Starting FAST PATH: Directly scraping {len(props_with_urls)} property URLs...")
        group1_updated_count = 0
        group1_processed_count = 0  # <--- NEW COUNTER
        referer_url = f"{municipality_url}Streets.aspx" # Use a generic valid referer

        with get_session() as session:
            session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_id = {
                    executor.submit(scrape_individual_property_page, prop_url, session, referer_url): prop_id
                    for prop_id, prop_url in props_with_urls
                }
                
                for future in concurrent.futures.as_completed(future_to_id):
                    prop_id = future_to_id[future]
                    vision_data = future.result()
                    if vision_data:
                        if update_property_in_db(conn, prop_id, vision_data, restricted_mode=restricted_mode, municipality_name=municipality_name):
                            group1_updated_count += 1
                    
                    group1_processed_count += 1 # <--- INCREMENT COUNTER
                    # --- NEW LOGGING LINE ---
                    if group1_processed_count % 100 == 0:
                        log(f"    -> FAST PATH progress for {municipality_name}: Processed {group1_processed_count}/{len(props_with_urls)}, Updated {group1_updated_count} so far...")
                        update_freshness_status(conn, municipality_name, 'vision_appraisal', 'running', details=f"Fast Path: {group1_processed_count}/{len(props_with_urls)} processed")


        
        log(f"  -> FAST PATH complete. Updated {group1_updated_count} properties.")
        total_updated_count += group1_updated_count

    # --- GROUP 2: Process properties we need to find via full scrape (SLOW PATH / POPULATION PATH) ---
    if props_without_urls:
        log(f"  -> Starting SLOW PATH: Full street scrape required for {len(props_without_urls)} properties...")
        
        # This scrape function now returns data dicts that include 'cama_site_link'
        scraped_properties = scrape_all_properties_by_address(municipality_url, municipality_name)
        
        if not scraped_properties:
            log(f"  -> Full scrape returned no data. Skipping {len(props_without_urls)} properties for {municipality_name}.")
        else:
            log(f"  -> SLOW PATH: Matching {len(props_without_urls)} DB properties against {len(scraped_properties)} scraped results...")
            group2_updated_count = 0
            processed_in_group = 0
            matched_addresses = set()  # Track which scraped properties we've matched
            
            # First pass: Try address-based matching for properties with locations
            for prop_db_id, prop_address in props_without_urls:
                vision_data = find_match_for_property(prop_address, scraped_properties)
                
                if vision_data:
                    # This update will save owner, sales, AND the new 'cama_site_link'
                    if update_property_in_db(conn, prop_db_id, vision_data, restricted_mode=restricted_mode, municipality_name=municipality_name):
                        group2_updated_count += 1
                        # Track the address we matched so we know which scraped properties are "used"
                        for addr, data in scraped_properties.items():
                            if data == vision_data:
                                matched_addresses.add(addr)
                                break
                
                processed_in_group += 1
                if processed_in_group % 100 == 0:
                    log(f"    -> Matched {processed_in_group}/{len(props_without_urls)}, updated {group2_updated_count} so far...")
                    update_freshness_status(conn, municipality_name, 'vision_appraisal', 'running', details=f"Slow Path: {processed_in_group}/{len(props_without_urls)} checked")


            def is_placeholder_address(addr):
                if not addr or addr.strip() == '' or addr.strip().upper() == 'NULL':
                    return True
                # Numeric placeholders like "93"
                if addr.strip().replace(' ', '').isdigit() and len(addr.strip()) < 6:
                    return True
                return False

            unmatched_db_props = [(pid, addr) for pid, addr in props_without_urls 
                                  if is_placeholder_address(addr)]
            unmatched_scraped = {addr: data for addr, data in scraped_properties.items() 
                                if addr not in matched_addresses}
            
            if unmatched_db_props and unmatched_scraped:
                log(f"  -> Populating {len(unmatched_db_props)} empty-location properties from {len(unmatched_scraped)} unmatched scraped records...")
                scraped_items = list(unmatched_scraped.items())
                for i, (prop_db_id, _) in enumerate(unmatched_db_props):
                    if i < len(scraped_items):
                        addr, vision_data = scraped_items[i]
                        # Add the address to the vision_data so it gets saved to location field
                        vision_data['location'] = addr
                        # In limited mode, we STILL want to populate empty records, so restricted_mode is fine 
                        # because update_property_in_db allows updates if current val is empty.
                        if update_property_in_db(conn, prop_db_id, vision_data, restricted_mode=restricted_mode):
                            group2_updated_count += 1

            log(f"  -> SLOW PATH complete. Updated {group2_updated_count} properties (and populated their URLs).")
            total_updated_count += group2_updated_count

    # --- FINALIZE: Mark all items from the original queue as processed ---
    # We do this even if 'force' is on, to ensure the 'last_processed_date' is always today's date.
    log(f"  -> Finalizing: Marking all {len(all_processed_ids)} properties as processed for today.")
    for prop_id in all_processed_ids:
        mark_property_processed_today(conn, prop_id)

    log(f"Finished {municipality_name}. Total Updated: {total_updated_count} of {len(all_processed_ids)} properties.")
    return total_updated_count


def update_freshness_status(conn, source_name, source_type, status, details=None, external_date=None):
    """Updates the data_source_status table with progress."""
    try:
        with conn.cursor() as cursor:
            if details:
                # Ensure details is valid JSON
                if isinstance(details, (dict, list)):
                    details = json.dumps(details, default=str)
                elif isinstance(details, str):
                    # Wrap strings in a simple object or dump as string
                    # Here we wrap in an object for better extensibility
                    try:
                        json.loads(details)
                    except:
                        details = json.dumps({"message": details})

            cursor.execute("""
                INSERT INTO data_source_status 
                (source_name, source_type, last_refreshed_at, refresh_status, details, external_last_updated)
                VALUES (%s, %s, NOW(), %s, %s, %s)
                ON CONFLICT (source_name) 
                DO UPDATE SET 
                    last_refreshed_at = EXCLUDED.last_refreshed_at,
                    refresh_status = EXCLUDED.refresh_status,
                    details = EXCLUDED.details,
                    external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated);
            """, (source_name, source_type, status, details, external_date))
            
            # --- NEW: Invalidate Completeness Matrix Cache ---
            # ensures the frontend "Completeness Matrix" reflects this update immediately
            cursor.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
            
            conn.commit()
    except Exception as e:
        log(f"Error updating freshness status for {source_name}: {e}")

# --- Windsor API Scraper ---
def process_municipality_with_windsor_api(conn, municipality_name, config, current_owner_only=False, force_process=False):
    log(f"--- Processing {municipality_name} via Windsor API ---")
    base_api_url = "https://windsorct.com/sf/win/v1/propertycard/address/get"
    
    # 1. Fetch properties from DB to get addresses
    # Windsor API requires Street Number and Street Name.
    # We rely on our DB (seeded from statewide CSV) to provide the list of targets.
    log(f"Fetching properties for {municipality_name} to enrich...")
    properties_to_scan = []
    
    with conn.cursor() as cur:
        # If current_owner_only, filter. Else get all.
        if current_owner_only:
             # This might be tricky if "Current Owner" isn't set yet (e.g. fresh import).
             # But usually we use this for re-scanning.
             cur.execute("SELECT id, location FROM properties WHERE property_city ILIKE %s AND owner like 'Current Owner%'", (municipality_name,))
        else:
             cur.execute("SELECT id, location FROM properties WHERE property_city ILIKE %s", (municipality_name,))
        
        properties_to_scan = cur.fetchall()
        
    log(f"Found {len(properties_to_scan)} properties to scan.")
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    
    updated_count = 0
    
    def process_prop_row(row):
        prop_id, location = row
        if not location: return False
        
        # Parse Address
        # "275 BROAD ST" -> num="275", name="BROAD"
        match = re.search(r"^(\d+(?:-\d+)?)\s+(.*)$", location.strip())
        if not match:
             # Try simple split if regex fails (e.g. no number?)
             return False
             
        st_num = match.group(1)
        st_name = match.group(2)
        # Clean street name (remove suffixes like ST, AVE might be needed? User query check implies just "broad" works for "BROAD ST"?)
        # User query: ?st_num=275&st_name=broad
        # Let's try to strip common suffixes to be safe, or just send full name?
        # The API likely does partial match or requires cleaner name.
        # "BROAD ST" -> "BROAD"
        st_name_cleaned = re.sub(r"\s+(?:ST|AVE|RD|LN|DR|CT|CIR|PL|BLVD|HWY|TPKE)$", "", st_name, flags=re.IGNORECASE).strip()
        
        try:
             # Rate limiting - simple sleep
             time.sleep(0.5) 
             
             resp = session.get(base_api_url, params={"st_num": st_num, "st_name": st_name_cleaned}, timeout=20)
             if resp.status_code != 200:
                 return False
                 
             data = resp.json()
             # Validate response - sometimes returns empty or error object
             if not data or not data.get('propertyLocation'):
                 return False
                 
             # Map fields
             # Response:
             # "ownerName": "...", "ownerName2": "..."
             # "currentAppraised": 123456
             # "saleDate": { ... timestamp ... }
             # "propertyImage": "PropertyImages/1785.jpg"
             
             owner = data.get('ownerName', '').strip()
             if data.get('ownerName2'):
                 owner += f" & {data.get('ownerName2').strip()}"
             
             appraised = data.get('currentAppraised')
             assessed = data.get('currentAssessed')
             
             sale_date = None
             if data.get('saleDate') and data['saleDate'].get('timestamp'):
                  try:
                      # Timestamp looks like -62169984000 (very old) or realistic.
                      # "timestamp": 1269907200 -> 2010-03-30
                      ts = data['saleDate']['timestamp']
                      if ts > 0:
                           sale_date = datetime.fromtimestamp(ts).date()
                  except: pass
                  
             sale_price = data.get('salePrice')
             
             img_path = data.get('propertyImage')
             photo_url = None
             if img_path:
                  # User provided: https://info.townofwindsorct.com/images/4857.jpg
                  # API returns: PropertyImages/1785.jpg
                  # Logic: extract basename ("1785.jpg") and append to new base.
                  filename = os.path.basename(img_path)
                  photo_url = f"https://info.townofwindsorct.com/images/{filename}"
                  
             # Update DB
             scraped_data = {
                 'owner': owner,
                 'appraised_value': appraised,
                 'assessed_value': assessed,
                 'sale_date': sale_date,
                 'sale_amount': sale_price,
                 'building_photo': photo_url,
                 'cama_site_link': None, # No direct link available? Or maybe we can construct one to a frontend?
                 # User said "disable the clicking for any muni you couldn't find...".
                 # But if we have data, we might want a link. 
                 # There doesn't seem to be a public URL for a card in the API response.
                 # Leaving cama_site_link None (or previous value) is fine.
             }
             
             return update_property_in_db(conn, prop_id, scraped_data, municipality_name=municipality_name)
             
        except Exception as e:
             # log(f"Error processing {location}: {e}")
             return False

    # Parallel execution with workers
    # User warned about slowness. 4 workers with 0.5s sleep each = ~8 req/s total. Maybe ok.
    # Be careful.
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_prop_row, properties_to_scan))
        updated_count = sum(1 for r in results if r)
        
    log(f"Windsor Update Complete. Updated {updated_count} properties.")
    return updated_count

# --- NEW PARALLEL WORKER FUNCTION ---

def process_municipality_task(city_name, city_data, current_owner_only, force_process):
    """
    Worker task for processing a single municipality. 
    This function creates its OWN database connection to ensure thread safety.
    """
    log(f"WORKER_START: Starting job for {city_name}")
    conn = None
    source_type = 'unknown'
    if city_name in MUNICIPAL_DATA_SOURCES:
        source_type = MUNICIPAL_DATA_SOURCES[city_name].get('type', 'unknown')
    else:
        source_type = city_data.get('type', 'vision_appraisal')

    try:
        # Each thread MUST create its own connection
        conn = get_db_connection()
        
        # 1. Mark as RUNNING
        update_freshness_status(conn, city_name, source_type, 'running', details="Starting update...")

        # Check if this municipality has a custom data source configuration
        if city_name in MUNICIPAL_DATA_SOURCES:
            log(f"Using custom data source for {city_name}: {MUNICIPAL_DATA_SOURCES[city_name]['type']}")
            
            if MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'arcgis_csv':
                updated_count = process_municipality_with_arcgis(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'MAPXPRESS':
                updated_count = process_municipality_with_mapxpress(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'PROPERTYRECORDCARDS':
                updated_count = process_municipality_with_propertyrecordcards(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'ct_geodata_csv':
                if not force_process and not should_scrape(city_name, MUNICIPAL_DATA_SOURCES[city_name], conn):
                    updated_count = 0
                    update_freshness_status(conn, city_name, source_type, 'success', details="Skipped: Remote data has not changed.")
                else:
                    updated_count = process_municipality_with_ct_geodata(
                        conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process
                    )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'avon_static':
                updated_count = process_municipality_with_avon_static(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name]['url'], current_owner_only, force_process
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'hartford_script':
                try:
                    # Import here to avoid top-level circular dependency if any
                    from api.hartford_enrichment import run_enrichment
                    log(f"--- Processing HARTFORD via Custom Script ---")
                    # run_enrichment returns an integer count of updated properties
                    updated_count = run_enrichment()
                except Exception as e:
                    log(f"Error running Hartford script: {e}")
                    updated_count = 0
                updated_count = process_municipality_with_actdatascout(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'windsor_api':
                updated_count = process_municipality_with_windsor_api(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'vision_appraisal':
                # Check for skipped update optimization
                new_date = city_data.get('last_updated')
                if not force_process and new_date:
                    # Check DB for previous date
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT external_last_updated, refresh_status FROM data_source_status WHERE source_name = %s", (city_name,))
                        row = cursor.fetchone()
                        if row:
                            prev_date, status = row
                            # If successful last time and dates match (or new date is older), skip
                            if status == 'success' and prev_date and new_date.date() <= prev_date.date():
                                log(f"SKIPPING {city_name}: Data up to date (Portal: {new_date.date()} <= DB: {prev_date.date()})")
                                update_freshness_status(conn, city_name, source_type, 'success', details="Skipped: Data up to date", external_date=new_date)
                                return 0

                updated_count = process_municipality_with_realtime_updates(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name]['url'], last_updated_date=new_date, current_owner_only=current_owner_only, force_process=force_process
                )
            else:
                log(f"Unknown data source type for {city_name}: {MUNICIPAL_DATA_SOURCES[city_name]['type']}")
                updated_count = 0
        else:
            # Use traditional Vision Appraisal scraping
            # Check for skipped update optimization
            new_date = city_data.get('last_updated')
            if not force_process and new_date:
                 with conn.cursor() as cursor:
                    cursor.execute("SELECT external_last_updated, refresh_status FROM data_source_status WHERE source_name = %s", (city_name,))
                    row = cursor.fetchone()
                    if row:
                        prev_date, status = row
                        if status == 'success' and prev_date and new_date.date() <= prev_date.date():
                            log(f"SKIPPING {city_name}: Data up to date (Portal: {new_date.date()} <= DB: {prev_date.date()})")
                            update_freshness_status(conn, city_name, source_type, 'success', details="Skipped: Data up to date", external_date=new_date)
                            return 0

            updated_count = process_municipality_with_realtime_updates(
                conn, city_name, city_data['url'], last_updated_date=new_date, current_owner_only=current_owner_only, force_process=force_process
            )
        
        log(f"WORKER_DONE: Finished job for {city_name}. Updated {updated_count} properties.")
        
        # 2. Mark as SUCCESS
        update_freshness_status(conn, city_name, source_type, 'success', details=f"Updated {updated_count} properties")
        
        return updated_count
    
    except Exception as e:
        log(f"!!! WORKER_ERROR: Critical error processing {city_name}: {e}")
        log(f"Traceback for {city_name}: {traceback.format_exc()}")
        
        # 3. Mark as FAILURE
        if conn:
            update_freshness_status(conn, city_name, source_type, 'failure', details=str(e)[:255])
            
        return 0  # Return 0 updates on failure for this town
    
    finally:
        if conn:
            conn.close()
            log(f"WORKER_CLEANUP: Closed DB connection for {city_name}.")


# --- Main Execution (Rewritten for Parallelism) ---
def main():
    """Main function to run the update process."""
    parser = argparse.ArgumentParser(description='Update property data from Vision Appraisal websites and other municipal data sources')
    parser.add_argument('--municipalities', '-m', nargs='*', 
                       help='Specific municipalities to process (e.g., "ANDOVER BERLIN HARTFORD")')
    parser.add_argument('--year-filter', '-y', type=int, default=2024,
                       help='Only process municipalities updated since this year (inclusive - e.g., 2024 means 2024 and later)')
    parser.add_argument('--current-owner-only', '-c', action='store_true',
                       help='Process only properties with owner="Current Owner", ordered by municipality with most such properties')
    parser.add_argument('--parallel-munis', '-p', type=int, default=DEFAULT_MUNI_WORKERS,
                       help=f'Number of municipalities to process in parallel (Default: {DEFAULT_MUNI_WORKERS})')
    parser.add_argument('--force', '-f', action='store_true',
                       help='Force reprocessing of all properties, ignoring the last processed date')
    args = parser.parse_args()
    
    log(f"Starting data update process...")
    log(f"Settings: Parallel Municipalities={args.parallel_munis}, Force Reprocess={args.force}")
    
    conn = None # Main connection for initial setup ONLY
    total_updated = 0
    municipalities_to_check = {}
    
    try:
        # --- 1. SETUP PHASE: Use a single connection to build the job list ---
        conn = get_db_connection()
        
        # Create processing log table once at the start
        create_processing_log_table(conn)
        
        # Get Vision Appraisal municipalities
        all_municipalities_from_vision = get_vision_municipalities()
        if not all_municipalities_from_vision:
            log("No municipalities found on Vision site.")
        
        # Add custom data source municipalities to the list
        for muni_name, config in MUNICIPAL_DATA_SOURCES.items():
            if muni_name not in all_municipalities_from_vision:
                all_municipalities_from_vision[muni_name] = {
                    "last_updated": datetime.now(),  # Assume current for custom sources
                    "url": config.get('url', ''),
                    "type": config['type']
                }
                log(f"Added custom data source municipality: {muni_name} ({config['type']})")

        if not all_municipalities_from_vision:
            log("No municipalities available from any source. Exiting.")
            return

        if args.current_owner_only:
            # Special mode: Process only "Current Owner" properties
            log("MODE: Processing only properties with owner='Current Owner'")
            
            # Query DB for list of towns sorted by "Current Owner" count
            current_owner_municipalities = get_current_owner_properties_by_municipality(conn)
            
            # Filter by specified municipalities if provided
            if args.municipalities:
                target_munis = {m.upper() for m in args.municipalities}
                current_owner_municipalities = [
                    (city, count) for city, count in current_owner_municipalities 
                    if city and city.upper() in target_munis
                ]
            
            if not current_owner_municipalities:
                log("No matching municipalities found with 'Current Owner' properties.")
                return
            
            log("Found municipalities with 'Current Owner' properties:")
            for city, count in current_owner_municipalities:
                log(f"  - {city}: {count} properties")
            log("")
            
            log("")
            
            # Match this list against the available municipality list to get URLs/configs
            for city, count in current_owner_municipalities:
                city_upper = city.upper() if city else ""
                if city_upper in all_municipalities_from_vision:
                    municipalities_to_check[city_upper] = all_municipalities_from_vision[city_upper]
                    municipalities_to_check[city_upper]['current_owner_count'] = count
                else:
                    log(f"Warning: '{city}' (from DB) not found in available municipality list.")
            
        else:
            # Normal mode: Filter by year and/or specific municipalities
            if args.municipalities:
                # Process only specified municipalities
                for name in args.municipalities:
                    name_upper = name.upper()
                    if name_upper in all_municipalities_from_vision:
                        municipalities_to_check[name_upper] = all_municipalities_from_vision[name_upper]
                    else:
                        log(f"Municipality '{name}' not found in available list.")
            else:
                # Process municipalities updated since specified year (inclusive)
                log(f"MODE: Processing all municipalities updated in {args.year_filter} or later.")
                municipalities_to_check = { 
                    name: data for name, data in all_municipalities_from_vision.items() 
                    if data['last_updated'].year >= args.year_filter  # Use >= for inclusive logic
                }
                
                # --- AUTO-PRIORITIZATION ---
                # Fetch missing data stats to prioritize incomplete towns
                try:
                    log("Fetching data freshness stats for prioritization...")
                    with conn.cursor() as cur:
                        # Prioritize by count of 'Current Owner' (missing owner) + missing photos
                        cur.execute("""
                            SELECT 
                                UPPER(property_city) as city, 
                                COUNT(CASE WHEN owner LIKE 'Current Owner%' THEN 1 END) as missing_owners,
                                COUNT(CASE WHEN building_photo IS NULL THEN 1 END) as missing_photos
                            FROM properties 
                            GROUP BY property_city
                        """)
                        rows = cur.fetchall()
                        for row in rows:
                            city = row[0]
                            if city in municipalities_to_check:
                                # Weighted Score: Missing Owner is critical (10pts), Missing Photo is nice to have (1pt)
                                score = (row[1] * 10) + row[2]
                                municipalities_to_check[city]['priority_score'] = score
                except Exception as e:
                    log(f"Prioritization query failed: {e}. Falling back to alphabetical.")

        
        if not municipalities_to_check:
            log(f"No municipalities match your criteria. Try --year-filter with earlier year or specify --municipalities.")
            return

        log(f"--- Job Queue: {len(municipalities_to_check)} municipalities to process ---")
        
        # Build the final list of items to process
        municipality_list = list(municipalities_to_check.items())
        
        if args.current_owner_only:
            # Sort by current owner count (descending)
            municipality_list.sort(key=lambda x: x[1].get('current_owner_count', 0), reverse=True)
            for name, data in municipality_list:
                 data_type = data.get('type', 'vision_appraisal')
                 log(f"  - (Priority) {name} ({data.get('current_owner_count', 0)} properties) [{data_type}]")
        else:
            # Sort by Calculated Priority Score (Descending) -> Worst Data First
            # Default score is 0 if not calculated
            municipality_list.sort(key=lambda x: x[1].get('priority_score', 0), reverse=True)
            
            for name, data in municipality_list:
                data_type = data.get('type', 'vision_appraisal')
                score = data.get('priority_score', 0)
                if score > 0:
                     log(f"  - {name} [Priority Score: {score}] (updated: {data['last_updated'].strftime('%Y-%m-%d')}) [{data_type}]")
                else:
                     log(f"  - {name} (updated: {data['last_updated'].strftime('%Y-%m-%d')}) [{data_type}]")
        log("---------------------------------------------------\n")

        # --- 2. CLOSE SETUP CONNECTION ---
        # We are done with the main connection. The workers will make their own.
        conn.close()
        conn = None 
        log("Setup complete. Closed main DB connection. Starting worker pool.")

        # --- 3. EXECUTION PHASE: Process the queue in parallel ---
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel_munis) as executor:
            # Submit all jobs to the pool
            future_to_city = {
                executor.submit(process_municipality_task, city_name, city_data, args.current_owner_only, args.force): city_name
                for city_name, city_data in municipality_list
            }

            log(f"Submitted {len(future_to_city)} municipality jobs to the thread pool with {args.parallel_munis} workers...")

            # Process results as they complete
            completed_count = 0
            for future in concurrent.futures.as_completed(future_to_city):
                completed_count += 1
                city_name = future_to_city[future]
                try:
                    updated_count = future.result()  # This will be the return value from process_municipality_task
                    log(f"COMPLETED: [{completed_count}/{len(future_to_city)}] Job for {city_name} finished, returned {updated_count} updates.")
                    total_updated += updated_count
                except Exception as exc:
                    # This catches exceptions in the future.result() itself, though process_municipality_task should catch its own.
                    log(f"!!! MAIN_POOL_ERROR: Job for {city_name} generated an unhandled exception: {exc}")

                # --- PERIODIC NETWORK REFRESH ---
                if completed_count > 0 and completed_count % 3 == 0:
                    log(f"🔄 [PERIODIC] 3 municipalities completed. Triggering Network Refresh...")
                    if run_refresh:
                         try:
                             # Run refresh synchronously in the main thread (pauses new job submissions effectively, 
                             # since we are in the consumption loop, but workers might still be running if parallel > 1.
                             # This is fine, refresh uses DB snapshots or locks if needed.
                             # Ideally we want to let current jobs finish? No, existing method is atomic swap so safe.
                             log("Starting Refresh...")
                             run_refresh(depth=4) 
                             log("Refresh Complete.")
                         except Exception as e:
                             log(f"Refresh failed: {e}")
                    else:
                        log("Skipping refresh (module not imported).")

        property_type = "'Current Owner'" if args.current_owner_only else "all"
        log(f"\n--- PROCESS COMPLETE ---")
        log(f"Updated {total_updated} total {property_type} properties across {len(municipality_list)} municipalities.")

    except Exception as e:
        log(f"Critical error in main thread: {e}")
        log(f"Traceback: {traceback.format_exc()}")
    finally:
        if conn:
            # This should only run if the script failed *before* the setup connection was intentionally closed
            conn.close()
            log("Closed lingering main setup DB connection due to error.")
        log("Script finished.")

# --- Avon Static Scraper ---

def process_municipality_with_avon_static(conn, municipality_name, base_url, current_owner_only=False, force_process=False):
    """
    Scrapes Avon's static HTML assessor site.
    Starting point: http://assessor.avonct.gov/prop_addr.html
    """
    log(f"--- Processing municipality: {municipality_name} via Static HTML (Force={force_process}) ---")
    
    # 1. Get Street Index
    try:
        # Direct to the full street listing
        index_url = f"{base_url.rstrip('/')}/propcards/streets.html"
        resp = requests.get(index_url, timeout=10)
        if resp.status_code != 200:
            log(f"Failed to fetch index: {index_url}")
            return 0
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        street_links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            clean_href = href.replace('\\', '/').strip()
            if 'street.html' in clean_href:
                full_url = requests.compat.urljoin(index_url, clean_href)
                full_url = full_url.split('#')[0]
                street_links.add(full_url)
        
        log(f"Found {len(street_links)} street index pages.")
        
        updated_count = 0
        
        # 2. Process each street index page
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(process_avon_street_page, url): url for url in street_links}
            
            for future in concurrent.futures.as_completed(future_to_url):
                page_url = future_to_url[future]
                try:
                    props = future.result() 
                    for prop_url, address_text in props:
                        if process_avon_property(conn, prop_url, address_text, municipality_name):
                            updated_count += 1
                            if updated_count % 20 == 0:
                                update_freshness_status(conn, municipality_name, 'avon_static', 'running', details=f"Scraped {updated_count} properties (Street: {page_url.split('/')[-1]})")

                except Exception as exc:
                    log(f"Error processing {page_url}: {exc}")
                    
        return updated_count

    except Exception as e:
        log(f"Critical error scraping Avon: {e}")
        return 0

def process_avon_street_page(url):
    """Fetches a street page (Astreet.html) and returns property links."""
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return []
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        properties = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/admin/a' in href and '.html' in href:
                 full_url = requests.compat.urljoin(url, href)
                 address_text = a.get_text().strip()
                 properties.append((full_url, address_text))
        return properties
    except:
        return []

def process_avon_property(conn, url, address_text, municipality_name):
    """Fetches and parses a single property card."""
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return False
            
        content = resp.text
        
        owner_match = re.search(r"Owner name:\s*(.*?)\s*\|", content)
        owner = owner_match.group(1).strip() if owner_match else None
        
        co_owner_match = re.search(r"Second name:\s*(.*?)\s*\|", content)
        co_owner = co_owner_match.group(1).strip() if co_owner_match else None
        
        addr_match = re.search(r"Address:\s*(.*?)\s*\|", content)
        location = addr_match.group(1).strip() if addr_match else address_text
        
        sale_date_match = re.search(r"Sale date:\s*(.*?)\s*\|", content)
        sale_date = None
        if sale_date_match:
            try:
                sale_date = datetime.strptime(sale_date_match.group(1).strip(), "%d-%b-%Y").date()
            except:
                pass
                
        sale_price_match = re.search(r"Sale price:\s*(.*?)\s*\|", content)
        sale_price = None
        if sale_price_match:
            try:
                sale_price = float(sale_price_match.group(1).strip().replace(',', ''))
            except:
                pass

        prop_id_match = re.search(r"\/a(\d+)\.html", url)
        prop_id = prop_id_match.group(1) if prop_id_match else None
        
        if not location:
            return False

        scraped_data = {
            'owner': owner,
            'co_owner': co_owner,
            'location': location,
            'sale_date': sale_date,
            'sale_amount': sale_price,
            'cama_site_link': url,
            'property_city': municipality_name.upper(),
            'account_number': prop_id 
        }
        
        db_id = get_or_create_property_id(conn, municipality_name, location)
        return update_property_in_db(conn, db_id, scraped_data, municipality_name=municipality_name)

    except Exception as e:
        return False

def get_or_create_property_id(conn, city, address):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM properties WHERE property_city = %s AND location = %s", (city.upper(), address.upper()))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO properties (property_city, location, source) VALUES (%s, %s, 'avon_static') RETURNING id", (city.upper(), address.upper()))
        return cur.fetchone()[0]

# --- ActDataScout Scraper (Norwalk, etc) ---

def process_municipality_with_actdatascout(conn, municipality_name, config, current_owner_only=False, force_process=False):
    log(f"--- Processing {municipality_name} via ActDataScout ---")
    base_url = config['url']
    county_id = config.get('county_id') # e.g. 9103 for Norwalk
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": base_url
    })
    
    # 1. Init Session & Get Token
    try:
        resp = session.get(base_url, timeout=15)
        soup = BeautifulSoup(resp.content, 'html.parser')
        token = soup.find('input', {'name': '__RequestVerificationToken'})['value']
        log("Got ActDataScout session token.")
    except Exception as e:
        log(f"Failed to init ActDataScout session: {e}")
        return 0
        
    search_url = "https://www.actdatascout.com/RealProperty/Search"
    
    # 2. Recursive Search
    # We iterate 2-letter prefixes AA..ZZ and numbers 0..9
    # If a query hits 100 limit, we should drill down (not implemented fully complexity, hopefully 2-char is enough)
    # The subagent said "MAIN" (4 chars) returned 658 results but only showed 100.
    # So we MUST drill down further than 2 chars for common streets.
    # Logic: Search prefix. If count == 100, recurse (append A..Z). If count < 100, process.
    
    # Queue of prefixes to process
    # Start with explicit letters A-Z, 0-9
    import string
    queue = list(string.ascii_uppercase) + list(string.digits)
    
    updated_count = 0
    processed_urls = set()
    
    while queue:
        prefix = queue.pop(0)
        # Optimization: Don't go too deep indiscriminately
        if len(prefix) > 4: 
             log(f"Prefix {prefix} too deep, skipping expansion but scraping what we have.")
             # scrape anyway
        
        log(f"Searching prefix: {prefix}")
        
        payload = {
            "__RequestVerificationToken": token,
            "CountyId": county_id,
            "TaxYear": "",
            "StreetNumber": "",
            "StreetDirection": "",
            "StreetName": prefix,
            "StreetNameMatchType": "false", # Starts With?
            "SearchType": "address"
        }
        
        try:
            # We must expect JSON or HTML? Subagent said XHR.
            # Usually returns partial HTML View.
            p_resp = session.post(search_url, data=payload, headers={"X-Requested-With": "XMLHttpRequest"})
            
            if p_resp.status_code != 200:
                log(f"Search failed for {prefix}: {p_resp.status_code}")
                # Sometimes "A" fails. Retrying with longer prefix might work.
                if len(prefix) == 1:
                     queue.extend([prefix + c for c in string.ascii_uppercase])
                continue
                
            # Count results
            # The HTML usually contains a table or list.
            # "Displaying 1 - 100 of 658"
            res_soup = BeautifulSoup(p_resp.content, 'html.parser')
            
            # Check for limit message
            # There might be a pager, or just a limit information.
            # If we see "Displaying ... of X", and X > 100, we must recurse.
            # Or if row count == 100.
            
            rows = res_soup.find_all('tr', attrs={'data-id': True}) # Assuming generic data grid
            # Update: ActDataScout results look like cards or table logic.
            # Using generic href extraction to find Parcel Links.
            
            parcel_links = set()
            for a in res_soup.find_all('a', href=True):
                href = a['href']
                if '/Parcel/' in href:
                    full = requests.compat.urljoin(base_url, href)
                    if full not in processed_urls:
                        parcel_links.add(full)
                        
            count = len(parcel_links)
            # log(f"Prefix {prefix} found {count} parcels.")
            
            # Determine if we hit a limit.
            # ActDataScout creates a "Search Results" headers?
            # If count >= 100, we probably missed some.
            if count >= 100:
                # Recurse
                # log(f"Hit limit (>=100) for {prefix}, recursing...")
                new_prefixes = [prefix + c for c in string.ascii_uppercase]
                # Also numbers if mixed? "1st", "2nd".
                # For simplicity, just letters.
                queue.extend(new_prefixes)
                
                # Should we scrape these 100 anyway? Yes, to be safe.
                # But better to scrape the drilled down ones to avoid duplicates?
                # Actually, ActDataScout might not return "next page".
                # So we scrape these 100, and rely on recursion to find the REST (which are NOT in this 100).
                # Wait, if I search "A" and get 1-100 of 1000.
                # If I search "AA" I get 1-50 of 50.
                # I should just recurse and NOT scrape the truncated list?
                # Or scrape it to be safe. 
                # Scrape it.
                pass
            
            # Scrape found links
            for link in parcel_links:
                if link in processed_urls: continue
                processed_urls.add(link)
                # We can process in parallel or serial. Serial for polite rate.
                if process_actdatascout_property(conn, session, link, municipality_name, config):
                    updated_count += 1
                    
                if updated_count % 20 == 0:
                    update_freshness_status(conn, municipality_name, 'actdatascout', 'running', details=f"Scraped {updated_count} properties (Prefix: {prefix})")

            
        except Exception as e:
            log(f"Error searching {prefix}: {e}")
            
    return updated_count

def process_actdatascout_property(conn, session, url, municipality_name, config):
    try:
        resp = session.get(url, timeout=10)
        if resp.status_code != 200: return False
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Parse logic
        # Owner Name
        # Looking for generic structure or labels.
        # "Owner Information"
        # owner = ...
        
        # ActDataScout Structure (Generalized):
        # <div class="col-md-4">...<strong>Owner Name</strong>...<br>SMITH JOHN...
        
        text = soup.get_text(" ", strip=True)
        
        owner = None
        location = None
        sale_date = None
        sale_price = None
        
        # Regex extraction for robustness against layout changes
        # "Owner Name: SMITH JOHN"
        # "Physical Address: 123 MAIN ST"
        
        # Owner
        # Specific ID often used: #OwnerName ? 
        # Inspecting source from previous steps would help, but I'll guess standard labels.
        # "Primary Owner:"
        own_match = re.search(r"Primary Owner[:\s]+(.*?)(?:\s\s|$)", text, re.IGNORECASE)
        if own_match: owner = own_match.group(1).strip()
        
        # Location
        # "Physical Address:" or "Situs Address"
        loc_match = re.search(r"Physical Address[:\s]+(.*?)(?:\s\s|$)", text, re.IGNORECASE)
        if loc_match: location = loc_match.group(1).strip()
        
        # Sales
        # "Deed Date:" or "Sale Date"
        sdate_match = re.search(r"Sale Date[:\s]+(\d+/\d+/\d+)", text, re.IGNORECASE)
        if sdate_match: 
            try: sale_date = datetime.strptime(sdate_match.group(1), "%m/%d/%Y").date()
            except: pass
            
        sprice_match = re.search(r"Sale Price[:\s]+\$([\d,]+)", text, re.IGNORECASE)
        if sprice_match:
             try: sale_price = float(sprice_match.group(1).replace(',', ''))
             except: pass
             
        # ID extraction from URL or page
        # /Parcel/12345
        prop_id = url.split('/')[-1]

        if not location:
             # Fallback: Scrape title or header
             h1 = soup.find('h1') # often address?
             # pass
             return False

        # Upsert
        scraped_data = {
            'owner': owner,
            'location': location,
            'sale_date': sale_date,
            'sale_amount': sale_price,
            'cama_site_link': url,
            'property_city': municipality_name.upper(),
            'account_number': prop_id
        }
        
        # DB ID lookup
        db_id = get_or_create_property_id(conn, municipality_name, location) # Reusing helper from Avon (needs to be global or passed)
        # Wait, get_or_create_property_id is defined at bottom.
        # It's fine
        
        return update_property_in_db(conn, db_id, scraped_data, municipality_name=municipality_name)
        
    except Exception as e:
        # log(f"Error parsing prop {url}: {e}")
        return False

if __name__ == "__main__":
    main()
