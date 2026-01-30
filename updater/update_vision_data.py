import threading
import os
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
import pandas as pd
# --- Shared Cache for Large CSVs ---
GEODATA_CACHE = {}
GEODATA_LOCK = threading.Lock()

# --- Network Refresh Control ---
REFRESH_LOCK = threading.Lock()
LAST_REFRESH_TIME = 0
REFRESH_COOLDOWN_SECONDS = 300 # 5 minutes between refreshes to avoid thrashing
# Add root directory to path for sibling imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
# Also add api directory explicitly just in case
sys.path.append(os.path.join(os.path.dirname(__file__), '../api'))

try:
    from api.safe_network_refresh import run_refresh
except ImportError:
    from safe_network_refresh import run_refresh

try:
    from scripts.sync_data_dates import update_status
except ImportError:
    # If scripts folder is not in path yet, update_status will be None 
    # and we'll handle it gracefully in the worker
    update_status = None

# Suppress only the single InsecureRequestWarning from urllib3 needed for this script
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
VISION_BASE_URL = "https://www.vgsi.com"
CONNECTICUT_DATABASE_URL = f"{VISION_BASE_URL}/connecticut-online-database/"
MAX_WORKERS = 12  # Approved reduction to avoid timeouts
DEFAULT_MUNI_WORKERS = 4 # Process fewer municipalities in parallel to reduce server pressure

# --- Municipality-specific data sources ---
MUNICIPAL_DATA_SOURCES = {
# update_vision_data.py

# ... inside MUNICIPAL_DATA_SOURCES dictionary ...
    "HARTFORD": {
        'type': 'ct_geodata_csv',
        'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0',
        'town_filter': 'Hartford'
    },
# ...
    # Add more municipalities here as needed
    'ANSONIA': {'type': 'MAPXPRESS', 'domain': 'ansonia.mapxpress.net'},
    'BEACON FALLS': {'type': 'MAPXPRESS', 'domain': 'beaconfalls.mapxpress.net'},
    # 'BERLIN': {'type': 'MAPXPRESS', 'domain': 'berlin.mapxpress.net'},
    'BETHANY': {'type': 'MAPXPRESS', 'domain': 'bethany.mapxpress.net'},
    'BETHLEHEM': {'type': 'MAPXPRESS', 'domain': 'bethlehem.mapxpress.net'},
    'BRIDGEPORT': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/bridgeportct/'},
    'BRISTOL': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/bristolct/'},
    'BROOKFIELD': {'type': 'MAPXPRESS', 'domain': 'brookfield.mapxpress.net'},
    'BURLINGTON': {'type': 'MAPXPRESS', 'domain': 'burlington.mapxpress.net'},
    'CANTON': {'type': 'MAPXPRESS', 'domain': 'canton.mapxpress.net'},
    'CHESHIRE': {'type': 'MAPXPRESS', 'domain': 'cheshire.mapxpress.net'},
    'COLCHESTER': {'type': 'MAPXPRESS', 'domain': 'colchester.mapxpress.net'},
    'COVENTRY': {'type': 'MAPXPRESS', 'domain': 'coventry.mapxpress.net'},
    'DERBY': {'type': 'MAPXPRESS', 'domain': 'derby.mapxpress.net'},
    'EAST HAVEN': {'type': 'MAPXPRESS', 'domain': 'easthaven.mapxpress.net'},
    'FARMINGTON': {'type': 'MAPXPRESS', 'domain': 'farmington.mapxpress.net'},
    'LITCHFIELD': {'type': 'MAPXPRESS', 'domain': 'litchfield.mapxpress.net'},
    'MIDDLEBURY': {'type': 'MAPXPRESS', 'domain': 'middlebury.mapxpress.net'},
    'NAUGATUCK': {'type': 'MAPXPRESS', 'domain': 'naugatuck.mapxpress.net'},
    'NEW BRITAIN': {'type': 'MAPXPRESS', 'domain': 'newbritain.mapxpress.net'},
    'NEWTOWN': {'type': 'MAPXPRESS', 'domain': 'newtown.mapxpress.net'},
    'OXFORD': {'type': 'MAPXPRESS', 'domain': 'oxford.mapxpress.net'},
    'PLAINVILLE': {'type': 'MAPXPRESS', 'domain': 'plainville.mapxpress.net'},
    'SALEM': {'type': 'MAPXPRESS', 'domain': 'salem.mapxpress.net'},
    'SEYMOUR': {'type': 'MAPXPRESS', 'domain': 'seymour.mapxpress.net'},
    'SOUTHBURY': {'type': 'MAPXPRESS', 'domain': 'southbury.mapxpress.net'},
    'SUFFIELD': {'type': 'MAPXPRESS', 'domain': 'suffield.mapxpress.net'},
    'WEST HAVEN': {'type': 'MAPXPRESS', 'domain': 'westhaven.mapxpress.net'},
    # PropertyRecordCards Municipalities
    'ANSONIA': {'type': 'PROPERTYRECORDCARDS', 'towncode': '002'},
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
    'WEST HARTFORD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/westhartfordct/'},
    'WOODBRIDGE': {'type': 'MAPXPRESS', 'domain': 'woodbridge.mapxpress.net'},

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
    'WATERBURY': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Waterbury'},
    # 'BRIDGEPORT': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Bridgeport'},
    'HARTFORD': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Hartford'},
    'STAMFORD': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/stamfordct/'},
    'NORWALK': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Norwalk'},
    'DANBURY': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Danbury'},
    'NEW BRITAIN': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'New Britain'},
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
    'SHELTON': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Shelton'},
    'TORRINGTON': {'type': 'ct_geodata_csv', 'url': 'https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0', 'town_filter': 'Torrington'},
    'TRUMBULL': {'type': 'vision_appraisal', 'url': 'https://gis.vgsi.com/trumbullct/'},
}

# --- Shared Cache for Large CSVs ---
GEODATA_CACHE = {}
GEODATA_LOCK = threading.Lock()

# --- Utility: Robust Requests with Retries ---
def requests_get_with_retries(url, session=None, headers=None, max_retries=5, timeout=30):
    """
    Performs a GET request with exponential backoff and jitter.
    If session is provided, use it; otherwise, use requests.get.
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            # Add small random jitter (0-2s) to avoid thundering herd
            if attempt > 0:
                wait_time = (2 ** attempt) + random.uniform(0, 2)
                time.sleep(wait_time)
            
            call_func = session.get if session else requests.get
            resp = call_func(url, verify=False, timeout=timeout, headers=headers)
            
            # If we get a 429 (Too Many Requests) or 5xx, retry
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                # log(f"  [RETRY] HTTP {resp.status_code} for {url} (Attempt {attempt+1}/{max_retries})")
                continue
                
            resp.raise_for_status()
            return resp
            
        except (requests.exceptions.RequestException, urllib3.exceptions.HTTPError) as e:
            last_exception = e
            # log(f"  [RETRY] Error fetching {url}: {e} (Attempt {attempt+1}/{max_retries})")
            continue
            
    # If we reached here, all retries failed
    raise last_exception if last_exception else Exception(f"Failed to fetch {url} after {max_retries} attempts")

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

def trigger_network_refresh(municipality_name):
    """
    Triggers a safe network refresh if cooldown has passed.
    Thread-safe.
    """
    global LAST_REFRESH_TIME
    
    # Quick check without lock first
    if time.time() - LAST_REFRESH_TIME < REFRESH_COOLDOWN_SECONDS:
        log(f"  [Info] Skipping network refresh after {municipality_name} (Cooldown active).")
        return

    # Acquire lock to ensure only one thread refreshes
    if REFRESH_LOCK.acquire(blocking=False):
        try:
            # Double check time after acquiring lock
            if time.time() - LAST_REFRESH_TIME < REFRESH_COOLDOWN_SECONDS:
                log(f"  [Info] Skipping network refresh after {municipality_name} (Cooldown active).")
                return
            
            log(f"🌐 Triggering Network Refresh after {municipality_name}...")
            success = run_refresh(depth=4, dry_run=False) # Use default depth
            
            if success:
                LAST_REFRESH_TIME = time.time()
                log(f"✅ Network Refresh updated successfully.")
            else:
                log(f"⚠️ Network Refresh failed or was aborted.")
                
        except Exception as e:
            log(f"❌ Error during triggered network refresh: {e}")
            traceback.print_exc()
        finally:
            REFRESH_LOCK.release()
    else:
        log(f"  [Info] Skipping network refresh after {municipality_name} (Refresh already in progress).")

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

def get_priority_municipalities(conn):
    """
    Ranks municipalities by "data debt":
    1. Count of 'Current Owner' placeholders.
    2. Count of properties with missing cama_site_link.
    3. Count of properties with missing building_photo/image_url.
    4. Count of multi-unit properties with missing unit details.
    """
    query = """
        SELECT 
            property_city,
            (
                COUNT(*) FILTER (WHERE owner = 'Current Owner') * 10 +
                COUNT(*) FILTER (WHERE cama_site_link IS NULL OR cama_site_link = '') * 5 +
                COUNT(*) FILTER (WHERE (building_photo IS NULL OR building_photo = '') AND (image_url IS NULL OR image_url = '')) * 2 +
                COUNT(*) FILTER (WHERE number_of_units > 1 AND (unit IS NULL OR unit = '')) * 15
            ) as priority_score,
            COUNT(*) FILTER (WHERE owner = 'Current Owner') as current_owner_count,
            COUNT(*) FILTER (WHERE (building_photo IS NULL OR building_photo = '') AND (image_url IS NULL OR image_url = '')) as missing_photos
        FROM properties
        WHERE property_city IS NOT NULL
        GROUP BY property_city
        HAVING (
            COUNT(*) FILTER (WHERE owner = 'Current Owner') > 0 OR
            COUNT(*) FILTER (WHERE cama_site_link IS NULL OR cama_site_link = '') > 0 OR
            COUNT(*) FILTER (WHERE (building_photo IS NULL OR building_photo = '') AND (image_url IS NULL OR image_url = '')) > 0 OR
            COUNT(*) FILTER (WHERE number_of_units > 1 AND (unit IS NULL OR unit = '')) > 0
        )
        ORDER BY priority_score DESC
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        log(f"Ranked {len(results)} municipalities by data debt (priority score).")
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
                
            if not loc: continue
            
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

            # Fallback: Inference for missing unit
            if 'unit' not in p_data or not p_data['unit']:
                import re
                m = re.search(r'\s([A-Z]|\d{1,4})$', loc)
                if m:
                    p_data['unit'] = m.group(1)

            if norm_addr not in processed_data: processed_data[norm_addr] = []
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
        photo_img = soup.find('img', src=re.compile(r'/photos/|/images/prop', re.I))
    
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
        
        resp = requests_get_with_retries(target_url, session=session, timeout=30)
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


def process_municipality_with_mapxpress(conn, municipality_name, data_source_config, current_owner_only=False, force_process=False, max_workers=5):
    """Process a municipality scraping MapXpress (Parallel)."""
    
    log(f"--- Processing municipality: {municipality_name} via MapXpress (Force={force_process}) ---", municipality=municipality_name)
    
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
        query += " AND p.owner ILIKE '%Current Owner%'"
        
    query += " ORDER BY p.location" 
    
    with conn.cursor() as cursor:
        cursor.execute(query, (municipality_name,))
        properties = cursor.fetchall()
        
    if not properties:
        log(f"No properties found with account_number for {municipality_name}.", municipality=municipality_name)
        return 0
        
    log(f"Found {len(properties)} properties to scrape for {municipality_name}. Starting parallel scrape (10 threads)...", municipality=municipality_name)
    
    domain = data_source_config['domain']
    if domain.endswith('/'): domain = domain[:-1]
    base_url_template = f"https://{domain}/PAGES/detail.asp?UNIQUE_ID={{}}"

    updated_count = 0
    processed_count = 0
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    # Parallel Processing using ThreadPoolExecutor
    # We use threads for I/O (scraping), but update DB in the main thread to ensure safety.
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
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
        'Unit:': 'unit'
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
                
    return data

def scrape_propertyrecordcards_property(session, base_url_template, row):
    """Worker function to scrape a single PropertyRecordCards property."""
    import time
    import random
    
    prop_id, location, account_num, serial_num, current_link = row
    unique_id = account_num if account_num else serial_num
    
    if not unique_id:
        return prop_id, None, None # Cannot scrape w/o ID
        
    target_url = base_url_template.format(unique_id)
    
    try:
        # Respectful jitter
        time.sleep(random.uniform(0.1, 0.5))
        
        resp = requests_get_with_retries(target_url, session=session, timeout=30)
        
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

def process_municipality_with_propertyrecordcards(conn, municipality_name, data_source_config, current_owner_only=False, force_process=False, max_workers=5):
    """Process a municipality using PropertyRecordCards (Parallel)."""
    
    log(f"--- Processing municipality: {municipality_name} via PropertyRecordCards (Force={force_process}) ---", municipality=municipality_name)
    
    # 1. Get Properties (same logic as MapXpress)
    # 1. Get Properties (same logic as MapXpress)
    query = """
        SELECT p.id, p.location, p.account_number, p.serial_number, p.cama_site_link 
        FROM properties p
        LEFT JOIN property_processing_log ppl ON p.id = ppl.property_id
        WHERE p.property_city ILIKE %s 
        AND (p.account_number IS NOT NULL AND p.account_number != '')
    """
    if not force_process:
        query += " AND (ppl.last_processed_date IS NULL OR ppl.last_processed_date < CURRENT_DATE)"
    if current_owner_only:
        query += " AND p.owner ILIKE '%Current Owner%'"
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
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    # 3. Parallel Execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_prop = {
            executor.submit(scrape_propertyrecordcards_property, session, base_url_template, row): row 
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
        response = requests_get_with_retries(prop_page_url, session=session, headers=headers, timeout=30)
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
            response = requests_get_with_retries(street_link, session=session, headers={'Referer': referer} if referer else {}, timeout=30)
        else:
            with requests.Session() as s:
                s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
                response = requests_get_with_retries(street_link, session=s, timeout=30)
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

def scrape_all_properties_by_address(municipality_url, municipality_name, max_workers=MAX_WORKERS):
    """Orchestrates the scraping of all properties for a municipality."""
    all_props_data = {}
    street_list_base_url = f"{municipality_url}Streets.aspx"
    
    with requests.Session() as main_session:
        main_session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        try:
            log(f"  -> Fetching street index for {municipality_name}...")
            response = requests_get_with_retries(street_list_base_url, session=main_session, timeout=30)
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
                with requests.Session() as s:
                    s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
                    resp = requests_get_with_retries(letter_link, session=s, timeout=30, headers={'Referer': street_list_base_url})
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
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
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
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

# --- Database & Matching Functions ---
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
            # Check for value change specifically for protected fields if not restricted
            if key in PROTECTED_FIELDS and not is_placeholder and not restricted_mode:
                if str(current_val).strip() == str(new_value).strip():
                    should_update = False
                else:
                    log(f"Updating {key} from '{current_val}' to '{new_value}' (owner change detected)", municipality=municipality_name)
        
        if should_update:
            log(f"Planning to update field {key} to {new_value} (current: {current_val})", municipality=municipality_name)
            update_fields.append(sql.SQL("{} = %s").format(sql.Identifier(key)))
            values.append(new_value)
        else:
            # print(f"DEBUG DB: Skipping field {key} because should_update is False", flush=True)
            pass
    
    if not update_fields:
        # print(f"DEBUG DB: No fields to update for property {property_db_id}", flush=True)
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

def process_municipality_with_realtime_updates(conn, municipality_name, municipality_url, last_updated_date=None, current_owner_only=False, force_process=False, max_workers=None):
    """
    Main orchestration logic for vision appraisal municipalities.
    """
    if max_workers is None:
        max_workers = MAX_WORKERS
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

        with requests.Session() as session:
            session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_id = {
                    executor.submit(scrape_individual_property_page, prop_url, session, referer_url): prop_id
                    for prop_id, prop_url in props_with_urls
                }
                
                for future in concurrent.futures.as_completed(future_to_id):
                    prop_id = future_to_id[future]
                    vision_data = future.result()
                    if vision_data:
                        # Ensure the URL is stored in the data dict
                        vision_data['cama_site_link'] = prop_url
                        if update_property_in_db(conn, prop_id, vision_data, restricted_mode=restricted_mode, municipality_name=municipality_name):
                            group1_updated_count += 1
                    
                    group1_processed_count += 1 # <--- INCREMENT COUNTER
                    # --- NEW LOGGING LINE ---
                    if group1_processed_count % 100 == 0:
                        log(f"    -> FAST PATH progress for {municipality_name}: Processed {group1_processed_count}/{len(props_with_urls)}, Updated {group1_updated_count} so far...")

        
        log(f"  -> FAST PATH complete. Updated {group1_updated_count} properties.")
        total_updated_count += group1_updated_count

    # --- GROUP 2: Process properties we need to find via full scrape (SLOW PATH / POPULATION PATH) ---
    if props_without_urls:
        log(f"  -> Starting SLOW PATH: Full street scrape required for {len(props_without_urls)} properties...")
        
        # This scrape function now returns data dicts that include 'cama_site_link'
        scraped_properties = scrape_all_properties_by_address(municipality_url, municipality_name, max_workers=max_workers)
        
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

    log(f"Finished {municipality_name}. Total Updated: {total_updated_count} of {len(all_processed_ids)} properties.")
    return total_updated_count


# --- NEW PARALLEL WORKER FUNCTIONS ---

def process_municipality_task(city_name, city_data, current_owner_only, force_process, max_workers=5):
    """
    Worker task for processing a single municipality. 
    This function creates its OWN database connection to ensure thread safety.
    """
    log(f"WORKER_START: Starting job for {city_name} (max_workers={max_workers})")
    conn = None
    try:
        # Each thread MUST create its own connection
        conn = get_db_connection()
        
        # Check if this municipality has a custom data source configuration
        if city_name in MUNICIPAL_DATA_SOURCES:
            log(f"Using custom data source for {city_name}: {MUNICIPAL_DATA_SOURCES[city_name]['type']}")
            
            if MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'arcgis_csv':
                updated_count = process_municipality_with_arcgis(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'MAPXPRESS':
                updated_count = process_municipality_with_mapxpress(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process, max_workers=max_workers
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'PROPERTYRECORDCARDS':
                updated_count = process_municipality_with_propertyrecordcards(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process, max_workers=max_workers
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'ct_geodata_csv':
                updated_count = process_municipality_with_ct_geodata(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name], current_owner_only, force_process
                )
            elif MUNICIPAL_DATA_SOURCES[city_name]['type'] == 'vision_appraisal':
                updated_count = process_municipality_with_realtime_updates(
                    conn, city_name, MUNICIPAL_DATA_SOURCES[city_name]['url'], last_updated_date=None, current_owner_only=current_owner_only, force_process=force_process, max_workers=max_workers
                )
            else:
                log(f"Unknown data source type for {city_name}: {MUNICIPAL_DATA_SOURCES[city_name]['type']}")
                updated_count = 0
        else:
            # Use traditional Vision Appraisal scraping
            updated_count = process_municipality_with_realtime_updates(
                conn, city_name, city_data['url'], last_updated_date=city_data.get('last_updated'), current_owner_only=current_owner_only, force_process=force_process, max_workers=max_workers
            )
        
        log(f"WORKER_DONE: Finished job for {city_name}. Updated {updated_count} properties.")
        
        # --- Update Data Freshness Report ---
        if update_status:
            try:
                # Determine source type for the report
                if city_name in MUNICIPAL_DATA_SOURCES:
                    stype = MUNICIPAL_DATA_SOURCES[city_name]['type']
                else:
                    stype = 'VISION' # Default for Vision Appraisal
                
                # Normalizing type labels to match report expectations
                type_map = {
                    'vision_appraisal': 'VISION',
                    'arcgis_csv': 'ARCGIS',
                    'MAPXPRESS': 'MAPXPRESS',
                    'PROPERTYRECORDCARDS': 'PROPERTYRECORDCARDS',
                    'ct_geodata_csv': 'ARCGIS'
                }
                report_type = type_map.get(stype, stype)
                
                # In this context, external_date is 'today' since we just finished a refresh
                # details includes property count
                update_status(
                    conn, 
                    city_name, 
                    report_type, 
                    date.today(), 
                    {"updated_properties": updated_count, "trigger": "background_updater"}
                )
                log(f"Synced freshness status for {city_name} ({report_type}).")
            except Exception as e:
                log(f"Failed to sync freshness status for {city_name}: {e}")

        # --- Trigger Safe Network Refresh ---
        try:
             trigger_network_refresh(city_name)
        except Exception as e:
             log(f"Failed to trigger network refresh for {city_name}: {e}")

        return updated_count
    
    except Exception as e:
        log(f"!!! WORKER_ERROR: Critical error processing {city_name}: {e}")
        log(f"Traceback for {city_name}: {traceback.format_exc()}")
        return 0  # Return 0 updates on failure for this town
    
    finally:
        if conn:
            conn.close()
            log(f"WORKER_CLEANUP: Closed DB connection for {city_name}.")

def process_service_group(service_name, municipalities, current_owner_only, force_process, muni_concurrency, inner_concurrency):
    """
    Manages a pool of workers for a specific service type (e.g., Vision, MapXpress).
    Running in its own thread to allow independent parallel execution.
    """
    log(f"SERVICE_SCHEDULER: Starting group '{service_name}' with {len(municipalities)} munis (Muni Limit: {muni_concurrency}, Inner Threads: {inner_concurrency})")
    
    updated_counts = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=muni_concurrency) as executor:
        future_to_city = {
            executor.submit(process_municipality_task, city_name, city_data, current_owner_only, force_process, inner_concurrency): city_name
            for city_name, city_data in municipalities
        }
        
        for future in concurrent.futures.as_completed(future_to_city):
             city_name = future_to_city[future]
             try:
                 count = future.result()
                 updated_counts.append(count)
             except Exception as e:
                 log(f"SERVICE_ERROR: {service_name} group failed on {city_name}: {e}")

    total_updated = sum(updated_counts)
    log(f"SERVICE_COMPLETE: Finished group '{service_name}'. Total updated: {total_updated}")
    return total_updated


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
    parser.add_argument('--priority', '-P', action='store_true',
                       help='Prioritize municipalities by data debt (missing units, photos, etc.)')
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
            if not current_owner_municipalities:
                log("No municipalities found with 'Current Owner' properties.")
                return
            
            log("Found municipalities with 'Current Owner' properties:")
            for city, count in current_owner_municipalities:
                log(f"  - {city}: {count} properties")
            log("")
            
            # Match this list against the available municipality list to get URLs/configs
            for city, count in current_owner_municipalities:
                city_upper = city.upper() if city else ""
                if city_upper in all_municipalities_from_vision:
                    municipalities_to_check[city_upper] = all_municipalities_from_vision[city_upper]
                    municipalities_to_check[city_upper]['current_owner_count'] = count
                else:
                    log(f"Warning: '{city}' (from DB) not found in available municipality list.")
            
        elif args.priority:
            # Priority mode: Rank by data debt
            log("MODE: Prioritizing by Data Debt (Missing photos, units, links)")
            priority_list = get_priority_municipalities(conn)
            
            for city, score, co_count, photo_count in priority_list:
                city_upper = city.upper() if city else ""
                if city_upper in all_municipalities_from_vision:
                    municipalities_to_check[city_upper] = all_municipalities_from_vision[city_upper]
                    municipalities_to_check[city_upper]['priority_score'] = score
                    municipalities_to_check[city_upper]['current_owner_count'] = co_count
                else:
                    # Check if it's a known vision town even if not in our custom data sources
                    # Actually all_municipalities_from_vision already includes them.
                    pass
            
            log(f"Found {len(municipalities_to_check)} municipalities to enrich based on priority.")

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
        elif args.priority:
            # Sort by priority score (descending)
            municipality_list.sort(key=lambda x: x[1].get('priority_score', 0), reverse=True)
            for name, data in municipality_list:
                data_type = data.get('type', 'vision_appraisal')
                log(f"  - (Data Debt) {name} (Score: {data.get('priority_score', 0)}) [{data_type}]")
        else:
            # Sort alphabetically
            municipality_list.sort()
            for name, data in municipality_list:
                data_type = data.get('type', 'vision_appraisal')
                log(f"  - {name} (updated: {data['last_updated'].strftime('%Y-%m-%d')}) [{data_type}]")
        log("---------------------------------------------------\n")

        # --- 2. CLOSE SETUP CONNECTION ---
        # We are done with the main connection. The workers will make their own.
        conn.close()
        conn = None 
        log("Setup complete. Closed main DB connection. Starting worker pool.")

        # --- 3. EXECUTION PHASE: Process the queue in parallel service groups ---
        
        # Group municipalities by service type
        service_groups = {}
        for name, data in municipality_list:
            # Determine type
            if name in MUNICIPAL_DATA_SOURCES:
                svc_type = MUNICIPAL_DATA_SOURCES[name]['type']
            else:
                svc_type = 'vision_appraisal'
                
            if svc_type not in service_groups:
                service_groups[svc_type] = []
            service_groups[svc_type].append((name, data))
            
        # Define limits per service
        # Total concurrent DB connections =~ Sum(muni_limit)
        service_config = {
            'vision_appraisal': {'muni_limit': 4, 'inner_limit': 8},
            'MAPXPRESS': {'muni_limit': 3, 'inner_limit': 5},
            'PROPERTYRECORDCARDS': {'muni_limit': 3, 'inner_limit': 5},
            'default': {'muni_limit': 2, 'inner_limit': 4}
        }
        
        # Use ThreadPoolExecutor to run SERVICE MANAGERS in parallel
        # We want all services to run simultaneously
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(service_groups)) as service_executor:
            future_to_service = {}
            for svc_type, munis in service_groups.items():
                config = service_config.get(svc_type, service_config['default'])
                
                # If command line arg specified a lower limit, respect it? 
                # Actually user asked for independent limits. We stick to config but clamp if arg is extremely low (debugging).
                # Let's just use the config for now as it's tuned for independent limits.
                
                future = service_executor.submit(
                    process_service_group, 
                    svc_type, 
                    munis, 
                    args.current_owner_only, 
                    args.force, 
                    config['muni_limit'], 
                    config['inner_limit']
                )
                future_to_service[future] = svc_type
                
            for future in concurrent.futures.as_completed(future_to_service):
                svc = future_to_service[future]
                try:
                    res = future.result()
                    total_updated += res
                except Exception as e:
                    log(f"FATAL ERROR managing service group {svc}: {e}")

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

if __name__ == "__main__":
    main()