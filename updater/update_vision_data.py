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
import argparse
import traceback
import pandas as pd
from io import StringIO

# Suppress only the single InsecureRequestWarning from urllib3 needed for this script
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
VISION_BASE_URL = "https://www.vgsi.com"
CONNECTICUT_DATABASE_URL = f"{VISION_BASE_URL}/connecticut-online-database/"
MAX_WORKERS = 6  # Workers for streets *within* one municipality OR for direct URL scraping
DEFAULT_MUNI_WORKERS = 4 # How many municipalities to process in parallel

# --- Municipality-specific data sources ---
MUNICIPAL_DATA_SOURCES = {
# update_vision_data.py

# ... inside MUNICIPAL_DATA_SOURCES dictionary ...
    "HARTFORD": {
        "type": "arcgis_csv",
        "url": "https://hub.arcgis.com/api/v3/datasets/8b4937b538f14e838c08ed86838c493b_42/downloads/data?format=csv&spatialRefId=2234&where=1%3D1",
        "column_mapping": {
            "OwnerFullName": "owner",
            "LastSalePrice": "sale_amount", 
            "LastSaleDate": "sale_date",
            "TotApprsdValue": "assessed_value",
            "TotApprsdValue": "appraised_value",  # Hartford doesn't distinguish, use same field
            "StreetNumberFrom": "address_number", # <-- ADD THIS LINE
            # "StreetName": "location", # <-- REMOVE OR COMMENT OUT THIS LINE
            "AccountNumber": "account_number",
            "PARCELNUMBER": "serial_number",
            "YearBuilt": "year_built",
            "TotAcreage": "acres",
            "LUCDescription": "property_type",
            "TotFinishdArea": "living_area",
            "Zip10": "property_zip",
            "LivingUnits": "number_of_units"
        }
    }
# ...
    # Add more municipalities here as needed
}

# --- Logging ---
def log(message):
    """Prints a message with a timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")
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
    """Processes ArcGIS DataFrame and returns a dictionary mapping addresses to property data."""
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
            
            # --- START OF CHANGE ---
            # Set the location directly from the constructed full address
            if full_address.strip():
                property_data['location'] = full_address.strip()
            # --- END OF CHANGE ---

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
            
            # Only store if we have meaningful data
            if len(property_data) >= 2:  # At least owner and one other field
                processed_data[normalized_address] = property_data
                
        except Exception as e:
            log(f"Error processing row: {e}")
            continue
    
    log(f"Successfully processed {len(processed_data)} address records for {municipality_name}")
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
    
    # Match database properties with ArcGIS data
    log(f"Matching {len(db_properties)} DB properties against {len(processed_arcgis_data)} ArcGIS records...")
    
    updated_count = 0
    processed_count = 0
    all_property_ids = []
    
    for prop_id, prop_location, prop_url in db_properties:
        all_property_ids.append(prop_id)
        processed_count += 1
        
        if prop_location:
            normalized_db_address = normalize_address_for_matching(prop_location)
            
            # Try to find a match in the ArcGIS data
            matched_data = None
            
            # Direct match
            if normalized_db_address in processed_arcgis_data:
                matched_data = processed_arcgis_data[normalized_db_address]
            else:
                # Fuzzy matching - check if DB address contains or is contained in ArcGIS addresses
                for arcgis_addr, arcgis_data in processed_arcgis_data.items():
                    if (normalized_db_address in arcgis_addr or arcgis_addr in normalized_db_address):
                        matched_data = arcgis_data
                        break
            
            if matched_data:
                if update_property_in_db(conn, prop_id, matched_data):
                    updated_count += 1
        
        if processed_count % 100 == 0:
            log(f"  -> Progress: {processed_count}/{len(db_properties)}, updated {updated_count} so far...")
    
    # Mark all properties as processed
    log(f"Marking all {len(all_property_ids)} properties as processed for today.")
    for prop_id in all_property_ids:
        mark_property_processed_today(conn, prop_id)
    
    log(f"Finished {municipality_name}. Updated {updated_count} of {len(db_properties)} properties.")
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
    try:
        time.sleep(0.4)  # Respectful delay
        headers = {
            'Referer': referer,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = session.get(prop_page_url, verify=False, timeout=20, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        data = {}
        
        # Vision Appraisal specific selectors (primary strategy)
        selectors = {
            'owner': ['span[id*="lblGenOwner"]', 'span[id*="lblOwner"]'],
            'sale_amount': ['span[id*="lblPrice"]'],
            'sale_date': ['span[id*="lblSaleDate"]'],
            'assessed_value': ['span[id*="lblGenAssessment"]', 'span#MainContent_lblGenAssessment'],
            'appraised_value': ['span[id*="lblGenAppraisal"]', 'span#MainContent_lblGenAppraisal']
        }
        
        for field, field_selectors in selectors.items():
            for selector in field_selectors:
                element = soup.select_one(selector)
                if element and element.text.strip():
                    text = element.text.strip()
                    if field == 'owner':
                        data[field] = text
                    elif field in ['sale_amount', 'assessed_value', 'appraised_value']:
                        cleaned = re.sub(r'[$,]', '', text)
                        if cleaned.replace('.', '', 1).replace('-', '').isdigit():
                            value = float(cleaned)
                            # Only store meaningful monetary values
                            if value > 0:
                                data[field] = value
                    elif field == 'sale_date':
                        try:
                            data[field] = datetime.strptime(text, '%m/%d/%Y').date()
                        except ValueError:
                            pass
                    if field in data:
                        break
        
        # Fallback strategy: Generic table search
        if len(data) < 2:  # If we didn't get much data, try fallback
            fallback_keywords = {
                'owner': 'Owner',
                'sale_amount': 'Sale Price',
                'sale_date': 'Sale Date',
                'assessed_value': 'Assessment',
                'appraised_value': 'Appraisal'
            }

            for field, keyword in fallback_keywords.items():
                if field in data:
                    continue

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
        
        return data if data else None

    except Exception:
        return None

def scrape_street_properties(street_link, municipality_url, referer):
    """Scrapes all properties on a single street page."""
    street_props = {}
    with requests.Session() as session:
        session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        try:
            time.sleep(0.5)
            response = session.get(street_link, verify=False, timeout=20, headers={'Referer': referer})
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            prop_links = soup.select("a[href*='.aspx?pid='], a[href*='.aspx?acct=']")
            for prop_link in prop_links:
                raw_address = prop_link.text.strip()
                # Clean address: remove "Mblu:" and other extraneous data
                address = re.sub(r'\s+Mblu:.*', '', raw_address, flags=re.IGNORECASE).strip()
                address = normalize_address(address)

                href = prop_link.get('href')
                if not href or not address:
                    continue
                
                prop_page_url = urljoin(municipality_url, href)
                
                prop_details = scrape_individual_property_page(prop_page_url, session, street_link)
                if prop_details:
                    # *** KEY ADDITION ***
                    # Add the specific parcel URL to the data dict so it can be saved in the DB
                    prop_details['cama_site_link'] = prop_page_url
                    street_props[address] = prop_details
                        
        except Exception:
            pass
        
    return street_props

def scrape_all_properties_by_address(municipality_url, municipality_name):
    """Orchestrates the scraping of all properties for a municipality."""
    all_props_data = {}
    street_list_base_url = f"{municipality_url}Streets.aspx"
    
    with requests.Session() as main_session:
        main_session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        try:
            log(f"  -> Fetching street index for {municipality_name}...")
            response = main_session.get(street_list_base_url, verify=False, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            letter_links = { urljoin(municipality_url, a['href']) for a in soup.select("a[href^='Streets.aspx?Letter=']") }
            if not letter_links:
                log(f"  -> No A-Z letter links found for {municipality_name}.")
                return {}
        except Exception as e:
            log(f"  -> Failed to get street index for {municipality_name}: {e}")
            return {}

        log(f"  -> Found {len(letter_links)} letter pages to scan for {municipality_name}.")

        for letter_link in sorted(list(letter_links)):
            letter = re.search(r'Letter=([A-Z])', letter_link).group(1) if re.search(r'Letter=([A-Z])', letter_link) else '?'
            
            try:
                time.sleep(0.5)
                response = main_session.get(letter_link, verify=False, timeout=20, headers={'Referer': street_list_base_url})
                soup = BeautifulSoup(response.content, 'html.parser')
                street_links = [ urljoin(municipality_url, a['href']) for a in soup.select("a[href^='Streets.aspx?Name=']") ]
            except Exception as e:
                log(f"  -> Failed to get streets for letter {letter}: {e}")
                continue

            if not street_links: 
                continue
            
            log(f"  ->  Processing {len(street_links)} streets for letter '{letter}'...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_street = {executor.submit(scrape_street_properties, link, municipality_url, letter_link): link for link in street_links}
                for future in concurrent.futures.as_completed(future_to_street):
                    street_data = future.result()
                    if street_data:
                        all_props_data.update(street_data)

    log(f"  -> Scraped {len(all_props_data)} properties total for {municipality_name}.")
    return all_props_data

# --- Database & Matching Functions ---
def update_property_in_db(conn, property_db_id, vision_data):
    """Updates a property record in the database with new information."""
    if not vision_data or not any(v is not None for v in vision_data.values()): 
        return False

    update_fields = []
    values = []
    
    # Dynamically build update query from the vision_data dict keys
    # This now automatically handles 'cama_site_link' if it's in the dict
    for key, value in vision_data.items():
        if value is not None:
            # Assume key is a valid column name (like 'owner', 'sale_date', 'cama_site_link')
            update_fields.append(sql.SQL(f"{key} = %s"))
            values.append(value)
    
    if not update_fields: 
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

def process_municipality_with_realtime_updates(conn, municipality_name, municipality_url, current_owner_only=False, force_process=False):
    """
    Process a municipality with real-time database updates, resumability, and direct URL optimization.
    """
    log(f"--- Processing municipality: {municipality_name} (Force={force_process}) ---")
    
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
        if prop_url:
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
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_id = {
                    executor.submit(scrape_individual_property_page, prop_url, session, referer_url): prop_id
                    for prop_id, prop_url in props_with_urls
                }
                
                for future in concurrent.futures.as_completed(future_to_id):
                    prop_id = future_to_id[future]
                    vision_data = future.result()
                    if vision_data:
                        if update_property_in_db(conn, prop_id, vision_data):
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
        scraped_properties = scrape_all_properties_by_address(municipality_url, municipality_name)
        
        if not scraped_properties:
            log(f"  -> Full scrape returned no data. Skipping {len(props_without_urls)} properties for {municipality_name}.")
        else:
            log(f"  -> SLOW PATH: Matching {len(props_without_urls)} DB properties against {len(scraped_properties)} scraped results...")
            group2_updated_count = 0
            processed_in_group = 0
            
            for prop_db_id, prop_address in props_without_urls:
                vision_data = find_match_for_property(prop_address, scraped_properties)
                
                if vision_data:
                    # This update will save owner, sales, AND the new 'cama_site_link'
                    if update_property_in_db(conn, prop_db_id, vision_data):
                        group2_updated_count += 1
                
                processed_in_group += 1
                if processed_in_group % 100 == 0:
                    log(f"    -> Matched {processed_in_group}/{len(props_without_urls)}, updated {group2_updated_count} so far...")

            log(f"  -> SLOW PATH complete. Updated {group2_updated_count} properties (and populated their URLs).")
            total_updated_count += group2_updated_count

    # --- FINALIZE: Mark all items from the original queue as processed ---
    # We do this even if 'force' is on, to ensure the 'last_processed_date' is always today's date.
    log(f"  -> Finalizing: Marking all {len(all_processed_ids)} properties as processed for today.")
    for prop_id in all_processed_ids:
        mark_property_processed_today(conn, prop_id)

    log(f"Finished {municipality_name}. Total Updated: {total_updated_count} of {len(all_processed_ids)} properties.")
    return total_updated_count


# --- NEW PARALLEL WORKER FUNCTION ---

def process_municipality_task(city_name, city_data, current_owner_only, force_process):
    """
    Worker task for processing a single municipality. 
    This function creates its OWN database connection to ensure thread safety.
    """
    log(f"WORKER_START: Starting job for {city_name}")
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
            else:
                log(f"Unknown data source type for {city_name}: {MUNICIPAL_DATA_SOURCES[city_name]['type']}")
                updated_count = 0
        else:
            # Use traditional Vision Appraisal scraping
            updated_count = process_municipality_with_realtime_updates(
                conn, city_name, city_data['url'], current_owner_only, force_process
            )
        
        log(f"WORKER_DONE: Finished job for {city_name}. Updated {updated_count} properties.")
        return updated_count
    
    except Exception as e:
        log(f"!!! WORKER_ERROR: Critical error processing {city_name}: {e}")
        log(f"Traceback for {city_name}: {traceback.format_exc()}")
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