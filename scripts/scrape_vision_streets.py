
import sys
import os
import re
import time
import argparse
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import psycopg2

# Add updater to path to import shared logic
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'updater')))
from update_data import (
    get_db_connection, 
    get_session, 
    scrape_individual_property_page, 
    update_property_in_db, 
    get_or_create_property_id,
    log,
    MAX_WORKERS
)
from api.municipal_config import MUNICIPAL_DATA_SOURCES

def get_street_index(session, base_url, municipality_name):
    """Fetches all street links from A-Z pages."""
    letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    street_links = []
    
    log(f"[{municipality_name}] Fetching Street Index from {base_url}Streets.aspx...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_letter = {
            executor.submit(fetch_letter_page, session, base_url, letter): letter 
            for letter in letters
        }
        
        for future in concurrent.futures.as_completed(future_to_letter):
            letter = future_to_letter[future]
            try:
                links = future.result()
                street_links.extend(links)
                log(f"  -> Letter {letter}: Found {len(links)} streets")
            except Exception as e:
                log(f"  !!! Error fetching letter {letter}: {e}")
                
    return street_links

def fetch_letter_page(session, base_url, letter):
    """Fetches a single letter page and returns street links."""
    # Ensure base_url ends with slash
    if not base_url.endswith('/'):
        base_url += '/'
        
    url = f"{base_url}Streets.aspx?Letter={letter}"
    headers = {'Referer': f"{base_url}Streets.aspx"}
    
    response = session.get(url, headers=headers, verify=False, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    
    links = []
    # Find all links with Streets.aspx?Name=
    for a in soup.find_all('a', href=re.compile(r'Streets\.aspx\?Name=', re.I)):
        href = a['href'].strip()
        full_url = urljoin(base_url, href)
        links.append(full_url)
        
    return links

def get_properties_on_street(session, street_url, base_url):
    """Fetches all property links on a street page."""
    try:
        response = session.get(street_url, verify=False, timeout=30)
        if response.status_code != 200:
            log(f"  -> Warning: Street {street_url} returned {response.status_code}")
            return []
            
        soup = BeautifulSoup(response.content, 'html.parser')
        prop_links = []
        
        # Find all Parcel.aspx?pid= links
        for a in soup.find_all('a', href=re.compile(r'\.aspx\?pid=', re.I)):
            href = a['href'].strip()
            if 'mailto:' in href: continue
            
            full_url = urljoin(base_url, href)
            
            # Extract PID
            pid_match = re.search(r'pid=(\d+)', href, re.I)
            pid = pid_match.group(1) if pid_match else 'unknown'
            
            prop_links.append((pid, full_url))
            
        return prop_links
    except Exception as e:
        log(f"Error scraping street {street_url}: {e}")
        return []

def worker_process_property(conn_str, session, pid, url, street_url, municipality_name):
    """Scrapes and updates a single property. Creates its own DB connection if needed."""
    # Note: Refactored to separate DB logic to main loop or safe handler
    try:
        data = scrape_individual_property_page(url, session, street_url, municipality=municipality_name)
        if not data: return None
        
        location = data.get('location')
        if not location: return None
        
        data['cama_site_link'] = url
        return (location, data)

    except Exception as e:
        log(f"Error processing PID {pid}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Street-by-Street Backfill for Vision Appraisal (VGSI) Municipalities')
    parser.add_argument('--town', required=True, help='Name of the municipality (e.g. BRIDGEPORT)')
    parser.add_argument('--dry-run', action='store_true', help='Scan streets but do not update DB')
    args = parser.parse_args()

    municipality_name = args.town.upper()
    
    # 1. Validate Config
    if municipality_name not in MUNICIPAL_DATA_SOURCES:
        log(f"Error: {municipality_name} not found in municipal_config.py")
        sys.exit(1)
        
    config = MUNICIPAL_DATA_SOURCES[municipality_name]
    if config['type'] != 'vision_appraisal':
        log(f"Error: {municipality_name} is Type '{config['type']}', not 'vision_appraisal'. This script only supports VGSI.")
        # Special case for STAMFORD if it was overridden, but in config it says 'vision_appraisal'
        sys.exit(1)
        
    base_url = config['url']
    if not base_url.endswith('/'): base_url += '/'

    log(f"Starting Street-by-Street Backfill for {municipality_name}")
    log(f"Base URL: {base_url}")
    
    conn = get_db_connection()
    session = get_session()
    
    # 2. Get Street Index
    street_links = get_street_index(session, base_url, municipality_name)
    log(f"Total Streets Found: {len(street_links)}")
    
    if args.dry_run:
        log("Dry run complete. Exiting.")
        return

    # 3. Discover Properties
    all_properties = [] 
    seen_pids = set()
    
    log("Discovering properties on all streets...")
    
    # Chunk streets
    chunk_size = 50
    for i in range(0, len(street_links), chunk_size):
        chunk = street_links[i:i + chunk_size]
        log(f"Processing street chunk {i}/{len(street_links)}...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_street = {executor.submit(get_properties_on_street, session, url, base_url): url for url in chunk}
            
            for future in concurrent.futures.as_completed(future_to_street):
                try:
                    props = future.result()
                    for pid, p_url in props:
                        if pid not in seen_pids:
                            seen_pids.add(pid)
                            all_properties.append((pid, p_url, future_to_street[future]))
                except Exception as e:
                    log(f"Error in street processing: {e}")
    
    log(f"\nTotal Unique Properties Found: {len(all_properties)}")
    
    # 4. Scrape & Update
    log("Starting Property Scrape & Update...")
    
    success_count = 0
    processed = 0
    
    # We will do scrape in parallel, but DB update in serial main thread to avoid concurrency issues with connections
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_prop = {
            executor.submit(scrape_individual_property_page, url, session, s_url, municipality_name): (pid, url) 
            for pid, url, s_url in all_properties
        }
        
        for future in concurrent.futures.as_completed(future_to_prop):
            pid, url = future_to_prop[future]
            processed += 1
            
            try:
                data = future.result()
                if data:
                    location = data.get('location')
                    if location:
                        data['cama_site_link'] = url
                        
                        # Use our robust get_safe_property_id from update_data if it exists, 
                        # otherwise implement the logic here.
                        # update_data has get_or_create_property_id, but we want the safe/retry one.
                        # It's not exported. Let's assume get_or_create_property_id is safe enough or we implement retry here.
                        # actually scrape_new_haven_streets.py defined its own get_safe_property_id. 
                        # We should use that logic.
                        
                        db_id = get_safe_property_id(conn, municipality_name, location)
                        
                        if db_id:
                            if update_property_in_db(conn, db_id, data, restricted_mode=False, municipality_name=municipality_name):
                                success_count += 1
            
            except Exception as e:
                log(f"Error processing {url}: {e}")
                
            if processed % 100 == 0:
                print(f"  -> {municipality_name} Progress: {processed}/{len(all_properties)}. Updated: {success_count}", end='\r')

    log(f"\nJob Complete for {municipality_name}. Successfully updated {success_count} properties.")
    conn.close()

def get_safe_property_id(conn, city, address):
    """Robust get_or_create that handles race conditions."""
    city = city.upper().strip()
    address = address.upper().strip()
    
    with conn.cursor() as cur:
        # 1. Try Select
        cur.execute("SELECT id FROM properties WHERE property_city = %s AND location = %s", (city, address))
        row = cur.fetchone()
        if row: return row[0]
            
        # 2. Try Insert
        try:
            cur.execute("INSERT INTO properties (property_city, location, source) VALUES (%s, %s, 'street_backfill') RETURNING id", (city, address))
            conn.commit()
            return cur.fetchone()[0]
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            # 3. Select Again
            cur.execute("SELECT id FROM properties WHERE property_city = %s AND location = %s", (city, address))
            row = cur.fetchone()
            if row: return row[0]
            return None
        except Exception as e:
            conn.rollback()
            log(f"Error in get_safe_property_id: {e}")
            return None

if __name__ == "__main__":
    main()
