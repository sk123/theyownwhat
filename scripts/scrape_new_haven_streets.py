
import sys
import os
import re
import time
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

MUNICIPALITY_NAME = 'New Haven'
BASE_URL = 'https://gis.vgsi.com/newhavenct/'

def get_street_index(session):
    """Fetches all street links from A-Z pages."""
    letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    street_links = []
    
    log(f"Fetching Street Index for {MUNICIPALITY_NAME}...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_letter = {
            executor.submit(fetch_letter_page, session, letter): letter 
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

def fetch_letter_page(session, letter):
    """Fetches a single letter page and returns street links."""
    url = f"{BASE_URL}Streets.aspx?Letter={letter}"
    headers = {'Referer': f"{BASE_URL}Streets.aspx"}
    response = session.get(url, headers=headers, verify=False, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    
    links = []
    # Find all links with Streets.aspx?Name=
    for a in soup.find_all('a', href=re.compile(r'Streets\.aspx\?Name=', re.I)):
        href = a['href'].strip()
        full_url = urljoin(BASE_URL, href)
        links.append(full_url)
        
    return links

def get_properties_on_street(session, street_url):
    """Fetches all property links on a street page."""
    try:
        response = session.get(street_url, verify=False, timeout=30)
        # 404s or 500s on specific streets shouldn't kill the whole process
        if response.status_code != 200:
            log(f"  -> Warning: Street {street_url} returned {response.status_code}")
            return []
            
        soup = BeautifulSoup(response.content, 'html.parser')
        prop_links = []
        
        # Find all Parcel.aspx?pid= links
        for a in soup.find_all('a', href=re.compile(r'\.aspx\?pid=', re.I)):
            href = a['href'].strip()
            # Skip mailto or other garbage if any
            if 'mailto:' in href: continue
            
            full_url = urljoin(BASE_URL, href)
            
            # Extract PID for logging/deduping
            pid_match = re.search(r'pid=(\d+)', href, re.I)
            pid = pid_match.group(1) if pid_match else 'unknown'
            
            prop_links.append((pid, full_url))
            
        return prop_links
    except Exception as e:
        log(f"Error scraping street {street_url}: {e}")
        return []

def worker_process_property(conn, session, pid, url, street_url):
    """Scrapes and updates a single property."""
    try:
        # Scrape
        data = scrape_individual_property_page(url, session, street_url, municipality=MUNICIPALITY_NAME)
        
        if not data:
            return False
            
        # Ensure we have a location to link to DB
        location = data.get('location')
        if not location:
            log(f"Skipping PID {pid}: No location found in scraped data.")
            return False
            
        # Add URL to data
        data['cama_site_link'] = url
        
        # Get/Create DB ID
        # use local robust version
        db_id = get_safe_property_id(conn, MUNICIPALITY_NAME, location)
        
        if not db_id:
            log(f"Failed to get DB ID for {location}")
            return False
            
        # Update
        # restricted_mode=False because we want to force fill everything
        success = update_property_in_db(conn, db_id, data, restricted_mode=False, municipality_name=MUNICIPALITY_NAME)
        return success

    except Exception as e:
        log(f"Error processing PID {pid}: {e}")
        return False

def get_safe_property_id(conn, city, address):
    """Robust get_or_create that handles race conditions."""
    city = city.upper().strip()
    address = address.upper().strip()
    
    with conn.cursor() as cur:
        # 1. Try Select
        cur.execute("SELECT id FROM properties WHERE property_city = %s AND location = %s", (city, address))
        row = cur.fetchone()
        if row:
            return row[0]
            
        # 2. Try Insert with On Conflict
        try:
            # We can't use ON CONFLICT DO NOTHING RETURNING id easily because it returns nothing on conflict.
            # So we try insert, if it fails, we select again.
            cur.execute("INSERT INTO properties (property_city, location, source) VALUES (%s, %s, 'new_haven_backfill') RETURNING id", (city, address))
            return cur.fetchone()[0]
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            # 3. Select Again (raced)
            cur.execute("SELECT id FROM properties WHERE property_city = %s AND location = %s", (city, address))
            row = cur.fetchone()
            if row:
                return row[0]
            else:
                # Should not happen unless deleted
                log(f"Critical: Failed to find property {address} after UniqueViolation.")
                return None
        except Exception as e:
            conn.rollback()
            log(f"Error in get_safe_property_id: {e}")
            raise

def main():
    log(f"Starting Street-by-Street Backfill for {MUNICIPALITY_NAME}")
    
    conn = get_db_connection()
    session = get_session()
    
    # 1. Get all streets
    street_links = get_street_index(session)
    log(f"Total Streets Found: {len(street_links)}")
    
    # 2. Get all properties (Parallel)
    all_properties = [] # List of (pid, url, source_street_url)
    seen_pids = set()
    
    log("Discovering properties on all streets...")
    
    # Chunk streets to avoid OOM
    chunk_size = 50
    for i in range(0, len(street_links), chunk_size):
        chunk = street_links[i:i + chunk_size]
        log(f"Processing street chunk {i}/{len(street_links)}...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_street = {executor.submit(get_properties_on_street, session, url): url for url in chunk}
            
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
                
    log(f"\nTotal Unique Properties Found: {len(all_properties)}")
    
    # 3. Scrape and Update (Parallel)
    log("Starting Property Scrape & Update...")
    
    # We need separate connections for threads if update_property_in_db uses the passed conn?
    # update_property_in_db uses the passed conn. 
    # psycopg2 connections are NOT thread safe.
    # So we should pass a new connection or use a pool, OR just use one connection and serialize writes?
    # update_data.py uses `process_municipality_task` which creates ONE connection, 
    # but then `process_municipality_with_vision` uses a ThreadPool.
    # Wait, `update_property_in_db` is called inside the thread pool in `update_data.py` (line 1957)?
    # NO. `update_data.py` calls `scrape` in thread pool, gets result, THEN calls `update_property_in_db` in the MAIN thread loop (line 1951).
    # "for future in as_completed... update_property_in_db..."
    # So the DB writes are serial in the main thread.
    
    # I should follow that pattern.
    
    success_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit scraping tasks
        future_to_prop = {
            executor.submit(scrape_individual_property_page, url, session, s_url, MUNICIPALITY_NAME): (pid, url) 
            for pid, url, s_url in all_properties
        }
        
        processed = 0
        for future in concurrent.futures.as_completed(future_to_prop):
            pid, url = future_to_prop[future]
            processed += 1
            
            try:
                data = future.result()
                if data:
                    location = data.get('location')
                    if location:
                        # Serial DB update
                        data['cama_site_link'] = url
                        db_id = get_or_create_property_id(conn, MUNICIPALITY_NAME, location)
                        if db_id:
                            if update_property_in_db(conn, db_id, data, restricted_mode=False, municipality_name=MUNICIPALITY_NAME):
                                success_count += 1
                
                if processed % 100 == 0:
                    print(f"  -> Property Progress: {processed}/{len(all_properties)}. Updated: {success_count}", end='\r')
                    
            except Exception as e:
                log(f"Error processing {url}: {e}")

    log(f"\nJob Complete. Successfully updated {success_count} properties.")
    conn.close()

if __name__ == "__main__":
    main()
