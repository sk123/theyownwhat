
import os
import re
import psycopg2
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import urllib3

# Suppress warnings for insecure requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

DATABASE_URL = os.environ.get("DATABASE_URL")
VISION_SCHEDULE_URL = "https://www.vgsi.com/connecticut-online-database/"

# Delayed import to ensure sys.path is set
def get_muni_configs():
    from updater.update_vision_data import MUNICIPAL_DATA_SOURCES
    return MUNICIPAL_DATA_SOURCES

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def update_status(conn, source_name, source_type, external_date, details=None):
    query = """
    INSERT INTO data_source_status (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
    VALUES (%s, %s, %s, NOW(), 'SUCCESS', %s)
    ON CONFLICT (source_name) DO UPDATE SET
        external_last_updated = EXCLUDED.external_last_updated,
        last_refreshed_at = EXCLUDED.last_refreshed_at,
        refresh_status = EXCLUDED.refresh_status,
        details = EXCLUDED.details;
    """
    with conn.cursor() as cur:
        cur.execute(query, (source_name, source_type, external_date, json.dumps(details) if details else None))
    conn.commit()

def sync_vision_dates(conn):
    print("Syncing Vision Appraisal dates...")
    try:
        resp = requests.get(VISION_SCHEDULE_URL, verify=False, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        
        # Find the table following the "Last Updated" header
        # Based on previous exploration, it's a table with rows containing city names and dates
        rows = soup.find_all("tr")
        found_count = 0
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 3:
                name_col = cols[0].get_text(strip=True)
                date_col = cols[2].get_text(strip=True)
                
                # Check if it looks like a city row (e.g. "Ansonia, CT")
                if ", CT" in name_col and re.match(r'\d{2}/\d{2}/\d{4}', date_col):
                    city_name = name_col.replace(", CT", "").strip().upper()
                    try:
                        update_date = datetime.strptime(date_col, "%m/%d/%Y").date()
                        update_status(conn, city_name, "VISION", update_date, {"url_found": cols[0].find("a")["href"] if cols[0].find("a") else None})
                        found_count += 1
                    except Exception as e:
                        print(f"Error parsing date for {city_name}: {e}")
        
        print(f"Synced {found_count} Vision municipalities.")
    except Exception as e:
        print(f"Error syncing Vision dates: {e}")
        import traceback
        traceback.print_exc()

def sync_ct_geodata_dates(conn):
    print("Syncing CT Geodata (ArcGIS) dates...")
    muni_configs = get_muni_configs()
    
    geodata_configs = {k: v for k, v in muni_configs.items() if v.get('type') == 'ct_geodata_csv'}
    
    checked_urls = {}
    
    for muni, cfg in geodata_configs.items():
        url = cfg.get('url')
        if not url: continue
        
        if url in checked_urls:
            external_date = checked_urls[url]
        else:
            try:
                # Use HEAD request to check Last-Modified header
                h = requests.head(url, verify=False, timeout=10)
                last_mod = h.headers.get('Last-Modified')
                if last_mod:
                    # Parse rfc1123 date: Wed, 21 Oct 2015 07:28:00 GMT
                    external_date = datetime.strptime(last_mod, "%a, %d %b %Y %H:%M:%S %Z").date()
                else:
                    external_date = datetime.now().date() # Fallback
                checked_urls[url] = external_date
            except Exception as e:
                print(f"Error checking {url}: {e}")
                external_date = datetime.now().date()
        
        update_status(conn, muni, "ARCGIS", external_date, {"url": url})

def sync_other_source_dates(conn):
    """Placeholders for MapXpress and PRC sources which don't have a central index."""
    print("Recording status for MapXpress and PRC sources...")
    muni_configs = get_muni_configs()
    
    # We'll just mark them as 'UNKNOWN' external date for now, requiring periodic refresh
    # For these sources, we rely on a 30-day stale check in the nightly_sync_worker.py
    other_types = ['MAPXPRESS', 'PROPERTYRECORDCARDS']
    count = 0
    for muni, cfg in muni_configs.items():
        stype = cfg.get('type')
        if stype in other_types:
            # We record today's date in details to show we checked it, 
            # but external_date is None because we can't reliably detect it.
            update_status(conn, muni, stype, None, {"config": cfg, "note": "External date not detectable; using stale-check logic."})
            count += 1
    print(f"Updated {count} MapXpress/PRC sources.")

def sync_business_dates(conn):
    print("Syncing Business Registry dates...")
    # These are expected nightly, so we'll just record today's date if they are available
    # For now, we'll mark them as "Daily" updates
    today = datetime.now().date()
    update_status(conn, "BUSINESSES", "BUSINESS_REGISTRY", today)
    update_status(conn, "PRINCIPALS", "BUSINESS_REGISTRY", today)

def main():
    conn = get_db_connection()
    try:
        sync_vision_dates(conn)
        sync_ct_geodata_dates(conn)
        sync_other_source_dates(conn)
        sync_business_dates(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
