import sys
import os
import argparse
import psycopg2

# Add updater to path to import shared logic
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'updater')))
from update_data import get_db_connection, get_session
from api.municipal_config import MUNICIPAL_DATA_SOURCES
from scripts.scrape_vision_streets import get_street_index, get_properties_on_street

def main():
    parser = argparse.ArgumentParser(description='Street-by-Street Gap Audit for Vision Appraisal (VGSI) Municipalities')
    parser.add_argument('--town', required=True, help='Name of the municipality (e.g. BRIDGEPORT)')
    args = parser.parse_args()

    municipality_name = args.town.upper()

    if municipality_name not in MUNICIPAL_DATA_SOURCES:
        print(f"Error: {municipality_name} not found in config.")
        return

    config = MUNICIPAL_DATA_SOURCES[municipality_name]
    if config['type'] != 'vision_appraisal':
        print(f"Error: {municipality_name} is '{config['type']}', not 'vision_appraisal'.")
        return

    base_url = config['url']
    if not base_url.endswith('/'): base_url += '/'

    print(f"Starting Street-by-Street Audit for {municipality_name}")
    print(f"Base URL: {base_url}")

    conn = get_db_connection()
    session = get_session()

    street_links = get_street_index(session, base_url, municipality_name)
    print(f"Found {len(street_links)} streets. Discovering properties...")

    # For audit, just pull all PIDs
    import concurrent.futures
    all_vision_pids = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_street = {executor.submit(get_properties_on_street, session, url, base_url): url for url in street_links}
        for future in concurrent.futures.as_completed(future_to_street):
            try:
                props = future.result()
                for pid, p_url in props:
                    all_vision_pids.add(pid)
            except Exception as e:
                pass

    print(f"Found {len(all_vision_pids)} unique properties on Vision Appraisal.")

    # Get DB count
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM properties WHERE upper(property_city) = %s", (municipality_name,))
        db_count = cur.fetchone()[0]

    print(f"Properties in Database for {municipality_name}: {db_count}")
    print(f"Discrepancy: {len(all_vision_pids) - db_count}")

    if len(all_vision_pids) > db_count:
        print("There are properties on Vision that are NOT in the database.")
        print(f"Run: docker exec ctdata_updater python scripts/scrape_vision_streets.py --town \"{municipality_name}\"")
    
    conn.close()

if __name__ == "__main__":
    main()
