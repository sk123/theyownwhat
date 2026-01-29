
import os
import time
import requests
import psycopg2
from bs4 import BeautifulSoup
import re
import concurrent.futures
import threading

DATABASE_URL = os.environ.get("DATABASE_URL")
BASE_URL = "http://assessor1.hartford.gov"
SEARCH_PAGE = f"{BASE_URL}/search-middle-ns.asp"
RESULTS_PAGE = f"{BASE_URL}/SearchResults.asp"
SUMMARY_URL = f"{BASE_URL}/summary-bottom.asp"

# Session per thread if needed, but requests.Session() is not thread-safe for simultaneous use.
# Better to create a session per task/batch or use a thread-local.
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        # Establish initial session cookies
        thread_local.session.get(SEARCH_PAGE, timeout=15)
    return thread_local.session

def format_parcel_id(pid):
    """Formats 135384211 into 135-384-211."""
    if not pid or len(str(pid)) < 9:
        return pid
    s = str(pid)
    # Handle cases where pid might be 135-384-211 already
    if "-" in s:
        return s
    return f"{s[:3]}-{s[3:6]}-{s[6:]}"

def enrich_property(prop_id, pid, old_loc, conn_params):
    """Enriches a single property record."""
    session = get_session()
    formatted_pid = format_parcel_id(pid)
    # print(f"Processing Prop ID {prop_id}: {old_loc} ({formatted_pid})...")

    try:
        # 1. Search for Parcel to set session
        payload = {
            "SearchParcel": formatted_pid,
            "SearchSubmitted": "yes",
            "cmdGo": "Go"
        }
        res = session.post(RESULTS_PAGE, data=payload, timeout=15)
        
        # 2. Extract Account Number
        soup = BeautifulSoup(res.text, 'lxml')
        links = soup.find_all('a', href=re.compile(r'Summary\.asp\?AccountNumber='))
        
        if not links:
            if "Summary.asp" in res.url:
               link_href = res.url
            else:
               return prop_id, False, "No results found"
        else:
            link_href = links[0]['href']
        
        acc_match = re.search(r'AccountNumber=(\d+)', link_href)
        if not acc_match:
            return prop_id, False, "Could not parse AccountNumber"
        
        acc_num = acc_match.group(1)
        
        # 3. Hit Summary to ensure session context
        session.get(f"{BASE_URL}/Summary.asp?AccountNumber={acc_num}", timeout=15)
        
        # 4. Fetch Summary Bottom for data
        sum_res = session.get(f"{SUMMARY_URL}?AccountNumber={acc_num}", timeout=15)
        sum_soup = BeautifulSoup(sum_res.text, 'lxml')
        
        new_data = {"account_number": acc_num}
        
        # Scrape Values
        total_val_td = sum_soup.find(string=re.compile("Total\s+Value"))
        if total_val_td:
            val_td = total_val_td.find_parent('td').find_next_sibling('td')
            if val_td:
                val_str = val_td.get_text(strip=True).replace(',', '').replace('$', '')
                try:
                new_data['assessed_value'] = float(val_str)
                # Hartford condos/units in these complexes often have a 36.75% ratio
                new_data['appraised_value'] = round(new_data['assessed_value'] / 0.3675, 2)
            except:
                pass

        # Scrape Image - Store source URL instead of downloading
        img_tag = sum_soup.find('img', src=re.compile('showimage'))
        if img_tag:
             # Storing the absolute URL on the assessor's site. 
             # Note: This may require a valid session to display in the browser.
             new_data['building_photo'] = f"{BASE_URL}/showimage.asp"
        
        # Scrape Location/Unit
        new_unit = None
        new_location = None
        
        tds = sum_soup.find_all('td')
        for td in tds:
            text = td.get_text(" ", strip=True)
            if "Location" in text:
                match = re.search(r'Location\s+(.*)', text, re.IGNORECASE)
                if match:
                    new_location = match.group(1).strip()
                    new_location = new_location.replace('\xa0', ' ').replace('  ', ' ')
                    unit_match = re.search(r'Unit\s+(.*)', new_location, re.IGNORECASE)
                    if unit_match:
                        new_unit = unit_match.group(1).strip()
            
            if "Property Account Number" in text:
                 match = re.search(r'Property Account Number\s+(.*)', text, re.IGNORECASE)
                 if match:
                     parsed_val = match.group(1).strip().replace('\xa0', ' ')
                     if parsed_val and not new_unit:
                         new_unit = parsed_val

        if new_location:
            new_data['location'] = new_location
            new_data['unit'] = new_unit
        
        return prop_id, True, new_data

    except Exception as e:
        return prop_id, False, str(e)

def run_enrichment(limit=None):
    if not DATABASE_URL:
        # ... existing ...
        return

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print("Fetching properties to enrich...")
    query = """
        SELECT id, link, location 
        FROM properties 
        WHERE property_city = 'Hartford' 
        AND location ILIKE '%%WEBSTER%%'
        AND link IS NOT NULL 
    """
    # ... rest ...
    if limit:
        query += f" LIMIT {limit}"
        
    cursor.execute(query)
    rows = cursor.fetchall()
    print(f"Found {len(rows)} properties to process.")

    updated_count = 0
    errors = 0
    
    # Use ThreadPoolExecutor for speed
    # Hartford's portal is a bit old, let's not go too high. 5-10 threads is safe.
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(enrich_property, r[0], r[1], r[2], DATABASE_URL) for r in rows]
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            prop_id, success, result = future.result()
            if success:
                data = result
                # We update immediately to ensure progress visibility
                with psycopg2.connect(DATABASE_URL) as update_conn:
                    with update_conn.cursor() as update_cur:
                        update_cur.execute("""
                            UPDATE properties 
                            SET location = COALESCE(%s, location), 
                                unit = COALESCE(%s, unit), 
                                account_number = %s,
                                assessed_value = COALESCE(%s, assessed_value),
                                appraised_value = COALESCE(%s, appraised_value),
                                building_photo = COALESCE(%s, building_photo)
                            WHERE id = %s
                        """, (
                            data.get("location"), 
                            data.get("unit"), 
                            data.get("account_number"),
                            data.get("assessed_value"),
                            data.get("appraised_value"),
                            data.get("building_photo"),
                            prop_id
                        ))
                updated_count += 1
                if i % 10 == 0:
                    print(f"Progress: {i}/{len(rows)} updated. Current: {data['location']}")
            else:
                errors += 1
                if i % 50 == 0:
                    print(f"Error at {i}/{len(rows)}: {result}")
            
            # Subtle delay to play nice
            time.sleep(0.1)

    print(f"\nFinished. Updated {updated_count} properties. Errors: {errors}")
    conn.close()
    return updated_count

if __name__ == "__main__":
    run_enrichment()
