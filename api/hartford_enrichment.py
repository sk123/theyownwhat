
import os
import time
import requests
import psycopg2
from bs4 import BeautifulSoup
import re
import concurrent.futures
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DATABASE_URL = os.environ.get("DATABASE_URL")
BASE_URL = "http://assessor1.hartford.gov"
SEARCH_PAGE = f"{BASE_URL}/search-middle-ns.asp"
RESULTS_PAGE = f"{BASE_URL}/SearchResults.asp"
SUMMARY_URL = f"{BASE_URL}/summary-bottom.asp"

# Session per thread if needed, but requests.Session() is not thread-safe for simultaneous use.
# Better to create a session per task/batch or use a thread-local.
thread_local = threading.local()

def get_session(pool_size=20):
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_session_local():
    if not hasattr(thread_local, "session"):
        thread_local.session = get_session()
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
    session = get_session_local()
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
            parent_td = total_val_td.find_parent('td')
            if parent_td:
                val_td = parent_td.find_next_sibling('td')
                if val_td:
                    val_str = val_td.get_text(strip=True).replace(',', '').replace('$', '')
                    try:
                        new_data['assessed_value'] = float(val_str)
                        # Hartford condos/units in these complexes often have a 36.75% ratio
                        new_data['appraised_value'] = round(new_data['assessed_value'] / 0.3675, 2)
                    except:
                        pass

        # --- NARRATIVE PARSING ---
        # "This property contains 0 - of land mainly classified as CONDO CONV RES with a(n) Condo Flat style building, 
        # built about 1940, having Brick exterior and Tar & Gravel roof cover, with 0 commercial unit(s) 
        # and 1 residential unit(s), 4 total room(s), 3 total bedroom(s), 1 total bath(s), ..."
        narrative_node = sum_soup.find(string=re.compile("This property contains", re.IGNORECASE))
        if narrative_node:
            parent_tag = narrative_node.find_parent()
            text = parent_tag.get_text(" ", strip=True)
            # Property Type / Style
            type_match = re.search(r'classified as\s+(.*?)\s+with a', text, re.IGNORECASE)
            if type_match: new_data['property_type'] = type_match.group(1).strip()
            
            style_match = re.search(r'with a\(n\)\s+(.*?)\s+style building', text, re.IGNORECASE)
            if style_match: new_data['style'] = style_match.group(1).strip()
            
            # Year Built
            yb_match = re.search(r'built about\s+(\d{4})', text, re.IGNORECASE)
            if yb_match: new_data['year_built'] = int(yb_match.group(1))
            
            # Acres
            acres_match = re.search(r'contains\s+([\d.]+)\s+-', text, re.IGNORECASE)
            if acres_match: 
                try: new_data['acres'] = float(acres_match.group(1))
                except: pass

            # Units
            res_units = re.search(r'(\d+)\s+residential unit', text, re.IGNORECASE)
            comm_units = re.search(r'(\d+)\s+commercial unit', text, re.IGNORECASE)
            total_units = 0
            if res_units: total_units += int(res_units.group(1))
            if comm_units: total_units += int(comm_units.group(1))
            if total_units > 0: new_data['number_of_units'] = total_units

            # Rooms/Beds/Baths
            rooms_match = re.search(r'(\d+)\s+total room', text, re.IGNORECASE)
            if rooms_match: new_data['total_rooms'] = int(rooms_match.group(1))
            beds_match = re.search(r'(\d+)\s+total bedroom', text, re.IGNORECASE)
            if beds_match: new_data['total_bedrooms'] = int(beds_match.group(1))
            baths_match = re.search(r'(\d+)\s+total bath', text, re.IGNORECASE)
            if baths_match: new_data['total_baths'] = int(baths_match.group(1))
            
            # If the above fails, try more flexible match for "(s)"
            if 'total_rooms' not in new_data:
                rooms_match = re.search(r'(\d+)\s+room', text, re.IGNORECASE)
                if rooms_match: new_data['total_rooms'] = int(rooms_match.group(1))
            if 'total_bedrooms' not in new_data:
                beds_match = re.search(r'(\d+)\s+bedroom', text, re.IGNORECASE)
                if beds_match: new_data['total_bedrooms'] = int(beds_match.group(1))
            if 'total_baths' not in new_data:
                baths_match = re.search(r'(\d+)\s+bath', text, re.IGNORECASE)
                if baths_match: new_data['total_baths'] = int(baths_match.group(1))

        # --- ADDITIONAL FIELD: ZONE ---
        zone_td = sum_soup.find('td', string=re.compile("Zoning", re.IGNORECASE))
        if zone_td:
            val_td = zone_td.find_next_sibling('td')
            if val_td:
                new_data['zone'] = val_td.get_text(strip=True)

        # --- PHOTO DOWNLOAD ---
        img_tag = sum_soup.find('img', src=re.compile('showimage'))
        if img_tag:
            try:
                img_url = f"{BASE_URL}/showimage.asp?{int(time.time()*1000)}"
                img_resp = session.get(img_url, timeout=10)
                if img_resp.status_code == 200 and len(img_resp.content) > 1000:
                    os.makedirs("api/static/hartford_images", exist_ok=True)
                    local_path = f"api/static/hartford_images/hartford_{acc_num}.jpg"
                    with open(local_path, "wb") as f:
                        f.write(img_resp.content)
                    new_data['building_photo'] = f"/api/static/hartford_images/hartford_{acc_num}.jpg"
                else:
                    # Fallback to the account number URL if we can't download now, maybe proxy will help later
                    new_data['building_photo'] = f"{BASE_URL}/showimage.asp?AccountNumber={acc_num}&Width=500"
            except:
                new_data['building_photo'] = f"{BASE_URL}/showimage.asp?AccountNumber={acc_num}&Width=500"
        
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
        print("DATABASE_URL not set.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print("Fetching properties to enrich...")
    query = """
        SELECT id, link, location 
        FROM properties 
        WHERE property_city = 'Hartford' 
        AND link IS NOT NULL 
        AND (building_photo IS NULL OR owner = 'Current Owner' OR assessed_value = 0 OR unit IS NULL OR unit LIKE '%CNDASC%')
    """
    
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
                                building_photo = COALESCE(%s, building_photo),
                                property_type = COALESCE(%s, property_type),
                                year_built = COALESCE(%s, year_built),
                                living_area = COALESCE(%s, living_area),
                                number_of_units = COALESCE(%s, number_of_units),
                                zone = COALESCE(%s, zone),
                                acres = COALESCE(%s, acres),
                                total_rooms = %s,
                                total_bedrooms = %s,
                                total_baths = %s
                            WHERE id = %s
                        """, (
                            data.get("location"), 
                            data.get("unit"), 
                            data.get("account_number"),
                            data.get("assessed_value"),
                            data.get("appraised_value"),
                            data.get("building_photo"),
                            data.get("property_type"),
                            data.get("year_built"),
                            data.get("living_area"),
                            data.get("number_of_units"),
                            data.get("zone"),
                            data.get("acres"),
                            data.get("total_rooms"),
                            data.get("total_bedrooms"),
                            data.get("total_baths"),
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
