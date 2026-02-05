
def process_municipality_with_avon_static(conn, municipality_name, base_url, current_owner_only=False, force_process=False):
    """
    Scrapes Avon's static HTML assessor site.
    Starting point: http://assessor.avonct.gov/prop_addr.html
    """
    log(f"--- Processing municipality: {municipality_name} via Static HTML (Force={force_process}) ---")
    
    # 1. Get Street Index
    try:
        index_url = f"{base_url.rstrip('/')}/prop_addr.html"
        resp = requests.get(index_url, timeout=10)
        if resp.status_code != 200:
            log(f"Failed to fetch index: {index_url}")
            return 0
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        street_links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Expected format: /propcards/Astreet.html or just Astreet.html
            # The site uses relative links sometimes with weird slashes
            clean_href = href.replace('\\', '/').strip()
            if 'street.html' in clean_href:
                # Resolve relative URL
                full_url = requests.compat.urljoin(index_url, clean_href)
                # Remove anchor
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
                    props = future.result() # returns list of (prop_url, address)
                    # Now process properties in parallel
                    # We can do this in the same executor or a nested one?
                    # Better to just collect all properties first? No, stream it.
                    
                    # Log found properties
                    # log(f"Found {len(props)} properties on {page_url}")
                    
                    # Process properties
                    for prop_url, address_text in props:
                        if process_avon_property(conn, prop_url, address_text, municipality_name):
                            updated_count += 1
                            
                except Exception as exc:
                    log(f"Error processing {page_url}: {exc}")
                    
        return updated_count

    except Exception as e:
        log(f"Critical error scraping Avon: {e}")
        return 0

def process_avon_street_page(url):
    """Fetches a street page (Astreet.html) and returns property links."""
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        return []
    
    soup = BeautifulSoup(resp.content, 'html.parser')
    properties = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        # Looking for links to property cards e.g. /propcards/5/admin/a581000101.html
        if '/admin/a' in href and '.html' in href:
             full_url = requests.compat.urljoin(url, href)
             address_text = a.get_text().strip()
             properties.append((full_url, address_text))
    return properties

def process_avon_property(conn, url, address_text, municipality_name):
    """Fetches and parses a single property card."""
    try:
        # Check if already processed (optimization)
        # TODO: ID extraction? 
        # URL: http://assessor.avonct.gov/propcards/5/admin/a581000101.html
        # ID is likely 5810001 or a581000101
        
        # Fetch
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return False
            
        content = resp.text
        
        # VERY BASIC REGEX PARSING due to weird text layout
        # Owner name: REVIS LINDA L AND |
        owner_match = re.search(r"Owner name:\s*(.*?)\s*\|", content)
        owner = owner_match.group(1).strip() if owner_match else None
        
        # Second name: STEPHEN E |
        co_owner_match = re.search(r"Second name:\s*(.*?)\s*\|", content)
        co_owner = co_owner_match.group(1).strip() if co_owner_match else None
        
        # Address: ONE ABBOTTSFORD | (Also avail in address_text)
        addr_match = re.search(r"Address:\s*(.*?)\s*\|", content)
        location = addr_match.group(1).strip() if addr_match else address_text
        
        # Sale date: 19-Feb-2003|
        sale_date_match = re.search(r"Sale date:\s*(.*?)\s*\|", content)
        sale_date = None
        if sale_date_match:
            try:
                sale_date = datetime.strptime(sale_date_match.group(1).strip(), "%d-%b-%Y").date()
            except:
                pass
                
        # Sale price: 525,974|
        sale_price_match = re.search(r"Sale price:\s*(.*?)\s*\|", content)
        sale_price = None
        if sale_price_match:
            try:
                sale_price = float(sale_price_match.group(1).strip().replace(',', ''))
            except:
                pass

        # Vol/Page? Vol: 454 Page: 278 |
        # Not critical but good to have.
        
        # Account Number / ID
        # Extract from URL filename? a581000101 -> 5810001?
        # User said 3970165 was account?
        # Let's use the filename ID as a unique ID for now.
        prop_id_match = re.search(r"\/a(\d+)\.html", url)
        prop_id = prop_id_match.group(1) if prop_id_match else None
        
        if not location:
            return False

        # UPSERT
        # We need to find the property_id in DB if it exists.
        # Use location + city matching logic.
        
        # Construct data dict
        scraped_data = {
            'owner': owner,
            'co_owner': co_owner,
            'location': location,
            'sale_date': sale_date,
            'sale_amount': sale_price,
            'cama_site_link': url,
            'property_city': municipality_name.upper(),
            'account_number': prop_id # Fallback
        }
        
        # Use the existing update_property_in_db function?
        # Need to resolve DB ID first.
        # Or better: search DB by location/town to update.
        
        # For now, let's reuse update_property_in_db logic which usually takes an ID.
        # But we don't have the ID yet if we are scraping from scratch.
        # We need `get_property_id_by_location` helper or similar.
        
        # Creating a temporary helper to find/create property
        db_id = get_or_create_property_id(conn, municipality_name, location)
        
        return update_property_in_db(conn, db_id, scraped_data, municipality_name=municipality_name)

    except Exception as e:
        # log(f"Error parse avon prop {url}: {e}")
        return False

def get_or_create_property_id(conn, city, address):
    # Simplified lookup
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM properties WHERE property_city = %s AND location = %s", (city.upper(), address.upper()))
        row = cur.fetchone()
        if row:
            return row[0]
        # Create
        cur.execute("INSERT INTO properties (property_city, location, source) VALUES (%s, %s, 'avon_static') RETURNING id", (city.upper(), address.upper()))
        return cur.fetchone()[0]
