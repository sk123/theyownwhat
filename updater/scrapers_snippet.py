def scrape_mapgeo_property(session, base_url, unique_id, layout_id):
    """
    Scrapes a single property from MapGeo using the ItemDetails layout.
    """
    try:
        # Construct detail URL
        # e.g. https://cromwellct.mapgeo.io/api/ui/datasets/properties/{id}?layoutId={layoutId}&layoutType=itemDetails
        detail_url = f"{base_url}/api/ui/datasets/properties/{unique_id}"
        params = {
            'layoutId': layout_id,
            'layoutType': 'itemDetails'
        }
        
        # MapGeo requires Referer/Origin for some requests
        headers = {
            'Referer': base_url + '/',
            'Origin': base_url,
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        resp = session.get(detail_url, params=params, headers=headers, timeout=10)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        
        data = resp.json()
        properties = data.get('data', {})
        
        # Extract fields
        # MapGeo fields can be dynamic, but usually:
        # ownerName, owners (list), streetName, streetNumber, etc.
        
        owner_name = properties.get('ownerName')
        if not owner_name and properties.get('owners'):
            owner_name = properties['owners'][0]
            
        # Mailing Address
        mailing_address = properties.get('mailingAddress')
        if not mailing_address:
            # Try to construct from parts if available
             mailing_address = f"{properties.get('mailingAddress1', '')} {properties.get('mailingAddress2', '')}".strip()

        # Sales info
        sale_date = properties.get('lastSaleDate') or properties.get('saleDate')
        sale_price = properties.get('lastSalePrice') or properties.get('salePrice')
        
        # Assessment
        assessed_value = properties.get('assessedValue') or properties.get('totalAssessedValue')
        appraised_value = properties.get('appraisedValue') or properties.get('totalAppraisedValue')
        
        # Year Built
        year_built = properties.get('yearBuilt')

        return {
            'owner': owner_name,
            'mailing_address': mailing_address,
            'sale_date': sale_date,
            'sale_price': sale_price,
            'assessed_value': assessed_value,
            'appraised_value': appraised_value,
            'year_built': year_built,
            'raw_data': properties # Store raw for debugging if needed (not persisted usually)
        }

    except Exception as e:
        print(f"Error scraping MapGeo property {unique_id}: {e}")
        return None

def process_municipality_with_mapgeo(conn, municipality_name, data_source_config, current_owner_only=False, force_process=False):
    """
    Processes a municipality using MapGeo API.
    """
    domain = data_source_config['domain'] # e.g. cromwellct.mapgeo.io
    if domain.startswith('http'):
        base_url = domain
    else:
        base_url = f"https://{domain}"
        
    # Get Layout ID from config or fallback
    layout_id = data_source_config.get('layout_id', '6032ef5b-8331-4abb-aa9d-e1a114b21443') # Default to Cromwell's if not set
    
    log(f"Starting MapGeo processing for {municipality_name} at {base_url} with layout {layout_id}")

    # 1. Discovery Phase
    # Use POST search to get all properties
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': base_url + '/',
        'Origin': base_url
    })
    
    # Try empty query to get all, or fallback to iterate A-Z if needed.
    # MapGeo often returns max 100 or so. We might need pagination?
    # The debug script showed 50 results for "Main".
    # We should inspect if there is pagination in the response.
    # The response had "items". 
    # Let's try to get all properties by using a wildcard or empty query.
    # And check for pagination tokens if any.
    
    log("Discovering properties...")
    discovered_count = 0
    all_properties = []
    
    # Simple pagination loop?
    # Or maybe we just search for "Street", "Ave", "Rd", etc. to cover most?
    # Or just iterate alphabets for street names?
    
    # Let's try a broad search first.
    payload = {
        "attributes": ["displayName", "ownerName", "id", "rowId"],
        "query": {
            "displayName": "", # Empty for all?
            "id": "",
            "ownerName": ""
        },
        "geometry": { "type": "FeatureCollection", "features": [] },
        "sort": []
    }
    
    # Actually, "Main" search worked. Empty might fail.
    # Let's use a list of common street suffixes + a-z to ensure coverage?
    # Or just iterate common search terms.
    # For now, let's assume we can paging works or we get a lot.
    # But wait, MapGeo is usually an SPA.
    # Let's try to search for '%' or '*' or just ''
    
    try:
        search_url = f"{base_url}/api/datasets/properties/search?format=json"
        # We'll just do one big search for now and see. 
        # If it's limited, we might need a better strategy later.
        # But even better: Use the DB's existing street names if we have them?
        # No, we want discovery.
        
        # Let's try iterating A-Z for query.
        chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        for char in chars:
            payload['query']['displayName'] = char
            resp = session.post(search_url, json=payload)
            if resp.status_code == 200:
                data = resp.json()
                items = data if isinstance(data, list) else data.get('items', [])
                log(f"Discovery: '{char}' returned {len(items)} items.")
                all_properties.extend(items)
            else:
                log(f"Discovery failed for '{char}': {resp.status_code}")
                
        # Deduplicate
        unique_props = {p.get('id'): p for p in all_properties if p.get('id')}
        log(f"Total unique properties discovered: {len(unique_props)}")
        
        # Upsert into DB
        with conn.cursor() as cur:
            for pid, prop in unique_props.items():
                owner = prop.get('ownerName')
                address = prop.get('displayName') # Usually address
                
                # Check if we should process
                # We always upsert to ensure we have the ID for scraping
                cur.execute("""
                    INSERT INTO properties (property_city, location, account_number, source, owner)
                    VALUES (%s, %s, %s, 'MAPGEO', %s)
                    ON CONFLICT (property_city, location) 
                    DO UPDATE SET account_number = EXCLUDED.account_number, source = 'MAPGEO'
                    RETURNING id
                """, (municipality_name.title(), address, pid, owner))
        conn.commit()
    except Exception as e:
        log(f"Discovery Error: {e}")
        conn.rollback()

    # 2. Process Properties
    # Select properties to process
    with conn.cursor() as cur:
        if force_process:
            query = """
                SELECT id, location, account_number 
                FROM properties 
                WHERE property_city = %s AND source = 'MAPGEO' AND account_number IS NOT NULL
            """
        elif current_owner_only:
             query = """
                SELECT id, location, account_number 
                FROM properties 
                WHERE property_city = %s AND source = 'MAPGEO' AND account_number IS NOT NULL
                AND (owner LIKE 'Current Owner%%' OR last_scraped_at IS NULL OR building_photo IS NULL)
            """
        else:
             query = """
                SELECT id, location, account_number 
                FROM properties 
                WHERE property_city = %s AND source = 'MAPGEO' AND account_number IS NOT NULL
                AND (last_scraped_at IS NULL OR last_scraped_at < NOW() - INTERVAL '30 days')
            """
        cur.execute(query, (municipality_name.title(),))
        properties_to_scrape = cur.fetchall()
        
    log(f"Found {len(properties_to_scrape)} properties to scrape detail for.")

    updated_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=DEFAULT_MUNI_WORKERS) as executor:
        future_to_prop = {
            executor.submit(scrape_mapgeo_property, session, base_url, prop[2], layout_id): prop 
            for prop in properties_to_scrape
        }
        
        for future in concurrent.futures.as_completed(future_to_prop):
            prop = future_to_prop[future]
            db_id, address, unique_id = prop
            try:
                data = future.result()
                if data:
                    # Update DB
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE properties 
                            SET owner = %s,
                                mailing_address = %s,
                                sale_date = %s,
                                sale_price = %s,
                                assessed_value = %s,
                                appraised_value = %s,
                                year_built = %s,
                                last_scraped_at = NOW()
                            WHERE id = %s
                        """, (
                            data['owner'], data['mailing_address'], 
                            data['sale_date'], data['sale_price'],
                            data['assessed_value'], data['appraised_value'],
                            data['year_built'],
                            db_id
                        ))
                    conn.commit()
                    updated_count += 1
            except Exception as e:
                log(f"Error updating property {address}: {e}")
                
    return updated_count

def process_municipality_with_tighe_bond(conn, municipality_name, data_source_config, current_owner_only=False, force_process=False):
    """
    Processes a municipality using Tighe & Bond ArcGIS + PropertyRecordCards.
    """
    # ArcGIS Find Endpoint
    find_url = data_source_config.get('arcgis_find_url')
    # Or construct it?
    # https://hostingdata3.tighebond.com/arcgis/rest/services/EastHamptonCT/EastHamptonDynamic/MapServer/find
    if not find_url:
        log(f"Missing 'arcgis_find_url' for {municipality_name}")
        return 0
        
    log(f"Starting Tighe & Bond processing for {municipality_name}")
    
    # 1. Discovery (ArcGIS)
    session = requests.Session()
    params = {
        'searchText': '%', # Wildcard for all?
        'contains': 'true',
        'searchFields': 'ParcelPolygon.Parcel_ID,CAMA.Owner,ParcelPolygon.StreetAddr',
        'layers': '0', 
        'returnGeometry': 'false',
        'f': 'pjson'
    }
    
    # We might need to iterate if % doesn't work or returns limit.
    # ArcGIS limit is often 1000.
    # Let's try iterating A-Z for completeness too.
    
    discovered_props = {}
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    
    log("Discovering properties via ArcGIS...")
    for char in chars:
        params['searchText'] = char
        try:
            resp = session.get(find_url, params=params, headers={'Referer': 'https://hosting.tighebond.com/'})
            if resp.status_code == 200:
                data = resp.json()
                results = data.get('results', [])
                log(f"Discovery: '{char}' returned {len(results)} items.")
                for res in results:
                    attrs = res.get('attributes', {})
                    acct = attrs.get('Account Number')
                    addr = attrs.get('Address')
                    owner = attrs.get('Owner')
                    if acct and addr:
                        discovered_props[acct] = {'address': addr, 'owner': owner}
            else:
                log(f"Discovery failed for '{char}': {resp.status_code}")
        except Exception as e:
            log(f"Discovery error {char}: {e}")
            
    log(f"Total unique properties discovered: {len(discovered_props)}")
    
    # Upsert
    with conn.cursor() as cur:
        for acct, data in discovered_props.items():
            cur.execute("""
                INSERT INTO properties (property_city, location, account_number, source, owner)
                VALUES (%s, %s, %s, 'TIGHE_BOND', %s)
                ON CONFLICT (property_city, location) 
                DO UPDATE SET account_number = EXCLUDED.account_number, source = 'TIGHE_BOND'
                RETURNING id
            """, (municipality_name.title(), data['address'], acct, data['owner']))
    conn.commit()
    
    # 2. Process (Scrape PropertyRecordCards)
    # Select properties
    with conn.cursor() as cur:
        if force_process:
             query = """
                SELECT id, location, account_number 
                FROM properties 
                WHERE property_city = %s AND source = 'TIGHE_BOND' AND account_number IS NOT NULL
            """
        else:
             query = """
                SELECT id, location, account_number 
                FROM properties 
                WHERE property_city = %s AND source = 'TIGHE_BOND' AND account_number IS NOT NULL
                AND (last_scraped_at IS NULL OR last_scraped_at < NOW() - INTERVAL '30 days')
            """
        cur.execute(query, (municipality_name.title(),))
        properties_to_scrape = cur.fetchall()
        
    log(f"Found {len(properties_to_scrape)} properties to scrape detail for.")
    
    updated_count = 0
    # PropertyRecordCards Base URL
    prc_base_url = "https://www.propertyrecordcards.com/PropertyResults.aspx?towncode=42&uniqueid={}"
    # Note: Towncode 42 is for East Hampton. We should probably put this in config or derive from somewhere.
    # For now, I'll use the config 'town_code' or default to 42 if name is East Hampton.
    town_code = data_source_config.get('town_code', '42') 
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=DEFAULT_MUNI_WORKERS) as executor:
        future_to_prop = {
            executor.submit(scrape_propertyrecordcards_property, session, prc_base_url.replace('42', str(town_code)), prop[2], town_code): prop
            for prop in properties_to_scrape
        }
        
        for future in concurrent.futures.as_completed(future_to_prop):
            prop = future_to_prop[future]
            db_id, address, acct = prop
            try:
                data = future.result()
                if data:
                    # scrape_propertyrecordcards_property returns a dict.
                    # We need to map it to DB columns.
                    # It returns: {owner, sale_date, sale_price, ...}
                    
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE properties 
                            SET owner = %s,
                                sale_date = %s,
                                sale_price = %s,
                                assessed_value = %s,
                                appraised_value = %s,
                                year_built = %s,
                                building_photo = %s,
                                last_scraped_at = NOW()
                            WHERE id = %s
                        """, (
                            data.get('owner'), 
                            data.get('sale_date'), data.get('sale_amount'), # Note key diff
                            data.get('assessed_value'), data.get('appraised_value'),
                            data.get('year_built'),
                            data.get('building_photo'),
                            db_id
                        ))
                    conn.commit()
                    updated_count += 1
            except Exception as e:
                log(f"Error updating property {address}: {e}")

    return updated_count
