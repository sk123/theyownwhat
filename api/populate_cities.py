import os
import json
import time
import psycopg2
import requests
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
CITIES = ["dc", "baltimore", "boston", "detroit", "philadelphia", "chicago", "miami"]

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def source_float(value):
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None

def source_int(value):
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None

def source_text(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.upper() in {"UNKNOWN", "UNKNOWN ADDRESS", "N/A", "NA", "NONE", "NULL"}:
        return None
    return text

def create_tables(conn, target_cities=None):
    if target_cities is None:
        target_cities = CITIES
    with conn.cursor() as cur:
        for city in target_cities:
            print(f"Creating/Verifying tables for {city}...")
            # Registrations
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}_hpd_registrations (
                    registration_id         TEXT PRIMARY KEY,
                    bbl                     TEXT,           
                    bin                     TEXT,           
                    building_address        TEXT,
                    building_city           TEXT,
                    building_zip            TEXT,
                    borough                 TEXT,           
                    lifecycle_stage         TEXT,
                    last_registration_date  DATE,
                    registration_end_date   DATE,
                    created_at              TIMESTAMP DEFAULT NOW(),
                    updated_at              TIMESTAMP DEFAULT NOW()
                );
            """)
            # Contacts
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}_hpd_contacts (
                    contact_id              BIGSERIAL PRIMARY KEY,
                    registration_id         TEXT,               
                    contact_type            TEXT,           
                    corporation_name        TEXT,
                    corporation_name_norm   TEXT,           
                    first_name              TEXT,
                    last_name               TEXT,
                    full_name               TEXT,           
                    full_name_norm          TEXT,           
                    business_address        TEXT,
                    business_city           TEXT,
                    business_state          TEXT,
                    business_zip            TEXT,
                    created_at              TIMESTAMP DEFAULT NOW()
                );
            """)
            # Properties
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}_properties (
                    bbl                     TEXT PRIMARY KEY,
                    address                 TEXT,
                    borough                 TEXT,
                    zip_code                TEXT,
                    owner_name              TEXT,
                    owner_name_norm         TEXT,           
                    land_use                TEXT,           
                    bld_class               TEXT,           
                    num_floors              NUMERIC,
                    units_res               NUMERIC,
                    units_total             NUMERIC,
                    year_built              INTEGER,
                    assessed_total          NUMERIC,
                    latitude                NUMERIC,
                    longitude               NUMERIC,
                    compliance_active       BOOLEAN DEFAULT FALSE,
                    compliance_record_id    TEXT,
                    compliance_expiration   DATE,
                    created_at              TIMESTAMP DEFAULT NOW(),
                    updated_at              TIMESTAMP DEFAULT NOW()
                );
            """)
            # Networks
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}_networks (
                    id                      BIGSERIAL PRIMARY KEY,
                    network_key             TEXT UNIQUE NOT NULL,   
                    anchor_type             TEXT NOT NULL,          
                    display_name            TEXT,
                    member_names            TEXT[],                 
                    member_addresses        TEXT[],                 
                    registration_ids        TEXT[],                 
                    bbl_list                TEXT[],                 
                    building_count          INTEGER DEFAULT 0,
                    unit_count              INTEGER DEFAULT 0,
                    borough_summary         JSONB,                  
                    connection_signals      TEXT DEFAULT '{{}}',
                    created_at              TIMESTAMP DEFAULT NOW(),
                    updated_at              TIMESTAMP DEFAULT NOW()
                );
            """)
            # Stats
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {city}_bbl_stats (
                    bbl                     TEXT PRIMARY KEY,
                    violations_total        INTEGER DEFAULT 0,
                    violations_open         INTEGER DEFAULT 0,
                    violations_class_c      INTEGER DEFAULT 0,   
                    violations_class_b      INTEGER DEFAULT 0,   
                    violations_class_a      INTEGER DEFAULT 0,   
                    violations_open_c       INTEGER DEFAULT 0,   
                    last_violation_date     DATE,
                    litigations_total       INTEGER DEFAULT 0,
                    litigations_open        INTEGER DEFAULT 0,
                    litigations_harassment  INTEGER DEFAULT 0,   
                    last_litigation_date    DATE,
                    evictions_total         INTEGER DEFAULT 0,
                    last_eviction_date      DATE,
                    updated_at              TIMESTAMPTZ DEFAULT NOW(),
                    is_rent_stabilized      BOOLEAN DEFAULT FALSE,
                    rs_units                INTEGER DEFAULT 0,
                    nhpd_subsidy            BOOLEAN DEFAULT FALSE,
                    nhpd_program            TEXT,
                    nhpd_expiration         DATE
                );
            """)
    conn.commit()
    print("All tables checked/created.")

def clean_tables(conn, target_cities=None):
    if target_cities is None:
        target_cities = CITIES
    with conn.cursor() as cur:
        for city in target_cities:
            print(f"Dropping existing tables for {city}...")
            cur.execute(f"DROP TABLE IF EXISTS {city}_hpd_registrations CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {city}_hpd_contacts CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {city}_properties CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {city}_networks CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {city}_bbl_stats CASCADE;")
    conn.commit()
    print("All old city data cleaned.")

def fetch_real_dc_properties(limit=1000000):
    print("Fetching real properties from DC GIS (Computer Assisted Mass Appraisal)...")
    url = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/40/query"
    properties = []
    chunk_size = 2000
    offset = 0
    
    while len(properties) < limit:
        params = {
            'where': 'OWNERNAME IS NOT NULL AND PREMISEADD IS NOT NULL',
            'outFields': 'SSL,OWNERNAME,PREMISEADD,ADDRESS1,CITYSTZIP,ASSESSMENT,PRMSWARD,USECODE',
            'resultOffset': offset,
            'resultRecordCount': chunk_size,
            'returnGeometry': 'false',
            'f': 'json'
        }
        features = []
        retries = 3
        while retries > 0:
            try:
                r = requests.get(url, params=params, timeout=25)
                r.raise_for_status()
                data = r.json()
                features = data.get('features', [])
                break
            except Exception as e:
                retries -= 1
                if retries == 0:
                    print(f"  Failed fetching DC chunk at offset {offset}: {e}")
                    break
                print(f"  Error fetching DC chunk: {e}. Retrying ({3 - retries}/3)...")
                time.sleep(2)
                
        if not features:
            break
            
        for f in features:
            attrs = f.get('attributes', {})
            if not attrs.get('SSL') or not attrs.get('OWNERNAME'):
                continue
            # Parse zip code
            city_st_zip = attrs.get('CITYSTZIP') or ""
            zip_code = None
            for word in city_st_zip.split():
                if word.isdigit() and len(word) == 5:
                    zip_code = word
                    break
            
            properties.append({
                'bbl': attrs.get('SSL').strip(),
                'address': attrs.get('PREMISEADD').strip(),
                'borough': f"WARD {attrs.get('PRMSWARD')}" if attrs.get('PRMSWARD') else "WARD UNKNOWN",
                'zip_code': zip_code,
                'owner_name': attrs.get('OWNERNAME').strip(),
                'owner_name_norm': attrs.get('OWNERNAME').strip().upper(),
                'mailing_address': source_text(attrs.get('ADDRESS1')) or '',
                'assessed_total': source_float(attrs.get('ASSESSMENT')),
                'units_res': None,
                'year_built': None,
                'land_use': attrs.get('USECODE') or None,
                'bld_class': attrs.get('USECODE') or None
            })
        offset += chunk_size
        print(f"  Fetched {len(properties)} DC properties...")
            
    return properties

def fetch_real_baltimore_properties(limit=1000000):
    print("Fetching real properties from Baltimore City GIS (dmxOwnership)...")
    url = "https://egis.baltimorecity.gov/egis/rest/services/Housing/dmxOwnership/MapServer/0/query"
    properties = []
    chunk_size = 2000
    offset = 0
    
    while len(properties) < limit:
        params = {
            'where': 'OWNER_1 IS NOT NULL AND BLOCKLOT IS NOT NULL',
            'outFields': 'BLOCKLOT,OWNER_1,OWNER_2,OWNER_3,MAILTOADD,ZIP_CODE,DWELUNIT,SALEPRIC,YEAR_BUILD,FULLADDR,WARD',
            'resultOffset': offset,
            'resultRecordCount': chunk_size,
            'returnGeometry': 'false',
            'f': 'json'
        }
        features = []
        retries = 3
        while retries > 0:
            try:
                r = requests.get(url, params=params, timeout=25)
                r.raise_for_status()
                data = r.json()
                features = data.get('features', [])
                break
            except Exception as e:
                retries -= 1
                if retries == 0:
                    print(f"  Failed fetching Baltimore chunk at offset {offset}: {e}")
                    break
                print(f"  Error fetching Baltimore chunk: {e}. Retrying ({3 - retries}/3)...")
                time.sleep(2)
                
        if not features:
            break
            
        for f in features:
            attrs = f.get('attributes', {})
            if not attrs.get('BLOCKLOT') or not attrs.get('OWNER_1'):
                continue
            # Build owner name
            owner_parts = [attrs.get('OWNER_1') or '', attrs.get('OWNER_2') or '', attrs.get('OWNER_3') or '']
            owner_name = " ".join([p.strip() for p in owner_parts if p.strip()])
            
            properties.append({
                'bbl': attrs.get('BLOCKLOT').strip(),
                'address': attrs.get('FULLADDR') or f"{attrs.get('BLDG_NO') or ''} {attrs.get('ST_NAME') or ''}".strip(),
                'borough': f"WARD {attrs.get('WARD')}" if attrs.get('WARD') else "WARD UNKNOWN",
                'zip_code': attrs.get('ZIP_CODE') or None,
                'owner_name': owner_name,
                'owner_name_norm': owner_name.upper(),
                'mailing_address': source_text(attrs.get('MAILTOADD')) or '',
                'assessed_total': source_float(attrs.get('SALEPRIC')),
                'units_res': source_int(attrs.get('DWELUNIT')),
                'year_built': source_int(attrs.get('YEAR_BUILD')),
                'land_use': None,
                'bld_class': None
            })
        offset += chunk_size
        print(f"  Fetched {len(properties)} Baltimore properties...")
            
    return properties

def fetch_real_philadelphia_properties(limit=1000000):
    print("Fetching real properties from Philadelphia OPA (Carto SQL API)...")
    url = "https://phl.carto.com/api/v2/sql"
    limit = min(limit, 50000)
    query = f"SELECT parcel_number, location, owner_1, owner_2, year_built, category_code, ST_Y(the_geom) as lat, ST_X(the_geom) as lng FROM opa_properties_public WHERE the_geom IS NOT NULL AND owner_1 IS NOT NULL AND location IS NOT NULL LIMIT {limit}"
    try:
        r = requests.get(url, params={'q': query}, timeout=30)
        r.raise_for_status()
        data = r.json()
        rows = data.get('rows', [])
        properties = []
        for row in rows:
            owner = row.get('owner_1') or ''
            if row.get('owner_2'):
                owner += f" & {row['owner_2']}"
            properties.append({
                'bbl': row.get('parcel_number').strip(),
                'address': row.get('location').strip(),
                'borough': 'PHILADELPHIA',
                'zip_code': None,
                'owner_name': owner.strip(),
                'owner_name_norm': owner.strip().upper(),
                'mailing_address': '',
                'assessed_total': None,
                'units_res': None,
                'year_built': source_int(row.get('year_built')),
                'land_use': row.get('category_code') or None,
                'bld_class': None,
                'latitude': source_float(row.get('lat')),
                'longitude': source_float(row.get('lng'))
            })
        print(f"  Fetched {len(properties)} Philadelphia properties.")
        return properties
    except Exception as e:
        print(f"  Failed fetching Philadelphia properties: {e}")
        return []

def fetch_real_boston_properties(limit=1000000):
    print("Fetching real properties from Analyze Boston (CKAN API) with pagination...")
    url = "https://data.boston.gov/api/3/action/datastore_search"
    properties = []
    chunk_size = 30000
    offset = 0
    
    while len(properties) < limit:
        params = {
            'resource_id': '6b7e460e-33f6-4e61-80bc-1bef2e73ac54',
            'limit': chunk_size,
            'offset': offset
        }
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            records = data.get('result', {}).get('records', [])
            if not records:
                break
                
            for row in records:
                pid = row.get('PID') or ''
                owner = row.get('OWNER') or ''
                address = f"{row.get('ST_NUM') or ''} {row.get('ST_NAME') or ''}".strip()
                if not pid or not owner or not address:
                    continue
                
                properties.append({
                    'bbl': pid.strip(),
                    'address': address,
                    'borough': row.get('CITY') or None,
                    'zip_code': row.get('ZIP_CODE') or None,
                    'owner_name': owner.strip(),
                    'owner_name_norm': owner.strip().upper(),
                    'mailing_address': row.get('MAIL_STREET_ADDRESS') or '',
                    'assessed_total': source_float(row.get('TOTAL_VALUE')),
                    'units_res': source_int(row.get('RES_UNITS')),
                    'year_built': source_int(row.get('YR_BUILT')),
                    'land_use': row.get('LU') or row.get('PROPERTY_TYPE') or None,
                    'bld_class': row.get('BLDG_TYPE') or None,
                    'latitude': source_float(row.get('LATITUDE')),
                    'longitude': source_float(row.get('LONGITUDE'))
                })
            
            print(f"  Fetched {len(properties)} Boston properties so far (offset: {offset})...")
            offset += len(records)
            
            # If records returned is less than chunk_size, we've reached the end
            if len(records) < chunk_size:
                break
                
        except Exception as e:
            print(f"  Failed fetching Boston properties at offset {offset}: {e}")
            break
            
    print(f"  Total Fetched {len(properties)} Boston properties.")
    return properties

def fetch_real_detroit_properties(limit=1000000):
    print("Fetching real properties from Detroit City GIS (Parcels (Current))...")
    url = "https://services2.arcgis.com/qvkbeam7Wirps6zC/ArcGIS/rest/services/Parcels_Current/FeatureServer/0/query"
    properties = []
    chunk_size = 5000
    offset = 0
    
    while len(properties) < limit:
        params = {
            'where': 'taxpayer_1 IS NOT NULL AND parcel_number IS NOT NULL',
            'outFields': 'parcel_number,address,ward,zip_code,taxpayer_1,taxpayer_2,taxpayer_street,taxpayer_city,taxpayer_state,taxpayer_zip,property_class_desc,year_built,assessed_value',
            'resultOffset': offset,
            'resultRecordCount': chunk_size,
            'returnGeometry': 'true',
            'outSR': '4326',
            'f': 'json'
        }
        features = []
        retries = 3
        while retries > 0:
            try:
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                features = data.get('features', [])
                break
            except Exception as e:
                retries -= 1
                if retries == 0:
                    print(f"  Failed fetching Detroit chunk at offset {offset}: {e}")
                    break
                print(f"  Error fetching Detroit chunk: {e}. Retrying ({3 - retries}/3)...")
                time.sleep(2)
                
        if not features:
            break
            
        for f in features:
            attrs = f.get('attributes', {})
            if not attrs.get('parcel_number') or not attrs.get('taxpayer_1'):
                continue
            
            # Combine taxpayer_1 and taxpayer_2
            owner_parts = [attrs.get('taxpayer_1') or '', attrs.get('taxpayer_2') or '']
            owner_name = " ".join([p.strip() for p in owner_parts if p.strip()])
            
            # Combine mailing address
            mail_parts = [
                attrs.get('taxpayer_street') or '',
                attrs.get('taxpayer_city') or '',
                attrs.get('taxpayer_state') or '',
                attrs.get('taxpayer_zip') or ''
            ]
            mailing_address = ", ".join([p.strip() for p in mail_parts if p.strip()])
            
            # Centroid calculations
            geom = f.get('geometry') or {}
            rings = geom.get('rings')
            lat, lng = None, None
            if rings and len(rings) > 0 and len(rings[0]) > 0:
                pts = rings[0]
                lng = sum(p[0] for p in pts) / len(pts)
                lat = sum(p[1] for p in pts) / len(pts)
            
            properties.append({
                'bbl': attrs.get('parcel_number').strip(),
                'address': attrs.get('address') or '',
                'borough': f"WARD {attrs.get('ward')}" if attrs.get('ward') else "WARD UNKNOWN",
                'zip_code': attrs.get('zip_code') or None,
                'owner_name': owner_name,
                'owner_name_norm': owner_name.upper(),
                'mailing_address': mailing_address,
                'assessed_total': source_float(attrs.get('assessed_value')),
                'units_res': None,
                'year_built': source_int(attrs.get('year_built')),
                'land_use': attrs.get('property_class_desc') or None,
                'bld_class': None,
                'latitude': lat,
                'longitude': lng
            })
        offset += chunk_size
        print(f"  Fetched {len(properties)} Detroit properties...")
            
    return properties

def fetch_real_chicago_properties(limit=5000):
    print("Fetching real properties from Chicago Business Licenses and Owners...")
    
    # 1. Fetch owners to build lookup
    print("  Fetching Chicago business owners registry...")
    owners_url = "https://data.cityofchicago.org/resource/ezma-pppn.json"
    owners_by_account = {}
    try:
        r = requests.get(owners_url, params={'$limit': 5000}, timeout=25)
        r.raise_for_status()
        owners_data = r.json()
        for row in owners_data:
            acc = row.get('account_number')
            if acc:
                name = row.get('owner_name') or f"{row.get('owner_first_name', '')} {row.get('owner_last_name', '')}".strip()
                if name:
                    owners_by_account.setdefault(acc, []).append(name)
    except Exception as e:
        print(f"  Warning: failed to fetch Chicago owners: {e}")

    # 2. Fetch licenses
    print("  Fetching Chicago active business licenses...")
    licenses_url = "https://data.cityofchicago.org/resource/r5kz-chrr.json"
    properties = []
    chunk_size = 2000
    offset = 0
    
    while len(properties) < limit:
        params = {
            '$limit': chunk_size,
            '$offset': offset,
            'city': 'CHICAGO',
            'license_status': 'AAI'
        }
        features = []
        retries = 3
        while retries > 0:
            try:
                r = requests.get(licenses_url, params=params, timeout=25)
                r.raise_for_status()
                features = r.json()
                break
            except Exception as e:
                retries -= 1
                if retries == 0:
                    print(f"  Failed fetching Chicago licenses chunk at offset {offset}: {e}")
                    break
                print(f"  Error fetching Chicago licenses chunk: {e}. Retrying ({3 - retries}/3)...")
                time.sleep(2)
                
        if not features:
            break
            
        for row in features:
            acc = row.get('account_number')
            owner_names = owners_by_account.get(acc, [])
            owner_name = owner_names[0] if owner_names else row.get('legal_name')
            if not owner_name:
                continue
                
            lat = source_float(row.get('latitude'))
            lng = source_float(row.get('longitude'))
            
            properties.append({
                'bbl': row.get('license_id'),
                'address': row.get('address') or 'UNKNOWN ADDRESS',
                'borough': 'CHICAGO',
                'zip_code': row.get('zip_code'),
                'owner_name': owner_name,
                'owner_name_norm': owner_name.upper(),
                'mailing_address': row.get('address') or '',
                'assessed_total': None,
                'units_res': 1,
                'year_built': None,
                'land_use': row.get('license_description'),
                'bld_class': None,
                'latitude': lat,
                'longitude': lng
            })
            
        offset += chunk_size
        print(f"  Fetched {len(properties)} Chicago properties...")
        if len(features) < chunk_size:
            break
            
    return properties

def fetch_real_miami_properties(limit=5000):
    print("Fetching real properties from Miami-Dade County GIS (MD_LandInformation)...")
    url = "https://gisweb.miamidade.gov/arcgis/rest/services/MD_LandInformation/MapServer/26/query"
    properties = []
    chunk_size = 2000
    offset = 0
    
    while len(properties) < limit:
        params = {
            'where': 'TRUE_OWNER1 IS NOT NULL AND TRUE_SITE_ADDR IS NOT NULL',
            'outFields': 'FOLIO,TRUE_SITE_ADDR,TRUE_SITE_CITY,TRUE_SITE_ZIP_CODE,TRUE_OWNER1,TRUE_OWNER2,TRUE_MAILING_ADDR1,TRUE_MAILING_ADDR2,TRUE_MAILING_CITY,TRUE_MAILING_STATE,TRUE_MAILING_ZIP_CODE,YEAR_BUILT,TOTAL_VAL_CUR,UNIT_COUNT',
            'resultOffset': offset,
            'resultRecordCount': chunk_size,
            'returnGeometry': 'false',
            'f': 'json'
        }
        features = []
        retries = 3
        while retries > 0:
            try:
                r = requests.get(url, params=params, timeout=25)
                r.raise_for_status()
                data = r.json()
                features = data.get('features', [])
                break
            except Exception as e:
                retries -= 1
                if retries == 0:
                    print(f"  Failed fetching Miami chunk at offset {offset}: {e}")
                    break
                print(f"  Error fetching Miami chunk: {e}. Retrying ({3 - retries}/3)...")
                time.sleep(2)
                
        if not features:
            break
            
        for f in features:
            attrs = f.get('attributes', {})
            folio = attrs.get('FOLIO')
            if not folio or not attrs.get('TRUE_OWNER1'):
                continue
            
            # Combine owners
            owner1 = attrs.get('TRUE_OWNER1') or ''
            owner2 = attrs.get('TRUE_OWNER2') or ''
            owner_name = f"{owner1} & {owner2}".strip(" & ") if owner2 else owner1.strip()
            
            # Combine mailing address
            mail_parts = [
                attrs.get('TRUE_MAILING_ADDR1') or '',
                attrs.get('TRUE_MAILING_ADDR2') or '',
                attrs.get('TRUE_MAILING_CITY') or '',
                attrs.get('TRUE_MAILING_STATE') or '',
                attrs.get('TRUE_MAILING_ZIP_CODE') or ''
            ]
            mailing_address = ", ".join([p.strip() for p in mail_parts if p.strip()])
            
            zip_val = attrs.get('TRUE_SITE_ZIP_CODE')
            if zip_val:
                zip_val = zip_val.split('-')[0].strip()
            
            properties.append({
                'bbl': folio.strip(),
                'address': attrs.get('TRUE_SITE_ADDR').strip(),
                'borough': attrs.get('TRUE_SITE_CITY') or 'MIAMI',
                'zip_code': zip_val,
                'owner_name': owner_name,
                'owner_name_norm': owner_name.upper(),
                'mailing_address': mailing_address,
                'assessed_total': source_float(attrs.get('TOTAL_VAL_CUR')),
                'units_res': source_int(attrs.get('UNIT_COUNT')) or 1,
                'year_built': source_int(attrs.get('YEAR_BUILT')) if source_int(attrs.get('YEAR_BUILT')) and source_int(attrs.get('YEAR_BUILT')) > 0 else None,
                'land_use': None,
                'bld_class': None
            })
        offset += chunk_size
        print(f"  Fetched {len(properties)} Miami properties...")
        
    return properties

def run_union_find_clustering(properties, city):
    print(f"Running Union-Find ownership network builder for {city}...")
    parent = {}
    
    def find(x):
        curr = x
        path = []
        while parent[curr] != curr:
            path.append(curr)
            curr = parent[curr]
        for node in path:
            parent[node] = curr
        return curr
        
    def union(x, y):
        root_x = find(x)
        root_y = find(y)
        if root_x != root_y:
            parent[root_x] = root_y

    def is_public_entity(name_norm: str) -> bool:
        if not name_norm:
            return False
        n = name_norm.upper()
        keywords = [
            "MAYOR AND CITY", "MAYOR & CITY", "CITY COUNCIL", "CITY COUNCIL OF BALTIMORE",
            "DISTRICT OF COLUMBIA", "UNITED STATES OF AMERICA", "DEPT OF HOUSING",
            "HOUSING AUTHORITY", "HOUSING AUTHORITY OF BALTIMORE", "HOUSING AUTHORITY OF THE CITY OF",
            "BALTIMORE CITY", "CITY OF BALTIMORE", "STATE OF MARYLAND", "STATE OF CA",
            "STATE OF CALIFORNIA", "CITY OF LOS ANGELES", "LOS ANGELES COUNTY",
            "U.S. GOVERNMENT", "UNITED STATES GOVERNMENT", "HOUSING AUTHORITY OF LA",
            "HOUSING AUTHORITY OF DC", "D C HOUSING AUTHORITY",
            "CITY HALL", "CITY OF", "TOWN OF", "MUNICIPAL", "BOARD OF EDUCATION",
            "REDEVELOPMENT AGENCY", "DEPT OF FINANCE", "DEPARTMENT OF FINANCE",
            "100 N HOLLIDAY ST", "401 E FAYETTE ST", "FAYETTE STREET", "CALVERT STREET",
            "FAYETTE ST", "CALVERT ST", "HOLLIDAY ST"
        ]
        return any(k in n for k in keywords)

    for p in properties:
        parent[p['bbl']] = p['bbl']

    by_owner = {}
    by_mail = {}
    for p in properties:
        bbl = p['bbl']
        owner = p['owner_name_norm']
        mail = p['mailing_address'].strip().upper() if p.get('mailing_address') else None
        
        if owner and not is_public_entity(owner):
            by_owner.setdefault(owner, []).append(bbl)
        if mail and len(mail) > 6 and not is_public_entity(owner) and not is_public_entity(mail):
            by_mail.setdefault(mail, []).append(bbl)

    for owner, bbls in by_owner.items():
        first = bbls[0]
        for other in bbls[1:]:
            union(first, other)

    for mail, bbls in by_mail.items():
        first = bbls[0]
        for other in bbls[1:]:
            union(first, other)

    groups = {}
    for p in properties:
        bbl = p['bbl']
        root = find(bbl)
        groups.setdefault(root, []).append(p)
        
    print(f"  Discovered {len(groups)} unique networks for {city}.")
    return groups

def save_city_data(conn, city, network_groups):
    db_networks = []
    db_properties = []
    db_contacts = []
    db_registrations = []

    for root_bbl, group in network_groups.items():
        # Get anchor owner name
        owner_counts = {}
        for p in group:
            owner_counts[p['owner_name']] = owner_counts.get(p['owner_name'], 0) + 1
        display_name = max(owner_counts, key=owner_counts.get)
        
        network_key = f"network_{city}_{root_bbl.lower().replace(' ', '_')}"
        
        # Anchor Type
        anchor_type = "corp"
        if not any(w in display_name.upper() for w in ["LLC", "INC", "CORP", "GROUP", "PARTNERS", "HOLDINGS", "ASSOCIATES", "TRUST"]):
            anchor_type = "person"
            
        member_names = sorted(list(set(p['owner_name'] for p in group)))
        member_addresses = sorted(list(set(p['mailing_address'] for p in group if p.get('mailing_address'))))

        bbl_list = []
        reg_ids = []
        borough_summary = {}
        total_units = 0

        for p in group:
            bbl = p['bbl']
            bbl_list.append(bbl)
            
            borough = p['borough']
            borough_summary[borough] = borough_summary.get(borough, 0) + 1
            total_units += (p.get('units_res') or 0)
            
            latitude = p.get('latitude')
            longitude = p.get('longitude')

            # Property
            db_properties.append((
                bbl, p['address'], borough, p['zip_code'], p['owner_name'], p['owner_name_norm'],
                p['land_use'], p['bld_class'], None, p['units_res'], p['units_res'], p['year_built'], p['assessed_total'],
                latitude, longitude
            ))

        db_networks.append((
            network_key, anchor_type, display_name, member_names, member_addresses,
            reg_ids, bbl_list, len(bbl_list), total_units, json.dumps(borough_summary),
            '{}'
        ))

    # Bulk insert
    with conn.cursor() as cur:
        print(f"Clearing old database records for {city}...")
        cur.execute(f"TRUNCATE TABLE {city}_networks, {city}_properties, {city}_hpd_contacts, {city}_hpd_registrations, {city}_bbl_stats CASCADE;")
        
        print(f"Inserting populated networks for {city}...")
        execute_values(cur, f"""
            INSERT INTO {city}_networks (network_key, anchor_type, display_name, member_names, member_addresses, registration_ids, bbl_list, building_count, unit_count, borough_summary, connection_signals)
            VALUES %s ON CONFLICT (network_key) DO NOTHING;
        """, db_networks)

        print(f"Inserting properties for {city}...")
        execute_values(cur, f"""
            INSERT INTO {city}_properties (bbl, address, borough, zip_code, owner_name, owner_name_norm, land_use, bld_class, num_floors, units_res, units_total, year_built, assessed_total, latitude, longitude)
            VALUES %s ON CONFLICT (bbl) DO NOTHING;
        """, db_properties)

        if db_contacts:
            print(f"Inserting contacts for {city}...")
            execute_values(cur, f"""
                INSERT INTO {city}_hpd_contacts (registration_id, contact_type, corporation_name, corporation_name_norm, first_name, last_name, full_name, full_name_norm, business_address, business_city, business_state, business_zip)
                VALUES %s;
            """, db_contacts)

        if db_registrations:
            print(f"Inserting registrations for {city}...")
            execute_values(cur, f"""
                INSERT INTO {city}_hpd_registrations (registration_id, bbl, bin, building_address, building_city, building_zip, borough, lifecycle_stage, last_registration_date, registration_end_date)
                VALUES %s ON CONFLICT (registration_id) DO NOTHING;
            """, db_registrations)

        # Update data_source_status for the city
        cur.execute("""
            INSERT INTO data_source_status (source_name, source_type, last_refreshed_at, refresh_status, details, external_last_updated)
            VALUES (%s, %s, NOW(), %s, %s, %s)
            ON CONFLICT (source_name)
            DO UPDATE SET 
                source_type = EXCLUDED.source_type,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details,
                external_last_updated = EXCLUDED.external_last_updated;
        """, (city.upper(), 'city_dataset', 'success', json.dumps({"properties_count": len(db_properties)}), None))

        # Invalidate the completeness cache
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()
    print(f"✓ Data successfully written for {city}.")

def populate_city(conn, city):
    print("=" * 60)
    print(f"Ingesting real-world data catalog for {city.upper()}...")
    print("=" * 60)
    
    properties = []
    if city == "dc":
        properties = fetch_real_dc_properties()
    elif city == "baltimore":
        properties = fetch_real_baltimore_properties()
    elif city == "la":
        raise ValueError("LA ingestion is disabled until a complete real-world ownership source is implemented.")
    elif city == "philadelphia":
        properties = fetch_real_philadelphia_properties()
    elif city == "boston":
        properties = fetch_real_boston_properties()
    elif city == "detroit":
        properties = fetch_real_detroit_properties()
    elif city == "chicago":
        properties = fetch_real_chicago_properties()
    elif city == "miami":
        properties = fetch_real_miami_properties()

    # If live fetching failed or returned less than 1,000 properties, raise an error
    if len(properties) < 1000:
        raise ValueError(f"Failed to fetch sufficient real-world data for {city} (only found {len(properties)} properties)")
        
    network_groups = run_union_find_clustering(properties, city)
    save_city_data(conn, city, network_groups)

if __name__ == "__main__":
    import sys
    c = get_connection()
    try:
        if len(sys.argv) > 1:
            target_cities = [sys.argv[1].lower()]
        else:
            target_cities = CITIES
            
        clean_tables(c, target_cities)
        create_tables(c, target_cities)
        for city in target_cities:
            populate_city(c, city)
    finally:
        c.close()
