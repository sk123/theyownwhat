import os
import json
import time
import re
import psycopg2
import requests
from psycopg2.extras import execute_values
from api.shared_utils import looks_like_person_owner

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
CITIES = ["dc", "baltimore", "boston", "detroit", "philadelphia", "chicago", "miami", "minneapolis"]

GENERIC_OWNER_NAMES = {
    "UNKNOWN",
    "UNKNOWN OWNER",
    "NO INFORMATION PROVIDED",
    "NOT AVAILABLE",
    "N/A",
    "NA",
    "NONE",
    "NULL",
    "TAXPAYER",
    "OWNER",
}

PUBLIC_ENTITY_KEYWORDS = [
    "MAYOR AND CITY", "MAYOR & CITY", "CITY COUNCIL", "CITY COUNCIL OF BALTIMORE",
    "DISTRICT OF COLUMBIA", "UNITED STATES OF AMERICA", "DEPT OF HOUSING",
    "HOUSING AUTHORITY", "HOUSING AUTHORITY OF BALTIMORE", "HOUSING AUTHORITY OF THE CITY OF",
    "BALTIMORE CITY", "CITY OF BALTIMORE", "STATE OF MARYLAND", "STATE OF CA",
    "STATE OF CALIFORNIA", "CITY OF LOS ANGELES", "LOS ANGELES COUNTY",
    "U.S. GOVERNMENT", "UNITED STATES GOVERNMENT", "HOUSING AUTHORITY OF LA",
    "HOUSING AUTHORITY OF DC", "D C HOUSING AUTHORITY",
    "CITY HALL", "CITY OF", "TOWN OF", "MUNICIPAL", "BOARD OF EDUCATION",
    "REDEVELOPMENT AGENCY", "DEPT OF FINANCE", "DEPARTMENT OF FINANCE",
    "HENNEPIN COUNTY", "CITY OF MINNEAPOLIS", "STATE OF MINNESOTA", "STATE OF MN",
    "MINNEAPOLIS PUBLIC HOUSING",
    "WASHINGTON METROPOLITAN AREA TRANSIT AUTHORITY", "WMATA",
    "POTOMAC ELECTRIC POWER", "PEPCO", "RAILROAD COMPANY",
    "MIAMI-DADE COUNTY", "COUNTY OF", "SCHOOL BOARD", "TRANSIT AGENCY",
    "UNIVERSITY", "COLLEGE", "SCHOOL", "ACADEMY",
    "CHURCH", "TEMPLE", "SYNAGOGUE", "CATHOLIC", "ARCHBISHOP", "DIOCESE",
    "FEDERAL", "GOVERNMENT", "DEPARTMENT", "AUTHORITY",
    "LAND BANK", "PARKS & RECREATION", "PARKS AND RECREATION",
]

PUBLIC_ADDRESS_KEYWORDS = [
    "100 N HOLLIDAY ST", "401 E FAYETTE ST", "FAYETTE STREET", "CALVERT STREET",
    "FAYETTE ST", "CALVERT ST", "HOLLIDAY ST", "CITY HALL",
]

MAIL_LINK_MAX_DISTINCT_OWNERS = {
    # Keep manager/developer office links, but stop massive agent/mailroom hubs.
    "dc": 75,
    "baltimore": 75,
    "boston": 75,
    "detroit": 75,
    "philadelphia": 75,
    # Chicago's current source is business licenses. A shared license/site
    # address is not evidence of shared property ownership.
    "chicago": 0,
    "miami": 75,
    "minneapolis": 75,
}

_MIAMI_COORD_TRANSFORMER = None

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

def is_care_of_line(value: str) -> bool:
    text = source_text(value)
    if not text:
        return False
    return bool(re.match(r"^(C/O|CARE OF|ATTN|ATTENTION|%)[\s:/-]+", text.upper()))

def miami_xy_to_wgs84(x, y):
    """Convert Miami-Dade source StatePlane coordinates to lon/lat when pyproj is available."""
    global _MIAMI_COORD_TRANSFORMER
    x_val = source_float(x)
    y_val = source_float(y)
    if x_val is None or y_val is None:
        return None, None
    try:
        if _MIAMI_COORD_TRANSFORMER is None:
            from pyproj import Transformer
            _MIAMI_COORD_TRANSFORMER = Transformer.from_crs("EPSG:2236", "EPSG:4326", always_xy=True)
        lng, lat = _MIAMI_COORD_TRANSFORMER.transform(x_val, y_val)
        return lat, lng
    except Exception:
        return None, None

def normalize_owner_name(value):
    text = source_text(value)
    if not text:
        return ""
    text = text.upper().replace("&", " AND ")
    text = re.sub(r"[`'\".,]", "", text)
    return re.sub(r"\s+", " ", text).strip()

def normalize_link_address(value):
    text = source_text(value)
    if not text:
        return ""
    text = text.upper().replace("&", " AND ")
    text = re.sub(r"[.,]", " ", text)
    text = re.sub(r"\bSUITE\b|\bSTE\b|\bUNIT\b|\bAPT\b|\bAPARTMENT\b|\bROOM\b|\bRM\b", "#", text)
    text = re.sub(r"\bFLOOR\b|\bFLR\b|\bFL\b", "FL", text)
    text = re.sub(r"\bSTREET\b", "ST", text)
    text = re.sub(r"\bAVENUE\b", "AVE", text)
    text = re.sub(r"\bROAD\b", "RD", text)
    text = re.sub(r"\bBOULEVARD\b", "BLVD", text)
    text = re.sub(r"\bNORTHWEST\b", "NW", text)
    text = re.sub(r"\bNORTHEAST\b", "NE", text)
    text = re.sub(r"\bSOUTHWEST\b", "SW", text)
    text = re.sub(r"\bSOUTHEAST\b", "SE", text)
    text = re.sub(r"\s*#\s*", " # ", text)
    return re.sub(r"\s+", " ", text).strip()

def is_public_entity(name_norm: str) -> bool:
    if not name_norm:
        return False
    n = normalize_owner_name(name_norm)
    if n in GENERIC_OWNER_NAMES:
        return True
    return any(k in n for k in PUBLIC_ENTITY_KEYWORDS)

def is_public_or_generic_address(address_norm: str) -> bool:
    if not address_norm:
        return False
    n = normalize_link_address(address_norm)
    return any(k in n for k in PUBLIC_ADDRESS_KEYWORDS)

def mailing_link_owner_limit(city: str) -> int:
    return MAIL_LINK_MAX_DISTINCT_OWNERS.get((city or "").lower(), 75)

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
                    mailing_address         TEXT,
                    mailing_address_norm    TEXT,
                    owner_email             TEXT,
                    owner_email_norm        TEXT,
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
            cur.execute(f"ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS mailing_address TEXT;")
            cur.execute(f"ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS mailing_address_norm TEXT;")
            cur.execute(f"ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS owner_email TEXT;")
            cur.execute(f"ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS owner_email_norm TEXT;")
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

def fetch_real_dc_properties():
    print("Fetching real properties from DC GIS (Computer Assisted Mass Appraisal)...")
    url = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/40/query"
    properties = []
    chunk_size = 2000
    offset = 0

    while True:
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

            mailing_address = ", ".join(
                part for part in [
                    source_text(attrs.get('ADDRESS1')),
                    source_text(attrs.get('CITYSTZIP')),
                ] if part
            )

            properties.append({
                'bbl': attrs.get('SSL').strip(),
                'address': attrs.get('PREMISEADD').strip(),
                'borough': f"WARD {attrs.get('PRMSWARD')}" if attrs.get('PRMSWARD') else "WARD UNKNOWN",
                'zip_code': zip_code,
                'owner_name': attrs.get('OWNERNAME').strip(),
                'owner_name_norm': attrs.get('OWNERNAME').strip().upper(),
                'mailing_address': mailing_address,
                'assessed_total': source_float(attrs.get('ASSESSMENT')),
                'units_res': None,
                'year_built': None,
                'land_use': attrs.get('USECODE') or None,
                'bld_class': attrs.get('USECODE') or None
            })
        offset += len(features)
        print(f"  Fetched {len(properties)} DC properties...")

    return properties

def fetch_real_baltimore_properties():
    print("Fetching real properties from Baltimore City GIS (dmxOwnership)...")
    url = "https://egis.baltimorecity.gov/egis/rest/services/Housing/dmxOwnership/MapServer/0/query"
    properties = []
    chunk_size = 2000
    offset = 0

    while True:
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
        offset += len(features)
        print(f"  Fetched {len(properties)} Baltimore properties...")

    return properties

def fetch_real_philadelphia_properties():
    print("Fetching real properties from Philadelphia OPA (Carto SQL API) with full pagination...")
    url = "https://phl.carto.com/api/v2/sql"
    properties = []
    chunk_size = 50000
    offset = 0

    while True:
        query = (
            "SELECT parcel_number, location, owner_1, owner_2, year_built, category_code, "
            "ST_Y(the_geom) as lat, ST_X(the_geom) as lng "
            "FROM opa_properties_public "
            "WHERE the_geom IS NOT NULL AND owner_1 IS NOT NULL AND location IS NOT NULL "
            f"ORDER BY parcel_number LIMIT {chunk_size} OFFSET {offset}"
        )
        try:
            r = requests.get(url, params={'q': query}, timeout=60)
            r.raise_for_status()
            data = r.json()
            rows = data.get('rows', [])
        except Exception as e:
            print(f"  Failed fetching Philadelphia properties at offset {offset}: {e}")
            break

        if not rows:
            break

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
        offset += len(rows)
        print(f"  Fetched {len(properties)} Philadelphia properties...")
        if len(rows) < chunk_size:
            break

    print(f"  Total fetched {len(properties)} Philadelphia properties.")
    return properties

def fetch_real_boston_properties():
    print("Fetching real properties from Analyze Boston (CKAN API) with pagination...")
    url = "https://data.boston.gov/api/3/action/datastore_search"
    properties = []
    chunk_size = 30000
    offset = 0

    while True:
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

def fetch_real_detroit_properties():
    print("Fetching real properties from Detroit City GIS (Parcels (Current))...")
    url = "https://services2.arcgis.com/qvkbeam7Wirps6zC/ArcGIS/rest/services/Parcels_Current/FeatureServer/0/query"
    properties = []
    chunk_size = 5000
    offset = 0

    while True:
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
        offset += len(features)
        print(f"  Fetched {len(properties)} Detroit properties...")

    return properties

def fetch_real_chicago_properties():
    print("Fetching all Chicago active business licenses and owners...")

    # 1. Fetch owners to build lookup
    print("  Fetching Chicago business owners registry...")
    owners_url = "https://data.cityofchicago.org/resource/ezma-pppn.json"
    owners_by_account = {}
    owner_chunk_size = 50000
    owner_offset = 0
    while True:
        try:
            r = requests.get(
                owners_url,
                params={
                    '$limit': owner_chunk_size,
                    '$offset': owner_offset,
                    '$order': 'account_number'
                },
                timeout=60
            )
            r.raise_for_status()
            owners_data = r.json()
        except Exception as e:
            print(f"  Warning: failed to fetch Chicago owners at offset {owner_offset}: {e}")
            break
        if not owners_data:
            break
        for row in owners_data:
            acc = row.get('account_number')
            if acc:
                name = row.get('owner_name') or f"{row.get('owner_first_name', '')} {row.get('owner_last_name', '')}".strip()
                if name:
                    owners_by_account.setdefault(acc, []).append(name)
        owner_offset += len(owners_data)
        print(f"  Fetched {owner_offset} Chicago owner rows...")
        if len(owners_data) < owner_chunk_size:
            break

    # 2. Fetch licenses
    print("  Fetching Chicago active business licenses...")
    licenses_url = "https://data.cityofchicago.org/resource/r5kz-chrr.json"
    properties = []
    chunk_size = 50000
    offset = 0

    while True:
        params = {
            '$limit': chunk_size,
            '$offset': offset,
            '$order': 'id',
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
                'bbl': row.get('id') or row.get('license_id'),
                'address': row.get('address') or 'UNKNOWN ADDRESS',
                'borough': 'CHICAGO',
                'zip_code': row.get('zip_code'),
                'owner_name': owner_name,
                'owner_name_norm': owner_name.upper(),
                'mailing_address': '',
                'assessed_total': None,
                'units_res': 0,
                'year_built': None,
                'land_use': row.get('license_description'),
                'bld_class': None,
                'latitude': lat,
                'longitude': lng
            })

        offset += len(features)
        print(f"  Fetched {len(properties)} Chicago properties...")
        if len(features) < chunk_size:
            break

    return properties

def fetch_real_miami_properties():
    print("Fetching all real properties from Miami-Dade County GIS (MD_LandInformation)...")
    url = "https://gisweb.miamidade.gov/arcgis/rest/services/MD_LandInformation/MapServer/26/query"
    properties = []
    chunk_size = 1000
    offset = 0

    while True:
        params = {
            'where': 'TRUE_OWNER1 IS NOT NULL AND TRUE_SITE_ADDR IS NOT NULL',
            'outFields': 'FOLIO,TRUE_SITE_ADDR,TRUE_SITE_CITY,TRUE_SITE_ZIP_CODE,TRUE_OWNER1,TRUE_OWNER2,TRUE_OWNER3,TRUE_MAILING_ADDR1,TRUE_MAILING_ADDR2,TRUE_MAILING_ADDR3,TRUE_MAILING_CITY,TRUE_MAILING_STATE,TRUE_MAILING_ZIP_CODE,YEAR_BUILT,TOTAL_VAL_CUR,UNIT_COUNT,BUILDING_COUNT,FLOOR_COUNT,DOR_CODE_CUR,DOR_DESC,X_COORD,Y_COORD',
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
            owner_parts = []
            care_of_lines = []
            for owner_field in ['TRUE_OWNER1', 'TRUE_OWNER2', 'TRUE_OWNER3']:
                owner_line = source_text(attrs.get(owner_field))
                if not owner_line:
                    continue
                if owner_field != 'TRUE_OWNER1' and is_care_of_line(owner_line):
                    care_of_lines.append(owner_line)
                    continue
                owner_parts.append(owner_line)
            owner_name = " & ".join(owner_parts).strip()

            # Combine mailing address
            mail_parts = [
                *care_of_lines,
                attrs.get('TRUE_MAILING_ADDR1') or '',
                attrs.get('TRUE_MAILING_ADDR2') or '',
                attrs.get('TRUE_MAILING_ADDR3') or '',
                attrs.get('TRUE_MAILING_CITY') or '',
                attrs.get('TRUE_MAILING_STATE') or '',
                attrs.get('TRUE_MAILING_ZIP_CODE') or ''
            ]
            mailing_address = ", ".join([p.strip() for p in mail_parts if p.strip()])

            zip_val = attrs.get('TRUE_SITE_ZIP_CODE')
            if zip_val:
                zip_val = zip_val.split('-')[0].strip()

            lat, lng = miami_xy_to_wgs84(attrs.get('X_COORD'), attrs.get('Y_COORD'))

            properties.append({
                'bbl': folio.strip(),
                'address': attrs.get('TRUE_SITE_ADDR').strip(),
                'borough': attrs.get('TRUE_SITE_CITY') or 'MIAMI',
                'zip_code': zip_val,
                'owner_name': owner_name,
                'owner_name_norm': owner_name.upper(),
                'mailing_address': mailing_address,
                'assessed_total': source_float(attrs.get('TOTAL_VAL_CUR')),
                'units_res': source_int(attrs.get('UNIT_COUNT')) or 0,
                'year_built': source_int(attrs.get('YEAR_BUILT')) if source_int(attrs.get('YEAR_BUILT')) and source_int(attrs.get('YEAR_BUILT')) > 0 else None,
                'land_use': attrs.get('DOR_DESC') or attrs.get('DOR_CODE_CUR') or None,
                'bld_class': attrs.get('DOR_CODE_CUR') or None,
                'num_floors': source_int(attrs.get('FLOOR_COUNT')),
                'latitude': lat,
                'longitude': lng
            })
        offset += len(features)
        print(f"  Fetched {len(properties)} Miami properties...")

    return properties

def fetch_real_minneapolis_properties():
    print("Fetching real properties from Minneapolis Active Rental Licenses (ArcGIS FeatureServer)...")
    url = "https://services.arcgis.com/afSMGVsC7QlRK1kZ/ArcGIS/rest/services/Active_Rental_Licenses/FeatureServer/0/query"
    properties = []
    chunk_size = 5000
    offset = 0

    while True:
        params = {
            'where': 'ownerName IS NOT NULL AND address IS NOT NULL',
            'outFields': 'apn,licenseNumber,category,tier,status,issueDate,expirationDate,address,ownerName,ownerAddress1,ownerAddress2,ownerCity,ownerState,ownerZip,ownerEmail,applicantName,applicantAddress1,applicantAddress2,applicantCity,applicantState,applicantZip,licensedUnits,ward,latitude,longitude',
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
                    print(f"  Failed fetching Minneapolis chunk at offset {offset}: {e}")
                    break
                print(f"  Error fetching Minneapolis chunk: {e}. Retrying ({3 - retries}/3)...")
                time.sleep(2)

        if not features:
            break

        for f in features:
            attrs = f.get('attributes', {})
            apn = attrs.get('apn')
            lic = attrs.get('licenseNumber')
            bbl = (apn or lic or '').strip()
            if not bbl or not attrs.get('ownerName'):
                continue

            # Combine mailing address
            mail_parts = [
                attrs.get('ownerAddress1') or '',
                attrs.get('ownerAddress2') or '',
                attrs.get('ownerCity') or '',
                attrs.get('ownerState') or '',
                attrs.get('ownerZip') or ''
            ]
            mailing_address = ", ".join([p.strip() for p in mail_parts if p.strip()])

            # Email fields (for grouping, if we want to also group by email)
            email = attrs.get('ownerEmail') or attrs.get('applicantEmail')

            properties.append({
                'bbl': bbl,
                'address': attrs.get('address').strip(),
                'borough': attrs.get('ward') or 'MINNEAPOLIS',
                'zip_code': attrs.get('ownerZip') or attrs.get('applicantZip') or None,
                'owner_name': attrs.get('ownerName').strip(),
                'owner_name_norm': attrs.get('ownerName').strip().upper(),
                'mailing_address': mailing_address,
                'assessed_total': None,
                'units_res': source_int(attrs.get('licensedUnits')) or 0,
                'year_built': None,
                'land_use': attrs.get('category') or None,
                'bld_class': attrs.get('tier') or None,
                'latitude': source_float(attrs.get('latitude')),
                'longitude': source_float(attrs.get('longitude')),
                'email': email
            })
        offset += len(features)
        print(f"  Fetched {len(properties)} Minneapolis properties...")
        if len(features) < chunk_size:
            break

    return properties

def run_union_find_clustering(properties, city):
    print(f"Running Union-Find ownership network builder for {city}...")
    deduped_properties = {}
    duplicate_count = 0
    for p in properties:
        bbl = p.get('bbl')
        if not bbl:
            continue
        if bbl in deduped_properties:
            duplicate_count += 1
            continue
        deduped_properties[bbl] = p
    if duplicate_count:
        print(f"  Dropped {duplicate_count} duplicate parcel rows before clustering {city}.")
    properties = list(deduped_properties.values())

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

    def is_generic_email(email: str) -> bool:
        if not email:
            return True
        email = email.lower().strip()
        if '@' not in email:
            return True
        domain = email.split('@')[-1]
        generic_domains = {
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
            'icloud.com', 'comcast.net', 'msn.com', 'live.com', 'charter.net',
            'sbcglobal.net', 'att.net', 'verizon.net', 'cox.net', 'bellsouth.net',
            'me.com', 'mac.com', 'mail.com', 'yandex.com', 'protonmail.com', 'proton.me',
            'centurylink.net', 'comcast.com', 'q.com', 'uslink.net', 'earthlink.net', 'juno.com'
        }
        return domain in generic_domains

    for p in properties:
        parent[p['bbl']] = p['bbl']

    by_owner = {}
    by_mail = {}
    by_email = {}
    mail_owners = {}
    for p in properties:
        bbl = p['bbl']
        owner = normalize_owner_name(p.get('owner_name_norm') or p.get('owner_name'))
        mail = normalize_link_address(p.get('mailing_address'))
        email = (p.get('email') or p.get('owner_email') or '').strip().lower()
        p['owner_name_norm'] = owner
        p['mailing_address_norm'] = mail
        p['owner_email_norm'] = email

        if owner and not is_public_entity(owner):
            by_owner.setdefault(owner, []).append(bbl)
        if mail and len(mail) > 6 and not is_public_entity(owner) and not is_public_or_generic_address(mail):
            by_mail.setdefault(mail, []).append(bbl)
            mail_owners.setdefault(mail, set()).add(owner)
        if email and not is_generic_email(email) and not is_public_entity(owner):
            by_email.setdefault(email.lower().strip(), []).append(bbl)

    for owner, bbls in by_owner.items():
        first = bbls[0]
        for other in bbls[1:]:
            union(first, other)

    skipped_mail_links = 0
    owner_limit = mailing_link_owner_limit(city)
    for mail, bbls in by_mail.items():
        distinct_owner_count = len(mail_owners.get(mail) or [])
        if distinct_owner_count > owner_limit:
            skipped_mail_links += 1
            continue
        first = bbls[0]
        for other in bbls[1:]:
            union(first, other)

    for email, bbls in by_email.items():
        first = bbls[0]
        for other in bbls[1:]:
            union(first, other)

    groups = {}
    for p in properties:
        bbl = p['bbl']
        root = find(bbl)
        groups.setdefault(root, []).append(p)

    if skipped_mail_links:
        print(f"  Skipped {skipped_mail_links} high-cardinality mailing-address linkers for {city}.")
    print(f"  Discovered {len(groups)} unique networks for {city}.")
    return groups

def save_city_data(conn, city, network_groups):
    db_networks = []
    db_properties = []
    db_contacts = []
    db_registrations = []
    duplicate_group_rows = 0

    for root_bbl, group in network_groups.items():
        unique_group = {}
        for p in group:
            bbl = p.get('bbl')
            if not bbl:
                continue
            if bbl in unique_group:
                duplicate_group_rows += 1
                continue
            unique_group[bbl] = p
        group = list(unique_group.values())
        if not group:
            continue

        # Keep the registered entity as the stable network identity, while
        # surfacing source-listed humans separately as evidence-backed leads.
        owner_counts = {}
        for p in group:
            owner_counts[p['owner_name']] = owner_counts.get(p['owner_name'], 0) + 1
        display_name = max(owner_counts, key=owner_counts.get)
        human_owner_counts = {
            name: count for name, count in owner_counts.items()
            if looks_like_person_owner(name)
        }
        entity_owner_counts = {
            name: count for name, count in owner_counts.items()
            if name not in human_owner_counts
        }
        source_human_names = [
            name for name, _ in sorted(
                human_owner_counts.items(), key=lambda item: (-item[1], item[0])
            )
        ]
        primary_human_name = source_human_names[0] if source_human_names else None

        network_key = f"network_{city}_{root_bbl.lower().replace(' ', '_')}"

        # Anchor Type
        anchor_type = "corp"
        if not any(w in display_name.upper() for w in ["LLC", "INC", "CORP", "GROUP", "PARTNERS", "HOLDINGS", "ASSOCIATES", "TRUST"]):
            anchor_type = "person"

        member_names = sorted(list(set(p['owner_name'] for p in group)))
        member_addresses = sorted(list(set(p['mailing_address'] for p in group if p.get('mailing_address'))))
        member_address_norms = sorted(list(set(p.get('mailing_address_norm') for p in group if p.get('mailing_address_norm'))))

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
                p.get('mailing_address'), p.get('mailing_address_norm'),
                p.get('email') or p.get('owner_email'), p.get('owner_email_norm'),
                p['land_use'], p['bld_class'], p.get('num_floors'), p['units_res'], p.get('units_total', p['units_res']), p['year_built'], p['assessed_total'],
                latitude, longitude
            ))

        connection_signals = {
            "member_name_count": len(member_names),
            "member_address_count": len(member_addresses),
            "member_address_norm_count": len(member_address_norms),
            "has_mailing_address_links": len(member_address_norms) > 0,
            "source_human_names": source_human_names,
            "primary_human_name": primary_human_name,
            "primary_entity_name": max(entity_owner_counts, key=entity_owner_counts.get) if entity_owner_counts else None,
            "principal_status": "source_listed_person" if primary_human_name else "unresolved_entity",
        }
        db_networks.append((
            network_key, anchor_type, display_name, member_names, member_addresses,
            reg_ids, bbl_list, len(bbl_list), total_units, json.dumps(borough_summary),
            json.dumps(connection_signals)
        ))

    # Bulk insert
    with conn.cursor() as cur:
        if duplicate_group_rows:
            print(f"  Dropped {duplicate_group_rows} duplicate property rows inside saved network groups for {city}.")

        print(f"Clearing old database records for {city}...")
        cur.execute(f"TRUNCATE TABLE {city}_networks, {city}_properties, {city}_hpd_contacts, {city}_hpd_registrations, {city}_bbl_stats CASCADE;")

        print(f"Inserting populated networks for {city}...")
        execute_values(cur, f"""
            INSERT INTO {city}_networks (network_key, anchor_type, display_name, member_names, member_addresses, registration_ids, bbl_list, building_count, unit_count, borough_summary, connection_signals)
            VALUES %s ON CONFLICT (network_key) DO NOTHING;
        """, db_networks)

        print(f"Inserting properties for {city}...")
        execute_values(cur, f"""
            INSERT INTO {city}_properties (
                bbl, address, borough, zip_code, owner_name, owner_name_norm,
                mailing_address, mailing_address_norm, owner_email, owner_email_norm,
                land_use, bld_class, num_floors, units_res, units_total, year_built, assessed_total, latitude, longitude
            )
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
        """, (city.upper(), 'city_dataset', 'success', json.dumps({
            "properties_count": len(db_properties),
            "unique_properties_count": len({row[0] for row in db_properties}),
            "duplicate_group_rows_skipped": duplicate_group_rows,
        }), None))

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
    elif city == "minneapolis":
        properties = fetch_real_minneapolis_properties()

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
