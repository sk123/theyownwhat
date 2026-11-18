import os
import json
import time
import re
import psycopg2
import requests
from psycopg2.extras import RealDictCursor, execute_values
from api.shared_utils import (
    US_STATE_CODES,
    extract_owner_family_key,
    get_email_match_key,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    is_registrar_address,
    load_registrar_address_substrings,
    looks_like_business_name,
    looks_like_person_owner,
    normalize_business_name,
    normalize_mailing_address_coarse,
    strip_benign_care_of_prefix,
)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
CITIES = ["dc", "baltimore", "boston", "detroit", "philadelphia", "chicago", "miami", "minneapolis"]

GENERIC_OWNER_NAMES = {
    "UNKNOWN",
    "UNKNOWN OWNER",
    "UNKNOWN OWNERS",
    "OWNER UNKNOWN",
    "UNKNOWN OWNER OF RECORD",
    "UNKNOWN HEIRS",
    "UNKNOWN HEIRS OF",
    "CURRENT OWNER",
    "OCCUPANT",
    "RESIDENT",
    "CURRENT RESIDENT",
    "REFERENCE ONLY",
    "NO INFORMATION PROVIDED",
    "NOT AVAILABLE",
    "UNAVAILABLE",
    "NAME NOT AVAILABLE",
    "NAME WITHHELD",
    "WITHHELD",
    "REDACTED",
    "CONFIDENTIAL",
    "CONFIDENTIAL OWNER",
    "CONFIDENTIAL OWNERS",
    "PRIVATE OWNER",
    "PRIVATE OWNERS",
    "NOT ON FILE",
    "UNLISTED",
    "N/A",
    "NA",
    "NONE",
    "NULL",
    "TAXPAYER",
    "TAXPAYER OF",
    "TAXPAYER OFF",
    "TAXPAYEROF",
    "TAXPAYER OF RECORD",
    "TAX PAYER",
    "TAX PAYER OF",
    "CURRENT TAXPAYER",
    "REAL ESTATE TAXPAYER",
    "R E TAXPAYER",
    "OWNER TAXPAYER",
    "OWNER",
    "OWNERS",
    "EXEMPT",
    "EXEMPT STATUS PENDING",
    "EXEMPT PROPERTY",
    "STREET",
    "CONDO MASTER",
    "CONDO MASTER RECORD",
    "CONDO MAIN",
    "MASTER CONDO",
    "VACANT",
    "VACANT VACANT",
    "SEE FICHE",
    "FICHE",
}

_GENERIC_OWNER_PREFIX_RE = re.compile(
    r"^(?:TAXPAYER\s+OF(?:\b|F\b)|TAX\s+PAYER\s+OF\b|EXEMPT\s+(?:STATUS|PROPERTY)\b|CONDO\s+(?:MASTER|MAIN)\b)",
    re.IGNORECASE,
)

_JOINT_OWNER_SPLIT_RE = re.compile(r"(?:\s*[&,;/+]\s*|\s+AND\s+)", re.IGNORECASE)


def _is_single_generic_owner_token(part: str) -> bool:
    n = normalize_owner_name(part)
    if not n or n in GENERIC_OWNER_NAMES:
        return True
    if _GENERIC_OWNER_PREFIX_RE.match(n):
        return True
    # Handle repeated space-joined redaction tokens like "NOT AVAILABLE NOT AVAILABLE"
    for phrase in (
        "NOT AVAILABLE",
        "CONFIDENTIAL OWNER",
        "PRIVATE OWNER",
        "UNKNOWN OWNER",
        "OWNER UNKNOWN",
        "NAME WITHHELD",
        "UNAVAILABLE",
        "CONFIDENTIAL",
        "REDACTED",
        "WITHHELD",
        "UNKNOWN",
    ):
        rem = re.sub(rf"\b{phrase}\b", " ", n)
        if not rem.strip():
            return True
    return False


def is_generic_owner_name(raw_name: str | None) -> bool:
    if not raw_name:
        return True
    if _is_single_generic_owner_token(raw_name):
        return True
    parts = [p.strip() for p in _JOINT_OWNER_SPLIT_RE.split(raw_name) if p and p.strip()]
    return bool(parts) and all(_is_single_generic_owner_token(p) for p in parts)

PUBLIC_ENTITY_KEYWORDS = [
    "MAYOR AND CITY", "MAYOR & CITY", "CITY COUNCIL", "CITY COUNCIL OF BALTIMORE",
    "DISTRICT OF COLUMBIA", "UNITED STATES OF AMERICA", "UNITED STATES", "DEPT OF HOUSING",
    "HOUSING AUTHORITY", "HOUSING AUTHORITY OF BALTIMORE", "HOUSING AUTHORITY OF THE CITY OF",
    "BALTIMORE CITY", "CITY OF BALTIMORE", "STATE OF MARYLAND", "STATE OF CA",
    "STATE OF CALIFORNIA", "CITY OF LOS ANGELES", "LOS ANGELES COUNTY",
    "U.S. GOVERNMENT", "UNITED STATES GOVERNMENT", "HOUSING AUTHORITY OF LA",
    "HOUSING AUTHORITY OF DC", "D C HOUSING AUTHORITY",
    "SECRETARY OF VETERANS", "SECRETARY OF HOUSING", "VETERANS AFFAIRS",
    "FANNIE MAE", "FREDDIE MAC", "FEDERAL NATIONAL MORTGAGE", "FEDERAL HOME LOAN",
    "MORTGAGE ELECTRONIC REGISTRATION",
    "COMMONWEALTH OF MASS", "STATE OF MASSACHUSETTS", "BOSTON HOUSING AUTHORITY",
    "BOSTON REDEVELOPMENT", "PLANNING AND DEVELOPMENT AGENCY",
    "STATE OF MICHIGAN", "MICHIGAN DEPT OF", "DEPT OF TRANSPORTATION", "WAYNE COUNTY",
    "DETROIT LAND BANK", "CITY OF DETROIT",
    "PHILADELPHIA HOUSING", "REDEVELOPMENT AUTHORITY", "CITY OF PHILADELPHIA",
    "COOK COUNTY", "CHICAGO HOUSING AUTHORITY", "CITY OF CHICAGO",
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
    "HIGHWAY COMMISSION", "HIGHWAY COMM", "STATE HWY", "GAME AND FISH",
    "FORESTRY COMMISSION", "CORPS OF ENGINEERS", "STATE OF ARKANSAS",
]

PUBLIC_ADDRESS_KEYWORDS = [
    "100 N HOLLIDAY ST", "401 E FAYETTE ST", "FAYETTE STREET", "CALVERT STREET",
    "FAYETTE ST", "CALVERT ST", "HOLLIDAY ST", "CITY HALL", "1 CITY HALL",
    "2 WOODWARD AVE", "500 GRISWOLD", "111 NW 1ST ST", "111 NW 1 ST",
]

MAIL_LINK_MAX_DISTINCT_OWNERS = {
    # Maximum distinct owner families allowed to link via a single fine mailing address.
    # Tightened from 25 -> 10 (plus coarse-building <= 12 and PO Box <= 4) so registered
    # agents, mortgage servicers, CPA/law offices, and UPS Store maildrops never bridge
    # unrelated landlords, while preserving numbered SFR/developer series (FKH, IH, Excel).
    "dc": 10,
    "baltimore": 10,
    "boston": 10,
    "detroit": 10,
    "philadelphia": 10,
    "chicago": 10,
    "miami": 10,
    "minneapolis": 10,
    "ma": 10,
}

MAIL_LINK_MAX_COARSE_BUILDING_FAMILIES = 12
PO_BOX_MAX_OWNER_FAMILIES = 4
MEGANET_SPLIT_MIN_BUILDINGS = 150
MEGANET_SPLIT_MAX_OWNER_FAMILIES = 30

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
    if looks_like_business_name(text):
        bnorm = normalize_business_name(text)
        return re.sub(r"^THE\s+", "", bnorm).strip() or bnorm
    text = text.upper().replace("&", " AND ")
    text = re.sub(r"[`'\".,]", "", text)
    return re.sub(r"\s+", " ", text).strip()

def normalize_link_address(value):
    text = source_text(value)
    if not text:
        return ""
    text = text.upper().replace("&", " AND ")
    text = re.sub(r"[.,]", " ", text)
    text = re.sub(r"\b(?:P\s*O\s*BOX|POBOX|P\.?O\.?\s*BOX|POST\s+OFFICE\s+BOX)\b", "PO BOX", text)
    # Peel a trailing state code off before touching unit markers: 'FL' is both
    # Florida and Floor, so an unguarded rewrite turned every Miami mailing
    # address into '... MIAMI # 33131' and destroyed the state token.
    state_tail = re.search(r"\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$", text)
    tail = ""
    if state_tail and state_tail.group(1) in US_STATE_CODES:
        tail = f" {state_tail.group(1)} {state_tail.group(2)}"
        text = text[: state_tail.start()].strip()
    # Unit markers only count when an actual unit token follows them.
    text = re.sub(
        r"\b(?:SUITE|STE|UNIT|APT|APARTMENT|ROOM|RM|FLOOR|FLR|FL)\b(?=\s*[\w#])", "#", text
    )
    text = f"{text}{tail}"
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
    raw = str(name_norm).strip()
    if is_generic_owner_name(raw):
        return True
    n = normalize_owner_name(raw)
    if is_excluded_third_party_entity(raw) or is_excluded_third_party_entity(n):
        return True
    return False

def is_public_or_generic_address(address_norm: str, registrar_substrings: tuple = (), raw_address: str = "") -> bool:
    if not address_norm and not raw_address:
        return True
    n = normalize_link_address(address_norm or raw_address)
    if not n:
        return True
    if any(k in n for k in PUBLIC_ADDRESS_KEYWORDS):
        return True
    if is_excluded_third_party_address(raw_address or address_norm, n, registrar_substrings):
        return True
    return False

def mailing_link_owner_limit(city: str) -> int:
    return MAIL_LINK_MAX_DISTINCT_OWNERS.get((city or "").lower(), 25)

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

def _parse_arcgis_date(value):
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        try:
            import datetime as dt
            return dt.datetime.fromtimestamp(value / 1000, tz=dt.timezone.utc).date()
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        import datetime as dt
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")[:26]).date()
    except Exception:
        return None


def fetch_real_baltimore_properties():
    print("Fetching real properties from Baltimore City GIS (baltegis dmxOwnership)...")
    url = "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxOwnership/MapServer/0/query"
    properties = []
    chunk_size = 2000
    last_oid = -1

    while True:
        params = {
            'where': f'OWNER_1 IS NOT NULL AND BLOCKLOT IS NOT NULL AND OBJECTID > {last_oid}',
            'outFields': 'OBJECTID,BLOCKLOT,OWNER_1,OWNER_2,OWNER_3,MAILTOADD,ZIP_CODE,DWELUNIT,FULLCASH,TAXBASE,YEAR_BUILD,FULLADDR,WARD',
            'resultRecordCount': chunk_size,
            'orderByFields': 'OBJECTID ASC',
            'returnGeometry': 'false',
            'f': 'json'
        }
        features = []
        data = {}
        retries = 3
        while retries > 0:
            try:
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                if "error" in data:
                    raise RuntimeError(f"ArcGIS error: {data['error']}")
                features = data.get('features', [])
                break
            except Exception as e:
                retries -= 1
                if retries == 0:
                    print(f"  Failed fetching Baltimore chunk after OBJECTID {last_oid}: {e}")
                    break
                print(f"  Error fetching Baltimore chunk: {e}. Retrying ({3 - retries}/3)...")
                time.sleep(2)

        if not features:
            break

        max_seen_oid = last_oid
        for f in features:
            attrs = f.get('attributes', {})
            oid = source_int(attrs.get('OBJECTID'))
            if oid is not None and oid > max_seen_oid:
                max_seen_oid = oid
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
                'assessed_total': source_float(attrs.get('FULLCASH')) or source_float(attrs.get('TAXBASE')),
                'units_res': source_int(attrs.get('DWELUNIT')),
                'year_built': source_int(attrs.get('YEAR_BUILD')),
                'land_use': None,
                'bld_class': None
            })

        if max_seen_oid <= last_oid:
            break
        last_oid = max_seen_oid
        print(f"  Fetched {len(properties)} Baltimore properties...")
        if len(features) < chunk_size and not data.get('exceededTransferLimit'):
            break

    return properties

def _fetch_philadelphia_active_rental_licenses(url: str = "https://phl.carto.com/api/v2/sql") -> dict:
    """Stream active Philadelphia L&I Rental Licenses keyed by `opa_account_num` using `cartodb_id` cursor pagination."""
    print("  Fetching active Philadelphia L&I Rental Licenses (business_licenses)...")
    by_opa = {}
    chunk_size = 50000
    last_id = 0
    session = requests.Session()
    try:
        while True:
            query = (
                "SELECT cartodb_id, licensenum, opa_account_num, licensestatus, "
                "initialissuedate, mostrecentissuedate, expirationdate, numberofunits, "
                "legalname, legalfirstname, legallastname, legalentitytype, "
                "business_name, business_mailing_address, "
                "ownercontact1name, ownercontact1mailingaddress, ownercontact1city, ownercontact1state, ownercontact1zippostalcode, "
                "ownercontact2name, ownercontact2mailingaddress, ownercontact2city, ownercontact2state, ownercontact2zippostalcode, "
                "ownerismanagingagentinphilly "
                "FROM business_licenses "
                "WHERE licensetype = 'Rental' AND licensestatus = 'Active' "
                f"AND opa_account_num IS NOT NULL AND cartodb_id > {last_id} "
                f"ORDER BY cartodb_id ASC LIMIT {chunk_size}"
            )
            rows = []
            for attempt in range(3):
                try:
                    r = session.get(url, params={"q": query}, timeout=60)
                    r.raise_for_status()
                    rows = r.json().get("rows", [])
                    break
                except Exception as e:
                    if attempt == 2:
                        print(f"  Warning: failed fetching Philly rental licenses after cartodb_id={last_id}: {e}")
                    else:
                        time.sleep(2 * (attempt + 1))
            if not rows:
                break

            max_id = last_id
            for row in rows:
                cid = source_int(row.get("cartodb_id"))
                if cid is not None and cid > max_id:
                    max_id = cid
                opa = source_text(row.get("opa_account_num"))
                lic = source_text(row.get("licensenum"))
                if not opa or not lic:
                    continue
                by_opa.setdefault(opa, []).append(row)

            if max_id <= last_id:
                break
            last_id = max_id
            if len(rows) < chunk_size:
                break
    finally:
        session.close()
    print(f"  Loaded active Philadelphia rental licenses across {len(by_opa):,} OPA parcels.")
    return by_opa


def fetch_real_philadelphia_properties():
    print("Fetching real properties from Philadelphia OPA + L&I Active Rental Licenses (Carto SQL API)...")
    url = "https://phl.carto.com/api/v2/sql"
    licenses_by_opa = _fetch_philadelphia_active_rental_licenses(url)

    properties = []
    chunk_size = 50000
    last_id = 0
    session = requests.Session()
    try:
        while True:
            query = (
                "SELECT cartodb_id, parcel_number, location, zip_code, geographic_ward, "
                "owner_1, owner_2, mailing_care_of, mailing_street, mailing_address_1, "
                "mailing_address_2, mailing_city_state, mailing_zip, market_value, "
                "year_built, number_stories, category_code, category_code_description, "
                "building_code_description, ST_Y(the_geom) as lat, ST_X(the_geom) as lng "
                "FROM opa_properties_public "
                "WHERE owner_1 IS NOT NULL AND location IS NOT NULL "
                f"AND parcel_number IS NOT NULL AND cartodb_id > {last_id} "
                f"ORDER BY cartodb_id ASC LIMIT {chunk_size}"
            )
            rows = []
            for attempt in range(3):
                try:
                    r = session.get(url, params={"q": query}, timeout=60)
                    r.raise_for_status()
                    rows = r.json().get("rows", [])
                    break
                except Exception as e:
                    if attempt == 2:
                        print(f"  Failed fetching Philadelphia properties after cartodb_id {last_id}: {e}")
                    else:
                        time.sleep(2 * (attempt + 1))

            if not rows:
                break

            max_id = last_id
            for row in rows:
                cid = source_int(row.get("cartodb_id"))
                if cid is not None and cid > max_id:
                    max_id = cid

                bbl = source_text(row.get("parcel_number"))
                addr = source_text(row.get("location"))
                owner1 = source_text(row.get("owner_1"))
                owner2 = source_text(row.get("owner_2"))
                if not bbl or not addr or not owner1:
                    continue

                care_of_lines = []
                owner_parts = [owner1]
                if owner2:
                    if is_care_of_line(owner2):
                        care_of_lines.append(owner2)
                    else:
                        owner_parts.append(owner2)
                owner = " & ".join(owner_parts).strip()
                owner_norm = normalize_owner_name(owner)
                owner_is_person = looks_like_person_owner(owner)

                mail_care = source_text(row.get("mailing_care_of"))
                if mail_care:
                    if not is_care_of_line(mail_care):
                        mail_care = f"C/O {mail_care}"
                    care_of_lines.insert(0, mail_care)

                street_primary = source_text(row.get("mailing_street")) or source_text(row.get("mailing_address_1"))
                street_secondary = source_text(row.get("mailing_address_2"))
                city_state = source_text(row.get("mailing_city_state"))
                mail_zip = source_text(row.get("mailing_zip"))
                if mail_zip:
                    mail_zip = mail_zip.split("-")[0].strip()

                mail_parts = [
                    *care_of_lines,
                    street_primary or "",
                    street_secondary or "",
                    city_state or "",
                    mail_zip or "",
                ]
                mailing_address = ", ".join(p.strip() for p in mail_parts if p and p.strip())

                zip_code = source_text(row.get("zip_code"))
                if zip_code:
                    zip_code = zip_code.split("-")[0].strip()[:5]
                ward = source_text(row.get("geographic_ward"))
                borough = f"WARD {ward.zfill(2)}" if ward and ward.isdigit() else (ward or "PHILADELPHIA")

                # Attach active Philadelphia Rental License(s) & contacts if present on this OPA parcel
                lic_rows = licenses_by_opa.get(bbl, ())
                primary_lic_id = None
                primary_issue_dt = None
                primary_exp_dt = None
                licensed_units = None
                extra_registrations = []
                extra_contacts = []

                for lrow in lic_rows:
                    lic_num = source_text(lrow.get("licensenum"))
                    if not lic_num:
                        continue
                    issue_dt = _parse_arcgis_date(lrow.get("mostrecentissuedate") or lrow.get("initialissuedate"))
                    exp_dt = _parse_arcgis_date(lrow.get("expirationdate"))
                    u_cnt = source_int(lrow.get("numberofunits"))
                    if u_cnt is not None and u_cnt > 0:
                        licensed_units = max(licensed_units or 0, u_cnt)
                    if primary_lic_id is None or (exp_dt and (primary_exp_dt is None or exp_dt > primary_exp_dt)):
                        primary_lic_id = lic_num
                        primary_issue_dt = issue_dt
                        primary_exp_dt = exp_dt

                    extra_registrations.append({
                        "registration_id": lic_num,
                        "status": source_text(lrow.get("licensestatus")) or "Active",
                        "issue_date": issue_dt,
                        "expiration_date": exp_dt,
                    })

                    legal_nm = source_text(lrow.get("legalname"))
                    legal_norm = normalize_owner_name(legal_nm)
                    deed_matches_license = bool(legal_norm and legal_norm == owner_norm)
                    is_managing_agent = (source_text(lrow.get("ownerismanagingagentinphilly")) or "").upper() == "YES"

                    # Always record the deed/legal corporate or individual owner on the registration
                    if not is_public_entity(owner):
                        extra_contacts.append({
                            "registration_id": lic_num,
                            "contact_type": "INDIVIDUALOWNER" if owner_is_person else "CORPORATEOWNER",
                            "corporation_name": None if owner_is_person else owner,
                            "full_name": owner if owner_is_person else None,
                            "business_address": source_text(lrow.get("business_mailing_address")) or street_primary,
                            "business_city": None,
                            "business_state": None,
                            "business_zip": mail_zip or zip_code,
                        })

                    # Legal individual first/last name when license matches current deed owner
                    l_first = source_text(lrow.get("legalfirstname"))
                    l_last = source_text(lrow.get("legallastname"))
                    if deed_matches_license and l_first and l_last:
                        l_full = f"{l_first} {l_last}".strip()
                        if looks_like_person_owner(l_full) and not is_public_entity(l_full):
                            extra_contacts.append({
                                "registration_id": lic_num,
                                "contact_type": "INDIVIDUALOWNER" if owner_is_person else "HEADOFFICER",
                                "corporation_name": None if owner_is_person else owner,
                                "first_name": l_first,
                                "last_name": l_last,
                                "full_name": l_full,
                                "business_address": source_text(lrow.get("business_mailing_address")) or street_primary,
                                "business_city": None,
                                "business_state": None,
                                "business_zip": mail_zip,
                            })

                    for idx_c in (1, 2):
                        cname = source_text(lrow.get(f"ownercontact{idx_c}name"))
                        if not cname or is_public_entity(cname):
                            continue
                        caddr = source_text(lrow.get(f"ownercontact{idx_c}mailingaddress"))
                        ccity = source_text(lrow.get(f"ownercontact{idx_c}city"))
                        cstate = source_text(lrow.get(f"ownercontact{idx_c}state"))
                        czip = source_text(lrow.get(f"ownercontact{idx_c}zippostalcode"))
                        c_is_person = looks_like_person_owner(cname)

                        # Crucial anti-meganet guard: when `ownerismanagingagentinphilly == 'Yes'` or
                        # the license legalname does not match the current OPA deed owner, classify as 'AGENT'
                        # rather than 'HEADOFFICER'/'INDIVIDUALOWNER'.
                        if is_managing_agent or not deed_matches_license or not c_is_person:
                            ctype = "AGENT"
                            corp_val = owner if not owner_is_person else (cname if not c_is_person else None)
                            full_val = cname if c_is_person else None
                        else:
                            ctype = "INDIVIDUALOWNER" if owner_is_person else "HEADOFFICER"
                            corp_val = None if owner_is_person else owner
                            full_val = cname

                        extra_contacts.append({
                            "registration_id": lic_num,
                            "contact_type": ctype,
                            "corporation_name": corp_val,
                            "full_name": full_val,
                            "business_address": caddr,
                            "business_city": ccity,
                            "business_state": cstate,
                            "business_zip": czip,
                        })

                properties.append({
                    "bbl": bbl,
                    "address": addr,
                    "borough": borough,
                    "zip_code": zip_code,
                    "owner_name": owner,
                    "owner_name_norm": owner.upper(),
                    "mailing_address": mailing_address,
                    "assessed_total": source_float(row.get("market_value")),
                    "num_floors": source_float(row.get("number_stories")),
                    "units_res": licensed_units,
                    "year_built": source_int(row.get("year_built")),
                    "land_use": source_text(row.get("category_code_description")) or source_text(row.get("category_code")),
                    "bld_class": source_text(row.get("building_code_description")),
                    "latitude": source_float(row.get("lat")),
                    "longitude": source_float(row.get("lng")),
                    "compliance_active": bool(primary_lic_id),
                    "compliance_record_id": primary_lic_id,
                    "compliance_expiration": primary_exp_dt,
                    "issue_date": primary_issue_dt,
                    "extra_registrations": extra_registrations,
                    "extra_contacts": extra_contacts,
                })

            if max_id <= last_id:
                break
            last_id = max_id
            print(f"  Fetched {len(properties):,} Philadelphia properties...")
            if len(rows) < chunk_size:
                break
    finally:
        session.close()

    print(f"  Total fetched {len(properties):,} Philadelphia properties.")
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

def _cook_county_land_use_label(prop_class: str | None) -> str | None:
    if not prop_class:
        return None
    c = prop_class.strip().upper()
    if c == "EX":
        return "EXEMPT"
    if c == "299":
        return "RESIDENTIAL CONDOMINIUM"
    if c == "211":
        return "APARTMENT 2-6 UNITS"
    if c == "212":
        return "MIXED-USE RESIDENTIAL/COMMERCIAL"
    if c.startswith("3"):
        return "MULTIFAMILY 7+ UNITS"
    if c.startswith("2"):
        return "RESIDENTIAL 1-6 UNITS"
    if c.startswith("5"):
        return "COMMERCIAL / INDUSTRIAL"
    if c.startswith("1"):
        return "VACANT LAND"
    return f"CLASS {c}"


def _stream_cook_socrata_by_pin(dataset_id: str, base_where: str, select_fields: str, label: str) -> list[dict]:
    url = f"https://datacatalog.cookcountyil.gov/resource/{dataset_id}.json"
    chunk_size = 50000
    last_pin = ""
    out = []
    session = requests.Session()
    try:
        while True:
            where = base_where if not last_pin else f"({base_where}) AND pin > '{last_pin}'"
            params = {
                "$select": select_fields,
                "$where": where,
                "$order": "pin ASC",
                "$limit": chunk_size,
            }
            batch = []
            for attempt in range(4):
                try:
                    r = session.get(url, params=params, timeout=60)
                    r.raise_for_status()
                    batch = r.json()
                    break
                except Exception as e:
                    if attempt == 3:
                        raise RuntimeError(f"Failed streaming Cook County {label} ({dataset_id}) after pin={last_pin}: {e}") from e
                    time.sleep(2 * (attempt + 1))
            if not batch:
                break
            out.extend(batch)
            next_pin = str(batch[-1].get("pin") or "").strip()
            if not next_pin or next_pin <= last_pin:
                break
            last_pin = next_pin
            print(f"  [{label}] streamed {len(out):,} Cook County rows...")
            if len(batch) < chunk_size:
                break
    finally:
        session.close()
    return out


def fetch_real_chicago_properties():
    """Fetch City of Chicago real property tax parcels from official Cook County Assessor open data:
      - `3723-97qp` (Assessor - Parcel Addresses, latest assessment year)
      - `pabr-t5kh` (Assessor - Parcel Universe: WGS84 lat/lon, ward, community area)
      - `uzyt-m557` (Assessor - Assessed Values: property class & certified/mailed total value)
    """
    from concurrent.futures import ThreadPoolExecutor

    print("Fetching City of Chicago property parcels from Cook County Assessor Open Data...")
    # Discover latest available year in `3723-97qp` metadata (fast O(1) view check, fallback '2026')
    target_year = "2026"
    try:
        meta = requests.get("https://datacatalog.cookcountyil.gov/api/views/3723-97qp.json", timeout=15).json()
        for col in meta.get("columns", []):
            if col.get("fieldName") == "year":
                largest = str((col.get("cachedContents") or {}).get("largest") or "").strip()
                if largest.isdigit() and len(largest) == 4:
                    target_year = largest
                break
    except Exception:
        pass
    print(f"  Using Cook County Assessor tax year: {target_year}")

    addr_where = f"year='{target_year}' AND prop_address_city_name='CHICAGO'"
    addr_select = (
        "pin,prop_address_full,prop_address_zipcode_1,"
        "mail_address_name,mail_address_full,mail_address_city_name,mail_address_state,mail_address_zipcode_1,"
        "owner_address_name,owner_address_full,owner_address_city_name,owner_address_state,owner_address_zipcode_1"
    )
    univ_where = "chicago_community_area_name IS NOT NULL"
    univ_select = "pin,lat,lon,ward_num,chicago_community_area_name,zip_code"
    val_where = f"year='{target_year}' AND township_code in ('70','71','72','73','74','75','76','77')"
    val_select = "pin,class,certified_tot,mailed_tot"

    with ThreadPoolExecutor(max_workers=3) as pool:
        fut_addr = pool.submit(_stream_cook_socrata_by_pin, "3723-97qp", addr_where, addr_select, "ParcelAddresses")
        fut_univ = pool.submit(_stream_cook_socrata_by_pin, "pabr-t5kh", univ_where, univ_select, "ParcelUniverse")
        fut_vals = pool.submit(_stream_cook_socrata_by_pin, "uzyt-m557", val_where, val_select, "AssessedValues")
        addr_rows = fut_addr.result()
        univ_rows = fut_univ.result()
        val_rows = fut_vals.result()

    univ_by_pin = {}
    for u in univ_rows:
        p = source_text(u.get("pin"))
        if p:
            univ_by_pin[p] = u

    val_by_pin = {}
    for v in val_rows:
        p = source_text(v.get("pin"))
        if p:
            val_by_pin[p] = v

    properties = []
    servicer_swaps = 0
    # Enforce City of Chicago geographic bounds (41.60..42.10, -87.95..-87.50) when coordinates exist
    for row in addr_rows:
        pin = source_text(row.get("pin"))
        prop_addr = source_text(row.get("prop_address_full"))
        if not pin or not prop_addr:
            continue

        mail_name = source_text(row.get("mail_address_name"))
        owner_alt = source_text(row.get("owner_address_name"))

        mail_addr_parts = [
            source_text(row.get("mail_address_full")) or "",
            source_text(row.get("mail_address_city_name")) or "",
            source_text(row.get("mail_address_state")) or "",
            (source_text(row.get("mail_address_zipcode_1")) or "").split("-")[0].strip(),
        ]
        mail_addr = ", ".join(p for p in mail_addr_parts if p)

        owner_addr_parts = [
            source_text(row.get("owner_address_full")) or "",
            source_text(row.get("owner_address_city_name")) or "",
            source_text(row.get("owner_address_state")) or "",
            (source_text(row.get("owner_address_zipcode_1")) or "").split("-")[0].strip(),
        ]
        owner_addr = ", ".join(p for p in owner_addr_parts if p)

        # Mortgage-escrow & 3rd-party servicer guard: Cook County Treasurer `mail_address_name`
        # often lists the mortgage servicer paying property taxes (e.g. `LOANDEPOTCOM LLC`,
        # `CORELOGIC`, `WELLS FARGO`), whereas `owner_address_name` holds the real assessee/owner!
        mail_is_servicer = (
            not mail_name
            or is_generic_owner_name(mail_name)
            or is_public_entity(mail_name)
            or (mail_addr and is_excluded_third_party_address(mail_addr))
        )
        if mail_is_servicer and owner_alt and not is_generic_owner_name(owner_alt) and not is_public_entity(owner_alt):
            chosen_owner = owner_alt
            chosen_mail = owner_addr or mail_addr
            servicer_swaps += 1
        else:
            chosen_owner = mail_name or owner_alt
            chosen_mail = mail_addr or owner_addr

        if not chosen_owner:
            continue

        u_info = univ_by_pin.get(pin, {})
        v_info = val_by_pin.get(pin, {})

        lat = source_float(u_info.get("lat"))
        lng = source_float(u_info.get("lon"))
        if lat is not None and lng is not None:
            if not (41.60 <= lat <= 42.10 and -87.95 <= lng <= -87.50):
                lat, lng = None, None

        comm_area = source_text(u_info.get("chicago_community_area_name"))
        ward = source_text(u_info.get("ward_num"))
        borough = comm_area.upper() if comm_area else (f"WARD {ward}" if ward else "CHICAGO")

        zip_code = (
            source_text(row.get("prop_address_zipcode_1"))
            or source_text(u_info.get("zip_code"))
        )
        if zip_code:
            zip_code = zip_code.split("-")[0].strip()[:5]

        prop_class = source_text(v_info.get("class"))
        assessed_val = source_float(v_info.get("certified_tot"))
        if assessed_val is None or assessed_val == 0:
            assessed_val = source_float(v_info.get("mailed_tot"))

        properties.append({
            "bbl": pin,
            "address": prop_addr,
            "borough": borough,
            "zip_code": zip_code,
            "owner_name": chosen_owner,
            "owner_name_norm": chosen_owner.upper(),
            "mailing_address": chosen_mail,
            "assessed_total": assessed_val,
            "units_res": None,
            "year_built": None,
            "land_use": _cook_county_land_use_label(prop_class),
            "bld_class": prop_class,
            "latitude": lat,
            "longitude": lng,
        })

    if servicer_swaps:
        print(f"  Recovered {servicer_swaps:,} true Chicago parcel owners where tax bill went to a mortgage servicer.")
    print(f"  Total assembled {len(properties):,} Chicago Cook County Assessor parcels.")
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
    # ArcGIS Server maxRecordCount on this FeatureServer is 2000; use OBJECTID cursor
    # pagination so we retrieve all ~23,000+ active rental licenses rather than stopping after page 1.
    chunk_size = 2000
    last_oid = -1

    while True:
        params = {
            'where': f'ownerName IS NOT NULL AND address IS NOT NULL AND OBJECTID > {last_oid}',
            'outFields': 'OBJECTID,apn,licenseNumber,category,tier,status,issueDate,expirationDate,address,ownerName,ownerAddress1,ownerAddress2,ownerCity,ownerState,ownerZip,ownerEmail,applicantName,applicantAddress1,applicantAddress2,applicantCity,applicantState,applicantZip,licensedUnits,ward,latitude,longitude',
            'resultRecordCount': chunk_size,
            'orderByFields': 'OBJECTID ASC',
            'returnGeometry': 'false',
            'f': 'json'
        }
        features = []
        data = {}
        retries = 3
        while retries > 0:
            try:
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                if "error" in data:
                    raise RuntimeError(f"ArcGIS error: {data['error']}")
                features = data.get('features', [])
                break
            except Exception as e:
                retries -= 1
                if retries == 0:
                    print(f"  Failed fetching Minneapolis chunk after OBJECTID {last_oid}: {e}")
                    break
                print(f"  Error fetching Minneapolis chunk: {e}. Retrying ({3 - retries}/3)...")
                time.sleep(2)

        if not features:
            break

        max_seen_oid = last_oid
        for f in features:
            attrs = f.get('attributes', {})
            oid = source_int(attrs.get('OBJECTID'))
            if oid is not None and oid > max_seen_oid:
                max_seen_oid = oid

            apn = source_text(attrs.get('apn'))
            lic = source_text(attrs.get('licenseNumber'))
            bbl = (apn or lic or '').strip()
            owner_raw = source_text(attrs.get('ownerName'))
            addr_raw = source_text(attrs.get('address'))
            if not bbl or not owner_raw or not addr_raw:
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

            email = source_text(attrs.get('ownerEmail')) or source_text(attrs.get('applicantEmail'))
            issue_dt = _parse_arcgis_date(attrs.get('issueDate'))
            exp_dt = _parse_arcgis_date(attrs.get('expirationDate'))
            lic_status = source_text(attrs.get('status'))
            applicant_name = source_text(attrs.get('applicantName'))
            app_street = " ".join(
                p.strip() for p in [attrs.get('applicantAddress1') or '', attrs.get('applicantAddress2') or ''] if p.strip()
            ) or None

            properties.append({
                'bbl': bbl,
                'address': addr_raw,
                'borough': f"WARD {attrs.get('ward')}" if source_text(attrs.get('ward')) and not str(attrs.get('ward')).upper().startswith('WARD') else (source_text(attrs.get('ward')) or 'MINNEAPOLIS'),
                'zip_code': source_text(attrs.get('ownerZip')) or source_text(attrs.get('applicantZip')) or None,
                'owner_name': owner_raw,
                'owner_name_norm': owner_raw.upper(),
                'mailing_address': mailing_address,
                'assessed_total': None,
                'units_res': source_int(attrs.get('licensedUnits')) or 0,
                'year_built': None,
                'land_use': source_text(attrs.get('category')) or None,
                'bld_class': source_text(attrs.get('tier')) or None,
                'latitude': source_float(attrs.get('latitude')),
                'longitude': source_float(attrs.get('longitude')),
                'email': email,
                'compliance_active': (lic_status or '').upper() in {'ACTIVE', 'ISSUED', 'APPROVED'} or bool(lic),
                'compliance_record_id': lic,
                'compliance_expiration': exp_dt,
                'registration_id': lic,
                'registration_status': lic_status,
                'issue_date': issue_dt,
                'owner_street': " ".join(p.strip() for p in [attrs.get('ownerAddress1') or '', attrs.get('ownerAddress2') or ''] if p.strip()) or None,
                'owner_city': source_text(attrs.get('ownerCity')),
                'owner_state': source_text(attrs.get('ownerState')),
                'owner_zip': source_text(attrs.get('ownerZip')),
                'applicant_name': applicant_name,
                'applicant_street': app_street,
                'applicant_city': source_text(attrs.get('applicantCity')),
                'applicant_state': source_text(attrs.get('applicantState')),
                'applicant_zip': source_text(attrs.get('applicantZip')),
            })

        if max_seen_oid <= last_oid:
            break
        last_oid = max_seen_oid
        print(f"  Fetched {len(properties)} Minneapolis properties...")
        if len(features) < chunk_size and not data.get('exceededTransferLimit'):
            break

    return properties

def run_union_find_clustering(properties, city, registrar_substrings: tuple = (), registered_agent_addrs=None):
    print(f"Running Union-Find ownership network builder for {city}...")
    ra_addrs = set(registered_agent_addrs or ())
    deduped_properties = {}
    duplicate_count = 0
    for p in properties:
        bbl = p.get('bbl')
        if not bbl:
            continue
        raw_owner = (p.get('owner_name') or '').strip()
        if not raw_owner or is_generic_owner_name(raw_owner):
            continue
        if bbl in deduped_properties:
            duplicate_count += 1
            continue
        deduped_properties[bbl] = p
    if duplicate_count:
        print(f"  Dropped {duplicate_count} duplicate parcel rows before clustering {city}.")
    all_valid_properties = list(deduped_properties.values())

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
        if domain in generic_domains:
            return True
        # Also block professional agent, law, CPA, bank, and mortgage-servicer email domains
        return get_email_match_key(email, {}) is None

    # Pre-pass: normalize all fields (needed by save_city_data for all parcels),
    # detect data-driven 3rd-party/servicer mailing addresses used by >=2 excluded
    # bank/trustee/government parcels, and filter out excluded 3rd-party entities
    # so they never form or contaminate landlord networks.
    excluded_coarse_addr_counts = {}
    eligible_properties = []
    excluded_entity_parcels = 0
    for p in all_valid_properties:
        raw_owner = (p.get('owner_name') or '').strip()
        owner = normalize_owner_name(raw_owner or p.get('owner_name_norm'))
        raw_mail = p.get('mailing_address') or ''
        clean_mail = strip_benign_care_of_prefix(raw_mail)
        mail = normalize_link_address(clean_mail)
        coarse_mail = normalize_mailing_address_coarse(clean_mail)
        email = (p.get('email') or p.get('owner_email') or '').strip().lower()
        p['owner_name_norm'] = owner
        p['mailing_address_norm'] = mail
        p['_coarse_mail'] = coarse_mail
        p['owner_email_norm'] = email

        if is_public_entity(raw_owner) or is_public_entity(owner):
            excluded_entity_parcels += 1
            if coarse_mail:
                excluded_coarse_addr_counts[coarse_mail] = excluded_coarse_addr_counts.get(coarse_mail, 0) + 1
            continue
        eligible_properties.append(p)

    contaminated_coarse_addrs = {
        caddr for caddr, cnt in excluded_coarse_addr_counts.items() if cnt >= 2
    }
    if excluded_entity_parcels:
        print(
            f"  Excluded {excluded_entity_parcels} 3rd-party/institutional parcel(s) and "
            f"{len(contaminated_coarse_addrs)} shared servicer/institutional mail hub(s) for {city}."
        )

    parent = {p['bbl']: p['bbl'] for p in eligible_properties}

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

    by_owner = {}
    by_mail = {}
    by_email = {}
    mail_families = {}
    mail_to_coarse = {}
    coarse_families = {}

    for p in eligible_properties:
        bbl = p['bbl']
        raw_owner = p.get('owner_name') or ''
        owner = p['owner_name_norm']
        family = extract_owner_family_key(raw_owner) or owner
        raw_mail = p.get('mailing_address') or ''
        mail = p['mailing_address_norm']
        coarse_mail = p.get('_coarse_mail') or ''
        email = p['owner_email_norm']

        if owner:
            by_owner.setdefault(owner, []).append(bbl)
        if (
            mail
            and len(mail) > 6
            and (not coarse_mail or (coarse_mail not in contaminated_coarse_addrs and coarse_mail not in ra_addrs))
            and not is_public_or_generic_address(mail, registrar_substrings, raw_address=raw_mail)
        ):
            by_mail.setdefault(mail, []).append(bbl)
            mail_families.setdefault(mail, set()).add(family)
            if coarse_mail:
                mail_to_coarse[mail] = coarse_mail
                coarse_families.setdefault(coarse_mail, set()).add(family)
        if email and not is_generic_email(email):
            by_email.setdefault(email, []).append(bbl)

    for owner, bbls in by_owner.items():
        first = bbls[0]
        for other in bbls[1:]:
            union(first, other)

    skipped_mail_links = 0
    owner_limit = mailing_link_owner_limit(city)
    for mail, bbls in by_mail.items():
        if owner_limit <= 0:
            skipped_mail_links += 1
            continue
        is_po_box = mail.startswith("PO BOX ")
        has_dedicated_suite = ("#" in mail) and not is_po_box
        max_fine = PO_BOX_MAX_OWNER_FAMILIES if is_po_box else (30 if has_dedicated_suite else owner_limit)
        max_coarse = 30 if has_dedicated_suite else MAIL_LINK_MAX_COARSE_BUILDING_FAMILIES
        distinct_family_count = len(mail_families.get(mail) or ())
        coarse_key = mail_to_coarse.get(mail)
        coarse_family_count = len(coarse_families.get(coarse_key) or ()) if coarse_key else distinct_family_count
        if distinct_family_count > max_fine or coarse_family_count > max_coarse:
            skipped_mail_links += 1
            continue
        first = bbls[0]
        for other in bbls[1:]:
            union(first, other)

    for email, bbls in by_email.items():
        first = bbls[0]
        for other in bbls[1:]:
            union(first, other)

    pass1_groups = {}
    for p in eligible_properties:
        bbl = p['bbl']
        pass1_groups.setdefault(find(bbl), []).append(p)

    # Pass 2: Anti-meganet guard. If transitive shared-mail hubs chain >30 unrelated
    # owner families into a >150-building cluster, re-cluster that component using
    # direct owner-name and corporate-email signals only (dropping mail bridges).
    groups = {}
    meganets_split = 0
    for root_bbl, comp_props in pass1_groups.items():
        if len(comp_props) > MEGANET_SPLIT_MIN_BUILDINGS:
            comp_fams = {
                extract_owner_family_key(p.get('owner_name')) or p.get('owner_name_norm')
                for p in comp_props
                if p.get('owner_name') or p.get('owner_name_norm')
            }
            if len(comp_fams) > MEGANET_SPLIT_MAX_OWNER_FAMILIES:
                meganets_split += 1
                sub_parent = {p['bbl']: p['bbl'] for p in comp_props}

                def sub_find(x):
                    while sub_parent[x] != x:
                        sub_parent[x] = sub_parent[sub_parent[x]]
                        x = sub_parent[x]
                    return x

                def sub_union(a, b):
                    ra, rb = sub_find(a), sub_find(b)
                    if ra != rb:
                        sub_parent[ra] = rb

                sub_by_owner = {}
                sub_by_email = {}
                for p in comp_props:
                    b = p['bbl']
                    if p.get('owner_name_norm'):
                        sub_by_owner.setdefault(p['owner_name_norm'], []).append(b)
                    if p.get('owner_email_norm') and not is_generic_email(p['owner_email_norm']):
                        sub_by_email.setdefault(p['owner_email_norm'], []).append(b)
                for b_list in sub_by_owner.values():
                    for oth in b_list[1:]:
                        sub_union(b_list[0], oth)
                for b_list in sub_by_email.values():
                    for oth in b_list[1:]:
                        sub_union(b_list[0], oth)
                for p in comp_props:
                    groups.setdefault(sub_find(p['bbl']), []).append(p)
                continue
        groups[root_bbl] = comp_props

    if skipped_mail_links:
        print(f"  Skipped {skipped_mail_links} high-cardinality/building mailing-address linkers for {city}.")
    if meganets_split:
        print(f"  Re-split {meganets_split} diffuse mail-chained mega-network(s) into direct-owner portfolios for {city}.")
    print(f"  Discovered {len(groups)} unique private landlord networks for {city}.")
    return groups

def save_city_data(conn, city, network_groups, all_properties=None):
    db_networks = []
    db_properties = []
    db_contacts = []
    db_registrations = []
    duplicate_group_rows = 0
    seen_property_bbls = set()

    def _record_property_and_regs(p):
        bbl = p.get('bbl')
        if not bbl or bbl in seen_property_bbls:
            return
        seen_property_bbls.add(bbl)
        borough = p.get('borough') or city.upper()
        latitude = p.get('latitude')
        longitude = p.get('longitude')

        reg_id = source_text(p.get('registration_id'))
        if reg_id:
            db_registrations.append((
                reg_id,
                bbl,
                None,
                p.get('address'),
                city.upper(),
                p.get('zip_code'),
                borough,
                p.get('registration_status') or 'Active',
                p.get('issue_date'),
                p.get('compliance_expiration'),
            ))
            owner_nm = source_text(p.get('owner_name'))
            if owner_nm and not is_public_entity(owner_nm):
                is_person = looks_like_person_owner(owner_nm)
                db_contacts.append((
                    reg_id,
                    'INDIVIDUALOWNER' if is_person else 'CORPORATEOWNER',
                    None if is_person else owner_nm,
                    None if is_person else normalize_owner_name(owner_nm),
                    None,
                    None,
                    owner_nm if is_person else None,
                    normalize_owner_name(owner_nm) if is_person else None,
                    p.get('owner_street') or p.get('mailing_address'),
                    p.get('owner_city'),
                    p.get('owner_state'),
                    p.get('owner_zip') or p.get('zip_code'),
                ))
            app_nm = source_text(p.get('applicant_name'))
            if app_nm and not is_public_entity(app_nm) and normalize_owner_name(app_nm) != normalize_owner_name(owner_nm):
                db_contacts.append((
                    reg_id,
                    'AGENT',
                    None if looks_like_person_owner(app_nm) else app_nm,
                    None if looks_like_person_owner(app_nm) else normalize_owner_name(app_nm),
                    None,
                    None,
                    app_nm if looks_like_person_owner(app_nm) else None,
                    normalize_owner_name(app_nm) if looks_like_person_owner(app_nm) else None,
                    p.get('applicant_street'),
                    p.get('applicant_city'),
                    p.get('applicant_state'),
                    p.get('applicant_zip'),
                ))

        for er in p.get('extra_registrations') or ():
            er_id = source_text(er.get('registration_id'))
            if er_id:
                db_registrations.append((
                    er_id,
                    bbl,
                    None,
                    p.get('address'),
                    city.upper(),
                    p.get('zip_code'),
                    borough,
                    er.get('status') or 'Active',
                    er.get('issue_date'),
                    er.get('expiration_date'),
                ))

        for ec in p.get('extra_contacts') or ():
            ec_reg = source_text(ec.get('registration_id'))
            if not ec_reg:
                continue
            ec_corp = source_text(ec.get('corporation_name'))
            ec_full = source_text(ec.get('full_name'))
            db_contacts.append((
                ec_reg,
                ec.get('contact_type') or 'AGENT',
                ec_corp,
                normalize_owner_name(ec_corp) if ec_corp else None,
                source_text(ec.get('first_name')),
                source_text(ec.get('last_name')),
                ec_full,
                normalize_owner_name(ec_full) if ec_full else None,
                source_text(ec.get('business_address')),
                source_text(ec.get('business_city')),
                source_text(ec.get('business_state')),
                source_text(ec.get('business_zip')),
            ))

        owner_norm = p.get('owner_name_norm') or normalize_owner_name(p.get('owner_name'))
        mail_norm = p.get('mailing_address_norm') or normalize_link_address(strip_benign_care_of_prefix(p.get('mailing_address')))
        email_norm = p.get('owner_email_norm') or (p.get('email') or p.get('owner_email') or '').strip().lower()
        db_properties.append((
            bbl, p.get('address'), borough, p.get('zip_code'), p.get('owner_name'), owner_norm,
            p.get('mailing_address'), mail_norm,
            p.get('email') or p.get('owner_email'), email_norm,
            p.get('land_use'), p.get('bld_class'), p.get('num_floors'), p.get('units_res'),
            p.get('units_total', p.get('units_res')), p.get('year_built'), p.get('assessed_total'),
            latitude, longitude,
            bool(p.get('compliance_active', False)),
            p.get('compliance_record_id'),
            p.get('compliance_expiration'),
        ))

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
            reg_id = source_text(p.get('registration_id'))
            if reg_id:
                reg_ids.append(reg_id)
            for er in p.get('extra_registrations') or ():
                er_id = source_text(er.get('registration_id'))
                if er_id:
                    reg_ids.append(er_id)
            _record_property_and_regs(p)

        has_mail_links = len(member_address_norms) > 0
        link_confidence = 1.0 if len(member_names) == 1 else (0.95 if primary_human_name else (0.85 if has_mail_links else 0.75))

        connection_signals = {
            "member_name_count": len(member_names),
            "member_address_count": len(member_addresses),
            "member_address_norm_count": len(member_address_norms),
            "has_mailing_address_links": has_mail_links,
            "source_human_names": source_human_names,
            "primary_human_name": primary_human_name,
            "primary_entity_name": max(entity_owner_counts, key=entity_owner_counts.get) if entity_owner_counts else None,
            "principal_status": "source_listed_person" if primary_human_name else "unresolved_entity",
            "provenance": ["OFFICIAL_MUNICIPAL_PROPERTY_RECORDS", f"{city.upper()}_OPEN_DATA"],
            "link_confidence": link_confidence,
            "data_transparency_note": "Assembled strictly from source-loaded property, tax parcel, and mailing records. No missing values inferred or generated."
        }
        db_networks.append((
            network_key, anchor_type, display_name, member_names, member_addresses,
            sorted(set(reg_ids)), bbl_list, len(bbl_list), total_units, json.dumps(borough_summary),
            json.dumps(connection_signals)
        ))

    if all_properties:
        for p in all_properties:
            _record_property_and_regs(p)

    # Bulk insert (NEVER wipe {city}_bbl_stats, and preserve externally-populated registrations/contacts)
    with conn.cursor() as cur:
        if duplicate_group_rows:
            print(f"  Dropped {duplicate_group_rows} duplicate property rows inside saved network groups for {city}.")

        print(f"Clearing old property & network records for {city} (preserving {city}_bbl_stats)...")
        tables_to_truncate = [f"{city}_networks", f"{city}_properties"]
        if db_contacts:
            tables_to_truncate.append(f"{city}_hpd_contacts")
        if db_registrations:
            tables_to_truncate.append(f"{city}_hpd_registrations")
        cur.execute(f"TRUNCATE TABLE {', '.join(tables_to_truncate)} CASCADE;")

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
                land_use, bld_class, num_floors, units_res, units_total, year_built, assessed_total, latitude, longitude,
                compliance_active, compliance_record_id, compliance_expiration
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
            "registrations_count": len(db_registrations),
            "contacts_count": len(db_contacts),
        }), None))

        # Invalidate the completeness & city stats caches
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix' OR key LIKE %s", (f"%{city}%",))
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

    registrar_substrings = load_registrar_address_substrings(conn)
    from api.oc_linker import load_oc_registered_agent_addresses
    ra_addrs = load_oc_registered_agent_addresses(conn)
    network_groups = run_union_find_clustering(
        properties, city, registrar_substrings=registrar_substrings, registered_agent_addrs=ra_addrs
    )
    save_city_data(conn, city, network_groups, all_properties=properties)
    if city in CITY_OC_STATE_MAP or any(p.get("extra_contacts") for p in properties):
        rebuild_city_networks_from_db(conn, city)


CITY_OC_STATE_MAP = {
    "philadelphia": "PA",
    "miami": "FL",
    "boston": "MA",
    "nyc": "NY",
    "dc": "DC",
    "baltimore": "MD",
    "detroit": "MI",
    "chicago": "IL",
    "minneapolis": "MN",
    "milwaukee": "WI",
}


def rebuild_city_networks_from_db(conn, city: str):
    """Rebuild {city}_networks in-place from {city}_properties (plus {city}_hpd_registrations / {city}_hpd_contacts and oc_* corporate registries) without wiping {city}_bbl_stats."""
    city = city.lower().strip()
    if not re.fullmatch(r"^[a-z][a-z0-9_]{1,23}$", city):
        raise ValueError(f"Unsupported city slug for DB network rebuild: {city}")

    print(f"Rebuilding {city}_networks in-place from {city}_properties...")
    if city == "boston":
        try:
            from ma.ingest_massgis import cross_enrich_boston_and_ma
            cross_enrich_boston_and_ma(conn)
        except Exception as ce_exc:
            conn.rollback()
            print(f"  Note: Boston <-> MA cross-enrichment skipped: {ce_exc}")
    oc_state = CITY_OC_STATE_MAP.get(city)
    oc_stats = {"evaluated": 0, "matched": 0}
    if oc_state:
        try:
            from api.oc_linker import link_jurisdiction_parcels_to_oc, resolve_oc_company_principals
            with conn.cursor() as cur:
                cur.execute(f"""
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS owner_co_name TEXT;
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS owner_is_truncated BOOLEAN DEFAULT FALSE;
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS mailing_address_coarse TEXT;
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS mailing_state TEXT;
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS last_sale_date DATE;
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS oc_jurisdiction_code TEXT;
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS oc_company_number TEXT;
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS oc_match_tier SMALLINT;
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS oc_match_confidence NUMERIC(4,3);
                    ALTER TABLE {city}_properties ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
                """)
            conn.commit()
            oc_stats = link_jurisdiction_parcels_to_oc(conn, city, oc_state, col_max_len=40)
            if oc_stats.get("matched"):
                print(
                    f"  Corporate registry linker matched {oc_stats['matched']:,} / {oc_stats['evaluated']:,} "
                    f"business parcels in {city} ({oc_state})."
                )
        except Exception as oc_exc:
            conn.rollback()
            print(f"  Note: OC corporate linking skipped for {city}: {oc_exc}")

    registrar_substrings = load_registrar_address_substrings(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                bbl, address, borough, zip_code, owner_name, owner_name_norm,
                mailing_address, mailing_address_norm, owner_email, owner_email_norm,
                land_use, bld_class, num_floors, units_res, units_total, year_built,
                assessed_total, latitude, longitude,
                oc_jurisdiction_code, oc_company_number, oc_match_tier, oc_match_confidence
            FROM {city}_properties
            WHERE bbl IS NOT NULL
        """)
        properties = [dict(r) for r in cur.fetchall()]

        # Also load official registration & owner-contact records when present
        # (e.g., D.C. DLCP Basic Business Rental Licenses, Detroit BSEED, Baltimore DHCD, Minneapolis).
        prop_owner_by_bbl = {
            p["bbl"]: normalize_owner_name(p.get("owner_name") or p.get("owner_name_norm"))
            for p in properties
            if p.get("bbl")
        }
        bbl_reg_ids = {}
        bbl_contact_humans = {}
        bbl_contact_addrs = {}
        try:
            cur.execute(f"""
                SELECT
                    r.bbl,
                    r.registration_id,
                    UPPER(COALESCE(r.lifecycle_stage, 'ACTIVE')) AS lifecycle_stage,
                    UPPER(COALESCE(c.contact_type, '')) AS contact_type,
                    c.full_name,
                    c.full_name_norm,
                    c.corporation_name,
                    c.corporation_name_norm,
                    c.business_address,
                    c.business_city,
                    c.business_state,
                    c.business_zip
                FROM {city}_hpd_registrations r
                LEFT JOIN {city}_hpd_contacts c ON c.registration_id = r.registration_id
                WHERE r.bbl IS NOT NULL
            """)
            for row in cur.fetchall():
                bbl = row["bbl"]
                reg_id = source_text(row.get("registration_id"))
                if reg_id:
                    bbl_reg_ids.setdefault(bbl, set()).add(reg_id)

                # Only use ACTIVE registrations/licenses verified against the current deed owner
                # (never Closed/Transferred/Expired historical licenses from prior owners or public entities).
                stage = row.get("lifecycle_stage") or ""
                if stage not in {"ACTIVE", "READY TO RENEW", "ISSUED", "REGISTERED", "APPROVED"}:
                    continue

                cur_deed_owner = prop_owner_by_bbl.get(bbl) or ""
                if not cur_deed_owner or cur_deed_owner in GENERIC_OWNER_NAMES or is_public_entity(cur_deed_owner):
                    continue

                ctype = row.get("contact_type") or ""
                if ctype not in {"INDIVIDUALOWNER", "HEADOFFICER", "JOINTOWNER"}:
                    continue

                corp_norm = normalize_owner_name(row.get("corporation_name_norm") or row.get("corporation_name"))
                human_raw = source_text(row.get("full_name"))
                human_norm = normalize_owner_name(row.get("full_name_norm") or human_raw)

                # Require the active license's entity name (or owner name) to match the current parcel deed owner
                # so property managers, sublessors, and condo unit licensees never bridge unrelated deed owners.
                owner_verified = (
                    (corp_norm and corp_norm == cur_deed_owner)
                    or (human_norm and human_norm == cur_deed_owner)
                )
                if not owner_verified:
                    continue

                if (
                    human_raw
                    and human_norm
                    and len(human_norm) >= 5
                    and human_norm not in GENERIC_OWNER_NAMES
                    and not is_public_entity(human_norm)
                ):
                    bbl_contact_humans.setdefault(bbl, set()).add((human_norm, human_raw))

                addr_parts = [
                    source_text(row.get("business_address")) or "",
                    source_text(row.get("business_city")) or "",
                    source_text(row.get("business_state")) or "",
                    source_text(row.get("business_zip")) or "",
                ]
                raw_contact_addr = " ".join(p for p in addr_parts if p).strip()
                clean_contact_addr = strip_benign_care_of_prefix(raw_contact_addr)
                norm_contact_addr = normalize_link_address(clean_contact_addr)
                if norm_contact_addr and len(norm_contact_addr) > 6 and not is_public_or_generic_address(norm_contact_addr, registrar_substrings, raw_address=raw_contact_addr):
                    bbl_contact_addrs.setdefault(bbl, set()).add(norm_contact_addr)
        except Exception as reg_exc:
            conn.rollback()
            print(f"  Note: registration/contact untangling skipped for {city}: {reg_exc}")

    if not properties:
        print(f"  Skipping {city}: 0 properties in {city}_properties.")
        return 0

    from api.oc_linker import load_oc_registered_agent_addresses
    ra_addrs = load_oc_registered_agent_addresses(conn)
    network_groups = run_union_find_clustering(
        properties, city, registrar_substrings=registrar_substrings, registered_agent_addrs=ra_addrs
    )

    # Second-stage untangling over verified ACTIVE license/registration owner contacts
    # with strict anti-managing-agent, anti-public-entity, and cluster-size caps.
    MAX_CONTACT_CLUSTER_BBLS = 250
    MAX_DISTINCT_OWNERS_PER_HUMAN = 12
    if bbl_contact_humans or bbl_contact_addrs:
        bbl_to_root = {}
        root_to_props = {}
        for root_bbl, group in network_groups.items():
            dedup = {}
            for p in group:
                if p.get("bbl") and p["bbl"] not in dedup:
                    dedup[p["bbl"]] = p
                    bbl_to_root[p["bbl"]] = root_bbl
            root_to_props[root_bbl] = list(dedup.values())

        parent_root = {r: r for r in root_to_props}
        comp_size = {r: len(props) for r, props in root_to_props.items()}
        comp_public = {
            r: any(is_public_entity(p.get("owner_name_norm") or p.get("owner_name") or "") for p in props)
            for r, props in root_to_props.items()
        }

        def find_root(r):
            while parent_root[r] != r:
                parent_root[r] = parent_root[parent_root[r]]
                r = parent_root[r]
            return r

        def union_roots_guarded(ra, rb) -> bool:
            ua, ub = find_root(ra), find_root(rb)
            if ua == ub:
                return False
            if comp_public.get(ua) or comp_public.get(ub):
                return False
            if comp_size[ua] + comp_size[ub] > MAX_CONTACT_CLUSTER_BBLS:
                return False
            parent_root[ua] = ub
            comp_size[ub] += comp_size[ua]
            return True

        owner_limit = min(max(mailing_link_owner_limit(city), 1), MAX_DISTINCT_OWNERS_PER_HUMAN)
        human_to_bbls = {}
        human_distinct_owners = {}
        for bbl, pairs in bbl_contact_humans.items():
            if bbl not in bbl_to_root:
                continue
            prop_owner = prop_owner_by_bbl.get(bbl, "")
            for h_norm, _ in pairs:
                human_to_bbls.setdefault(h_norm, []).append(bbl)
                if prop_owner:
                    human_distinct_owners.setdefault(h_norm, set()).add(prop_owner)

        contact_human_merges = 0
        for h_norm, bbls in human_to_bbls.items():
            if len(human_distinct_owners.get(h_norm, set())) > MAX_DISTINCT_OWNERS_PER_HUMAN:
                continue
            roots = list({bbl_to_root[b] for b in bbls if b in bbl_to_root})
            if len(roots) >= 2:
                anchor = roots[0]
                for other in roots[1:]:
                    if union_roots_guarded(anchor, other):
                        contact_human_merges += 1

        caddr_to_bbls = {}
        caddr_distinct_owners = {}
        for bbl, addrs in bbl_contact_addrs.items():
            if bbl not in bbl_to_root:
                continue
            prop_owner = prop_owner_by_bbl.get(bbl, "")
            for addr_norm in addrs:
                caddr_to_bbls.setdefault(addr_norm, []).append(bbl)
                if prop_owner:
                    caddr_distinct_owners.setdefault(addr_norm, set()).add(prop_owner)

        contact_addr_merges = 0
        if mailing_link_owner_limit(city) > 0:
            for addr_norm, bbls in caddr_to_bbls.items():
                if len(caddr_distinct_owners.get(addr_norm, set())) > owner_limit:
                    continue
                roots = list({bbl_to_root[b] for b in bbls if b in bbl_to_root})
                if len(roots) >= 2:
                    anchor = roots[0]
                    for other in roots[1:]:
                        if union_roots_guarded(anchor, other):
                            contact_addr_merges += 1

        merged_groups = {}
        for r, props in root_to_props.items():
            merged_groups.setdefault(find_root(r), []).extend(props)
        network_groups = merged_groups
        if contact_human_merges or contact_addr_merges:
            print(
                f"  Registration/license contact untangling merged {contact_human_merges} human-owner links "
                f"and {contact_addr_merges} billing-address links for {city}."
            )

    # Third-stage corporate registry untangling (`oc_companies` / `oc_officers`)
    oc_keys = list({
        (p["oc_jurisdiction_code"], p["oc_company_number"])
        for p in properties
        if p.get("oc_jurisdiction_code") and p.get("oc_company_number")
    })
    from api.oc_linker import cluster_with_oc_and_series, resolve_oc_company_principals
    oc_resolved = {}
    if oc_keys:
        try:
            oc_resolved = resolve_oc_company_principals(conn, oc_keys)
        except Exception as oc_res_exc:
            conn.rollback()
            print(f"  Note: OC principal resolution skipped for {city}: {oc_res_exc}")

    network_groups, oc_merges = cluster_with_oc_and_series(
        properties,
        network_groups,
        oc_resolved,
        is_public_fn=is_public_entity,
        generic_owner_names=GENERIC_OWNER_NAMES,
    )
    if oc_merges:
        print(f"  State corporate registry & series untangling merged {oc_merges} multi-entity links in {city}.")

    db_networks = []
    for root_bbl, group in network_groups.items():
        unique_group = {}
        for p in group:
            bbl = p.get('bbl')
            if bbl and bbl not in unique_group:
                unique_group[bbl] = p
        group = list(unique_group.values())
        if not group:
            continue

        owner_counts = {}
        for p in group:
            owner_counts[p['owner_name']] = owner_counts.get(p['owner_name'], 0) + 1
        display_name = max(owner_counts, key=owner_counts.get)
        human_owner_counts = {
            name: count for name, count in owner_counts.items()
            if looks_like_person_owner(name)
        }
        # Fold in licensed/registered human owner contacts (excluding high-cardinality managing agents)
        for p in group:
            for h_norm, h_raw in bbl_contact_humans.get(p['bbl'], ()):
                if looks_like_person_owner(h_raw):
                    human_owner_counts[h_raw] = human_owner_counts.get(h_raw, 0) + 1

        oc_company_ids = set()
        oc_holding_entities = set()
        oc_raw_hq_addrs = set()
        for p in group:
            oj, on = p.get("oc_jurisdiction_code"), p.get("oc_company_number")
            if oj and on:
                oc_company_ids.add(f"{oj}/{on}")
                info = oc_resolved.get((oj, on), {})
                for h_raw in info.get("humans", ()):
                    if looks_like_person_owner(h_raw):
                        human_owner_counts[h_raw] = human_owner_counts.get(h_raw, 0) + 1
                oc_holding_entities.update(info.get("holding_entities", ()))
                oc_raw_hq_addrs.update(info.get("raw_hq_addresses", ()))

        entity_owner_counts = {
            name: count for name, count in owner_counts.items()
            if not looks_like_person_owner(name)
        }
        source_human_names = [
            name for name, _ in sorted(
                human_owner_counts.items(), key=lambda item: (-item[1], item[0])
            )
        ]
        primary_human_name = source_human_names[0] if source_human_names else None
        network_key = f"network_{city}_{root_bbl.lower().replace(' ', '_')}"
        anchor_type = "corp"
        if not any(w in display_name.upper() for w in ["LLC", "INC", "CORP", "GROUP", "PARTNERS", "HOLDINGS", "ASSOCIATES", "TRUST"]):
            anchor_type = "person"

        member_names = sorted(list(set(p['owner_name'] for p in group if p.get('owner_name')) | oc_holding_entities))
        member_addresses = sorted(list(set(p['mailing_address'] for p in group if p.get('mailing_address')) | oc_raw_hq_addrs))
        member_address_norms = sorted(list(set(p.get('mailing_address_norm') for p in group if p.get('mailing_address_norm'))))
        bbl_list = [p['bbl'] for p in group]
        reg_ids_set = set()
        borough_summary = {}
        total_units = 0
        for p in group:
            reg_ids_set.update(bbl_reg_ids.get(p['bbl'], ()))
            borough = p.get('borough') or city.upper()
            borough_summary[borough] = borough_summary.get(borough, 0) + 1
            total_units += int(float(p.get('units_res') or 0))
        reg_ids = sorted(reg_ids_set)

        has_mail_links = len(member_address_norms) > 0
        has_license_human = any(bbl_contact_humans.get(b) for b in bbl_list)
        link_confidence = 0.99 if oc_company_ids else (1.0 if len(member_names) == 1 else (0.95 if primary_human_name else (0.85 if has_mail_links else 0.75)))
        provenance = ["OFFICIAL_MUNICIPAL_PROPERTY_RECORDS", f"{city.upper()}_OPEN_DATA"]
        if reg_ids or has_license_human:
            provenance.append(f"{city.upper()}_RENTAL_REGISTRATIONS_AND_LICENSES")
        if oc_company_ids:
            provenance.append(f"{(oc_state or city).upper()}_STATE_CORPORATE_REGISTRY")
        connection_signals = {
            "member_name_count": len(member_names),
            "member_address_count": len(member_addresses),
            "member_address_norm_count": len(member_address_norms),
            "has_mailing_address_links": has_mail_links,
            "has_registration_contact_links": bool(has_license_human),
            "oc_matched_companies": sorted(oc_company_ids)[:25],
            "source_human_names": source_human_names[:25],
            "primary_human_name": primary_human_name,
            "primary_entity_name": max(entity_owner_counts, key=entity_owner_counts.get) if entity_owner_counts else None,
            "principal_status": "source_listed_person" if primary_human_name else "unresolved_entity",
            "provenance": provenance,
            "link_confidence": link_confidence,
            "data_transparency_note": "Assembled strictly from source-loaded property, tax parcel, state corporate registry, and official rental license/registration records. No missing values inferred or generated.",
        }
        db_networks.append((
            network_key, anchor_type, display_name, member_names, member_addresses,
            reg_ids, bbl_list, len(bbl_list), total_units, json.dumps(borough_summary),
            json.dumps(connection_signals)
        ))

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {city}_networks;")
        execute_values(cur, f"""
            INSERT INTO {city}_networks (
                network_key, anchor_type, display_name, member_names, member_addresses,
                registration_ids, bbl_list, building_count, unit_count, borough_summary, connection_signals
            )
            VALUES %s ON CONFLICT (network_key) DO NOTHING;
        """, db_networks, page_size=2000)
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix' OR key LIKE %s", (f"%{city}%",))
    conn.commit()
    print(f"✓ Rebuilt {len(db_networks):,} networks in {city}_networks (preserved {city}_bbl_stats).")
    return len(db_networks)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Populate or rebuild multi-city landlord networks.")
    parser.add_argument("city", nargs="?", default="all", help="Target city or 'all'")
    parser.add_argument("--rebuild-networks", action="store_true", help="Rebuild {city}_networks from existing {city}_properties without wiping bbl_stats")
    args = parser.parse_args()

    c = get_connection()
    try:
        target_cities = CITIES if args.city.lower() == "all" else [args.city.lower()]
        if args.rebuild_networks:
            for city in target_cities:
                rebuild_city_networks_from_db(c, city)
        else:
            clean_tables(c, target_cities)
            create_tables(c, target_cities)
            for city in target_cities:
                populate_city(c, city)
    finally:
        c.close()
