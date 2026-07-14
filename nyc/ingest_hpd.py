#!/usr/bin/env python3
"""
nyc/ingest_hpd.py
=================
Ingests NYC HPD building registration data and PLUTO residential parcels
into the nyc_* tables.

Data sources (all free Socrata, no auth required):
  - HPD Registrations:  https://data.cityofnewyork.us/resource/tesw-yqqr.json
  - HPD Contacts:       https://data.cityofnewyork.us/resource/feu5-w2e2.json
  - PLUTO (MapPLUTO):   https://data.cityofnewyork.us/resource/64uk-42ks.json

Usage:
    # Full ingest
    DATABASE_URL=... python -m nyc.ingest_hpd

    # Dry run (fetch only, no DB writes)
    DATABASE_URL=... python -m nyc.ingest_hpd --dry-run

    # Limit rows for testing
    DATABASE_URL=... python -m nyc.ingest_hpd --limit 2000 --dry-run

    # Re-run specific step only
    DATABASE_URL=... python -m nyc.ingest_hpd --step registrations
    DATABASE_URL=... python -m nyc.ingest_hpd --step contacts
    DATABASE_URL=... python -m nyc.ingest_hpd --step pluto
"""

import os
import sys
import argparse
import logging
import json
import time
from datetime import date
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
import requests

# Allow running from project root or as a module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.shared_utils import normalize_business_name, normalize_person_name

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("nyc-ingest")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")

# Socrata endpoints
HPD_REGISTRATIONS_URL = "https://data.cityofnewyork.us/resource/tesw-yqqr.json"
HPD_CONTACTS_URL      = "https://data.cityofnewyork.us/resource/feu5-w2e2.json"
PLUTO_URL             = "https://data.cityofnewyork.us/resource/64uk-42ks.json"

# Only ingest contacts of these types — they carry real people/entities
ACTIONABLE_CONTACT_TYPES = {"headofficer", "individualowner", "corporateowner", "agent", "officer"}

# PLUTO residential land-use codes (01–04)
RESIDENTIAL_LAND_USES = {"01", "02", "03", "04"}

# Socrata page size (max 50000)
PAGE_SIZE = 50_000

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(DATABASE_URL)


def apply_schema(conn):
    """Apply schema.sql if tables don't yet exist."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("Schema applied.")


def update_ingest_status(conn, source_name: str, status: str, details: Optional[str] = None):
    """Write ingest status to the existing data_source_status table."""
    try:
        detail_json = json.dumps({"message": details}) if details else None
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_source_status (source_name, source_type, last_refreshed_at, refresh_status, details)
                VALUES (%s, 'nyc', NOW(), %s, %s::jsonb)
                ON CONFLICT (source_name)
                DO UPDATE SET
                    last_refreshed_at = NOW(),
                    refresh_status    = EXCLUDED.refresh_status,
                    details           = COALESCE(EXCLUDED.details, data_source_status.details);
                """,
                (source_name, status, detail_json),
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Could not update data_source_status: {e}")


# ---------------------------------------------------------------------------
# Socrata paginator
# ---------------------------------------------------------------------------
def socrata_pages(url: str, where: Optional[str] = None, select: Optional[str] = None, limit: int = 0):
    """
    Generator that yields rows dict-by-dict from a Socrata JSON endpoint,
    paging transparently.  If `limit` > 0, stops after that many total rows.
    """
    offset = 0
    fetched = 0
    session = requests.Session()

    while True:
        page_limit = PAGE_SIZE
        if limit > 0:
            remaining = limit - fetched
            if remaining <= 0:
                break
            page_limit = min(PAGE_SIZE, remaining)

        params = {"$limit": page_limit, "$offset": offset, "$order": ":id"}
        if where:
            params["$where"] = where
        if select:
            params["$select"] = select

        try:
            r = session.get(url, params=params, timeout=120)
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            logger.error(f"Socrata fetch failed at offset {offset}: {e}")
            raise

        if not rows:
            break

        for row in rows:
            yield row

        fetched += len(rows)
        offset  += len(rows)

        if len(rows) < page_limit:
            break  # last page

        logger.info(f"  … {fetched:,} rows fetched so far")
        time.sleep(0.2)  # be polite


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------
def safe_upper(val) -> str:
    return str(val).strip().upper() if val else ""


def build_bbl(borough_code, block, lot) -> Optional[str]:
    """Construct a 10-digit BBL string from PLUTO components."""
    try:
        b = str(int(borough_code)).zfill(1)
        bl = str(int(block)).zfill(5)
        l = str(int(lot)).zfill(4)
        return f"{b}{bl}{l}"
    except Exception:
        return None


def parse_date(val) -> Optional[date]:
    if not val:
        return None
    try:
        return date.fromisoformat(str(val)[:10])
    except Exception:
        return None


def norm_contact_name(row: dict) -> tuple[str, str]:
    """
    Returns (full_name, full_name_norm) for an HPD contact row.
    For persons: "LASTNAME FIRSTNAME".
    For corps (no first/last): corporation_name.
    """
    first = safe_upper(row.get("firstname") or row.get("first_name"))
    last  = safe_upper(row.get("lastname")  or row.get("last_name"))
    corp  = safe_upper(row.get("corporationname") or row.get("corporation_name"))

    if last and first:
        full = f"{last} {first}"
        return full, normalize_person_name(full)
    elif last:
        return last, normalize_person_name(last)
    elif corp:
        return corp, normalize_business_name(corp)
    return "", ""


# ---------------------------------------------------------------------------
# Step 1: Ingest HPD Registrations
# ---------------------------------------------------------------------------
def ingest_registrations(conn, dry_run: bool = False, limit: int = 0) -> int:
    logger.info("=== Step 1: HPD Registrations ===")
    update_ingest_status(conn, "NYC_HPD_REGISTRATIONS", "running", "Fetching registrations")

    # Real field names from the API (no bbl, boroname, or city fields):
    # BBL is constructed from boroid + block + lot
    # borough comes from 'boro', zip from 'zip'
    select_fields = (
        "registrationid,buildingid,boroid,boro,block,lot,bin,"
        "housenumber,streetname,zip,"
        "lastregistrationdate,registrationenddate"
    )

    rows_seen  = 0
    batch      = []
    batch_size = 5_000
    total_upserted = 0

    upsert_sql = """
        INSERT INTO nyc_hpd_registrations (
            registration_id, bbl, bin,
            building_address, building_city, building_zip, borough,
            lifecycle_stage, last_registration_date, registration_end_date
        ) VALUES %s
        ON CONFLICT (registration_id) DO UPDATE SET
            bbl                    = EXCLUDED.bbl,
            bin                    = EXCLUDED.bin,
            building_address       = EXCLUDED.building_address,
            building_city          = EXCLUDED.building_city,
            building_zip           = EXCLUDED.building_zip,
            borough                = EXCLUDED.borough,
            lifecycle_stage        = EXCLUDED.lifecycle_stage,
            last_registration_date = EXCLUDED.last_registration_date,
            registration_end_date  = EXCLUDED.registration_end_date,
            updated_at             = NOW();
    """

    def flush(batch, conn):
        if not batch or dry_run:
            return len(batch)
        # Deduplicate by registration_id (first column) — keep last seen
        seen: dict[str, tuple] = {}
        for row in batch:
            seen[row[0]] = row
        deduped = list(seen.values())
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, deduped)
        conn.commit()
        return len(deduped)

    for row in socrata_pages(HPD_REGISTRATIONS_URL, select=select_fields, limit=limit):
        reg_id = safe_upper(row.get("registrationid"))
        if not reg_id:
            continue

        house  = safe_upper(row.get("housenumber"))
        street = safe_upper(row.get("streetname"))
        addr   = f"{house} {street}".strip() if house or street else None

        # Construct BBL from boroid + block + lot
        bbl = build_bbl(row.get("boroid"), row.get("block"), row.get("lot"))

        batch.append((
            reg_id,
            bbl,
            safe_upper(row.get("bin")) or None,
            addr,
            None,  # building_city not in this endpoint
            safe_upper(row.get("zip")) or None,
            safe_upper(row.get("boro")) or None,   # e.g. "BROOKLYN"
            None,  # lifecyclestage not in this endpoint
            parse_date(row.get("lastregistrationdate")),
            parse_date(row.get("registrationenddate")),
        ))
        rows_seen += 1

        if len(batch) >= batch_size:
            total_upserted += flush(batch, conn)
            batch = []

    total_upserted += flush(batch, conn)
    logger.info(f"Registrations: {rows_seen:,} fetched, {total_upserted:,} upserted (dry_run={dry_run})")
    update_ingest_status(conn, "NYC_HPD_REGISTRATIONS", "success", f"{total_upserted} records")
    return total_upserted


# ---------------------------------------------------------------------------
# Step 2: Ingest HPD Contacts
# ---------------------------------------------------------------------------
def ingest_contacts(conn, dry_run: bool = False, limit: int = 0) -> int:
    logger.info("=== Step 2: HPD Contacts ===")
    update_ingest_status(conn, "NYC_HPD_CONTACTS", "running", "Fetching contacts")

    # Filter to actionable contact types on the Socrata side to reduce transfer volume
    type_filter = " OR ".join(
        f"type='{t}'" for t in {
            "HeadOfficer", "IndividualOwner", "CorporateOwner", "Agent", "Officer",
            "JointOwner"
        }
    )
    where = f"({type_filter})"

    select_fields = (
        "registrationcontactid,registrationid,type,"
        "corporationname,firstname,lastname,"
        "businesshousenumber,businessstreetname,businessapartment,"
        "businesscity,businessstate,businesszip"
    )

    # Clear existing contacts before re-ingest so we don't accumulate stale rows
    if not dry_run:
        logger.info("Clearing existing nyc_hpd_contacts …")
        with conn.cursor() as cur:
            cur.execute("DELETE FROM nyc_hpd_contacts;")
        conn.commit()

    upsert_sql = """
        INSERT INTO nyc_hpd_contacts (
            registration_id, contact_type,
            corporation_name, corporation_name_norm,
            first_name, last_name, full_name, full_name_norm,
            business_address, business_city, business_state, business_zip
        ) VALUES %s;
    """

    batch      = []
    batch_size = 5_000
    total_ins  = 0
    rows_seen  = 0

    def flush(batch, conn):
        if not batch or dry_run:
            return len(batch)
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, batch)
        conn.commit()
        return len(batch)

    for row in socrata_pages(HPD_CONTACTS_URL, where=where, select=select_fields, limit=limit):
        reg_id = safe_upper(row.get("registrationid"))
        if not reg_id:
            continue

        contact_type = safe_upper(row.get("type"))
        corp         = safe_upper(row.get("corporationname"))
        corp_norm    = normalize_business_name(corp) if corp else None

        full_name, full_name_norm = norm_contact_name(row)
        if not full_name:
            continue  # skip blank rows

        # Build mailing address
        house  = safe_upper(row.get("businesshousenumber"))
        street = safe_upper(row.get("businessstreetname"))
        apt    = safe_upper(row.get("businessapartment"))
        parts  = [p for p in [house, street, apt] if p]
        addr   = " ".join(parts) or None

        batch.append((
            reg_id,
            contact_type or None,
            corp  or None,
            corp_norm or None,
            safe_upper(row.get("firstname")) or None,
            safe_upper(row.get("lastname"))  or None,
            full_name,
            full_name_norm or None,
            addr,
            safe_upper(row.get("businesscity"))  or None,
            safe_upper(row.get("businessstate")) or None,
            safe_upper(row.get("businesszip"))   or None,
        ))
        rows_seen += 1

        if len(batch) >= batch_size:
            total_ins += flush(batch, conn)
            batch = []

    total_ins += flush(batch, conn)
    logger.info(f"Contacts: {rows_seen:,} fetched, {total_ins:,} inserted (dry_run={dry_run})")
    update_ingest_status(conn, "NYC_HPD_CONTACTS", "success", f"{total_ins} records")
    return total_ins


# ---------------------------------------------------------------------------
# Step 3: Ingest PLUTO (residential lots only)
# ---------------------------------------------------------------------------
def ingest_pluto(conn, dry_run: bool = False, limit: int = 0) -> int:
    logger.info("=== Step 3: PLUTO residential parcels ===")
    update_ingest_status(conn, "NYC_PLUTO", "running", "Fetching PLUTO")

    # PLUTO landuse is a bare integer (1 not 01). Residential = 1-4.
    # 1=1-2 family, 2=multifam walkup, 3=multifam elevator, 4=mixed residential
    land_use_filter = " OR ".join(f"landuse='{int(lu)}'" for lu in sorted(RESIDENTIAL_LAND_USES))
    where = f"({land_use_filter})"

    select_fields = (
        "bbl,address,borough,borocode,zipcode,ownername,landuse,bldgclass,"
        "numfloors,unitsres,unitstotal,yearbuilt,assesstot"
    )  # latitude/longitude not always present in PLUTO Socrata — skip for now

    upsert_sql = """
        INSERT INTO nyc_properties (
            bbl, address, borough, zip_code,
            owner_name, owner_name_norm,
            land_use, bld_class,
            num_floors, units_res, units_total,
            year_built, assessed_total,
            latitude, longitude
        ) VALUES %s
        ON CONFLICT (bbl) DO UPDATE SET
            address        = EXCLUDED.address,
            borough        = EXCLUDED.borough,
            zip_code       = EXCLUDED.zip_code,
            owner_name     = EXCLUDED.owner_name,
            owner_name_norm = EXCLUDED.owner_name_norm,
            land_use       = EXCLUDED.land_use,
            bld_class      = EXCLUDED.bld_class,
            num_floors     = EXCLUDED.num_floors,
            units_res      = EXCLUDED.units_res,
            units_total    = EXCLUDED.units_total,
            year_built     = EXCLUDED.year_built,
            assessed_total = EXCLUDED.assessed_total,
            latitude       = EXCLUDED.latitude,
            longitude      = EXCLUDED.longitude,
            updated_at     = NOW();
    """

    def safe_numeric(val):
        try:
            return float(val) if val not in (None, "", "nan") else None
        except Exception:
            return None

    def safe_int(val):
        try:
            return int(float(val)) if val not in (None, "", "nan") else None
        except Exception:
            return None

    batch      = []
    batch_size = 5_000
    total_ups  = 0
    rows_seen  = 0
    skipped    = 0

    def flush(batch, conn):
        if not batch or dry_run:
            return len(batch)
        seen = {}
        for r in batch:
            seen[r[0]] = r  # deduplicate by BBL
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, list(seen.values()))
        conn.commit()
        return len(seen)

    for row in socrata_pages(PLUTO_URL, where=where, select=select_fields, limit=limit):
        raw_bbl = safe_upper(row.get("bbl"))
        # PLUTO Socrata returns BBL as decimal string e.g. "2054800111.00000000"
        # Strip the decimal part to match HPD registration BBL format
        bbl = raw_bbl.split(".")[0] if raw_bbl else None
        if not bbl:
            skipped += 1
            continue

        owner      = safe_upper(row.get("ownername"))
        owner_norm = normalize_business_name(owner) if owner else None

        # Expand short borough codes to full names
        BORO_MAP = {"MN": "MANHATTAN", "BK": "BROOKLYN", "BX": "BRONX",
                    "QN": "QUEENS", "SI": "STATEN ISLAND",
                    "1": "MANHATTAN", "2": "BRONX", "3": "BROOKLYN",
                    "4": "QUEENS", "5": "STATEN ISLAND"}
        raw_boro = safe_upper(row.get("borough") or row.get("borocode") or "")
        borough_full = BORO_MAP.get(raw_boro, raw_boro) or None

        # Zero-pad landuse to 2 digits for consistent storage
        lu_raw = str(row.get("landuse") or "").strip()
        land_use_stored = lu_raw.zfill(2) if lu_raw.isdigit() else lu_raw or None

        batch.append((
            bbl,
            safe_upper(row.get("address")) or None,
            borough_full,
            safe_upper(row.get("zipcode")) or None,
            owner  or None,
            owner_norm or None,
            land_use_stored,
            safe_upper(row.get("bldgclass")) or None,
            safe_numeric(row.get("numfloors")),
            safe_numeric(row.get("unitsres")),
            safe_numeric(row.get("unitstotal")),
            safe_int(row.get("yearbuilt")),
            safe_numeric(row.get("assesstot")),
            None,  # latitude
            None,  # longitude
        ))
        rows_seen += 1

        if len(batch) >= batch_size:
            total_ups += flush(batch, conn)
            batch = []

    total_ups += flush(batch, conn)
    logger.info(
        f"PLUTO: {rows_seen:,} residential lots fetched, "
        f"{total_ups:,} upserted, {skipped:,} skipped (dry_run={dry_run})"
    )
    if rows_seen == 0:
        logger.warning("⚠️  Zero PLUTO rows fetched — check landuse filter and field names")
    update_ingest_status(conn, "NYC_PLUTO", "success", f"{total_ups} records")
    return total_ups


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def ingest_hpd(dry_run: bool = False, limit: int = 0, step: Optional[str] = None):
    """
    Public entry point — called by updater/refresh_system_data.py and CLI.

    Args:
        dry_run: Fetch data but skip all DB writes.
        limit:   Max rows per endpoint (0 = unlimited).
        step:    Run only 'registrations', 'contacts', or 'pluto'.
    """
    conn = get_conn()
    try:
        apply_schema(conn)

        run_all = step is None
        if run_all or step == "registrations":
            ingest_registrations(conn, dry_run=dry_run, limit=limit)
        if run_all or step == "contacts":
            ingest_contacts(conn, dry_run=dry_run, limit=limit)
        if run_all or step == "pluto":
            ingest_pluto(conn, dry_run=dry_run, limit=limit)

        # Update main NYC source status
        if not dry_run:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO data_source_status (source_name, source_type, last_refreshed_at, refresh_status, details, external_last_updated)
                    VALUES ('NYC', 'city_dataset', NOW(), 'success', '{"message": "Ingest HPD complete"}', NULL)
                    ON CONFLICT (source_name)
                    DO UPDATE SET 
                        last_refreshed_at = NOW(),
                        refresh_status = 'success',
                        details = EXCLUDED.details,
                        external_last_updated = EXCLUDED.external_last_updated;
                """)
            conn.commit()

        logger.info("=== HPD ingest complete ===")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest NYC HPD + PLUTO data into nyc_* tables.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but make no DB writes.")
    parser.add_argument("--limit",   type=int, default=0,    help="Max rows per endpoint (0 = all).")
    parser.add_argument(
        "--step",
        choices=["registrations", "contacts", "pluto"],
        default=None,
        help="Run only a specific step (default: all).",
    )
    args = parser.parse_args()
    ingest_hpd(dry_run=args.dry_run, limit=args.limit, step=args.step)
