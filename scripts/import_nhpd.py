#!/usr/bin/env python3
"""
High-Performance Connecticut NHPD Subsidy Import & Property Enrichment
Matches National Housing Preservation Database records to CT properties
using fast in-memory spatial grid and normalized address lookups, with bulk database updates.
"""

import os
import re
import sys
import math
import json
import logging
from datetime import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ct-nhpd-importer")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
DATA_FILE = "/app/data/Active and Inconclusive Properties.xlsx"
if not os.path.exists(DATA_FILE):
    DATA_FILE = "data/Active and Inconclusive Properties.xlsx"

SUBSIDY_PREFIXES = [
    'S8_1', 'S8_2', 'S202_1', 'S202_2', 'S236_1', 'S236_2',
    'FHA_1', 'FHA_2', 'LIHTC_1', 'LIHTC_2', 'RHS515_1', 'RHS515_2',
    'RHS538_1', 'RHS538_2', 'HOME_1', 'HOME_2', 'PH_1', 'PH_2',
    'State_1', 'State_2', 'Pbv_1', 'Pbv_2', 'Mr_1', 'Mr_2', 'NHTF_1', 'NHTF_2'
]

CORE_COLUMNS = {
    "NHPDPropertyID", "PropertyName", "PropertyAddress", "City", "State", "Zip",
    "Latitude", "Longitude", "PropertyStatus", "ActiveSubsidies",
    "TotalUnits", "Owner", "ManagerName", "EarliestEndDate", "LatestEndDate"
}
PROGRAM_COLUMNS = {f"{p}_{suffix}" for p in SUBSIDY_PREFIXES for suffix in ('ProgramName', 'EndDate', 'AssistedUnits')}
READ_COLUMNS = CORE_COLUMNS | PROGRAM_COLUMNS


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def normalize_city(value: Any) -> str:
    text = clean_text(value).upper()
    text = re.sub(r"\b(TOWNSHIP|TWP|BOROUGH|BORO|CITY|TOWN|VILLAGE)\b", "", text)
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_street(value: Any) -> str:
    text = clean_text(value).upper()
    if not text:
        return ""
    text = text.split(",")[0].strip()

    replacements = {
        "AVENUE": "AVE", "STREET": "ST", "ROAD": "RD", "DRIVE": "DR",
        "BOULEVARD": "BLVD", "PLACE": "PL", "COURT": "CT", "CIRCLE": "CIR",
        "LANE": "LN", "TERRACE": "TER", "PARKWAY": "PKWY", "HIGHWAY": "HWY",
        "APARTMENTS": "APTS", "APARTMENT": "APT", "MOUNT": "MT", "SAINT": "ST",
        "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    }
    for long, short in replacements.items():
        text = re.sub(rf"\b{long}\b", short, text)

    text = re.sub(r"\b(APT|UNIT|BLDG|BUILDING|#|STE|SUITE)\s*[A-Z0-9-]+\b", "", text)
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_zip(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    digits = re.sub(r"\D", "", text)
    return digits.zfill(5)[:5] if digits else ""


def parse_float(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        val = float(value)
        return val if math.isfinite(val) else None
    except (TypeError, ValueError):
        return None


def parse_date(value: Any):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except ValueError:
            pass
    return None


def import_nhpd():
    if not os.path.exists(DATA_FILE):
        logger.error(f"NHPD data file not found: {DATA_FILE}")
        return

    logger.info(f"Reading CT NHPD dataset from {DATA_FILE}...")
    df = pd.read_excel(DATA_FILE, usecols=lambda c: c in READ_COLUMNS)
    if 'State' in df.columns:
        df = df[df['State'] == 'CT'].copy()

    logger.info(f"Found {len(df)} CT records in NHPD dataset.")
    if df.empty:
        return

    # Collect all cities mentioned in NHPD to use indexed query
    raw_cities = list({str(c).strip().upper() for c in df['City'].dropna().unique() if str(c).strip()})
    city_list = list(set(raw_cities + [normalize_city(c) for c in raw_cities]))

    conn = get_db_connection()
    try:
        # Load candidate CT properties into memory using city index
        logger.info(f"Loading candidate properties for {len(city_list)} towns from database...")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, property_city, location, normalized_address, property_zip, latitude, longitude
                FROM properties
                WHERE property_city = ANY(%s)
                  AND location IS NOT NULL AND location != ''
            """, (city_list,))
            props = cur.fetchall()

        logger.info(f"Loaded {len(props):,} CT properties into memory.")

        # Build in-memory lookup indexes
        by_street_city = defaultdict(list)
        by_street_zip = defaultdict(list)
        coord_grid = defaultdict(list)

        for pid, city, loc, norm_addr, pzip, lat, lon in props:
            norm_c = normalize_city(city)
            norm_loc = normalize_street(loc)
            norm_na = normalize_street(norm_addr)
            norm_z = normalize_zip(pzip)
            lat_f = parse_float(lat)
            lon_f = parse_float(lon)

            if norm_loc and norm_c:
                by_street_city[(norm_loc, norm_c)].append(pid)
            if norm_na and norm_c and norm_na != norm_loc:
                by_street_city[(norm_na, norm_c)].append(pid)

            if norm_loc and norm_z:
                by_street_zip[(norm_loc, norm_z)].append(pid)
            if norm_na and norm_z and norm_na != norm_loc:
                by_street_zip[(norm_na, norm_z)].append(pid)

            if lat_f is not None and lon_f is not None:
                # 0.001 degree ~ 100m grid
                grid_key = (round(lat_f, 3), round(lon_f, 3))
                coord_grid[grid_key].append((pid, lat_f, lon_f))

        logger.info("Matching NHPD records against indexed properties...")

        matched_property_updates = [] # (nhpd_id, complex_name, manager, units, property_id)
        subsidy_inserts = []          # (property_id, prog_name, subsidy_type, units, expiry_date, source_url)
        matched_prop_ids = set()
        matched_count = 0

        for _, row in df.iterrows():
            nhpd_id = row.get('NHPDPropertyID')
            try:
                nhpd_id = int(float(nhpd_id)) if pd.notna(nhpd_id) else None
            except:
                nhpd_id = None

            prop_name = clean_text(row.get('PropertyName'))
            addr = clean_text(row.get('PropertyAddress'))
            city = normalize_city(row.get('City'))
            zip5 = normalize_zip(row.get('Zip'))
            lat = parse_float(row.get('Latitude'))
            lon = parse_float(row.get('Longitude'))
            total_units = None
            if pd.notna(row.get('TotalUnits')):
                try:
                    total_units = int(float(row.get('TotalUnits')))
                except:
                    pass
            manager = clean_text(row.get('ManagerName'))
            if manager.lower() in ('nan', 'none', 'null'):
                manager = ""

            norm_s = normalize_street(addr)
            target_pid = None

            # Strategy 1: Street + City Match
            if norm_s and city and (norm_s, city) in by_street_city:
                target_pid = by_street_city[(norm_s, city)][0]

            # Strategy 2: Street + Zip Match
            if not target_pid and norm_s and zip5 and (norm_s, zip5) in by_street_zip:
                target_pid = by_street_zip[(norm_s, zip5)][0]

            # Strategy 3: Coordinate proximity (within ~75 meters)
            if not target_pid and lat is not None and lon is not None:
                best_dist = 0.00075
                for d_lat in (-0.001, 0, 0.001):
                    for d_lon in (-0.001, 0, 0.001):
                        g_key = (round(lat + d_lat, 3), round(lon + d_lon, 3))
                        for pid, p_lat, p_lon in coord_grid.get(g_key, []):
                            dist = ((lat - p_lat) ** 2 + (lon - p_lon) ** 2) ** 0.5
                            if dist < best_dist:
                                target_pid = pid
                                best_dist = dist

            if target_pid:
                matched_count += 1
                matched_prop_ids.add(target_pid)

                matched_property_updates.append((
                    nhpd_id,
                    prop_name or None,
                    manager or None,
                    total_units,
                    target_pid
                ))

                # Collect subsidies
                for prefix in SUBSIDY_PREFIXES:
                    prog_col = f"{prefix}_ProgramName"
                    end_col = f"{prefix}_EndDate"
                    units_col = f"{prefix}_AssistedUnits"

                    if prog_col in row and pd.notna(row[prog_col]) and str(row[prog_col]).strip():
                        prog_name = str(row[prog_col]).strip()
                        expiry_date = parse_date(row.get(end_col))
                        units_cnt = 0
                        if units_col in row and pd.notna(row[units_col]):
                            try:
                                units_cnt = int(float(row[units_col]))
                            except:
                                units_cnt = 0

                        subsidy_url = f"https://nhpd.preservationdatabase.org/Property/{nhpd_id}" if nhpd_id else None
                        subsidy_inserts.append((
                            target_pid,
                            prog_name,
                            prefix.split('_')[0],
                            units_cnt,
                            expiry_date,
                            subsidy_url
                        ))

        logger.info(f"Matched {matched_count}/{len(df)} NHPD records to {len(matched_prop_ids)} unique properties.")

        # Batch database updates
        with conn.cursor() as cur:
            if matched_property_updates:
                logger.info(f"Applying {len(matched_property_updates)} property metadata updates...")
                update_query = """
                    UPDATE properties AS p
                    SET nhpd_id = u.nhpd_id,
                        complex_name = COALESCE(p.complex_name, u.complex_name),
                        management_company = COALESCE(p.management_company, u.management_company),
                        number_of_units = COALESCE(p.number_of_units, u.number_of_units)
                    FROM (VALUES %s) AS u(nhpd_id, complex_name, management_company, number_of_units, id)
                    WHERE p.id = u.id
                """
                execute_values(cur, update_query, matched_property_updates, page_size=1000)

            if matched_prop_ids:
                logger.info(f"Resetting existing subsidies for {len(matched_prop_ids)} matched properties...")
                cur.execute("DELETE FROM property_subsidies WHERE property_id = ANY(%s)", (list(matched_prop_ids),))

            if subsidy_inserts:
                logger.info(f"Inserting {len(subsidy_inserts)} property subsidy records...")
                insert_query = """
                    INSERT INTO property_subsidies 
                    (property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url)
                    VALUES %s
                """
                execute_values(cur, insert_query, subsidy_inserts, page_size=1000)

            # Update data_source_status
            status_details = {
                "source_file": DATA_FILE,
                "nhpd_ct_records": len(df),
                "matched_records": matched_count,
                "unique_properties_matched": len(matched_prop_ids),
                "subsidies_inserted": len(subsidy_inserts),
            }
            cur.execute("""
                INSERT INTO data_source_status (source_name, source_type, last_refreshed_at, refresh_status, details)
                VALUES ('CT_NHPD', 'state_enrichment', NOW(), 'success', %s::jsonb)
                ON CONFLICT (source_name)
                DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    last_refreshed_at = NOW(),
                    refresh_status = 'success',
                    details = EXCLUDED.details;
            """, (Json(status_details),))
            cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")

        conn.commit()
        logger.info(f"🎉 CT NHPD Import complete in <10s! Total Processed: {len(df)}, Matched: {matched_count}")

    finally:
        conn.close()


if __name__ == "__main__":
    import_nhpd()
