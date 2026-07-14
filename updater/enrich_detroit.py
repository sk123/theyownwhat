#!/usr/bin/env python3
"""
Detroit Ingestion & Subsidy Integration Script
─────────────────────────────────────────────
Enriches detroit_properties, detroit_hpd_registrations, and detroit_bbl_stats with:
  1. Active Residential Certificates of Compliance (from BSEED)
  2. Blight Tickets (to populate violations_total, violations_open, last_violation_date)
  3. Rental Registrations (to populate detroit_hpd_registrations)
  4. NHPD Subsidy Data (from National Housing Preservation Database, matched by address)
"""

import io
import csv
import argparse
import datetime as dt
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Iterator

import psycopg2
from psycopg2.extras import execute_values, Json
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")
NHPD_USER = os.environ.get("NHPD_USER", "sk@ctfairhousing.org")
NHPD_PASS = os.environ.get("NHPD_PASS", "cReKFao8v#32")

# ArcGIS URLs
URL_COMPLIANCE = "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/bseed_active_residential_compliance_certificates/FeatureServer/0/query"
URL_BLIGHT = "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/blight_tickets/FeatureServer/0/query"
URL_RENTAL_REG = "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/bseed_rental_registrations/FeatureServer/0/query"
URL_NHPD_DOWNLOAD = "https://preservationdatabase.org/wp-content/uploads/nhpd/mi_nhpd.csv"
URL_NHPD_LOGIN = "https://preservationdatabase.org/wp-login.php"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("enrich-detroit")


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.upper() in {"NAN", "NULL", "\\N"}:
        return ""
    return text


def normalize_compact(value: Any) -> str:
    return "".join(clean_text(value).upper().split())


def parse_source_date(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        try:
            return dt.datetime.fromtimestamp(value / 1000, tz=dt.timezone.utc).date()
        except Exception:
            return None
    text = clean_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(text[:26], fmt).date()
        except ValueError:
            pass
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def iter_arcgis_features(url: str, fields: str, page_size: int = 5000, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    offset = 0
    yielded = 0
    while True:
        current_limit = min(page_size, limit - yielded) if limit else page_size
        if current_limit <= 0:
            break
        params = {
            "where": "1=1",
            "outFields": fields,
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": current_limit,
            "orderByFields": "OBJECTID",
        }
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"Error fetching from {url} offset {offset}: {e}")
            break

        if "error" in data:
            logger.error(f"ArcGIS error for {url}: {data['error']}")
            break

        features = data.get("features", [])
        if not features:
            break

        for feature in features:
            attrs = feature.get("attributes") or {}
            yield attrs
            yielded += 1
            if limit and yielded >= limit:
                return

        offset += len(features)
        if len(features) < current_limit and not data.get("exceededTransferLimit"):
            break


def update_source_status(conn, name: str, source_type: str, status: str, details: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO data_source_status
                (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
            VALUES (%s, %s, NOW(), NOW(), %s, %s)
            ON CONFLICT (source_name)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = EXCLUDED.external_last_updated,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
        """, (name, source_type, status, Json(details)))
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def enrich_compliance(conn, limit: Optional[int] = None):
    logger.info("Ingesting Active Residential Certificates of Compliance...")
    # Fetch all BBLs from detroit_properties to ensure valid parcel mapping
    with conn.cursor() as cur:
        cur.execute("SELECT bbl FROM detroit_properties WHERE bbl IS NOT NULL")
        valid_bbls = {row[0] for row in cur.fetchall()}

    logger.info(f"Loaded {len(valid_bbls):,} valid Detroit parcel BBLs for mapping.")

    # Reset compliance fields
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE detroit_properties 
            SET compliance_active = FALSE, compliance_record_id = NULL, compliance_expiration = NULL;
        """)
    conn.commit()

    features_processed = 0
    matched_parcels = 0
    compliance_updates = []

    fields = "record_id,parcel_id,issued_date,expired_date"
    for attrs in iter_arcgis_features(URL_COMPLIANCE, fields, limit=limit):
        features_processed += 1
        parcel_id = clean_text(attrs.get("parcel_id"))
        if not parcel_id:
            continue

        if parcel_id in valid_bbls:
            matched_parcels += 1
            record_id = attrs.get("record_id")
            issued_dt = parse_source_date(attrs.get("issued_date"))
            expired_dt = parse_source_date(attrs.get("expired_date"))
            compliance_updates.append((parcel_id, record_id, expired_dt))

    if compliance_updates:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_compliance (
                    bbl TEXT,
                    record_id TEXT,
                    expiration DATE
                ) ON COMMIT DROP;
            """)
            execute_values(
                cur,
                "INSERT INTO temp_compliance (bbl, record_id, expiration) VALUES %s",
                compliance_updates,
                page_size=2000
            )
            cur.execute("""
                UPDATE detroit_properties p
                SET compliance_active = TRUE,
                    compliance_record_id = t.record_id,
                    compliance_expiration = t.expiration,
                    updated_at = NOW()
                FROM temp_compliance t
                WHERE p.bbl = t.bbl;
            """)
        conn.commit()

    logger.info(f"Compliance ingestion complete: Processed {features_processed:,} records, matched {matched_parcels:,} properties.")
    return {
        "features_processed": features_processed,
        "matched_properties": matched_parcels
    }


def enrich_rental_registrations(conn, limit: Optional[int] = None):
    logger.info("Ingesting Rental Registrations...")
    with conn.cursor() as cur:
        cur.execute("SELECT bbl FROM detroit_properties WHERE bbl IS NOT NULL")
        valid_bbls = {row[0] for row in cur.fetchall()}

    # Clear detroit_hpd_registrations table
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE detroit_hpd_registrations CASCADE;")
    conn.commit()

    features_processed = 0
    matched_parcels = 0
    registrations = []

    fields = "record_id,parcel_id,address,issued_date"
    for attrs in iter_arcgis_features(URL_RENTAL_REG, fields, limit=limit):
        features_processed += 1
        parcel_id = clean_text(attrs.get("parcel_id"))
        if not parcel_id:
            continue

        if parcel_id in valid_bbls:
            matched_parcels += 1
            record_id = attrs.get("record_id")
            addr = attrs.get("address")
            issued_dt = parse_source_date(attrs.get("issued_date"))
            
            # Formats match `{city}_hpd_registrations`
            registrations.append((
                record_id,
                parcel_id,
                addr,
                issued_dt,
                issued_dt
            ))

    if registrations:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO detroit_hpd_registrations (
                    registration_id,
                    bbl,
                    building_address,
                    last_registration_date,
                    registration_end_date
                ) VALUES %s
                ON CONFLICT (registration_id) DO NOTHING;
                """,
                registrations,
                page_size=2000
            )
        conn.commit()

    logger.info(f"Rental registrations ingestion complete: Processed {features_processed:,} records, matched {matched_parcels:,} properties.")
    return {
        "features_processed": features_processed,
        "matched_properties": matched_parcels
    }


def enrich_blight_and_subsidies(conn, limit: Optional[int] = None):
    logger.info("Ingesting Blight Tickets...")
    with conn.cursor() as cur:
        cur.execute("SELECT bbl, UPPER(TRIM(address)) AS addr FROM detroit_properties WHERE bbl IS NOT NULL")
        properties_map = cur.fetchall()

    valid_bbls = {row[0] for row in properties_map}
    address_to_bbl = {row[1]: row[0] for row in properties_map if row[1]}

    # Dictionary to collect stats by BBL
    # fields: violations_total, violations_open, last_violation_date, nhpd_subsidy, nhpd_program, nhpd_expiration
    stats_by_bbl = defaultdict(lambda: {
        "violations_total": 0,
        "violations_open": 0,
        "last_violation_date": None,
        "nhpd_subsidy": False,
        "nhpd_program": None,
        "nhpd_expiration": None
    })

    # 1. Ingest Blight Tickets
    blight_processed = 0
    blight_matched = 0
    fields = "ticket_id,parcel_id,ticket_issued_date,amt_balance_due"
    for attrs in iter_arcgis_features(URL_BLIGHT, fields, limit=limit):
        blight_processed += 1
        parcel_id = clean_text(attrs.get("parcel_id"))
        if not parcel_id:
            continue

        if parcel_id in valid_bbls:
            blight_matched += 1
            issued_dt = parse_source_date(attrs.get("ticket_issued_date"))
            balance = float(attrs.get("amt_balance_due") or 0)
            is_open = balance > 0

            bbl_stat = stats_by_bbl[parcel_id]
            bbl_stat["violations_total"] += 1
            if is_open:
                bbl_stat["violations_open"] += 1
            
            if issued_dt:
                if not bbl_stat["last_violation_date"] or issued_dt > bbl_stat["last_violation_date"]:
                    bbl_stat["last_violation_date"] = issued_dt

    logger.info(f"Blight Tickets complete: Processed {blight_processed:,}, matched {blight_matched:,}.")

    # 2. Ingest NHPD Subsidies
    nhpd_processed = 0
    nhpd_matched = 0
    if not NHPD_USER or not NHPD_PASS:
        logger.warning("NHPD_USER / NHPD_PASS not set — skipping NHPD download.")
    else:
        logger.info("Authenticating with NHPD to fetch Michigan subsidies...")
        session = requests.Session()
        session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        try:
            session.get(URL_NHPD_LOGIN, timeout=30)
            r = session.post(URL_NHPD_LOGIN, data={
                "log": NHPD_USER, "pwd": NHPD_PASS,
                "wp-submit": "Log In", "redirect_to": "/nhpd-downloads/", "testcookie": "1",
            }, timeout=30, allow_redirects=True)
            r.raise_for_status()

            if "login" in r.url:
                logger.error("NHPD login rejected — check credentials.")
            else:
                logger.info("NHPD login successful. Downloading MI dataset…")
                r = session.get(URL_NHPD_DOWNLOAD, timeout=120, stream=True)
                r.raise_for_status()
                content = r.content.decode("utf-8", errors="replace")
                logger.info(f"Downloaded {len(content):,} bytes. Parsing Michigan NHPD rows…")

                reader = csv.DictReader(io.StringIO(content))
                for row in reader:
                    nhpd_processed += 1
                    county = str(row.get("county_fips_code") or row.get("county") or "").strip()
                    # Wayne County FIPS 26163, 163, or name WAYNE
                    if county not in {"26163", "163", "WAYNE"}:
                        continue

                    addr = str(row.get("normalized_address") or row.get("property_address") or "").strip().upper()
                    prog = str(row.get("primary_subsidy_program") or row.get("program_name") or "").strip()
                    exp = parse_source_date(row.get("earliest_expiration_date") or "")

                    if addr in address_to_bbl:
                        bbl = address_to_bbl[addr]
                        nhpd_matched += 1
                        stats_by_bbl[bbl]["nhpd_subsidy"] = True
                        stats_by_bbl[bbl]["nhpd_program"] = prog
                        stats_by_bbl[bbl]["nhpd_expiration"] = exp

        except Exception as e:
            logger.error(f"NHPD download/parse failed: {e}")

    logger.info(f"NHPD processing complete: Matched {nhpd_matched:,} Wayne County subsidy records to properties.")

    # 3. Rebuild detroit_bbl_stats
    logger.info("Writing detroit_bbl_stats...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE detroit_bbl_stats CASCADE;")
        
        values = []
        for bbl, stat in stats_by_bbl.items():
            values.append((
                bbl,
                stat["violations_total"],
                stat["violations_open"],
                0, 0, 0, 0, # classes
                stat["last_violation_date"],
                0, 0, 0, None, # litigations
                0, None, # evictions
                stat["nhpd_subsidy"],
                stat["nhpd_program"],
                stat["nhpd_expiration"]
            ))

        if values:
            execute_values(
                cur,
                """
                INSERT INTO detroit_bbl_stats (
                    bbl,
                    violations_total,
                    violations_open,
                    violations_class_c,
                    violations_class_b,
                    violations_class_a,
                    violations_open_c,
                    last_violation_date,
                    litigations_total,
                    litigations_open,
                    litigations_harassment,
                    last_litigation_date,
                    evictions_total,
                    last_eviction_date,
                    nhpd_subsidy,
                    nhpd_program,
                    nhpd_expiration
                ) VALUES %s
                """,
                values,
                page_size=2000
            )
    conn.commit()
    logger.info("detroit_bbl_stats rebuild complete.")

    return {
        "blight_processed": blight_processed,
        "blight_matched": blight_matched,
        "nhpd_processed": nhpd_processed,
        "nhpd_matched": nhpd_matched
    }


def main():
    parser = argparse.ArgumentParser(description="Detroit Ingestion & Subsidy Integration Pipeline")
    parser.add_argument("--limit", type=int, default=None, help="Row limit for smoke testing/quick runs")
    args = parser.parse_args()

    conn = get_db_connection()
    try:
        # Step 1: Active Certificates of Compliance
        compliance_details = enrich_compliance(conn, limit=args.limit)
        update_source_status(conn, "DETROIT_COMPLIANCE", "city_dataset", "success", compliance_details)

        # Step 2: Rental Registrations
        rental_details = enrich_rental_registrations(conn, limit=args.limit)
        update_source_status(conn, "DETROIT_RENTAL_REG", "city_dataset", "success", rental_details)

        # Step 3: Blight Tickets & NHPD Subsidies
        blight_nhpd_details = enrich_blight_and_subsidies(conn, limit=args.limit)
        update_source_status(conn, "DETROIT_ENFORCEMENT", "city_enforcement", "success", blight_nhpd_details)

        logger.info("Detroit enrichment pipeline completed successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
