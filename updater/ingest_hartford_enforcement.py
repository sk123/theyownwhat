#!/usr/bin/env python3
"""
Ingest official Hartford Code Enforcement cases into `code_enforcement`.

Primary download: Hartford Open Data CSV endpoint.
Automatic fallback: Official City of Hartford ArcGIS FeatureServer backing the same
dataset (`7cf59bd708a94395be61954d6f430e3f`, Layer 1), using OBJECTID cursor pagination
when `data.hartford.gov` fails DNS or HTTP resolution.
"""
import datetime as dt
import os
import sys
import time
import logging
from email.utils import parsedate_to_datetime

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values
import requests

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
HARTFORD_ENFORCEMENT_CSV_URL = "https://data.hartford.gov/api/download/v1/items/7cf59bd708a94395be61954d6f430e3f/csv?layers=1"
HARTFORD_ENFORCEMENT_FEATURESERVER_URL = (
    "https://utility.arcgis.com/usrsvcs/servers/7cf59bd708a94395be61954d6f430e3f/"
    "rest/services/HartfordOpenDataTables/FeatureServer/1/query"
)


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def parse_http_date(value):
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).date()
    except Exception:
        return None


def parse_record_date(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int, float)):
        if val <= 0:
            return None
        try:
            return dt.datetime.fromtimestamp(val / 1000, tz=dt.timezone.utc).date().isoformat()
        except Exception:
            return None
    text = str(val).strip()
    if not text or text.lower() in ("nan", "none", "null"):
        return None
    # Handle epoch strings if returned as text
    if text.isdigit() and len(text) >= 11:
        try:
            return dt.datetime.fromtimestamp(int(text) / 1000, tz=dt.timezone.utc).date().isoformat()
        except Exception:
            pass
    return text


def update_status(conn, status, details, external_last_updated=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
            VALUES ('HARTFORD_CODE_ENFORCEMENT', 'city_enforcement', %s, NOW(), %s, %s)
            ON CONFLICT (source_name)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = EXCLUDED.external_last_updated,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
            """,
            (external_last_updated, status, Json(details)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix' OR key LIKE 'dashboard_summary:%'")
    conn.commit()


def ensure_schema(conn):
    schema_path = os.path.join(os.path.dirname(__file__), "..", "scripts", "code_enforcement.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def download_latest_dataframe():
    """Fetch official Hartford enforcement records via CSV or official FeatureServer cursor."""
    logger.info("Trying Hartford Open Data CSV: %s", HARTFORD_ENFORCEMENT_CSV_URL)
    try:
        response = requests.get(HARTFORD_ENFORCEMENT_CSV_URL, timeout=15)
        response.raise_for_status()
        temp_path = "/tmp/hartford_enforcement.csv"
        with open(temp_path, "wb") as f:
            f.write(response.content)
        df = pd.read_csv(temp_path)
        df.columns = [c.lstrip("\ufeff") for c in df.columns]
        return df, parse_http_date(response.headers.get("Last-Modified")), HARTFORD_ENFORCEMENT_CSV_URL
    except Exception as csv_exc:
        logger.warning(
            "CSV hub %s unavailable (%s); falling back to official Hartford ArcGIS FeatureServer...",
            HARTFORD_ENFORCEMENT_CSV_URL,
            csv_exc,
        )

    rows = []
    last_oid = -1
    page_size = 2000
    session = requests.Session()
    max_opened_date = None
    try:
        while True:
            params = {
                "where": f"OBJECTID > {last_oid}",
                "outFields": "*",
                "orderByFields": "OBJECTID ASC",
                "resultRecordCount": page_size,
                "returnGeometry": "false",
                "f": "json",
            }
            payload = None
            for attempt in range(1, 4):
                try:
                    resp = session.get(HARTFORD_ENFORCEMENT_FEATURESERVER_URL, params=params, timeout=60)
                    resp.raise_for_status()
                    data = resp.json()
                    if "error" in data:
                        raise RuntimeError(f"ArcGIS error: {data['error']}")
                    payload = data
                    break
                except Exception as exc:
                    if attempt >= 3:
                        logger.error("FeatureServer fetch failed after OBJECTID %s: %s", last_oid, exc)
                        return None, None, HARTFORD_ENFORCEMENT_FEATURESERVER_URL
                    time.sleep(2 ** attempt)

            features = payload.get("features", [])
            if not features:
                break

            batch_max_oid = last_oid
            for feat in features:
                attrs = feat.get("attributes") or {}
                oid = attrs.get("OBJECTID")
                if isinstance(oid, (int, float)) and int(oid) > batch_max_oid:
                    batch_max_oid = int(oid)
                opened_iso = parse_record_date(attrs.get("Date_Opened"))
                if opened_iso:
                    try:
                        d_val = dt.date.fromisoformat(opened_iso[:10])
                        if max_opened_date is None or d_val > max_opened_date:
                            max_opened_date = d_val
                    except Exception:
                        pass
                rows.append(attrs)

            if batch_max_oid <= last_oid:
                break
            last_oid = batch_max_oid
            logger.info("  Fetched %d Hartford enforcement rows from FeatureServer...", len(rows))
            if len(features) < page_size and not payload.get("exceededTransferLimit"):
                break
    finally:
        session.close()

    if not rows:
        return None, None, HARTFORD_ENFORCEMENT_FEATURESERVER_URL

    return pd.DataFrame(rows), max_opened_date, HARTFORD_ENFORCEMENT_FEATURESERVER_URL


def ingest_dataframe(df: pd.DataFrame, external_last_updated=None, source_url: str = HARTFORD_ENFORCEMENT_CSV_URL):
    logger.info("Processing %d Hartford enforcement records from %s...", len(df), source_url)

    conn = get_db_connection()
    ensure_schema(conn)
    cur = conn.cursor()

    logger.info("Building ParcelID -> property_id mapping...")
    cur.execute(
        """
        SELECT id, split_part(cama_site_link, '-', 2) as parcel_id
        FROM properties
        WHERE property_city IN ('HARTFORD', 'Hartford', 'hartford')
          AND cama_site_link IS NOT NULL
          AND cama_site_link LIKE '%-%'
        """
    )
    parcel_map = {row[1]: row[0] for row in cur.fetchall() if row[1]}
    logger.info("Mapped %d parcels in Hartford.", len(parcel_map))

    def clean_val(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        val_str = str(val).strip()
        if val_str.lower() in ("nan", "none", "null", ""):
            return None
        return val_str

    ingest_by_gid = {}
    for _, row in df.iterrows():
        gid = clean_val(row.get("GlobalID"))
        if not gid:
            continue
        parcel_id = clean_val(row.get("ParcelID"))
        property_id = parcel_map.get(parcel_id)

        date_opened = parse_record_date(row.get("Date_Opened"))
        date_closed = parse_record_date(row.get("Date_Closed"))
        record_module = clean_val(row.get("Record_Module"))

        first = clean_val(row.get("Contact_First_Name")) or ""
        last = clean_val(row.get("Contact_Last_Name")) or ""
        contact_name = f"{first} {last}".strip() or None

        ingest_by_gid[gid] = (
            property_id,
            "HARTFORD",
            clean_val(row.get("Case_Number")),
            parcel_id,
            clean_val(row.get("Address")),
            clean_val(row.get("Unit")),
            record_module,
            clean_val(row.get("Dept_Division")),
            clean_val(row.get("Record_Name")),
            clean_val(row.get("Record_Type")),
            clean_val(row.get("Record_Status")),
            date_opened,
            date_closed,
            clean_val(row.get("Property_Owner")),
            contact_name,
            clean_val(row.get("Inspector_Name")),
            clean_val(row.get("Inspection_Type")),
            gid,
        )

    ingest_data = list(ingest_by_gid.values())
    if not ingest_data:
        cur.close()
        conn.close()
        raise RuntimeError("Official Hartford feed returned 0 valid GlobalID records; refusing snapshot cleanup.")

    logger.info("Upserting %d unique Hartford enforcement records...", len(ingest_data))
    upsert_query = """
        INSERT INTO code_enforcement (
            property_id, municipality, case_number, parcel_id, address, unit,
            record_module, dept_division, record_name, record_type, record_status,
            date_opened, date_closed, property_owner, contact_name,
            inspector_name, inspection_type, global_id
        ) VALUES %s
        ON CONFLICT (global_id) DO UPDATE SET
            property_id = EXCLUDED.property_id,
            property_owner = EXCLUDED.property_owner,
            record_status = EXCLUDED.record_status,
            date_closed = EXCLUDED.date_closed,
            updated_at = CURRENT_TIMESTAMP
    """

    execute_values(cur, upsert_query, ingest_data, page_size=2000)
    current_global_ids = [(row[17],) for row in ingest_data if row[17]]
    cur.execute(
        """
        CREATE TEMP TABLE hartford_current_global_ids (
            global_id TEXT PRIMARY KEY
        ) ON COMMIT DROP
        """
    )
    execute_values(
        cur,
        "INSERT INTO hartford_current_global_ids (global_id) VALUES %s ON CONFLICT DO NOTHING",
        current_global_ids,
        page_size=2000,
    )
    cur.execute(
        """
        DELETE FROM code_enforcement ce
        WHERE ce.municipality = 'HARTFORD'
          AND NOT EXISTS (
              SELECT 1
              FROM hartford_current_global_ids current_ids
              WHERE current_ids.global_id = ce.global_id
          )
        """
    )
    deleted_stale = cur.rowcount
    conn.commit()
    logger.info("Successfully ingested/updated %d enforcement records (removed %d stale).", len(ingest_data), deleted_stale)

    linked_count = sum(1 for row in ingest_data if row[0] is not None)
    update_status(
        conn,
        "success",
        {
            "source_url": source_url,
            "source_records": len(ingest_data),
            "matched_records": linked_count,
            "records_without_local_property": len(ingest_data) - linked_count,
            "removed_records_not_in_current_source": deleted_stale,
        },
        external_last_updated,
    )

    cur.close()
    conn.close()


if __name__ == "__main__":
    df, external_date, used_url = download_latest_dataframe()
    if df is None or df.empty:
        conn = get_db_connection()
        try:
            update_status(
                conn,
                "failure",
                {
                    "source_url": used_url,
                    "message": "Official Hartford source download failed; no fallback data was ingested.",
                },
                None,
            )
        finally:
            conn.close()
        sys.exit(1)
    ingest_dataframe(df, external_date, used_url)
