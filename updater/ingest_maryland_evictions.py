#!/usr/bin/env python3
"""
Ingest official Maryland District Court eviction events for Baltimore City.

The source does not publish property addresses, parcel IDs, landlords, or block-lot
identifiers, so this script stores city/ZIP-level court event data only. It does
not write parcel-level bbl_stats eviction counts.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import time
from typing import Any, Dict, Iterator, Optional

import psycopg2
from psycopg2.extras import Json, execute_values
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
DATASET_ID = "mvqb-b4hf"
SOURCE_NAME = "MARYLAND_DISTRICT_COURT_EVICTIONS"
STATUS_SOURCE_NAME = "BALTIMORE_EVICTIONS"
SOCRATA_API = f"https://opendata.maryland.gov/resource/{DATASET_ID}.json"
SOCRATA_METADATA = f"https://opendata.maryland.gov/api/views/{DATASET_ID}"
BALTIMORE_COUNTY = "Baltimore City"

logger = logging.getLogger("maryland-evictions")


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NAN", "NULL", "\\N"}:
        return None
    return text


def parse_date(value: Any) -> Optional[dt.date]:
    text = clean_text(value)
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def parse_int(value: Any) -> Optional[int]:
    text = clean_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def get_source_modified_date() -> Optional[dt.date]:
    response = requests.get(SOCRATA_METADATA, timeout=30)
    response.raise_for_status()
    metadata = response.json()
    updated_at = metadata.get("rowsUpdatedAt") or metadata.get("viewLastModified")
    if not updated_at:
        return None
    try:
        return dt.datetime.fromtimestamp(int(updated_at), tz=dt.timezone.utc).date()
    except Exception:
        return None


def iter_baltimore_events(page_size: int = 50000, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    offset = 0
    yielded = 0
    headers = {}
    app_token = os.environ.get("MARYLAND_SOCRATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token

    while True:
        current_limit = min(page_size, limit - yielded) if limit else page_size
        if current_limit <= 0:
            break
        response = requests.get(
            SOCRATA_API,
            params={
                "$limit": current_limit,
                "$offset": offset,
                "$order": "casenumber,eventdate,eventtype",
                "county": BALTIMORE_COUNTY,
            },
            headers=headers,
            timeout=120,
        )
        response.raise_for_status()
        records = response.json()
        if not records:
            break
        for record in records:
            yield record
            yielded += 1
            if limit and yielded >= limit:
                return
        offset += len(records)
        if len(records) < current_limit:
            break


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS city_eviction_events (
                id BIGSERIAL PRIMARY KEY,
                source_name TEXT NOT NULL,
                city TEXT NOT NULL,
                jurisdiction TEXT NOT NULL,
                case_number TEXT,
                event_type TEXT,
                event_date DATE,
                event_comment TEXT,
                location TEXT,
                tenant_city TEXT,
                tenant_state TEXT,
                tenant_zip TEXT,
                case_type TEXT,
                evicted_date DATE,
                event_year INTEGER,
                eviction_year INTEGER,
                raw JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_city_eviction_events_city
                ON city_eviction_events (city);
            CREATE INDEX IF NOT EXISTS idx_city_eviction_events_source_jurisdiction
                ON city_eviction_events (source_name, jurisdiction);
            CREATE INDEX IF NOT EXISTS idx_city_eviction_events_event_date
                ON city_eviction_events (event_date);
            CREATE INDEX IF NOT EXISTS idx_city_eviction_events_tenant_zip
                ON city_eviction_events (tenant_zip);
            CREATE INDEX IF NOT EXISTS idx_city_eviction_events_case_number
                ON city_eviction_events (case_number);
            """
        )
    conn.commit()


def update_status(conn, status: str, details: Dict[str, Any], external_last_updated: Optional[dt.date]):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
            VALUES (%s, 'court_evictions', %s, NOW(), %s, %s)
            ON CONFLICT (source_name)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = EXCLUDED.external_last_updated,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
            """,
            (STATUS_SOURCE_NAME, external_last_updated, status, Json(details)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def event_tuple(record: Dict[str, Any]):
    return (
        SOURCE_NAME,
        "baltimore",
        BALTIMORE_COUNTY,
        clean_text(record.get("casenumber")),
        clean_text(record.get("eventtype")),
        parse_date(record.get("eventdate")),
        clean_text(record.get("eventcomment")),
        clean_text(record.get("location")),
        clean_text(record.get("tenantcity")),
        clean_text(record.get("tenantstate")),
        clean_text(record.get("tenantzipcode")),
        clean_text(record.get("casetype")),
        parse_date(record.get("evicteddate")),
        parse_int(record.get("year")),
        parse_int(record.get("evictionyear")),
        Json(record),
    )


def ingest_baltimore(limit: Optional[int] = None) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        ensure_schema(conn)
        external_last_updated = get_source_modified_date()
        rows = []
        event_type_counts: Dict[str, int] = {}
        zip_counts: Dict[str, int] = {}
        min_event_date = None
        max_event_date = None
        max_evicted_date = None

        logger.info("Fetching official Maryland eviction events for %s...", BALTIMORE_COUNTY)
        for record in iter_baltimore_events(limit=limit):
            rows.append(event_tuple(record))
            event_type = clean_text(record.get("eventtype")) or "Unknown"
            tenant_zip = clean_text(record.get("tenantzipcode")) or "Unknown"
            event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
            zip_counts[tenant_zip] = zip_counts.get(tenant_zip, 0) + 1
            event_date = parse_date(record.get("eventdate"))
            evicted_date = parse_date(record.get("evicteddate"))
            min_event_date = min(filter(None, [min_event_date, event_date]), default=None)
            max_event_date = max(filter(None, [max_event_date, event_date]), default=None)
            max_evicted_date = max(filter(None, [max_evicted_date, evicted_date]), default=None)

        batch_size = 5000
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM city_eviction_events
                WHERE source_name = %s AND jurisdiction = %s
                """,
                (SOURCE_NAME, BALTIMORE_COUNTY),
            )
            removed_stale = cur.rowcount
            for start in range(0, len(rows), batch_size):
                batch = rows[start : start + batch_size]
                execute_values(
                    cur,
                    """
                    INSERT INTO city_eviction_events (
                        source_name, city, jurisdiction, case_number, event_type,
                        event_date, event_comment, location, tenant_city, tenant_state,
                        tenant_zip, case_type, evicted_date, event_year, eviction_year, raw
                    ) VALUES %s
                    """,
                    batch,
                )
        conn.commit()

        details = {
            "source_url": SOCRATA_API,
            "source_dataset": f"https://opendata.maryland.gov/d/{DATASET_ID}",
            "source_records": len(rows),
            "matched_records": 0,
            "matched_parcels": 0,
            "property_level_available": False,
            "join_note": "Official Maryland dataset does not include address, parcel, landlord, or block-lot fields.",
            "min_event_date": str(min_event_date) if min_event_date else None,
            "max_event_date": str(max_event_date) if max_event_date else None,
            "max_evicted_date": str(max_evicted_date) if max_evicted_date else None,
            "event_type_counts": event_type_counts,
            "top_tenant_zip_counts": dict(sorted(zip_counts.items(), key=lambda item: item[1], reverse=True)[:20]),
            "removed_records_not_in_current_source": removed_stale,
        }
        update_status(conn, "success", details, external_last_updated)
        return details
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest official Maryland eviction events.")
    parser.add_argument("--limit", type=int, default=None, help="Optional source row limit for smoke tests.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    start = time.time()
    details = ingest_baltimore(limit=args.limit)
    logger.info("Maryland eviction ingest complete in %.1fs: %s", time.time() - start, details)


if __name__ == "__main__":
    main()
