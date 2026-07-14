#!/usr/bin/env python3
"""
Source-only code enforcement enrichment for non-NYC city parcel stats.

This script intentionally uses only official source records with explicit parcel
or parcel-crosswalk identifiers. It does not geocode, fuzzily match addresses,
or fabricate unavailable class/eviction metrics.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, Iterator, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")

BOSTON_CKAN_API = "https://data.boston.gov/api/3/action"
BOSTON_BUILDING_RESOURCE_ID = "800a2663-1d6a-46e7-9356-bedb70f5332c"
BOSTON_PUBLIC_WORKS_RESOURCE_ID = "90ed3816-5e70-443c-803d-9a71f44470be"
BOSTON_SAM_LAYER_URL = "https://gis.boston.gov/arcgis/rest/services/SAM/Live_SAM_Address/FeatureServer/1/query"

BALTIMORE_CODE_LAYER_BASE = "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer"
BALTIMORE_DHCD_LAYER_BASE = "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/DHCD_Open_Baltimore_Datasets/FeatureServer"

BOSTON_FEEDS = [
    {
        "label": "building_property_violations",
        "resource_id": BOSTON_BUILDING_RESOURCE_ID,
        "package_id": "building-and-property-violations1",
    },
    {
        "label": "public_works_violations",
        "resource_id": BOSTON_PUBLIC_WORKS_RESOURCE_ID,
        "package_id": "public-works-violations",
    },
]

BALTIMORE_FEEDS = [
    {
        "label": "fta_citations",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/11/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "status_field": "CitationStatus",
        "open_feed": False,
    },
    {
        "label": "open_notices_interior",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/13/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_notices_exterior",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/14/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_notices_interior_exterior",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/15/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_cleaning_work_orders_1_30",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/18/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_cleaning_work_orders_31_60",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/19/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_cleaning_work_orders_61_90",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/20/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_cleaning_work_orders_90_plus",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/21/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_boarding_work_orders_1_5",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/23/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_boarding_work_orders_6_10",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/24/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_boarding_work_orders_11_plus",
        "url": f"{BALTIMORE_CODE_LAYER_BASE}/25/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "status_field": "Status",
        "open_feed": True,
    },
    {
        "label": "open_vacant_building_notices",
        "url": f"{BALTIMORE_DHCD_LAYER_BASE}/1/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateNotice",
        "status_field": None,
        "open_feed": True,
        "open_if_dates_null": ("DateCancel", "DateAbate"),
    },
]

NON_OPEN_STATUSES = {
    "ABATED",
    "CANCELLED",
    "CANCELED",
    "CLOSED",
    "COMPLIANCE",
    "COMPLETE",
    "COMPLETED",
    "DISMISSED",
    "PAID",
    "RESOLVED",
    "SETTLED",
    "VOID",
}

logger = logging.getLogger("city-enforcement")


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


def max_date(left: Optional[dt.date], right: Optional[dt.date]) -> Optional[dt.date]:
    if left and right:
        return max(left, right)
    return left or right


def is_explicitly_open(status: Any) -> bool:
    text = clean_text(status).upper()
    if not text:
        return False
    if text in NON_OPEN_STATUSES:
        return False
    if "CLOSED" in text or "COMPLIANCE" in text or "PAID" in text:
        return False
    return text == "OPEN" or text in {"NEW", "PENDING", "EXTENSION", "ACTIVE", "ISSUED"} or "OPEN" in text


def is_open_source_record(attrs: Dict[str, Any], feed: Dict[str, Any]) -> bool:
    null_date_fields = feed.get("open_if_dates_null")
    if null_date_fields:
        return all(not parse_source_date(attrs.get(field)) for field in null_date_fields)

    status_field = feed.get("status_field")
    if status_field:
        return is_explicitly_open(attrs.get(status_field))

    return bool(feed.get("open_feed"))


def load_city_bbl_map(conn, city: str) -> Dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT bbl FROM {city}_properties WHERE bbl IS NOT NULL")
        rows = cur.fetchall()
    return {normalize_compact(row[0]): row[0] for row in rows if normalize_compact(row[0])}


def update_status(
    conn,
    source_name: str,
    source_type: str,
    status: str,
    details: Dict[str, Any],
    external_last_updated: Optional[dt.date] = None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
            VALUES (%s, %s, %s, NOW(), %s, %s)
            ON CONFLICT (source_name)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = EXCLUDED.external_last_updated,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
            """,
            (source_name, source_type, external_last_updated, status, Json(details)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def fetch_ckan_package_modified(package_id: str) -> Optional[dt.date]:
    response = requests.get(f"{BOSTON_CKAN_API}/package_show", params={"id": package_id}, timeout=30)
    response.raise_for_status()
    data = response.json()
    result = data.get("result", {})
    dates = [parse_source_date(result.get("metadata_modified"))]
    for resource in result.get("resources", []):
        dates.append(parse_source_date(resource.get("last_modified")))
    dates = [value for value in dates if value]
    return max(dates) if dates else None


def iter_ckan_records(resource_id: str, page_size: int = 30000, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    offset = 0
    yielded = 0
    while True:
        current_limit = min(page_size, limit - yielded) if limit else page_size
        if current_limit <= 0:
            break
        response = requests.get(
            f"{BOSTON_CKAN_API}/datastore_search",
            params={"resource_id": resource_id, "limit": current_limit, "offset": offset},
            timeout=120,
        )
        response.raise_for_status()
        result = response.json().get("result", {})
        records = result.get("records", [])
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


def iter_arcgis_features(url: str, page_size: int = 5000, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    offset = 0
    yielded = 0
    while True:
        current_limit = min(page_size, limit - yielded) if limit else page_size
        if current_limit <= 0:
            break
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": current_limit,
            "orderByFields": "OBJECTID",
        }
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise RuntimeError(f"ArcGIS error for {url}: {data['error']}")
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


def fetch_boston_sam_to_parcel(sam_ids: Iterable[str]) -> Dict[str, str]:
    ids = sorted({clean_text(sam_id) for sam_id in sam_ids if clean_text(sam_id).isdigit()}, key=int)
    sam_to_parcel: Dict[str, str] = {}
    chunk_size = 250
    for start in range(0, len(ids), chunk_size):
        chunk = ids[start : start + chunk_size]
        where = f"SAM_ADDRESS_ID IN ({','.join(chunk)})"
        response = requests.post(
            BOSTON_SAM_LAYER_URL,
            data={
                "where": where,
                "outFields": "SAM_ADDRESS_ID,PARCEL",
                "returnGeometry": "false",
                "f": "json",
            },
            timeout=120,
        )
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise RuntimeError(f"Boston SAM crosswalk error: {data['error']}")
        for feature in data.get("features", []):
            attrs = feature.get("attributes") or {}
            sam_id = clean_text(attrs.get("SAM_ADDRESS_ID"))
            parcel = clean_text(attrs.get("PARCEL"))
            if sam_id and parcel:
                sam_to_parcel[sam_id] = parcel
    return sam_to_parcel


def merge_stat(bucket: Dict[str, Any], *, total: int, open_count: int, last_date: Optional[dt.date]):
    bucket["violations_total"] += total
    bucket["violations_open"] += open_count
    bucket["last_violation_date"] = max_date(bucket.get("last_violation_date"), last_date)


def empty_stat() -> Dict[str, Any]:
    return {
        "violations_total": 0,
        "violations_open": 0,
        "last_violation_date": None,
    }


def rebuild_city_bbl_stats(conn, city: str, stats_by_bbl: Dict[str, Dict[str, Any]]):
    table = f"{city}_bbl_stats"
    values = [
        (
            bbl,
            stats["violations_total"],
            stats["violations_open"],
            None,
            None,
            None,
            None,
            stats.get("last_violation_date"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        for bbl, stats in stats_by_bbl.items()
        if stats.get("violations_total", 0) > 0
    ]
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table}")
        if values:
            execute_values(
                cur,
                f"""
                INSERT INTO {table} (
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
                    is_rent_stabilized,
                    rs_units,
                    nhpd_subsidy,
                    nhpd_program,
                    nhpd_expiration
                )
                VALUES %s
                """,
                values,
            )
    conn.commit()


def enrich_boston(conn, limit: Optional[int] = None) -> Dict[str, Any]:
    bbl_map = load_city_bbl_map(conn, "boston")
    stats_by_sam = defaultdict(empty_stat)
    feed_details = []
    external_dates = []
    total_records = 0
    records_without_sam = 0

    for feed in BOSTON_FEEDS:
        label = feed["label"]
        resource_count = 0
        resource_without_sam = 0
        resource_external_date = fetch_ckan_package_modified(feed["package_id"])
        external_dates.append(resource_external_date)
        logger.info("Fetching Boston %s records...", label)
        for record in iter_ckan_records(feed["resource_id"], limit=limit):
            resource_count += 1
            total_records += 1
            sam_id = clean_text(record.get("sam_id"))
            if not sam_id:
                resource_without_sam += 1
                records_without_sam += 1
                continue
            merge_stat(
                stats_by_sam[sam_id],
                total=1,
                open_count=1 if is_explicitly_open(record.get("status")) else 0,
                last_date=parse_source_date(record.get("status_dttm")),
            )
        feed_details.append(
            {
                "source": label,
                "resource_id": feed["resource_id"],
                "records": resource_count,
                "records_without_sam_id": resource_without_sam,
                "external_last_updated": str(resource_external_date) if resource_external_date else None,
            }
        )

    logger.info("Resolving %d Boston SAM IDs through official SAM parcel crosswalk...", len(stats_by_sam))
    sam_to_parcel = fetch_boston_sam_to_parcel(stats_by_sam.keys())
    stats_by_bbl = defaultdict(empty_stat)
    records_without_crosswalk = 0
    records_without_property = 0
    matched_records = 0

    for sam_id, sam_stats in stats_by_sam.items():
        parcel = sam_to_parcel.get(sam_id)
        total = sam_stats["violations_total"]
        if not parcel:
            records_without_crosswalk += total
            continue
        bbl = bbl_map.get(normalize_compact(parcel))
        if not bbl:
            records_without_property += total
            continue
        matched_records += total
        merge_stat(
            stats_by_bbl[bbl],
            total=total,
            open_count=sam_stats["violations_open"],
            last_date=sam_stats.get("last_violation_date"),
        )

    rebuild_city_bbl_stats(conn, "boston", stats_by_bbl)
    external_dates = [value for value in external_dates if value]
    details = {
        "sources": feed_details,
        "sam_crosswalk_source": BOSTON_SAM_LAYER_URL,
        "source_records": total_records,
        "records_without_sam_id": records_without_sam,
        "records_without_sam_crosswalk": records_without_crosswalk,
        "records_without_local_property": records_without_property,
        "matched_source_records": matched_records,
        "matched_parcels": len(stats_by_bbl),
        "class_breakdown_available": False,
        "eviction_source_available": False,
        "eviction_note": "No official parcel-level Boston eviction bulk feed is configured.",
    }
    update_status(
        conn,
        "BOSTON_ENFORCEMENT",
        "city_enforcement",
        "success",
        details,
        max(external_dates) if external_dates else None,
    )
    return details


def enrich_baltimore(conn, limit: Optional[int] = None) -> Dict[str, Any]:
    bbl_map = load_city_bbl_map(conn, "baltimore")
    stats_by_bbl = defaultdict(empty_stat)
    feed_details = []
    total_records = 0
    matched_records = 0
    records_without_blocklot = 0
    records_without_property = 0

    for feed in BALTIMORE_FEEDS:
        label = feed["label"]
        logger.info("Fetching Baltimore %s records...", label)
        resource_count = 0
        resource_matched = 0
        resource_without_key = 0
        resource_without_property = 0
        for attrs in iter_arcgis_features(feed["url"], limit=limit):
            resource_count += 1
            total_records += 1
            raw_key = attrs.get(feed["key_field"])
            compact_key = normalize_compact(raw_key)
            if not compact_key:
                resource_without_key += 1
                records_without_blocklot += 1
                continue
            bbl = bbl_map.get(compact_key)
            if not bbl:
                resource_without_property += 1
                records_without_property += 1
                continue
            open_count = 1 if is_open_source_record(attrs, feed) else 0
            merge_stat(
                stats_by_bbl[bbl],
                total=1,
                open_count=open_count,
                last_date=parse_source_date(attrs.get(feed["date_field"])),
            )
            matched_records += 1
            resource_matched += 1
        feed_details.append(
            {
                "source": label,
                "url": feed["url"],
                "records": resource_count,
                "matched_records": resource_matched,
                "records_without_blocklot": resource_without_key,
                "records_without_local_property": resource_without_property,
            }
        )

    rebuild_city_bbl_stats(conn, "baltimore", stats_by_bbl)
    details = {
        "sources": feed_details,
        "source_records": total_records,
        "matched_source_records": matched_records,
        "records_without_blocklot": records_without_blocklot,
        "records_without_local_property": records_without_property,
        "matched_parcels": len(stats_by_bbl),
        "class_breakdown_available": False,
        "eviction_source_available": False,
        "eviction_note": "No official parcel-level Baltimore eviction bulk feed is configured.",
    }
    update_status(conn, "BALTIMORE_ENFORCEMENT", "city_enforcement", "success", details, None)
    return details


def clean_address_custom(addr: Any) -> str:
    if not addr or not isinstance(addr, str):
        return ""
    addr = addr.upper()
    if 'WASHINGTON' in addr:
        addr = addr.split('WASHINGTON')[0]
    addr = re.sub(r'\b(SUITE|STE|APT|APARTMENT|UNIT|FLOOR|FL|#|DEPT)\b.*', '', addr)
    replacements = {
        'STREET': 'ST',
        'AVENUE': 'AVE',
        'ROAD': 'RD',
        'DRIVE': 'DR',
        'COURT': 'CT',
        'PLACE': 'PL',
        'BOULEVARD': 'BLVD',
        'LANE': 'LN',
        'TERRACE': 'TER',
        'CIRCLE': 'CIR',
        'PARKWAY': 'PKWY',
        'PKY': 'PKWY',
    }
    for k, v in replacements.items():
        addr = re.sub(r'\b' + k + r'\b', v, addr)
    addr = re.sub(r'[^A-Z0-9]', '', addr)
    return addr


def extract_street_words_custom(addr: Any) -> set[str]:
    if not addr or not isinstance(addr, str):
        return set()
    addr = addr.upper()
    if 'WASHINGTON' in addr:
        addr = addr.split('WASHINGTON')[0]
    addr = re.sub(r'\b(SUITE|STE|APT|APARTMENT|UNIT|FLOOR|FL|#|DEPT)\b.*', '', addr)
    words = re.findall(r'\b[A-Z0-9]+\b', addr)
    ignore = {'ST', 'AVE', 'RD', 'DR', 'CT', 'PL', 'BLVD', 'LN', 'TER', 'CIR', 'PKWY', 'NE', 'NW', 'SE', 'SW', 'N', 'S', 'E', 'W', 'STREET', 'AVENUE', 'ROAD'}
    return {w for w in words if w not in ignore and not w.isdigit()}


def enrich_dc(conn, limit: Optional[int] = None) -> Dict[str, Any]:
    logger.info("Fetching D.C. properties for enrichment mapping...")
    with conn.cursor() as cur:
        cur.execute("SELECT bbl, address, owner_name, owner_name_norm FROM dc_properties")
        db_props = cur.fetchall()

    db_address_map = {}
    db_owner_map = {}
    for bbl, address, owner_name, owner_name_norm in db_props:
        norm_addr = clean_address_custom(address)
        if norm_addr:
            db_address_map[norm_addr] = bbl
        
        if owner_name_norm:
            if owner_name_norm not in db_owner_map:
                db_owner_map[owner_name_norm] = []
            db_owner_map[owner_name_norm].append((bbl, address, extract_street_words_custom(address)))

    csv_path = "data/dc_violations.csv"
    if not os.path.exists(csv_path):
        logger.error(f"D.C. violations file not found at {csv_path}")
        return {
            "error": f"File not found at {csv_path}",
            "source_records": 0,
            "matched_parcels": 0,
        }

    logger.info("Loading and parsing D.C. violations CSV...")
    df = pd.read_csv(csv_path)
    df = df.iloc[1:]  # Skip row 0 which is 'Total' summary row
    if limit is not None:
        df = df.head(limit)

    stats_by_bbl = defaultdict(empty_stat)
    matched_direct = 0
    matched_owner_street = 0
    unmatched_records = 0
    total_records = len(df)
    external_dates = []

    for idx, row in df.iterrows():
        raw_addr = row.get('Full Address')
        raw_owner = row.get('Owner Fullname')
        violation_date_str = row.get('Cap Created Date')
        
        violation_date = parse_source_date(violation_date_str)
        if violation_date:
            external_dates.append(violation_date)
            
        norm_addr = clean_address_custom(raw_addr)
        
        bbl = None
        if norm_addr in db_address_map:
            bbl = db_address_map[norm_addr]
            matched_direct += 1
        else:
            owner_norm = str(raw_owner).strip().upper() if pd.notna(raw_owner) else ""
            if owner_norm in db_owner_map:
                csv_street_words = extract_street_words_custom(raw_addr)
                candidates = []
                for db_bbl, db_addr, db_street_words in db_owner_map[owner_norm]:
                    if csv_street_words & db_street_words:
                        candidates.append(db_bbl)
                if len(candidates) == 1:
                    bbl = candidates[0]
                    matched_owner_street += 1
        
        if bbl:
            merge_stat(
                stats_by_bbl[bbl],
                total=1,
                open_count=1,
                last_date=violation_date,
            )
        else:
            unmatched_records += 1

    logger.info("Rebuilding D.C. BBL stats in database...")
    rebuild_city_bbl_stats(conn, "dc", stats_by_bbl)
    
    external_last_updated = max(external_dates) if external_dates else None
    
    details = {
        "sources": [
            {
                "source": "dc_violations_csv",
                "path": csv_path,
                "records": total_records,
                "matched_direct": matched_direct,
                "matched_owner_street": matched_owner_street,
                "unmatched_records": unmatched_records,
            }
        ],
        "source_records": total_records,
        "matched_source_records": matched_direct + matched_owner_street,
        "matched_parcels": len(stats_by_bbl),
        "class_breakdown_available": False,
        "eviction_source_available": False,
        "eviction_note": "No official parcel-level D.C. eviction bulk feed is configured.",
    }
    
    update_status(
        conn,
        "DC_ENFORCEMENT",
        "city_enforcement",
        "success",
        details,
        external_last_updated,
    )
    return details


def mark_unavailable_sources(conn):
    update_status(
        conn,
        "DC_EVICTIONS",
        "court_evictions",
        "unavailable",
        {
            "source_records": 0,
            "matched_parcels": 0,
            "message": "D.C. publishes monthly eviction PDFs, but no official parcel-level bulk feed is configured.",
        },
        None,
    )
    for city in ("BOSTON",):
        update_status(
            conn,
            f"{city}_EVICTIONS",
            "court_evictions",
            "unavailable",
            {
                "source_records": 0,
                "matched_parcels": 0,
                "message": f"No official parcel-level {city.title()} eviction bulk feed is configured.",
            },
            None,
        )


def main():
    parser = argparse.ArgumentParser(description="Enrich city parcel stats from real-world enforcement sources.")
    parser.add_argument("--city", choices=["all", "boston", "baltimore", "dc"], default="all")
    parser.add_argument("--limit", type=int, default=None, help="Optional per-feed source row limit for smoke tests.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    start = time.time()
    conn = get_db_connection()
    try:
        if args.city in ("all", "boston"):
            details = enrich_boston(conn, limit=args.limit)
            logger.info("Boston enforcement enrichment complete: %s", json.dumps(details, default=str))
        if args.city in ("all", "baltimore"):
            details = enrich_baltimore(conn, limit=args.limit)
            logger.info("Baltimore enforcement enrichment complete: %s", json.dumps(details, default=str))
        if args.city in ("all", "dc"):
            details = enrich_dc(conn, limit=args.limit)
            logger.info("D.C. enforcement enrichment complete: %s", json.dumps(details, default=str))
        if args.city == "all":
            mark_unavailable_sources(conn)
    finally:
        conn.close()
    logger.info("City enforcement enrichment finished in %.1fs", time.time() - start)


if __name__ == "__main__":
    main()
