#!/usr/bin/env python3
"""
Source-only code enforcement enrichment for non-NYC city parcel stats.

This script intentionally uses only official source records with explicit parcel
or parcel-crosswalk identifiers. It does not geocode, fuzzily match addresses,
or fabricate unavailable class/eviction metrics, and it preserves NHPD subsidy
and rent-stabilization fields in `{city}_bbl_stats`.
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

BALTIMORE_CODE_LAYER_BASE = "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer"
BALTIMORE_DHCD_LAYER_BASE = "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/DHCD_Open_Baltimore_Datasets/FeatureServer"

MINNEAPOLIS_VIOLATIONS_URL = "https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/CaseViolations/FeatureServer/0/query"

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


def normalize_apn(value: Any) -> str:
    """Normalize APN/parcel strings (strip spaces, hyphens, leading zeros only when matching safely)."""
    return re.sub(r"[\s\-]", "", clean_text(value).upper())


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
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
    ):
        try:
            return dt.datetime.strptime(text[:26].strip(), fmt).date()
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
    if "CLOSED" in text or "COMPLIANCE" in text or "PAID" in text or "RESOLVED" in text or "ABATED" in text:
        return False
    return (
        text == "OPEN"
        or text in {"NEW", "PENDING", "EXTENSION", "ACTIVE", "ISSUED", "IN PROGRESS", "VIOLATION"}
        or "OPEN" in text
    )


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
    mapping: Dict[str, str] = {}
    for (raw_bbl,) in rows:
        compact = normalize_compact(raw_bbl)
        if compact:
            mapping[compact] = raw_bbl
        apn_norm = normalize_apn(raw_bbl)
        if apn_norm and apn_norm not in mapping:
            mapping[apn_norm] = raw_bbl
    return mapping


def update_status(
    conn,
    source_name: str,
    source_type: str,
    status: str,
    details: Dict[str, Any],
    external_last_updated: Optional[dt.date] = None,
):
    # Maintain both `{CITY}_ENFORCEMENT` and `{CITY}_CODE_ENFORCEMENT` so
    # FreshnessModal and `/api/{city}/stats` always read consistent freshness metadata.
    names = [source_name]
    if source_name.endswith("_ENFORCEMENT") and not source_name.endswith("_CODE_ENFORCEMENT"):
        prefix = source_name[: -len("_ENFORCEMENT")]
        names.append(f"{prefix}_CODE_ENFORCEMENT")

    with conn.cursor() as cur:
        for name in names:
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
                (name, source_type, external_last_updated, status, Json(details)),
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


def _get_json_with_retry(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 90,
    max_attempts: int = 4,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    client = session or requests
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(
                url,
                params=params,
                timeout=timeout,
                headers={"User-Agent": "TheyOwnWhat-DataAudit/1.0"},
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            backoff = min(2 ** attempt, 16)
            logger.warning(
                "Transient fetch failure (%d/%d) for %s: %s — retrying in %ds",
                attempt,
                max_attempts,
                url,
                exc,
                backoff,
            )
            time.sleep(backoff)
    assert last_exc is not None
    raise last_exc


def iter_ckan_records(resource_id: str, page_size: int = 5000, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    offset = 0
    yielded = 0
    session = requests.Session()
    try:
        while True:
            current_limit = min(page_size, limit - yielded) if limit else page_size
            if current_limit <= 0:
                break
            payload = _get_json_with_retry(
                f"{BOSTON_CKAN_API}/datastore_search",
                params={"resource_id": resource_id, "limit": current_limit, "offset": offset},
                timeout=90,
                max_attempts=4,
                session=session,
            )
            result = payload.get("result", {})
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
    finally:
        session.close()


def iter_arcgis_features(
    url: str,
    page_size: int = 2000,
    limit: Optional[int] = None,
    out_fields: str = "*",
) -> Iterator[Dict[str, Any]]:
    """Iterate ArcGIS FeatureServer/MapServer layers with OBJECTID cursor pagination."""
    session = requests.Session()
    last_oid: Optional[int] = -1
    use_cursor = True
    offset = 0
    yielded = 0
    try:
        while True:
            current_limit = min(page_size, limit - yielded) if limit else page_size
            if current_limit <= 0:
                break
            params: Dict[str, Any] = {
                "outFields": out_fields,
                "returnGeometry": "false",
                "f": "json",
                "resultRecordCount": current_limit,
                "orderByFields": "OBJECTID ASC",
            }
            if use_cursor and last_oid is not None:
                params["where"] = f"OBJECTID > {last_oid}"
            else:
                params["where"] = "1=1"
                params["resultOffset"] = offset

            data = _get_json_with_retry(url, params=params, timeout=90, max_attempts=4, session=session)
            if "error" in data:
                if use_cursor and offset == 0 and last_oid == -1:
                    # Fallback if layer uses a non-OBJECTID PK
                    use_cursor = False
                    params.pop("orderByFields", None)
                    params["where"] = "1=1"
                    params["resultOffset"] = 0
                    data = _get_json_with_retry(url, params=params, timeout=90, max_attempts=4, session=session)
                if "error" in data:
                    raise RuntimeError(f"ArcGIS error for {url}: {data['error']}")

            features = data.get("features", [])
            if not features:
                break

            max_oid_in_batch: Optional[int] = None
            for feature in features:
                attrs = feature.get("attributes") or {}
                oid = attrs.get("OBJECTID")
                if isinstance(oid, (int, float)):
                    oid_int = int(oid)
                    if max_oid_in_batch is None or oid_int > max_oid_in_batch:
                        max_oid_in_batch = oid_int
                yield attrs
                yielded += 1
                if limit and yielded >= limit:
                    return

            if use_cursor:
                if max_oid_in_batch is None or (last_oid is not None and max_oid_in_batch <= last_oid):
                    break
                last_oid = max_oid_in_batch
            else:
                offset += len(features)

            if len(features) < current_limit and not data.get("exceededTransferLimit"):
                break
    finally:
        session.close()


def fetch_boston_sam_to_parcel(sam_ids: Iterable[str]) -> Dict[str, str]:
    ids = sorted({clean_text(sam_id) for sam_id in sam_ids if clean_text(sam_id).isdigit()}, key=int)
    sam_to_parcel: Dict[str, str] = {}
    chunk_size = 250
    session = requests.Session()
    try:
        for start in range(0, len(ids), chunk_size):
            chunk = ids[start : start + chunk_size]
            where = f"SAM_ADDRESS_ID IN ({','.join(chunk)})"
            response = session.post(
                BOSTON_SAM_LAYER_URL,
                data={
                    "where": where,
                    "outFields": "SAM_ADDRESS_ID,PARCEL_ID,MAP_PAR_ID",
                    "returnGeometry": "false",
                    "f": "json",
                },
                timeout=90,
            )
            response.raise_for_status()
            data = response.json()
            if "error" in data:
                raise RuntimeError(f"Boston SAM crosswalk error: {data['error']}")
            for feature in data.get("features", []):
                attrs = feature.get("attributes") or {}
                sam_id = clean_text(attrs.get("SAM_ADDRESS_ID"))
                parcel = clean_text(attrs.get("PARCEL_ID") or attrs.get("MAP_PAR_ID") or attrs.get("PARCEL"))
                if sam_id and parcel:
                    sam_to_parcel[sam_id] = parcel
    finally:
        session.close()
    return sam_to_parcel


def merge_stat(
    bucket: Dict[str, Any],
    *,
    total: int,
    open_count: int,
    last_date: Optional[dt.date],
    class_a: int = 0,
    class_b: int = 0,
    class_c: int = 0,
    open_c: int = 0,
):
    bucket["violations_total"] += total
    bucket["violations_open"] += open_count
    bucket["violations_class_a"] += class_a
    bucket["violations_class_b"] += class_b
    bucket["violations_class_c"] += class_c
    bucket["violations_open_c"] += open_c
    bucket["last_violation_date"] = max_date(bucket.get("last_violation_date"), last_date)


def empty_stat() -> Dict[str, Any]:
    return {
        "violations_total": 0,
        "violations_open": 0,
        "violations_class_a": 0,
        "violations_class_b": 0,
        "violations_class_c": 0,
        "violations_open_c": 0,
        "last_violation_date": None,
    }


def rebuild_city_bbl_stats(conn, city: str, stats_by_bbl: Dict[str, Dict[str, Any]]):
    """Upsert violation counts into `{city}_bbl_stats` WITHOUT wiping NHPD subsidies or rent-stab columns."""
    table = f"{city}_bbl_stats"
    values = [
        (
            bbl,
            int(stats["violations_total"]),
            int(stats["violations_open"]),
            int(stats.get("violations_class_a", 0)),
            int(stats.get("violations_class_b", 0)),
            int(stats.get("violations_class_c", 0)),
            int(stats.get("violations_open_c", 0)),
            stats.get("last_violation_date"),
        )
        for bbl, stats in stats_by_bbl.items()
        if stats.get("violations_total", 0) > 0
    ]
    with conn.cursor() as cur:
        # Reset stale violation counts first while preserving NHPD, litigation, eviction, and rent-stab columns
        cur.execute(
            f"""
            UPDATE {table}
            SET violations_total = 0,
                violations_open = 0,
                violations_class_a = 0,
                violations_class_b = 0,
                violations_class_c = 0,
                violations_open_c = 0,
                last_violation_date = NULL,
                updated_at = NOW()
            WHERE COALESCE(violations_total, 0) > 0
               OR COALESCE(violations_open, 0) > 0
               OR COALESCE(violations_open_c, 0) > 0
               OR last_violation_date IS NOT NULL
            """
        )
        if values:
            execute_values(
                cur,
                f"""
                INSERT INTO {table} (
                    bbl,
                    violations_total,
                    violations_open,
                    violations_class_a,
                    violations_class_b,
                    violations_class_c,
                    violations_open_c,
                    last_violation_date,
                    updated_at
                )
                VALUES %s
                ON CONFLICT (bbl) DO UPDATE SET
                    violations_total = EXCLUDED.violations_total,
                    violations_open = EXCLUDED.violations_open,
                    violations_class_a = EXCLUDED.violations_class_a,
                    violations_class_b = EXCLUDED.violations_class_b,
                    violations_class_c = EXCLUDED.violations_class_c,
                    violations_open_c = EXCLUDED.violations_open_c,
                    last_violation_date = EXCLUDED.last_violation_date,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                page_size=2000,
            )
        # Clean up completely empty rows that have neither violations nor subsidies/evictions
        cur.execute(
            f"""
            DELETE FROM {table}
            WHERE COALESCE(violations_total, 0) = 0
              AND COALESCE(violations_open, 0) = 0
              AND COALESCE(violations_open_c, 0) = 0
              AND COALESCE(litigations_total, 0) = 0
              AND COALESCE(evictions_total, 0) = 0
              AND COALESCE(rs_units, 0) = 0
              AND NOT COALESCE(is_rent_stabilized, false)
              AND NOT COALESCE(nhpd_subsidy, false)
            """
        )
        cur.execute("DELETE FROM kv_cache WHERE key LIKE %s", (f"%{city}%",))
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
        bbl = bbl_map.get(normalize_compact(parcel)) or bbl_map.get(normalize_apn(parcel))
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
    external_dates = []
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
            bbl = bbl_map.get(compact_key) or bbl_map.get(normalize_apn(raw_key))
            if not bbl:
                resource_without_property += 1
                records_without_property += 1
                continue
            open_count = 1 if is_open_source_record(attrs, feed) else 0
            rec_date = parse_source_date(attrs.get(feed["date_field"]))
            if rec_date:
                external_dates.append(rec_date)
            merge_stat(
                stats_by_bbl[bbl],
                total=1,
                open_count=open_count,
                last_date=rec_date,
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
        "eviction_source_available": True,
        "eviction_note": "Baltimore District Court Warrants of Restitution are stored in city_eviction_events.",
    }
    update_status(
        conn,
        "BALTIMORE_ENFORCEMENT",
        "city_enforcement",
        "success",
        details,
        max(external_dates) if external_dates else None,
    )
    return details


def enrich_minneapolis(conn, limit: Optional[int] = None) -> Dict[str, Any]:
    """Ingest official Minneapolis Regulatory Services Housing Code CaseViolations."""
    bbl_map = load_city_bbl_map(conn, "minneapolis")
    stats_by_bbl = defaultdict(empty_stat)
    external_dates = []
    total_records = 0
    matched_records = 0
    records_without_apn = 0
    records_without_property = 0
    distinct_cases = set()

    out_fields = (
        "OBJECTID,APN,Violation_Case_Number,Case_Type,Case_Group_Desc,"
        "Status_Violation_Case,Status_Violation,Processed_Date,Resolved_Date"
    )
    logger.info("Fetching Minneapolis Regulatory Services CaseViolations...")
    for attrs in iter_arcgis_features(MINNEAPOLIS_VIOLATIONS_URL, page_size=2000, limit=limit, out_fields=out_fields):
        total_records += 1
        raw_apn = attrs.get("APN")
        apn_norm = normalize_apn(raw_apn)
        if not apn_norm:
            records_without_apn += 1
            continue
        bbl = bbl_map.get(normalize_compact(raw_apn)) or bbl_map.get(apn_norm)
        if not bbl:
            # Minneapolis APNs sometimes omit leading zeros (e.g. '102824110025' vs '0102824110025')
            if apn_norm.isdigit() and len(apn_norm) == 12:
                bbl = bbl_map.get("0" + apn_norm)
        if not bbl:
            records_without_property += 1
            continue

        matched_records += 1
        case_num = clean_text(attrs.get("Violation_Case_Number"))
        if case_num:
            distinct_cases.add((bbl, case_num))

        proc_date = parse_source_date(attrs.get("Processed_Date"))
        res_date = parse_source_date(attrs.get("Resolved_Date"))
        if proc_date:
            external_dates.append(proc_date)

        viol_status = clean_text(attrs.get("Status_Violation")) or clean_text(attrs.get("Status_Violation_Case"))
        is_open = (res_date is None) and is_explicitly_open(viol_status)

        merge_stat(
            stats_by_bbl[bbl],
            total=1,
            open_count=1 if is_open else 0,
            last_date=proc_date,
        )

    rebuild_city_bbl_stats(conn, "minneapolis", stats_by_bbl)
    latest_date = max(external_dates) if external_dates else None
    details = {
        "sources": [
            {
                "source": "minneapolis_regulatory_services_case_violations",
                "url": MINNEAPOLIS_VIOLATIONS_URL,
                "records": total_records,
                "matched_records": matched_records,
                "distinct_matched_cases": len(distinct_cases),
                "records_without_apn": records_without_apn,
                "records_without_local_property": records_without_property,
            }
        ],
        "source_records": total_records,
        "matched_source_records": matched_records,
        "distinct_matched_cases": len(distinct_cases),
        "matched_parcels": len(stats_by_bbl),
        "class_breakdown_available": False,
        "eviction_source_available": False,
    }
    update_status(
        conn,
        "MINNEAPOLIS_ENFORCEMENT",
        "city_enforcement",
        "success",
        details,
        latest_date,
    )
    return details


def clean_address_custom(addr: Any) -> str:
    if not addr or not isinstance(addr, str):
        return ""
    addr = addr.upper()
    if "WASHINGTON" in addr:
        addr = addr.split("WASHINGTON")[0]
    addr = re.sub(r"\b(SUITE|STE|APT|APARTMENT|UNIT|FLOOR|FL|#|DEPT)\b.*", "", addr)
    replacements = {
        "STREET": "ST",
        "AVENUE": "AVE",
        "ROAD": "RD",
        "DRIVE": "DR",
        "COURT": "CT",
        "PLACE": "PL",
        "BOULEVARD": "BLVD",
        "LANE": "LN",
        "TERRACE": "TER",
        "CIRCLE": "CIR",
        "PARKWAY": "PKWY",
        "PKY": "PKWY",
    }
    for k, v in replacements.items():
        addr = re.sub(r"\b" + k + r"\b", v, addr)
    addr = re.sub(r"[^A-Z0-9]", "", addr)
    return addr


def extract_street_words_custom(addr: Any) -> set[str]:
    if not addr or not isinstance(addr, str):
        return set()
    addr = addr.upper()
    if "WASHINGTON" in addr:
        addr = addr.split("WASHINGTON")[0]
    addr = re.sub(r"\b(SUITE|STE|APT|APARTMENT|UNIT|FLOOR|FL|#|DEPT)\b.*", "", addr)
    words = re.findall(r"\b[A-Z0-9]+\b", addr)
    ignore = {
        "ST", "AVE", "RD", "DR", "CT", "PL", "BLVD", "LN", "TER", "CIR", "PKWY",
        "NE", "NW", "SE", "SW", "N", "S", "E", "W", "STREET", "AVENUE", "ROAD",
    }
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
        logger.error("D.C. violations file not found at %s", csv_path)
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
    has_status_col = any(col in df.columns for col in ("Status", "Record Status", "Case Status"))

    for _, row in df.iterrows():
        raw_addr = row.get("Full Address")
        raw_owner = row.get("Owner Fullname")
        violation_date_str = row.get("Cap Created Date")

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
                for db_bbl, _, db_street_words in db_owner_map[owner_norm]:
                    if csv_street_words & db_street_words:
                        candidates.append(db_bbl)
                if len(candidates) == 1:
                    bbl = candidates[0]
                    matched_owner_street += 1

        if bbl:
            status_val = row.get("Status") or row.get("Record Status") or row.get("Case Status")
            open_cnt = (1 if is_explicitly_open(status_val) else 0) if has_status_col else 0
            merge_stat(
                stats_by_bbl[bbl],
                total=1,
                open_count=open_cnt,
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
                "status_column_present": has_status_col,
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


def enrich_philadelphia(conn, limit: Optional[int] = None) -> Dict[str, Any]:
    """Ingest official Philadelphia L&I Code Violations aggregated by `opa_account_num` via Carto SQL."""
    url = "https://phl.carto.com/api/v2/sql"
    bbl_map = load_city_bbl_map(conn, "philadelphia")
    stats_by_bbl = defaultdict(empty_stat)
    external_dates = []
    total_records = 0
    matched_records = 0
    records_without_property = 0
    page_size = min(limit, 50000) if limit else 50000
    last_opa = ""
    fetched_groups = 0
    session = requests.Session()

    open_pred = "UPPER(COALESCE(violationstatus, '')) IN ('OPEN', 'IN VIOLATION', 'FAILED', 'SVN ISSUED', 'VIOLATION', 'DEMOLISH')"
    class_c_pred = "UPPER(COALESCE(caseprioritydesc, '')) IN ('IMMINENTLY DANGEROUS', 'UNSAFE')"
    class_b_pred = "UPPER(COALESCE(caseprioritydesc, '')) = 'HAZARDOUS'"
    class_a_pred = "UPPER(COALESCE(caseprioritydesc, '')) IN ('STANDARD', 'CONSTRUCTION SERVICES', 'NON-HAZARDOUS')"

    logger.info("Fetching Philadelphia L&I Code Violations grouped by opa_account_num...")
    try:
        while True:
            safe_last = last_opa.replace("'", "''")
            where_cursor = f"AND opa_account_num > '{safe_last}'" if last_opa else ""
            query = (
                "SELECT opa_account_num, "
                "COUNT(*)::int AS total_cnt, "
                f"COUNT(*) FILTER (WHERE {open_pred})::int AS open_cnt, "
                f"COUNT(*) FILTER (WHERE {class_a_pred})::int AS class_a_cnt, "
                f"COUNT(*) FILTER (WHERE {class_b_pred})::int AS class_b_cnt, "
                f"COUNT(*) FILTER (WHERE {class_c_pred})::int AS class_c_cnt, "
                f"COUNT(*) FILTER (WHERE ({open_pred}) AND ({class_c_pred}))::int AS open_c_cnt, "
                "MAX(violationdate) AS last_vdate "
                "FROM violations "
                f"WHERE opa_account_num IS NOT NULL AND opa_account_num != '' {where_cursor} "
                f"GROUP BY opa_account_num ORDER BY opa_account_num ASC LIMIT {page_size}"
            )
            payload = _get_json_with_retry(url, params={"q": query}, timeout=90, max_attempts=4, session=session)
            rows = payload.get("rows", [])
            if not rows:
                break

            for row in rows:
                raw_opa = clean_text(row.get("opa_account_num"))
                if not raw_opa:
                    continue
                last_opa = raw_opa
                fetched_groups += 1
                tot = int(row.get("total_cnt") or 0)
                total_records += tot

                bbl = bbl_map.get(normalize_compact(raw_opa)) or bbl_map.get(normalize_apn(raw_opa))
                if not bbl:
                    records_without_property += tot
                    continue

                matched_records += tot
                rec_date = parse_source_date(row.get("last_vdate"))
                if rec_date:
                    external_dates.append(rec_date)
                merge_stat(
                    stats_by_bbl[bbl],
                    total=tot,
                    open_count=int(row.get("open_cnt") or 0),
                    class_a=int(row.get("class_a_cnt") or 0),
                    class_b=int(row.get("class_b_cnt") or 0),
                    class_c=int(row.get("class_c_cnt") or 0),
                    open_c=int(row.get("open_c_cnt") or 0),
                    last_date=rec_date,
                )

            if limit and fetched_groups >= limit:
                break
            if len(rows) < page_size:
                break
    finally:
        session.close()

    rebuild_city_bbl_stats(conn, "philadelphia", stats_by_bbl)
    latest_date = max(external_dates) if external_dates else None
    details = {
        "sources": [
            {
                "source": "philadelphia_li_code_violations_carto",
                "url": url,
                "records": total_records,
                "matched_records": matched_records,
                "records_without_local_property": records_without_property,
            }
        ],
        "source_records": total_records,
        "matched_source_records": matched_records,
        "matched_parcels": len(stats_by_bbl),
        "class_breakdown_available": True,
        "eviction_source_available": False,
        "eviction_note": "No official parcel-level Philadelphia eviction bulk feed is configured.",
    }
    update_status(
        conn,
        "PHILADELPHIA_ENFORCEMENT",
        "city_enforcement",
        "success",
        details,
        latest_date,
    )
    return details


def main():
    parser = argparse.ArgumentParser(description="Enrich city parcel stats from real-world enforcement sources.")
    parser.add_argument(
        "--city",
        choices=["all", "boston", "baltimore", "dc", "minneapolis", "philadelphia"],
        default="all",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional per-feed source row limit for smoke tests.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    start = time.time()
    conn = get_db_connection()
    try:
        city_runners = [
            ("boston", "BOSTON_ENFORCEMENT", enrich_boston, "Boston"),
            ("baltimore", "BALTIMORE_ENFORCEMENT", enrich_baltimore, "Baltimore"),
            ("dc", "DC_ENFORCEMENT", enrich_dc, "D.C."),
            ("minneapolis", "MINNEAPOLIS_ENFORCEMENT", enrich_minneapolis, "Minneapolis"),
            ("philadelphia", "PHILADELPHIA_ENFORCEMENT", enrich_philadelphia, "Philadelphia"),
        ]
        for city_arg, status_key, runner_fn, label in city_runners:
            if args.city in ("all", city_arg):
                try:
                    details = runner_fn(conn, limit=args.limit)
                    logger.info("%s enforcement enrichment complete: %s", label, json.dumps(details, default=str))
                except Exception as exc:
                    logger.exception("%s enforcement enrichment failed: %s", label, exc)
                    try:
                        conn.rollback()
                        update_status(conn, status_key, "city_enforcement", "failure", {"error": str(exc)}, None)
                    except Exception as status_exc:
                        logger.error("Failed to record failure status for %s: %s", status_key, status_exc)
    finally:
        conn.close()
    logger.info("City enforcement enrichment finished in %.1fs", time.time() - start)


if __name__ == "__main__":
    main()
