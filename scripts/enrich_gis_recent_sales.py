#!/usr/bin/env python3
"""
scripts/enrich_gis_recent_sales.py
===================================
Enriches municipal GIS parcel tables (`boston`, `miami`, `philadelphia`, `dc`,
`detroit`, `baltimore`) with official `last_sale_date` and `last_sale_price`
records from their authoritative CAMA / statewide parcel rolls so network
profiles surface Recent Transactions alongside CT, MA, FL, CO, and NC.

Strict Source-Only Policy:
  - Never fabricates or interpolates sale dates or prices.
  - Zero or negative nominal placeholders (`<= 0`) are stored as NULL for
    `last_sale_price` (while preserving exact recorded deed `last_sale_date`).
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

import psycopg2
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("enrich-gis-sales")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")

TARGET_CITIES = ("boston", "miami", "philadelphia", "dc", "detroit", "baltimore")


def _ensure_sale_columns(conn) -> None:
    with conn.cursor() as cur:
        for city in TARGET_CITIES:
            tbl = f"{city}_properties"
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS last_sale_date DATE")
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS last_sale_price NUMERIC")
    conn.commit()


def _parse_epoch_ms_date(val) -> Optional[str]:
    if val is None or val == "":
        return None
    try:
        ms = float(val)
    except (TypeError, ValueError):
        return None
    if ms <= -2208988800000 or ms > 4102444800000:
        return None
    try:
        d = dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).date()
        if 1900 <= d.year <= 2030:
            return d.isoformat()
    except Exception:
        return None
    return None


def _clean_price(val) -> Optional[str]:
    if val is None or val == "":
        return None
    try:
        num = float(val)
        if num > 0 and num < 10_000_000_000:
            return f"{num:.2f}"
    except (TypeError, ValueError):
        pass
    return None


def _bulk_apply_sales(conn, city: str, records: Dict[str, Tuple[Optional[str], Optional[str]]], norm_strip_spaces: bool = False) -> int:
    if not records:
        return 0
    tbl = f"{city}_properties"
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
    for bbl_key, (s_date, s_price) in records.items():
        if not bbl_key or (not s_date and not s_price):
            continue
        writer.writerow([bbl_key, s_date or r"\N", s_price or r"\N"])
    buf.seek(0)

    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE _tmp_gis_sales (bbl_key TEXT, s_date DATE, s_price NUMERIC) ON COMMIT DROP")
        cur.copy_from(buf, "_tmp_gis_sales", sep="\t", null=r"\N")
        cur.execute("CREATE INDEX ON _tmp_gis_sales (bbl_key)")
        if norm_strip_spaces:
            cur.execute(
                f"""
                UPDATE {tbl} p
                SET last_sale_date = COALESCE(s.s_date, p.last_sale_date),
                    last_sale_price = COALESCE(s.s_price, p.last_sale_price)
                FROM _tmp_gis_sales s
                WHERE REGEXP_REPLACE(UPPER(p.bbl), '\\s+', '', 'g') = s.bbl_key
                """
            )
        else:
            cur.execute(
                f"""
                UPDATE {tbl} p
                SET last_sale_date = COALESCE(s.s_date, p.last_sale_date),
                    last_sale_price = COALESCE(s.s_price, p.last_sale_price)
                FROM _tmp_gis_sales s
                WHERE p.bbl = s.bbl_key
                """
            )
        updated = cur.rowcount
    conn.commit()
    return updated


def enrich_boston_from_massgis(conn) -> int:
    logger.info("[boston] Cross-populating sale dates & prices from MassGIS (`ma_properties`)...")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE boston_properties b
            SET last_sale_date = m.last_sale_date,
                last_sale_price = NULLIF(m.last_sale_price, 0)
            FROM ma_properties m
            WHERE m.bbl = ('M035-' || b.bbl)
              AND (m.last_sale_date IS NOT NULL OR m.last_sale_price IS NOT NULL)
            """
        )
        direct = cur.rowcount
        cur.execute(
            """
            WITH addr_sales AS (
                SELECT DISTINCT ON (UPPER(TRIM(address)))
                    UPPER(TRIM(address)) AS addr_key,
                    last_sale_date,
                    NULLIF(last_sale_price, 0) AS last_sale_price
                FROM ma_properties
                WHERE borough = 'BOSTON'
                  AND address IS NOT NULL
                  AND last_sale_date IS NOT NULL
                ORDER BY UPPER(TRIM(address)), last_sale_date DESC
            )
            UPDATE boston_properties b
            SET last_sale_date = a.last_sale_date,
                last_sale_price = COALESCE(a.last_sale_price, b.last_sale_price)
            FROM addr_sales a
            WHERE b.last_sale_date IS NULL
              AND UPPER(TRIM(b.address)) = a.addr_key
            """
        )
        fallback = cur.rowcount
    conn.commit()
    logger.info("[boston] Updated %d parcels by MassGIS PID + %d by exact Boston address.", direct, fallback)
    return direct + fallback


def enrich_miami_from_fdor(conn) -> int:
    logger.info("[miami] Cross-populating sale dates & prices from FDOR (`fl_properties`, C23 Miami-Dade)...")
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH miami_sales AS (
                SELECT DISTINCT ON (UPPER(TRIM(address)), UPPER(TRIM(COALESCE(zip_code, ''))))
                    UPPER(TRIM(address)) AS addr_key,
                    UPPER(TRIM(COALESCE(zip_code, ''))) AS zip_key,
                    last_sale_date,
                    NULLIF(last_sale_price, 0) AS last_sale_price
                FROM fl_properties
                WHERE bbl LIKE 'C23-%'
                  AND address IS NOT NULL
                  AND last_sale_date IS NOT NULL
                ORDER BY UPPER(TRIM(address)), UPPER(TRIM(COALESCE(zip_code, ''))), last_sale_date DESC
            )
            UPDATE miami_properties m
            SET last_sale_date = s.last_sale_date,
                last_sale_price = s.last_sale_price
            FROM miami_sales s
            WHERE UPPER(TRIM(m.address)) = s.addr_key
              AND (s.zip_key = '' OR m.zip_code IS NULL OR UPPER(TRIM(m.zip_code)) = s.zip_key)
            """
        )
        updated = cur.rowcount
    conn.commit()
    logger.info("[miami] Updated %d Miami-Dade parcels from FDOR C23 sale records.", updated)
    return updated


def enrich_philadelphia_opa(conn) -> int:
    logger.info("[philadelphia] Streaming `sale_date` and `sale_price` from Philadelphia OPA Carto SQL API...")
    url = "https://phl.carto.com/api/v2/sql"
    query = (
        "SELECT parcel_number, sale_date::date AS sd, sale_price "
        "FROM opa_properties_public "
        "WHERE parcel_number IS NOT NULL AND sale_date IS NOT NULL"
    )
    r = requests.get(url, params={"q": query, "format": "csv"}, stream=True, timeout=120)
    r.raise_for_status()
    records: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    reader = csv.DictReader(r.iter_lines(decode_unicode=True))
    for row in reader:
        pn = (row.get("parcel_number") or "").strip()
        sd = (row.get("sd") or "").strip()[:10]
        if not pn or len(sd) != 10:
            continue
        yr = int(sd[:4]) if sd[:4].isdigit() else 0
        s_date = sd if 1900 <= yr <= 2030 else None
        s_price = _clean_price(row.get("sale_price"))
        if s_date or s_price:
            records[pn] = (s_date, s_price)
    updated = _bulk_apply_sales(conn, "philadelphia", records)
    logger.info("[philadelphia] Updated %d / %d OPA sale records.", updated, len(records))
    return updated


def _fetch_arcgis_chunk(url: str, where: str, out_fields: str, offset: int, count: int) -> list:
    params = {
        "where": where,
        "outFields": out_fields,
        "resultOffset": offset,
        "resultRecordCount": count,
        "returnGeometry": "false",
        "f": "json",
    }
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return data.get("features") or []
        except Exception:
            if attempt == 2:
                return []
            time.sleep(1.5 * (attempt + 1))
    return []


def enrich_dc_cama_sales(conn) -> int:
    logger.info("[dc] Fetching `SALEDATE` and `SALEPRICE` from DC GIS CAMA FeatureServer/40...")
    url = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/40/query"
    where = "SSL IS NOT NULL AND SALEDATE IS NOT NULL"
    cnt_resp = requests.get(url, params={"where": where, "returnCountOnly": "true", "f": "json"}, timeout=20).json()
    total = int(cnt_resp.get("count") or 0)
    chunk = 2000
    records: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [
            pool.submit(_fetch_arcgis_chunk, url, where, "SSL,SALEDATE,SALEPRICE", off, chunk)
            for off in range(0, total + chunk, chunk)
        ]
        for fut in as_completed(futs):
            for feat in fut.result():
                a = feat.get("attributes") or {}
                ssl = "".join(str(a.get("SSL") or "").upper().split())
                if not ssl:
                    continue
                sd = _parse_epoch_ms_date(a.get("SALEDATE"))
                sp = _clean_price(a.get("SALEPRICE"))
                if sd or sp:
                    records[ssl] = (sd, sp)
    updated = _bulk_apply_sales(conn, "dc", records, norm_strip_spaces=True)
    logger.info("[dc] Updated %d / %d DC SSL sale records.", updated, len(records))
    return updated


def enrich_detroit_sales(conn) -> int:
    logger.info("[detroit] Fetching `sale_date` and `sale_price` from Detroit Parcels_Current...")
    url = "https://services2.arcgis.com/qvkbeam7Wirps6zC/ArcGIS/rest/services/Parcels_Current/FeatureServer/0/query"
    where = "parcel_number IS NOT NULL AND sale_date IS NOT NULL"
    cnt_resp = requests.get(url, params={"where": where, "returnCountOnly": "true", "f": "json"}, timeout=20).json()
    total = int(cnt_resp.get("count") or 0)
    chunk = 5000
    records: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [
            pool.submit(_fetch_arcgis_chunk, url, where, "parcel_number,sale_date,sale_price", off, chunk)
            for off in range(0, total + chunk, chunk)
        ]
        for fut in as_completed(futs):
            for feat in fut.result():
                a = feat.get("attributes") or {}
                pn = str(a.get("parcel_number") or "").strip()
                if not pn:
                    continue
                sd = _parse_epoch_ms_date(a.get("sale_date"))
                sp = _clean_price(a.get("sale_price"))
                if sd or sp:
                    records[pn] = (sd, sp)
    updated = _bulk_apply_sales(conn, "detroit", records)
    logger.info("[detroit] Updated %d / %d Detroit parcel sale records.", updated, len(records))
    return updated


def enrich_baltimore_sales(conn) -> int:
    logger.info("[baltimore] Fetching `SALEDATE_good` and `SALEPRIC` from Baltimore dmxOwnership...")
    url = "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/dmxOwnership/MapServer/0/query"
    records: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    last_oid = -1
    chunk = 2000
    session = requests.Session()
    try:
        while True:
            params = {
                "where": f"BLOCKLOT IS NOT NULL AND SALEDATE_good IS NOT NULL AND OBJECTID > {last_oid}",
                "outFields": "OBJECTID,BLOCKLOT,SALEDATE_good,SALEPRIC",
                "orderByFields": "OBJECTID ASC",
                "resultRecordCount": chunk,
                "returnGeometry": "false",
                "f": "json",
            }
            feats = []
            data = {}
            for attempt in range(3):
                try:
                    r = session.get(url, params=params, timeout=30)
                    r.raise_for_status()
                    data = r.json()
                    feats = data.get("features") or []
                    break
                except Exception:
                    if attempt == 2:
                        break
                    time.sleep(1.5 * (attempt + 1))
            if not feats:
                break
            max_oid = last_oid
            for f in feats:
                a = f.get("attributes") or {}
                oid = a.get("OBJECTID")
                if oid is not None and int(oid) > max_oid:
                    max_oid = int(oid)
                bl = str(a.get("BLOCKLOT") or "").strip()
                if not bl:
                    continue
                sd = _parse_epoch_ms_date(a.get("SALEDATE_good"))
                sp = _clean_price(a.get("SALEPRIC"))
                if sd or sp:
                    records[bl] = (sd, sp)
            if max_oid <= last_oid:
                break
            last_oid = max_oid
            if len(feats) < chunk and not data.get("exceededTransferLimit"):
                break
    finally:
        session.close()
    updated = _bulk_apply_sales(conn, "baltimore", records)
    logger.info("[baltimore] Updated %d / %d Baltimore BLOCKLOT sale records.", updated, len(records))
    return updated


def main() -> int:
    t0 = time.time()
    conn = psycopg2.connect(DATABASE_URL)
    try:
        _ensure_sale_columns(conn)
        enrich_boston_from_massgis(conn)
        enrich_miami_from_fdor(conn)
        enrich_philadelphia_opa(conn)
        enrich_dc_cama_sales(conn)
        enrich_detroit_sales(conn)
        enrich_baltimore_sales(conn)
    finally:
        conn.close()
    logger.info("✓ All 6 municipal GIS recent-sale enrichments complete in %.1fs!", time.time() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
