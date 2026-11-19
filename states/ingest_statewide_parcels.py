#!/usr/bin/env python3
"""
states/ingest_statewide_parcels.py
==================================
Unified, source-only statewide property parcel, rental licensing, code enforcement,
corporate-registry piercing (`oc_linker`), and guarded network clustering pipeline
built on PostgreSQL 15 LIST partitions (`us_properties` / `us_networks` -> `{jur}_*`):

Supported statewide jurisdictions:
  - `fl` (Florida — All 67 counties via FloridaGIO Statewide Parcels + Sunbiz `us_fl`
          + FL DBPR Ch. 509 Apartment Licenses `hrlodge1..7.csv`; also enriches `miami`)
  - `ny` (New York State — All 33 non-NYC participating counties via NYS ITS Public Tax
          Parcels + NY DOS `us_ny` + Buffalo Rental Registry, Code Violations & Housing Court)
  - `co` (Colorado — Colorado OIT Public Parcels + CO SOS & True Names `us_co`
          + Denver Active Residential Rental Property Licenses)
  - `vt` (Vermont — VCGI Standardized Parcels + Statewide Grand List)
  - `wi` (Wisconsin — SCO/DOA Statewide Parcels V11, Class 1 Residential & Class 2 Multifamily/Comm)
  - `nc` (North Carolina — NC OneMap Parcels across all 100 counties)

Strict Source-Only Policy (`THEYOWNWHAT_SOURCE_ONLY=true`):
  - Zero fabricated, interpolated, or guessed owner names, principals, units, or coordinates.
  - Full 3rd-party bank, servicer, trustee, GSE, government, and registered-agent exclusion.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import csv
import datetime as dt
import gc
import io
import json
import os
import re
import subprocess
import sys
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values
import requests

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_jurisdiction_partition
from api.oc_linker import (
    demangle_assessor_owner,
    link_jurisdiction_parcels_to_oc,
    resolve_oc_company_principals,
)
from api.populate_cities import (
    GENERIC_OWNER_NAMES,
    is_public_entity,
    is_public_or_generic_address,
    load_registrar_address_substrings,
    normalize_link_address,
    normalize_owner_name,
    rebuild_city_networks_from_db,
    run_union_find_clustering,
    source_float,
    source_int,
    source_text,
)
from api.shared_utils import (
    US_STATE_CODES,
    compose_address,
    is_excluded_third_party_entity,
    looks_like_business_name,
    looks_like_person_owner,
    normalize_business_name,
    normalize_mailing_address_coarse,
    normalize_person_name,
    strip_benign_care_of_prefix,
)

csv.field_size_limit(10_000_000)

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TheyOwnWhatPublicRecordsSync/1.0)"
}


def _resolve_database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@localhost")
    return raw


DATABASE_URL = _resolve_database_url()

STATE_BOUNDS: Dict[str, Tuple[float, float, float, float]] = {
    "fl": (24.30, 31.10, -87.70, -79.90),
    "ny": (40.45, 45.05, -79.80, -71.80),
    "co": (36.95, 41.05, -109.10, -102.00),
    "vt": (42.70, 45.05, -73.48, -71.45),
    "wi": (42.45, 47.15, -92.95, -86.20),
    "nc": (33.75, 36.62, -84.35, -75.40),
    "ar": (33.00, 36.52, -94.65, -89.60),
    "va": (36.52, 39.48, -83.69, -75.22),
}

FL_COUNTY_NAMES: Dict[int, str] = {
    11: "ALACHUA", 12: "BAKER", 13: "BAY", 14: "BRADFORD", 15: "BREVARD", 16: "BROWARD",
    17: "CALHOUN", 18: "CHARLOTTE", 19: "CITRUS", 20: "CLAY", 21: "COLLIER", 22: "COLUMBIA",
    23: "MIAMI-DADE", 24: "DESOTO", 25: "DIXIE", 26: "DUVAL", 27: "ESCAMBIA", 28: "FLAGLER",
    29: "FRANKLIN", 30: "GADSDEN", 31: "GILCHRIST", 32: "GLADES", 33: "GULF", 34: "HAMILTON",
    35: "HARDEE", 36: "HENDRY", 37: "HERNANDO", 38: "HIGHLANDS", 39: "HILLSBOROUGH", 40: "HOLMES",
    41: "INDIAN RIVER", 42: "JACKSON", 43: "JEFFERSON", 44: "LAFAYETTE", 45: "LAKE", 46: "LEE",
    47: "LEON", 48: "LEVY", 49: "LIBERTY", 50: "MADISON", 51: "MANATEE", 52: "MARION",
    53: "MARTIN", 54: "MONROE", 55: "NASSAU", 56: "OKALOOSA", 57: "OKEECHOBEE", 58: "ORANGE",
    59: "OSCEOLA", 60: "PALM BEACH", 61: "PASCO", 62: "PINELLAS", 63: "POLK", 64: "PUTNAM",
    65: "ST. JOHNS", 66: "ST. LUCIE", 67: "SANTA ROSA", 68: "SARASOTA", 69: "SEMINOLE",
    70: "SUMTER", 71: "SUWANNEE", 72: "TAYLOR", 73: "UNION", 74: "VOLUSIA", 75: "WAKULLA",
    76: "WALTON", 77: "WASHINGTON",
}

FL_DOR_LABELS: Dict[str, str] = {
    "001": "SINGLE FAMILY RESIDENTIAL",
    "002": "MOBILE HOME",
    "003": "MULTI-FAMILY (10+ UNITS)",
    "004": "RESIDENTIAL CONDOMINIUM",
    "005": "COOPERATIVE",
    "006": "RETIREMENT / ASSISTED HOUSING",
    "007": "MISCELLANEOUS RESIDENTIAL",
    "008": "MULTI-FAMILY (2-9 UNITS)",
    "012": "MIXED-USE RESIDENTIAL / COMMERCIAL",
    "028": "MOBILE HOME PARK",
    "039": "RESIDENTIAL LODGING",
}


@contextmanager
def _stream_csv_via_curl(url: str):
    proc = subprocess.Popen(
        ["curl", "-fsSL", "--compressed", "-A", HTTP_HEADERS["User-Agent"], url],
        stdout=subprocess.PIPE,
        bufsize=4 * 1024 * 1024,
    )
    assert proc.stdout is not None
    text_stream = io.TextIOWrapper(proc.stdout, encoding="utf-8", errors="replace")
    try:
        yield csv.reader(text_stream)
    finally:
        text_stream.close()
        ret = proc.wait(timeout=30)
        if ret != 0:
            raise RuntimeError(f"curl stream exited with code {ret} for {url}")


def _clean_zip5(raw_val: Any, allow_leading_zero_4: bool = False) -> Optional[str]:
    txt = source_text(raw_val)
    if not txt:
        return None
    m5 = re.search(r"\b(\d{5})\b", txt)
    if m5:
        return m5.group(1)
    digits = re.sub(r"[^0-9]", "", txt)
    if allow_leading_zero_4 and len(digits) == 4:
        return "0" + digits
    if len(digits) >= 5:
        return digits[:5]
    return None


def _parse_any_date(raw_val: Any) -> Optional[dt.date]:
    if raw_val is None:
        return None
    if isinstance(raw_val, (int, float)) and raw_val > 10_000_000_000:
        # Epoch milliseconds (ArcGIS date fields)
        try:
            d = dt.datetime.fromtimestamp(float(raw_val) / 1000.0, tz=dt.timezone.utc).date()
            if 1750 <= d.year <= 2035:
                return d
        except Exception:
            return None
    txt = source_text(raw_val)
    if not txt:
        return None
    # ISO YYYY-MM-DD
    m_iso = re.match(r"^(\d{4})-(\d{2})-(\d{2})", txt)
    if m_iso:
        try:
            d = dt.date(int(m_iso.group(1)), int(m_iso.group(2)), int(m_iso.group(3)))
            if 1750 <= d.year <= 2035:
                return d
        except ValueError:
            pass
    # M/D/YYYY
    m_us = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", txt)
    if m_us:
        try:
            d = dt.date(int(m_us.group(3)), int(m_us.group(1)), int(m_us.group(2)))
            if 1750 <= d.year <= 2035:
                return d
        except ValueError:
            pass
    return None


def _validate_coords(jur: str, lat: Optional[float], lng: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if lat is None or lng is None:
        return None, None
    bounds = STATE_BOUNDS.get(jur)
    if not bounds:
        return lat, lng
    min_lat, max_lat, min_lon, max_lon = bounds
    if min_lat <= lat <= max_lat and min_lon <= lng <= max_lon:
        return lat, lng
    return None, None


def _extract_feat_coords(jur: str, feat: Dict[str, Any], lat_attr: Optional[str] = None, lng_attr: Optional[str] = None) -> Tuple[Optional[float], Optional[float]]:
    attrs = feat.get("attributes") or {}
    if lat_attr and lng_attr:
        lat = source_float(attrs.get(lat_attr))
        lng = source_float(attrs.get(lng_attr))
        return _validate_coords(jur, lat, lng)
    c = feat.get("centroid") or feat.get("geometry") or {}
    lng = source_float(c.get("x"))
    lat = source_float(c.get("y"))
    return _validate_coords(jur, lat, lng)


def _record_status(conn, source_name: str, source_type: str, details: Dict[str, Any], ext_date: Optional[dt.date] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status (
                source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details
            )
            VALUES (%s, %s, %s, NOW(), 'success', %s)
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                last_refreshed_at = NOW(),
                refresh_status = 'success',
                details = EXCLUDED.details;
            """,
            (source_name, source_type, ext_date, Json(details)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def _arcgis_oid_minmax(query_url: str, oid_field: str = "OBJECTID") -> Tuple[int, int]:
    resp = requests.get(
        query_url,
        params={
            "where": "1=1",
            "outStatistics": json.dumps([
                {"statisticType": "min", "onStatisticField": oid_field, "outStatisticFieldName": "mn"},
                {"statisticType": "max", "onStatisticField": oid_field, "outStatisticFieldName": "mx"},
            ]),
            "f": "json",
        },
        headers=HTTP_HEADERS,
        timeout=35,
    ).json()
    attrs = ((resp.get("features") or [{}])[0].get("attributes")) or {}
    return int(attrs.get("mn") or 1), int(attrs.get("mx") or 1_000_000)


def _fetch_arcgis_cursor_shard(
    query_url: str,
    where_base: str,
    out_fields: str,
    mapper_fn: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
    *,
    label: str,
    oid_field: str = "OBJECTID",
    page_size: int = 2000,
    return_geometry: bool = False,
    return_centroid: bool = False,
) -> List[Dict[str, Any]]:
    last_oid = -1
    rows: List[Dict[str, Any]] = []
    session = requests.Session()
    session.headers.update(HTTP_HEADERS)
    try:
        while True:
            where = f"({where_base}) AND {oid_field} > {last_oid}"
            params: Dict[str, Any] = {
                "where": where,
                "outFields": out_fields,
                "returnGeometry": "true" if return_geometry else "false",
                "orderByFields": f"{oid_field} ASC",
                "resultRecordCount": page_size,
                "f": "json",
            }
            if return_centroid:
                params["returnCentroid"] = "true"
            if return_geometry or return_centroid:
                params["outSR"] = "4326"

            data: Dict[str, Any] = {}
            for attempt in range(5):
                try:
                    resp = session.get(query_url, params=params, timeout=60)
                    resp.raise_for_status()
                    data = resp.json()
                    if "error" in data:
                        raise RuntimeError(f"ArcGIS error: {data['error']}")
                    break
                except Exception as exc:
                    if attempt == 4:
                        raise RuntimeError(f"[{label}] failed after {oid_field}={last_oid}: {exc}") from exc
                    time.sleep(2 * (attempt + 1))

            features = data.get("features") or []
            if not features:
                break

            max_oid = last_oid
            for feat in features:
                attrs = feat.get("attributes") or {}
                oid = source_int(attrs.get(oid_field))
                if oid is not None and oid > max_oid:
                    max_oid = oid
                mapped = mapper_fn(feat)
                if mapped is not None:
                    rows.append(mapped)

            if max_oid <= last_oid:
                break
            last_oid = max_oid
            if len(features) < page_size and not data.get("exceededTransferLimit"):
                break
    finally:
        session.close()

    print(f"  [{label}] fetched {len(rows):,} valid parcels.")
    return rows


def _sharded_oid_fetch(
    query_url: str,
    extra_where: str,
    out_fields: str,
    mapper_fn: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
    *,
    jur_upper: str,
    oid_field: str = "OBJECTID",
    page_size: int = 2000,
    num_shards: int = 10,
    max_workers: int = 8,
    return_geometry: bool = False,
    return_centroid: bool = False,
) -> List[Dict[str, Any]]:
    min_oid, max_oid = _arcgis_oid_minmax(query_url, oid_field=oid_field)
    print(f"▶ [{jur_upper}] OID range {min_oid:,}..{max_oid:,} across {num_shards} shards (workers={max_workers})...")
    step = max(1, (max_oid - min_oid + 1) // num_shards)
    shards = []
    for i in range(num_shards):
        lo = min_oid + i * step
        hi = max_oid if i == num_shards - 1 else (lo + step - 1)
        w = f"{oid_field} >= {lo} AND {oid_field} <= {hi} AND ({extra_where})"
        shards.append((w, f"{jur_upper}-Shard-{i + 1}/{num_shards}"))

    all_rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [
            pool.submit(
                _fetch_arcgis_cursor_shard,
                query_url,
                w,
                out_fields,
                mapper_fn,
                label=lbl,
                oid_field=oid_field,
                page_size=page_size,
                return_geometry=return_geometry,
                return_centroid=return_centroid,
            )
            for w, lbl in shards
        ]
        for fut in as_completed(futs):
            all_rows.extend(fut.result())
    return all_rows


def _build_parcel_record(
    *,
    jur: str,
    bbl: str,
    address: str,
    borough: str,
    zip_code: Optional[str],
    owner1: str,
    owner2: Optional[str] = None,
    care_of: Optional[str] = None,
    mailing_address: str = "",
    explicit_mail_state: Optional[str] = None,
    land_use: Optional[str] = None,
    bld_class: Optional[str] = None,
    units: Optional[int] = None,
    year_built: Optional[int] = None,
    assessed_total: Optional[float] = None,
    last_sale_date: Optional[dt.date] = None,
    last_sale_price: Optional[float] = None,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    source_system: str,
    col_max_len: int = 40,
) -> Optional[Dict[str, Any]]:
    if not bbl or not address or not owner1:
        return None
    demangled = demangle_assessor_owner(
        owner1,
        owner2=owner2,
        care_of=care_of,
        mailing_address=mailing_address,
        explicit_mailing_state=explicit_mail_state,
        col_max_len=col_max_len,
    )
    cleaned_owner = demangled.cleaned_owner or owner1
    clean_mail = strip_benign_care_of_prefix(mailing_address)
    coarse_mail = normalize_mailing_address_coarse(clean_mail)
    if year_built is not None and (year_built <= 1650 or year_built > 2030):
        year_built = None
    if units is not None and (units < 0 or units > 25000):
        units = None

    return {
        "bbl": bbl[:80],
        "address": address.strip(),
        "borough": (borough or jur.upper()).strip().upper(),
        "zip_code": zip_code,
        "owner_name": cleaned_owner,
        "owner_name_norm": demangled.owner_norm or normalize_owner_name(cleaned_owner),
        "owner_co_name": demangled.care_of_entity,
        "owner_is_truncated": demangled.is_truncated,
        "mailing_address": mailing_address,
        "mailing_address_norm": normalize_link_address(clean_mail),
        "mailing_address_coarse": coarse_mail,
        "mailing_state": demangled.mailing_state,
        "land_use": land_use,
        "bld_class": bld_class,
        "num_floors": None,
        "units_res": units,
        "units_total": units,
        "year_built": year_built,
        "assessed_total": assessed_total,
        "last_sale_date": last_sale_date,
        "last_sale_price": last_sale_price,
        "latitude": lat,
        "longitude": lng,
        "source_system": source_system,
    }


# ==============================================================================
# STATE-SPECIFIC PARCEL MAPPERS & FETCHERS
# ==============================================================================

def fetch_vermont_parcels() -> List[Dict[str, Any]]:
    url = (
        "https://services1.arcgis.com/BkFxaEFNwHqX3tAw/arcgis/rest/services/"
        "FS_VCGI_OPENDATA_Cadastral_VTPARCELS_poly_standardized_parcels_SP_v1/FeatureServer/0/query"
    )
    fields = (
        "OBJECTID,SPAN,PARCID,TNAME,TOWN,E911ADDR,LOCAPROP,OWNER1,OWNER2,"
        "ADDRGL1,ADDRGL2,CITYGL,STGL,ZIPGL,DESCPROP,CAT,REAL_FLV,IMPRV_LV"
    )
    where = "OWNER1 IS NOT NULL AND (E911ADDR IS NOT NULL OR LOCAPROP IS NOT NULL) AND IMPRV_LV > 0"

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        owner1 = source_text(a.get("OWNER1"))
        site = source_text(a.get("E911ADDR")) or source_text(a.get("LOCAPROP"))
        span = source_text(a.get("SPAN")) or source_text(a.get("PARCID"))
        town = source_text(a.get("TNAME")) or source_text(a.get("TOWN")) or "VERMONT"
        if not owner1 or not site or not span:
            return None
        bbl = f"VT-{span}"
        mstreet = " ".join(p for p in (source_text(a.get("ADDRGL1")), source_text(a.get("ADDRGL2"))) if p)
        mst = source_text(a.get("STGL"))
        mzip = _clean_zip5(a.get("ZIPGL"), allow_leading_zero_4=True)
        mail = compose_address(mstreet, source_text(a.get("CITYGL")) or "", mst or "", mzip or "")
        lat, lng = _extract_feat_coords("vt", feat)
        cat = source_text(a.get("CAT"))
        desc = source_text(a.get("DESCPROP")) or (f"VT CAT {cat}" if cat else None)
        return _build_parcel_record(
            jur="vt",
            bbl=bbl,
            address=site,
            borough=town,
            zip_code=None,
            owner1=owner1,
            owner2=source_text(a.get("OWNER2")),
            mailing_address=mail,
            explicit_mail_state=mst,
            land_use=desc,
            bld_class=cat,
            assessed_total=source_float(a.get("REAL_FLV")) or source_float(a.get("IMPRV_LV")),
            lat=lat,
            lng=lng,
            source_system="VT_VCGI_GRAND_LIST",
        )

    return _sharded_oid_fetch(url, where, fields, _map, jur_upper="VT", num_shards=6, max_workers=6, return_centroid=True)


def fetch_colorado_parcels() -> List[Dict[str, Any]]:
    url = "https://gis.colorado.gov/public/rest/services/Address_and_Parcel/Colorado_Public_Parcels/FeatureServer/0/query"
    fields = (
        "OBJECTID,countyName,countyFips,parcel_id,situsAdd,sitAddCty,sitAddZip,"
        "owner,owner2,ownerAdd,ownAddCty,ownAddStt,ownAddZip,landUseCde,landUseDsc,"
        "saleDate,salePrice,apprValTot,asedValTot"
    )
    where = (
        "owner IS NOT NULL AND situsAdd IS NOT NULL "
        "AND (apprValTot IS NULL OR apprValTot <> '0')"
    )

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        owner1 = source_text(a.get("owner"))
        site = source_text(a.get("situsAdd"))
        pid = source_text(a.get("parcel_id"))
        if not owner1 or not site or not pid:
            return None
        lu_desc = source_text(a.get("landUseDsc")) or ""
        lu_up = lu_desc.upper()
        if any(ex in lu_up for ex in ("EXEMPT", "FOREST", "MINING", "GRAZING", "AGRICULTUR", "VACANT LAND")):
            return None
        cfips = (source_text(a.get("countyFips")) or "000").zfill(3)
        cname = source_text(a.get("countyName")) or "COLORADO"
        city = source_text(a.get("sitAddCty")) or cname
        bbl = f"CO{cfips}-{pid}"
        mst = source_text(a.get("ownAddStt"))
        mzip = _clean_zip5(a.get("ownAddZip"))
        mail = compose_address(source_text(a.get("ownerAdd")) or "", source_text(a.get("ownAddCty")) or "", mst or "", mzip or "")
        lat, lng = _extract_feat_coords("co", feat)
        val = source_float(a.get("apprValTot")) or source_float(a.get("asedValTot"))
        return _build_parcel_record(
            jur="co",
            bbl=bbl,
            address=site,
            borough=city,
            zip_code=_clean_zip5(a.get("sitAddZip")),
            owner1=owner1,
            owner2=source_text(a.get("owner2")),
            mailing_address=mail,
            explicit_mail_state=mst,
            land_use=lu_desc or None,
            bld_class=source_text(a.get("landUseCde")),
            assessed_total=val,
            last_sale_date=_parse_any_date(a.get("saleDate")),
            last_sale_price=source_float(a.get("salePrice")),
            lat=lat,
            lng=lng,
            source_system="CO_OIT_PUBLIC_PARCELS",
        )

    return _sharded_oid_fetch(url, where, fields, _map, jur_upper="CO", num_shards=10, max_workers=8, return_centroid=True)


def fetch_new_york_state_parcels() -> List[Dict[str, Any]]:
    url = "https://services6.arcgis.com/EbVsqZ18sv1kVJ3k/arcgis/rest/services/NYS_Tax_Parcels_Public/FeatureServer/1/query"
    fields = (
        "OBJECTID,SWIS_SBL_ID,SBL,PRINT_KEY,COUNTY_NAME,MUNI_NAME,CITYTOWN_NAME,"
        "PARCEL_ADDR,LOC_ZIP,PROP_CLASS,FULL_MARKET_VAL,TOTAL_AV,YR_BLT,"
        "PRIMARY_OWNER,ADD_OWNER,MAIL_ADDR,PO_BOX,MAIL_CITY,MAIL_STATE,MAIL_ZIP"
    )
    where = (
        "PRIMARY_OWNER IS NOT NULL AND PARCEL_ADDR IS NOT NULL "
        "AND COUNTY_NAME NOT IN ('New York','Kings','Queens','Bronx','Richmond') "
        "AND (PROP_CLASS LIKE '2%' OR PROP_CLASS IN ('411','414','415','416','417','418','481','482','483'))"
    )

    ny_class_labels = {
        "210": "SINGLE FAMILY RESIDENCE",
        "215": "SINGLE FAMILY WITH ACCESSORY APT",
        "220": "TWO FAMILY RESIDENCE",
        "230": "THREE FAMILY RESIDENCE",
        "240": "RURAL RESIDENCE",
        "270": "MOBILE HOME",
        "271": "MULTIPLE MOBILE HOMES",
        "280": "MULTIPLE RESIDENCES",
        "281": "MULTIPLE RESIDENCES",
        "283": "RESIDENCE WITH COMMERCIAL USE",
        "411": "APARTMENTS (4+ UNITS)",
        "416": "MOBILE HOME PARK",
        "418": "ROOMING / BOARDING HOUSE",
        "481": "DOWNTOWN ROW MIXED-USE",
        "482": "DOWNTOWN ROW DETACHED MIXED-USE",
        "483": "CONVERTED RESIDENCE MIXED-USE",
    }

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        owner1 = source_text(a.get("PRIMARY_OWNER"))
        site = source_text(a.get("PARCEL_ADDR"))
        swis_sbl = source_text(a.get("SWIS_SBL_ID")) or source_text(a.get("SBL")) or source_text(a.get("PRINT_KEY"))
        if not owner1 or not site or not swis_sbl:
            return None
        county = source_text(a.get("COUNTY_NAME")) or "NY"
        muni = source_text(a.get("MUNI_NAME")) or source_text(a.get("CITYTOWN_NAME")) or county
        pclass = source_text(a.get("PROP_CLASS")) or ""
        units_hint = None
        if pclass == "210":
            units_hint = 1
        elif pclass in ("215", "220"):
            units_hint = 2
        elif pclass == "230":
            units_hint = 3

        mstreet = source_text(a.get("MAIL_ADDR")) or (f"PO BOX {a['PO_BOX']}" if source_text(a.get("PO_BOX")) else "")
        mst = source_text(a.get("MAIL_STATE"))
        mzip = _clean_zip5(a.get("MAIL_ZIP"), allow_leading_zero_4=True)
        mail = compose_address(mstreet, source_text(a.get("MAIL_CITY")) or "", mst or "", mzip or "")
        lat, lng = _extract_feat_coords("ny", feat)
        return _build_parcel_record(
            jur="ny",
            bbl=f"NY-{swis_sbl}",
            address=site,
            borough=f"{muni.upper()} ({county.upper()})",
            zip_code=_clean_zip5(a.get("LOC_ZIP"), allow_leading_zero_4=True),
            owner1=owner1,
            owner2=source_text(a.get("ADD_OWNER")),
            mailing_address=mail,
            explicit_mail_state=mst,
            land_use=ny_class_labels.get(pclass, f"ORPTS {pclass}" if pclass else None),
            bld_class=pclass or None,
            units=units_hint,
            year_built=source_int(a.get("YR_BLT")),
            assessed_total=source_float(a.get("FULL_MARKET_VAL")) or source_float(a.get("TOTAL_AV")),
            lat=lat,
            lng=lng,
            source_system="NYS_ORPTS_PUBLIC_PARCELS",
        )

    return _sharded_oid_fetch(
        url, where, fields, _map, jur_upper="NY", page_size=1000, num_shards=12, max_workers=8, return_centroid=True
    )


def fetch_florida_parcels() -> List[Dict[str, Any]]:
    url = (
        "https://services9.arcgis.com/Gh9awoU677aKree0/arcgis/rest/services/"
        "Florida_Statewide_Parcel_Centroid_Version/FeatureServer/0/query"
    )
    fields = (
        "OBJECTID,CO_NO,PARCEL_ID,STATE_PAR_,DOR_UC,JV,LND_VAL,NO_RES_UNT,ACT_YR_BLT,EFF_YR_BLT,"
        "SALE_PRC1,SALE_YR1,SALE_MO1,OWN_NAME,OWN_ADDR1,OWN_ADDR2,OWN_CITY,OWN_STATE_,OWN_ZIPCD,"
        "PHY_ADDR1,PHY_ADDR2,PHY_CITY,PHY_ZIPCD"
    )
    # Capture 100% of multifamily apartments (003, 008), co-ops (005), retirement housing (006),
    # improved misc residential (007), mixed-use residential (012), improved mobile home parks (028),
    # PLUS all non-homesteaded investor/owner single-family homes (001), mobile homes (002), and condos (004).
    where = (
        "OWN_NAME IS NOT NULL AND PHY_ADDR1 IS NOT NULL AND "
        "((DOR_UC IN ('003','005','006','008','012')) OR "
        "(DOR_UC IN ('007','028') AND (NO_RES_UNT > 0 OR ACT_YR_BLT > 0)) OR "
        "(DOR_UC IN ('001','002','004') AND (AV_HMSTD IS NULL OR AV_HMSTD = 0)))"
    )

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        owner1 = source_text(a.get("OWN_NAME"))
        pa1 = source_text(a.get("PHY_ADDR1"))
        pa2 = source_text(a.get("PHY_ADDR2"))
        site = " ".join(p for p in (pa1, pa2) if p)
        co_no = source_int(a.get("CO_NO")) or 0
        pid = source_text(a.get("STATE_PAR_")) or source_text(a.get("PARCEL_ID"))
        if not owner1 or not site or not pid:
            return None
        bbl = pid if pid.startswith("C") else f"FL{co_no:02d}-{pid}"
        county_label = FL_COUNTY_NAMES.get(co_no, f"COUNTY {co_no}")
        phy_city = source_text(a.get("PHY_CITY")) or county_label
        mstreet = " ".join(p for p in (source_text(a.get("OWN_ADDR1")), source_text(a.get("OWN_ADDR2"))) if p)
        mst = source_text(a.get("OWN_STATE_"))
        mzip = _clean_zip5(a.get("OWN_ZIPCD"))
        mail = compose_address(mstreet, source_text(a.get("OWN_CITY")) or "", mst or "", mzip or "")
        lat, lng = _extract_feat_coords("fl", feat)
        dor_uc = (source_text(a.get("DOR_UC")) or "").zfill(3) if source_text(a.get("DOR_UC")) else ""
        sale_yr = source_int(a.get("SALE_YR1"))
        sale_mo = source_int(a.get("SALE_MO1")) or 1
        sale_dt = None
        if sale_yr and 1900 <= sale_yr <= 2030 and 1 <= sale_mo <= 12:
            sale_dt = dt.date(sale_yr, sale_mo, 1)
        sale_prc = source_float(a.get("SALE_PRC1"))
        if sale_prc is not None and sale_prc <= 0:
            sale_prc = None
        return _build_parcel_record(
            jur="fl",
            bbl=bbl,
            address=site,
            borough=phy_city,
            zip_code=_clean_zip5(a.get("PHY_ZIPCD")),
            owner1=owner1,
            mailing_address=mail,
            explicit_mail_state=mst,
            land_use=FL_DOR_LABELS.get(dor_uc, f"DOR {dor_uc}" if dor_uc else None),
            bld_class=dor_uc or None,
            units=source_int(a.get("NO_RES_UNT")),
            year_built=source_int(a.get("ACT_YR_BLT")) or source_int(a.get("EFF_YR_BLT")),
            assessed_total=source_float(a.get("JV")),
            last_sale_date=sale_dt,
            last_sale_price=sale_prc,
            lat=lat,
            lng=lng,
            source_system="FL_GIO_FDOR_NAL",
            col_max_len=40,
        )

    return _sharded_oid_fetch(
        url, where, fields, _map, jur_upper="FL", page_size=2000, num_shards=16, max_workers=8, return_geometry=True
    )


def fetch_wisconsin_parcels() -> List[Dict[str, Any]]:
    url = "https://services3.arcgis.com/n6uYoouQZW75n5WI/arcgis/rest/services/Wisconsin_Statewide_Parcels_DB/FeatureServer/0/query"
    fields = (
        "OBJECTID,STATEID,PARCELID,CONAME,PLACENAME,SITEADRESS,ZIPCODE,"
        "OWNERNME1,OWNERNME2,PSTLADRESS,CNTASSDVALUE,ESTFMKVALUE,IMPVALUE,PROPCLASS,LONGITUDE,LATITUDE"
    )
    # Class 1 = Residential (1-3 units), Class 2 = Commercial (includes 4+ unit apartments under Wis. Stat. § 70.32)
    where = (
        "IMPVALUE > 0 AND OWNERNME1 IS NOT NULL AND SITEADRESS IS NOT NULL "
        "AND (PROPCLASS LIKE '%1%' OR PROPCLASS LIKE '%2%')"
    )

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        owner1 = source_text(a.get("OWNERNME1"))
        site = source_text(a.get("SITEADRESS"))
        sid = source_text(a.get("STATEID")) or source_text(a.get("PARCELID"))
        if not owner1 or not site or not sid:
            return None
        coname = source_text(a.get("CONAME")) or "WISCONSIN"
        place = re.sub(r"^(?:CITY|VILLAGE|TOWN)\s+OF\s+", "", (source_text(a.get("PLACENAME")) or coname).upper()).strip()
        pclass = source_text(a.get("PROPCLASS")) or ""
        lu = "RESIDENTIAL (CLASS 1)" if "1" in pclass and "2" not in pclass else (
            "APARTMENTS / COMMERCIAL (CLASS 2)" if "2" in pclass and "1" not in pclass else f"MIXED CLASS {pclass}"
        )
        lat, lng = _extract_feat_coords("wi", feat, lat_attr="LATITUDE", lng_attr="LONGITUDE")
        return _build_parcel_record(
            jur="wi",
            bbl=f"WI-{sid}",
            address=site,
            borough=place or coname,
            zip_code=_clean_zip5(a.get("ZIPCODE")),
            owner1=owner1,
            owner2=source_text(a.get("OWNERNME2")),
            mailing_address=source_text(a.get("PSTLADRESS")) or "",
            land_use=lu,
            bld_class=pclass or None,
            assessed_total=source_float(a.get("ESTFMKVALUE")) or source_float(a.get("CNTASSDVALUE")),
            lat=lat,
            lng=lng,
            source_system="WI_SCO_DOA_V11",
        )

    return _sharded_oid_fetch(
        url, where, fields, _map, jur_upper="WI", page_size=2000, num_shards=12, max_workers=8, return_geometry=False
    )


def fetch_north_carolina_parcels() -> List[Dict[str, Any]]:
    url = "https://services.nconemap.gov/secure/rest/services/NC1Map_Parcels/FeatureServer/0/query"
    fields = (
        "objectid,nparno,parno,cntyname,ownname,ownname2,mailadd,maddrno,maddpref,maddstr,maddsttyp,maddstsuf,munit,mcity,mstate,mzip,"
        "siteadd,saddno,saddpref,saddstr,saddsttyp,saddstsuf,sunit,scity,szip,parusecode,parusedesc,improvval,parval,struct,structyear,saledate"
    )
    # NC OneMap has sparse objectid blocks between counties; query the 100 NC counties via county list
    nc_counties = [
        "Alamance", "Alexander", "Alleghany", "Anson", "Ashe", "Avery", "Beaufort", "Bertie", "Bladen", "Brunswick",
        "Buncombe", "Burke", "Cabarrus", "Caldwell", "Camden", "Carteret", "Caswell", "Catawba", "Chatham", "Cherokee",
        "Chowan", "Clay", "Cleveland", "Columbus", "Craven", "Cumberland", "Currituck", "Dare", "Davidson", "Davie",
        "Duplin", "Durham", "Edgecombe", "Forsyth", "Franklin", "Gaston", "Gates", "Graham", "Granville", "Greene",
        "Guilford", "Halifax", "Harnett", "Haywood", "Henderson", "Hertford", "Hoke", "Hyde", "Iredell", "Jackson",
        "Johnston", "Jones", "Lee", "Lenoir", "Lincoln", "Macon", "Madison", "Martin", "McDowell", "Mecklenburg",
        "Mitchell", "Montgomery", "Moore", "Nash", "New Hanover", "Northampton", "Onslow", "Orange", "Pamlico",
        "Pasquotank", "Pender", "Perquimans", "Person", "Pitt", "Polk", "Randolph", "Richmond", "Robeson", "Rockingham",
        "Rowan", "Rutherford", "Sampson", "Scotland", "Stanly", "Stokes", "Surry", "Swain", "Transylvania", "Tyrrell",
        "Union", "Vance", "Wake", "Warren", "Washington", "Watauga", "Wayne", "Wilkes", "Wilson", "Yadkin", "Yancey",
    ]

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        raw_own1 = source_text(a.get("ownname"))
        own2 = source_text(a.get("ownname2"))
        if raw_own1 and ";" in raw_own1 and not own2:
            parts = [p.strip() for p in raw_own1.split(";") if p.strip()]
            owner1 = parts[0] if parts else raw_own1
            own2 = parts[1] if len(parts) > 1 else None
        else:
            owner1 = raw_own1

        site = source_text(a.get("siteadd"))
        if not site:
            site_parts = [
                source_text(a.get("saddno")),
                source_text(a.get("saddpref")),
                source_text(a.get("saddstr")),
                source_text(a.get("saddsttyp")),
                source_text(a.get("saddstsuf")),
                source_text(a.get("sunit")),
            ]
            site = " ".join(p for p in site_parts if p).strip() or None

        npid = source_text(a.get("nparno")) or source_text(a.get("parno"))
        if not owner1 or not site or not npid:
            return None
        udesc = source_text(a.get("parusedesc")) or ""
        uup = udesc.upper()
        if any(ex in uup for ex in ("EXEMPT", "GOVERNMENT", "FOREST", "TIMBER", "VACANT", "INDUSTRIAL", "CHURCH")):
            return None
        cnty = source_text(a.get("cntyname")) or "NC"
        scity = source_text(a.get("scity")) or cnty
        mst = source_text(a.get("mstate"))
        mzip = _clean_zip5(a.get("mzip"))
        raw_madd = source_text(a.get("mailadd")) or ""
        if not raw_madd:
            mail_parts = [
                source_text(a.get("maddrno")),
                source_text(a.get("maddpref")),
                source_text(a.get("maddstr")),
                source_text(a.get("maddsttyp")),
                source_text(a.get("maddstsuf")),
                source_text(a.get("munit")),
            ]
            raw_madd = " ".join(p for p in mail_parts if p).strip()
        mcity = source_text(a.get("mcity")) or ""
        mail = compose_address(raw_madd, mcity, mst or "", mzip or "") if (mcity and raw_madd) else raw_madd
        lat, lng = _extract_feat_coords("nc", feat)
        return _build_parcel_record(
            jur="nc",
            bbl=f"NC-{npid}",
            address=site,
            borough=scity,
            zip_code=_clean_zip5(a.get("szip")),
            owner1=owner1,
            owner2=own2,
            mailing_address=mail,
            explicit_mail_state=mst,
            land_use=udesc or source_text(a.get("parusecode")),
            bld_class=source_text(a.get("parusecode")),
            year_built=source_int(a.get("structyear")),
            assessed_total=source_float(a.get("parval")) or source_float(a.get("improvval")),
            last_sale_date=_parse_any_date(a.get("saledate")),
            lat=lat,
            lng=lng,
            source_system="NC_ONEMAP_PARCELS",
        )

    print(f"▶ [NC] Fetching improved parcels across {len(nc_counties)} North Carolina counties...")
    all_rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [
            pool.submit(
                _fetch_arcgis_cursor_shard,
                url,
                (
                    f"cntyname = '{c}' AND (improvval > 0 OR struct = 'Y') "
                    f"AND ownname IS NOT NULL AND ownname <> '' "
                    f"AND ((siteadd IS NOT NULL AND siteadd <> '') OR (saddstr IS NOT NULL AND saddstr <> ''))"
                ),
                fields,
                _map,
                label=f"NC-{c}",
                oid_field="objectid",
                page_size=5000,
                return_geometry=True,
            )
            for c in nc_counties
        ]
        for fut in as_completed(futs):
            all_rows.extend(fut.result())
    return all_rows


# ==============================================================================
# STATE LICENSING & ENFORCEMENT OVERLAYS
# ==============================================================================

def enrich_fl_dbpr_apartments(conn, target_jurisdictions: Sequence[str] = ("fl", "miami")) -> Dict[str, Any]:
    """Download FL DBPR Division of Hotels & Restaurants `hrlodge1..7.csv` extracts and attach
    active Chapter 509 apartment building licenses (`NAPT`, `TAPT`) onto `fl` and/or `miami`."""
    print(f"▶ [FL DBPR] Downloading & linking Chapter 509 Apartment Licenses for {list(target_jurisdictions)}...")
    extract_urls = [
        f"http://www.myfloridalicense.com/dbpr/sto/file_download/extracts/hrlodge{i}.csv"
        for i in range(1, 8)
    ]

    # Collect active apartment records once
    apt_records: List[Dict[str, Any]] = []
    for u in extract_urls:
        with _stream_csv_via_curl(u) as reader:
            header = next(reader, None)
            if not header:
                continue
            idx = {c.strip(): i for i, c in enumerate(header)}
            i_rank = idx.get("Rank Code", 3)
            i_pstat = idx.get("Primary Status Code", 28)
            i_lic = idx.get("License Number", 27)
            i_lname = idx.get("Licensee Name", 2)
            i_dba = idx.get("Business Name", 14)
            i_mstr = idx.get("Mailing Street Address", 6)
            i_mcity = idx.get("Mailing City", 9)
            i_mst = idx.get("Mailing State Code", 10)
            i_mzip = idx.get("Mailing Zip Code", 11)
            i_lstr = idx.get("Location Street Address", 16)
            i_lcity = idx.get("Location City", 19)
            i_lzip = idx.get("Location Zip Code", 21)
            i_lcnty = idx.get("Location County", 23)
            i_exp = idx.get("License Expiry Date", 30)
            i_insp = idx.get("Last Inspection Date", 31)
            i_units = idx.get("Number of Seats or Rental Units", 32)

            for row in reader:
                if len(row) <= i_units:
                    continue
                rank = (row[i_rank] or "").strip().upper()
                pstat = (row[i_pstat] or "").strip()
                if rank not in ("NAPT", "TAPT") or pstat != "20":
                    continue
                lic_no = source_text(row[i_lic])
                lstr = source_text(row[i_lstr])
                lname = source_text(row[i_lname])
                if not lic_no or not lstr or not lname:
                    continue
                apt_records.append({
                    "lic_no": f"DBPR-{lic_no}",
                    "rank": rank,
                    "licensee": lname,
                    "dba": source_text(row[i_dba]),
                    "loc_addr": lstr,
                    "loc_addr_norm": normalize_link_address(lstr),
                    "loc_city": (source_text(row[i_lcity]) or "").upper(),
                    "loc_zip": _clean_zip5(row[i_lzip]),
                    "loc_county": (source_text(row[i_lcnty]) or "").upper(),
                    "mail_addr": source_text(row[i_mstr]) or "",
                    "mail_city": source_text(row[i_mcity]) or "",
                    "mail_state": source_text(row[i_mst]) or "FL",
                    "mail_zip": _clean_zip5(row[i_mzip]) or "",
                    "exp_date": _parse_any_date(row[i_exp]),
                    "insp_date": _parse_any_date(row[i_insp]),
                    "units": source_int(row[i_units]),
                })

    print(f"  [FL DBPR] Parsed {len(apt_records):,} active Chapter 509 apartment building licenses statewide.")
    summary: Dict[str, Any] = {"active_apartment_licenses_statewide": len(apt_records), "matched_by_jurisdiction": {}}

    for jur in target_jurisdictions:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{jur}_properties",))
            if not cur.fetchone()[0]:
                continue
            cur.execute(f"SELECT bbl, address, borough, zip_code, owner_name FROM {jur}_properties WHERE address IS NOT NULL")
            by_addr_zip: Dict[Tuple[str, str], str] = {}
            by_addr_city: Dict[Tuple[str, str], List[str]] = defaultdict(list)
            for bbl, addr, boro, zp, _own in cur.fetchall():
                anorm = normalize_link_address(addr)
                if not anorm:
                    continue
                z5 = _clean_zip5(zp)
                if z5:
                    by_addr_zip.setdefault((anorm, z5), bbl)
                if boro:
                    by_addr_city[(anorm, boro.strip().upper())].append(bbl)

        reg_rows = []
        contact_rows = []
        prop_updates = []
        seen_regs = set()

        for rec in apt_records:
            if jur == "miami" and rec["loc_county"] not in ("DADE", "MIAMI-DADE", "") and not (rec["loc_zip"] or "").startswith("33"):
                continue
            anorm = rec["loc_addr_norm"]
            if not anorm:
                continue
            bbl = None
            if rec["loc_zip"]:
                bbl = by_addr_zip.get((anorm, rec["loc_zip"]))
            if not bbl and rec["loc_city"]:
                cands = by_addr_city.get((anorm, rec["loc_city"]), [])
                if len(cands) == 1:
                    bbl = cands[0]
            if not bbl or rec["lic_no"] in seen_regs:
                continue
            seen_regs.add(rec["lic_no"])

            is_corp = looks_like_business_name(rec["licensee"])
            ctype = "HEADOFFICER" if is_corp else "INDIVIDUALOWNER"
            lnorm = normalize_business_name(rec["licensee"]) if is_corp else normalize_person_name(rec["licensee"])

            reg_rows.append((
                rec["lic_no"],
                bbl,
                rec["loc_addr"],
                rec["loc_city"],
                rec["loc_zip"],
                rec["loc_city"] or jur.upper(),
                "ACTIVE",
                rec["insp_date"],
                rec["exp_date"],
            ))
            contact_rows.append((
                rec["lic_no"],
                ctype,
                rec["licensee"] if is_corp else None,
                lnorm if is_corp else None,
                None if is_corp else rec["licensee"],
                None if is_corp else lnorm,
                rec["mail_addr"],
                rec["mail_city"],
                rec["mail_state"],
                rec["mail_zip"],
            ))
            prop_updates.append((rec["lic_no"], rec["exp_date"], rec["units"] or 0, bbl))

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {jur}_hpd_contacts WHERE registration_id LIKE 'DBPR-%'")
            cur.execute(f"DELETE FROM {jur}_hpd_registrations WHERE registration_id LIKE 'DBPR-%'")
            if reg_rows:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {jur}_hpd_registrations (
                        registration_id, bbl, building_address, building_city, building_zip,
                        borough, lifecycle_stage, last_registration_date, registration_end_date
                    ) VALUES %s ON CONFLICT DO NOTHING;
                    """,
                    reg_rows,
                    page_size=2000,
                )
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {jur}_hpd_contacts (
                        registration_id, contact_type, corporation_name, corporation_name_norm,
                        full_name, full_name_norm, business_address, business_city, business_state, business_zip
                    ) VALUES %s;
                    """,
                    contact_rows,
                    page_size=2000,
                )
                execute_values(
                    cur,
                    f"""
                    UPDATE {jur}_properties AS p
                    SET compliance_active = TRUE,
                        compliance_record_id = v.lic_no,
                        compliance_expiration = v.exp_dt,
                        units_res = CASE WHEN COALESCE(p.units_res, 0) = 0 AND v.u > 0 THEN v.u ELSE p.units_res END,
                        units_total = CASE WHEN COALESCE(p.units_total, 0) = 0 AND v.u > 0 THEN v.u ELSE p.units_total END
                    FROM (VALUES %s) AS v(lic_no, exp_dt, u, bbl)
                    WHERE p.bbl = v.bbl;
                    """,
                    prop_updates,
                    template="(%s, %s::date, %s::numeric, %s)",
                    page_size=2000,
                )
        conn.commit()
        summary["matched_by_jurisdiction"][jur] = len(reg_rows)
        print(f"  ✓ [FL DBPR -> {jur}] Linked {len(reg_rows):,} active apartment building licenses.")

    _record_status(conn, "FL_DBPR_APARTMENTS", "rental_licensing", summary)
    return summary


def enrich_denver_rental_licenses(conn) -> int:
    """Attach Denver Active Residential Rental Property Licenses (`FeatureServer/52`) onto `co`."""
    url = (
        "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/"
        "ODC_residential_rental_property_active_licenses/FeatureServer/52/query"
    )
    print("▶ [CO / Denver] Fetching Denver Active Residential Rental Property Licenses...")
    with conn.cursor() as cur:
        cur.execute("SELECT bbl, address, zip_code FROM co_properties WHERE borough = 'DENVER' OR zip_code LIKE '802%'")
        by_pid: Dict[str, str] = {}
        by_addr: Dict[str, str] = {}
        for bbl, addr, _zp in cur.fetchall():
            # CO031-<parcel_id>
            raw_pid = bbl.split("-", 1)[-1].lstrip("0")
            if raw_pid:
                by_pid[raw_pid] = bbl
            anorm = normalize_link_address(addr)
            if anorm:
                by_addr.setdefault(anorm, bbl)

    last_oid = -1
    reg_rows = []
    contact_rows = []
    prop_updates = []
    seen = set()
    session = requests.Session()
    session.headers.update(HTTP_HEADERS)
    try:
        while True:
            resp = session.get(
                url,
                params={
                    "where": f"OBJECTID > {last_oid}",
                    "outFields": "*",
                    "orderByFields": "OBJECTID ASC",
                    "resultRecordCount": 2000,
                    "f": "json",
                },
                timeout=45,
            ).json()
            feats = resp.get("features") or []
            if not feats:
                break
            for f in feats:
                a = f.get("attributes") or {}
                oid = source_int(a.get("OBJECTID")) or 0
                if oid > last_oid:
                    last_oid = oid
                lnum = source_text(a.get("LICENSE_NUM"))
                pnum = (source_text(a.get("PARCEL_NUM")) or "").lstrip("0")
                addr = source_text(a.get("ADDRESS"))
                ent = source_text(a.get("ENTITY_NAME"))
                if not lnum or lnum in seen:
                    continue
                bbl = by_pid.get(pnum) if pnum else None
                if not bbl and addr:
                    bbl = by_addr.get(normalize_link_address(addr))
                if not bbl:
                    continue
                seen.add(lnum)
                reg_id = f"DEN-{lnum}"
                iss_dt = _parse_any_date(a.get("ISSUANCE_DATE"))
                exp_dt = _parse_any_date(a.get("EXPIRATION_DATE"))
                reg_rows.append((reg_id, bbl, addr, "DENVER", _clean_zip5(a.get("ZIP")), "DENVER", "ACTIVE", iss_dt, exp_dt))
                if ent and not is_excluded_third_party_entity(ent):
                    is_corp = looks_like_business_name(ent)
                    lnorm = normalize_business_name(ent) if is_corp else normalize_person_name(ent)
                    contact_rows.append((
                        reg_id,
                        "HEADOFFICER" if is_corp else "INDIVIDUALOWNER",
                        ent if is_corp else None,
                        lnorm if is_corp else None,
                        None if is_corp else ent,
                        None if is_corp else lnorm,
                    ))
                prop_updates.append((reg_id, exp_dt, bbl))
            if len(feats) < 2000:
                break
    finally:
        session.close()

    with conn.cursor() as cur:
        cur.execute("DELETE FROM co_hpd_contacts WHERE registration_id LIKE 'DEN-%'")
        cur.execute("DELETE FROM co_hpd_registrations WHERE registration_id LIKE 'DEN-%'")
        if reg_rows:
            execute_values(
                cur,
                """
                INSERT INTO co_hpd_registrations (
                    registration_id, bbl, building_address, building_city, building_zip,
                    borough, lifecycle_stage, last_registration_date, registration_end_date
                ) VALUES %s ON CONFLICT DO NOTHING;
                """,
                reg_rows,
                page_size=2000,
            )
        if contact_rows:
            execute_values(
                cur,
                """
                INSERT INTO co_hpd_contacts (
                    registration_id, contact_type, corporation_name, corporation_name_norm, full_name, full_name_norm
                ) VALUES %s;
                """,
                contact_rows,
                page_size=2000,
            )
        if prop_updates:
            execute_values(
                cur,
                """
                UPDATE co_properties AS p
                SET compliance_active = TRUE,
                    compliance_record_id = v.rid,
                    compliance_expiration = v.exp_dt
                FROM (VALUES %s) AS v(rid, exp_dt, bbl)
                WHERE p.bbl = v.bbl;
                """,
                prop_updates,
                template="(%s, %s::date, %s)",
                page_size=2000,
            )
    conn.commit()
    _record_status(
        conn,
        "DENVER_RENTAL_LICENSES",
        "rental_licensing",
        {"matched_licenses": len(reg_rows), "source_url": url},
    )
    print(f"  ✓ [CO / Denver] Linked {len(reg_rows):,} active Denver residential rental licenses.")
    return len(reg_rows)


def enrich_ny_buffalo_enforcement_and_registry(conn) -> Dict[str, int]:
    """Attach Buffalo Rental Registry (`hpqg-ihzt`), Code Violations (`ivrf-k9vm`),
    and Housing Court Cases (`k3vq-gzry`) onto `ny` Erie/Buffalo parcels."""
    print("▶ [NY / Buffalo] Enriching Buffalo Rental Registry, Code Violations & Housing Court Cases...")
    with conn.cursor() as cur:
        cur.execute("SELECT bbl, address, zip_code FROM ny_properties WHERE borough LIKE '%BUFFALO%' OR borough LIKE '%ERIE%'")
        by_sbl: Dict[str, str] = {}
        by_addr: Dict[str, str] = {}
        for bbl, addr, _zp in cur.fetchall():
            # bbl = NY-<SWIS_SBL_ID>; Buffalo SWIS = 140200 + 20-char SBL
            raw = bbl.replace("NY-", "")
            if len(raw) > 6:
                sbl_tail = raw[6:].rstrip("0")
                if sbl_tail:
                    by_sbl[sbl_tail] = bbl
            anorm = normalize_link_address(addr)
            if anorm:
                by_addr.setdefault(anorm, bbl)

    def _match_buf(sbl_raw: Optional[str], addr_raw: Optional[str]) -> Optional[str]:
        if sbl_raw:
            stail = re.sub(r"[^0-9A-Za-z]", "", sbl_raw).rstrip("0")
            if stail and stail in by_sbl:
                return by_sbl[stail]
        if addr_raw:
            anorm = normalize_link_address(addr_raw)
            if anorm and anorm in by_addr:
                return by_addr[anorm]
        return None

    # 1. Buffalo Rental Registry (hpqg-ihzt)
    reg_rows = []
    seen_reg = set()
    with _stream_csv_via_curl("https://data.buffalony.gov/api/views/hpqg-ihzt/rows.csv?accessType=DOWNLOAD") as reader:
        hdr = next(reader, [])
        idx = {c.strip().lower(): i for i, c in enumerate(hdr)}
        i_stat = idx.get("license status", idx.get("licstatus"))
        i_lic = idx.get("license number", idx.get("licenseno"))
        i_sbl = idx.get("sbl")
        i_addr = idx.get("address")
        i_zip = idx.get("zip code", idx.get("zip_code"))
        i_iss = idx.get("issued datetime", idx.get("issdttm"))
        i_exp = idx.get("expiration datetime", idx.get("expdttm"))
        for row in reader:
            if len(row) < 10:
                continue
            lstat = (row[i_stat] if i_stat is not None and i_stat < len(row) else "").strip().upper()
            if lstat != "ACTIVE":
                continue
            lic = source_text(row[i_lic]) if i_lic is not None and i_lic < len(row) else None
            sbl = source_text(row[i_sbl]) if i_sbl is not None and i_sbl < len(row) else None
            addr = source_text(row[i_addr]) if i_addr is not None and i_addr < len(row) else None
            bbl = _match_buf(sbl, addr)
            if not bbl or not lic or lic in seen_reg:
                continue
            seen_reg.add(lic)
            reg_rows.append((
                f"BUF-{lic}",
                bbl,
                addr,
                "BUFFALO",
                _clean_zip5(row[i_zip]) if i_zip is not None and i_zip < len(row) else None,
                "BUFFALO (ERIE)",
                "ACTIVE",
                _parse_any_date(row[i_iss]) if i_iss is not None and i_iss < len(row) else None,
                _parse_any_date(row[i_exp]) if i_exp is not None and i_exp < len(row) else None,
            ))

    # 2. Buffalo Code Violations (ivrf-k9vm) & Housing Court (k3vq-gzry)
    stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "vt": 0, "vo": 0, "last_v": None, "lt": 0, "lo": 0, "last_l": None
    })
    matched_viols = 0
    with _stream_csv_via_curl("https://data.buffalony.gov/api/views/ivrf-k9vm/rows.csv?accessType=DOWNLOAD") as reader:
        hdr = next(reader, [])
        idx = {c.strip().lower(): i for i, c in enumerate(hdr)}
        for row in reader:
            if len(row) < 12:
                continue
            sbl = source_text(row[idx["sbl"]]) if "sbl" in idx and idx["sbl"] < len(row) else None
            addr = source_text(row[idx["address"]]) if "address" in idx and idx["address"] < len(row) else None
            bbl = _match_buf(sbl, addr)
            if not bbl:
                continue
            matched_viols += 1
            st = stats[bbl]
            st["vt"] += 1
            status_up = (row[idx["status"]] if "status" in idx else "").strip().upper()
            if status_up in ("ACTIVE", "OPEN", "IN VIOLATION", "PENDING"):
                st["vo"] += 1
            vdt = _parse_any_date(row[idx["date"]]) if "date" in idx else None
            if vdt and (st["last_v"] is None or vdt > st["last_v"]):
                st["last_v"] = vdt

    matched_court = 0
    with _stream_csv_via_curl("https://data.buffalony.gov/api/views/k3vq-gzry/rows.csv?accessType=DOWNLOAD") as reader:
        hdr = next(reader, [])
        idx = {c.strip().lower(): i for i, c in enumerate(hdr)}
        for row in reader:
            if len(row) < 8:
                continue
            addr = source_text(row[idx["address"]]) if "address" in idx and idx["address"] < len(row) else None
            bbl = _match_buf(None, addr)
            if not bbl:
                continue
            matched_court += 1
            st = stats[bbl]
            st["lt"] += 1
            if (row[idx["status"]] if "status" in idx else "").strip().upper() == "OPEN":
                st["lo"] += 1
            ldt = _parse_any_date(row[idx["adddttm"]]) if "adddttm" in idx else None
            if ldt and (st["last_l"] is None or ldt > st["last_l"]):
                st["last_l"] = ldt

    with conn.cursor() as cur:
        cur.execute("DELETE FROM ny_hpd_registrations WHERE registration_id LIKE 'BUF-%'")
        if reg_rows:
            execute_values(
                cur,
                """
                INSERT INTO ny_hpd_registrations (
                    registration_id, bbl, building_address, building_city, building_zip,
                    borough, lifecycle_stage, last_registration_date, registration_end_date
                ) VALUES %s ON CONFLICT DO NOTHING;
                """,
                reg_rows,
                page_size=2000,
            )
            execute_values(
                cur,
                """
                UPDATE ny_properties AS p
                SET compliance_active = TRUE,
                    compliance_record_id = v.rid,
                    compliance_expiration = v.exp_dt
                FROM (VALUES %s) AS v(rid, bbl, exp_dt)
                WHERE p.bbl = v.bbl;
                """,
                [(r[0], r[1], r[8]) for r in reg_rows],
                template="(%s, %s, %s::date)",
                page_size=2000,
            )
        if stats:
            stat_rows = [
                ("ny", bbl, s["vt"], s["vo"], s["last_v"], s["lt"], s["lo"], s["last_l"])
                for bbl, s in stats.items()
            ]
            execute_values(
                cur,
                """
                INSERT INTO ny_bbl_stats (
                    jurisdiction, bbl, violations_total, violations_open, last_violation_date,
                    litigations_total, litigations_open, last_litigation_date, updated_at
                )
                VALUES %s
                ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                    violations_total = EXCLUDED.violations_total,
                    violations_open = EXCLUDED.violations_open,
                    last_violation_date = EXCLUDED.last_violation_date,
                    litigations_total = EXCLUDED.litigations_total,
                    litigations_open = EXCLUDED.litigations_open,
                    last_litigation_date = EXCLUDED.last_litigation_date,
                    updated_at = NOW();
                """,
                stat_rows,
                template="(%s, %s, %s, %s, %s::date, %s, %s, %s::date, NOW())",
                page_size=2000,
            )
    conn.commit()
    details = {
        "buffalo_active_rental_registrations": len(reg_rows),
        "buffalo_matched_code_violations": matched_viols,
        "buffalo_matched_housing_court_cases": matched_court,
        "matched_parcels": len(stats),
    }
    _record_status(conn, "BUFFALO_ENFORCEMENT", "city_enforcement", details)
    print(f"  ✓ [NY / Buffalo] {len(reg_rows):,} rental registrations, {matched_viols:,} violations, {matched_court:,} court cases.")
    return details


# ==============================================================================
# PARTITIONED PERSISTENCE & NETWORK CLUSTERING
# ==============================================================================

def build_and_save_state_networks(conn, jur: str, state_code: str, source_tag: str, *, skip_oc_relink: bool = False) -> Tuple[int, int]:
    oc_stats = {"evaluated": 0, "matched": 0, "tier1": 0, "tier2": 0, "tier3": 0, "tier4": 0, "quarantined": 0}
    if not skip_oc_relink:
        oc_stats = link_jurisdiction_parcels_to_oc(conn, jur, state_code, col_max_len=40)
        if oc_stats["evaluated"]:
            print(
                f"  [{jur.upper()}] Corporate registry linker evaluated {oc_stats['evaluated']:,} business parcels "
                f"-> {oc_stats['matched']:,} matched (T1={oc_stats['tier1']:,}, T2={oc_stats['tier2']:,}, T3={oc_stats['tier3']:,})."
            )

    registrar_substrings = load_registrar_address_substrings(conn)
    properties: List[Dict[str, Any]] = []
    with conn.cursor(name=f"state_props_scan_{jur}", cursor_factory=RealDictCursor) as scan_cur:
        scan_cur.itersize = 50000
        scan_cur.execute(
            f"""
            SELECT bbl, address, borough, zip_code, owner_name, owner_name_norm,
                   mailing_address, mailing_address_norm, owner_email, owner_email_norm,
                   land_use, bld_class, num_floors, units_res, units_total, year_built,
                   assessed_total, latitude, longitude,
                   oc_jurisdiction_code, oc_company_number, oc_match_tier, oc_match_confidence
            FROM {jur}_properties
            WHERE bbl IS NOT NULL
            """
        )
        for r in scan_cur:
            # Exclude SF fractional-week timeshare assessor sub-lots (`bld_class = 'TS'`,
            # e.g., 800+ timeshare-week rows at a single downtown hotel/garage) from CA
            # private landlord portfolios.
            if jur == "ca" and str(r.get("bld_class") or "").strip().upper() == "TS":
                continue
            properties.append(dict(r))

    prop_owner_by_bbl = {
        p["bbl"]: normalize_owner_name(p.get("owner_name") or p.get("owner_name_norm"))
        for p in properties
        if p.get("bbl")
    }
    bbl_reg_ids: Dict[str, Set[str]] = defaultdict(set)
    bbl_contact_humans: Dict[str, Set[Tuple[str, str]]] = defaultdict(set)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT r.bbl, r.registration_id,
                   UPPER(COALESCE(r.lifecycle_stage, 'ACTIVE')) AS lifecycle_stage,
                   UPPER(COALESCE(c.contact_type, '')) AS contact_type,
                   c.full_name, c.full_name_norm,
                   c.corporation_name, c.corporation_name_norm
            FROM {jur}_hpd_registrations r
            LEFT JOIN {jur}_hpd_contacts c
              ON c.jurisdiction = r.jurisdiction AND c.registration_id = r.registration_id
            WHERE r.bbl IS NOT NULL
            """
        )
        for r in cur.fetchall():
            bbl = r.get("bbl")
            rid = r.get("registration_id")
            if bbl and rid:
                bbl_reg_ids[bbl].add(rid)
            stage = r.get("lifecycle_stage") or ""
            if stage not in {"ACTIVE", "READY TO RENEW", "ISSUED", "REGISTERED", "APPROVED"}:
                continue
            cur_deed_owner = prop_owner_by_bbl.get(bbl) or ""
            if not cur_deed_owner or cur_deed_owner in GENERIC_OWNER_NAMES or is_public_entity(cur_deed_owner):
                continue
            if (r.get("contact_type") or "") not in {"INDIVIDUALOWNER", "HEADOFFICER", "JOINTOWNER"}:
                continue
            corp_norm = normalize_owner_name(r.get("corporation_name_norm") or r.get("corporation_name"))
            human_raw = (r.get("full_name") or "").strip()
            human_norm = normalize_owner_name(r.get("full_name_norm") or human_raw)
            if not ((corp_norm and corp_norm == cur_deed_owner) or (human_norm and human_norm == cur_deed_owner)):
                continue
            if human_raw and human_norm and len(human_norm) >= 5 and human_norm not in GENERIC_OWNER_NAMES and not is_public_entity(human_norm):
                from api.shared_utils import canonicalize_person_name
                h_canon = canonicalize_person_name(human_raw) or human_norm
                bbl_contact_humans[bbl].add((h_canon, human_raw))

    if not properties:
        return 0, 0

    n_properties = len(properties)
    n_municipalities = len({p.get("borough") for p in properties if p.get("borough")})

    from api.oc_linker import cluster_with_oc_and_series, load_oc_registered_agent_addresses
    from api.shared_utils import extract_owner_family_key
    ra_addrs = load_oc_registered_agent_addresses(conn)
    network_groups = run_union_find_clustering(
        properties, jur, registrar_substrings=registrar_substrings, registered_agent_addrs=ra_addrs
    )

    if bbl_contact_humans:
        bbl_to_r_c: Dict[str, str] = {}
        r_to_ps_c: Dict[str, List[dict]] = {}
        bbl_hq_c: Dict[str, str] = {}
        for rb, grp in network_groups.items():
            ded = {p["bbl"]: p for p in grp if p.get("bbl")}
            if ded:
                for b_id, p_obj in ded.items():
                    bbl_to_r_c[b_id] = rb
                    if p_obj.get("_building_key") or p_obj.get("_coarse_mail"):
                        bbl_hq_c[b_id] = p_obj.get("_building_key") or p_obj.get("_coarse_mail")
                r_to_ps_c[rb] = list(ded.values())
        par_rc = {r: r for r in r_to_ps_c}
        sz_rc = {r: len(ps) for r, ps in r_to_ps_c.items()}
        pub_rc = {
            r: any(is_public_entity(p.get("owner_name_norm") or p.get("owner_name") or "") for p in ps)
            for r, ps in r_to_ps_c.items()
        }

        def _find_rc(x: str) -> str:
            while par_rc[x] != x:
                par_rc[x] = par_rc[par_rc[x]]
                x = par_rc[x]
            return x

        h_to_bbls_c: Dict[str, List[str]] = defaultdict(list)
        h_fams_c: Dict[str, Set[str]] = defaultdict(set)
        h_hqs_c: Dict[str, Dict[str, int]] = defaultdict(dict)
        for b_id, pairs in bbl_contact_humans.items():
            if b_id not in bbl_to_r_c:
                continue
            po = prop_owner_by_bbl.get(b_id, "")
            pf = extract_owner_family_key(po) or po
            bhq = bbl_hq_c.get(b_id) or ""
            for hc, _ in pairs:
                h_to_bbls_c[hc].append(b_id)
                if pf:
                    h_fams_c[hc].add(pf)
                if bhq:
                    h_hqs_c[hc][bhq] = h_hqs_c[hc].get(bhq, 0) + 1

        c_merges = 0
        for hc, b_list in h_to_bbls_c.items():
            if len(h_fams_c[hc]) >= 8:
                hm = h_hqs_c.get(hc) or {}
                tot_m = sum(hm.values())
                top2 = sum(sorted(hm.values(), reverse=True)[:2]) if hm else 0
                if len(hm) >= 6 and (top2 / max(tot_m, 1)) < 0.30:
                    continue
            roots = list({bbl_to_r_c[b_id] for b_id in b_list if b_id in bbl_to_r_c})
            if len(roots) >= 2:
                anc = _find_rc(roots[0])
                for oth in roots[1:]:
                    uo = _find_rc(oth)
                    if anc != uo and not pub_rc.get(anc) and not pub_rc.get(uo) and sz_rc[anc] + sz_rc[uo] <= 5000:
                        par_rc[uo] = anc
                        sz_rc[anc] += sz_rc[uo]
                        c_merges += 1
        if c_merges:
            m_ng_c: Dict[str, List[dict]] = {}
            for r, ps in r_to_ps_c.items():
                m_ng_c.setdefault(_find_rc(r), []).extend(ps)
            network_groups = m_ng_c
            print(f"  [{jur.upper()}] Verified active registration contacts merged {c_merges:,} owner-principal links.")
    oc_keys = list({
        (p["oc_jurisdiction_code"], p["oc_company_number"])
        for p in properties
        if p.get("oc_jurisdiction_code") and p.get("oc_company_number")
    })
    oc_resolved = resolve_oc_company_principals(conn, oc_keys) if oc_keys else {}

    network_groups, oc_merges = cluster_with_oc_and_series(
        properties,
        network_groups,
        oc_resolved,
        is_public_fn=is_public_entity,
        generic_owner_names=GENERIC_OWNER_NAMES,
    )
    del properties
    gc.collect()
    if oc_merges:
        print(f"  [{jur.upper()}] Corporate registry & series untangling merged {oc_merges:,} multi-entity links.")

    insert_sql = f"""
        INSERT INTO {jur}_networks (
            jurisdiction, network_key, anchor_type, display_name, member_names,
            member_addresses, registration_ids, bbl_list, building_count, unit_count,
            borough_summary, connection_signals, oc_company_ids
        )
        VALUES %s
        ON CONFLICT (jurisdiction, network_key) DO NOTHING;
    """
    n_networks = 0
    batch: List[tuple] = []
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {jur}_networks;")
        while network_groups:
            root_bbl, group = network_groups.popitem()
            dedup = {p["bbl"]: p for p in group if p.get("bbl")}
            group = list(dedup.values())
            if not group:
                continue

            owner_counts: Dict[str, int] = {}
            for p in group:
                owner_counts[p["owner_name"]] = owner_counts.get(p["owner_name"], 0) + 1
            display_name = max(owner_counts, key=owner_counts.get)

            human_counts: Dict[str, int] = {
                nm: cnt for nm, cnt in owner_counts.items() if looks_like_person_owner(nm)
            }
            for p in group:
                for _, h_raw in bbl_contact_humans.get(p["bbl"], ()):
                    if looks_like_person_owner(h_raw):
                        human_counts[h_raw] = human_counts.get(h_raw, 0) + 1
            oc_company_ids = set()
            oc_holding_entities = set()
            oc_raw_hq_addrs = set()
            for p in group:
                oj, on = p.get("oc_jurisdiction_code"), p.get("oc_company_number")
                if oj and on:
                    oc_company_ids.add(f"{oj}/{on}")
                    info = oc_resolved.get((oj, on), {})
                    for h in info.get("humans", ()):
                        if looks_like_person_owner(h):
                            human_counts[h] = human_counts.get(h, 0) + 1
                    oc_holding_entities.update(info.get("holding_entities", ()))
                    oc_raw_hq_addrs.update(info.get("raw_hq_addresses", ()))

            entity_counts = {
                nm: cnt for nm, cnt in owner_counts.items() if not looks_like_person_owner(nm)
            }
            source_humans = [
                nm for nm, _ in sorted(human_counts.items(), key=lambda kv: (-kv[1], kv[0]))
            ]
            primary_human = source_humans[0] if source_humans else None
            safe_root = re.sub(r"[^a-z0-9_.-]", "_", root_bbl.lower())
            network_key = f"network_{jur}_{safe_root}"
            anchor_type = (
                "person"
                if not any(
                    w in display_name.upper()
                    for w in ("LLC", "INC", "CORP", "GROUP", "PARTNERS", "HOLDINGS", "ASSOCIATES", "TRUST", "LP", "REALTY")
                )
                else "corp"
            )
            member_names = sorted({p["owner_name"] for p in group if p.get("owner_name")} | oc_holding_entities)
            member_addrs = sorted({p["mailing_address"] for p in group if p.get("mailing_address")} | oc_raw_hq_addrs)
            member_addr_norms = sorted({p["mailing_address_norm"] for p in group if p.get("mailing_address_norm")})
            bbl_list = [p["bbl"] for p in group]
            reg_ids_set: Set[str] = set()
            borough_summary: Dict[str, int] = {}
            total_units = 0
            for p in group:
                reg_ids_set.update(bbl_reg_ids.get(p["bbl"], ()))
                bname = p.get("borough") or state_code
                borough_summary[bname] = borough_summary.get(bname, 0) + 1
                total_units += int(float(p.get("units_res") or 0))

            provenance = [source_tag]
            if oc_company_ids:
                provenance.append(f"{state_code}_STATE_CORPORATE_REGISTRY")
            if reg_ids_set:
                provenance.append(f"{state_code}_RENTAL_LICENSES")

            connection_signals = {
                "member_name_count": len(member_names),
                "member_address_count": len(member_addrs),
                "member_address_norm_count": len(member_addr_norms),
                "has_mailing_address_links": bool(member_addr_norms),
                "oc_matched_companies": sorted(oc_company_ids)[:25],
                "source_human_names": source_humans[:25],
                "primary_human_name": primary_human,
                "primary_entity_name": max(entity_counts, key=entity_counts.get) if entity_counts else None,
                "principal_status": "source_listed_person" if primary_human else "unresolved_entity",
                "provenance": provenance,
                "link_confidence": 0.99 if oc_company_ids else (1.0 if len(member_names) == 1 else 0.88),
                "data_transparency_note": (
                    f"Assembled strictly from official {state_code} statewide property tax parcels "
                    "and verified corporate/licensing records where present. No values inferred or fabricated."
                ),
            }
            batch.append((
                jur,
                network_key,
                anchor_type,
                display_name,
                member_names,
                member_addrs,
                sorted(reg_ids_set),
                bbl_list,
                len(bbl_list),
                total_units,
                json.dumps(borough_summary),
                json.dumps(connection_signals),
                sorted(oc_company_ids) if oc_company_ids else None,
            ))
            n_networks += 1
            if len(batch) >= 25000:
                execute_values(cur, insert_sql, batch, page_size=2500)
                batch.clear()

        if batch:
            execute_values(cur, insert_sql, batch, page_size=2500)
            batch.clear()
        cur.execute(
            "DELETE FROM kv_cache WHERE key IN ('completeness_matrix', %s) OR key LIKE %s",
            (f"city_stats:v1:{jur}", f"city_top_nets:v1:{jur}:%"),
        )
    conn.commit()
    del oc_resolved
    gc.collect()
    _record_status(
        conn,
        jur.upper(),
        "state_dataset",
        {
            "properties_count": n_properties,
            "networks_count": n_networks,
            "municipalities_loaded": n_municipalities,
            "oc_matched_parcels": oc_stats.get("matched", 0),
        },
    )
    return n_properties, n_networks


def fetch_arkansas_parcels() -> List[Dict[str, Any]]:
    """Fetch Arkansas GIS Office Statewide Parcels across all 75 counties."""
    url = "https://gis.arkansas.gov/arcgis/rest/services/FEATURESERVICES/Planning_Cadastre/FeatureServer/6/query"
    fields = (
        "objectid,countyid,parcelid,ownername,adrlabel,adrnum,predir,pstrnam,"
        "pstrtype,psufdir,adrcity,adrzip5,parceltype,assessvalue,impvalue,"
        "landvalue,totalvalue,county"
    )
    where = "ownername IS NOT NULL"

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        owner1 = source_text(a.get("ownername"))
        pid = source_text(a.get("countyid")) or source_text(a.get("parcelid")) or str(a.get("objectid") or "")
        county = (source_text(a.get("county")) or source_text(a.get("adrcity")) or "ARKANSAS").upper()
        if not owner1 or not pid:
            return None
        site = source_text(a.get("adrlabel"))
        if not site:
            parts = [
                p for p in (
                    source_text(a.get("adrnum")),
                    source_text(a.get("predir")),
                    source_text(a.get("pstrnam")),
                    source_text(a.get("pstrtype")),
                    source_text(a.get("psufdir")),
                ) if p and p != "0"
            ]
            site = " ".join(parts) if parts else f"PARCEL {pid}"
        tot = source_float(a.get("totalvalue")) or source_float(a.get("assessvalue"))
        if tot is not None and tot <= 0:
            tot = None
        lat, lng = _extract_feat_coords("ar", feat)
        return _build_parcel_record(
            jur="ar",
            bbl=f"AR-{pid}"[:80],
            address=site,
            borough=county,
            zip_code=_clean_zip5(a.get("adrzip5")),
            owner1=owner1,
            land_use=source_text(a.get("parceltype")),
            assessed_total=tot,
            lat=lat,
            lng=lng,
            source_system="ARKANSAS_GIS_PARCELS",
            col_max_len=40,
        )

    return _sharded_oid_fetch(
        url,
        where,
        fields,
        _map,
        jur_upper="AR",
        oid_field="objectid",
        page_size=2000,
        num_shards=16,
        max_workers=8,
        return_centroid=True,
    )


STATE_PIPELINE_SPEC: Dict[str, Tuple[str, str, Callable[[], List[Dict[str, Any]]]]] = {
    "vt": ("VT", "VT_VCGI_STANDARDIZED_PARCELS", fetch_vermont_parcels),
    "co": ("CO", "CO_OIT_PUBLIC_PARCELS", fetch_colorado_parcels),
    "ny": ("NY", "NYS_ORPTS_PUBLIC_PARCELS", fetch_new_york_state_parcels),
    "fl": ("FL", "FL_GIO_STATEWIDE_PARCELS", fetch_florida_parcels),
    "wi": ("WI", "WI_SCO_DOA_STATEWIDE_PARCELS", fetch_wisconsin_parcels),
    "nc": ("NC", "NC_ONEMAP_STATEWIDE_PARCELS", fetch_north_carolina_parcels),
    "ar": ("AR", "ARKANSAS_GIS_PARCELS", fetch_arkansas_parcels),
}


def ingest_state(conn, jur: str, *, rebuild_only: bool = False) -> Tuple[int, int]:
    state_code, source_tag, fetch_fn = STATE_PIPELINE_SPEC[jur]
    t0 = time.time()
    ensure_jurisdiction_partition(conn, jur)

    if not rebuild_only:
        raw_parcels = fetch_fn()
        deduped: Dict[str, Dict[str, Any]] = {}
        for p in raw_parcels:
            bbl = p["bbl"]
            if bbl not in deduped:
                deduped[bbl] = p
            else:
                prev = deduped[bbl]
                if (p.get("units_res") or 0) > (prev.get("units_res") or 0):
                    prev["units_res"] = p["units_res"]
                    prev["units_total"] = p["units_total"]
                if (p.get("assessed_total") or 0) > (prev.get("assessed_total") or 0):
                    prev["assessed_total"] = p["assessed_total"]
        del raw_parcels

        rows = [
            (
                jur,
                p["bbl"],
                p["address"],
                p["borough"],
                p["zip_code"],
                p["owner_name"],
                p["owner_name_norm"],
                p["owner_co_name"],
                p["owner_is_truncated"],
                p["mailing_address"],
                p["mailing_address_norm"],
                p["mailing_address_coarse"],
                p["mailing_state"],
                p["land_use"],
                p["bld_class"],
                p["num_floors"],
                p["units_res"],
                p["units_total"],
                p["year_built"],
                p["assessed_total"],
                p["last_sale_date"],
                p["last_sale_price"],
                p["latitude"],
                p["longitude"],
                p["source_system"],
            )
            for p in deduped.values()
        ]
        del deduped
        print(f"Writing {len(rows):,} deduplicated `{jur}` parcels into `{jur}_properties`...")
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {jur}_properties;")
            execute_values(
                cur,
                f"""
                INSERT INTO {jur}_properties (
                    jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
                    owner_co_name, owner_is_truncated, mailing_address, mailing_address_norm,
                    mailing_address_coarse, mailing_state, land_use, bld_class, num_floors,
                    units_res, units_total, year_built, assessed_total, last_sale_date,
                    last_sale_price, latitude, longitude, source_system
                )
                VALUES %s
                ON CONFLICT (jurisdiction, bbl) DO NOTHING;
                """,
                rows,
                page_size=3000,
            )
        conn.commit()
        del rows

    # Run state-specific licensing / enforcement overlays before network clustering
    if jur == "fl":
        enrich_fl_dbpr_apartments(conn, target_jurisdictions=("fl", "miami"))
        # Ensure FL_SUNBIZ_CORPORATIONS (`us_fl`) + RA hub quarantine have committed if running concurrently
        wait_deadline = time.time() + 1200
        while time.time() < wait_deadline:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM data_source_status WHERE source_name = 'FL_SUNBIZ_CORPORATIONS' AND refresh_status = 'success'"
                )
                if cur.fetchone():
                    break
            conn.commit()
            print("  [FL] Waiting 10s for concurrent FL_SUNBIZ_CORPORATIONS ingestion to commit...")
            time.sleep(10)
    elif jur == "co":
        enrich_denver_rental_licenses(conn)
    elif jur == "ny":
        enrich_ny_buffalo_enforcement_and_registry(conn)

    p_cnt, n_cnt = build_and_save_state_networks(conn, jur, state_code, source_tag)
    print(f"✓ [{jur.upper()}] Complete in {time.time() - t0:.1f}s: {p_cnt:,} parcels -> {n_cnt:,} landlord networks.")
    return p_cnt, n_cnt


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest statewide property parcels, licensing, OC links & networks.")
    parser.add_argument(
        "--states",
        nargs="+",
        default=["vt", "co", "ny", "fl", "wi", "nc", "ar"],
        choices=["vt", "co", "ny", "fl", "wi", "nc", "ar"],
        help="Statewide jurisdictions to ingest.",
    )
    parser.add_argument("--rebuild-only", action="store_true", help="Rebuild OC links & networks from existing properties.")
    parser.add_argument("--upgrade-cities", nargs="*", default=[], help="Legacy cities to rebuild with OC corporate piercing (e.g. philadelphia miami).")
    args = parser.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    try:
        for st in args.states:
            ingest_state(conn, st, rebuild_only=args.rebuild_only)
        for city in args.upgrade_cities:
            rebuild_city_networks_from_db(conn, city)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
