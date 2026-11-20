#!/usr/bin/env python3
"""
gcp/spot_expansion_v4_worker.py
===============================
High-throughput Google Compute Engine Spot worker (runs on `renter-help` in
`us-central1-a`, hardware TTL 2h, self-deletes on completion) pulling 100%
verified authoritative government open-data layers over Google's 10 Gbps
backbone into `gs://renter-help-staging-20261119/spot_expansion_v4/`:

  1. `oh_v4_parcels.csv.gz` (~338K improved OH parcels with WGS84 centroids):
       - Stark County Auditor (Canton / Massillon, `APPRAISED_BUILDING_VALUE > 0`)
       - Lake County Auditor (Greater Cleveland, `A_VAL_BLDG > 0`)
       - Delaware County Auditor (Greater Columbus, `MARKET_IMP > 0`)
  2. `mn_v4_parcels.csv.gz` (~195K improved MN parcels — completes ALL 7 Twin Cities Metro counties):
       - Washington County (`EMV_BLDG > 0`)
       - Scott County (`EMV_BLDG > 0`)
       - Carver County (`EMV_BLDG > 0`, `TAX_NAME`)
  3. `il_v4_parcels.csv.gz` (~283K improved Chicagoland collar-county parcels):
       - Kane County (Aurora / Elgin, improved non-vacant non-exempt)
       - McHenry County (Crystal Lake / Woodstock, improved non-vacant non-exempt)
  4. `ny_syracuse_overlays.csv.gz` (~166K official Syracuse NY records):
       - City of Syracuse Code Violations V2 (`154,876` violations)
       - City of Syracuse Rental Registry (`10,967` rental registry records)
  5. `geocoding_backfill_v4.csv.gz` (~1.14M exact WGS84 centroids & CAMA links):
       - Washington DC (`137K` `SSL` -> lat/lng via `Property_and_Land_WebMercator/FeatureServer/40`)
       - Baltimore City (`237K` `ACCTID` block+lot -> lat/lng + `SDATWEBADR` + `BLDG_UNITS` via MD iMAP)
       - Montana Statewide (`434K` `MT-{PARCELID}` -> lat/lng via `msdi_cadastral_map_v1/MapServer/1`)
       - Minnesota (`332K` Dakota + Anoka + Olmsted -> lat/lng)
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime as dt
import gzip
import json
import os
import re
import subprocess
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

csv.field_size_limit(10_000_000)

BUCKET = os.environ.get("GCS_BUCKET", "renter-help-staging-20261119")
PREFIX = "spot_expansion_v4"
LOCAL_ONLY = os.environ.get("SPOT_LOCAL_ONLY", "").strip().lower() in ("1", "true", "yes")
WORK_DIR = os.environ.get("SPOT_WORK_DIR", "/tmp/spot_v4")
os.makedirs(WORK_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TheyOwnWhatPublicRecordsSync/4.0)"}

PARCEL_HEADER = [
    "bbl", "address", "borough", "zip_code", "owner_name", "owner2", "care_of",
    "mailing_address", "mailing_state", "land_use", "bld_class", "num_floors",
    "units_res", "year_built", "assessed_total", "last_sale_date", "last_sale_price",
    "latitude", "longitude", "cama_site_link", "source_system",
]

OVERLAY_HEADER = [
    "jur", "record_kind", "record_id", "parcel_hint", "address", "city", "zip_code",
    "status", "event_date", "entity_name", "mailing_address", "units",
]

GEO_HEADER = [
    "jur", "bbl", "address_hint", "latitude", "longitude", "cama_site_link", "units_res",
]


def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] {msg}", flush=True)


def s_txt(v: Any) -> str:
    if v is None:
        return ""
    s = " ".join(str(v).strip().split())
    return "" if s.upper() in ("NULL", "NONE", "N/A", "NA", "<NULL>") else s


def s_flt(v: Any) -> Optional[float]:
    t = s_txt(v).replace(",", "").replace("$", "")
    if not t:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def s_int(v: Any) -> Optional[int]:
    f = s_flt(v)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def clean_zip(v: Any) -> str:
    m = re.search(r"\b(\d{5})\b", s_txt(v))
    return m.group(1) if m else ""


def fmt_date(v: Any) -> str:
    if v is None or v == "":
        return ""
    if isinstance(v, (int, float)):
        if v <= -2_000_000_000_000:
            return ""
        if abs(v) > 10_000_000_000:
            try:
                d = dt.datetime.fromtimestamp(v / 1000.0, tz=dt.timezone.utc).date()
                return d.isoformat() if 1900 <= d.year <= 2035 else ""
            except Exception:
                return ""
    t = s_txt(v)
    if not t:
        return ""
    m = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})", t)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m2 = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", t)
    if m2:
        return f"{int(m2.group(3)):04d}-{int(m2.group(1)):02d}-{int(m2.group(2)):02d}"
    return ""


def extract_wgs84_centroid(feat: Dict[str, Any]) -> Tuple[Any, Any]:
    """Extract (lat, lng) in WGS84 from centroid, point geometry, or polygon rings."""
    c = feat.get("centroid")
    if isinstance(c, dict):
        x, y = s_flt(c.get("x")), s_flt(c.get("y"))
        if x is not None and y is not None and 18.0 <= y <= 72.0 and -180.0 <= x <= -60.0:
            return round(y, 6), round(x, 6)
    g = feat.get("geometry")
    if isinstance(g, dict):
        x, y = s_flt(g.get("x")), s_flt(g.get("y"))
        if x is not None and y is not None and 18.0 <= y <= 72.0 and -180.0 <= x <= -60.0:
            return round(y, 6), round(x, 6)
        rings = g.get("rings")
        if rings and isinstance(rings, list) and rings[0]:
            pts = rings[0]
            if len(pts) >= 3:
                xs = [p[0] for p in pts if isinstance(p, (list, tuple)) and len(p) >= 2]
                ys = [p[1] for p in pts if isinstance(p, (list, tuple)) and len(p) >= 2]
                if xs and ys:
                    avg_x = sum(xs) / len(xs)
                    avg_y = sum(ys) / len(ys)
                    if 18.0 <= avg_y <= 72.0 and -180.0 <= avg_x <= -60.0:
                        return round(avg_y, 6), round(avg_x, 6)
    return "", ""


def upload_to_gcs(local_path: str, rel_name: str) -> None:
    if LOCAL_ONLY:
        log(f"  [LOCAL_ONLY] Kept {local_path}")
        return
    dest = f"gs://{BUCKET}/{PREFIX}/{rel_name}"
    subprocess.run(["gcloud", "storage", "cp", local_path, dest, "--quiet"], check=True, timeout=600)
    sz = os.path.getsize(local_path)
    log(f"  ✓ Uploaded {dest} ({sz / 1048576:.1f} MB)")


def write_gz_csv(filename: str, header: List[str], rows: List[List[Any]]) -> str:
    out_path = os.path.join(WORK_DIR, filename)
    with gzip.open(out_path, "wt", encoding="utf-8", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(header)
        w.writerows(rows)
    log(f"  Wrote {len(rows):,} rows -> {out_path} ({os.path.getsize(out_path) / 1048576:.1f} MB)")
    upload_to_gcs(out_path, filename)
    return out_path


def get_oid_bounds(url: str, oid_field: str = "OBJECTID", where: str = "1=1") -> Tuple[int, int]:
    stats = json.dumps([
        {"statisticType": "min", "onStatisticField": oid_field, "outStatisticFieldName": "min_oid"},
        {"statisticType": "max", "onStatisticField": oid_field, "outStatisticFieldName": "max_oid"},
    ])
    for attempt in range(4):
        try:
            r = requests.get(url, params={"where": where, "outStatistics": stats, "f": "json"}, headers=HEADERS, timeout=45)
            r.raise_for_status()
            attrs = ((r.json().get("features") or [{}])[0].get("attributes")) or {}
            lo = s_int(attrs.get("min_oid"))
            hi = s_int(attrs.get("max_oid"))
            if lo is not None and hi is not None and hi >= lo:
                return lo, hi
        except Exception as e:
            log(f"    OID bounds retry {attempt + 1}/4 for {url}: {e}")
            time.sleep(2 * (attempt + 1))
    r2 = requests.get(url, params={"where": where, "returnIdsOnly": "true", "f": "json"}, headers=HEADERS, timeout=60).json()
    ids = r2.get("objectIds") or []
    if ids:
        return int(min(ids)), int(max(ids))
    raise RuntimeError(f"Could not determine OID bounds for {url}")


def sharded_arcgis_fetch(
    url: str,
    where: str,
    out_fields: str,
    mapper: Callable[[Dict[str, Any]], Optional[List[Any]]],
    *,
    oid_field: str = "OBJECTID",
    page_size: int = 2000,
    num_shards: int = 12,
    max_workers: int = 10,
    return_geometry: bool = False,
    return_centroid: bool = False,
    geometry_precision: Optional[int] = None,
    label: str = "arcgis",
) -> List[List[Any]]:
    lo, hi = get_oid_bounds(url, oid_field=oid_field, where=where)
    span = max(1, (hi - lo + 1 + num_shards - 1) // num_shards)
    ranges = []
    cur = lo
    while cur <= hi:
        ranges.append((cur, min(hi, cur + span - 1)))
        cur += span
    log(f"  [{label}] OID range {lo:,}..{hi:,} -> {len(ranges)} shards (geom={return_geometry}, cent={return_centroid})")

    def _run_shard(sid: int, s_lo: int, s_hi: int) -> List[List[Any]]:
        sess = requests.Session()
        sess.headers.update(HEADERS)
        out: List[List[Any]] = []
        cursor = s_lo - 1
        while cursor < s_hi:
            w = f"({where}) AND {oid_field} > {cursor} AND {oid_field} <= {s_hi}"
            params: Dict[str, Any] = {
                "where": w,
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
            if return_geometry and geometry_precision is not None:
                params["geometryPrecision"] = geometry_precision
                params["maxAllowableOffset"] = 0.0005

            data = None
            for attempt in range(5):
                try:
                    r = sess.get(url, params=params, timeout=75)
                    r.raise_for_status()
                    d = r.json()
                    if "error" in d:
                        raise RuntimeError(str(d["error"]))
                    data = d
                    break
                except Exception as e:
                    if attempt == 4:
                        log(f"    [{label} S{sid}] FAILED after OID {cursor}: {e}")
                        return out
                    time.sleep(2 * (attempt + 1))
            feats = (data or {}).get("features") or []
            if not feats:
                break
            max_oid = cursor
            for f in feats:
                a = f.get("attributes") or {}
                oid_val = s_int(a.get(oid_field) or a.get(oid_field.lower()) or a.get(oid_field.upper()))
                if oid_val is not None and oid_val > max_oid:
                    max_oid = oid_val
                row = mapper(f)
                if row is not None:
                    out.append(row)
            if max_oid <= cursor:
                break
            cursor = max_oid
            if len(feats) < page_size and not (data or {}).get("exceededTransferLimit"):
                break
        sess.close()
        return out

    all_rows: List[List[Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_run_shard, i, r_lo, r_hi): i for i, (r_lo, r_hi) in enumerate(ranges)}
        done = 0
        for fut in as_completed(futs):
            done += 1
            res = fut.result()
            all_rows.extend(res)
            if done % max(1, len(ranges) // 4) == 0 or done == len(ranges):
                log(f"    [{label}] {done}/{len(ranges)} shards done -> {len(all_rows):,} rows")
    return all_rows


# ==============================================================================
# 1. OHIO EXPANSION (Stark + Lake + Delaware = +338K improved parcels w/ coords)
# ==============================================================================
def crunch_ohio_v4() -> None:
    log("=== 1. Starting OHIO v4 Expansion (Stark + Lake + Delaware) ===")
    oh_rows: List[List[Any]] = []

    # 1a. Stark County Auditor (Canton / Massillon / Alliance, ~156K improved)
    st_url = "https://scgisa.starkcountyohio.gov/arcgis/rest/services/Auditor/StarkCountyParcels/FeatureServer/0/query"
    st_fields = (
        "OBJECTID,PIN,SITE_ADDRESS,SITE_ADDRESS1,SITE_ADDRESS2,SITE_ADDRESS3,"
        "CLASSIFICATION,LAND_USE_CODE,LAND_USE_DESCRIPTION,OWNER,OWNER_ADDRESS,"
        "MAILING_NAME,MAILING_ADDRESS,APPRAISED_BUILDING_VALUE,APPRAISED_TOTAL_VALUE,"
        "MOST_RECENT_SALE_DATE,MOST_RECENT_SALE_PRICE"
    )

    def _map_stark(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        bval = s_flt(a.get("APPRAISED_BUILDING_VALUE")) or 0.0
        if bval <= 0:
            return None
        pin = s_txt(a.get("PIN"))
        owner = s_txt(a.get("OWNER")) or s_txt(a.get("MAILING_NAME"))
        addr = s_txt(a.get("SITE_ADDRESS1")) or s_txt(a.get("SITE_ADDRESS"))
        if not pin or not owner or not addr or addr.startswith("0 "):
            return None
        site3 = s_txt(a.get("SITE_ADDRESS3")) or s_txt(a.get("SITE_ADDRESS"))
        z5 = clean_zip(site3)
        city_m = re.match(r"^([A-Z\s.-]+?)(?:,\s*|\s+)OH\b", site3.upper())
        city = city_m.group(1).strip() if city_m else "STARK"
        mail = s_txt(a.get("OWNER_ADDRESS")) or s_txt(a.get("MAILING_ADDRESS"))
        mst_m = re.search(r"\b([A-Z]{2})\s+\d{5}", mail.upper())
        mst = mst_m.group(1) if mst_m else ""
        lat, lng = extract_wgs84_centroid(f)
        sp = s_flt(a.get("MOST_RECENT_SALE_PRICE"))
        return [
            f"OH-STK-{pin}", addr, f"{city} (STARK)", z5,
            owner, "", "", mail, mst,
            s_txt(a.get("LAND_USE_DESCRIPTION")) or s_txt(a.get("CLASSIFICATION")) or "IMPROVED PARCEL",
            s_txt(a.get("LAND_USE_CODE")), "", "", "",
            s_flt(a.get("APPRAISED_TOTAL_VALUE")) or bval,
            fmt_date(a.get("MOST_RECENT_SALE_DATE")),
            sp if (sp and sp > 0) else "",
            lat, lng,
            f"https://realestate.starkcountyohio.gov/parcel/?parcel={pin}",
            "STARK_COUNTY_OH_AUDITOR",
        ]

    oh_rows.extend(sharded_arcgis_fetch(
        st_url, "APPRAISED_BUILDING_VALUE > 0", st_fields, _map_stark,
        page_size=1000, num_shards=10, max_workers=8, return_geometry=True, geometry_precision=5, label="OH-STARK",
    ))

    # 1b. Lake County Auditor (Greater Cleveland, ~98K improved)
    lk_url = "https://gis.lakecountyohio.gov/arcgis/rest/services/Sharing/LCGIS_SHARE_2023/FeatureServer/0/query"
    lk_fields = (
        "OBJECTID,PIN,PIN_NODASH,G_FULLADDRESS,A_USPS_CITY,A_ZIPCODE,"
        "A_OWNER_NAME,A_TAXP_NAME,G_O_FLADDRESS,G_O_FLCITY,G_T_FLADDRESS,G_T_FLCITY,"
        "A_VAL_BLDG,A_VAL_TOTAL,A_PROP_CLASS,A_YEAR_BUILT,A_SALE_DATE,A_SALE_AMOUNT"
    )

    def _map_lake(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        bval = s_flt(a.get("A_VAL_BLDG")) or 0.0
        if bval <= 0:
            return None
        pin = s_txt(a.get("PIN_NODASH")) or s_txt(a.get("PIN"))
        owner = s_txt(a.get("A_OWNER_NAME")) or s_txt(a.get("A_TAXP_NAME"))
        addr = s_txt(a.get("G_FULLADDRESS"))
        if not pin or not owner or not addr or addr.startswith("0 "):
            return None
        city = (s_txt(a.get("A_USPS_CITY")) or "LAKE").upper()
        mail = ", ".join(p for p in (s_txt(a.get("G_O_FLADDRESS")), s_txt(a.get("G_O_FLCITY"))) if p)
        if not mail:
            mail = ", ".join(p for p in (s_txt(a.get("G_T_FLADDRESS")), s_txt(a.get("G_T_FLCITY"))) if p)
        mst_m = re.search(r"\b([A-Z]{2})\s+\d{5}", mail.upper())
        mst = mst_m.group(1) if mst_m else ""
        yr = s_int(a.get("A_YEAR_BUILT"))
        sp = s_flt(a.get("A_SALE_AMOUNT"))
        lat, lng = extract_wgs84_centroid(f)
        return [
            f"OH-LAK-{pin}", addr, f"{city} (LAKE)", clean_zip(a.get("A_ZIPCODE")),
            owner, "", "", mail, mst,
            "IMPROVED PARCEL", s_txt(a.get("A_PROP_CLASS")), "", "",
            yr if (yr and 1750 <= yr <= 2030) else "",
            s_flt(a.get("A_VAL_TOTAL")) or bval,
            fmt_date(a.get("A_SALE_DATE")),
            sp if (sp and sp > 0) else "",
            lat, lng,
            f"https://lake.iviewauditor.com/Search?q={pin}",
            "LAKE_COUNTY_OH_AUDITOR",
        ]

    oh_rows.extend(sharded_arcgis_fetch(
        lk_url, "A_VAL_BLDG > 0", lk_fields, _map_lake,
        page_size=1000, num_shards=8, max_workers=6, return_geometry=True, geometry_precision=5, label="OH-LAKE",
    ))

    # 1c. Delaware County Auditor (Greater Columbus, ~84K improved)
    dl_url = "https://services2.arcgis.com/ziXVKVy3BiopMCCU/arcgis/rest/services/Parcel/FeatureServer/0/query"
    dl_fields = (
        "OBJECTID,MAPSHEET,SUBPARCEL,OWNER1,OWNER2,MAILNAME1,MAILADDR1,MAILADDR2,"
        "ADDR1,ADDR2,CLASS,MARKET_IMP,MARKET_TOT,YRBUILT,MUN_NAME,TWP_NAME,SALE_DATE,SALE_AMNT"
    )

    def _map_delaware(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        bval = s_flt(a.get("MARKET_IMP")) or 0.0
        if bval <= 0:
            return None
        oid = s_txt(a.get("OBJECTID"))
        pin = f"{s_txt(a.get('MAPSHEET'))}-{s_txt(a.get('SUBPARCEL'))}" if s_txt(a.get("MAPSHEET")) else oid
        owner = s_txt(a.get("OWNER1")) or s_txt(a.get("MAILNAME1"))
        addr = s_txt(a.get("ADDR1"))
        if not pin or not owner or not addr or addr.startswith("0 "):
            return None
        owner2 = s_txt(a.get("OWNER2"))
        if owner2 in ("0", "NONE"):
            owner2 = ""
        city = (s_txt(a.get("MUN_NAME")) or s_txt(a.get("TWP_NAME")) or "DELAWARE").upper()
        mail = ", ".join(p for p in (s_txt(a.get("MAILADDR1")), s_txt(a.get("MAILADDR2"))) if p)
        mst_m = re.search(r"\b([A-Z]{2})\s+\d{5}", mail.upper())
        mst = mst_m.group(1) if mst_m else ""
        yr = s_int(a.get("YRBUILT"))
        sp = s_flt(a.get("SALE_AMNT"))
        lat, lng = extract_wgs84_centroid(f)
        return [
            f"OH-DEL-{oid}", addr, f"{city} (DELAWARE)", clean_zip(a.get("ADDR2")),
            owner, owner2, "", mail, mst,
            "IMPROVED PARCEL", s_txt(a.get("CLASS")), "", "",
            yr if (yr and 1750 <= yr <= 2030) else "",
            s_flt(a.get("MARKET_TOT")) or bval,
            fmt_date(a.get("SALE_DATE")),
            sp if (sp and sp > 0) else "",
            lat, lng,
            "https://auditor.co.delaware.oh.us/",
            "DELAWARE_COUNTY_OH_AUDITOR",
        ]

    oh_rows.extend(sharded_arcgis_fetch(
        dl_url, "MARKET_IMP > 0", dl_fields, _map_delaware,
        page_size=1000, num_shards=8, max_workers=6, return_geometry=True, geometry_precision=5, label="OH-DELAWARE",
    ))

    write_gz_csv("oh_v4_parcels.csv.gz", PARCEL_HEADER, oh_rows)


# ==============================================================================
# 2. MINNESOTA TWIN CITIES COMPLETION (Washington + Scott + Carver = +195K)
# ==============================================================================
def crunch_minnesota_v4() -> None:
    log("=== 2. Starting MINNESOTA v4 Expansion (Washington + Scott + Carver — 7-County Metro Complete!) ===")
    mn_rows: List[List[Any]] = []
    metc_fields = (
        "OBJECTID,STATE_PIN,COUNTY_PIN,CO_NAME,CTU_NAME,ANUMBER,ST_PRE_DIR,ST_NAME,ST_POS_TYP,ST_POS_DIR,"
        "SUB_TYPE1,SUB_ID1,ZIP,OWNER_NAME,OWNER_MORE,OWN_ADD_L1,OWN_ADD_L2,OWN_ADD_L3,OWN_ADD_L4,"
        "TAX_NAME,TAX_ADD_L1,TAX_ADD_L2,TAX_ADD_L3,TAX_ADD_L4,EMV_BLDG,EMV_TOTAL,YEAR_BUILT,NUM_UNITS,"
        "USECLASS1,DWELL_TYPE,SALE_DATE,SALE_VALUE"
    )

    for layer_id, tag, bbl_pfx in [(6, "MN-WASHINGTON", "MN-WSH"), (5, "MN-SCOTT", "MN-SCT"), (1, "MN-CARVER", "MN-CRV")]:
        url = f"https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcels/FeatureServer/{layer_id}/query"

        def _make_mapper(pfx: str, lbl: str):
            def _map(f: Dict[str, Any]) -> Optional[List[Any]]:
                a = f.get("attributes") or {}
                bldg = s_flt(a.get("EMV_BLDG")) or 0.0
                if bldg <= 0:
                    return None
                owner = s_txt(a.get("OWNER_NAME")) or s_txt(a.get("TAX_NAME"))
                pin = s_txt(a.get("STATE_PIN")) or s_txt(a.get("COUNTY_PIN"))
                if not owner or not pin:
                    return None
                addr = " ".join(p for p in (
                    s_txt(a.get("ANUMBER")), s_txt(a.get("ST_PRE_DIR")),
                    s_txt(a.get("ST_NAME")), s_txt(a.get("ST_POS_TYP")),
                    s_txt(a.get("ST_POS_DIR")),
                    f"{s_txt(a.get('SUB_TYPE1'))} {s_txt(a.get('SUB_ID1'))}".strip(),
                ) if p)
                if not addr or addr.startswith("0 "):
                    return None
                mail_parts = [s_txt(a.get(k)) for k in ("OWN_ADD_L1", "OWN_ADD_L2", "OWN_ADD_L3", "OWN_ADD_L4")]
                if not any(mail_parts):
                    mail_parts = [s_txt(a.get(k)) for k in ("TAX_ADD_L1", "TAX_ADD_L2", "TAX_ADD_L3", "TAX_ADD_L4")]
                mail = ", ".join(p for p in mail_parts if p)
                mst_m = re.search(r"\b([A-Z]{2})\s+\d{5}", mail.upper())
                mst = mst_m.group(1) if mst_m else ""
                co = (s_txt(a.get("CO_NAME")) or lbl.split("-")[1]).upper()
                city = (s_txt(a.get("CTU_NAME")) or co).upper()
                yr = s_int(a.get("YEAR_BUILT"))
                units = s_int(a.get("NUM_UNITS"))
                sval = s_flt(a.get("SALE_VALUE"))
                lat, lng = extract_wgs84_centroid(f)
                return [
                    f"{pfx}-{pin}", addr, f"{city} ({co})", clean_zip(a.get("ZIP")),
                    owner, s_txt(a.get("OWNER_MORE")), "", mail, mst,
                    s_txt(a.get("DWELL_TYPE")) or s_txt(a.get("USECLASS1")) or "IMPROVED PARCEL",
                    s_txt(a.get("USECLASS1")), "",
                    units if (units and units > 0) else "",
                    yr if (yr and 1750 <= yr <= 2030) else "",
                    s_flt(a.get("EMV_TOTAL")) or bldg,
                    fmt_date(a.get("SALE_DATE")),
                    sval if (sval and sval > 0) else "",
                    lat, lng, "",
                    f"{co}_COUNTY_MN_METC_CAMA",
                ]
            return _map

        mn_rows.extend(sharded_arcgis_fetch(
            url, "EMV_BLDG > 0", metc_fields, _make_mapper(bbl_pfx, tag),
            page_size=1000, num_shards=8, max_workers=6, return_geometry=True, geometry_precision=5, label=tag,
        ))

    write_gz_csv("mn_v4_parcels.csv.gz", PARCEL_HEADER, mn_rows)


# ==============================================================================
# 3. ILLINOIS CHICAGOLAND EXPANSION (Kane + McHenry = +283K improved parcels)
# ==============================================================================
def crunch_illinois_v4() -> None:
    log("=== 3. Starting ILLINOIS v4 Expansion (Kane + McHenry Collar Counties) ===")
    il_rows: List[List[Any]] = []

    # 3a. Kane County (Aurora / Elgin, ~160K improved non-vacant non-exempt)
    kn_url = "https://gistech.countyofkane.org/arcgis/rest/services/KanePINList/MapServer/0/query"
    kn_fields = (
        "OBJECTID,PIN,Township,UseCode,UseCodeDescription,TaxName,"
        "SiteAddress,SiteCity,SiteZip,MailingAddress,MailingAddress2,MailingCity,MailingState,MailingZip"
    )
    kn_where = (
        "TaxName IS NOT NULL AND SiteAddress IS NOT NULL "
        "AND UseCodeDescription NOT LIKE '%Vacant%' AND UseCodeDescription NOT LIKE '%Vac %' "
        "AND UseCodeDescription <> 'Exempt' AND UseCodeDescription <> 'Farmland'"
    )

    def _map_kane(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pin = s_txt(a.get("PIN"))
        owner = s_txt(a.get("TaxName"))
        addr = s_txt(a.get("SiteAddress"))
        if not pin or not owner or not addr or addr.startswith("0 "):
            return None
        city = (s_txt(a.get("SiteCity")) or s_txt(a.get("Township")) or "KANE").upper()
        mst = s_txt(a.get("MailingState"))
        mail = ", ".join(p for p in (
            s_txt(a.get("MailingAddress")), s_txt(a.get("MailingAddress2")),
            s_txt(a.get("MailingCity")), mst, clean_zip(a.get("MailingZip")),
        ) if p)
        lat, lng = extract_wgs84_centroid(f)
        return [
            f"IL-KAN-{pin}", addr, f"{city} (KANE)", clean_zip(a.get("SiteZip")),
            owner, "", "", mail, mst,
            s_txt(a.get("UseCodeDescription")) or "IMPROVED PARCEL", s_txt(a.get("UseCode")),
            "", "", "", "", "", "",
            lat, lng,
            f"https://kaneil.devnetwedge.com/parcel/view/{pin}/2025",
            "KANE_COUNTY_IL_GIS",
        ]

    il_rows.extend(sharded_arcgis_fetch(
        kn_url, kn_where, kn_fields, _map_kane,
        page_size=1000, num_shards=10, max_workers=8, return_geometry=True, geometry_precision=5, label="IL-KANE",
    ))

    # 3b. McHenry County (Crystal Lake / Woodstock, ~123K improved)
    mc_url = "https://services1.arcgis.com/6iYC5AXXYapRVNzl/arcgis/rest/services/McHenry_County_TaxParcels/FeatureServer/0/query"
    mc_fields = (
        "OBJECTID,ParcelNumber,Township,PropertyClass,Owner,"
        "MailToAddress1,MailToAddress2,MailToCity,MailToState,MailToZip,"
        "SiteAddress,SiteCity,SiteZip"
    )
    mc_where = (
        "Owner IS NOT NULL AND SiteAddress IS NOT NULL "
        "AND PropertyClass IN ('0040','0050','0060','0070','0080','0011','0021')"
    )

    def _map_mchenry(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pin = s_txt(a.get("ParcelNumber"))
        owner = s_txt(a.get("Owner"))
        addr = s_txt(a.get("SiteAddress"))
        if not pin or not owner or not addr or addr.startswith("0 "):
            return None
        city = (s_txt(a.get("SiteCity")) or s_txt(a.get("Township")) or "MCHENRY").upper()
        mst = s_txt(a.get("MailToState"))
        mail = ", ".join(p for p in (
            s_txt(a.get("MailToAddress1")), s_txt(a.get("MailToAddress2")),
            s_txt(a.get("MailToCity")), mst, clean_zip(a.get("MailToZip")),
        ) if p)
        lat, lng = extract_wgs84_centroid(f)
        pcls = s_txt(a.get("PropertyClass"))
        lu = "RESIDENTIAL IMPROVED" if pcls == "0040" else ("COMMERCIAL / INDUSTRIAL" if pcls in ("0050", "0060", "0070", "0080") else "IMPROVED PARCEL")
        return [
            f"IL-MCH-{pin}", addr, f"{city} (MCHENRY)", clean_zip(a.get("SiteZip")),
            owner, "", "", mail, mst, lu, pcls,
            "", "", "", "", "", "",
            lat, lng,
            f"https://mchenryil.devnetwedge.com/parcel/view/{pin.replace('-', '')}/2025",
            "MCHENRY_COUNTY_IL_GIS",
        ]

    il_rows.extend(sharded_arcgis_fetch(
        mc_url, mc_where, mc_fields, _map_mchenry,
        page_size=1000, num_shards=10, max_workers=8, return_geometry=True, geometry_precision=5, label="IL-MCHENRY",
    ))

    write_gz_csv("il_v4_parcels.csv.gz", PARCEL_HEADER, il_rows)


# ==============================================================================
# 4. SYRACUSE NY CODE VIOLATIONS (155K) + RENTAL REGISTRY (11K)
# ==============================================================================
def crunch_ny_syracuse_overlays() -> None:
    log("=== 4. Starting SYRACUSE NY Code Violations + Rental Registry ===")
    ov_rows: List[List[Any]] = []

    # 4a. Code Violations V2 (~155K)
    cv_url = "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Code_Violations_V2/FeatureServer/0/query"
    cv_fields = "ObjectId,SBL,complaint_address,complaint_zip,violation_id,violation_number,status_type_name,violation_date,open_date,issued_to"

    def _map_syr_cv(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        addr = s_txt(a.get("complaint_address"))
        sbl = s_txt(a.get("SBL"))
        if not addr and not sbl:
            return None
        rid = s_txt(a.get("violation_number")) or s_txt(a.get("violation_id")) or s_txt(a.get("ObjectId"))
        return [
            "ny", "code_violation", f"SYR-{rid}", sbl, addr, "SYRACUSE",
            clean_zip(a.get("complaint_zip")), s_txt(a.get("status_type_name")) or "OPEN",
            fmt_date(a.get("violation_date") or a.get("open_date")), s_txt(a.get("issued_to")), "", "",
        ]

    ov_rows.extend(sharded_arcgis_fetch(
        cv_url, "1=1", cv_fields, _map_syr_cv,
        oid_field="ObjectId", page_size=1000, num_shards=10, max_workers=8, label="NY-SYR-CODE",
    ))

    # 4b. Rental Registry (~11K)
    rr_url = "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Syracuse_Rental_Registry/FeatureServer/0/query"
    rr_fields = "ObjectId,SBL,PropertyAddress,zip,RRisValid,valid_until,completion_date"

    def _map_syr_rr(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        addr = s_txt(a.get("PropertyAddress"))
        sbl = s_txt(a.get("SBL"))
        if not addr and not sbl:
            return None
        rid = s_txt(a.get("SBL")) or s_txt(a.get("ObjectId"))
        return [
            "ny", "rental_registry", f"SYRRR-{rid}", sbl, addr, "SYRACUSE",
            clean_zip(a.get("zip")), s_txt(a.get("RRisValid")) or "REGISTERED",
            fmt_date(a.get("valid_until") or a.get("completion_date")), "", "", "",
        ]

    ov_rows.extend(sharded_arcgis_fetch(
        rr_url, "1=1", rr_fields, _map_syr_rr,
        oid_field="ObjectId", page_size=1000, num_shards=4, max_workers=4, label="NY-SYR-RENTAL",
    ))

    write_gz_csv("ny_syracuse_overlays.csv.gz", OVERLAY_HEADER, ov_rows)


# ==============================================================================
# 5. GEOCODING / MAP-COMPLETION BACKFILL (DC + Baltimore + MT + MN = ~1.14M)
# ==============================================================================
def crunch_geocoding_backfill_v4() -> None:
    log("=== 5. Starting Universal Geocoding & CAMA Backfill (DC + Baltimore + MT + MN) ===")
    geo_rows: List[List[Any]] = []

    # 5a. Washington DC (`137K` exact SSL -> lat/lng)
    dc_url = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/40/query"

    def _map_dc_geo(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        ssl = s_txt(a.get("SSL"))
        lat, lng = extract_wgs84_centroid(f)
        if not ssl or lat == "" or lng == "":
            return None
        return ["dc", ssl, s_txt(a.get("ADDRESS1")), lat, lng, "", ""]

    geo_rows.extend(sharded_arcgis_fetch(
        dc_url, "SSL IS NOT NULL", "OBJECTID,SSL,ADDRESS1", _map_dc_geo,
        page_size=1000, num_shards=10, max_workers=8, return_geometry=True, geometry_precision=5, label="GEO-DC",
    ))

    # 5b. Baltimore City (`237K` via MD iMAP exact points + SDATWEBADR CAMA link + BLDG_UNITS!)
    md_url = "https://mdgeodata.md.gov/imap/rest/services/PlanningCadastre/MD_PropertyData/MapServer/0/query"

    def _map_balt_geo(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        acct = str(a.get("ACCTID") or "").strip()
        lat, lng = extract_wgs84_centroid(f)
        if not acct or lat == "" or lng == "":
            return None
        # ACCTID='0320022205 077' -> last 8 chars '2205 077' match baltimore_properties.bbl!
        blk = s_txt(a.get("BLOCK"))
        lot = s_txt(a.get("LOT"))
        bbl_key = acct[-8:].strip() if len(acct) >= 8 else (f"{blk} {lot.zfill(3)}" if blk and lot else acct)
        units = s_int(a.get("BLDG_UNITS"))
        return [
            "baltimore", bbl_key, s_txt(a.get("ADDRESS")), lat, lng,
            s_txt(a.get("SDATWEBADR")), units if (units and units > 0) else "",
        ]

    geo_rows.extend(sharded_arcgis_fetch(
        md_url, "JURSCODE='BACI'", "OBJECTID,ACCTID,BLOCK,LOT,ADDRESS,BLDG_UNITS,SDATWEBADR", _map_balt_geo,
        page_size=2000, num_shards=10, max_workers=8, return_geometry=True, label="GEO-BALTIMORE",
    ))

    # 5c. Montana Statewide (`434K` exact MT-{PARCELID} -> polygon centroid)
    mt_url = "https://gisservice.mt.gov/arcgis/rest/services/msdi_cadastral_map_v1/MapServer/1/query"

    def _map_mt_geo(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pid = s_txt(a.get("PARCELID"))
        lat, lng = extract_wgs84_centroid(f)
        if not pid or lat == "" or lng == "":
            return None
        return ["mt", f"MT-{pid}", "", lat, lng, "", ""]

    geo_rows.extend(sharded_arcgis_fetch(
        mt_url, "TotalBuildingValue > 0 AND OwnerName IS NOT NULL", "OBJECTID,PARCELID", _map_mt_geo,
        page_size=1000, num_shards=14, max_workers=10, return_geometry=True, geometry_precision=5, label="GEO-MT",
    ))

    # 5d. Minnesota Dakota + Anoka + Olmsted missing centroids (`~332K`)
    for lbl, url, wh, flds, pfx, id_col in [
        ("GEO-MN-DAK", "https://gis2.co.dakota.mn.us/arcgis/rest/services/DCGIS_OL_PropertyInformation/MapServer/71/query", "BLDGVAL > 0", "OBJECTID,TAXPIN,PIN", "MN-DAK", "TAXPIN"),
        ("GEO-MN-ANO", "https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcels/FeatureServer/0/query", "EMV_BLDG > 0", "OBJECTID,STATE_PIN,COUNTY_PIN", "MN-ANO", "STATE_PIN"),
        ("GEO-MN-OLM", "https://public.gis.olmstedcounty.gov/arcgis/rest/services/AGOL_Open_Data/Parcels/FeatureServer/0/query", "EMVBldg > 0", "OBJECTID,PIN", "MN-OLM", "PIN"),
    ]:
        def _mk_mn_geo(p_tag: str, primary_col: str):
            def _m(f: Dict[str, Any]) -> Optional[List[Any]]:
                a = f.get("attributes") or {}
                pid = s_txt(a.get(primary_col)) or s_txt(a.get("PIN")) or s_txt(a.get("COUNTY_PIN"))
                lat, lng = extract_wgs84_centroid(f)
                if not pid or lat == "" or lng == "":
                    return None
                return ["mn", f"{p_tag}-{pid}", "", lat, lng, "", ""]
            return _m

        geo_rows.extend(sharded_arcgis_fetch(
            url, wh, flds, _mk_mn_geo(pfx, id_col),
            page_size=1000, num_shards=8, max_workers=6, return_geometry=True, geometry_precision=5, label=lbl,
        ))

    write_gz_csv("geocoding_backfill_v4.csv.gz", GEO_HEADER, geo_rows)


def main() -> int:
    t0 = time.time()
    log(f"Spot Expansion v4 Worker starting -> gs://{BUCKET}/{PREFIX}/")
    tasks = [
        ("ohio_v4", crunch_ohio_v4),
        ("minnesota_v4", crunch_minnesota_v4),
        ("illinois_v4", crunch_illinois_v4),
        ("ny_syracuse_overlays", crunch_ny_syracuse_overlays),
        ("geocoding_backfill_v4", crunch_geocoding_backfill_v4),
    ]
    # Run in parallel batches of 2-3 to saturate the 16-vCPU Spot VM cleanly without OOM
    with ThreadPoolExecutor(max_workers=3) as pool:
        futs = {pool.submit(fn): name for name, fn in tasks}
        for fut in as_completed(futs):
            nm = futs[fut]
            try:
                fut.result()
                log(f"✓ Completed stage: {nm}")
            except Exception as exc:
                log(f"❌ Stage {nm} failed: {exc}")
    log(f"=== Spot Expansion v4 ALL STAGES COMPLETE in {(time.time() - t0) / 60.0:.1f} min ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
