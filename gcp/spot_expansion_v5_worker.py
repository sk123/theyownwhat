#!/usr/bin/env python3
"""
gcp/spot_expansion_v5_worker.py
===============================
High-throughput Google Compute Engine Spot worker (runs on `renter-help` in
`us-central1-a`, hardware TTL 2h, self-deletes on completion) harvesting 100%
authoritative federal & county open-data layers over Google's 10 Gbps backbone
into `gs://renter-help-staging-20261119/spot_expansion_v5/`:

  1. `hud_federal_subsidies_v5.csv.gz` (~122K authoritative 50-state federal
     housing subsidy + REAC Physical Inspection Score records):
       - HUD Low-Income Housing Tax Credit (LIHTC) (`50.5K`)
       - HUD Multifamily Properties Assisted (Section 8 / 202 / 811 + REAC scores + expiration + mgmt) (`23.8K`)
       - HUD FHA-Insured Multifamily Properties (incl. REAC scores) (`17.8K`)
       - HUD Section 202 Supportive Housing for the Elderly (`11.0K`)
       - USDA Rural Development Section 515 Multifamily Housing (`13.3K`)
       - HUD Public Housing Developments (`6.2K`)
  2. `oh_summit_v5_parcels.csv.gz` (~220K+ deduplicated improved Summit County /
     Akron OH parcels with WGS84 centroids, owner names, mailing addresses,
     year built, assessed values).
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
PREFIX = "spot_expansion_v5"
LOCAL_ONLY = os.environ.get("SPOT_LOCAL_ONLY", "").strip().lower() in ("1", "true", "yes")
WORK_DIR = os.environ.get("SPOT_WORK_DIR", "/tmp/spot_v5")
os.makedirs(WORK_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TheyOwnWhatPublicRecordsSync/5.0)"}

SUBSIDY_HEADER = [
    "source_dataset",
    "federal_id",
    "property_name",
    "address",
    "city",
    "state",
    "zip_code",
    "program_name",
    "subsidy_type",
    "total_units",
    "assisted_units",
    "expiry_date",
    "reac_score",
    "reac_date",
    "mgmt_agent_name",
    "mgmt_agent_address",
    "latitude",
    "longitude",
    "source_url",
]

PARCEL_HEADER = [
    "bbl", "address", "borough", "zip_code", "owner_name", "owner2", "care_of",
    "mailing_address", "mailing_state", "land_use", "bld_class", "num_floors",
    "units_res", "year_built", "assessed_total", "last_sale_date", "last_sale_price",
    "latitude", "longitude", "cama_site_link", "source_system",
]


def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] [SPOT-V5] {msg}", flush=True)


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
        # Check YYYYMMDD integer (e.g. USDA 20070605)
        iv = int(round(v))
        if 19000101 <= iv <= 20991231:
            s = str(iv)
            try:
                d = dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
                return d.isoformat()
            except ValueError:
                pass
        if abs(v) > 10_000_000_000:
            try:
                d = dt.datetime.fromtimestamp(v / 1000.0, tz=dt.timezone.utc).date()
                return d.isoformat() if 1900 <= d.year <= 2099 else ""
            except Exception:
                return ""
        return ""
    s = s_txt(v)
    if not s:
        return ""
    if re.match(r"^\d{8}$", s):
        try:
            d = dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            return d.isoformat()
        except ValueError:
            return ""
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    m2 = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m2:
        try:
            d = dt.date(int(m2.group(3)), int(m2.group(1)), int(m2.group(2)))
            return d.isoformat()
        except ValueError:
            return ""
    return ""


def _geom_centroid(geom: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    if not geom:
        return "", ""
    if "x" in geom and "y" in geom and geom["x"] is not None and geom["y"] is not None:
        x, y = float(geom["x"]), float(geom["y"])
        if -180.0 <= x <= 180.0 and -90.0 <= y <= 90.0:
            return f"{y:.6f}", f"{x:.6f}"
    rings = geom.get("rings")
    if rings and rings[0]:
        pts = rings[0]
        xs = [p[0] for p in pts if len(p) >= 2]
        ys = [p[1] for p in pts if len(p) >= 2]
        if xs and ys:
            cx, cy = sum(xs) / len(xs), sum(ys) / len(ys)
            if -180.0 <= cx <= 180.0 and -90.0 <= cy <= 90.0:
                return f"{cy:.6f}", f"{cx:.6f}"
    return "", ""


def _arcgis_page_worker(
    base_url: str,
    oid_field: str,
    where: str,
    out_fields: str,
    lo_oid: int,
    hi_oid: int,
    page_size: int = 2000,
    return_geometry: bool = True,
) -> List[Dict[str, Any]]:
    sess = requests.Session()
    sess.headers.update(HEADERS)
    out: List[Dict[str, Any]] = []
    cur_min = lo_oid
    while cur_min <= hi_oid:
        clause = f"({where}) AND {oid_field} >= {cur_min} AND {oid_field} <= {hi_oid}"
        params = {
            "where": clause,
            "outFields": out_fields,
            "orderByFields": f"{oid_field} ASC",
            "resultRecordCount": str(page_size),
            "returnGeometry": "true" if return_geometry else "false",
            "outSR": "4326",
            "f": "json",
        }
        feats = None
        for attempt in range(4):
            try:
                r = sess.get(f"{base_url}/query", params=params, timeout=60)
                r.raise_for_status()
                d = r.json()
                if "error" in d:
                    raise RuntimeError(str(d["error"]))
                feats = d.get("features") or []
                break
            except Exception:
                if attempt == 3:
                    return out
                time.sleep(1.5 * (attempt + 1))
        if not feats:
            break
        out.extend(feats)
        last_oid = feats[-1].get("attributes", {}).get(oid_field)
        if last_oid is None or int(last_oid) < cur_min:
            break
        cur_min = int(last_oid) + 1
        if len(feats) < page_size:
            break
    return out


def fetch_arcgis_parallel(
    label: str,
    base_url: str,
    where: str,
    out_fields: str,
    oid_field: str = "OBJECTID",
    page_size: int = 2000,
    workers: int = 8,
    return_geometry: bool = True,
) -> List[Dict[str, Any]]:
    t0 = time.time()
    r = requests.get(
        f"{base_url}/query",
        params={
            "where": where,
            "outStatistics": json.dumps([
                {"statisticType": "min", "onStatisticField": oid_field, "outStatisticFieldName": "min_oid"},
                {"statisticType": "max", "onStatisticField": oid_field, "outStatisticFieldName": "max_oid"},
                {"statisticType": "count", "onStatisticField": oid_field, "outStatisticFieldName": "cnt"},
            ]),
            "f": "json",
        },
        headers=HEADERS,
        timeout=45,
    )
    r.raise_for_status()
    stats = (r.json().get("features") or [{}])[0].get("attributes") or {}
    min_oid = stats.get("min_oid") or stats.get("MIN_OID")
    max_oid = stats.get("max_oid") or stats.get("MAX_OID")
    cnt = stats.get("cnt") or stats.get("CNT") or 0
    if min_oid is None or max_oid is None:
        log(f"  {label}: OID stats missing ({stats}); falling back to sequential pagination.")
        return _arcgis_page_worker(base_url, oid_field, where, out_fields, 0, 50_000_000, page_size, return_geometry)

    min_oid, max_oid = int(min_oid), int(max_oid)
    span = max(1, max_oid - min_oid + 1)
    chunks = max(workers * 3, min(120, int(cnt // 4000) + 1))
    step = max(1, (span + chunks - 1) // chunks)
    ranges = [(lo, min(max_oid, lo + step - 1)) for lo in range(min_oid, max_oid + 1, step)]
    log(f"  {label}: ~{cnt:,} rows (OID {min_oid}..{max_oid}), {len(ranges)} chunks across {workers} workers...")

    all_feats: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [
            pool.submit(_arcgis_page_worker, base_url, oid_field, where, out_fields, lo, hi, page_size, return_geometry)
            for lo, hi in ranges
        ]
        for fut in as_completed(futs):
            all_feats.extend(fut.result())
    log(f"  ✓ {label}: fetched {len(all_feats):,} features in {time.time() - t0:.1f}s")
    return all_feats


def upload_to_gcs(local_path: str) -> None:
    if LOCAL_ONLY:
        log(f"  [LOCAL_ONLY] Keeping {local_path}")
        return
    dst = f"gs://{BUCKET}/{PREFIX}/{os.path.basename(local_path)}"
    subprocess.run(["gcloud", "storage", "cp", local_path, dst, "--quiet"], check=True, timeout=600)
    log(f"  ✓ Uploaded {os.path.basename(local_path)} -> {dst}")


def harvest_federal_subsidies() -> str:
    out_path = os.path.join(WORK_DIR, "hud_federal_subsidies_v5.csv.gz")
    rows: List[List[Any]] = []

    # 1. HUD LIHTC (50.5K nationwide)
    lihtc_url = "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/LIHTC/FeatureServer/0"
    feats = fetch_arcgis_parallel(
        "HUD_LIHTC",
        lihtc_url,
        "1=1",
        "OBJECTID,HUD_ID,PROJECT,PROJ_ADD,PROJ_CTY,PROJ_ST,PROJ_ZIP,N_UNITS,LI_UNITS,YR_PIS,YR_ALLOC,CONTACT,COMPANY,CO_ADD,CO_CTY,CO_ST,CO_ZIP",
        page_size=2000,
        workers=8,
    )
    for ft in feats:
        a = ft.get("attributes") or {}
        lat, lng = _geom_centroid(ft.get("geometry"))
        yr_pis = s_int(a.get("YR_PIS"))
        exp = f"{yr_pis + 30}-12-31" if yr_pis and 1986 <= yr_pis <= 2030 else ""
        mgmt_addr = " ".join(x for x in [s_txt(a.get("CO_ADD")), s_txt(a.get("CO_CTY")), s_txt(a.get("CO_ST")), clean_zip(a.get("CO_ZIP"))] if x)
        rows.append([
            "HUD_LIHTC",
            s_txt(a.get("HUD_ID")) or str(a.get("OBJECTID")),
            s_txt(a.get("PROJECT")),
            s_txt(a.get("PROJ_ADD")),
            s_txt(a.get("PROJ_CTY")),
            s_txt(a.get("PROJ_ST"))[:2].upper(),
            clean_zip(a.get("PROJ_ZIP")),
            "LIHTC (Low-Income Housing Tax Credit)",
            "LIHTC",
            s_int(a.get("N_UNITS")) or "",
            s_int(a.get("LI_UNITS")) or "",
            exp,
            "",
            "",
            s_txt(a.get("COMPANY")) or s_txt(a.get("CONTACT")),
            mgmt_addr,
            lat,
            lng,
            "https://hudgis-hud.opendata.arcgis.com/datasets/HUD::low-income-housing-tax-credit-properties",
        ])

    # 2. HUD Multifamily Assisted (23.8K nationwide — includes REAC scores + expiration + Section 8/202/811!)
    mf_url = "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/MULTIFAMILY_PROPERTIES_ASSISTED/FeatureServer/0"
    mf_fields = (
        "OBJECTID,PROPERTY_ID,PROPERTY_NAME_TEXT,ADDRESS_LINE1_TEXT,STD_ADDR,STD_CITY,PLACED_BASE_CITY_NAME_TEXT,"
        "STD_ST,STD_ZIP5,PROPERTY_CATEGORY_NAME,PROGRAM_TYPE1,PROGRAM_TYPE2,TOTAL_UNIT_COUNT,TOTAL_ASSISTED_UNIT_COUNT,"
        "EXPIRATION_DATE1,EXPIRATION_DATE2,REAC_LAST_INSPECTION_SCORE,REAC_LAST_INSPECTION_DATE,"
        "MGMT_AGENT_ORG_NAME,MGMT_CONTACT_ADDRESS_LINE1,MGMT_CONTACT_CITY_NAME,MGMT_CONTACT_STATE_CODE,MGMT_CONTACT_ZIP_CODE,LAT,LON"
    )
    feats = fetch_arcgis_parallel("HUD_MF_ASSISTED", mf_url, "1=1", mf_fields, page_size=2000, workers=8)
    for ft in feats:
        a = ft.get("attributes") or {}
        lat, lng = _geom_centroid(ft.get("geometry"))
        if not lat and a.get("LAT") and a.get("LON"):
            lat, lng = f"{float(a['LAT']):.6f}", f"{float(a['LON']):.6f}"
        prog = s_txt(a.get("PROGRAM_TYPE1")) or s_txt(a.get("PROPERTY_CATEGORY_NAME")) or "HUD Assisted Multifamily"
        p2 = s_txt(a.get("PROGRAM_TYPE2"))
        if p2 and p2 not in prog:
            prog = f"{prog}; {p2}"
        mgmt_addr = " ".join(x for x in [
            s_txt(a.get("MGMT_CONTACT_ADDRESS_LINE1")),
            s_txt(a.get("MGMT_CONTACT_CITY_NAME")),
            s_txt(a.get("MGMT_CONTACT_STATE_CODE")),
            clean_zip(a.get("MGMT_CONTACT_ZIP_CODE")),
        ] if x)
        rows.append([
            "HUD_MF_ASSISTED",
            s_txt(a.get("PROPERTY_ID")) or str(a.get("OBJECTID")),
            s_txt(a.get("PROPERTY_NAME_TEXT")),
            s_txt(a.get("STD_ADDR")) or s_txt(a.get("ADDRESS_LINE1_TEXT")),
            s_txt(a.get("STD_CITY")) or s_txt(a.get("PLACED_BASE_CITY_NAME_TEXT")),
            s_txt(a.get("STD_ST"))[:2].upper(),
            clean_zip(a.get("STD_ZIP5")),
            prog,
            s_txt(a.get("PROPERTY_CATEGORY_NAME")) or "HUD_ASSISTED",
            s_int(a.get("TOTAL_UNIT_COUNT")) or "",
            s_int(a.get("TOTAL_ASSISTED_UNIT_COUNT")) or "",
            fmt_date(a.get("EXPIRATION_DATE1")) or fmt_date(a.get("EXPIRATION_DATE2")),
            s_int(a.get("REAC_LAST_INSPECTION_SCORE")) if a.get("REAC_LAST_INSPECTION_SCORE") is not None else "",
            fmt_date(a.get("REAC_LAST_INSPECTION_DATE")),
            s_txt(a.get("MGMT_AGENT_ORG_NAME")),
            mgmt_addr,
            lat,
            lng,
            "https://hudgis-hud.opendata.arcgis.com/datasets/HUD::multifamily-properties-assisted",
        ])

    # 3. HUD FHA-Insured Multifamily (17.8K nationwide — includes REAC scores!)
    ins_url = "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/HUD_Insured_Multifamily_Properties/FeatureServer/0"
    feats = fetch_arcgis_parallel("HUD_INSURED_MF", ins_url, "1=1", mf_fields, page_size=2000, workers=8)
    for ft in feats:
        a = ft.get("attributes") or {}
        lat, lng = _geom_centroid(ft.get("geometry"))
        if not lat and a.get("LAT") and a.get("LON"):
            lat, lng = f"{float(a['LAT']):.6f}", f"{float(a['LON']):.6f}"
        prog = s_txt(a.get("PROGRAM_TYPE1")) or s_txt(a.get("PROPERTY_CATEGORY_NAME")) or "HUD FHA-Insured Multifamily"
        mgmt_addr = " ".join(x for x in [
            s_txt(a.get("MGMT_CONTACT_ADDRESS_LINE1")),
            s_txt(a.get("MGMT_CONTACT_CITY_NAME")),
            s_txt(a.get("MGMT_CONTACT_STATE_CODE")),
            clean_zip(a.get("MGMT_CONTACT_ZIP_CODE")),
        ] if x)
        rows.append([
            "HUD_INSURED_MF",
            s_txt(a.get("PROPERTY_ID")) or str(a.get("OBJECTID")),
            s_txt(a.get("PROPERTY_NAME_TEXT")),
            s_txt(a.get("STD_ADDR")) or s_txt(a.get("ADDRESS_LINE1_TEXT")),
            s_txt(a.get("STD_CITY")) or s_txt(a.get("PLACED_BASE_CITY_NAME_TEXT")),
            s_txt(a.get("STD_ST"))[:2].upper(),
            clean_zip(a.get("STD_ZIP5")),
            prog,
            "HUD_INSURED_MF",
            s_int(a.get("TOTAL_UNIT_COUNT")) or "",
            s_int(a.get("TOTAL_ASSISTED_UNIT_COUNT")) or "",
            fmt_date(a.get("EXPIRATION_DATE1")),
            s_int(a.get("REAC_LAST_INSPECTION_SCORE")) if a.get("REAC_LAST_INSPECTION_SCORE") is not None else "",
            fmt_date(a.get("REAC_LAST_INSPECTION_DATE")),
            s_txt(a.get("MGMT_AGENT_ORG_NAME")),
            mgmt_addr,
            lat,
            lng,
            "https://hudgis-hud.opendata.arcgis.com/datasets/HUD::hud-insured-multifamily-properties",
        ])

    # 4. USDA Rural Development Section 515 Multifamily (13.3K nationwide)
    usda_url = "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/USDA_Rural_Housing_Assets/FeatureServer/0"
    usda_fields = (
        "OBJECTID,PROJECT_NAME,PRJ_ADDRESS_LINE1,PRJ_ADDRESS_CITY,PRJ_ADDRESS_STATE,PRJ_ADDRESS_ZIP,"
        "RESTRICTIVE_CLAUSE_EXPIRATION,HUD_UNITS,RA_UNITS,TOTAL_UNITS,RENTAL_DESIGNATION,"
        "MGMT_AGENT_NAME,MGMT_AGENT_ADDR_LINE1,MGMT_AGENT_ADDR_CITY,MGMT_AGENT_ADDR_STATE,MGMT_AGENT_ADDR_ZIP"
    )
    feats = fetch_arcgis_parallel("USDA_RURAL_515", usda_url, "1=1", usda_fields, page_size=4000, workers=6)
    for ft in feats:
        a = ft.get("attributes") or {}
        lat, lng = _geom_centroid(ft.get("geometry"))
        assisted = (s_int(a.get("RA_UNITS")) or 0) + (s_int(a.get("HUD_UNITS")) or 0)
        mgmt_addr = " ".join(x for x in [
            s_txt(a.get("MGMT_AGENT_ADDR_LINE1")),
            s_txt(a.get("MGMT_AGENT_ADDR_CITY")),
            s_txt(a.get("MGMT_AGENT_ADDR_STATE")),
            clean_zip(a.get("MGMT_AGENT_ADDR_ZIP")),
        ] if x)
        rows.append([
            "USDA_RURAL_515",
            str(a.get("OBJECTID")),
            s_txt(a.get("PROJECT_NAME")),
            s_txt(a.get("PRJ_ADDRESS_LINE1")),
            s_txt(a.get("PRJ_ADDRESS_CITY")),
            s_txt(a.get("PRJ_ADDRESS_STATE"))[:2].upper(),
            clean_zip(a.get("PRJ_ADDRESS_ZIP")),
            f"USDA Section 515 ({s_txt(a.get('RENTAL_DESIGNATION')) or 'Rural Multifamily'})",
            "USDA_515",
            s_int(a.get("TOTAL_UNITS")) or "",
            assisted or "",
            fmt_date(a.get("RESTRICTIVE_CLAUSE_EXPIRATION")),
            "",
            "",
            s_txt(a.get("MGMT_AGENT_NAME")),
            mgmt_addr,
            lat,
            lng,
            "https://hudgis-hud.opendata.arcgis.com/datasets/HUD::usda-rural-housing-assets",
        ])

    with gzip.open(out_path, "wt", encoding="utf-8", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(SUBSIDY_HEADER)
        w.writerows(rows)
    log(f"✓ Wrote {len(rows):,} total federal housing subsidy / REAC rows to {out_path}")
    upload_to_gcs(out_path)
    return out_path


def harvest_oh_summit() -> str:
    out_path = os.path.join(WORK_DIR, "oh_summit_v5_parcels.csv.gz")
    url = "https://services3.arcgis.com/3Ukh5HzAdI6WZ3KP/arcgis/rest/services/TaxParcels_dashboard/FeatureServer/0"
    fields = (
        "OBJECTID,PARCELID,CVTTXDSCRP,USECD,USEDSCRP,CLASSCD,SITEADDRESS,"
        "OWNERNME1,OWNERNME2,PSTLADDRESS,PSTLCITY,PSTLSTATE,PSTLZIP5,"
        "FLOORCOUNT,RESYRBLT,LNDVALUE,CNTASSDVAL"
    )
    feats = fetch_arcgis_parallel(
        "OH_SUMMIT",
        url,
        "CNTASSDVAL > LNDVALUE AND OWNERNME1 IS NOT NULL",
        fields,
        page_size=2000,
        workers=10,
        return_geometry=True,
    )
    seen: set = set()
    rows: List[List[Any]] = []
    for ft in feats:
        a = ft.get("attributes") or {}
        pid = s_txt(a.get("PARCELID"))
        owner = s_txt(a.get("OWNERNME1"))
        if not pid or not owner:
            continue
        bbl = f"OH-SUM-{pid}"
        if bbl in seen:
            continue
        seen.add(bbl)
        addr = s_txt(a.get("SITEADDRESS"))
        city = s_txt(a.get("CVTTXDSCRP")) or "SUMMIT COUNTY"
        m_parts = [s_txt(a.get("PSTLADDRESS")), s_txt(a.get("PSTLCITY")), s_txt(a.get("PSTLSTATE")), clean_zip(a.get("PSTLZIP5"))]
        mail = " ".join(p for p in m_parts if p)
        lat, lng = _geom_centroid(ft.get("geometry"))
        cama = f"https://fiscaloffice.summitoh.net/index.php/property-tax-search?parcel={pid}"
        rows.append([
            bbl,
            addr,
            f"{city} (SUMMIT)",
            clean_zip(a.get("PSTLZIP5")),
            owner,
            s_txt(a.get("OWNERNME2")),
            "",
            mail,
            s_txt(a.get("PSTLSTATE"))[:2].upper(),
            s_txt(a.get("USEDSCRP")) or s_txt(a.get("USECD")),
            s_txt(a.get("CLASSCD")),
            s_int(a.get("FLOORCOUNT")) or "",
            "",
            s_int(a.get("RESYRBLT")) or "",
            s_flt(a.get("CNTASSDVAL")) or "",
            "",
            "",
            lat,
            lng,
            cama,
            "SUMMIT_COUNTY_OH_FISCAL",
        ])

    with gzip.open(out_path, "wt", encoding="utf-8", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(PARCEL_HEADER)
        w.writerows(rows)
    log(f"✓ Wrote {len(rows):,} deduplicated Summit County (Akron) OH parcels to {out_path}")
    upload_to_gcs(out_path)
    return out_path


def main() -> int:
    t0 = time.time()
    log("=== Starting Spot Expansion V5 (Nationwide Federal Subsidies/REAC + Summit County/Akron OH) ===")
    harvest_federal_subsidies()
    harvest_oh_summit()
    log(f"=== Spot Expansion V5 COMPLETE in {time.time() - t0:.1f}s ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
