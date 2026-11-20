#!/usr/bin/env python3
"""
gcp/spot_expansion_v3_worker.py
===============================
High-throughput Google Compute Engine Spot worker (runs on `renter-help` in
`us-central1-a`, auto-deletes on completion) that pulls verified, authoritative
state/county government open-data layers over Google's 10 Gbps backbone and
uploads clean, improved-building-only `.csv.gz` spines to
`gs://renter-help-staging-20261119/spot_expansion_v3/`:

  1. `oh_expansion_parcels.csv.gz`:
       - Franklin County Auditor (Columbus, OH): `BLDVALUEBASE > 0` (~451K parcels)
       - Hamilton County CAGIS (Cincinnati, OH): `MKTIMP > 0` (~282K parcels)
  2. `mn_expansion_parcels.csv.gz`:
       - MnGeo Official Statewide Opt-In Parcels (23 counties outside existing
         Hennepin & Ramsey CAMA: Anoka, Dakota, Washington, St. Louis/Duluth,
         Olmsted/Rochester, Sherburne, Chisago, Winona, etc., `emv_bldg > 0`, ~780K)
  3. `il_lake_parcels.csv.gz`:
       - Lake County, IL Official OpenData_ParcelPolygons (~279K addressed parcels)
  4. `fl_ny_code_overlays.csv.gz`:
       - City of Orlando Code Enforcement Cases (`k6e8-nw6w`, 2019+)
       - City of Gainesville Code Complaints/Violations (`vu9p-a5f7`, 2019+)
       - City of Rochester NBD Code Enforcement (`Building_Blocks_Code_Enforcement`)
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime as dt
import gzip
import json
import math
import os
import re
import subprocess
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

csv.field_size_limit(10_000_000)

BUCKET = os.environ.get("GCS_BUCKET", "renter-help-staging-20261119")
PREFIX = "spot_expansion_v3"
LOCAL_ONLY = os.environ.get("SPOT_LOCAL_ONLY", "").strip().lower() in ("1", "true", "yes")
WORK_DIR = os.environ.get("SPOT_WORK_DIR", "/tmp/spot_v3")
os.makedirs(WORK_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TheyOwnWhatPublicRecordsSync/3.0)"}

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
    if isinstance(v, (int, float)) and v > 10_000_000_000:
        try:
            d = dt.datetime.fromtimestamp(v / 1000.0, tz=dt.timezone.utc)
            if 1900 <= d.year <= 2030:
                return d.strftime("%Y-%m-%d")
        except Exception:
            return ""
    t = s_txt(v)
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", t)
    if m:
        return m.group(1)
    m2 = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", t)
    if m2:
        return f"{m2.group(3)}-{int(m2.group(1)):02d}-{int(m2.group(2)):02d}"
    return ""


def web_mercator_to_wgs84(x: Optional[float], y: Optional[float]) -> Tuple[Any, Any]:
    if x is None or y is None or abs(x) < 1000 or abs(y) < 1000:
        return "", ""
    try:
        lon = x * 180.0 / 20037508.34
        lat = math.degrees(2.0 * math.atan(math.exp((y / 20037508.34) * math.pi)) - math.pi / 2.0)
        if 24.0 <= lat <= 50.0 and -126.0 <= lon <= -65.0:
            return round(lat, 6), round(lon, 6)
    except Exception:
        pass
    return "", ""


def upload_to_gcs(local_path: str, rel_name: str) -> None:
    if LOCAL_ONLY:
        log(f"  -> Saved locally {local_path} ({os.path.getsize(local_path)/1e6:.2f} MB; local-only mode)")
        return
    dst = f"gs://{BUCKET}/{PREFIX}/{rel_name}"
    subprocess.run(
        ["gcloud", "storage", "cp", local_path, dst, "--quiet"],
        check=True,
        timeout=600,
    )
    sz = os.path.getsize(local_path) / 1e6
    log(f"  -> Uploaded {dst} ({sz:.2f} MB)")


def write_gz_csv(filename: str, header: List[str], rows: List[List[Any]]) -> str:
    path = os.path.join(WORK_DIR, filename)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(header)
        w.writerows(rows)
    log(f"  Wrote {filename}: {len(rows):,} rows ({os.path.getsize(path)/1e6:.2f} MB)")
    upload_to_gcs(path, filename)
    return path


def get_oid_bounds(url: str, oid_field: str = "OBJECTID", where: str = "1=1") -> Tuple[int, int]:
    r = requests.get(
        url,
        params={
            "where": where,
            "outStatistics": json.dumps([
                {"statisticType": "min", "onStatisticField": oid_field, "outStatisticFieldName": "min_oid"},
                {"statisticType": "max", "onStatisticField": oid_field, "outStatisticFieldName": "max_oid"},
            ]),
            "f": "json",
        },
        headers=HEADERS,
        timeout=60,
    ).json()
    attrs = ((r.get("features") or [{}])[0].get("attributes")) or {}
    return int(attrs.get("min_oid") or 1), int(attrs.get("max_oid") or 1_000_000)


def sharded_arcgis_fetch(
    url: str,
    where_base: str,
    out_fields: str,
    row_mapper: Callable[[Dict[str, Any]], Optional[List[Any]]],
    *,
    oid_field: str = "OBJECTID",
    page_size: int = 2000,
    num_shards: int = 12,
    max_workers: int = 10,
    return_centroid: bool = False,
    label: str = "ARCGIS",
) -> List[List[Any]]:
    lo_all, hi_all = get_oid_bounds(url, oid_field=oid_field, where="1=1")
    span = max(1, hi_all - lo_all + 1)
    step = max(1, (span + num_shards - 1) // num_shards)
    shards = [(lo_all + i * step, min(hi_all, lo_all + (i + 1) * step - 1)) for i in range(num_shards)]
    shards = [(a, b) for a, b in shards if a <= b]
    log(f"[{label}] OID bounds {lo_all:,}..{hi_all:,} ({len(shards)} shards, page={page_size}, centroid={return_centroid})")

    def _run_shard(lo: int, hi: int, sid: int) -> List[List[Any]]:
        sess = requests.Session()
        sess.headers.update(HEADERS)
        cur_oid = lo - 1
        out: List[List[Any]] = []
        while True:
            where = f"{oid_field} > {cur_oid} AND {oid_field} <= {hi}"
            if where_base and where_base != "1=1":
                where = f"({where}) AND ({where_base})"
            params: Dict[str, Any] = {
                "where": where,
                "outFields": out_fields,
                "returnGeometry": "false",
                "orderByFields": f"{oid_field} ASC",
                "resultRecordCount": page_size,
                "f": "json",
            }
            if return_centroid:
                params["returnCentroid"] = "true"
                params["outSR"] = "4326"
            data: Dict[str, Any] = {}
            for att in range(6):
                try:
                    resp = sess.get(url, params=params, timeout=75)
                    resp.raise_for_status()
                    data = resp.json()
                    if "error" not in data:
                        break
                    log(f"  [{label}] shard {sid} API err: {str(data.get('error'))[:120]}")
                except Exception as exc:
                    if att == 5:
                        log(f"  [{label}] shard {sid} failed at oid={cur_oid}: {exc}")
                time.sleep(min(20, 2 * (att + 1)))
            feats = data.get("features") or []
            if not feats:
                break
            max_seen = cur_oid
            for f in feats:
                a = f.get("attributes") or {}
                oid = int(a.get(oid_field) or a.get(oid_field.lower()) or a.get(oid_field.upper()) or 0)
                if oid > max_seen:
                    max_seen = oid
                mapped = row_mapper(f)
                if mapped is not None:
                    out.append(mapped)
            if max_seen <= cur_oid:
                break
            cur_oid = max_seen
            if len(feats) < page_size and not data.get("exceededTransferLimit"):
                break
        return out

    all_rows: List[List[Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_run_shard, a, b, i): i for i, (a, b) in enumerate(shards)}
        done_cnt = 0
        for fut in as_completed(futs):
            done_cnt += 1
            rows = fut.result()
            all_rows.extend(rows)
            if done_cnt % 4 == 0 or done_cnt == len(shards):
                log(f"  [{label}] {done_cnt}/{len(shards)} shards done ({len(all_rows):,} rows)")
    return all_rows


def fetch_socrata_paged(
    domain: str,
    dataset_id: str,
    select: str,
    where: str,
    order: str,
    page_size: int = 50000,
    max_rows: int = 1_000_000,
) -> List[Dict[str, Any]]:
    url = f"https://{domain}/resource/{dataset_id}.json"
    sess = requests.Session()
    sess.headers.update(HEADERS)
    rows: List[Dict[str, Any]] = []
    offset = 0
    while len(rows) < max_rows:
        params = {
            "$select": select,
            "$where": where,
            "$order": order,
            "$limit": min(page_size, max_rows - len(rows)),
            "$offset": offset,
        }
        batch: List[Dict[str, Any]] = []
        for att in range(5):
            try:
                r = sess.get(url, params=params, timeout=90)
                r.raise_for_status()
                batch = r.json()
                break
            except Exception as exc:
                if att == 4:
                    log(f"  [SOCRATA {domain}/{dataset_id}] failed at offset={offset}: {exc}")
                time.sleep(2 * (att + 1))
        if not batch or not isinstance(batch, list):
            break
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < params["$limit"]:
            break
    log(f"  [SOCRATA {domain}/{dataset_id}] fetched {len(rows):,} rows")
    return rows


# ==============================================================================
# 1. OHIO EXPANSION: Franklin County (Columbus) + Hamilton County (Cincinnati)
# ==============================================================================
def crunch_ohio_expansion() -> None:
    log("=== 1. Starting OHIO Expansion (Franklin/Columbus + Hamilton/Cincinnati) ===")
    oh_rows: List[List[Any]] = []

    # 1a. Franklin County Auditor (Columbus, OH) — 451K improved parcels
    fr_url = "https://gis.franklincountyohio.gov/hosting/rest/services/ParcelFeatures/Parcel_Features/MapServer/0/query"
    fr_fields = (
        "OBJECTID,PARCELID,SITEADDRESS,ZIPCD,CVTTXDSCRP,OWNERNME1,OWNERNME2,"
        "PSTLADDRES,PSTLCITYSTZIP,MAILNME1,MAILNME2,CLASSCD,CLASSDSCRP,"
        "FLOORCOUNT,RESYRBLT,BLDVALUEBASE,TOTVALUEBASE,SALEDATE,SALEPRICE,RENTAL"
    )

    def _map_franklin(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pid = s_txt(a.get("PARCELID"))
        owner = s_txt(a.get("OWNERNME1"))
        addr = s_txt(a.get("SITEADDRESS"))
        if not pid or not owner or not addr or addr.startswith("0 "):
            return None
        city_raw = s_txt(a.get("CVTTXDSCRP")).split("-")[0].strip().upper() or "COLUMBUS"
        mail = s_txt(a.get("PSTLADDRES"))
        if not mail:
            mail = ", ".join(p for p in (s_txt(a.get("MAILNME1")), s_txt(a.get("MAILNME2"))) if p)
        mst_m = re.search(r"\b([A-Z]{2})\s+\d{5}", mail.upper())
        mst = mst_m.group(1) if mst_m else ""
        yr = s_int(a.get("RESYRBLT"))
        flr = s_int(a.get("FLOORCOUNT"))
        tot = s_flt(a.get("TOTVALUEBASE")) or s_flt(a.get("BLDVALUEBASE"))
        sprice = s_flt(a.get("SALEPRICE"))
        lu = s_txt(a.get("CLASSDSCRP")) or "IMPROVED PARCEL"
        if s_txt(a.get("RENTAL")).upper() == "Y":
            lu = f"{lu} [RENTAL]"
        return [
            f"OH-FRA-{pid}", addr, f"{city_raw} (FRANKLIN)", clean_zip(a.get("ZIPCD")),
            owner, s_txt(a.get("OWNERNME2")), "", mail, mst,
            lu, s_txt(a.get("CLASSCD")),
            flr if (flr and 1 <= flr <= 120) else "",
            "", yr if (yr and 1750 <= yr <= 2030) else "",
            tot if tot is not None else "",
            fmt_date(a.get("SALEDATE")),
            sprice if (sprice and sprice > 0) else "",
            "", "",
            f"https://property.franklincountyauditor.com/_web/datalets/datalet.aspx?mode=profileall&sIndex=0&idx=1&LMparent=20&id={pid}",
            "FRANKLIN_COUNTY_AUDITOR_CAMA",
        ]

    fr_rows = sharded_arcgis_fetch(
        fr_url, "BLDVALUEBASE > 0 AND OWNERNME1 IS NOT NULL", fr_fields, _map_franklin,
        oid_field="OBJECTID", page_size=2000, num_shards=16, max_workers=12,
        return_centroid=False, label="OH-FRANKLIN",
    )
    oh_rows.extend(fr_rows)
    del fr_rows

    # 1b. Hamilton County CAGIS (Cincinnati, OH) — 282K improved parcels
    ham_url = "https://services.arcgis.com/JyZag7oO4NteHGiq/arcgis/rest/services/Parcel_Polygons/FeatureServer/0/query"
    ham_fields = (
        "OBJECTID,PARCELID,ADDRNO,ADDRST,ADDRSF,TAXDST_DIS,OWNNM1,OWNNM2,"
        "OWNAD1,OWNAD1A,OWNAD2,OWNADCITY,OWNADSTATE,OWNADZIP,"
        "MKTIMP,MKT_TOTAL_VAL,CLASS,NUM_UNITS,SALDAT,SALAMT,RENT_REG_FLAG"
    )

    def _map_hamilton(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pid = s_txt(a.get("PARCELID"))
        owner = s_txt(a.get("OWNNM1"))
        addr = " ".join(p for p in (s_txt(a.get("ADDRNO")), s_txt(a.get("ADDRST")), s_txt(a.get("ADDRSF"))) if p)
        if not pid or not owner or not addr or addr.startswith("0 "):
            return None
        m_st = " ".join(p for p in (s_txt(a.get("OWNAD1")), s_txt(a.get("OWNAD1A"))) if p)
        m_ct = s_txt(a.get("OWNADCITY"))
        m_st2 = s_txt(a.get("OWNADSTATE"))
        m_zp = clean_zip(a.get("OWNADZIP"))
        if m_st and (m_ct or m_st2 or m_zp):
            mail = ", ".join(p for p in (m_st, m_ct, m_st2, m_zp) if p)
        else:
            mail = ", ".join(p for p in (m_st, s_txt(a.get("OWNAD2"))) if p)
        city = s_txt(a.get("TAXDST_DIS")).split("-")[0].strip().upper() or "CINCINNATI"
        units = s_int(a.get("NUM_UNITS"))
        tot = s_flt(a.get("MKT_TOTAL_VAL")) or s_flt(a.get("MKTIMP"))
        sprice = s_flt(a.get("SALAMT"))
        pcls = s_txt(a.get("CLASS"))
        lu = f"OHIO CLASS {pcls}" if pcls else "IMPROVED PARCEL"
        if s_txt(a.get("RENT_REG_FLAG")).upper() in ("Y", "YES", "1"):
            lu = f"{lu} [RENTAL REG]"
        c = f.get("centroid") or {}
        return [
            f"OH-HAM-{pid}", addr, f"{city} (HAMILTON)", "",
            owner, s_txt(a.get("OWNNM2")), "", mail, m_st2,
            lu, pcls, "",
            units if (units and units > 0) else "", "",
            tot if tot is not None else "",
            fmt_date(a.get("SALDAT")),
            sprice if (sprice and sprice > 0) else "",
            round(float(c["y"]), 6) if c.get("y") is not None else "",
            round(float(c["x"]), 6) if c.get("x") is not None else "",
            f"https://wedge1.hcauditor.org/view/re/{pid}/2025/summary",
            "HAMILTON_COUNTY_CAGIS_CAMA",
        ]

    ham_rows = sharded_arcgis_fetch(
        ham_url, "MKTIMP > 0 AND OWNNM1 IS NOT NULL", ham_fields, _map_hamilton,
        oid_field="OBJECTID", page_size=2000, num_shards=14, max_workers=10,
        return_centroid=True, label="OH-HAMILTON",
    )
    oh_rows.extend(ham_rows)
    del ham_rows

    # 1c. Lucas County Auditor (Toledo, OH) — ~184K non-vacant addressed parcels
    luc_url = "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Parcel_General_Land_Use_Classification_2025/FeatureServer/0/query"
    luc_fields = "objectid,parid,property_address,mailing_address,owner,class,luc,taxdist"

    def _map_lucas(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pid = s_txt(a.get("parid"))
        owner = s_txt(a.get("owner"))
        addr = s_txt(a.get("property_address"))
        luc = s_txt(a.get("luc"))
        if not pid or not owner or not addr or addr.startswith("0 ") or luc in ("100", "300", "400", "500", "600", "700"):
            return None
        mail = s_txt(a.get("mailing_address"))
        mst_m = re.search(r"\b([A-Z]{2})\s+\d{5}", mail.upper())
        mst = mst_m.group(1) if mst_m else ""
        city = s_txt(a.get("taxdist")).split("-")[0].strip().upper() or "TOLEDO"
        return [
            f"OH-LUC-{pid}", addr, f"{city} (LUCAS)", "",
            owner, "", "", mail, mst,
            f"OHIO LUC {luc}" if luc else "IMPROVED PARCEL", luc, "", "", "", "", "", "",
            "", "",
            f"https://icare.co.lucas.oh.us/LucasCare/Datalets/Datalet.aspx?sIndex=0&idx=1&pin={pid}",
            "LUCAS_COUNTY_AUDITOR_CAMA",
        ]

    luc_rows = sharded_arcgis_fetch(
        luc_url, "property_address IS NOT NULL AND owner IS NOT NULL", luc_fields, _map_lucas,
        oid_field="objectid", page_size=2000, num_shards=10, max_workers=8,
        return_centroid=False, label="OH-LUCAS",
    )
    oh_rows.extend(luc_rows)
    del luc_rows

    write_gz_csv("oh_expansion_parcels.csv.gz", PARCEL_HEADER, oh_rows)
    del oh_rows


# ==============================================================================
# 2. MINNESOTA TWIN CITIES EXPANSION (Dakota County + Anoka County — sub-second!)
# ==============================================================================
def crunch_minnesota_expansion() -> None:
    log("=== 2. Starting MINNESOTA Twin Cities Expansion (Dakota County + Anoka County) ===")
    mn_rows: List[List[Any]] = []

    # 2a. Dakota County Official GIS (`167K` parcels, `BLDGVAL > 0`)
    dk_url = "https://gis2.co.dakota.mn.us/arcgis/rest/services/DCGIS_OL_PropertyInformation/MapServer/71/query"
    dk_fields = (
        "OBJECTID,TAXPIN,PIN,SITEADDRESS,MUNICIPALITY,P_CITY_ST_ZIP,"
        "FULLNAME,FULLNAME_PUBLIC,JOINT_OWNER,OWN_ADD_L1,OWN_ADD_L2,OWN_ADD_L3,"
        "BLDGVAL,TOTALVAL,YEAR_BUILT,YRBUILT,DWELL_TYPE,USE1_DESC,SALE_DATE,SALE_VALUE"
    )

    def _map_dakota(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        bldg = s_flt(a.get("BLDGVAL")) or 0.0
        if bldg <= 0:
            return None
        owner = s_txt(a.get("FULLNAME")) or s_txt(a.get("FULLNAME_PUBLIC"))
        pin = s_txt(a.get("TAXPIN")) or s_txt(a.get("PIN"))
        addr = s_txt(a.get("SITEADDRESS"))
        if not owner or not pin or not addr or addr.startswith("0 "):
            return None
        mail = ", ".join(p for p in (s_txt(a.get("OWN_ADD_L1")), s_txt(a.get("OWN_ADD_L2")), s_txt(a.get("OWN_ADD_L3"))) if p)
        mst_m = re.search(r"\b([A-Z]{2})\s+\d{5}", mail.upper())
        mst = mst_m.group(1) if mst_m else ""
        city = s_txt(a.get("MUNICIPALITY")).upper() or "DAKOTA COUNTY"
        yr = s_int(a.get("YEAR_BUILT")) or s_int(a.get("YRBUILT"))
        tot = s_flt(a.get("TOTALVAL")) or bldg
        sval = s_flt(a.get("SALE_VALUE"))
        lu = s_txt(a.get("DWELL_TYPE")) or s_txt(a.get("USE1_DESC")) or "IMPROVED PARCEL"
        return [
            f"MN-DAK-{pin}", addr, f"{city} (DAKOTA)", clean_zip(a.get("P_CITY_ST_ZIP")),
            owner, s_txt(a.get("JOINT_OWNER")), "", mail, mst,
            lu, s_txt(a.get("USE1_DESC")), "", "",
            yr if (yr and 1750 <= yr <= 2030) else "",
            tot if tot is not None else "",
            fmt_date(a.get("SALE_DATE")),
            sval if (sval and sval > 0) else "",
            "", "",
            f"https://gis2.co.dakota.mn.us/PropertyInformationSearch/PropertyDetail.aspx?pin={pin}",
            "DAKOTA_COUNTY_MN_CAMA",
        ]

    dk_rows = sharded_arcgis_fetch(
        dk_url, "BLDGVAL > 0", dk_fields, _map_dakota,
        oid_field="OBJECTID", page_size=1000, num_shards=10, max_workers=8,
        return_centroid=False, label="MN-DAKOTA",
    )
    mn_rows.extend(dk_rows)
    del dk_rows

    # 2b. Anoka County via Metropolitan Council Official GIS (`140K` parcels, `EMV_BLDG > 0`)
    an_url = "https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcels/FeatureServer/0/query"
    an_fields = (
        "OBJECTID,STATE_PIN,COUNTY_PIN,CO_NAME,CTU_NAME,ANUMBER,ST_PRE_DIR,ST_NAME,ST_POS_TYP,ST_POS_DIR,"
        "SUB_TYPE1,SUB_ID1,ZIP,OWNER_NAME,OWNER_MORE,OWN_ADD_L1,OWN_ADD_L2,OWN_ADD_L3,OWN_ADD_L4,"
        "TAX_NAME,TAX_ADD_L1,TAX_ADD_L2,TAX_ADD_L3,EMV_BLDG,EMV_TOTAL,YEAR_BUILT,NUM_UNITS,"
        "USECLASS1,DWELL_TYPE,SALE_DATE,SALE_VALUE"
    )

    def _map_anoka(f: Dict[str, Any]) -> Optional[List[Any]]:
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
            mail_parts = [s_txt(a.get(k)) for k in ("TAX_ADD_L1", "TAX_ADD_L2", "TAX_ADD_L3")]
        mail = ", ".join(p for p in mail_parts if p)
        mst_m = re.search(r"\b([A-Z]{2})\s+\d{5}", mail.upper())
        mst = mst_m.group(1) if mst_m else ""
        co = s_txt(a.get("CO_NAME")).upper() or "ANOKA"
        city = s_txt(a.get("CTU_NAME")).upper() or co
        yr = s_int(a.get("YEAR_BUILT"))
        units = s_int(a.get("NUM_UNITS"))
        tot = s_flt(a.get("EMV_TOTAL")) or bldg
        sval = s_flt(a.get("SALE_VALUE"))
        lu = s_txt(a.get("DWELL_TYPE")) or s_txt(a.get("USECLASS1")) or "IMPROVED PARCEL"
        return [
            f"MN-ANO-{pin}", addr, f"{city} ({co})", clean_zip(a.get("ZIP")),
            owner, s_txt(a.get("OWNER_MORE")), "", mail, mst,
            lu, s_txt(a.get("USECLASS1")), "",
            units if (units and units > 0) else "",
            yr if (yr and 1750 <= yr <= 2030) else "",
            tot if tot is not None else "",
            fmt_date(a.get("SALE_DATE")),
            sval if (sval and sval > 0) else "",
            "", "", "",
            "ANOKA_COUNTY_MN_METC_CAMA",
        ]

    an_rows = sharded_arcgis_fetch(
        an_url, "EMV_BLDG > 0", an_fields, _map_anoka,
        oid_field="OBJECTID", page_size=1000, num_shards=10, max_workers=8,
        return_centroid=False, label="MN-ANOKA",
    )
    mn_rows.extend(an_rows)
    del an_rows

    # 2c. Olmsted County Official GIS (Rochester, MN — ~65K improved parcels, TaxYear 2027)
    olm_url = "https://public.gis.olmstedcounty.gov/arcgis/rest/services/AGOL_Open_Data/Parcels/FeatureServer/0/query"
    olm_fields = (
        "OBJECTID,PIN,SiteAddrNo,SitePreDir,SiteStName,SiteStType,SitePostDir,SiteUnitNo,"
        "SiteCity,SiteZip5,OwnerName1,OwnerName2,InCareOf,OwnerAddr1,OwnerAddr2,OwnerCity,OwnerState,OwnerZIP5,"
        "EMVBldg,EMVTotal,LandUseDes,Class,LivngUnits"
    )

    def _map_olmsted(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        bldg = s_flt(a.get("EMVBldg")) or 0.0
        if bldg <= 0:
            return None
        pid = s_txt(a.get("PIN"))
        owner = s_txt(a.get("OwnerName1"))
        addr = " ".join(p for p in (
            s_txt(a.get("SiteAddrNo")), s_txt(a.get("SitePreDir")),
            s_txt(a.get("SiteStName")), s_txt(a.get("SiteStType")),
            s_txt(a.get("SitePostDir")),
            f"UNIT {s_txt(a.get('SiteUnitNo'))}" if s_txt(a.get("SiteUnitNo")) else "",
        ) if p)
        if not pid or not owner or not addr or addr.startswith("0 "):
            return None
        mail = ", ".join(p for p in (
            s_txt(a.get("OwnerAddr1")), s_txt(a.get("OwnerAddr2")),
            s_txt(a.get("OwnerCity")), s_txt(a.get("OwnerState")), clean_zip(a.get("OwnerZIP5")),
        ) if p)
        city = s_txt(a.get("SiteCity")).upper() or "ROCHESTER"
        units = s_int(a.get("LivngUnits"))
        tot = s_flt(a.get("EMVTotal")) or bldg
        return [
            f"MN-OLM-{pid}", addr, f"{city} (OLMSTED)", clean_zip(a.get("SiteZip5")),
            owner, s_txt(a.get("OwnerName2")), s_txt(a.get("InCareOf")), mail, s_txt(a.get("OwnerState")),
            s_txt(a.get("LandUseDes")) or s_txt(a.get("Class")) or "IMPROVED PARCEL", s_txt(a.get("Class")), "",
            units if (units and units > 0) else "", "",
            tot if tot is not None else "", "", "",
            "", "", "",
            "OLMSTED_COUNTY_MN_CAMA",
        ]

    olm_rows = sharded_arcgis_fetch(
        olm_url, "EMVBldg > 0 AND OwnerName1 IS NOT NULL", olm_fields, _map_olmsted,
        oid_field="OBJECTID", page_size=2000, num_shards=8, max_workers=6,
        return_centroid=False, label="MN-OLMSTED",
    )
    mn_rows.extend(olm_rows)
    del olm_rows

    write_gz_csv("mn_expansion_parcels.csv.gz", PARCEL_HEADER, mn_rows)
    del mn_rows


# ==============================================================================
# 3. ILLINOIS LAKE COUNTY EXPANSION (~279K addressed parcels)
# ==============================================================================
def crunch_il_lake() -> None:
    log("=== 3. Starting ILLINOIS Lake County Expansion (~279K addressed parcels) ===")
    lake_url = "https://services3.arcgis.com/HESxeTbDliKKvec2/arcgis/rest/services/OpenData_ParcelPolygons/FeatureServer/0/query"
    lake_fields = (
        "OBJECTID,PIN,taxpayer_name,taxpayer_addr_line_care_of,"
        "taxpayer_addr_line_1,taxpayer_addr_line_2,taxpayer_addr_city,taxpayer_addr_state,taxpayer_addr_zip,"
        "situs_addr_line_1_First,situs_addr_line_2_First,situs_addr_city_First,situs_addr_zip_legdat_First,"
        "INSIDE_X,INSIDE_Y"
    )

    def _map_lake(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pin = s_txt(a.get("PIN"))
        owner = s_txt(a.get("taxpayer_name"))
        addr = " ".join(p for p in (s_txt(a.get("situs_addr_line_1_First")), s_txt(a.get("situs_addr_line_2_First"))) if p)
        if not pin or not owner or not addr or addr.startswith("0 "):
            return None
        city = s_txt(a.get("situs_addr_city_First")).upper() or "LAKE COUNTY"
        m_st = " ".join(p for p in (s_txt(a.get("taxpayer_addr_line_1")), s_txt(a.get("taxpayer_addr_line_2"))) if p)
        m_ct = s_txt(a.get("taxpayer_addr_city"))
        m_st2 = s_txt(a.get("taxpayer_addr_state"))
        m_zp = clean_zip(a.get("taxpayer_addr_zip"))
        mail = ", ".join(p for p in (m_st, m_ct, m_st2, m_zp) if p)
        lat, lon = web_mercator_to_wgs84(s_flt(a.get("INSIDE_X")), s_flt(a.get("INSIDE_Y")))
        return [
            f"IL-LAKE-{pin}", addr, f"{city} (LAKE)", clean_zip(a.get("situs_addr_zip_legdat_First")),
            owner, "", s_txt(a.get("taxpayer_addr_line_care_of")), mail, m_st2,
            "ADDRESSED PARCEL", "", "", "", "", "", "", "",
            lat, lon,
            f"https://tax.lakecountyil.gov/datalets/datalet.aspx?mode=profileall&pin={pin}",
            "LAKE_COUNTY_IL_GIS_PARCELS",
        ]

    lake_rows = sharded_arcgis_fetch(
        lake_url, "taxpayer_name IS NOT NULL AND situs_addr_line_1_First IS NOT NULL", lake_fields, _map_lake,
        oid_field="OBJECTID", page_size=2000, num_shards=12, max_workers=10,
        return_centroid=False, label="IL-LAKE",
    )
    write_gz_csv("il_lake_parcels.csv.gz", PARCEL_HEADER, lake_rows)
    del lake_rows


# ==============================================================================
# 3b. NEW YORK EXPANSION: City of Rochester (Monroe County) RPS Tax Parcels
# ==============================================================================
def crunch_ny_rochester() -> None:
    log("=== 3b. Starting NY City of Rochester (Monroe County) RPS Parcels ===")
    url = "https://maps.cityofrochester.gov/arcgis/rest/services/App_PropertyInformation/ROC_Parcel_Query_RPS_Merged/MapServer/0/query"
    fields = (
        "OBJECTID,PARCELID,PRINTKEY,SITEADDRESS,CITY,ZIP5,CLASSCD,CLASSDSCRP,"
        "OWNERNME1,PSTLADDRESS,PSTLCITY,CURRENT_LAND_VALUE,CURRENT_TOTAL_VALUE,"
        "SALE_DATE,SALE_PRICE,NORESUNITS"
    )

    def _map_roch(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pid = s_txt(a.get("PARCELID"))
        owner = s_txt(a.get("OWNERNME1"))
        addr = s_txt(a.get("SITEADDRESS"))
        if not pid or not owner or not addr or addr.startswith("0 "):
            return None
        cls = s_txt(a.get("CLASSCD"))
        # Skip purely vacant land (NYS class 3xx except 312 residential with improvements)
        if cls.startswith("3") and cls != "312":
            tot_v = s_flt(a.get("CURRENT_TOTAL_VALUE")) or 0
            land_v = s_flt(a.get("CURRENT_LAND_VALUE")) or 0
            if tot_v <= land_v:
                return None
        mail = ", ".join(p for p in (s_txt(a.get("PSTLADDRESS")), s_txt(a.get("PSTLCITY"))) if p)
        mst_m = re.search(r"\b([A-Z]{2})\s+\d{5}", mail.upper())
        mst = mst_m.group(1) if mst_m else "NY"
        units = s_int(a.get("NORESUNITS"))
        tot = s_flt(a.get("CURRENT_TOTAL_VALUE"))
        sprice = s_flt(a.get("SALE_PRICE"))
        c = f.get("centroid") or {}
        return [
            f"NY-MONROE-{pid}", addr, "ROCHESTER (MONROE)", clean_zip(a.get("ZIP5")),
            owner, "", "", mail, mst,
            s_txt(a.get("CLASSDSCRP")) or "IMPROVED PARCEL", cls, "",
            units if (units and units > 0) else "", "",
            tot if tot is not None else "",
            fmt_date(a.get("SALE_DATE")),
            sprice if (sprice and sprice > 10) else "",
            round(float(c["y"]), 6) if c.get("y") is not None else "",
            round(float(c["x"]), 6) if c.get("x") is not None else "",
            f"https://maps.cityofrochester.gov/propinfo/?sbl={pid}",
            "ROCHESTER_NY_RPS_CAMA",
        ]

    rows = sharded_arcgis_fetch(
        url, "OWNERNME1 IS NOT NULL AND SITEADDRESS IS NOT NULL", fields, _map_roch,
        oid_field="OBJECTID", page_size=5000, num_shards=8, max_workers=6,
        return_centroid=True, label="NY-ROCHESTER",
    )
    write_gz_csv("ny_rochester_parcels.csv.gz", PARCEL_HEADER, rows)
    del rows


# ==============================================================================
# 4. FLORIDA & NEW YORK CODE ENFORCEMENT OVERLAYS (Orlando, Gainesville, Rochester)
# ==============================================================================
def crunch_fl_ny_overlays() -> None:
    log("=== 4. Starting FL (Orlando + Gainesville) & NY (Rochester) Code Enforcement Overlays ===")
    ov_rows: List[List[Any]] = []

    # 4a. City of Orlando Code Enforcement Cases (2019+)
    orl = fetch_socrata_paged(
        "data.cityoforlando.net", "k6e8-nw6w",
        "apno,caseinfostatus,derived_address,casetype,casedt,parcel_id",
        "casedt >= '2019-01-01T00:00:00'",
        "apno ASC",
        page_size=50000,
        max_rows=300000,
    )
    for r in orl:
        rid = s_txt(r.get("apno"))
        if not rid:
            continue
        raw_addr = re.sub(r"\s+", " ", s_txt(r.get("derived_address"))).strip()
        raw_addr = re.sub(r"\s+ORLANDO\s+FL\b.*$", "", raw_addr, flags=re.IGNORECASE).strip()
        if "NO ADDRESS" in raw_addr.upper():
            raw_addr = ""
        ov_rows.append([
            "fl", "violation", f"ORL-{rid}", s_txt(r.get("parcel_id")),
            raw_addr, "ORLANDO", "",
            s_txt(r.get("caseinfostatus")) or "OPEN", fmt_date(r.get("casedt")),
            "", "", "",
        ])
    del orl

    # 4b. City of Gainesville Code Violations (2019+)
    gnv = fetch_socrata_paged(
        "data.cityofgainesville.org", "vu9p-a5f7",
        "number,parcel,case_type,address,status,infraction",
        "infraction >= '2019-01-01T00:00:00'",
        "number ASC",
        page_size=50000,
        max_rows=100000,
    )
    for r in gnv:
        rid = s_txt(r.get("number"))
        if not rid:
            continue
        ov_rows.append([
            "fl", "violation", f"GNV-{rid}", s_txt(r.get("parcel")),
            s_txt(r.get("address")), "GAINESVILLE", "",
            s_txt(r.get("status")) or "OPEN", fmt_date(r.get("infraction")),
            "", "", "",
        ])
    del gnv

    # 4c. City of Rochester NBD Code Enforcement
    roch_url = "https://maps.cityofrochester.gov/arcgis/rest/services/NBD/Building_Blocks_Code_Enforcement/MapServer/0/query"
    roch_fields = "OBJECTID,CaseNumber,Status,CaseOpenDate,SBLNumber,StreetNumber,StreetName,Zip,ParcelOwner"
    roch_rows = sharded_arcgis_fetch(
        roch_url, "1=1", roch_fields,
        lambda f: [
            "ny", "violation",
            f"ROCH-{s_txt(f['attributes'].get('CaseNumber')) or f['attributes'].get('OBJECTID')}",
            s_txt(f["attributes"].get("SBLNumber")),
            " ".join(p for p in (s_txt(f["attributes"].get("StreetNumber")), s_txt(f["attributes"].get("StreetName"))) if p),
            "ROCHESTER", clean_zip(f["attributes"].get("Zip")),
            s_txt(f["attributes"].get("Status")) or "OPEN",
            fmt_date(f["attributes"].get("CaseOpenDate")),
            s_txt(f["attributes"].get("ParcelOwner")),
            "", "",
        ],
        oid_field="OBJECTID", page_size=5000, num_shards=4, max_workers=4,
        return_centroid=False, label="NY-ROCHESTER-CODE",
    )
    ov_rows.extend(roch_rows)
    del roch_rows

    write_gz_csv("fl_ny_code_overlays.csv.gz", OVERLAY_HEADER, ov_rows)
    del ov_rows


def main() -> int:
    t0 = time.time()
    log(f"=== SPOT EXPANSION V3 WORKER STARTING (bucket=gs://{BUCKET}/{PREFIX}/) ===")
    summary: Dict[str, Any] = {"started_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "tasks": {}}
    # Run the 4 independent state/overlay pipelines concurrently (separate target gov servers!)
    tasks = [
        ("oh_expansion", crunch_ohio_expansion),
        ("mn_expansion", crunch_minnesota_expansion),
        ("il_lake", crunch_il_lake),
        ("ny_rochester", crunch_ny_rochester),
        ("fl_ny_overlays", crunch_fl_ny_overlays),
    ]
    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = {pool.submit(fn): name for name, fn in tasks}
        for fut in as_completed(futs):
            name = futs[fut]
            try:
                fut.result()
                summary["tasks"][name] = "OK"
                log(f"✓ Task `{name}` finished cleanly!")
            except Exception as exc:
                summary["tasks"][name] = f"ERROR: {exc}"
                log(f"✘ Task `{name}` failed: {exc}")

    summary["elapsed_sec"] = round(time.time() - t0, 1)
    summary["finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    done_path = os.path.join(WORK_DIR, "done.json")
    with open(done_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    upload_to_gcs(done_path, "done.json")
    log(f"=== SPOT EXPANSION V3 FINISHED in {summary['elapsed_sec']}s: {summary['tasks']} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
