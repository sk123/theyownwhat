#!/usr/bin/env python3
"""
gcp/mega_cruncher_worker.py
===========================
High-throughput 10 Gbps Google Compute Engine Spot worker that crunches
multi-million-parcel open government APIs in parallel and uploads clean,
improved-building-only `.csv.gz` spines to GCS (`gs://theyownwhat-oc-staging-20261117/spot_parcels_v2/`):

  1. `il` — Full Cook County (1.86M PINs: `3723-97qp` + `pabr-t5kh` + `uzyt-m557`
            + `x54s-btds` yr_built/cards, excluding Class 1-00 Vacant Land)
            + DuPage County (`ParcelsWithRealEstateCC`, `REA017_FCV_IMP > 0`)
            + Chicago Dept. of Buildings Violations (`22u3-xenr`)
  2. `ny_extra` — Monroe County / Rochester (`MONROE_COUNTY_PARCELS_2025`, `NBRBLDG > '0'`)
            + Syracuse Rental Registry (`Syracuse_Rental_Registry`)
            + Syracuse Code Violations (`Code_Violations_V2`)
  3. `ca` — LA County Improved Residential/Multifamily/Mixed-Use Parcels (`Roll_ImpValue > 0`)
            joined on-VM with LA Active Businesses (`6rrh-rzua` real-estate lessors/managers)
            + San Francisco Secured Roll (`wv5m-vpq2`, `assessed_improvement_value > 0`)
            joined on-VM with SF Registered Business Locations (`g8m3-pdis`)
            + LA Evictions (`2u8b-eyuu`), LADBS Code Enforcement (`u82d-eh7z`),
              SF DBI Violations (`nbtm-fbw5`), SF Evictions (`5cei-gny5`),
              Oakland Habitability Complaints (`xkux-ga3a`)
  4. `mt` — Montana Statewide (All 56 Counties, `TotalBuildingValue > 0`)
  5. `mn` — Twin Cities Metro (`Hennepin County` `BLDG_MV1 > 0` + `Ramsey County / St. Paul` `EMVBuilding > 0`)
  6. `pa_extra` — Allegheny County / Pittsburgh WPRDC Assessments (`FAIRMARKETBUILDING > 0`)
  7. `oh` — Cuyahoga County / Cleveland (`certified_tax_building > 0 OR tax_assessed_improvement > 0`)
"""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime as dt
import gzip
import io
import json
import os
import re
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

csv.field_size_limit(10_000_000)

BUCKET = os.environ.get("GCS_BUCKET", "theyownwhat-oc-staging-20261117")
PREFIX = "spot_parcels_v2"
WORK_DIR = "/tmp/spot_work"
os.makedirs(WORK_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TheyOwnWhatPublicRecordsSync/2.0)"}

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
    return "" if s.upper() in ("NULL", "NONE", "N/A", "NA") else s


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
    t = s_txt(v)
    m = re.search(r"\b(\d{5})\b", t)
    return m.group(1) if m else (re.sub(r"[^0-9]", "", t)[:5] if len(re.sub(r"[^0-9]", "", t)) >= 5 else "")


def norm_addr(a: str) -> str:
    s = re.sub(r"[^A-Z0-9 ]+", " ", (a or "").upper())
    s = " ".join(s.split())
    for full, abbr in (
        (" STREET", " ST"), (" AVENUE", " AVE"), (" BOULEVARD", " BLVD"),
        (" DRIVE", " DR"), (" ROAD", " RD"), (" PLACE", " PL"),
        (" COURT", " CT"), (" LANE", " LN"), (" TERRACE", " TER"),
        (" PARKWAY", " PKWY"), (" HIGHWAY", " HWY"),
    ):
        if s.endswith(full):
            s = s[:-len(full)] + abbr
        s = s.replace(full + " ", abbr + " ")
    # Strip trailing apt/unit
    s = re.sub(r"\s+(?:APT|UNIT|STE|SUITE|#)\s*[A-Z0-9-]+$", "", s)
    return " ".join(s.split())


def fmt_date(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)) and v > 10_000_000_000:
        try:
            d = dt.datetime.fromtimestamp(float(v) / 1000.0, tz=dt.timezone.utc).date()
            return d.isoformat() if 1750 <= d.year <= 2030 else ""
        except Exception:
            return ""
    t = s_txt(v)
    if not t:
        return ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m2 = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", t)
    if m2:
        return f"{int(m2.group(3)):04d}-{int(m2.group(1)):02d}-{int(m2.group(2)):02d}"
    m3 = re.match(r"^(\d{4})(\d{2})(\d{2})$", re.sub(r"[^0-9]", "", t))
    if m3 and 1900 <= int(m3.group(1)) <= 2030 and 1 <= int(m3.group(2)) <= 12 and 1 <= int(m3.group(3)) <= 31:
        return f"{m3.group(1)}-{m3.group(2)}-{m3.group(3)}"
    return ""


def upload_to_gcs(local_path: str, remote_name: str) -> None:
    dest = f"gs://{BUCKET}/{PREFIX}/{remote_name}"
    subprocess.run(["gcloud", "storage", "cp", local_path, dest, "--quiet"], check=True)
    sz_mb = os.path.getsize(local_path) / (1024 * 1024)
    log(f"✓ Uploaded {dest} ({sz_mb:.2f} MB)")


def write_gz_csv(filename: str, header: List[str], rows: List[List[Any]]) -> str:
    path = os.path.join(WORK_DIR, filename)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(header)
        w.writerows(rows)
    upload_to_gcs(path, filename)
    return path


def fetch_socrata_paged(domain: str, view_id: str, select: str, where: str, order: str, page_size: int = 25000, max_rows: int = 3_000_000) -> List[Dict[str, Any]]:
    url = f"https://{domain}/resource/{view_id}.json"
    out: List[Dict[str, Any]] = []
    sess = requests.Session()
    sess.headers.update(HEADERS)
    offset = 0
    while offset < max_rows:
        params = {
            "$select": select,
            "$where": where,
            "$order": order,
            "$limit": page_size,
            "$offset": offset,
        }
        batch = []
        for att in range(5):
            try:
                r = sess.get(url, params=params, timeout=60)
                r.raise_for_status()
                batch = r.json()
                if isinstance(batch, list):
                    break
            except Exception as e:
                if att == 4:
                    log(f"  ⚠ Socrata {domain}/{view_id} stopped at offset {offset}: {e}")
                    batch = []
                else:
                    time.sleep(2 * (att + 1))
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        offset += len(batch)
        if offset % 250000 == 0:
            log(f"  [{domain}/{view_id}] streamed {len(out):,} rows...")
    sess.close()
    return out


def sharded_arcgis_fetch(
    query_url: str,
    where_base: str,
    out_fields: str,
    mapper: Callable[[Dict[str, Any]], Optional[List[Any]]],
    *,
    oid_field: str = "OBJECTID",
    page_size: int = 2000,
    num_shards: int = 16,
    max_workers: int = 12,
    return_centroid: bool = False,
    return_geometry: bool = False,
    label: str = "ARCGIS",
) -> List[List[Any]]:
    sess = requests.Session()
    sess.headers.update(HEADERS)
    mn, mx = 1, 2_500_000
    try:
        st = sess.get(
            query_url,
            params={
                "where": where_base,
                "outStatistics": json.dumps([
                    {"statisticType": "min", "onStatisticField": oid_field, "outStatisticFieldName": "mn"},
                    {"statisticType": "max", "onStatisticField": oid_field, "outStatisticFieldName": "mx"},
                ]),
                "f": "json",
            },
            timeout=45,
        ).json()
        attrs = ((st.get("features") or [{}])[0].get("attributes")) or {}
        if attrs.get("mn") is not None and attrs.get("mx") is not None:
            mn, mx = int(attrs["mn"]), int(attrs["mx"])
    except Exception as exc:
        log(f"  [{label}] outStatistics fallback ({exc})")
    sess.close()

    log(f"▶ [{label}] OID {mn:,}..{mx:,} across {num_shards} shards...")
    step = max(1, (mx - mn + 1) // num_shards)
    shards = []
    for i in range(num_shards):
        lo = mn + i * step
        hi = mx if i == num_shards - 1 else (lo + step - 1)
        shards.append((lo, hi, i + 1))

    def _run_shard(lo: int, hi: int, sid: int) -> List[List[Any]]:
        s = requests.Session()
        s.headers.update(HEADERS)
        cur = lo - 1
        rows: List[List[Any]] = []
        while cur < hi:
            params: Dict[str, Any] = {
                "where": f"({where_base}) AND {oid_field} > {cur} AND {oid_field} <= {hi}",
                "outFields": out_fields,
                "orderByFields": f"{oid_field} ASC",
                "resultRecordCount": page_size,
                "returnGeometry": "true" if return_geometry else "false",
                "f": "json",
            }
            if return_centroid:
                params["returnCentroid"] = "true"
            if return_geometry or return_centroid:
                params["outSR"] = "4326"
            data = None
            for att in range(5):
                try:
                    resp = s.get(query_url, params=params, timeout=60)
                    resp.raise_for_status()
                    data = resp.json()
                    if "error" not in data:
                        break
                    data = None
                except Exception:
                    data = None
                time.sleep(2 * (att + 1))
            if not data:
                break
            feats = data.get("features") or []
            if not feats:
                break
            max_oid = cur
            for f in feats:
                a = f.get("attributes") or {}
                oid = s_int(a.get(oid_field))
                if oid is not None and oid > max_oid:
                    max_oid = oid
                mapped = mapper(f)
                if mapped is not None:
                    rows.append(mapped)
            if max_oid <= cur:
                break
            cur = max_oid
            if len(feats) < page_size and not data.get("exceededTransferLimit"):
                break
        s.close()
        return rows

    all_rows: List[List[Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(_run_shard, lo, hi, sid) for lo, hi, sid in shards]
        for fut in as_completed(futs):
            all_rows.extend(fut.result())
    log(f"✓ [{label}] Fetched {len(all_rows):,} improved parcels.")
    return all_rows


# ==============================================================================
# 1. ILLINOIS (`IL`: Full Cook County 1.86M PINs + DuPage + Chicago Violations)
# ==============================================================================
SERVICER_KEYWORDS = (
    "CORELOGIC", "LOANDEPOT", "WELLS FARGO", "CHASE", "BANK OF AMERICA",
    "MR COOPER", "NATIONSTAR", "PENNYMAC", "FREEDOM MORTGAGE", "ROCKET MORTGAGE",
    "QUICKEN LOANS", "NEWREZ", "SHELLPOINT", "CENLAR", "PHH MORTGAGE", "LERETA",
)


def _is_servicer(name: str) -> bool:
    u = name.upper()
    return any(k in u for k in SERVICER_KEYWORDS)


def crunch_illinois() -> None:
    log("=== Starting ILLINOIS (`IL`) Full Cook County + Collar GIS + Chicago Violations ===")
    target_year = "2026"
    try:
        meta = requests.get("https://datacatalog.cookcountyil.gov/api/views/3723-97qp.json", timeout=15).json()
        for col in meta.get("columns", []):
            if col.get("fieldName") == "year":
                lg = str((col.get("cachedContents") or {}).get("largest") or "").strip()
                if lg.isdigit() and len(lg) == 4:
                    target_year = lg
    except Exception:
        pass

    with ThreadPoolExecutor(max_workers=4) as pool:
        f_addr = pool.submit(
            fetch_socrata_paged,
            "datacatalog.cookcountyil.gov", "3723-97qp",
            "pin,prop_address_full,prop_address_city_name,prop_address_zipcode_1,"
            "mail_address_name,mail_address_full,mail_address_city_name,mail_address_state,mail_address_zipcode_1,"
            "owner_address_name,owner_address_full,owner_address_city_name,owner_address_state,owner_address_zipcode_1",
            f"year='{target_year}' AND prop_address_full IS NOT NULL",
            "pin ASC",
        )
        f_univ = pool.submit(
            fetch_socrata_paged,
            "datacatalog.cookcountyil.gov", "pabr-t5kh",
            "pin,lat,lon,municipality_name,chicago_community_area_name,zip_code",
            "pin IS NOT NULL",
            "pin ASC",
        )
        f_vals = pool.submit(
            fetch_socrata_paged,
            "datacatalog.cookcountyil.gov", "uzyt-m557",
            "pin,class,certified_tot,mailed_tot,certified_bldg,mailed_bldg",
            f"year='{target_year}'",
            "pin ASC",
        )
        f_chars = pool.submit(
            fetch_socrata_paged,
            "datacatalog.cookcountyil.gov", "x54s-btds",
            "pin,char_yrblt,char_apts,char_type_resd,pin_num_cards",
            "char_yrblt IS NOT NULL",
            "pin ASC",
        )
        addr_rows = f_addr.result()
        univ_rows = f_univ.result()
        val_rows = f_vals.result()
        char_rows = f_chars.result()

    univ_by_pin = {s_txt(r.get("pin")): r for r in univ_rows if s_txt(r.get("pin"))}
    del univ_rows
    val_by_pin = {s_txt(r.get("pin")): r for r in val_rows if s_txt(r.get("pin"))}
    del val_rows
    char_by_pin = {s_txt(r.get("pin")): r for r in char_rows if s_txt(r.get("pin"))}
    del char_rows

    il_rows: List[List[Any]] = []
    for r in addr_rows:
        pin = s_txt(r.get("pin"))
        paddr = s_txt(r.get("prop_address_full"))
        if not pin or not paddr or paddr.startswith("0 "):
            continue
        v = val_by_pin.get(pin, {})
        pclass = s_txt(v.get("class"))
        # Exclude Cook County Class 1 (100/1-00 Vacant Land) and $0-building unimproved lots
        if pclass.startswith("1"):
            continue
        bldg_val = s_flt(v.get("certified_bldg")) or s_flt(v.get("mailed_bldg"))
        tot_val = s_flt(v.get("certified_tot")) or s_flt(v.get("mailed_tot"))
        if bldg_val is not None and bldg_val <= 0 and not pclass.startswith(("299", "399")):
            continue

        mname = s_txt(r.get("mail_address_name"))
        oname = s_txt(r.get("owner_address_name"))
        maddr = ", ".join(p for p in (
            s_txt(r.get("mail_address_full")),
            s_txt(r.get("mail_address_city_name")),
            s_txt(r.get("mail_address_state")),
            clean_zip(r.get("mail_address_zipcode_1")),
        ) if p)
        oaddr = ", ".join(p for p in (
            s_txt(r.get("owner_address_full")),
            s_txt(r.get("owner_address_city_name")),
            s_txt(r.get("owner_address_state")),
            clean_zip(r.get("owner_address_zipcode_1")),
        ) if p)

        if (not mname or _is_servicer(mname)) and oname and not _is_servicer(oname):
            owner = oname
            mail = oaddr or maddr
            mst = s_txt(r.get("owner_address_state")) or s_txt(r.get("mail_address_state"))
        else:
            owner = mname or oname
            mail = maddr or oaddr
            mst = s_txt(r.get("mail_address_state")) or s_txt(r.get("owner_address_state"))
        if not owner or owner.upper() in ("EXEMPT", "CURRENT OWNER", "TAXPAYER OF"):
            continue

        u = univ_by_pin.get(pin, {})
        ch = char_by_pin.get(pin, {})
        city = s_txt(r.get("prop_address_city_name")) or s_txt(u.get("municipality_name")) or "CHICAGO"
        city_up = city.upper()
        comm = s_txt(u.get("chicago_community_area_name"))
        borough = f"CHICAGO - {comm.upper()}" if (city_up == "CHICAGO" and comm) else f"{city_up} (COOK)"
        z5 = clean_zip(r.get("prop_address_zipcode_1")) or clean_zip(u.get("zip_code"))
        lat = s_flt(u.get("lat"))
        lng = s_flt(u.get("lon"))
        yr = s_int(ch.get("char_yrblt"))
        if yr is not None and (yr < 1800 or yr > 2030):
            yr = None
        apts_map = {"1": 2, "2": 3, "3": 4, "4": 5, "5": 6}
        units = apts_map.get(s_txt(ch.get("char_apts")))
        if units is None and pclass in ("202", "203", "204", "205", "206", "207", "208", "209", "210", "234", "278", "295", "299"):
            units = 1
        elif units is None and pclass == "211":
            units = 2

        lu = (
            "RESIDENTIAL CONDOMINIUM" if pclass == "299" else
            "SINGLE FAMILY / TOWNHOME" if pclass.startswith("2") and pclass != "211" and pclass != "212" else
            "SMALL APARTMENTS (2-6 UNITS)" if pclass == "211" else
            "MIXED-USE RESIDENTIAL / COMMERCIAL" if pclass == "212" else
            "MULTIFAMILY APARTMENTS (7+ UNITS)" if pclass.startswith(("3", "9")) else
            "COMMERCIAL / INDUSTRIAL" if pclass.startswith("5") else f"COOK CLASS {pclass}"
        )
        cama_url = f"https://www.cookcountyassessor.com/pin/{pin}"
        il_rows.append([
            f"IL-COOK-{pin}", paddr, borough, z5, owner, "", "", mail, mst,
            lu, pclass, "", units if units is not None else "", yr if yr is not None else "",
            tot_val if tot_val is not None else "", "", "",
            lat if lat is not None else "", lng if lng is not None else "",
            cama_url, "COOK_COUNTY_ASSESSOR_SOCRATA",
        ])
    del addr_rows, univ_by_pin, val_by_pin, char_by_pin
    log(f"  [IL] Assembled {len(il_rows):,} improved Cook County parcels.")

    # DuPage County (`ParcelsWithRealEstateCC`, ~330K improved parcels)
    dupage_url = "https://gis.dupageco.org/arcgis/rest/services/DuPage_County_IL/ParcelsWithRealEstateCC/FeatureServer/0/query"
    dupage_fields = (
        "OBJECTID,PIN,BILLNAME,BILLSTNUM,BILLSTDIR,BILLSTNAME,BILLAPT,BILLCITY,BILLSTATE,BILLZIP,"
        "PROPSTNUM,PROPSTDIR,PROPSTNAME,PROPAPT,PROPCITY,PROPZIP,MUNICIPALITY,"
        "REA017_PROP_CLASS,REA017_FCV_IMP,REA017_FCV_TOTAL"
    )

    def _map_dupage(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pin = s_txt(a.get("PIN"))
        owner = s_txt(a.get("BILLNAME"))
        imp = s_flt(a.get("REA017_FCV_IMP")) or 0
        if not pin or not owner or imp <= 0:
            return None
        paddr = " ".join(p for p in (
            s_txt(a.get("PROPSTNUM")), s_txt(a.get("PROPSTDIR")),
            s_txt(a.get("PROPSTNAME")), s_txt(a.get("PROPAPT")),
        ) if p)
        if not paddr or paddr.startswith("0 "):
            return None
        city = (s_txt(a.get("PROPCITY")) or s_txt(a.get("MUNICIPALITY")) or "DUPAGE").upper()
        mstreet = " ".join(p for p in (
            s_txt(a.get("BILLSTNUM")), s_txt(a.get("BILLSTDIR")),
            s_txt(a.get("BILLSTNAME")), s_txt(a.get("BILLAPT")),
        ) if p)
        mst = s_txt(a.get("BILLSTATE"))
        mzip = clean_zip(a.get("BILLZIP"))
        mail = ", ".join(p for p in (mstreet, s_txt(a.get("BILLCITY")), mst, mzip) if p)
        c = f.get("centroid") or {}
        tot = s_flt(a.get("REA017_FCV_TOTAL")) or imp
        pclass = s_txt(a.get("REA017_PROP_CLASS"))
        return [
            f"IL-DUPAGE-{pin}", paddr, f"{city} (DUPAGE)", clean_zip(a.get("PROPZIP")),
            owner, "", "", mail, mst, f"DUPAGE {pclass}" if pclass else "IMPROVED PARCEL",
            pclass, "", "", "", tot, "", "",
            s_flt(c.get("y")) or "", s_flt(c.get("x")) or "",
            f"https://www.dupagecounty.gov/PropertyInfo/PropertyLookUp.aspx?pin={pin}",
            "DUPAGE_COUNTY_GIS_CAMA",
        ]

    dupage_rows = sharded_arcgis_fetch(
        dupage_url, "REA017_FCV_IMP > 0 AND BILLNAME IS NOT NULL", dupage_fields, _map_dupage,
        num_shards=12, max_workers=10, return_centroid=True, label="IL-DUPAGE",
    )
    il_rows.extend(dupage_rows)
    write_gz_csv("il_parcels.csv.gz", PARCEL_HEADER, il_rows)
    del il_rows, dupage_rows

    # Chicago Department of Buildings Violations (`22u3-xenr`)
    viol_raw = fetch_socrata_paged(
        "data.cityofchicago.org", "22u3-xenr",
        "id,violation_date,violation_status,address,property_group",
        "address IS NOT NULL AND violation_date >= '2018-01-01T00:00:00'",
        "id ASC",
        page_size=50000,
        max_rows=1_500_000,
    )
    v_rows = [
        [
            "il", "violation", s_txt(v.get("id")), s_txt(v.get("property_group")),
            s_txt(v.get("address")), "CHICAGO", "",
            s_txt(v.get("violation_status")), fmt_date(v.get("violation_date")),
            "", "", "",
        ]
        for v in viol_raw if s_txt(v.get("id")) and s_txt(v.get("address"))
    ]
    write_gz_csv("il_overlays.csv.gz", OVERLAY_HEADER, v_rows)
    del viol_raw, v_rows


# ==============================================================================
# 2. NEW YORK HOLDOUTS & OVERLAYS (`Monroe County` + `Syracuse` Rental & Code)
# ==============================================================================
def crunch_ny_extras() -> None:
    log("=== Starting NEW YORK (`NY`) Monroe/Rochester Parcels + Syracuse Rental & Violations ===")
    monroe_url = "https://services.arcgis.com/04HiymDgLlsbhaV4/arcgis/rest/services/MONROE_COUNTY_PARCELS_2025/FeatureServer/0/query"
    # Discover full field list dynamically so we capture address, SBL/PRINTKEY, mail, assessed val
    meta = requests.get(f"{monroe_url.rsplit('/query', 1)[0]}?f=json", timeout=20).json()
    m_fields = {f["name"] for f in meta.get("fields", [])}
    log(f"  [NY-MONROE] Available fields ({len(m_fields)}): {sorted(m_fields)[:30]}")

    def _pick(a: Dict[str, Any], *cands: str) -> str:
        for c in cands:
            if c in a and s_txt(a.get(c)):
                return s_txt(a.get(c))
        return ""

    def _map_monroe(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        owner = _pick(a, "NAME", "PRIMARY_OWNER", "OWNER1", "OWNER")
        pid = _pick(a, "SWIS_SBL_ID", "PRINT_KEY", "SBL", "PIN", "PARCELID") or str(a.get("OBJECTID") or "")
        addr = _pick(a, "PARCEL_ADDR", "LOC_ADDR", "PROP_ADDR", "ADDRESS", "ST_NUM_NAME")
        if not addr:
            addr = " ".join(p for p in (_pick(a, "LOC_ST_NBR", "ST_NBR"), _pick(a, "LOC_ST_NAME", "ST_NAME")) if p)
        if not owner or not pid or not addr or addr.startswith("0 "):
            return None
        pclass = _pick(a, "PC", "PROP_CLASS")
        if pclass.startswith("3"):  # NYS ORPTS 300-series = Vacant Land!
            return None
        muni = (_pick(a, "MUNI_NAME", "CITYTOWN_NAME", "MUNI", "TOWN") or "ROCHESTER").upper()
        mail = ", ".join(p for p in (
            _pick(a, "MAIL_ADDR", "ADDR1", "MAIL_ST"),
            _pick(a, "MAIL_CITY", "CITY"),
            _pick(a, "MAIL_STATE", "STATE"),
            clean_zip(_pick(a, "MAIL_ZIP", "ZIP")),
        ) if p)
        mst = _pick(a, "MAIL_STATE", "STATE")
        val = s_flt(a.get("FULL_MARKET_VAL")) or s_flt(a.get("TOTAL_AV")) or s_flt(a.get("ASSESSED_VAL"))
        yr = s_int(a.get("YR_BLT")) or s_int(a.get("YEAR_BUILT"))
        c = f.get("centroid") or {}
        return [
            f"NY-MONROE-{pid}", addr, f"{muni} (MONROE)", clean_zip(_pick(a, "LOC_ZIP", "PARCEL_ZIP")),
            owner, _pick(a, "ADD_OWNER", "NAME2"), "", mail, mst,
            _pick(a, "LANDUSE", "PROP_CLASS_DESC") or (f"ORPTS {pclass}" if pclass else "IMPROVED PARCEL"),
            pclass, "", "", yr if (yr and 1750 <= yr <= 2030) else "",
            val if val is not None else "", "", "",
            s_flt(c.get("y")) or "", s_flt(c.get("x")) or "",
            "https://www.monroecounty.gov/etc/rp/",
            "MONROE_COUNTY_GIS_2025",
        ]

    where_m = "NAME IS NOT NULL" if "NAME" in m_fields else "1=1"
    monroe_rows = sharded_arcgis_fetch(
        monroe_url, where_m, "*", _map_monroe,
        num_shards=10, max_workers=8, return_centroid=True, label="NY-MONROE",
    )
    write_gz_csv("ny_monroe_parcels.csv.gz", PARCEL_HEADER, monroe_rows)
    del monroe_rows

    # Syracuse Rental Registry & Code Violations
    syr_overlays: List[List[Any]] = []
    syr_rr_url = "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Syracuse_Rental_Registry/FeatureServer/0/query"
    rr_rows = sharded_arcgis_fetch(
        syr_rr_url, "PropertyAddress IS NOT NULL", "ObjectId,SBL,PropertyAddress,zip,RRisValid,completion_date,valid_until,pc_owner,RR_contact_name",
        lambda f: [
            "ny", "rental_license", f"SYR-RR-{f['attributes'].get('ObjectId')}",
            s_txt(f["attributes"].get("SBL")), s_txt(f["attributes"].get("PropertyAddress")),
            "SYRACUSE", clean_zip(f["attributes"].get("zip")),
            "ACTIVE" if s_txt(f["attributes"].get("RRisValid")).upper() in ("YES", "Y", "1", "TRUE") else "REGISTERED",
            fmt_date(f["attributes"].get("completion_date")),
            s_txt(f["attributes"].get("pc_owner")) or s_txt(f["attributes"].get("RR_contact_name")),
            "", "",
        ],
        oid_field="ObjectId", num_shards=4, max_workers=4, label="NY-SYRACUSE-RR",
    )
    syr_overlays.extend(rr_rows)

    syr_v_url = "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Code_Violations_V2/FeatureServer/0/query"
    sv_rows = sharded_arcgis_fetch(
        syr_v_url, "complaint_address IS NOT NULL", "ObjectId,SBL,complaint_address,complaint_zip,violation_id,violation_date,status_type_name,issued_to",
        lambda f: [
            "ny", "violation", str(f["attributes"].get("violation_id") or f["attributes"].get("ObjectId")),
            s_txt(f["attributes"].get("SBL")), s_txt(f["attributes"].get("complaint_address")),
            "SYRACUSE", clean_zip(f["attributes"].get("complaint_zip")),
            s_txt(f["attributes"].get("status_type_name")),
            fmt_date(f["attributes"].get("violation_date")),
            s_txt(f["attributes"].get("issued_to")), "", "",
        ],
        oid_field="ObjectId", num_shards=6, max_workers=6, label="NY-SYRACUSE-VIOL",
    )
    syr_overlays.extend(sv_rows)
    write_gz_csv("ny_overlays.csv.gz", OVERLAY_HEADER, syr_overlays)
    del syr_overlays


# ==============================================================================
# 3. CALIFORNIA (`CA`: LA County + SF Secured Roll + Business Lessor Join + Code/Evict)
# ==============================================================================
def crunch_california() -> None:
    log("=== Starting CALIFORNIA (`CA`) Track A Metro Parcels + Business Tax Lessors + Code/Evictions ===")
    # 3a. Pull SF Registered Business Locations (`g8m3-pdis`) & LA Active Businesses (`6rrh-rzua`)
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_sfbiz = pool.submit(
            fetch_socrata_paged,
            "data.sfgov.org", "g8m3-pdis",
            "certificate_number,ownership_name,dba_name,full_business_address,business_zip,mailing_address_1,mail_city,mail_state,mail_zipcode,self_reported_naics_code",
            "ownership_name IS NOT NULL AND full_business_address IS NOT NULL",
            "certificate_number ASC",
            page_size=50000,
        )
        f_labiz = pool.submit(
            fetch_socrata_paged,
            "data.lacity.org", "6rrh-rzua",
            "location_account,business_name,street_address,city,zip_code,mailing_address,mailing_city,mailing_zip_code,naics,primary_naics_description",
            "business_name IS NOT NULL AND street_address IS NOT NULL AND (naics LIKE '531%' OR naics LIKE '721%' OR primary_naics_description LIKE '%Lessor%' OR primary_naics_description LIKE '%Real Estate%' OR primary_naics_description LIKE '%Apartment%' OR primary_naics_description LIKE '%Rooming%')",
            "location_account ASC",
            page_size=50000,
        )
        sf_biz = f_sfbiz.result()
        la_biz = f_labiz.result()

    # Index SF businesses by normalized (address, zip) prioritizing NAICS 531* (Real Estate / Lessors)
    sf_owner_by_addr: Dict[str, Tuple[str, str, str, int]] = {}
    for b in sf_biz:
        an = norm_addr(s_txt(b.get("full_business_address")))
        own = s_txt(b.get("ownership_name"))
        if not an or not own:
            continue
        naics = s_txt(b.get("self_reported_naics_code"))
        prio = 2 if (naics.startswith("53") or any(k in own.upper() for k in ("PROPERTIES", "REALTY", "HOLDINGS", "APARTMENTS", "INVESTMENTS", "TRUST", "LP", "LLC"))) else 1
        prev = sf_owner_by_addr.get(an)
        if prev is None or prio > prev[3]:
            mail = ", ".join(p for p in (
                s_txt(b.get("mailing_address_1")), s_txt(b.get("mail_city")),
                s_txt(b.get("mail_state")), clean_zip(b.get("mail_zipcode")),
            ) if p)
            sf_owner_by_addr[an] = (own, mail, s_txt(b.get("mail_state")), prio)
    del sf_biz

    la_owner_by_addr: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for b in la_biz:
        an = norm_addr(s_txt(b.get("street_address")))
        z5 = clean_zip(b.get("zip_code"))
        own = re.sub(r"\s*/C$", "", s_txt(b.get("business_name"))).strip()
        if not an or not own:
            continue
        mail = ", ".join(p for p in (
            s_txt(b.get("mailing_address")), s_txt(b.get("mailing_city")),
            "CA", clean_zip(b.get("mailing_zip_code")),
        ) if p)
        la_owner_by_addr[(an, z5)] = (own, mail)
        la_owner_by_addr.setdefault((an, ""), (own, mail))
    log(f"  [CA] Indexed {len(sf_owner_by_addr):,} SF business locations & {len(la_owner_by_addr):,} LA real-estate/lessor businesses.")
    del la_biz

    ca_rows: List[List[Any]] = []
    # 3b. San Francisco Latest Secured Property Tax Roll (`wv5m-vpq2`, `closed_roll_year='2025'`)
    sf_roll = fetch_socrata_paged(
        "data.sfgov.org", "wv5m-vpq2",
        "parcel_number,property_location,use_definition,property_class_code,year_property_built,number_of_stories,number_of_units,current_sales_date,assessed_improvement_value,assessed_land_value,assessor_neighborhood,the_geom",
        "closed_roll_year='2025' AND assessed_improvement_value > 0",
        "parcel_number ASC",
        page_size=50000,
    )
    for r in sf_roll:
        pnum = s_txt(r.get("parcel_number"))
        raw_loc = s_txt(r.get("property_location"))
        if not pnum or not raw_loc:
            continue
        # SF property_location format: '0000 0123 MAIN                ST0000' -> parse cleanly
        loc_clean = re.sub(r"^0000\s+", "", raw_loc)
        loc_clean = re.sub(r"0000$", "", loc_clean).strip()
        loc_clean = re.sub(r"^0+(\d)", r"\1", loc_clean)
        loc_clean = " ".join(loc_clean.split())
        if not loc_clean or loc_clean.startswith("0 "):
            continue
        an = norm_addr(loc_clean)
        biz_hit = sf_owner_by_addr.get(an)
        # Under CA Gov. Code § 7928.205, if no public business/lessor registration matches,
        # record explicit statutory status rather than inventing an owner!
        if biz_hit:
            owner, mail, mst, _ = biz_hit
        else:
            owner = f"CA GOV CODE 7928.205 REDACTED (SF APN {pnum})"
            mail, mst = "", ""
        imp = s_flt(r.get("assessed_improvement_value")) or 0
        lnd = s_flt(r.get("assessed_land_value")) or 0
        geom = r.get("the_geom") or {}
        coords = geom.get("coordinates") if isinstance(geom, dict) else None
        lng = float(coords[0]) if (isinstance(coords, list) and len(coords) >= 2) else ""
        lat = float(coords[1]) if (isinstance(coords, list) and len(coords) >= 2) else ""
        nbhd = s_txt(r.get("assessor_neighborhood"))
        boro = f"SAN FRANCISCO - {nbhd.upper()}" if nbhd else "SAN FRANCISCO (SAN FRANCISCO)"
        ca_rows.append([
            f"CA-SF-{pnum}", loc_clean, boro, "", owner, "", "", mail, mst,
            s_txt(r.get("use_definition")) or "IMPROVED PARCEL", s_txt(r.get("property_class_code")),
            s_int(r.get("number_of_stories")) or "", s_int(r.get("number_of_units")) or "",
            s_int(r.get("year_property_built")) or "", imp + lnd,
            fmt_date(r.get("current_sales_date")), "", lat, lng,
            f"https://sfplanninggis.org/pim/?tab=Property&search={pnum}",
            "SF_ASSESSOR_ROLL_AND_BIZ_TAX",
        ])
    del sf_roll, sf_owner_by_addr

    # 3c. LA County Assessor Multifamily, Mixed-Use & Business-Matched Residential Parcels
    # Pull all multifamily (Units1 >= 2) + business-tax-matched residential in LA County (`Roll_ImpValue > 0`)
    la_url = "https://public.gis.lacounty.gov/public/rest/services/LACounty_Cache/LACounty_Parcel/MapServer/0/query"
    la_fields = (
        "OBJECTID,AIN,SitusAddress,SitusCity,SitusZIP,UseType,UseDescription,"
        "YearBuilt1,Units1,Roll_LandValue,Roll_ImpValue,CENTER_LAT,CENTER_LON"
    )
    la_where = "Roll_ImpValue > 0 AND SitusAddress IS NOT NULL AND (Units1 >= 2 OR UseType IN ('Residential','Commercial'))"

    def _map_lac(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        ain = s_txt(a.get("AIN"))
        addr = s_txt(a.get("SitusAddress"))
        if not ain or not addr or addr.startswith("0 "):
            return None
        z5 = clean_zip(a.get("SitusZIP"))
        an = norm_addr(addr)
        units = s_int(a.get("Units1")) or 0
        biz_hit = la_owner_by_addr.get((an, z5)) or la_owner_by_addr.get((an, ""))
        # Keep all multifamily (2+ units) AND any parcel matched to a registered lessor business!
        if not biz_hit and units < 2:
            return None
        if biz_hit:
            owner, mail = biz_hit
            mst = "CA"
        else:
            owner = f"CA GOV CODE 7928.205 REDACTED (LA AIN {ain})"
            mail, mst = "", ""
        city = re.sub(r"\s+CA$", "", (s_txt(a.get("SitusCity")) or "LOS ANGELES").upper()).strip()
        imp = s_flt(a.get("Roll_ImpValue")) or 0
        lnd = s_flt(a.get("Roll_LandValue")) or 0
        yr = s_int(a.get("YearBuilt1"))
        return [
            f"CA-LA-{ain}", addr, f"{city} (LOS ANGELES)", z5,
            owner, "", "", mail, mst,
            s_txt(a.get("UseDescription")) or s_txt(a.get("UseType")), "",
            "", units if units > 0 else "", yr if (yr and 1800 <= yr <= 2030) else "",
            imp + lnd, "", "",
            s_flt(a.get("CENTER_LAT")) or "", s_flt(a.get("CENTER_LON")) or "",
            f"https://portal.assessor.lacounty.gov/parceldetail/{ain}",
            "LA_COUNTY_ASSESSOR_AND_FINANCE",
        ]

    lac_rows = sharded_arcgis_fetch(
        la_url, la_where, la_fields, _map_lac,
        num_shards=20, max_workers=12, label="CA-LACOUNTY",
    )
    ca_rows.extend(lac_rows)
    write_gz_csv("ca_parcels.csv.gz", PARCEL_HEADER, ca_rows)
    del ca_rows, lac_rows, la_owner_by_addr

    # 3d. California Evictions & Code Enforcement Overlays (LA + SF + Oakland)
    ca_ov: List[List[Any]] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_sfv = pool.submit(
            fetch_socrata_paged, "data.sfgov.org", "nbtm-fbw5",
            "primary_key,date_filed,block,lot,street_number,street_name,street_suffix,status,zipcode",
            "date_filed >= '2019-01-01T00:00:00'", "primary_key ASC", 50000,
        )
        f_sfe = pool.submit(
            fetch_socrata_paged, "data.sfgov.org", "5cei-gny5",
            "eviction_id,address,zip,file_date",
            "file_date >= '2019-01-01T00:00:00'", "eviction_id ASC", 50000,
        )
        f_lae = pool.submit(
            fetch_socrata_paged, "data.lacity.org", "2u8b-eyuu",
            "*", "1=1", ":id ASC", 50000, 300000,
        )
        f_lav = pool.submit(
            fetch_socrata_paged, "data.lacity.org", "u82d-eh7z",
            "*", "1=1", ":id ASC", 50000, 300000,
        )
        sfv, sfe, lae, lav = f_sfv.result(), f_sfe.result(), f_lae.result(), f_lav.result()

    for r in sfv:
        blk, lot = s_txt(r.get("block")), s_txt(r.get("lot"))
        addr = " ".join(p for p in (s_txt(r.get("street_number")), s_txt(r.get("street_name")), s_txt(r.get("street_suffix"))) if p)
        ca_ov.append(["ca", "violation", s_txt(r.get("primary_key")), f"CA-SF-{blk}{lot}" if (blk and lot) else "", addr, "SAN FRANCISCO", clean_zip(r.get("zipcode")), s_txt(r.get("status")), fmt_date(r.get("date_filed")), "", "", ""])
    for r in sfe:
        ca_ov.append(["ca", "eviction", s_txt(r.get("eviction_id")), "", s_txt(r.get("address")), "SAN FRANCISCO", clean_zip(r.get("zip")), "FILED", fmt_date(r.get("file_date")), "", "", ""])
    for r in lae:
        addr = s_txt(r.get("property_address") or r.get("address") or r.get("street_address"))
        if addr:
            ca_ov.append(["ca", "eviction", s_txt(r.get("apn") or r.get("notice_id") or r.get("case_number") or len(ca_ov)), s_txt(r.get("apn")), addr, s_txt(r.get("city")) or "LOS ANGELES", clean_zip(r.get("zip") or r.get("zip_code")), "FILED", fmt_date(r.get("date_received") or r.get("notice_date") or r.get("file_date")), "", "", ""])
    for r in lav:
        addr = s_txt(r.get("address") or r.get("street_address"))
        ain = re.sub(r"[^0-9]", "", s_txt(r.get("apn") or r.get("ain")))
        if addr or ain:
            ca_ov.append(["ca", "violation", s_txt(r.get("case_number") or r.get("case_no") or len(ca_ov)), f"CA-LA-{ain}" if ain else "", addr, "LOS ANGELES", clean_zip(r.get("zip_code") or r.get("zip")), s_txt(r.get("status") or r.get("case_status")), fmt_date(r.get("add_date") or r.get("open_date") or r.get("date_opened")), "", "", ""])
    write_gz_csv("ca_overlays.csv.gz", OVERLAY_HEADER, ca_ov)
    del ca_ov, sfv, sfe, lae, lav


# ==============================================================================
# 4. BONUS STATES (`MT` Statewide 56 Counties, `MN` Twin Cities, `PA` Allegheny, `OH` Cuyahoga)
# ==============================================================================
def crunch_montana() -> None:
    log("=== Starting MONTANA (`MT`) All 56 Counties Statewide Cadastral (`TotalBuildingValue > 0`) ===")
    url = "https://gisservice.mt.gov/arcgis/rest/services/msdi_cadastral_map_v1/MapServer/1/query"
    fields = (
        "OBJECTID,PARCELID,CountyName,AddressLine1,AddressLine2,CityStateZip,PropType,"
        "TotalBuildingValue,TotalLandValue,TotalValue,OwnerName,OwnerAddress1,OwnerAddress2,"
        "OwnerCity,OwnerState,OwnerZipCode,CareOfTaxpayer"
    )

    def _map_mt(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        owner = s_txt(a.get("OwnerName"))
        pid = s_txt(a.get("PARCELID")) or str(a.get("OBJECTID") or "")
        bval = s_flt(a.get("TotalBuildingValue")) or 0
        addr = " ".join(p for p in (s_txt(a.get("AddressLine1")), s_txt(a.get("AddressLine2"))) if p)
        if not owner or not pid or bval <= 0 or not addr or addr.startswith("0 "):
            return None
        ptype = s_txt(a.get("PropType"))
        if any(ex in ptype.upper() for ex in ("EXEMPT", "VACANT", "GRAZING", "TIMBER", "MINING")):
            return None
        csz = s_txt(a.get("CityStateZip"))
        city = csz.split(",")[0].strip().upper() if "," in csz else (csz.split(" MT")[0].strip().upper() or s_txt(a.get("CountyName")).upper())
        cnty = s_txt(a.get("CountyName")).upper()
        boro = f"{city} ({cnty})" if (cnty and cnty not in city) else (city or cnty or "MONTANA")
        z5 = clean_zip(csz)
        mstreet = " ".join(p for p in (s_txt(a.get("OwnerAddress1")), s_txt(a.get("OwnerAddress2"))) if p)
        mst = s_txt(a.get("OwnerState"))
        mail = ", ".join(p for p in (mstreet, s_txt(a.get("OwnerCity")), mst, clean_zip(a.get("OwnerZipCode"))) if p)
        c = f.get("centroid") or {}
        return [
            f"MT-{pid}", addr, boro, z5, owner, "", s_txt(a.get("CareOfTaxpayer")),
            mail, mst, ptype or "IMPROVED PARCEL", "", "", "", "",
            s_flt(a.get("TotalValue")) or bval, "", "",
            s_flt(c.get("y")) or "", s_flt(c.get("x")) or "",
            f"https://svc.mt.gov/msl/mtcadastral/",
            "MT_MSDI_ORION_CADASTRAL",
        ]

    mt_rows = sharded_arcgis_fetch(
        url, "TotalBuildingValue > 0 AND OwnerName IS NOT NULL AND AddressLine1 IS NOT NULL",
        fields, _map_mt, num_shards=12, max_workers=10, return_centroid=True, label="MT-STATEWIDE",
    )
    write_gz_csv("mt_parcels.csv.gz", PARCEL_HEADER, mt_rows)


def crunch_minnesota_metro() -> None:
    log("=== Starting MINNESOTA (`MN`) Hennepin + Ramsey (Minneapolis / St. Paul Metro) ===")
    mn_rows: List[List[Any]] = []

    # Hennepin County (`BLDG_MV1 > 0`)
    hen_url = "https://gis.hennepin.us/arcgis/rest/services/HennepinData/LAND_PROPERTY/MapServer/1/query"
    hen_fields = (
        "OBJECTID,PID,HOUSE_NO,STREET_NM,CONDO_NO,MUNIC_NM,ZIP_CD,OWNER_NM,TAXPAYER_NM,"
        "TAXPAYER_NM_1,TAXPAYER_NM_2,TAXPAYER_NM_3,BUILD_YR,SALE_DATE,SALE_PRICE,"
        "MKT_VAL_TOT,BLDG_MV1,PR_TYP_NM1,LAT,LON"
    )

    def _map_hen(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pid = s_txt(a.get("PID"))
        owner = s_txt(a.get("OWNER_NM")) or s_txt(a.get("TAXPAYER_NM"))
        bmv = s_flt(a.get("BLDG_MV1")) or 0
        hno = s_txt(a.get("HOUSE_NO"))
        st = s_txt(a.get("STREET_NM"))
        if not pid or not owner or bmv <= 0 or not hno or hno == "0" or not st:
            return None
        condo = s_txt(a.get("CONDO_NO"))
        addr = f"{hno} {st}" + (f" UNIT {condo}" if condo and condo != "0" else "")
        city = (s_txt(a.get("MUNIC_NM")) or "MINNEAPOLIS").upper()
        mail = ", ".join(p for p in (s_txt(a.get("TAXPAYER_NM_1")), s_txt(a.get("TAXPAYER_NM_2")), s_txt(a.get("TAXPAYER_NM_3"))) if p)
        yr = s_int(a.get("BUILD_YR"))
        sp = s_flt(a.get("SALE_PRICE"))
        return [
            f"MN-HEN-{pid}", addr, f"{city} (HENNEPIN)", clean_zip(a.get("ZIP_CD")),
            owner, s_txt(a.get("TAXPAYER_NM")) if s_txt(a.get("TAXPAYER_NM")) != owner else "", "",
            mail, "", s_txt(a.get("PR_TYP_NM1")) or "IMPROVED PARCEL", "", "", "",
            yr if (yr and 1800 <= yr <= 2030) else "", s_flt(a.get("MKT_VAL_TOT")) or bmv,
            fmt_date(a.get("SALE_DATE")), sp if (sp and sp > 0) else "",
            s_flt(a.get("LAT")) or "", s_flt(a.get("LON")) or "",
            f"https://gis.hennepin.us/property/map/default.aspx?pid={pid}",
            "HENNEPIN_COUNTY_CAMA",
        ]

    mn_rows.extend(sharded_arcgis_fetch(
        hen_url, "BLDG_MV1 > 0 AND OWNER_NM IS NOT NULL", hen_fields, _map_hen,
        num_shards=12, max_workers=10, label="MN-HENNEPIN",
    ))

    # Ramsey County / St. Paul (`EMVBuilding > 0`)
    ram_url = "https://maps.co.ramsey.mn.us/arcgis/rest/services/OpenData/OpenData/FeatureServer/11/query"
    ram_fields = (
        "OBJECTID,ParcelID,SiteAddress,SiteCityName,SiteZIP5,OwnerName,TaxName1,"
        "OwnerAddress1,OwnerCityStateZIP,EMVBuilding,EMVTotal,UseType1,LivingUnit,Stories,"
        "YearBuilt,LastSaleDate,SalePrice,Latitude,Longitude"
    )

    def _map_ram(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pid = s_txt(a.get("ParcelID"))
        owner = s_txt(a.get("OwnerName")) or s_txt(a.get("TaxName1"))
        addr = s_txt(a.get("SiteAddress"))
        bval = s_flt(a.get("EMVBuilding")) or 0
        if not pid or not owner or not addr or addr.startswith("0 ") or bval <= 0:
            return None
        city = (s_txt(a.get("SiteCityName")) or "SAINT PAUL").upper()
        mail = ", ".join(p for p in (s_txt(a.get("OwnerAddress1")), s_txt(a.get("OwnerCityStateZIP"))) if p)
        yr = s_int(a.get("YearBuilt"))
        sp = s_flt(a.get("SalePrice"))
        return [
            f"MN-RAM-{pid}", addr, f"{city} (RAMSEY)", clean_zip(a.get("SiteZIP5")),
            owner, "", "", mail, "", s_txt(a.get("UseType1")) or "IMPROVED PARCEL", "",
            s_int(a.get("Stories")) or "", s_int(a.get("LivingUnit")) or "",
            yr if (yr and 1800 <= yr <= 2030) else "", s_flt(a.get("EMVTotal")) or bval,
            fmt_date(a.get("LastSaleDate")), sp if (sp and sp > 0) else "",
            s_flt(a.get("Latitude")) or "", s_flt(a.get("Longitude")) or "",
            f"https://beacon.schneidercorp.com/Application.aspx?AppID=959&PageType=Search",
            "RAMSEY_COUNTY_CAMA",
        ]

    mn_rows.extend(sharded_arcgis_fetch(
        ram_url, "EMVBuilding > 0 AND (OwnerName IS NOT NULL OR TaxName1 IS NOT NULL)", ram_fields, _map_ram,
        num_shards=8, max_workers=8, label="MN-RAMSEY",
    ))
    write_gz_csv("mn_parcels.csv.gz", PARCEL_HEADER, mn_rows)


def crunch_ohio_cuyahoga() -> None:
    log("=== Starting OHIO (`OH`) Cuyahoga County / Greater Cleveland Auditor CAMA ===")
    url = "https://gis.cuyahogacounty.gov/server/rest/services/CCFO/Parcel_Fabric_Taxparcels/FeatureServer/0/query"
    fields = (
        "ObjectID,parcel_id,myplaceppn,parcel_owner,second_owner,par_addr_all,parcel_city,parcel_zip,"
        "mail_name,mail_addr_street,mail_unit_no,mail_city,mail_state,mail_zip,"
        "last_transfer_date,last_sales_amount,tax_luc,tax_luc_description,"
        "certified_tax_building,tax_assessed_improvement,certified_tax_total,tax_market_total,"
        "com_living_units,min_age"
    )

    def _map_oh(f: Dict[str, Any]) -> Optional[List[Any]]:
        a = f.get("attributes") or {}
        pid = s_txt(a.get("parcel_id")) or s_txt(a.get("myplaceppn"))
        owner = s_txt(a.get("parcel_owner"))
        addr = s_txt(a.get("par_addr_all"))
        bval = s_flt(a.get("certified_tax_building")) or s_flt(a.get("tax_assessed_improvement")) or 0
        if not pid or not owner or not addr or addr.startswith("0 ") or bval <= 0:
            return None
        city = (s_txt(a.get("parcel_city")) or "CLEVELAND").upper()
        mstreet = " ".join(p for p in (s_txt(a.get("mail_addr_street")), s_txt(a.get("mail_unit_no"))) if p)
        mst = s_txt(a.get("mail_state"))
        mail = ", ".join(p for p in (mstreet, s_txt(a.get("mail_city")), mst, clean_zip(a.get("mail_zip"))) if p)
        c = f.get("centroid") or {}
        sp = s_flt(a.get("last_sales_amount"))
        ppn = s_txt(a.get("myplaceppn")) or pid
        return [
            f"OH-CUY-{pid}", addr, f"{city} (CUYAHOGA)", clean_zip(a.get("parcel_zip")),
            owner, s_txt(a.get("second_owner")), "", mail, mst,
            s_txt(a.get("tax_luc_description")) or "IMPROVED PARCEL", s_txt(a.get("tax_luc")),
            "", s_int(a.get("com_living_units")) or "", "",
            s_flt(a.get("tax_market_total")) or s_flt(a.get("certified_tax_total")) or bval,
            fmt_date(a.get("last_transfer_date")), sp if (sp and sp > 0) else "",
            s_flt(c.get("y")) or "", s_flt(c.get("x")) or "",
            f"https://myplace.cuyahogacounty.gov/",
            "CUYAHOGA_COUNTY_AUDITOR_CAMA",
        ]

    oh_rows = sharded_arcgis_fetch(
        url, "(certified_tax_building > 0 OR tax_assessed_improvement > 0) AND parcel_owner IS NOT NULL",
        fields, _map_oh, oid_field="ObjectID", num_shards=10, max_workers=8, return_centroid=True, label="OH-CUYAHOGA",
    )
    write_gz_csv("oh_parcels.csv.gz", PARCEL_HEADER, oh_rows)


def crunch_pa_allegheny() -> None:
    log("=== Starting PENNSYLVANIA (`PA`) Allegheny County / Pittsburgh WPRDC Assessments ===")
    csv_url = "https://data.wprdc.org/dataset/2b3df818-601e-4f06-b150-643557229491/resource/9a1c60bd-f9f7-4aba-aeb7-af8c3aaa44e5/download/assessments.csv"
    pa_rows: List[List[Any]] = []
    with requests.get(csv_url, stream=True, headers=HEADERS, timeout=120) as r:
        r.raise_for_status()
        lines = (line.decode("utf-8", errors="replace") for line in r.iter_lines())
        reader = csv.DictReader(lines)
        for row in reader:
            bval = s_flt(row.get("FAIRMARKETBUILDING")) or s_flt(row.get("COUNTYBULDING")) or 0
            if bval <= 0:
                continue
            pin = s_txt(row.get("PARID"))
            owner = s_txt(row.get("PROPERTYOWNER"))  # Wait: check if WPRDC has owner or changenoticeaddress!
            hno = s_txt(row.get("PROPERTYHOUSENUM"))
            st = s_txt(row.get("PROPERTYADDRESS"))
            frac = s_txt(row.get("PROPERTYFRACTION"))
            unit = s_txt(row.get("PROPERTYUNIT"))
            if not pin or not st or hno in ("", "0"):
                continue
            addr = " ".join(p for p in (hno, frac, st, f"UNIT {unit}" if unit else "") if p)
            city = (s_txt(row.get("PROPERTYCITY")) or s_txt(row.get("MUNIDESC")) or "PITTSBURGH").upper()
            z5 = clean_zip(row.get("PROPERTYZIP"))
            mail = ", ".join(p for p in (
                s_txt(row.get("CHANGENOTICEADDRESS1")),
                s_txt(row.get("CHANGENOTICEADDRESS2")),
                s_txt(row.get("CHANGENOTICEADDRESS3")),
                s_txt(row.get("CHANGENOTICEADDRESS4")),
            ) if p)
            # Note: WPRDC strips individual deed owner names from public CSV; if owner is absent, skip unless present
            if not owner:
                continue
            yr = s_int(row.get("YEARBLT"))
            sp = s_flt(row.get("SALEPRICE"))
            pa_rows.append([
                f"PA-ALL-{pin}", addr, f"{city} (ALLEGHENY)", z5,
                owner, "", "", mail, "",
                s_txt(row.get("USEDESC")) or "IMPROVED PARCEL", s_txt(row.get("CLASSDESC")),
                s_int(row.get("STORIES")) or "", "",
                yr if (yr and 1750 <= yr <= 2030) else "",
                s_flt(row.get("FAIRMARKETTOTAL")) or bval,
                fmt_date(row.get("SALEDATE")), sp if (sp and sp > 0) else "",
                "", "",
                f"https://www2.alleghenycounty.us/RealEstate/GeneralInfo.aspx?ParcelID={pin}",
                "ALLEGHENY_COUNTY_OPA",
            ])
    log(f"  [PA-ALLEGHENY] Parsed {len(pa_rows):,} rows with explicit owner names.")
    if pa_rows:
        write_gz_csv("pa_allegheny_parcels.csv.gz", PARCEL_HEADER, pa_rows)


def main() -> int:
    t0 = time.time()
    tasks = [
        ("ny_extra", crunch_ny_extras),
        ("il", crunch_illinois),
        ("ca", crunch_california),
        ("mt", crunch_montana),
        ("mn", crunch_minnesota_metro),
        ("oh", crunch_ohio_cuyahoga),
    ]
    summary: Dict[str, Any] = {"started_at": dt.datetime.now(dt.timezone.utc).isoformat(), "completed": {}, "errors": {}}
    # Run 3 state pipelines concurrently at a time on the 16-vCPU Spot VM
    with ThreadPoolExecutor(max_workers=3) as pool:
        fut_map = {pool.submit(fn): name for name, fn in tasks}
        for fut in as_completed(fut_map):
            name = fut_map[fut]
            try:
                fut.result()
                summary["completed"][name] = dt.datetime.now(dt.timezone.utc).isoformat()
            except Exception as exc:
                log(f"✘ [{name}] failed: {exc}")
                summary["errors"][name] = str(exc)
            status_path = os.path.join(WORK_DIR, "status.json")
            with open(status_path, "w", encoding="utf-8") as sf:
                json.dump(summary, sf, indent=2)
            upload_to_gcs(status_path, "status.json")

    summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    summary["elapsed_sec"] = round(time.time() - t0, 1)
    done_path = os.path.join(WORK_DIR, "done.json")
    with open(done_path, "w", encoding="utf-8") as df:
        json.dump(summary, df, indent=2)
    upload_to_gcs(done_path, "done.json")
    log(f"=== ALL SPOT MEGA-CRUNCHER JOBS FINISHED IN {summary['elapsed_sec']}s ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
