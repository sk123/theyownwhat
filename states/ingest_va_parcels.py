#!/usr/bin/env python3
"""
states/ingest_va_parcels.py
===========================
Virginia (`va`) multi-source authoritative parcel, CAMA assessment, recent-sale,
and OpenCorporates (`us_va` + cross-state foreign parent) ownership-network
ingestion pipeline.

Official Authoritative Sources Combined (Zero Fabricated Fields):
  1. VGIN `Virginia Parcels: Local Schema Tables (2026 Q3)` (`Virginia_Parcel_Dataset_LocalSchemas_2026Q3.gdb`)
     joined 1-for-1 on `VGIN_QPID` to `Virginia Parcel Dataset (2026 Q3)` (`Virginia_Parcel_Dataset_2026Q3.gdb`)
     for parcel IDs, locality names, and NAD83 -> WGS84 centroids across ~81 Virginia counties & cities
     (including Richmond City, Chesterfield County, Alexandria City, Newport News, Portsmouth, Roanoke City,
     Roanoke County, Montgomery County/Blacksburg, Hanover County, James City County, York County,
     Lynchburg City, Fauquier County, Danville City, Staunton City, etc.).
  2. Hampton Roads Planning District Commission / HRGeo Regional GIS (`HRGeo_Parcels_Public/MapServer/0`)
     for Hampton Roads localities whose VGIN submission omits owner names (`Virginia Beach`, `Norfolk`,
     `Suffolk`, `Gloucester`, `Isle of Wight`, `Williamsburg`, `Franklin`).
  3. Henrico County Official GIS (`Henrico_County_Tax_Parcels_0322/FeatureServer/0`) for Henrico County
     parcels (`OWNER_CURRENT`, `FULL_ADDRESS`, `MAILING_ADDRESS_01..04`, `LAST_SALE_DATE`, `LAST_SALE_PRICE`,
     `NUMBER_UNITS_APARTMENT`).
  4. Prince William County Official GIS (`gisweb.pwcva.gov/.../CAMA_Parcels/MapServer/4`) for Prince William
     County CAMA parcels (`CAMA_OWNER_CUR`, `CAMA_ADDRESS2..ZIPCODE`, `RecordedDate`).
  5. OpenCorporates Virginia State Corporation Commission (`us_va` — 2.75M companies, 999K officers) +
     national parent registries (`us_de`, `us_md`, `us_dc`, `us_nc`, `us_ny`, `us_fl`).

Source-Honesty Note:
  Several Northern Virginia localities (most notably Fairfax County, Arlington County, and Loudoun County)
  intentionally redact deed owner names from bulk public GIS exports under locality privacy policy. Those
  localities are explicitly disclosed in the UI data-gaps banner rather than guessed or fabricated.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import pyogrio
from pyogrio.raw import read as raw_read
from pyproj import Transformer
import shapely

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_jurisdiction_partition
from api.populate_cities import source_float, source_int, source_text
from states.ingest_statewide_parcels import (
    DATABASE_URL,
    _build_parcel_record,
    _clean_zip5,
    _parse_any_date,
    _sharded_oid_fetch,
    _validate_coords,
    build_and_save_state_networks,
)

DEFAULT_DATA_DIR = os.path.join(REPO_ROOT, "data", "va")
VGIN_LOCAL_SCHEMAS_ITEM_ID = "523d89ebf23d4d84957f9fe5b9158bd9"
VGIN_PARCELS_GEOM_ITEM_ID = "29627d7c051a47dc8ce71b4484531ab3"


def _resolve_gdb_paths(refresh: bool = False) -> Tuple[str, str]:
    """Locate or automatically download & unzip VGIN's latest quarterly File Geodatabases."""
    import glob
    import subprocess

    os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)
    local_matches = sorted(glob.glob(os.path.join(DEFAULT_DATA_DIR, "Virginia_Parcel_Dataset_LocalSchemas*.gdb")))
    geom_matches = sorted(
        p for p in glob.glob(os.path.join(DEFAULT_DATA_DIR, "Virginia_Parcel_Dataset_*.gdb"))
        if "LocalSchemas" not in os.path.basename(p)
    )
    if refresh or not local_matches:
        zpath = os.path.join(DEFAULT_DATA_DIR, "va_local_schema_tables.zip")
        url = f"https://www.arcgis.com/sharing/rest/content/items/{VGIN_LOCAL_SCHEMAS_ITEM_ID}/data"
        print(f"  [VA / VGIN] Downloading latest LocalSchemas GDB from {url}...")
        subprocess.run(["curl", "-fsSL", "-o", zpath, url], check=True)
        subprocess.run(["unzip", "-qo", zpath, "-d", DEFAULT_DATA_DIR], check=True)
        os.remove(zpath)
        local_matches = sorted(glob.glob(os.path.join(DEFAULT_DATA_DIR, "Virginia_Parcel_Dataset_LocalSchemas*.gdb")))
    if refresh or not geom_matches:
        zpath = os.path.join(DEFAULT_DATA_DIR, "va_parcels_geom.zip")
        url = f"https://www.arcgis.com/sharing/rest/content/items/{VGIN_PARCELS_GEOM_ITEM_ID}/data"
        print(f"  [VA / VGIN] Downloading latest Virginia Parcels Geometry GDB from {url}...")
        subprocess.run(["curl", "-fsSL", "-o", zpath, url], check=True)
        subprocess.run(["unzip", "-qo", zpath, "-d", DEFAULT_DATA_DIR], check=True)
        os.remove(zpath)
        geom_matches = sorted(
            p for p in glob.glob(os.path.join(DEFAULT_DATA_DIR, "Virginia_Parcel_Dataset_*.gdb"))
            if "LocalSchemas" not in os.path.basename(p)
        )
    return local_matches[-1], geom_matches[-1]

OWNER1_PRIORITY = [
    "OWNER_CURRENT",
    "CURRENTOWNER",
    "CURROWN",
    "OWNERNAME",
    "OWNER_NAME",
    "OWNERNME1",
    "OWN_NAME1",
    "OWN_NAME",
    "OWNERNAME1",
    "OWNERS_NAME_1",
    "PROPERTYOWNER",
    "REM_OWN_NAME",
    "REM_OWN_NA",
    "OWNER_N",
    "OWNER1_L_1",
    "OWNER_OF_RECORD",
    "OWNER_NAME_1",
    "OWNER1",
    "OWNER",
    "OWNERS",
    "OWNRNAME",
    "RENAME",
    "NAME1",
    "ACCOUNT_NAME",
    "ACCOUNTNAME",
    "GRANTEE",
    "TAXPAYER",
]

OWNER2_PRIORITY = [
    "CO_OWNER_CURRENT",
    "OWNERNAME2",
    "OWNERNME2",
    "OWN_NAME2",
    "OWNER2",
    "CO_OWNER2",
    "CO_OWNER_OF_RECORD",
    "PROPERTYOWNERNAME2",
    "OWNERS_NAME_2",
    "ACCOUNT_NAME2",
    "NAME2",
]

PARCEL_ID_PRIORITY = [
    "GPIN",
    "PARCELID",
    "PARCEL_ID",
    "PIN",
    "PID_RE",
    "TAXID",
    "PAR_GPIN",
    "MAP_PIN",
    "CAMA_PIN",
    "PARCELNUMBER",
    "ACCT_ID",
    "ACCOUNTNO",
]

FULL_SITE_ADDR_PRIORITY = [
    "FULL_ADDRESS",
    "FULL_ADDRE",
    "SITEADDRES",
    "SITE_ADDRE",
    "SITE_ADDR",
    "ADDRESS_GI",
    "LOCADDR",
    "PROPADDR",
    "PROP_ADDR",
    "SITE_ADD1",
    "PSTLADDRES",
    "PROPERTY_ADDRESS",
    "SITUS",
    "PHYSADDR",
]

HOUSE_NUM_PRIORITY = [
    "HOUSENUM",
    "HOUSE_NUMB",
    "ST_NUM",
    "ADDRNBR",
    "ADDRESS_NU",
    "LOCNUM",
    "STRTNUMB",
    "ASRLOCATIO",
    "PARCEL_LOCATION_NUMBER",
    "SITUS_ADDRESS_NUMBER",
]

STREET_NAME_PRIORITY = [
    "ST_NAME",
    "STREETNAME",
    "STREET_NAM",
    "STREET_ADD",
    "PARCEL_LOCATION_STREET",
    "SITUS_ADDRESS_STREET_NAME",
    "ROADNAME",
]

STREET_TYPE_PRIORITY = [
    "ST_TYPE",
    "STREETTYPE",
    "STREET_TYP",
]

SITE_ZIP_PRIORITY = [
    "SITE_ZIP",
    "ZIP_CODE",
    "ZIPCODE",
    "ZIP",
    "PSTLZIP5",
    "ADDRESSZIP",
]

MAIL_ADDR1_PRIORITY = [
    "MAILING_ADDRESS_01",
    "MAILADDRES",
    "MAIL_ADDRE",
    "MAILADDR",
    "MAIL_ADD1",
    "MAILINGADD",
    "MAIL1",
    "OWNERMAILINGADDR",
    "BILLINGADDR",
    "OWN_ADD",
    "OWN_ADDR1",
    "OWNERADDR1",
    "OWNERADDRE",
    "OWNER_ADDR",
    "OWNRADR1",
    "OWNER_A",
    "MLADDRESS",
]

MAIL_ADDR2_PRIORITY = [
    "MAILING_ADDRESS_02",
    "MAIL_ADD2",
    "MAILINGA_1",
    "MAIL2",
    "OWN_ADD2",
    "OWNRADR2",
    "OWNERADD_1",
]

MAIL_CITY_PRIORITY = [
    "MAILCITY",
    "MAIL_CITY",
    "OWNERCITY",
    "OWNER_CITY",
    "OWN_CITY",
    "BILLINGCITY",
    "MLCITY",
]

MAIL_STATE_PRIORITY = [
    "MAILSTATE",
    "MAIL_STATE",
    "OWNERSTATE",
    "OWNER_STAT",
    "OWN_STAT",
    "BILLINGSTATE",
    "MLSTATE",
]

MAIL_ZIP_PRIORITY = [
    "MAILZIP",
    "MAIL_ZIP",
    "MAINZIPCOD",
    "OWNERZIP",
    "OWN_ZIP",
    "BILLINGZIP",
    "MLZIP5",
]

TOTAL_VAL_PRIORITY = [
    "TOTALASSES",
    "TOTAL_ASSESSMENT",
    "TOTALVALUE",
    "NTOTAL_VAL",
    "TOTAL_VALU",
    "TOTALVAL1",
    "TOTVALUE",
    "YR26_TOTAL",
    "FAIRMARKET",
    "FAIRMARKETVALUE",
    "ASSESSMENT",
]

LAND_VAL_PRIORITY = [
    "LANDVALUE",
    "NLAND_VALU",
    "LAND_VALUE",
    "LANDVAL1",
    "PRVLNDVAL",
    "TOTLLVAL",
    "LAND_VAL",
    "LNDVALUE",
]

IMP_VAL_PRIORITY = [
    "DWELLINGVA",
    "IMPROVEMEN",
    "NBLDG_VALU",
    "BLDG_VALUE",
    "IMPROVEMENTVALUE",
    "PRVIMPVAL",
    "TOTLIVAL",
    "BLDG_VAL",
    "IMPVALUE",
]

SALE_DATE_PRIORITY = [
    "LAST_SALE_DATE",
    "LASTSALEDA",
    "SALEDATE",
    "SALE_DATE",
    "SALE_DATE_1",
    "SALEDATE1",
    "SALE1D",
    "SALE1DT",
    "DEED_DATE",
]

SALE_PRICE_PRIORITY = [
    "LAST_SALE_PRICE",
    "LASTSALEPR",
    "SALEPRICE",
    "NSALE_PRIC",
    "SALE_PRICE",
    "SALE_PRICE_1",
    "SALEAMT1",
    "SALE1AMT",
    "CONSIDERAT",
]

YEAR_BUILT_PRIORITY = [
    "YEAR_BUILT",
    "YEARBUILT",
    "YRBUILT",
    "YR_BUILT",
    "RESYRBLT",
    "EFFYRBUILT",
]

UNITS_PRIORITY = [
    "NUMBER_UNITS_APARTMENT",
    "PA_UNITS",
    "UNITS",
    "LIVUNIT",
    "NUMUNITS",
]

LAND_USE_PRIORITY = [
    "LANDUSE",
    "LANDDESC",
    "PROPERTYDE",
    "USE_DESCRIPTION",
    "PROP_DESC",
    "PROPDESC",
    "LANDDES1",
    "ZONING",
    "ZONEDESC",
]


def _pick_col(fields: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    up = {c.upper(): c for c in fields}
    for cand in candidates:
        if cand in up:
            return up[cand]
    return None


def _to_qpid(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, (int, np.integer)):
        iv = int(val)
        return iv if iv > 0 else None
    if isinstance(val, (float, np.floating)):
        if np.isnan(val):
            return None
        iv = int(val)
        return iv if iv > 0 else None
    s = str(val).strip()
    if not s or s.upper() in ("NONE", "NULL", "NAN", "NAT"):
        return None
    try:
        iv = int(float(s))
        return iv if iv > 0 else None
    except ValueError:
        return None


def _clean_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, (float, np.floating)) and np.isnan(val):
        return None
    s = html.unescape(str(val)).strip()
    if not s or s.upper() in ("NONE", "NULL", "NAT", "NAN", "<NULL>"):
        return None
    s = re.sub(r"\s+", " ", s)
    return s


def _clean_money(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float, np.integer, np.floating)):
        if isinstance(val, (float, np.floating)) and np.isnan(val):
            return None
        fv = float(val)
        return fv if 0 < fv < 50_000_000_000 else None
    s = _clean_str(val)
    if not s:
        return None
    s_num = re.sub(r"[^0-9.-]", "", s)
    if not s_num or s_num in ("-", ".", "-."):
        return None
    try:
        fv = float(s_num)
        return fv if 0 < fv < 50_000_000_000 else None
    except ValueError:
        return None


def _clean_sale_date(val: Any) -> Optional[dt.date]:
    if val is None:
        return None
    if isinstance(val, dt.datetime):
        d = val.date()
        return d if 1950 <= d.year <= 2027 else None
    if isinstance(val, dt.date):
        return val if 1950 <= val.year <= 2027 else None
    s = _clean_str(val)
    if not s or s.startswith("1776") or s.startswith("1899") or s.startswith("1900"):
        return None
    d = _parse_any_date(s)
    if d and 1950 <= d.year <= 2027:
        return d
    return None


def _clean_locality_label(raw_loc: str) -> str:
    s = re.sub(r"^TBL_", "", raw_loc or "").replace("_", " ").strip().upper()
    return re.sub(r"\s+", " ", s)


def _load_vgin_geometry_lookup(
    parcel_geom_gdb: str, wanted_qpids: Set[int]
) -> Dict[int, Tuple[str, str, Optional[float], Optional[float]]]:
    """Stream `Virginia_Parcel_Dataset_*.gdb` (`VA_Parcels`) in 300k chunks,
    projecting centroids ONLY for `wanted_qpids` to keep RAM flat and finish in ~15s."""
    if not wanted_qpids or not os.path.exists(parcel_geom_gdb):
        return {}
    info = pyogrio.read_info(parcel_geom_gdb, layer="VA_Parcels")
    total_feats = info["features"]
    crs = info["crs"]
    tr = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)

    wanted_arr = np.fromiter(wanted_qpids, dtype=np.int64, count=len(wanted_qpids))
    wanted_arr.sort()

    lookup: Dict[int, Tuple[str, str, Optional[float], Optional[float]]] = {}
    chunk = 300_000
    t0 = time.time()
    for offset in range(0, total_feats, chunk):
        meta_g, _, geom, fdata = raw_read(
            parcel_geom_gdb,
            layer="VA_Parcels",
            skip_features=offset,
            max_features=chunk,
            columns=["VGIN_QPID", "LOCALITY", "PARCELID"],
            read_geometry=True,
        )
        g_idx = {c: i for i, c in enumerate(meta_g["fields"])}
        qpid_raw = fdata[g_idx["VGIN_QPID"]]
        valid_mask = ~np.isnan(qpid_raw)
        if not np.any(valid_mask):
            continue
        qpids_i64 = np.where(valid_mask, qpid_raw, -1).astype(np.int64)
        idx_pos = np.searchsorted(wanted_arr, qpids_i64)
        idx_pos_clipped = np.minimum(idx_pos, len(wanted_arr) - 1)
        hit_mask = valid_mask & (wanted_arr[idx_pos_clipped] == qpids_i64)
        hit_indices = np.nonzero(hit_mask)[0]
        if len(hit_indices) == 0:
            continue

        sub_geom = geom[hit_indices]
        gs = shapely.from_wkb(sub_geom)
        pts = shapely.centroid(gs)
        xs = shapely.get_x(pts)
        ys = shapely.get_y(pts)
        lons, lats = tr.transform(xs, ys)

        locs = fdata[g_idx["LOCALITY"]][hit_indices]
        pids = fdata[g_idx["PARCELID"]][hit_indices]
        hit_qpids = qpids_i64[hit_indices]

        for j in range(len(hit_indices)):
            q = int(hit_qpids[j])
            loc_s = str(locs[j] or "").strip().upper()
            pid_s = str(pids[j] or "").strip()
            lat_f = float(lats[j]) if not np.isnan(lats[j]) else None
            lon_f = float(lons[j]) if not np.isnan(lons[j]) else None
            lat_v, lon_v = _validate_coords("va", lat_f, lon_f)
            lookup[q] = (pid_s, loc_s, lat_v, lon_v)

    print(
        f"  [VA / VGIN Geom] Resolved {len(lookup):,} / {len(wanted_qpids):,} VGIN_QPID centroids in {time.time() - t0:.1f}s."
    )
    return lookup


def fetch_vgin_local_schema_parcels(refresh_gdb: bool = False) -> Tuple[List[Dict[str, Any]], Set[str]]:
    """Extract all owner-bearing locality tables from VGIN LocalSchemas GDB."""
    local_schemas_gdb, parcel_geom_gdb = _resolve_gdb_paths(refresh=refresh_gdb)

    layers = pyogrio.list_layers(local_schemas_gdb)
    selected_tables: List[Tuple[str, str, Dict[str, Optional[str]]]] = []
    wanted_qpids: Set[int] = set()

    print(f"▶ [VA / VGIN LocalSchemas] Scanning {len(layers)} locality tables in {os.path.basename(local_schemas_gdb)}...")
    for layer_name, _ in layers:
        if not layer_name.startswith("TBL_"):
            continue
        info = pyogrio.read_info(local_schemas_gdb, layer=layer_name)
        fields = list(info["fields"])
        if info["features"] <= 0:
            continue

        own1_col = _pick_col(fields, OWNER1_PRIORITY)
        # Fallback for counties (Brunswick, Buckingham, Sussex, Amelia) that store owner in `NAME`
        if not own1_col:
            up = {c.upper(): c for c in fields}
            if "NAME" in up:
                _, _, _, sample_fd = raw_read(
                    local_schemas_gdb, layer=layer_name, columns=[up["NAME"]], max_features=100, read_geometry=False
                )
                sample_vals = [_clean_str(v) for v in sample_fd[0]]
                alpha_vals = [
                    v for v in sample_vals
                    if v and re.search(r"[A-Z]{3,}", v.upper()) and not re.match(r"^[\d\s./()-]+$", v)
                    and " IN PART" not in v.upper() and not v.upper().endswith(".DWG")
                ]
                if len(alpha_vals) >= 10:
                    own1_col = up["NAME"]

        if not own1_col:
            continue

        qpid_col = _pick_col(fields, ["VGIN_QPID"])
        col_map = {
            "own1": own1_col,
            "own2": _pick_col(fields, OWNER2_PRIORITY),
            "pid": _pick_col(fields, PARCEL_ID_PRIORITY),
            "site_full": _pick_col(fields, FULL_SITE_ADDR_PRIORITY),
            "hnum": _pick_col(fields, HOUSE_NUM_PRIORITY),
            "sname": _pick_col(fields, STREET_NAME_PRIORITY),
            "stype": _pick_col(fields, STREET_TYPE_PRIORITY),
            "szip": _pick_col(fields, SITE_ZIP_PRIORITY),
            "maddr1": _pick_col(fields, MAIL_ADDR1_PRIORITY),
            "maddr2": _pick_col(fields, MAIL_ADDR2_PRIORITY),
            "mcity": _pick_col(fields, MAIL_CITY_PRIORITY),
            "mstate": _pick_col(fields, MAIL_STATE_PRIORITY),
            "mzip": _pick_col(fields, MAIL_ZIP_PRIORITY),
            "totval": _pick_col(fields, TOTAL_VAL_PRIORITY),
            "lndval": _pick_col(fields, LAND_VAL_PRIORITY),
            "impval": _pick_col(fields, IMP_VAL_PRIORITY),
            "sdate": _pick_col(fields, SALE_DATE_PRIORITY),
            "sprice": _pick_col(fields, SALE_PRICE_PRIORITY),
            "yrblt": _pick_col(fields, YEAR_BUILT_PRIORITY),
            "units": _pick_col(fields, UNITS_PRIORITY),
            "luse": _pick_col(fields, LAND_USE_PRIORITY),
            "qpid": qpid_col,
            "loc_name": _pick_col(fields, ["VGIN_LOCALITY_NAME"]),
        }
        selected_tables.append((layer_name, _clean_locality_label(layer_name), col_map))

        if qpid_col:
            _, _, _, qfd = raw_read(
                local_schemas_gdb, layer=layer_name, columns=[qpid_col], read_geometry=False
            )
            qarr = qfd[0]
            if np.issubdtype(qarr.dtype, np.floating):
                valid = qarr[~np.isnan(qarr)].astype(np.int64)
                wanted_qpids.update(int(x) for x in valid if x > 0)
            elif np.issubdtype(qarr.dtype, np.integer):
                wanted_qpids.update(int(x) for x in qarr if x > 0)
            else:
                for raw_q in qarr:
                    qv = _to_qpid(raw_q)
                    if qv:
                        wanted_qpids.add(qv)

    print(f"  [VA / VGIN LocalSchemas] Found {len(selected_tables)} owner-bearing locality tables ({len(wanted_qpids):,} QPIDs).")
    geom_lookup = _load_vgin_geometry_lookup(parcel_geom_gdb, wanted_qpids)

    parcels: List[Dict[str, Any]] = []
    covered_localities: Set[str] = set()

    for layer_name, fallback_loc, cmap in selected_tables:
        needed_cols = sorted({c for c in cmap.values() if c is not None})
        meta_t, _, _, fdata = raw_read(
            local_schemas_gdb, layer=layer_name, columns=needed_cols, read_geometry=False
        )
        col_idx = {c: i for i, c in enumerate(meta_t["fields"])}
        n_rows = len(fdata[0]) if needed_cols else 0
        loc_added = 0

        def _col(key: str):
            cn = cmap.get(key)
            return fdata[col_idx[cn]] if cn is not None else None

        a_own1 = _col("own1")
        a_own2 = _col("own2")
        a_pid = _col("pid")
        a_sfull = _col("site_full")
        a_hnum = _col("hnum")
        a_sname = _col("sname")
        a_stype = _col("stype")
        a_szip = _col("szip")
        a_m1 = _col("maddr1")
        a_m2 = _col("maddr2")
        a_mc = _col("mcity")
        a_ms = _col("mstate")
        a_mz = _col("mzip")
        a_tv = _col("totval")
        a_lv = _col("lndval")
        a_iv = _col("impval")
        a_sd = _col("sdate")
        a_sp = _col("sprice")
        a_yb = _col("yrblt")
        a_un = _col("units")
        a_lu = _col("luse")
        a_qp = _col("qpid")
        a_ln = _col("loc_name")

        for i in range(n_rows):
            owner1 = _clean_str(a_own1[i]) if a_own1 is not None else None
            if not owner1 or owner1.upper() in ("HOLE", "UNKNOWN", "UNASSIGNED", "N/A", "0"):
                continue

            qpid_val = _to_qpid(a_qp[i]) if a_qp is not None else None

            g_pid, g_loc, lat, lng = geom_lookup.get(qpid_val, ("", "", None, None)) if qpid_val else ("", "", None, None)
            locality = (_clean_str(a_ln[i]) if a_ln is not None else None) or g_loc or fallback_loc
            locality_up = locality.upper()

            pid = (_clean_str(a_pid[i]) if a_pid is not None else None) or g_pid or (str(qpid_val) if qpid_val else f"{layer_name}-{i}")
            loc_slug = re.sub(r"[^A-Z0-9]+", "_", locality_up).strip("_")[:24]
            bbl = f"VA-{loc_slug}-{pid}"[:80]

            site_addr = _clean_str(a_sfull[i]) if a_sfull is not None else None
            if not site_addr or site_addr in ("0", "0 "):
                parts = []
                if a_hnum is not None:
                    hn = _clean_str(a_hnum[i])
                    if hn and hn not in ("0", "0.0"):
                        parts.append(hn.removesuffix(".0"))
                if a_sname is not None:
                    sn = _clean_str(a_sname[i])
                    if sn:
                        parts.append(sn)
                if a_stype is not None:
                    st = _clean_str(a_stype[i])
                    if st:
                        parts.append(st)
                if parts:
                    site_addr = " ".join(parts)

            if not site_addr:
                site_addr = f"PARCEL {pid}"

            m_parts = []
            for arr in (a_m1, a_m2, a_mc):
                if arr is not None:
                    v = _clean_str(arr[i])
                    if v:
                        m_parts.append(v)
            mst = _clean_str(a_ms[i]) if a_ms is not None else None
            mzip = _clean_zip5(a_mz[i]) if a_mz is not None else None
            tail = " ".join(x for x in (mst, mzip) if x)
            if tail:
                m_parts.append(tail)
            mailing_addr = ", ".join(m_parts)

            tot_val = _clean_money(a_tv[i]) if a_tv is not None else None
            if tot_val is None:
                lv = _clean_money(a_lv[i]) if a_lv is not None else None
                iv = _clean_money(a_iv[i]) if a_iv is not None else None
                if lv is not None or iv is not None:
                    tot_val = (lv or 0.0) + (iv or 0.0)

            sprice = _clean_money(a_sp[i]) if a_sp is not None else None
            sdate = _clean_sale_date(a_sd[i]) if a_sd is not None else None
            yr_built = source_int(a_yb[i]) if a_yb is not None else None
            units = source_int(a_un[i]) if a_un is not None else None
            luse = _clean_str(a_lu[i]) if a_lu is not None else None
            szip = _clean_zip5(a_szip[i]) if a_szip is not None else None
            owner2 = _clean_str(a_own2[i]) if a_own2 is not None else None

            rec = _build_parcel_record(
                jur="va",
                bbl=bbl,
                address=site_addr,
                borough=locality_up,
                zip_code=szip,
                owner1=owner1,
                owner2=owner2,
                mailing_address=mailing_addr,
                explicit_mail_state=mst,
                land_use=luse,
                units=units,
                year_built=yr_built,
                assessed_total=tot_val,
                last_sale_date=sdate,
                last_sale_price=sprice,
                lat=lat,
                lng=lng,
                source_system="VA_VGIN_LOCAL_SCHEMAS_2026Q3",
                col_max_len=40,
            )
            if rec is not None:
                parcels.append(rec)
                loc_added += 1

        if loc_added >= 100:
            covered_localities.add(fallback_loc)

    print(
        f"  ✓ [VA / VGIN LocalSchemas] Extracted {len(parcels):,} valid owner-attributed parcels across {len(covered_localities)} localities."
    )
    return parcels, covered_localities


def fetch_hrgeo_hampton_roads_parcels(skip_agencies: Set[str]) -> List[Dict[str, Any]]:
    """Fetch Hampton Roads Regional GIS (`HRGeo_Parcels_Public`) for cities/counties
    whose VGIN table omitted owner names (Virginia Beach, Norfolk, Suffolk, Gloucester, Williamsburg)."""
    url = "https://geo.hrsd.com/hrgeo/rest/services/regionalgis/HRGeo_Parcels_Public/MapServer/0/query"
    fields = (
        "OBJECTID,FIPS,PARCELID,OWNERNME1,PSTLADDRESS,PSTLCITY,PSTLZIP5,"
        "MLADDRESS,MLCITY,MLSTATE,MLZIP5,TOTVALUE,RESYRBLT,ZONING,SRCAGENCY"
    )
    # Only pull Hampton Roads localities not already covered in VGIN LocalSchemas
    target_agencies = ["Virginia Beach", "Norfolk", "Suffolk", "Gloucester", "Williamsburg"]
    quoted = ",".join(f"'{a}'" for a in target_agencies if a.upper() not in skip_agencies)
    where = f"OWNERNME1 IS NOT NULL AND SRCAGENCY IN ({quoted})"

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        owner1 = _clean_str(a.get("OWNERNME1"))
        pid = _clean_str(a.get("PARCELID")) or str(a.get("OBJECTID") or "")
        agency = (_clean_str(a.get("SRCAGENCY")) or "HAMPTON ROADS").upper()
        if agency in ("VIRGINIA BEACH", "NORFOLK", "SUFFOLK", "WILLIAMSBURG"):
            borough = f"{agency} CITY"
        elif agency == "GLOUCESTER":
            borough = "GLOUCESTER COUNTY"
        else:
            borough = agency
        if not owner1 or not pid:
            return None
        site = _clean_str(a.get("PSTLADDRESS")) or f"PARCEL {pid}"
        szip = _clean_zip5(a.get("PSTLZIP5"))
        mst = _clean_str(a.get("MLSTATE"))
        mzip = _clean_zip5(a.get("MLZIP5"))
        mail_parts = [p for p in (_clean_str(a.get("MLADDRESS")), _clean_str(a.get("MLCITY"))) if p]
        tail = " ".join(x for x in (mst, mzip) if x)
        if tail:
            mail_parts.append(tail)
        c = feat.get("centroid") or feat.get("geometry") or {}
        lat, lng = _validate_coords("va", source_float(c.get("y")), source_float(c.get("x")))
        loc_slug = re.sub(r"[^A-Z0-9]+", "_", borough).strip("_")[:24]
        return _build_parcel_record(
            jur="va",
            bbl=f"VA-{loc_slug}-{pid}"[:80],
            address=site,
            borough=borough,
            zip_code=szip,
            owner1=owner1,
            mailing_address=", ".join(mail_parts),
            explicit_mail_state=mst,
            land_use=_clean_str(a.get("ZONING")),
            year_built=source_int(a.get("RESYRBLT")),
            assessed_total=_clean_money(a.get("TOTVALUE")),
            lat=lat,
            lng=lng,
            source_system="VA_HRGEO_REGIONAL_PARCELS",
            col_max_len=40,
        )

    return _sharded_oid_fetch(
        url,
        where,
        fields,
        _map,
        jur_upper="VA-HRGEO",
        page_size=2000,
        num_shards=8,
        max_workers=6,
        return_centroid=True,
    )


def fetch_henrico_parcels() -> List[Dict[str, Any]]:
    """Fetch Henrico County Official Tax Parcels & CAMA (`cow003@henrico.gov`)."""
    url = "https://services.arcgis.com/LxWK4CxNTBBlLshT/arcgis/rest/services/Henrico_County_Tax_Parcels_0322/FeatureServer/0/query"
    fields = (
        "OBJECTID,GPIN,FULL_ADDRESS,ZIP_CODE,OWNER_CURRENT,MAILING_ADDRESS_01,"
        "MAILING_ADDRESS_02,MAILING_ADDRESS_03,MAILING_ADDRESS_04,USE_DESCRIPTION,"
        "NUMBER_UNITS_APARTMENT,IMPROVEMENTS_VALUE_CURRENT,LAND_VALUE_CURRENT,"
        "LAST_SALE_DATE,LAST_SALE_PRICE,YEAR_BUILT"
    )
    where = "OWNER_CURRENT IS NOT NULL"

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        owner1 = _clean_str(a.get("OWNER_CURRENT"))
        gpin = _clean_str(a.get("GPIN")) or str(a.get("OBJECTID") or "")
        if not owner1 or not gpin:
            return None
        site = _clean_str(a.get("FULL_ADDRESS")) or f"PARCEL {gpin}"
        mparts = [
            p for p in (
                _clean_str(a.get("MAILING_ADDRESS_01")),
                _clean_str(a.get("MAILING_ADDRESS_02")),
                _clean_str(a.get("MAILING_ADDRESS_03")),
                _clean_str(a.get("MAILING_ADDRESS_04")),
            ) if p
        ]
        lv = _clean_money(a.get("LAND_VALUE_CURRENT")) or 0.0
        iv = _clean_money(a.get("IMPROVEMENTS_VALUE_CURRENT")) or 0.0
        tot = (lv + iv) if (lv + iv) > 0 else None
        c = feat.get("centroid") or feat.get("geometry") or {}
        lat, lng = _validate_coords("va", source_float(c.get("y")), source_float(c.get("x")))
        return _build_parcel_record(
            jur="va",
            bbl=f"VA-HENRICO_COUNTY-{gpin}"[:80],
            address=site,
            borough="HENRICO COUNTY",
            zip_code=_clean_zip5(a.get("ZIP_CODE")),
            owner1=owner1,
            mailing_address=", ".join(mparts),
            land_use=_clean_str(a.get("USE_DESCRIPTION")),
            units=source_int(a.get("NUMBER_UNITS_APARTMENT")),
            year_built=source_int(a.get("YEAR_BUILT")),
            assessed_total=tot,
            last_sale_date=_clean_sale_date(a.get("LAST_SALE_DATE")),
            last_sale_price=_clean_money(a.get("LAST_SALE_PRICE")),
            lat=lat,
            lng=lng,
            source_system="VA_HENRICO_COUNTY_CAMA",
            col_max_len=40,
        )

    return _sharded_oid_fetch(
        url,
        where,
        fields,
        _map,
        jur_upper="VA-HENRICO",
        page_size=2000,
        num_shards=6,
        max_workers=6,
        return_centroid=True,
    )


def fetch_prince_william_parcels() -> List[Dict[str, Any]]:
    """Fetch Prince William County Official CAMA Parcels (`gisweb.pwcva.gov`)."""
    url = "https://gisweb.pwcva.gov/arcgis/rest/services/GTS/CAMA_Parcels/MapServer/4/query"
    fields = (
        "OBJECTID,GPIN,StreetNumber,StreetName,StreetType,UnitNumber,ZipCode,"
        "RecordedDate,CAMA_OWNER_CUR,CAMA_ADDRESS2,CAMA_ADDRESS3,CAMA_CITY,"
        "CAMA_STATE,CAMA_ZIPCODE,CAMA_USECODE"
    )
    where = "CAMA_OWNER_CUR IS NOT NULL"

    def _map(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        a = feat.get("attributes") or {}
        owner1 = _clean_str(a.get("CAMA_OWNER_CUR"))
        gpin = _clean_str(a.get("GPIN")) or str(a.get("OBJECTID") or "")
        if not owner1 or not gpin:
            return None
        sparts = [
            p for p in (
                _clean_str(a.get("StreetNumber")),
                _clean_str(a.get("StreetName")),
                _clean_str(a.get("StreetType")),
                _clean_str(a.get("UnitNumber")),
            ) if p and p != "0"
        ]
        site = " ".join(sparts) if sparts else f"PARCEL {gpin}"
        mst = _clean_str(a.get("CAMA_STATE"))
        mzip = _clean_zip5(a.get("CAMA_ZIPCODE"))
        mparts = [
            p for p in (
                _clean_str(a.get("CAMA_ADDRESS2")),
                _clean_str(a.get("CAMA_ADDRESS3")),
                _clean_str(a.get("CAMA_CITY")),
            ) if p
        ]
        tail = " ".join(x for x in (mst, mzip) if x)
        if tail:
            mparts.append(tail)
        c = feat.get("centroid") or feat.get("geometry") or {}
        lat, lng = _validate_coords("va", source_float(c.get("y")), source_float(c.get("x")))
        return _build_parcel_record(
            jur="va",
            bbl=f"VA-PRINCE_WILLIAM_COUNTY-{gpin}"[:80],
            address=site,
            borough="PRINCE WILLIAM COUNTY",
            zip_code=_clean_zip5(a.get("ZipCode")),
            owner1=owner1,
            mailing_address=", ".join(mparts),
            explicit_mail_state=mst,
            land_use=_clean_str(a.get("CAMA_USECODE")),
            last_sale_date=_clean_sale_date(a.get("RecordedDate")),
            lat=lat,
            lng=lng,
            source_system="VA_PRINCE_WILLIAM_CAMA",
            col_max_len=40,
        )

    return _sharded_oid_fetch(
        url,
        where,
        fields,
        _map,
        jur_upper="VA-PWC",
        page_size=1000,
        num_shards=6,
        max_workers=4,
        return_centroid=True,
    )


def ingest_virginia(*, rebuild_only: bool = False, refresh_gdb: bool = False) -> Tuple[int, int]:
    t0 = time.time()
    conn = psycopg2.connect(DATABASE_URL)
    try:
        ensure_jurisdiction_partition(conn, "va")
        if not rebuild_only:
            vgin_rows, covered = fetch_vgin_local_schema_parcels(refresh_gdb=refresh_gdb)
            hr_rows = fetch_hrgeo_hampton_roads_parcels(covered)
            hen_rows = fetch_henrico_parcels()
            pwc_rows = fetch_prince_william_parcels()

            deduped: Dict[str, Dict[str, Any]] = {}
            for p in (*vgin_rows, *hr_rows, *hen_rows, *pwc_rows):
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
            del vgin_rows, hr_rows, hen_rows, pwc_rows

            rows = [
                (
                    "va",
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
            localities_cnt = len({p["borough"] for p in deduped.values() if p.get("borough")})
            del deduped

            print(
                f"Writing {len(rows):,} deduplicated Virginia (`va`) parcels across {localities_cnt} localities into `va_properties`..."
            )
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE va_properties;")
                execute_values(
                    cur,
                    """
                    INSERT INTO va_properties (
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

        p_cnt, n_cnt = build_and_save_state_networks(conn, "va", "VA", "VA_STATEWIDE_PARCELS")
        print(f"✓ [VA] Complete in {time.time() - t0:.1f}s: {p_cnt:,} parcels -> {n_cnt:,} networks.")
        return p_cnt, n_cnt
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Virginia (`va`) parcels, OpenCorporates links & networks.")
    parser.add_argument("--rebuild-only", action="store_true", help="Rebuild OC links & networks from existing `va_properties`.")
    parser.add_argument("--refresh-gdb", action="store_true", help="Re-download latest VGIN quarterly File Geodatabases from ArcGIS Online.")
    args = parser.parse_args()
    ingest_virginia(rebuild_only=args.rebuild_only, refresh_gdb=args.refresh_gdb)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
