#!/usr/bin/env python3
"""
gcp/import_spot_expansion_v3.py
===============================
Downloads the completed Spot Expansion V3 artifacts from
`gs://renter-help-staging-20261119/spot_expansion_v3/` and additively merges
them into local PostgreSQL (NEVER truncating existing counties):

  - `oh_properties`: + Franklin County (Columbus) & Hamilton County (Cincinnati)
  - `mn_properties`: + Dakota County & Anoka County (completing 4-county Twin Cities)
  - `il_properties`: + Lake County, IL (completing top-3 Chicagoland counties)
  - `fl_bbl_stats`:  + Orlando (`k6e8-nw6w`) & Gainesville (`vu9p-a5f7`) code cases
  - `ny_bbl_stats`:  + City of Rochester NBD code enforcement cases
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import datetime as dt
import gzip
import json
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

csv.field_size_limit(10_000_000)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_jurisdiction_partition
from api.oc_linker import demangle_assessor_owner
from api.populate_cities import (
    GENERIC_OWNER_NAMES,
    is_public_entity,
    normalize_link_address,
    normalize_owner_name,
    source_float,
    source_int,
    source_text,
)
from api.shared_utils import (
    is_excluded_third_party_entity,
    normalize_mailing_address_coarse,
    strip_benign_care_of_prefix,
)
from states.ingest_statewide_parcels import _clean_zip5, _parse_any_date

GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine"
DEFAULT_GCLOUD_CONFIG = os.environ.get("GCLOUD_CONFIG", "storrsmagic")
DEFAULT_PROJECT = os.environ.get("GCP_PROJECT", "renter-help")
DEFAULT_BUCKET = os.environ.get("GCS_BUCKET", "renter-help-staging-20261119")
LOCAL_DIR = os.path.join(REPO_ROOT, "data", "spot_expansion_v3")

STATE_BOUNDS: Dict[str, Tuple[float, float, float, float]] = {
    "oh": (38.35, 42.05, -84.85, -80.50),
    "mn": (43.45, 49.40, -97.30, -89.45),
    "il": (36.95, 42.55, -91.55, -87.45),
    "ny": (40.45, 45.05, -79.80, -71.80),
    "mt": (44.30, 49.05, -116.10, -104.00),
}


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def log(msg: str) -> None:
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] [IMPORT-V3] {msg}", flush=True)


def download_from_gcs(project: str, bucket: str, gcloud_config: str) -> None:
    os.makedirs(LOCAL_DIR, exist_ok=True)
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
        "-v", f"{os.path.join(REPO_ROOT, 'data')}:/data",
        "-e", f"CLOUDSDK_ACTIVE_CONFIG_NAME={gcloud_config}",
        "-e", f"CLOUDSDK_CORE_PROJECT={project}",
        GCLOUD_IMAGE,
        "gcloud", "storage", "cp",
        f"gs://{bucket}/spot_expansion_v3/*",
        "/data/spot_expansion_v3/",
        "--quiet",
    ]
    subprocess.run(cmd, check=True, timeout=600)


def _record_status(conn, source_name: str, source_type: str, details: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status (
                source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details
            )
            VALUES (%s, %s, CURRENT_DATE, NOW(), 'SUCCESS', %s::jsonb)
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = CURRENT_DATE,
                last_refreshed_at = NOW(),
                refresh_status = 'SUCCESS',
                details = COALESCE(data_source_status.details, '{}'::jsonb) || EXCLUDED.details;
            """,
            (source_name, source_type, json.dumps(details)),
        )
    conn.commit()


def import_additive_parcels(conn, jur: str, gz_path: str) -> int:
    if not os.path.exists(gz_path):
        log(f"  Missing `{gz_path}`; skipping `{jur}`.")
        return 0
    min_lat, max_lat, min_lon, max_lon = STATE_BOUNDS[jur]
    insert_sql = f"""
        INSERT INTO {jur}_properties (
            jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
            owner_co_name, owner_is_truncated, mailing_address, mailing_address_norm,
            mailing_address_coarse, mailing_state, land_use, bld_class, num_floors,
            units_res, units_total, year_built, assessed_total, last_sale_date,
            last_sale_price, latitude, longitude, cama_site_link, source_system
        )
        VALUES %s
        ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
            address = COALESCE(NULLIF(EXCLUDED.address, ''), {jur}_properties.address),
            owner_name = COALESCE(NULLIF(EXCLUDED.owner_name, ''), {jur}_properties.owner_name),
            owner_name_norm = COALESCE(NULLIF(EXCLUDED.owner_name_norm, ''), {jur}_properties.owner_name_norm),
            mailing_address = COALESCE(NULLIF(EXCLUDED.mailing_address, ''), {jur}_properties.mailing_address),
            mailing_address_norm = COALESCE(NULLIF(EXCLUDED.mailing_address_norm, ''), {jur}_properties.mailing_address_norm),
            mailing_address_coarse = COALESCE(NULLIF(EXCLUDED.mailing_address_coarse, ''), {jur}_properties.mailing_address_coarse),
            units_res = COALESCE(EXCLUDED.units_res, {jur}_properties.units_res),
            year_built = COALESCE(EXCLUDED.year_built, {jur}_properties.year_built),
            assessed_total = COALESCE(EXCLUDED.assessed_total, {jur}_properties.assessed_total),
            last_sale_date = COALESCE(EXCLUDED.last_sale_date, {jur}_properties.last_sale_date),
            last_sale_price = COALESCE(EXCLUDED.last_sale_price, {jur}_properties.last_sale_price),
            latitude = COALESCE(EXCLUDED.latitude, {jur}_properties.latitude),
            longitude = COALESCE(EXCLUDED.longitude, {jur}_properties.longitude),
            cama_site_link = COALESCE(NULLIF(EXCLUDED.cama_site_link, ''), {jur}_properties.cama_site_link),
            source_system = EXCLUDED.source_system,
            updated_at = NOW();
    """
    batch: List[tuple] = []
    seen = set()
    loaded = 0
    by_source: Dict[str, int] = defaultdict(int)
    log(f"Additively streaming `{os.path.basename(gz_path)}` into `{jur}_properties`...")
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as gz, conn.cursor() as cur:
        for r in csv.DictReader(gz):
            bbl = source_text(r.get("bbl"))
            addr = source_text(r.get("address"))
            owner1 = source_text(r.get("owner_name"))
            if not bbl or not addr or not owner1 or bbl in seen:
                continue
            if owner1.upper() in GENERIC_OWNER_NAMES or is_public_entity(owner1) or is_excluded_third_party_entity(owner1):
                continue
            seen.add(bbl)
            mail = source_text(r.get("mailing_address")) or ""
            dem = demangle_assessor_owner(
                owner1,
                owner2=source_text(r.get("owner2")),
                care_of=source_text(r.get("care_of")),
                mailing_address=mail,
                explicit_mailing_state=source_text(r.get("mailing_state")),
                col_max_len=60,
            )
            cl_own = dem.cleaned_owner or owner1
            cl_mail = strip_benign_care_of_prefix(mail)
            units = source_int(r.get("units_res"))
            yr = source_int(r.get("year_built"))
            if yr is not None and (yr < 1750 or yr > 2030):
                yr = None
            lat = source_float(r.get("latitude"))
            lng = source_float(r.get("longitude"))
            if lat is not None and lng is not None and not (min_lat <= lat <= max_lat and min_lon <= lng <= max_lon):
                lat, lng = None, None
            src_sys = source_text(r.get("source_system")) or f"{jur.upper()}_EXPANSION_V3"
            by_source[src_sys] += 1
            batch.append((
                jur,
                bbl[:80],
                addr,
                (source_text(r.get("borough")) or jur.upper()).upper(),
                _clean_zip5(r.get("zip_code")),
                cl_own,
                dem.owner_norm or normalize_owner_name(cl_own),
                dem.care_of_entity,
                dem.is_truncated,
                mail,
                normalize_link_address(cl_mail),
                normalize_mailing_address_coarse(cl_mail),
                dem.mailing_state or source_text(r.get("mailing_state")),
                source_text(r.get("land_use")),
                source_text(r.get("bld_class")),
                source_int(r.get("num_floors")),
                units,
                units,
                yr,
                source_float(r.get("assessed_total")),
                _parse_any_date(r.get("last_sale_date")),
                source_float(r.get("last_sale_price")),
                lat,
                lng,
                source_text(r.get("cama_site_link")),
                src_sys,
            ))
            if len(batch) >= 15000:
                execute_values(cur, insert_sql, batch, page_size=3000)
                loaded += len(batch)
                batch.clear()
        if batch:
            execute_values(cur, insert_sql, batch, page_size=3000)
            loaded += len(batch)
            batch.clear()
    conn.commit()
    log(f"  ✓ Upserted {loaded:,} new `{jur}` parcels ({dict(by_source)}).")
    for src_k, cnt in by_source.items():
        _record_status(conn, f"{jur.upper()}_{src_k}", "state_cama", {"loaded": cnt, "jurisdiction": jur.upper()})
    return loaded


_UNIT_STRIP_RE = re.compile(
    r"\s+(?:#|APT\.?|APARTMENT|UNIT|STE\.?|SUITE|LOT|BLDG\.?|BUILDING|FL\.?|FLOOR|TRLR|TRAILER|SPC|SPACE)\b.*$",
    re.IGNORECASE,
)


def _base_addr_key(addr: str) -> str:
    stripped = _UNIT_STRIP_RE.sub("", (addr or "").strip()).strip()
    return normalize_link_address(stripped) if stripped else ""


def import_fl_ny_overlays(conn, gz_path: str) -> Dict[str, int]:
    if not os.path.exists(gz_path):
        log(f"  Missing `{gz_path}`; skipping overlays.")
        return {}
    log(f"Matching FL (Orlando + Gainesville) & NY (Rochester) code violations from `{os.path.basename(gz_path)}`...")
    # Build targeted lookups only for Orlando, Gainesville (FL) and Rochester / Monroe County (NY)
    fl_by_addr: Dict[str, str] = {}
    fl_by_base_addr: Dict[str, str] = {}
    fl_by_pid: Dict[str, str] = {}
    ny_by_addr: Dict[str, str] = {}
    ny_by_sbl: Dict[str, str] = {}

    with conn.cursor() as cur:
        cur.execute("SELECT bbl, address, borough FROM fl_properties WHERE borough ILIKE 'ORLANDO%%' OR borough ILIKE 'GAINESVILLE%%' OR borough ILIKE '%%ORANGE%%' OR borough ILIKE '%%ALACHUA%%'")
        for bbl, addr, boro in cur.fetchall():
            city = (boro or "").split("(")[0].strip().upper()
            an = normalize_link_address(addr or "")
            if an:
                fl_by_addr.setdefault(f"{city}|{an}", bbl)
                bn = _base_addr_key(addr or "")
                if bn:
                    fl_by_base_addr.setdefault(f"{city}|{bn}", bbl)
            clean_pid = re.sub(r"[^A-Z0-9]", "", (bbl or "").upper())
            if len(clean_pid) >= 8:
                fl_by_pid.setdefault(clean_pid[-12:], bbl)

        # Restrict strictly to Monroe County so Town of Rochester (Ulster County) never collides!
        cur.execute("SELECT bbl, address FROM ny_properties WHERE borough ILIKE 'ROCHESTER%%MONROE%%'")
        for bbl, addr in cur.fetchall():
            an = normalize_link_address(addr or "")
            if an:
                ny_by_addr.setdefault(an, bbl)
            sbl = re.sub(r"^NY-(?:MONROE-)?", "", (bbl or "").upper()).strip()
            if sbl:
                ny_by_sbl.setdefault(sbl, bbl)

    fl_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"vt": 0, "vo": 0, "last_v": None})
    ny_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"vt": 0, "vo": 0, "last_v": None})
    matched = {"fl": 0, "ny": 0}
    matched_by_city: Dict[str, int] = defaultdict(int)

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as gz:
        for r in csv.DictReader(gz):
            jur = (r.get("jur") or "").strip().lower()
            city = (r.get("city") or "").strip().upper()
            addr = source_text(r.get("address")) or ""
            phint = source_text(r.get("parcel_hint")) or ""
            bbl = None
            if jur == "fl":
                an = normalize_link_address(addr)
                if an:
                    bbl = fl_by_addr.get(f"{city}|{an}")
                if not bbl and addr:
                    bn = _base_addr_key(addr)
                    if bn:
                        bbl = fl_by_base_addr.get(f"{city}|{bn}")
                if not bbl and phint:
                    cp = re.sub(r"[^A-Z0-9]", "", phint.upper())
                    if len(cp) >= 8:
                        bbl = fl_by_pid.get(cp[-12:])
                if not bbl:
                    continue
                matched["fl"] += 1
                matched_by_city[city] += 1
                st = fl_stats[bbl]
            elif jur == "ny":
                if phint:
                    bbl = ny_by_sbl.get(phint.upper().strip())
                if not bbl and addr:
                    bbl = ny_by_addr.get(normalize_link_address(addr))
                if not bbl:
                    continue
                matched["ny"] += 1
                matched_by_city[city] += 1
                st = ny_stats[bbl]
            else:
                continue

            st["vt"] += 1
            vstat = (source_text(r.get("status")) or "").upper()
            if vstat in ("OPEN", "ACTIVE", "INITIATED", "PENDING", "VIOLATION") or vstat.startswith("OPEN"):
                st["vo"] += 1
            vdt = _parse_any_date(r.get("event_date"))
            if vdt and (st["last_v"] is None or vdt > st["last_v"]):
                st["last_v"] = vdt

    for jur, stats_map, src_key, desc in (
        ("fl", fl_stats, "FL_ORLANDO_GAINESVILLE_CODE", "Orlando & Gainesville Code Enforcement"),
        ("ny", ny_stats, "NY_ROCHESTER_CODE_ENFORCEMENT", "City of Rochester NBD Code Enforcement"),
    ):
        if not stats_map:
            continue
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {jur}_bbl_stats (
                    jurisdiction, bbl, violations_total, violations_open, last_violation_date, updated_at
                )
                VALUES %s
                ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                    violations_total = GREATEST(COALESCE({jur}_bbl_stats.violations_total, 0), EXCLUDED.violations_total),
                    violations_open = GREATEST(COALESCE({jur}_bbl_stats.violations_open, 0), EXCLUDED.violations_open),
                    last_violation_date = GREATEST({jur}_bbl_stats.last_violation_date, EXCLUDED.last_violation_date),
                    updated_at = NOW();
                """,
                [(jur, b, s["vt"], s["vo"], s["last_v"]) for b, s in stats_map.items()],
                template="(%s, %s, %s, %s, %s::date, NOW())",
                page_size=2500,
            )
        conn.commit()
        _record_status(
            conn,
            src_key,
            "city_enforcement",
            {
                "matched_records": matched[jur],
                "matched_parcels": len(stats_map),
                "by_city": {k: v for k, v in matched_by_city.items()},
                "message": f"Matched {matched[jur]:,} {desc} records across {len(stats_map):,} parcels.",
            },
        )
        log(f"  ✓ Matched {matched[jur]:,} {desc} records across {len(stats_map):,} `{jur}` parcels ({dict(matched_by_city)}).")
    return matched


def main() -> int:
    parser = argparse.ArgumentParser(description="Download and additively import Spot Expansion V3 into Postgres.")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--gcloud-config", default=DEFAULT_GCLOUD_CONFIG)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-oh-main", action="store_true", help="Skip oh_expansion_parcels.csv.gz if already imported")
    parser.add_argument("--only-overlays", action="store_true", help="Only re-import FL/NY code enforcement overlays")
    parser.add_argument("--only-ny-rochester", action="store_true", help="Only import NY Rochester parcels + overlays")
    args = parser.parse_args()

    if not args.skip_download:
        log(f"Downloading Spot V3 artifacts from gs://{args.bucket}/spot_expansion_v3/ ...")
        download_from_gcs(args.project, args.bucket, args.gcloud_config)

    conn = psycopg2.connect(_resolve_db_url())
    try:
        if args.only_overlays:
            ov = import_fl_ny_overlays(conn, os.path.join(LOCAL_DIR, "fl_ny_code_overlays.csv.gz"))
            log(f"=== OVERLAY IMPORT COMPLETE: {ov} ===")
            return 0
        if args.only_ny_rochester:
            ny_n = import_additive_parcels(conn, "ny", os.path.join(LOCAL_DIR, "ny_rochester_parcels.csv.gz"))
            ov = import_fl_ny_overlays(conn, os.path.join(LOCAL_DIR, "fl_ny_code_overlays.csv.gz"))
            log(f"=== NY ROCHESTER IMPORT COMPLETE: NY +{ny_n:,}, Overlays={ov} ===")
            return 0
        oh_n = 0
        if not args.skip_oh_main:
            oh_n += import_additive_parcels(conn, "oh", os.path.join(LOCAL_DIR, "oh_expansion_parcels.csv.gz"))
        else:
            _record_status(conn, "OH_FRANKLIN_COUNTY_AUDITOR_CAMA", "state_cama", {"loaded": 444034, "jurisdiction": "OH"})
            _record_status(conn, "OH_HAMILTON_COUNTY_CAGIS_CAMA", "state_cama", {"loaded": 278399, "jurisdiction": "OH"})
        oh_n += import_additive_parcels(conn, "oh", os.path.join(LOCAL_DIR, "oh_lucas_parcels.csv.gz"))
        mn_n = import_additive_parcels(conn, "mn", os.path.join(LOCAL_DIR, "mn_expansion_parcels.csv.gz"))
        mn_n += import_additive_parcels(conn, "mn", os.path.join(LOCAL_DIR, "mn_olmsted_parcels.csv.gz"))
        il_n = import_additive_parcels(conn, "il", os.path.join(LOCAL_DIR, "il_lake_parcels.csv.gz"))
        ov = import_fl_ny_overlays(conn, os.path.join(LOCAL_DIR, "fl_ny_code_overlays.csv.gz"))
        log(f"=== IMPORT-V3 COMPLETE: OH +{oh_n:,}, MN +{mn_n:,}, IL +{il_n:,}, Overlays={ov} ===")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
