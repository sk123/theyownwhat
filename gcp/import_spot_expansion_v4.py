#!/usr/bin/env python3
"""
gcp/import_spot_expansion_v4.py
===============================
Downloads and additively imports Spot Expansion V4 artifacts from
`gs://renter-help-staging-20261119/spot_expansion_v4/` into local PostgreSQL
(NEVER truncating existing counties or properties):

  1. `oh_v4_parcels.csv.gz`         -> `oh_properties` (+Stark, +Lake, +Delaware)
  2. `mn_v4_parcels.csv.gz`         -> `mn_properties` (+Washington, +Scott, +Carver — 7-County Twin Cities Complete)
  3. `il_v4_parcels.csv.gz`         -> `il_properties` (+Kane, +McHenry — 5-County Chicagoland)
  4. `ny_syracuse_overlays.csv.gz`  -> `ny_bbl_stats` + `ny_properties.compliance_active` (Syracuse Code & Rental Registry)
  5. `geocoding_backfill_v4.csv.gz` -> `latitude`/`longitude` (+ Baltimore `cama_site_link` / `units_res`)
       across `dc_properties`, `baltimore_properties`, `mt_properties`, and `mn_properties`.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import datetime as dt
import gzip
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extras import execute_values

csv.field_size_limit(10_000_000)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.populate_cities import normalize_link_address, source_float, source_int, source_text
from gcp.import_spot_expansion_v3 import (
    DEFAULT_BUCKET,
    DEFAULT_GCLOUD_CONFIG,
    DEFAULT_PROJECT,
    GCLOUD_IMAGE,
    _base_addr_key,
    _record_status,
    _resolve_db_url,
    import_additive_parcels,
)
from states.ingest_statewide_parcels import _parse_any_date

LOCAL_DIR = os.path.join(REPO_ROOT, "data", "spot_expansion_v4")

GEO_BOUNDS: Dict[str, Tuple[float, float, float, float]] = {
    "dc": (38.75, 39.05, -77.15, -76.85),
    "baltimore": (39.15, 39.42, -76.78, -76.48),
    "mt": (44.30, 49.05, -116.10, -104.00),
    "mn": (43.45, 49.40, -97.30, -89.45),
}


def log(msg: str) -> None:
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] [IMPORT-V4] {msg}", flush=True)


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
        f"gs://{bucket}/spot_expansion_v4/*",
        "/data/spot_expansion_v4/",
        "--quiet",
    ]
    subprocess.run(cmd, check=True, timeout=600)


def _syr_sbl_compact(raw: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (raw or "").upper())


def import_ny_syracuse_overlays(conn, gz_path: str) -> Dict[str, int]:
    if not os.path.exists(gz_path):
        log(f"  Missing `{gz_path}`; skipping Syracuse overlays.")
        return {}
    log(f"Matching Syracuse NY code violations & rental registry from `{os.path.basename(gz_path)}`...")
    by_addr: Dict[str, str] = {}
    by_base_addr: Dict[str, str] = {}
    by_sbl: Dict[str, str] = {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bbl, address
            FROM ny_properties
            WHERE borough ILIKE 'SYRACUSE%%' OR borough ILIKE '%%ONONDAGA%%'
            """
        )
        for bbl, addr in cur.fetchall():
            an = normalize_link_address(addr or "")
            if an:
                by_addr.setdefault(an, bbl)
                bn = _base_addr_key(addr or "")
                if bn:
                    by_base_addr.setdefault(bn, bbl)
            # Match NYS GIS SWIS+SBL or raw SBL suffix
            sbl_clean = _syr_sbl_compact(re.sub(r"^NY-", "", bbl or ""))
            if len(sbl_clean) >= 6:
                by_sbl.setdefault(sbl_clean, bbl)

    code_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"vt": 0, "vo": 0, "last_v": None})
    rental_updates: Dict[str, Tuple[bool, str, Any]] = {}
    matched_code = 0
    matched_rental = 0

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as gz:
        for r in csv.DictReader(gz):
            kind = (r.get("record_kind") or "").strip().lower()
            addr = source_text(r.get("address")) or ""
            phint = source_text(r.get("parcel_hint")) or ""
            bbl = None
            if addr:
                an = normalize_link_address(addr)
                if an:
                    bbl = by_addr.get(an)
                if not bbl:
                    bn = _base_addr_key(addr)
                    if bn:
                        bbl = by_base_addr.get(bn)
            if not bbl and phint:
                bbl = by_sbl.get(_syr_sbl_compact(phint))
            if not bbl:
                continue

            if kind == "code_violation":
                matched_code += 1
                st = code_stats[bbl]
                st["vt"] += 1
                vstat = (source_text(r.get("status")) or "").upper()
                if "OPEN" in vstat or "ACTIVE" in vstat or "VIOLATION" in vstat or "PENDING" in vstat:
                    st["vo"] += 1
                vdt = _parse_any_date(r.get("event_date"))
                if vdt and (st["last_v"] is None or vdt > st["last_v"]):
                    st["last_v"] = vdt
            elif kind == "rental_registry":
                matched_rental += 1
                st_str = (source_text(r.get("status")) or "").upper()
                is_valid = st_str in ("YES", "VALID", "ACTIVE", "TRUE", "REGISTERED")
                exp_dt = _parse_any_date(r.get("event_date"))
                prev = rental_updates.get(bbl)
                if prev is None or (is_valid and not prev[0]):
                    rental_updates[bbl] = (is_valid, source_text(r.get("record_id")) or phint, exp_dt)

    if code_stats:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO ny_bbl_stats (
                    jurisdiction, bbl, violations_total, violations_open, last_violation_date, updated_at
                )
                VALUES %s
                ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                    violations_total = GREATEST(COALESCE(ny_bbl_stats.violations_total, 0), EXCLUDED.violations_total),
                    violations_open = GREATEST(COALESCE(ny_bbl_stats.violations_open, 0), EXCLUDED.violations_open),
                    last_violation_date = GREATEST(ny_bbl_stats.last_violation_date, EXCLUDED.last_violation_date),
                    updated_at = NOW();
                """,
                [("ny", b, s["vt"], s["vo"], s["last_v"]) for b, s in code_stats.items()],
                template="(%s, %s, %s, %s, %s::date, NOW())",
                page_size=2500,
            )
        conn.commit()
        _record_status(
            conn,
            "NY_SYRACUSE_CODE_ENFORCEMENT",
            "city_enforcement",
            {
                "matched_records": matched_code,
                "matched_parcels": len(code_stats),
                "message": f"Matched {matched_code:,} Syracuse Code Violations across {len(code_stats):,} NY parcels.",
            },
        )

    if rental_updates:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE ny_properties p
                SET compliance_active = v.active,
                    compliance_record_id = COALESCE(NULLIF(v.rec_id, ''), p.compliance_record_id),
                    compliance_expiration = COALESCE(v.exp_dt, p.compliance_expiration),
                    updated_at = NOW()
                FROM (VALUES %s) AS v(bbl, active, rec_id, exp_dt)
                WHERE p.jurisdiction = 'ny' AND p.bbl = v.bbl;
                """,
                [(b, act, rid[:80], exp) for b, (act, rid, exp) in rental_updates.items()],
                template="(%s, %s::boolean, %s, %s::date)",
                page_size=2500,
            )
        conn.commit()
        _record_status(
            conn,
            "NY_SYRACUSE_RENTAL_REGISTRY",
            "city_registry",
            {
                "matched_records": matched_rental,
                "matched_parcels": len(rental_updates),
                "message": f"Matched {matched_rental:,} Syracuse Rental Registry entries across {len(rental_updates):,} NY parcels.",
            },
        )

    log(
        f"  ✓ Syracuse NY overlays: {matched_code:,} violations ({len(code_stats):,} parcels), "
        f"{matched_rental:,} rental registry ({len(rental_updates):,} parcels)."
    )
    return {"code": matched_code, "code_parcels": len(code_stats), "rental": matched_rental, "rental_parcels": len(rental_updates)}


def import_geocoding_backfill(conn, gz_path: str) -> Dict[str, int]:
    if not os.path.exists(gz_path):
        log(f"  Missing `{gz_path}`; skipping geocoding backfill.")
        return {}
    log(f"Applying exact WGS84 geocoding & CAMA backfill from `{os.path.basename(gz_path)}`...")
    by_jur: Dict[str, Dict[str, Tuple[float, float, str, Any]]] = defaultdict(dict)
    balt_by_addr: Dict[str, Tuple[float, float, str, Any]] = {}

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as gz:
        for r in csv.DictReader(gz):
            jur = (r.get("jur") or "").strip().lower()
            bbl = source_text(r.get("bbl"))
            lat = source_float(r.get("latitude"))
            lng = source_float(r.get("longitude"))
            if jur not in GEO_BOUNDS or not bbl or lat is None or lng is None:
                continue
            lo_lat, hi_lat, lo_lng, hi_lng = GEO_BOUNDS[jur]
            if not (lo_lat <= lat <= hi_lat and lo_lng <= lng <= hi_lng):
                continue
            cama = source_text(r.get("cama_site_link")) or ""
            units = source_int(r.get("units_res"))
            by_jur[jur].setdefault(bbl, (lat, lng, cama, units))
            if jur == "baltimore":
                # Also index normalized block/lot variants & address fallback
                compact_bl = " ".join(bbl.split())
                by_jur[jur].setdefault(compact_bl, (lat, lng, cama, units))
                ah = normalize_link_address(source_text(r.get("address_hint")) or "")
                if ah:
                    balt_by_addr.setdefault(ah, (lat, lng, cama, units))

    updated: Dict[str, int] = {}
    for jur, mapping in by_jur.items():
        if not mapping:
            continue
        is_city = jur in ("dc", "baltimore")
        rows = [(b, lat, lng, cama, units) for b, (lat, lng, cama, units) in mapping.items()]
        tot_upd = 0
        with conn.cursor() as cur:
            if jur == "baltimore":
                # Check if baltimore_properties has cama_site_link column
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='baltimore_properties'"
                )
                bcols = {x[0] for x in cur.fetchall()}
                has_cama = "cama_site_link" in bcols
                has_units = "units_res" in bcols
                # Match by exact bbl OR whitespace-normalized bbl OR address fallback
                cur.execute("SELECT bbl, address FROM baltimore_properties WHERE latitude IS NULL")
                resolved_balt: List[Tuple[str, float, float, str, Any]] = []
                for db_bbl, db_addr in cur.fetchall():
                    hit = mapping.get(db_bbl) or mapping.get(" ".join((db_bbl or "").split()))
                    if not hit and db_addr:
                        hit = balt_by_addr.get(normalize_link_address(db_addr))
                    if hit:
                        resolved_balt.append((db_bbl, hit[0], hit[1], hit[2], hit[3]))
                for i in range(0, len(resolved_balt), 10000):
                    chunk = resolved_balt[i : i + 10000]
                    set_extra = ""
                    if has_cama:
                        set_extra += ", cama_site_link = COALESCE(NULLIF(p.cama_site_link, ''), NULLIF(v.cama, ''))"
                    if has_units:
                        set_extra += ", units_res = COALESCE(p.units_res, v.units)"
                    execute_values(
                        cur,
                        f"""
                        UPDATE baltimore_properties p
                        SET latitude = v.lat, longitude = v.lng {set_extra}
                        FROM (VALUES %s) AS v(bbl, lat, lng, cama, units)
                        WHERE p.bbl = v.bbl AND p.latitude IS NULL;
                        """,
                        chunk,
                        template="(%s, %s::double precision, %s::double precision, %s, %s::integer)",
                        page_size=2500,
                    )
                    tot_upd += cur.rowcount
            elif is_city:
                for i in range(0, len(rows), 10000):
                    chunk = [(b, lat, lng) for b, lat, lng, _, _ in rows[i : i + 10000]]
                    execute_values(
                        cur,
                        f"""
                        UPDATE {jur}_properties p
                        SET latitude = v.lat, longitude = v.lng
                        FROM (VALUES %s) AS v(bbl, lat, lng)
                        WHERE p.bbl = v.bbl AND p.latitude IS NULL;
                        """,
                        chunk,
                        template="(%s, %s::double precision, %s::double precision)",
                        page_size=2500,
                    )
                    tot_upd += cur.rowcount
            else:
                for i in range(0, len(rows), 15000):
                    chunk = [(b, lat, lng) for b, lat, lng, _, _ in rows[i : i + 15000]]
                    execute_values(
                        cur,
                        f"""
                        UPDATE {jur}_properties p
                        SET latitude = v.lat, longitude = v.lng
                        FROM (VALUES %s) AS v(bbl, lat, lng)
                        WHERE p.jurisdiction = %s AND p.bbl = v.bbl AND p.latitude IS NULL;
                        """.replace("WHERE p.jurisdiction = %s", f"WHERE p.jurisdiction = '{jur}'"),
                        chunk,
                        template="(%s, %s::double precision, %s::double precision)",
                        page_size=3000,
                    )
                    tot_upd += cur.rowcount
        conn.commit()
        updated[jur] = tot_upd
        log(f"  ✓ Backfilled exact WGS84 coordinates on {tot_upd:,} `{jur}` parcel(s).")
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Download and additively import Spot Expansion V4 into Postgres.")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--gcloud-config", default=DEFAULT_GCLOUD_CONFIG)
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()

    if not args.skip_download:
        log(f"Downloading Spot V4 artifacts from gs://{args.bucket}/spot_expansion_v4/ ...")
        download_from_gcs(args.project, args.bucket, args.gcloud_config)

    conn = psycopg2.connect(_resolve_db_url())
    try:
        oh_n = import_additive_parcels(conn, "oh", os.path.join(LOCAL_DIR, "oh_v4_parcels.csv.gz"))
        mn_n = import_additive_parcels(conn, "mn", os.path.join(LOCAL_DIR, "mn_v4_parcels.csv.gz"))
        il_n = import_additive_parcels(conn, "il", os.path.join(LOCAL_DIR, "il_v4_parcels.csv.gz"))
        syr = import_ny_syracuse_overlays(conn, os.path.join(LOCAL_DIR, "ny_syracuse_overlays.csv.gz"))
        geo = import_geocoding_backfill(conn, os.path.join(LOCAL_DIR, "geocoding_backfill_v4.csv.gz"))
        log(f"=== IMPORT-V4 COMPLETE: OH +{oh_n:,}, MN +{mn_n:,}, IL +{il_n:,}, Syracuse={syr}, GeoBackfill={geo} ===")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
