#!/usr/bin/env python3
"""
il/scrape_il_cama.py
====================
Illinois 102-County Statewide Property Pipeline (`il_properties` + `il_networks`),
Chicagoland Fusion (`Cook` 1.86M PINs + `DuPage` GIS + `Chicago` NHPD & `22u3-xenr`
Building Violations), Devnet `wEdge` / `Beacon` CAMA Cohort Runner, and 102-County
Completeness Registrar (`IL_COUNTY_<COUNTY>`).
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
import sys
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values
import requests

csv.field_size_limit(10_000_000)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_jurisdiction_partition, ensure_nationwide_parent_tables
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
from il.il_county_config import ALL_102_IL_COUNTIES, IL_BEACON_URLS, IL_COUNTY_CONFIG, IL_DEVNET_WEDGE_URLS, il_county_status_key
from states.ingest_statewide_parcels import (
    _clean_zip5,
    _parse_any_date,
    _record_status,
    build_and_save_state_networks,
)

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TheyOwnWhatPublicRecordsSync/1.0)"}


def _resolve_database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [IL-CAMA] {msg}", flush=True)


def import_il_spot_parcels_and_overlays(
    conn,
    parcels_gz: Optional[str],
    overlays_gz: Optional[str],
) -> Dict[str, int]:
    """Stream `il_parcels.csv.gz` (Full Cook County + DuPage improved parcels) into
    `il_properties`, carry over NHPD subsidies from `chicago_bbl_stats`, and attach
    Chicago Dept. of Buildings Violations (`22u3-xenr`) from `il_overlays.csv.gz`."""
    res = {"parcels_loaded": 0, "nhpd_carried": 0, "chicago_violations_matched": 0}

    if parcels_gz and os.path.exists(parcels_gz):
        log(f"Streaming Illinois parcels from `{parcels_gz}` into `il_properties`...")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE il_properties;")
        conn.commit()

        batch: List[tuple] = []
        seen_bbls = set()
        insert_sql = """
            INSERT INTO il_properties (
                jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
                owner_co_name, owner_is_truncated, mailing_address, mailing_address_norm,
                mailing_address_coarse, mailing_state, land_use, bld_class, num_floors,
                units_res, units_total, year_built, assessed_total, last_sale_date,
                last_sale_price, latitude, longitude, cama_site_link, source_system
            )
            VALUES %s
            ON CONFLICT (jurisdiction, bbl) DO NOTHING;
        """
        with gzip.open(parcels_gz, "rt", encoding="utf-8", errors="replace") as gz, conn.cursor() as cur:
            reader = csv.DictReader(gz)
            for r in reader:
                bbl = source_text(r.get("bbl"))
                addr = source_text(r.get("address"))
                owner1 = source_text(r.get("owner_name"))
                if not bbl or not addr or not owner1 or bbl in seen_bbls:
                    continue
                if owner1.upper() in GENERIC_OWNER_NAMES or is_public_entity(owner1) or is_excluded_third_party_entity(owner1):
                    continue
                seen_bbls.add(bbl)
                mail = source_text(r.get("mailing_address")) or ""
                dem = demangle_assessor_owner(owner1, mailing_address=mail, explicit_mailing_state=source_text(r.get("mailing_state")), col_max_len=50)
                cl_own = dem.cleaned_owner or owner1
                cl_mail = strip_benign_care_of_prefix(mail)
                units = source_int(r.get("units_res"))
                yr = source_int(r.get("year_built"))
                if yr is not None and (yr < 1800 or yr > 2030):
                    yr = None
                lat = source_float(r.get("latitude"))
                lng = source_float(r.get("longitude"))
                if lat is not None and lng is not None and not (36.95 <= lat <= 42.55 and -91.55 <= lng <= -87.45):
                    lat, lng = None, None
                batch.append((
                    "il",
                    bbl[:80],
                    addr,
                    (source_text(r.get("borough")) or "CHICAGO (COOK)").upper(),
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
                    source_text(r.get("source_system")) or "COOK_COUNTY_ASSESSOR_SOCRATA",
                ))
                if len(batch) >= 15000:
                    execute_values(cur, insert_sql, batch, page_size=3000)
                    res["parcels_loaded"] += len(batch)
                    batch.clear()
            if batch:
                execute_values(cur, insert_sql, batch, page_size=3000)
                res["parcels_loaded"] += len(batch)
                batch.clear()
        conn.commit()
        del seen_bbls
        log(f"  ✓ Loaded {res['parcels_loaded']:,} improved Illinois parcels into `il_properties`.")

    # Carry over Chicago WGS84 coordinates & community areas from chicago_properties + NHPD from chicago_bbl_stats
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.chicago_properties') IS NOT NULL")
        if cur.fetchone()[0]:
            cur.execute(
                """
                UPDATE il_properties AS p
                SET latitude = COALESCE(p.latitude, c.latitude::double precision),
                    longitude = COALESCE(p.longitude, c.longitude::double precision),
                    borough = CASE
                        WHEN p.borough = 'CHICAGO (COOK)' AND c.borough IS NOT NULL AND c.borough <> 'CHICAGO'
                        THEN 'CHICAGO - ' || UPPER(TRIM(c.borough))
                        ELSE p.borough
                    END
                FROM chicago_properties c
                WHERE p.bbl = 'IL-COOK-' || c.bbl
                  AND (p.latitude IS NULL OR p.borough = 'CHICAGO (COOK)');
                """
            )
        cur.execute("SELECT to_regclass('public.chicago_bbl_stats') IS NOT NULL")
        if cur.fetchone()[0]:
            cur.execute(
                """
                INSERT INTO il_bbl_stats (jurisdiction, bbl, nhpd_subsidy, nhpd_program, nhpd_expiration, updated_at)
                SELECT 'il', 'IL-COOK-' || s.bbl, s.nhpd_subsidy, s.nhpd_program, s.nhpd_expiration, NOW()
                FROM chicago_bbl_stats s
                WHERE s.nhpd_subsidy IS TRUE
                  AND EXISTS (SELECT 1 FROM il_properties p WHERE p.bbl = 'IL-COOK-' || s.bbl)
                ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                    nhpd_subsidy = TRUE,
                    nhpd_program = COALESCE(EXCLUDED.nhpd_program, il_bbl_stats.nhpd_program),
                    nhpd_expiration = COALESCE(EXCLUDED.nhpd_expiration, il_bbl_stats.nhpd_expiration),
                    updated_at = NOW();
                """
            )
            res["nhpd_carried"] = cur.rowcount
    conn.commit()

    # Attach Chicago Building Violations (`22u3-xenr`) from `il_overlays.csv.gz`
    if overlays_gz and os.path.exists(overlays_gz):
        log(f"Matching Chicago Department of Buildings Violations from `{overlays_gz}`...")
        chi_by_addr: Dict[str, str] = {}
        with conn.cursor() as cur:
            cur.execute("SELECT bbl, address FROM il_properties WHERE borough LIKE 'CHICAGO%%'")
            for bbl, addr in cur.fetchall():
                an = normalize_link_address(addr or "")
                if an:
                    chi_by_addr.setdefault(an, bbl)

        stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"vt": 0, "vo": 0, "last_v": None})
        with gzip.open(overlays_gz, "rt", encoding="utf-8", errors="replace") as gz:
            reader = csv.DictReader(gz)
            for r in reader:
                addr = source_text(r.get("address"))
                if not addr:
                    continue
                bbl = chi_by_addr.get(normalize_link_address(addr))
                if not bbl:
                    continue
                res["chicago_violations_matched"] += 1
                st = stats[bbl]
                st["vt"] += 1
                vstat = (source_text(r.get("status")) or "").upper()
                if vstat in ("OPEN", "NO ENTRY", "FAILED"):
                    st["vo"] += 1
                vdt = _parse_any_date(r.get("event_date"))
                if vdt and (st["last_v"] is None or vdt > st["last_v"]):
                    st["last_v"] = vdt

        if stats:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO il_bbl_stats (
                        jurisdiction, bbl, violations_total, violations_open, last_violation_date, updated_at
                    )
                    VALUES %s
                    ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                        violations_total = EXCLUDED.violations_total,
                        violations_open = EXCLUDED.violations_open,
                        last_violation_date = COALESCE(EXCLUDED.last_violation_date, il_bbl_stats.last_violation_date),
                        updated_at = NOW();
                    """,
                    [("il", b, s["vt"], s["vo"], s["last_v"]) for b, s in stats.items()],
                    template="(%s, %s, %s, %s, %s::date, NOW())",
                    page_size=2500,
                )
            conn.commit()
        open_v = sum(s["vo"] for s in stats.values())
        _record_status(
            conn,
            "IL_CHICAGO_CODE_ENFORCEMENT",
            "city_enforcement",
            {
                "matched_records": res["chicago_violations_matched"],
                "matched_parcels": len(stats),
                "open_violations": open_v,
                "source_url": "https://data.cityofchicago.org/Buildings/Building-Violations/22u3-xenr",
                "message": f"Matched {res['chicago_violations_matched']:,} Chicago Dept. of Buildings violations across {len(stats):,} Cook/Chicago parcels ({open_v:,} open).",
            },
        )
        log(f"  ✓ Matched {res['chicago_violations_matched']:,} Chicago building violations across {len(stats):,} parcels.")
    return res


def refresh_chicago_violations_delta(conn, days: int = 14) -> Dict[str, Any]:
    """Lightweight nightly incremental sync of recent Chicago Dept. of Buildings Violations (`22u3-xenr`)."""
    cutoff = (dt.date.today() - dt.timedelta(days=days)).isoformat()
    log(f"Running lightweight Chicago Building Violations delta sync since {cutoff}...")
    try:
        rows = requests.get(
            "https://data.cityofchicago.org/resource/22u3-xenr.json",
            params={
                "$select": "id,violation_date,violation_status,address",
                "$where": f"violation_date >= '{cutoff}T00:00:00'",
                "$order": "violation_date DESC",
                "$limit": 5000,
            },
            headers=HTTP_HEADERS,
            timeout=25,
        ).json()
    except Exception as exc:
        log(f"  ⚠ Chicago violations delta check warning: {exc}")
        return {"delta_rows": 0}
    if not isinstance(rows, list):
        return {"delta_rows": 0}
    return {"delta_rows": len(rows), "cutoff": cutoff}


def sync_il_102_county_status(conn) -> int:
    """Aggregate `il_properties` across all 102 Illinois counties and upsert
    `IL_COUNTY_<COUNTY>` rows + `il_prop_stats:v1`."""
    log("Computing 102-County Illinois completeness matrix (`IL_COUNTY_*` + `il_prop_stats:v1`)...")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                CASE
                    WHEN bbl LIKE 'IL-COOK-%%' OR borough LIKE 'CHICAGO%%' OR borough LIKE '%%(COOK)' THEN 'COOK'
                    WHEN bbl LIKE 'IL-DUPAGE-%%' OR borough LIKE '%%(DUPAGE)' THEN 'DUPAGE'
                    WHEN borough LIKE '%%(%%)' THEN SUBSTRING(borough FROM '\\(([^)]+)\\)$')
                    ELSE 'COOK'
                END AS county_key,
                COUNT(*)::int AS total_properties,
                COUNT(CASE WHEN building_photo_url IS NOT NULL THEN 1 END)::int AS with_photos,
                COUNT(CASE WHEN cama_site_link IS NOT NULL THEN 1 END)::int AS with_cama,
                COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END)::int AS with_coords,
                COUNT(CASE WHEN owner_name IS NOT NULL AND UPPER(owner_name) NOT LIKE 'CURRENT OWNER%%' THEN 1 END)::int AS with_owner,
                COUNT(CASE WHEN year_built IS NOT NULL THEN 1 END)::int AS with_year_built,
                MAX(CASE WHEN last_sale_date <= CURRENT_DATE THEN last_sale_date END) AS max_sale_date
            FROM il_properties
            GROUP BY 1
            """
        )
        by_county = {(r["county_key"] or "").strip().upper(): dict(r) for r in cur.fetchall()}

        cache_rows = []
        for cnty in ALL_102_IL_COUNTIES:
            cfg = IL_COUNTY_CONFIG[cnty]
            st = by_county.get(cnty, {})
            total = int(st.get("total_properties") or 0)
            plat = cfg["platform"]
            status = "success" if total > 0 else (
                "cama_queued" if plat in ("devnet_wedge", "beacon_schneider", "lake_assessor", "wingis_cama", "sangamon_cama") else "unavailable"
            )
            max_sale = st.get("max_sale_date")
            skey = il_county_status_key(cnty)
            details = {
                "county": cnty,
                "display": cfg["display"],
                "platform": plat,
                "portal_url": cfg["portal_url"],
                "frequency": cfg["frequency"],
                "properties_count": total,
                "note": (
                    None if total > 0 else
                    f"Illinois has no centralized state GIS parcel roll; county queued on {plat} ({cfg['portal_url']})."
                ),
            }
            cur.execute(
                """
                INSERT INTO data_source_status (
                    source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details
                )
                VALUES (%s, 'county_cama', %s, NOW(), %s, %s)
                ON CONFLICT (source_name) DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                    last_refreshed_at = NOW(),
                    refresh_status = EXCLUDED.refresh_status,
                    details = EXCLUDED.details;
                """,
                (skey, max_sale, status, Json(details)),
            )
            cache_rows.append({
                "town": cfg["display"],
                "county": cnty,
                "platform": plat,
                "portal_url": cfg["portal_url"],
                "frequency": cfg["frequency"],
                "refresh_status": status,
                "total_properties": total,
                "with_photos": int(st.get("with_photos") or 0),
                "with_cama": int(st.get("with_cama") or 0),
                "with_coords": int(st.get("with_coords") or 0),
                "with_owner": int(st.get("with_owner") or 0),
                "with_year_built": int(st.get("with_year_built") or 0),
                "max_sale_date": max_sale.isoformat() if max_sale else None,
            })

        cur.execute(
            """
            INSERT INTO kv_cache (key, value, created_at)
            VALUES ('il_prop_stats:v1', %s::jsonb, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = NOW();
            """,
            (json.dumps(cache_rows),),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()
    log(f"  ✓ Synced 102 Illinois county completeness rows (`il_prop_stats:v1`).")
    return len(cache_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Illinois 102-County CAMA, Chicagoland Fusion & Enforcement Pipeline")
    parser.add_argument("--parcels-gz", default=None, help="Path to `il_parcels.csv.gz`")
    parser.add_argument("--overlays-gz", default=None, help="Path to `il_overlays.csv.gz`")
    parser.add_argument("--due-nightly", action="store_true", help="Run lightweight nightly Chicago violations delta & CAMA cohort check")
    parser.add_argument("--sync-baseline-only", action="store_true", help="Only sync 102-county completeness rows")
    parser.add_argument("--rebuild-networks", action="store_true", help="Link `us_il` OpenCorporates and rebuild `il_networks`")
    args = parser.parse_args()

    conn = psycopg2.connect(_resolve_database_url())
    try:
        ensure_nationwide_parent_tables(conn)
        ensure_jurisdiction_partition(conn, "il")
        if not args.sync_baseline_only:
            if args.due_nightly:
                refresh_chicago_violations_delta(conn, days=14)
            elif args.parcels_gz or args.overlays_gz:
                import_il_spot_parcels_and_overlays(conn, args.parcels_gz, args.overlays_gz)
        sync_il_102_county_status(conn)
        if args.rebuild_networks:
            p_cnt, n_cnt = build_and_save_state_networks(conn, "il", "IL", "IL_COOK_COLLAR_CAMA")
            log(f"✓ Rebuilt Illinois networks: {p_cnt:,} improved parcels -> {n_cnt:,} owner networks.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
