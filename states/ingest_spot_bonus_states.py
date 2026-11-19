#!/usr/bin/env python3
"""
states/ingest_spot_bonus_states.py
==================================
Streams GCP Spot-crunched improved parcel spines into PostgreSQL partitions and
builds OpenCorporates-linked ownership networks for:

  - `mt` (Montana — All 56 Counties via Montana MSDI / DOR ORION Cadastral,
          `TotalBuildingValue > 0`, linked to `us_mt`)
  - `mn` (Minnesota — Twin Cities Metro unifying `minneapolis_properties`
          Active Rental Licenses, owner emails, contacts & violations with
          Hennepin County & Ramsey County / St. Paul CAMA, linked to `us_mn`)
  - `oh` (Ohio — Cuyahoga County / Greater Cleveland Auditor CAMA,
          `certified_tax_building > 0`, linked to `us_oh`)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

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
from states.ingest_statewide_parcels import (
    _clean_zip5,
    _parse_any_date,
    build_and_save_state_networks,
)

BONUS_BOUNDS: Dict[str, Tuple[float, float, float, float]] = {
    "mt": (44.30, 49.05, -116.10, -104.00),
    "mn": (43.45, 49.40, -97.30, -89.45),
    "oh": (38.35, 42.05, -84.85, -80.50),
}


def _resolve_database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [BONUS-STATES] {msg}", flush=True)


def _stream_gz_into_partition(conn, jur: str, gz_path: str) -> int:
    log(f"Streaming `{gz_path}` into `{jur}_properties`...")
    ensure_jurisdiction_partition(conn, jur)
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {jur}_properties;")
    conn.commit()

    min_lat, max_lat, min_lon, max_lon = BONUS_BOUNDS[jur]
    batch: List[tuple] = []
    seen = set()
    loaded = 0
    insert_sql = f"""
        INSERT INTO {jur}_properties (
            jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
            owner_co_name, owner_is_truncated, mailing_address, mailing_address_norm,
            mailing_address_coarse, mailing_state, land_use, bld_class, num_floors,
            units_res, units_total, year_built, assessed_total, last_sale_date,
            last_sale_price, latitude, longitude, cama_site_link, source_system
        )
        VALUES %s
        ON CONFLICT (jurisdiction, bbl) DO NOTHING;
    """
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as gz, conn.cursor() as cur:
        reader = csv.DictReader(gz)
        for r in reader:
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
            if yr is not None and (yr < 1800 or yr > 2030):
                yr = None
            lat = source_float(r.get("latitude"))
            lng = source_float(r.get("longitude"))
            if lat is not None and lng is not None and not (min_lat <= lat <= max_lat and min_lon <= lng <= max_lon):
                lat, lng = None, None
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
                source_text(r.get("source_system")) or f"{jur.upper()}_COUNTY_CAMA",
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
    log(f"  ✓ Loaded {loaded:,} improved `{jur}` parcels.")
    return loaded


def unify_minneapolis_into_mn(conn) -> Dict[str, int]:
    """Fuse `minneapolis_properties` (Active Rental Licenses, licensed unit counts,
    owner emails, registrations, contacts & code violations/NHPD) onto `mn_*`
    (`MN-HEN-<PID>` / normalized Minneapolis street address)."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.minneapolis_properties') IS NOT NULL")
        if not cur.fetchone()[0]:
            return {}

    log("Unifying `minneapolis_properties` (Minneapolis Active Rental Licenses & Enforcement) into `mn_*`...")
    mpls_by_addr: Dict[str, str] = {}
    mpls_by_pid: Dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT bbl, address FROM mn_properties WHERE borough ILIKE 'MINNEAPOLIS%%'")
        for bbl, addr in cur.fetchall():
            raw_pid = bbl.replace("MN-HEN-", "").lstrip("0")
            if raw_pid:
                mpls_by_pid[raw_pid] = bbl
            an = normalize_link_address(addr or "")
            if an:
                mpls_by_addr.setdefault(an, bbl)

    updates = []
    inserts = []
    old_to_mn_bbl: Dict[str, str] = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT bbl, address, borough, zip_code, owner_name, owner_name_norm,
                   mailing_address, owner_email, owner_email_norm, units_res,
                   land_use, bld_class, latitude, longitude,
                   compliance_active, compliance_record_id, compliance_expiration
            FROM minneapolis_properties
            WHERE bbl IS NOT NULL AND address IS NOT NULL AND owner_name IS NOT NULL
            """
        )
        for r in cur.fetchall():
            apn = (r["bbl"] or "").strip()
            pid_clean = apn.replace("-", "").lstrip("0")
            an = normalize_link_address(r["address"] or "")
            target_bbl = mpls_by_pid.get(pid_clean) or (mpls_by_addr.get(an) if an else None)
            if target_bbl:
                old_to_mn_bbl[apn] = target_bbl
                updates.append((
                    r.get("owner_email"),
                    r.get("owner_email_norm"),
                    source_int(r.get("units_res")),
                    bool(r.get("compliance_active", True)),
                    r.get("compliance_record_id"),
                    r.get("compliance_expiration"),
                    target_bbl,
                ))
            else:
                new_bbl = f"MN-MPLS-{apn}"[:80]
                old_to_mn_bbl[apn] = new_bbl
                mail = r.get("mailing_address") or ""
                cl_mail = strip_benign_care_of_prefix(mail)
                inserts.append((
                    "mn",
                    new_bbl,
                    r["address"],
                    "MINNEAPOLIS (HENNEPIN)",
                    _clean_zip5(r.get("zip_code")),
                    r["owner_name"],
                    r.get("owner_name_norm") or normalize_owner_name(r["owner_name"]),
                    mail,
                    normalize_link_address(cl_mail),
                    normalize_mailing_address_coarse(cl_mail),
                    r.get("owner_email"),
                    r.get("owner_email_norm"),
                    r.get("land_use"),
                    r.get("bld_class"),
                    source_int(r.get("units_res")),
                    source_int(r.get("units_res")),
                    source_float(r.get("latitude")),
                    source_float(r.get("longitude")),
                    bool(r.get("compliance_active", True)),
                    r.get("compliance_record_id"),
                    r.get("compliance_expiration"),
                    "MINNEAPOLIS_RENTAL_LICENSES",
                ))

    with conn.cursor() as cur:
        if updates:
            execute_values(
                cur,
                """
                UPDATE mn_properties AS p
                SET owner_email = COALESCE(v.em, p.owner_email),
                    owner_email_norm = COALESCE(v.em_norm, p.owner_email_norm),
                    units_res = CASE WHEN COALESCE(v.u, 0) > COALESCE(p.units_res, 0) THEN v.u ELSE p.units_res END,
                    units_total = CASE WHEN COALESCE(v.u, 0) > COALESCE(p.units_total, 0) THEN v.u ELSE p.units_total END,
                    compliance_active = v.cact,
                    compliance_record_id = COALESCE(v.cid, p.compliance_record_id),
                    compliance_expiration = COALESCE(v.cexp, p.compliance_expiration),
                    updated_at = NOW()
                FROM (VALUES %s) AS v(em, em_norm, u, cact, cid, cexp, bbl)
                WHERE p.bbl = v.bbl;
                """,
                updates,
                template="(%s, %s, %s::numeric, %s::boolean, %s, %s::date, %s)",
                page_size=2500,
            )
        if inserts:
            execute_values(
                cur,
                """
                INSERT INTO mn_properties (
                    jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
                    mailing_address, mailing_address_norm, mailing_address_coarse,
                    owner_email, owner_email_norm, land_use, bld_class, units_res, units_total,
                    latitude, longitude, compliance_active, compliance_record_id,
                    compliance_expiration, source_system
                )
                VALUES %s
                ON CONFLICT (jurisdiction, bbl) DO NOTHING;
                """,
                inserts,
                page_size=2500,
            )

        # Carry over Minneapolis registrations, contacts & bbl_stats (violations + NHPD)
        cur.execute("SELECT to_regclass('public.minneapolis_bbl_stats') IS NOT NULL")
        if cur.fetchone()[0] and old_to_mn_bbl:
            cur.execute(
                """
                SELECT bbl, violations_total, violations_open, last_violation_date,
                       nhpd_subsidy, nhpd_program, nhpd_expiration
                FROM minneapolis_bbl_stats
                """
            )
            st_rows = {}
            for obbl, vt, vo, lvd, nsub, nprog, nexp in cur.fetchall():
                tbbl = old_to_mn_bbl.get((obbl or "").strip())
                if tbbl:
                    st_rows[tbbl] = ("mn", tbbl, vt or 0, vo or 0, lvd, bool(nsub), nprog, nexp)
            if st_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO mn_bbl_stats (
                        jurisdiction, bbl, violations_total, violations_open, last_violation_date,
                        nhpd_subsidy, nhpd_program, nhpd_expiration, updated_at
                    )
                    VALUES %s
                    ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                        violations_total = GREATEST(mn_bbl_stats.violations_total, EXCLUDED.violations_total),
                        violations_open = GREATEST(mn_bbl_stats.violations_open, EXCLUDED.violations_open),
                        last_violation_date = COALESCE(EXCLUDED.last_violation_date, mn_bbl_stats.last_violation_date),
                        nhpd_subsidy = mn_bbl_stats.nhpd_subsidy OR EXCLUDED.nhpd_subsidy,
                        nhpd_program = COALESCE(EXCLUDED.nhpd_program, mn_bbl_stats.nhpd_program),
                        nhpd_expiration = COALESCE(EXCLUDED.nhpd_expiration, mn_bbl_stats.nhpd_expiration),
                        updated_at = NOW();
                    """,
                    list(st_rows.values()),
                    template="(%s, %s, %s, %s, %s::date, %s::boolean, %s, %s::date, NOW())",
                    page_size=2000,
                )
    conn.commit()
    log(f"  ✓ Fused Minneapolis rental licenses into `mn_*` ({len(updates):,} CAMA parcels enriched, {len(inserts):,} added).")
    return {"mpls_enriched": len(updates), "mpls_inserted": len(inserts)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest GCP Spot bonus states (`mt`, `mn`, `oh`)")
    parser.add_argument("--spot-dir", default=os.path.join(REPO_ROOT, "data", "spot_parcels_v2"), help="Directory holding `<jur>_parcels.csv.gz`")
    parser.add_argument("--states", nargs="+", default=["mt", "mn", "oh"], choices=["mt", "mn", "oh"])
    parser.add_argument("--rebuild-only", action="store_true")
    args = parser.parse_args()

    tags = {
        "mt": ("MT", "MT_MSDI_ORION_CADASTRAL"),
        "mn": ("MN", "MN_TWIN_CITIES_CAMA_AND_RENTAL_LICENSES"),
        "oh": ("OH", "OH_CUYAHOGA_AUDITOR_CAMA"),
    }
    conn = psycopg2.connect(_resolve_database_url())
    try:
        ensure_nationwide_parent_tables(conn)
        for jur in args.states:
            st_code, src_tag = tags[jur]
            gz_path = os.path.join(args.spot_dir, f"{jur}_parcels.csv.gz")
            if not args.rebuild_only:
                if not os.path.exists(gz_path):
                    log(f"⚠ Missing `{gz_path}`; skipping `{jur}`.")
                    continue
                _stream_gz_into_partition(conn, jur, gz_path)
                if jur == "mn":
                    unify_minneapolis_into_mn(conn)
            p_cnt, n_cnt = build_and_save_state_networks(conn, jur, st_code, src_tag)
            log(f"✓ [{st_code}] Complete: {p_cnt:,} improved parcels -> {n_cnt:,} owner networks.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
