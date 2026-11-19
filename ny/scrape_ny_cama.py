#!/usr/bin/env python3
"""
ny/scrape_ny_cama.py
====================
New York 62-County Unified Pipeline (`NYC` + `57` Non-NYC Counties), Monroe &
Syracuse Open-Data Importer, Reusable SDG `ImageMate Online` (`imo.*`) CAMA
Scraper, and 62-County Completeness Registrar (`NY_COUNTY_<COUNTY>`).

Brings the CT / MA / VA standard to New York State (`ny_properties` + `ny_networks`):
  1. Fuses `nyc_properties` (`768K` NYC PLUTO/HPD buildings across Manhattan,
     Brooklyn, Queens, Bronx & Staten Island), `nyc_hpd_registrations`,
     `nyc_hpd_contacts` (HEADOFFICER / INDIVIDUALOWNER / CORPORATEOWNER), and
     `nyc_bbl_stats` (HPD Open & Class C violations, Litigations, Marshal
     Evictions, Rent Stabilization & NHPD Subsidies) directly into `ny_*`—linking
     NYC owners to their Westchester, Long Island, Hudson Valley & Upstate
     portfolios in one unified 62-county graph!
  2. Streams `ny_monroe_parcels.csv.gz` (Monroe County / Rochester 2025 GIS
     parcels) and `ny_overlays.csv.gz` (Syracuse Rental Registry & Code
     Violations) produced by the GCP Spot Mega-Cruncher.
  3. Provides a reusable SDG `ImageMate Online` (`imo.*`) CAMA scraper engine
     for holdout & upstate NY counties (`--due-nightly` / `--counties`).
  4. Registers honest per-county `NY_COUNTY_<COUNTY>` completeness rows across
     all 62 New York counties (`ny_prop_stats:v1`).
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
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
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
    is_public_or_generic_address,
    load_registrar_address_substrings,
    normalize_link_address,
    normalize_owner_name,
    source_float,
    source_int,
    source_text,
)
from api.shared_utils import (
    is_excluded_third_party_entity,
    looks_like_business_name,
    looks_like_person_owner,
    normalize_business_name,
    normalize_mailing_address_coarse,
    normalize_person_name,
    strip_benign_care_of_prefix,
)
from ny.ny_county_config import ALL_62_NY_COUNTIES, NY_COUNTY_CONFIG, NY_SDG_IMAGEMATE_URLS, ny_county_status_key
from states.ingest_statewide_parcels import (
    _clean_zip5,
    _parse_any_date,
    _record_status,
    build_and_save_state_networks,
    enrich_ny_buffalo_enforcement_and_registry,
)

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TheyOwnWhatPublicRecordsSync/1.0)"}


def _resolve_database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [NY-CAMA] {msg}", flush=True)


def unify_nyc_into_ny(conn) -> Dict[str, int]:
    """Copy all `nyc_properties`, `nyc_hpd_registrations`, `nyc_hpd_contacts`, and
    `nyc_bbl_stats` into `ny_*` in native PostgreSQL SQL, and backfill NYC parcel
    `mailing_address` from verified HPD CORPORATEOWNER / HEADOFFICER / INDIVIDUALOWNER
    business addresses so NYC owners link directly to suburban & upstate NY holdings."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.nyc_properties') IS NOT NULL")
        if not cur.fetchone()[0]:
            log("`nyc_properties` not found; skipping NYC -> NY fusion.")
            return {}

    log("Unifying `nyc_properties` (5 NYC Boroughs: PLUTO + HPD + Evictions) into `ny_*`...")
    with conn.cursor() as cur:
        # 1. Prune any ORPTS 300-series vacant land or nominal unaddressed slivers in ny_properties
        cur.execute(
            """
            DELETE FROM ny_properties
            WHERE bld_class LIKE '3%%'
               OR land_use ILIKE '%%VACANT%%'
               OR (
                   COALESCE(units_res, 0) = 0
                   AND COALESCE(year_built, 0) = 0
                   AND COALESCE(assessed_total, 0) <= 5000
                   AND (address ~ '^0(\\s|$)' OR address ILIKE '%%UNASSIGNED%%')
               );
            """
        )
        pruned_vacant = cur.rowcount

        # 2. Upsert NYC 5-borough buildings into ny_properties (standardizing borough label with county suffix)
        cur.execute(
            """
            INSERT INTO ny_properties (
                jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
                land_use, bld_class, num_floors, units_res, units_total, year_built,
                assessed_total, latitude, longitude, compliance_active, compliance_record_id,
                compliance_expiration, cama_site_link, source_system, updated_at
            )
            SELECT
                'ny',
                p.bbl,
                p.address,
                CASE UPPER(TRIM(COALESCE(p.borough, '')))
                    WHEN 'MANHATTAN' THEN 'MANHATTAN (NEW YORK)'
                    WHEN 'MN' THEN 'MANHATTAN (NEW YORK)'
                    WHEN 'BROOKLYN' THEN 'BROOKLYN (KINGS)'
                    WHEN 'BK' THEN 'BROOKLYN (KINGS)'
                    WHEN 'QUEENS' THEN 'QUEENS (QUEENS)'
                    WHEN 'QN' THEN 'QUEENS (QUEENS)'
                    WHEN 'BRONX' THEN 'BRONX (BRONX)'
                    WHEN 'BX' THEN 'BRONX (BRONX)'
                    WHEN 'STATEN ISLAND' THEN 'STATEN ISLAND (RICHMOND)'
                    WHEN 'SI' THEN 'STATEN ISLAND (RICHMOND)'
                    ELSE COALESCE(NULLIF(UPPER(TRIM(p.borough)), ''), 'NEW YORK CITY (NEW YORK)')
                END,
                p.zip_code,
                p.owner_name,
                p.owner_name_norm,
                p.land_use,
                p.bld_class,
                p.num_floors,
                p.units_res,
                p.units_total,
                p.year_built,
                p.assessed_total,
                p.latitude::double precision,
                p.longitude::double precision,
                COALESCE(p.compliance_active, FALSE),
                p.compliance_record_id,
                p.compliance_expiration,
                'https://zola.planning.nyc.gov/lot/' || SUBSTRING(p.bbl FROM 1 FOR 1) || '/' ||
                    LTRIM(SUBSTRING(p.bbl FROM 2 FOR 5), '0') || '/' ||
                    LTRIM(SUBSTRING(p.bbl FROM 7 FOR 4), '0'),
                'NYC_DOF_PLUTO_HPD',
                NOW()
            FROM nyc_properties p
            WHERE p.bbl IS NOT NULL
              AND p.address IS NOT NULL
              AND p.owner_name IS NOT NULL
              AND COALESCE(p.land_use, '') NOT ILIKE '%%VACANT%%'
              AND COALESCE(p.bld_class, '') NOT LIKE 'V%%'
            ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                owner_name = EXCLUDED.owner_name,
                owner_name_norm = EXCLUDED.owner_name_norm,
                borough = EXCLUDED.borough,
                units_res = COALESCE(EXCLUDED.units_res, ny_properties.units_res),
                units_total = COALESCE(EXCLUDED.units_total, ny_properties.units_total),
                year_built = COALESCE(EXCLUDED.year_built, ny_properties.year_built),
                assessed_total = COALESCE(EXCLUDED.assessed_total, ny_properties.assessed_total),
                compliance_active = EXCLUDED.compliance_active,
                compliance_record_id = EXCLUDED.compliance_record_id,
                compliance_expiration = EXCLUDED.compliance_expiration,
                cama_site_link = EXCLUDED.cama_site_link,
                updated_at = NOW();
            """
        )
        nyc_props_upserted = cur.rowcount

        # 3. Copy NYC HPD Registrations & Contacts into ny_hpd_registrations & ny_hpd_contacts
        cur.execute(
            """
            INSERT INTO ny_hpd_registrations (
                jurisdiction, registration_id, bbl, bin, building_address, building_city,
                building_zip, borough, lifecycle_stage, last_registration_date, registration_end_date
            )
            SELECT
                'ny', r.registration_id, r.bbl, r.bin, r.building_address, r.building_city,
                r.building_zip, r.borough, r.lifecycle_stage, r.last_registration_date, r.registration_end_date
            FROM nyc_hpd_registrations r
            WHERE r.registration_id IS NOT NULL
            ON CONFLICT (jurisdiction, registration_id) DO UPDATE SET
                bbl = EXCLUDED.bbl,
                lifecycle_stage = EXCLUDED.lifecycle_stage,
                last_registration_date = EXCLUDED.last_registration_date,
                registration_end_date = EXCLUDED.registration_end_date,
                updated_at = NOW();
            """
        )
        nyc_regs_copied = cur.rowcount

        cur.execute("DELETE FROM ny_hpd_contacts WHERE registration_id IN (SELECT registration_id FROM nyc_hpd_registrations)")
        cur.execute(
            """
            INSERT INTO ny_hpd_contacts (
                jurisdiction, registration_id, contact_type, corporation_name, corporation_name_norm,
                first_name, last_name, full_name, full_name_norm, business_address,
                business_city, business_state, business_zip, source_tag
            )
            SELECT
                'ny', c.registration_id, c.contact_type, c.corporation_name, c.corporation_name_norm,
                c.first_name, c.last_name, c.full_name, c.full_name_norm, c.business_address,
                c.business_city, c.business_state, c.business_zip, COALESCE(c.source_tag, 'NYC_HPD')
            FROM nyc_hpd_contacts c
            JOIN nyc_hpd_registrations r ON r.registration_id = c.registration_id;
            """
        )
        nyc_contacts_copied = cur.rowcount

        # 4. Backfill NYC parcel `mailing_address` on `ny_properties` from primary HPD owner contact
        #    so NYC HPD office addresses link directly to Westchester/Long Island/Upstate tax-bill addresses!
        cur.execute(
            """
            WITH primary_hpd_addr AS (
                SELECT DISTINCT ON (r.bbl)
                    r.bbl,
                    TRIM(CONCAT_WS(', ',
                        NULLIF(TRIM(c.business_address), ''),
                        NULLIF(TRIM(c.business_city), ''),
                        NULLIF(TRIM(c.business_state), ''),
                        NULLIF(LEFT(TRIM(c.business_zip), 5), '')
                    )) AS full_biz_addr,
                    UPPER(TRIM(c.business_state)) AS biz_state
                FROM nyc_hpd_registrations r
                JOIN nyc_hpd_contacts c ON c.registration_id = r.registration_id
                WHERE r.bbl IS NOT NULL
                  AND c.business_address IS NOT NULL
                  AND TRIM(c.business_address) <> ''
                  AND c.contact_type IN ('CORPORATEOWNER', 'HEADOFFICER', 'INDIVIDUALOWNER')
                ORDER BY r.bbl,
                    CASE c.contact_type
                        WHEN 'CORPORATEOWNER' THEN 1
                        WHEN 'HEADOFFICER' THEN 2
                        ELSE 3
                    END,
                    r.last_registration_date DESC NULLS LAST
            )
            UPDATE ny_properties AS p
            SET mailing_address = pha.full_biz_addr,
                mailing_state = COALESCE(NULLIF(pha.biz_state, ''), p.mailing_state)
            FROM primary_hpd_addr pha
            WHERE p.bbl = pha.bbl
              AND COALESCE(p.mailing_address, '') = '';
            """
        )
        hpd_mail_backfilled = cur.rowcount

        # 5. Copy NYC HPD Violations, Litigations, Marshal Evictions, Rent Stabilization & NHPD into ny_bbl_stats
        cur.execute(
            """
            INSERT INTO ny_bbl_stats (
                jurisdiction, bbl, violations_total, violations_open, violations_class_c,
                violations_class_b, violations_class_a, violations_open_c, last_violation_date,
                litigations_total, litigations_open, litigations_harassment, last_litigation_date,
                evictions_total, last_eviction_date, is_rent_stabilized, rs_units,
                nhpd_subsidy, nhpd_program, nhpd_expiration, updated_at
            )
            SELECT
                'ny', s.bbl, s.violations_total, s.violations_open, s.violations_class_c,
                s.violations_class_b, s.violations_class_a, s.violations_open_c, s.last_violation_date,
                s.litigations_total, s.litigations_open, s.litigations_harassment, s.last_litigation_date,
                s.evictions_total, s.last_eviction_date, s.is_rent_stabilized, s.rs_units,
                s.nhpd_subsidy, s.nhpd_program, s.nhpd_expiration, NOW()
            FROM nyc_bbl_stats s
            WHERE EXISTS (SELECT 1 FROM ny_properties p WHERE p.bbl = s.bbl)
            ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                violations_total = EXCLUDED.violations_total,
                violations_open = EXCLUDED.violations_open,
                violations_class_c = EXCLUDED.violations_class_c,
                violations_class_b = EXCLUDED.violations_class_b,
                violations_class_a = EXCLUDED.violations_class_a,
                violations_open_c = EXCLUDED.violations_open_c,
                last_violation_date = EXCLUDED.last_violation_date,
                litigations_total = EXCLUDED.litigations_total,
                litigations_open = EXCLUDED.litigations_open,
                litigations_harassment = EXCLUDED.litigations_harassment,
                last_litigation_date = EXCLUDED.last_litigation_date,
                evictions_total = EXCLUDED.evictions_total,
                last_eviction_date = EXCLUDED.last_eviction_date,
                is_rent_stabilized = EXCLUDED.is_rent_stabilized,
                rs_units = EXCLUDED.rs_units,
                nhpd_subsidy = EXCLUDED.nhpd_subsidy OR ny_bbl_stats.nhpd_subsidy,
                nhpd_program = COALESCE(EXCLUDED.nhpd_program, ny_bbl_stats.nhpd_program),
                nhpd_expiration = COALESCE(EXCLUDED.nhpd_expiration, ny_bbl_stats.nhpd_expiration),
                updated_at = NOW();
            """
        )
        nyc_stats_copied = cur.rowcount

        # Normalize backfilled NYC mailing addresses in Python for cross-county Union-Find matching
        cur.execute(
            """
            SELECT bbl, mailing_address
            FROM ny_properties
            WHERE source_system = 'NYC_DOF_PLUTO_HPD'
              AND mailing_address IS NOT NULL
              AND mailing_address <> ''
              AND mailing_address_norm IS NULL
            """
        )
        norm_updates = []
        for bbl, maddr in cur.fetchall():
            cl = strip_benign_care_of_prefix(maddr)
            norm_updates.append((
                normalize_link_address(cl),
                normalize_mailing_address_coarse(cl),
                bbl,
            ))
        if norm_updates:
            execute_values(
                cur,
                """
                UPDATE ny_properties AS p
                SET mailing_address_norm = v.mnorm,
                    mailing_address_coarse = v.mcoarse
                FROM (VALUES %s) AS v(mnorm, mcoarse, bbl)
                WHERE p.bbl = v.bbl;
                """,
                norm_updates,
                page_size=5000,
            )

        # Backfill NYS GIS Tax Parcel Explorer card links on non-NYC parcels missing cama_site_link
        cur.execute(
            """
            UPDATE ny_properties
            SET cama_site_link = 'https://gisservices.its.ny.gov/arcgis/apps/webappviewer/index.html?id=086fd70c510647c28c8a6298c0db9a5a'
            WHERE cama_site_link IS NULL AND bbl LIKE 'NY-%%';
            """
        )
    conn.commit()
    summary = {
        "pruned_vacant_ny_rows": pruned_vacant,
        "nyc_buildings_fused": nyc_props_upserted,
        "nyc_hpd_registrations_copied": nyc_regs_copied,
        "nyc_hpd_contacts_copied": nyc_contacts_copied,
        "nyc_hpd_mailing_backfilled": hpd_mail_backfilled,
        "nyc_bbl_stats_copied": nyc_stats_copied,
    }
    log(
        f"  ✓ Unified NYC into `ny_*`: {nyc_props_upserted:,} NYC buildings, "
        f"{nyc_regs_copied:,} HPD registrations, {nyc_contacts_copied:,} HPD contacts, "
        f"{hpd_mail_backfilled:,} HPD business addresses normalized for cross-county linking."
    )
    return summary


def import_ny_spot_extras(conn, parcels_gz: Optional[str], overlays_gz: Optional[str]) -> Dict[str, int]:
    """Import Monroe County / Rochester (`ny_monroe_parcels.csv.gz`) and Syracuse
    Rental Registry + Code Violations (`ny_overlays.csv.gz`) from local/GCS staging."""
    res = {"monroe_parcels": 0, "syracuse_registrations": 0, "syracuse_violations": 0}

    if parcels_gz and os.path.exists(parcels_gz):
        log(f"Importing Monroe County / Rochester parcels from `{parcels_gz}`...")
        batch = []
        with gzip.open(parcels_gz, "rt", encoding="utf-8", errors="replace") as gz:
            reader = csv.DictReader(gz)
            for r in reader:
                owner1 = source_text(r.get("owner_name"))
                addr = source_text(r.get("address"))
                bbl = source_text(r.get("bbl"))
                if not bbl or not addr or not owner1 or is_public_entity(owner1) or is_excluded_third_party_entity(owner1):
                    continue
                mail = source_text(r.get("mailing_address")) or ""
                dem = demangle_assessor_owner(owner1, owner2=source_text(r.get("owner2")), mailing_address=mail, col_max_len=50)
                cl_own = dem.cleaned_owner or owner1
                cl_mail = strip_benign_care_of_prefix(mail)
                batch.append((
                    "ny",
                    bbl[:80],
                    addr,
                    (source_text(r.get("borough")) or "ROCHESTER (MONROE)").upper(),
                    _clean_zip5(r.get("zip_code"), allow_leading_zero_4=True),
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
                    source_int(r.get("units_res")),
                    source_int(r.get("units_res")),
                    source_int(r.get("year_built")),
                    source_float(r.get("assessed_total")),
                    _parse_any_date(r.get("last_sale_date")),
                    source_float(r.get("last_sale_price")),
                    source_float(r.get("latitude")),
                    source_float(r.get("longitude")),
                    source_text(r.get("cama_site_link")) or "https://www.monroecounty.gov/etc/rp/",
                    source_text(r.get("source_system")) or "MONROE_COUNTY_GIS_2025",
                ))
        if batch:
            dedup = {row[1]: row for row in batch}
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO ny_properties (
                        jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
                        owner_co_name, owner_is_truncated, mailing_address, mailing_address_norm,
                        mailing_address_coarse, mailing_state, land_use, bld_class, num_floors,
                        units_res, units_total, year_built, assessed_total, last_sale_date,
                        last_sale_price, latitude, longitude, cama_site_link, source_system
                    )
                    VALUES %s
                    ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                        owner_name = EXCLUDED.owner_name,
                        owner_name_norm = EXCLUDED.owner_name_norm,
                        mailing_address = EXCLUDED.mailing_address,
                        mailing_address_norm = EXCLUDED.mailing_address_norm,
                        mailing_address_coarse = EXCLUDED.mailing_address_coarse,
                        assessed_total = COALESCE(EXCLUDED.assessed_total, ny_properties.assessed_total),
                        cama_site_link = EXCLUDED.cama_site_link,
                        updated_at = NOW();
                    """,
                    list(dedup.values()),
                    page_size=3000,
                )
            conn.commit()
            res["monroe_parcels"] = len(dedup)
            log(f"  ✓ Imported {len(dedup):,} Monroe County / Rochester parcels.")

    if overlays_gz and os.path.exists(overlays_gz):
        log(f"Importing Syracuse Rental Registry & Code Violations from `{overlays_gz}`...")
        syr_by_addr: Dict[str, str] = {}
        syr_by_sbl: Dict[str, str] = {}
        with conn.cursor() as cur:
            cur.execute("SELECT bbl, address FROM ny_properties WHERE borough ILIKE '%%ONONDAGA%%' OR borough ILIKE '%%SYRACUSE%%'")
            for bbl, addr in cur.fetchall():
                an = normalize_link_address(addr or "")
                if an:
                    syr_by_addr.setdefault(an, bbl)
                raw_sbl = re.sub(r"[^0-9A-Za-z]", "", bbl.replace("NY-", "")[6:]).rstrip("0")
                if raw_sbl:
                    syr_by_sbl[raw_sbl] = bbl

        reg_rows = []
        contact_rows = []
        stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"vt": 0, "vo": 0, "last_v": None})
        seen_regs = set()

        with gzip.open(overlays_gz, "rt", encoding="utf-8", errors="replace") as gz:
            reader = csv.DictReader(gz)
            for r in reader:
                kind = source_text(r.get("record_kind"))
                sbl_raw = re.sub(r"[^0-9A-Za-z]", "", source_text(r.get("parcel_hint")) or "").rstrip("0")
                addr_raw = source_text(r.get("address"))
                bbl = syr_by_sbl.get(sbl_raw) if sbl_raw else None
                if not bbl and addr_raw:
                    bbl = syr_by_addr.get(normalize_link_address(addr_raw.split("&")[0]))
                if not bbl:
                    continue
                if kind == "rental_license":
                    rid = source_text(r.get("record_id"))
                    if not rid or rid in seen_regs:
                        continue
                    seen_regs.add(rid)
                    reg_rows.append((
                        "ny", rid, bbl, addr_raw, "SYRACUSE", _clean_zip5(r.get("zip_code")),
                        "SYRACUSE (ONONDAGA)", source_text(r.get("status")) or "ACTIVE",
                        _parse_any_date(r.get("event_date")), None,
                    ))
                    ent = source_text(r.get("entity_name"))
                    if ent and not is_public_entity(ent) and not is_excluded_third_party_entity(ent):
                        is_corp = looks_like_business_name(ent)
                        nm_norm = normalize_business_name(ent) if is_corp else normalize_person_name(ent)
                        contact_rows.append((
                            "ny", rid, "CORPORATEOWNER" if is_corp else "INDIVIDUALOWNER",
                            ent if is_corp else None, nm_norm if is_corp else None,
                            None if is_corp else ent, None if is_corp else nm_norm,
                            "SYRACUSE_RENTAL_REGISTRY",
                        ))
                elif kind == "violation":
                    res["syracuse_violations"] += 1
                    st = stats[bbl]
                    st["vt"] += 1
                    stat_up = (source_text(r.get("status")) or "").upper()
                    if any(k in stat_up for k in ("OPEN", "ACTIVE", "VIOLATION", "PENDING", "ISSUED")):
                        st["vo"] += 1
                    vdt = _parse_any_date(r.get("event_date"))
                    if vdt and (st["last_v"] is None or vdt > st["last_v"]):
                        st["last_v"] = vdt

        with conn.cursor() as cur:
            cur.execute("DELETE FROM ny_hpd_contacts WHERE source_tag = 'SYRACUSE_RENTAL_REGISTRY'")
            cur.execute("DELETE FROM ny_hpd_registrations WHERE registration_id LIKE 'SYR-RR-%%'")
            if reg_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO ny_hpd_registrations (
                        jurisdiction, registration_id, bbl, building_address, building_city,
                        building_zip, borough, lifecycle_stage, last_registration_date, registration_end_date
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
                        compliance_record_id = v.rid
                    FROM (VALUES %s) AS v(rid, bbl)
                    WHERE p.bbl = v.bbl;
                    """,
                    [(x[1], x[2]) for x in reg_rows],
                    page_size=2000,
                )
            if contact_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO ny_hpd_contacts (
                        jurisdiction, registration_id, contact_type, corporation_name, corporation_name_norm,
                        full_name, full_name_norm, source_tag
                    ) VALUES %s;
                    """,
                    contact_rows,
                    page_size=2000,
                )
            if stats:
                execute_values(
                    cur,
                    """
                    INSERT INTO ny_bbl_stats (
                        jurisdiction, bbl, violations_total, violations_open, last_violation_date, updated_at
                    )
                    VALUES %s
                    ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                        violations_total = EXCLUDED.violations_total,
                        violations_open = EXCLUDED.violations_open,
                        last_violation_date = COALESCE(EXCLUDED.last_violation_date, ny_bbl_stats.last_violation_date),
                        updated_at = NOW();
                    """,
                    [("ny", b, s["vt"], s["vo"], s["last_v"]) for b, s in stats.items()],
                    template="(%s, %s, %s, %s, %s::date, NOW())",
                    page_size=2000,
                )
        conn.commit()
        res["syracuse_registrations"] = len(reg_rows)
        _record_status(
            conn,
            "SYRACUSE_ENFORCEMENT",
            "city_enforcement",
            {
                "matched_properties": len(reg_rows),
                "matched_records": res["syracuse_violations"],
                "syracuse_rental_registrations": len(reg_rows),
                "syracuse_code_violations": res["syracuse_violations"],
                "source_url": "https://data.syr.gov/",
                "message": f"Linked {len(reg_rows):,} Syracuse rental registrations & {res['syracuse_violations']:,} Syracuse code violations.",
            },
        )
        log(f"  ✓ Linked {len(reg_rows):,} Syracuse rental registrations & {res['syracuse_violations']:,} code violations.")
    return res


def probe_or_scrape_sdg_imagemate_county(conn, county: str) -> Dict[str, Any]:
    """Lightweight SDG `ImageMate Online` (`imo.*`) portal health, public-access check,
    and direct county CAMA card link enricher for NY counties."""
    c_up = county.strip().upper()
    base_url = NY_SDG_IMAGEMATE_URLS.get(c_up)
    if not base_url:
        return {"county": c_up, "status": "skipped"}

    sess = requests.Session()
    sess.headers.update(HTTP_HEADERS)
    portal_reachable = False
    public_login_ok = False
    try:
        # ImageMate Online standard public guest entry (`index.aspx` or `imo/search.aspx`)
        r = sess.get(base_url, timeout=12, allow_redirects=True)
        portal_reachable = r.status_code < 500
        if "public" in r.text.lower() or "search.aspx" in r.text.lower() or "swis" in r.text.lower():
            public_login_ok = True
    except Exception as exc:
        log(f"  ⚠ [{c_up}] SDG ImageMate probe warning: {exc}")
    finally:
        sess.close()

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ny_properties
            SET cama_site_link = %s,
                cama_last_scraped_at = NOW()
            WHERE borough ILIKE %s
              AND (cama_site_link IS NULL OR cama_site_link LIKE 'https://gisservices.its.ny.gov%%');
            """,
            (base_url, f"%({c_up})%"),
        )
        linked = cur.rowcount
    conn.commit()
    return {
        "county": c_up,
        "portal_url": base_url,
        "reachable": portal_reachable,
        "public_access_detected": public_login_ok,
        "cama_links_updated": linked,
    }


def sync_ny_62_county_status(conn) -> int:
    """Aggregate `ny_properties` across all 62 New York counties (5 NYC boroughs + 57
    non-NYC counties) and upsert `NY_COUNTY_<COUNTY>` rows + `ny_prop_stats:v1`."""
    log("Computing 62-County New York completeness matrix (`NY_COUNTY_*` + `ny_prop_stats:v1`)...")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                CASE
                    WHEN borough LIKE '%%(%%)%%' THEN SUBSTRING(borough FROM '\\(([^)]+)\\)$')
                    WHEN borough ILIKE 'MANHATTAN%%' THEN 'NEW YORK'
                    WHEN borough ILIKE 'BROOKLYN%%' THEN 'KINGS'
                    WHEN borough ILIKE 'QUEENS%%' THEN 'QUEENS'
                    WHEN borough ILIKE 'BRONX%%' THEN 'BRONX'
                    WHEN borough ILIKE 'STATEN ISLAND%%' THEN 'RICHMOND'
                    ELSE TRIM(UPPER(borough))
                END AS county_key,
                COUNT(*)::int AS total_properties,
                COUNT(CASE WHEN building_photo_url IS NOT NULL THEN 1 END)::int AS with_photos,
                COUNT(CASE WHEN cama_site_link IS NOT NULL THEN 1 END)::int AS with_cama,
                COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END)::int AS with_coords,
                COUNT(CASE WHEN owner_name IS NOT NULL AND UPPER(owner_name) NOT LIKE 'CURRENT OWNER%%' THEN 1 END)::int AS with_owner,
                COUNT(CASE WHEN year_built IS NOT NULL THEN 1 END)::int AS with_year_built,
                MAX(CASE WHEN last_sale_date <= CURRENT_DATE THEN last_sale_date END) AS max_sale_date
            FROM ny_properties
            WHERE borough IS NOT NULL
            GROUP BY 1
            """
        )
        by_county: Dict[str, Dict[str, Any]] = {}
        for r in cur.fetchall():
            ck = (r.get("county_key") or "").strip().upper()
            if ck == "ST LAWRENCE":
                ck = "ST. LAWRENCE"
            if ck:
                by_county[ck] = dict(r)

        cache_rows = []
        for cnty in ALL_62_NY_COUNTIES:
            cfg = NY_COUNTY_CONFIG[cnty]
            st = by_county.get(cnty, {})
            total = int(st.get("total_properties") or 0)
            plat = cfg["platform"]
            if total > 0 and plat in ("nyc_hpd_pluto", "monroe_gis", "nys_orpts_plus_buffalo", "nys_orpts_plus_syracuse"):
                status = "success"
            elif total > 0:
                status = "orpts_gis" if plat == "nys_orpts_gis" else "success"
            else:
                # Explicitly surface NYS GIS non-redistribution counties queued on County Roll / SDG ImageMate
                status = "cama_queued" if plat in ("sdg_imagemate", "dutchess_parcelaccess", "nassau_lrv") else "unavailable"

            max_sale = st.get("max_sale_date")
            skey = ny_county_status_key(cnty)
            details = {
                "county": cnty,
                "display": cfg["display"],
                "platform": plat,
                "portal_url": cfg["portal_url"],
                "frequency": cfg["frequency"],
                "properties_count": total,
                "note": (
                    None if total > 0 else
                    "County opts out of NYS ITS statewide bulk GIS redistribution; queued via SDG ImageMate / RPTL § 1590 county assessment roll."
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
            VALUES ('ny_prop_stats:v1', %s::jsonb, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = NOW();
            """,
            (json.dumps(cache_rows),),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()
    loaded_cnt = sum(1 for r in cache_rows if r["total_properties"] > 0)
    log(f"  ✓ Synced 62 NY county completeness rows ({loaded_cnt}/62 counties populated; holdouts explicitly badged).")
    return len(cache_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="New York 62-County Unification, SDG ImageMate CAMA & Enforcement Pipeline")
    parser.add_argument("--monroe-gz", default=None, help="Path to `ny_monroe_parcels.csv.gz`")
    parser.add_argument("--overlays-gz", default=None, help="Path to `ny_overlays.csv.gz`")
    parser.add_argument("--due-nightly", action="store_true", help="Run nightly SDG ImageMate CAMA cohort probe & link refresh")
    parser.add_argument("--max-counties", type=int, default=10, help="Max SDG ImageMate counties per nightly cohort")
    parser.add_argument("--sync-baseline-only", action="store_true", help="Only sync 62-county completeness rows")
    parser.add_argument("--rebuild-networks", action="store_true", help="Rebuild unified 62-county `ny_networks`")
    args = parser.parse_args()

    conn = psycopg2.connect(_resolve_database_url())
    try:
        ensure_nationwide_parent_tables(conn)
        ensure_jurisdiction_partition(conn, "ny")
        if not args.sync_baseline_only:
            if not args.due_nightly:
                unify_nyc_into_ny(conn)
                import_ny_spot_extras(conn, args.monroe_gz, args.overlays_gz)
            target_imo = sorted(NY_SDG_IMAGEMATE_URLS.keys())[: max(1, args.max_counties)] if args.due_nightly else sorted(NY_SDG_IMAGEMATE_URLS.keys())
            for cnty in target_imo:
                probe_or_scrape_sdg_imagemate_county(conn, cnty)
        sync_ny_62_county_status(conn)
        if args.rebuild_networks:
            p_cnt, n_cnt = build_and_save_state_networks(conn, "ny", "NY", "NY_UNIFIED_62_COUNTY_HPD_ORPTS")
            log(f"✓ Rebuilt unified New York networks: {p_cnt:,} properties -> {n_cnt:,} owner networks.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
