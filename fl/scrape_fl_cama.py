#!/usr/bin/env python3
"""
fl/scrape_fl_cama.py
====================
Florida 67-County Unified CAMA Enrichment, Miami-Dade Fusion, Code Enforcement
Overlay, and 67-County Freshness Registrar (`FL_COUNTY_<COUNTY>`).

Brings the CT / MA / VA standard to Florida (`fl_properties` + `fl_networks`):
  1. Prunes vacant land, commercial parking lots mislabeled under FDOR `028`
     with 0 residential units, reference folios, railroad corridors, and
     unimproved paper lots.
  2. Unifies `miami_properties` (`538K` Miami-Dade Property Appraiser parcels)
     into `fl_properties` (`C23-%` / `C23-MD-<FOLIO>`), upgrading FDOR's
     30-char truncated `OWN_NAME` with Miami-Dade's untruncated `TRUE_OWNER1..3`,
     carrying over NHPD subsidies, and attaching direct Miami-Dade PA record cards.
  3. Enriches Hillsborough County (`C39-%`, Tampa metro) from live HCPA
     `HC_Parcels` ArcGIS FeatureServer (`BLDG > 0` improved parcels, untruncated
     owners, units, sales, and direct HCPA parcel-card links) and populates
     official County Property Appraiser CAMA links across all 67 counties.
  4. Attaches live Florida municipal/county code enforcement cases onto
     `fl_bbl_stats` from Miami-Dade RER Code Compliance (Open, Lien & Closed
     5-Year views), City of Orlando (`k6e8-nw6w`), and City of Gainesville
     (`vu9p-a5f7`).
  5. Registers honest per-county `FL_COUNTY_<COUNTY>` completeness rows across
     all 67 Florida counties in `data_source_status`.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import datetime as dt
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values
import requests

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
from fl.fl_county_config import FL_COUNTY_CONFIG, fl_co_no_from_bbl, fl_county_status_key
from states.ingest_statewide_parcels import (
    _clean_zip5,
    _parse_any_date,
    _record_status,
    build_and_save_state_networks,
    enrich_fl_dbpr_apartments,
)

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TheyOwnWhatPublicRecordsSync/1.0)"}


def _resolve_database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [FL-CAMA] {msg}", flush=True)


def _is_nonbuilding_miami_land_use(land_use: Optional[str], bld_class: Optional[str], units: Optional[int], yr: Optional[int]) -> bool:
    lu = (land_use or "").upper()
    bc = (bld_class or "").strip()
    u = int(units or 0)
    y = int(yr or 0)
    if any(tok in lu for tok in ("VACANT", "REFERENCE FOLIO", "RAILROAD", "SUBMERGED", "RIGHT OF WAY", "R/W", "COMMON AREA", "SEWAGE", "UTILITY")):
        return True
    if bc in ("0000", "0081", "0066", "1081", "1066", "4081", "4066", "8080", "9862", "2865"):
        return True
    if "PARKING LOT" in lu and u == 0:
        return True
    if u == 0 and y == 0 and any(tok in lu for tok in ("EXTRA FEA", "ACREAGE", "AGRICULTURAL", "MINERAL")):
        return True
    return False


def prune_fl_vacant_and_nonbuilding_lots(conn) -> Dict[str, int]:
    """Remove commercial parking lots (`DOR_UC='028'` with 0 units & no year built),
    misc residential extra-feature slivers (`DOR_UC='007'` with 0 units & no year built),
    vacant land (`DOR_UC='010'` / `VACANT%`), and nominal unaddressed paper slivers."""
    log("Pruning vacant land, 0-unit commercial parking lots, and unimproved slivers from `fl_properties`...")
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM fl_properties
            WHERE bld_class IN ('000', '010', '040', '099')
               OR land_use ILIKE '%%VACANT%%'
               OR land_use ILIKE '%%REFERENCE FOLIO%%'
               OR land_use ILIKE '%%RAILROAD%%'
               OR (bld_class IN ('028', '007') AND COALESCE(units_res, 0) = 0 AND COALESCE(year_built, 0) = 0)
               OR (
                   COALESCE(units_res, 0) = 0
                   AND COALESCE(year_built, 0) = 0
                   AND COALESCE(assessed_total, 0) <= 5000
                   AND (address ~ '^0(\\s|$)' OR address ILIKE '%%UNASSIGNED%%' OR address ILIKE 'NO ADDRESS%%')
               );
            """
        )
        pruned = cur.rowcount
    conn.commit()
    log(f"  ✓ Pruned {pruned:,} vacant / parking-lot / unimproved rows from `fl_properties`.")
    return {"pruned_rows": pruned}


def unify_miami_into_fl(conn) -> Dict[str, int]:
    """Fuse `miami_properties` (`MD_LandInformation`) into `fl_properties` (`C23-%`):
      - Upgrades existing `C23-%` FDOR rows with Miami-Dade's untruncated `TRUE_OWNER1..3`,
        floor/unit counts, coordinates, and direct Miami-Dade Property Appraiser card URLs.
      - Removes `C23-%` rows whose untruncated Miami-Dade owner name reveals a public/excluded
        agency or non-building parcel.
      - Inserts remaining improved Miami-Dade residential/multifamily/commercial buildings as
        `C23-MD-<FOLIO>` without double-counting any `C23-%` address.
      - Carries over NHPD subsidies from `miami_bbl_stats` onto `fl_bbl_stats`."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.miami_properties') IS NOT NULL")
        if not cur.fetchone()[0]:
            log("`miami_properties` table not present; skipping Miami-Dade fusion.")
            return {}

    log("Unifying `miami_properties` (Miami-Dade Property Appraiser) into `fl_properties` (`C23-%`)...")
    c23_by_addr_zip: Dict[Tuple[str, str], str] = {}
    c23_by_addr_city: Dict[Tuple[str, str], str] = {}
    existing_md_folios: Set[str] = set()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT bbl, address, borough, zip_code, owner_name
            FROM fl_properties
            WHERE bbl LIKE 'C23-%%'
            """
        )
        for r in cur.fetchall():
            bbl = r["bbl"]
            if bbl.startswith("C23-MD-"):
                existing_md_folios.add(bbl[len("C23-MD-"):])
            anorm = normalize_link_address(r.get("address") or "")
            if not anorm:
                continue
            z5 = _clean_zip5(r.get("zip_code"))
            city = (r.get("borough") or "").strip().upper()
            if z5:
                c23_by_addr_zip.setdefault((anorm, z5), bbl)
            if city:
                c23_by_addr_city.setdefault((anorm, city), bbl)

    updates: List[tuple] = []
    delete_c23_bbls: Set[str] = set()
    inserts: List[tuple] = []
    folio_to_fl_bbl: Dict[str, str] = {}
    skipped_vacant = 0
    skipped_public = 0

    with conn.cursor(name="scan_miami_for_fl_unify", cursor_factory=RealDictCursor) as scan_cur:
        scan_cur.itersize = 25000
        scan_cur.execute(
            """
            SELECT bbl, address, borough, zip_code, owner_name, mailing_address,
                   assessed_total, units_res, year_built, land_use, bld_class,
                   num_floors, latitude, longitude
            FROM miami_properties
            WHERE bbl IS NOT NULL AND address IS NOT NULL AND owner_name IS NOT NULL
            """
        )
        for r in scan_cur:
            folio = (r["bbl"] or "").strip()
            addr = (r["address"] or "").strip()
            city = (r.get("borough") or "MIAMI").strip().upper()
            z5 = _clean_zip5(r.get("zip_code"))
            raw_owner = (r.get("owner_name") or "").strip()
            lu = r.get("land_use")
            bc = r.get("bld_class")
            units = source_int(r.get("units_res"))
            yr = source_int(r.get("year_built"))
            if yr is not None and (yr <= 1700 or yr > 2030):
                yr = None

            anorm = normalize_link_address(addr)
            matched_bbl = None
            if anorm and z5:
                matched_bbl = c23_by_addr_zip.get((anorm, z5))
            if not matched_bbl and anorm and city:
                matched_bbl = c23_by_addr_city.get((anorm, city))

            if _is_nonbuilding_miami_land_use(lu, bc, units, yr):
                skipped_vacant += 1
                if matched_bbl:
                    delete_c23_bbls.add(matched_bbl)
                continue

            if (
                raw_owner.upper() in GENERIC_OWNER_NAMES
                or is_public_entity(raw_owner)
                or is_excluded_third_party_entity(raw_owner)
            ):
                skipped_public += 1
                if matched_bbl:
                    delete_c23_bbls.add(matched_bbl)
                continue

            mail = (r.get("mailing_address") or "").strip()
            demangled = demangle_assessor_owner(raw_owner, mailing_address=mail, col_max_len=120)
            clean_owner = demangled.cleaned_owner or raw_owner
            clean_mail = strip_benign_care_of_prefix(mail)
            coarse_mail = normalize_mailing_address_coarse(clean_mail)
            cama_url = f"https://www.miamidade.gov/Apps/PA/propertysearch/#/folio/{folio}"
            lat = source_float(r.get("latitude"))
            lng = source_float(r.get("longitude"))
            if lat is not None and lng is not None and not (24.30 <= lat <= 31.10 and -87.70 <= lng <= -79.90):
                lat, lng = None, None
            val = source_float(r.get("assessed_total"))
            floors = source_int(r.get("num_floors"))

            if matched_bbl:
                folio_to_fl_bbl[folio] = matched_bbl
                updates.append((
                    clean_owner,
                    demangled.owner_norm or normalize_owner_name(clean_owner),
                    demangled.care_of_entity,
                    False,
                    mail,
                    normalize_link_address(clean_mail),
                    coarse_mail,
                    demangled.mailing_state,
                    units,
                    floors,
                    yr,
                    val,
                    lat,
                    lng,
                    cama_url,
                    matched_bbl,
                ))
            else:
                new_bbl = f"C23-MD-{folio}"
                folio_to_fl_bbl[folio] = new_bbl
                if anorm and z5:
                    c23_by_addr_zip[(anorm, z5)] = new_bbl
                if anorm and city:
                    c23_by_addr_city[(anorm, city)] = new_bbl
                inserts.append((
                    "fl",
                    new_bbl,
                    addr,
                    city,
                    z5,
                    clean_owner,
                    demangled.owner_norm or normalize_owner_name(clean_owner),
                    demangled.care_of_entity,
                    False,
                    mail,
                    normalize_link_address(clean_mail),
                    coarse_mail,
                    demangled.mailing_state,
                    lu,
                    bc,
                    floors,
                    units,
                    units,
                    yr,
                    val,
                    lat,
                    lng,
                    cama_url,
                    "MIAMI_DADE_PA_CAMA",
                ))

    with conn.cursor() as cur:
        if delete_c23_bbls:
            cur.execute("DELETE FROM fl_properties WHERE bbl = ANY(%s)", (list(delete_c23_bbls),))
        if updates:
            execute_values(
                cur,
                """
                UPDATE fl_properties AS p
                SET owner_name = v.own,
                    owner_name_norm = v.own_norm,
                    owner_co_name = COALESCE(v.own_co, p.owner_co_name),
                    owner_is_truncated = v.trunc,
                    mailing_address = CASE WHEN COALESCE(v.mail, '') <> '' THEN v.mail ELSE p.mailing_address END,
                    mailing_address_norm = CASE WHEN COALESCE(v.mail_norm, '') <> '' THEN v.mail_norm ELSE p.mailing_address_norm END,
                    mailing_address_coarse = COALESCE(v.mail_coarse, p.mailing_address_coarse),
                    mailing_state = COALESCE(v.mail_st, p.mailing_state),
                    units_res = CASE WHEN COALESCE(v.u, 0) > COALESCE(p.units_res, 0) THEN v.u ELSE p.units_res END,
                    units_total = CASE WHEN COALESCE(v.u, 0) > COALESCE(p.units_total, 0) THEN v.u ELSE p.units_total END,
                    num_floors = COALESCE(v.flrs, p.num_floors),
                    year_built = COALESCE(p.year_built, v.yr),
                    assessed_total = COALESCE(v.val, p.assessed_total),
                    latitude = COALESCE(p.latitude, v.lat),
                    longitude = COALESCE(p.longitude, v.lng),
                    cama_site_link = v.cama_url,
                    cama_last_scraped_at = NOW(),
                    updated_at = NOW()
                FROM (VALUES %s) AS v(
                    own, own_norm, own_co, trunc, mail, mail_norm, mail_coarse, mail_st,
                    u, flrs, yr, val, lat, lng, cama_url, bbl
                )
                WHERE p.bbl = v.bbl;
                """,
                updates,
                template="(%s, %s, %s, %s::boolean, %s, %s, %s, %s, %s::numeric, %s::numeric, %s::int, %s::numeric, %s::double precision, %s::double precision, %s, %s)",
                page_size=2500,
            )
        if inserts:
            execute_values(
                cur,
                """
                INSERT INTO fl_properties (
                    jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
                    owner_co_name, owner_is_truncated, mailing_address, mailing_address_norm,
                    mailing_address_coarse, mailing_state, land_use, bld_class, num_floors,
                    units_res, units_total, year_built, assessed_total,
                    latitude, longitude, cama_site_link, cama_last_scraped_at, source_system
                )
                VALUES %s
                ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                    owner_name = EXCLUDED.owner_name,
                    owner_name_norm = EXCLUDED.owner_name_norm,
                    mailing_address = EXCLUDED.mailing_address,
                    mailing_address_norm = EXCLUDED.mailing_address_norm,
                    mailing_address_coarse = EXCLUDED.mailing_address_coarse,
                    units_res = EXCLUDED.units_res,
                    units_total = EXCLUDED.units_total,
                    num_floors = EXCLUDED.num_floors,
                    year_built = COALESCE(fl_properties.year_built, EXCLUDED.year_built),
                    assessed_total = COALESCE(EXCLUDED.assessed_total, fl_properties.assessed_total),
                    cama_site_link = EXCLUDED.cama_site_link,
                    cama_last_scraped_at = NOW(),
                    updated_at = NOW();
                """,
                [(row[:-1] + (dt.datetime.now(dt.timezone.utc), row[-1])) for row in inserts],
                page_size=2500,
            )

        # Carry over NHPD subsidies from miami_bbl_stats onto unified fl_bbl_stats
        cur.execute("SELECT to_regclass('public.miami_bbl_stats') IS NOT NULL")
        if cur.fetchone()[0] and folio_to_fl_bbl:
            cur.execute(
                """
                SELECT bbl, nhpd_subsidy, nhpd_program, nhpd_expiration
                FROM miami_bbl_stats
                WHERE nhpd_subsidy IS TRUE
                """
            )
            nhpd_carry = []
            for folio_s, nhpd_sub, nhpd_prog, nhpd_exp in cur.fetchall():
                target_bbl = folio_to_fl_bbl.get((folio_s or "").strip())
                if target_bbl:
                    nhpd_carry.append(("fl", target_bbl, bool(nhpd_sub), nhpd_prog, nhpd_exp))
            if nhpd_carry:
                dedup_nhpd = {r[1]: r for r in nhpd_carry}
                execute_values(
                    cur,
                    """
                    INSERT INTO fl_bbl_stats (jurisdiction, bbl, nhpd_subsidy, nhpd_program, nhpd_expiration, updated_at)
                    VALUES %s
                    ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                        nhpd_subsidy = TRUE,
                        nhpd_program = COALESCE(EXCLUDED.nhpd_program, fl_bbl_stats.nhpd_program),
                        nhpd_expiration = COALESCE(EXCLUDED.nhpd_expiration, fl_bbl_stats.nhpd_expiration),
                        updated_at = NOW();
                    """,
                    list(dedup_nhpd.values()),
                    template="(%s, %s, %s::boolean, %s, %s::date, NOW())",
                    page_size=2000,
                )
    conn.commit()
    summary = {
        "c23_upgraded_from_miami_pa": len(updates),
        "miami_improved_inserted_into_fl": len(inserts),
        "c23_pruned_vacant_or_public": len(delete_c23_bbls),
        "miami_skipped_vacant": skipped_vacant,
        "miami_skipped_public": skipped_public,
    }
    log(
        f"  ✓ Miami-Dade unified into `fl_properties`: {len(updates):,} C23 rows upgraded, "
        f"{len(inserts):,} improved Miami-Dade parcels added, {len(delete_c23_bbls):,} vacant/agency C23 rows pruned."
    )
    return summary


def enrich_hillsborough_hcpa_cama(conn) -> Dict[str, int]:
    """Stream improved (`BLDG > 0`) parcels from Hillsborough County Property Appraiser
    (`HC_Parcels` FeatureServer) to enrich `C39-%` (Tampa / Hillsborough) rows with
    untruncated owners, unit counts, sale dates/prices, and direct HCPA parcel-card links."""
    url = "https://services.arcgis.com/apTfC6SUmnNfnxuF/arcgis/rest/services/HC_Parcels/FeatureServer/0/query"
    log("Enriching Hillsborough County (`C39-%`, Tampa metro) from live HCPA `HC_Parcels` FeatureServer...")

    c39_by_addr_zip: Dict[Tuple[str, str], str] = {}
    c39_by_addr_city: Dict[Tuple[str, str], str] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT bbl, address, borough, zip_code FROM fl_properties WHERE bbl LIKE 'C39-%%'")
        for bbl, addr, boro, zp in cur.fetchall():
            anorm = normalize_link_address(addr or "")
            if not anorm:
                continue
            z5 = _clean_zip5(zp)
            city = (boro or "").strip().upper()
            if z5:
                c39_by_addr_zip.setdefault((anorm, z5), bbl)
            if city:
                c39_by_addr_city.setdefault((anorm, city), bbl)

    if not c39_by_addr_zip and not c39_by_addr_city:
        return {"matched": 0}

    last_oid = -1
    updates: Dict[str, tuple] = {}
    scanned = 0
    session = requests.Session()
    session.headers.update(HTTP_HEADERS)
    try:
        while True:
            params = {
                "where": f"BLDG > 0 AND OWNER IS NOT NULL AND SITE_ADDR IS NOT NULL AND OBJECTID > {last_oid}",
                "outFields": "OBJECTID,FOLIO,OWNER,SITE_ADDR,SITE_CITY,SITE_ZIP,tUNITS,tSTORIES,ACT,JUST,S_DATE,S_AMT",
                "orderByFields": "OBJECTID ASC",
                "resultRecordCount": 2000,
                "returnGeometry": "false",
                "f": "json",
            }
            data = None
            for attempt in range(4):
                try:
                    resp = session.get(url, params=params, timeout=35)
                    resp.raise_for_status()
                    data = resp.json()
                    if "error" in data:
                        raise RuntimeError(str(data["error"]))
                    break
                except Exception as exc:
                    if attempt == 3:
                        log(f"  ⚠ HCPA stream stopped after OBJECTID={last_oid}: {exc}")
                        data = None
                    else:
                        time.sleep(2 * (attempt + 1))
            if not data:
                break
            feats = data.get("features") or []
            if not feats:
                break
            for f in feats:
                a = f.get("attributes") or {}
                oid = source_int(a.get("OBJECTID")) or 0
                if oid > last_oid:
                    last_oid = oid
                scanned += 1
                site = source_text(a.get("SITE_ADDR"))
                folio = source_text(a.get("FOLIO"))
                owner = source_text(a.get("OWNER"))
                if not site or not folio or not owner:
                    continue
                anorm = normalize_link_address(site)
                z5 = _clean_zip5(a.get("SITE_ZIP"))
                city = (source_text(a.get("SITE_CITY")) or "").upper()
                bbl = c39_by_addr_zip.get((anorm, z5)) if (anorm and z5) else None
                if not bbl and anorm and city:
                    bbl = c39_by_addr_city.get((anorm, city))
                if not bbl:
                    continue
                cama_link = f"https://gis.hcpafl.org/propertysearch/#/parcel/basic/{folio}"
                units = source_int(a.get("tUNITS"))
                stories = source_int(a.get("tSTORIES"))
                yr = source_int(a.get("ACT"))
                if yr is not None and (yr <= 1750 or yr > 2030):
                    yr = None
                s_dt = _parse_any_date(a.get("S_DATE"))
                s_amt = source_float(a.get("S_AMT"))
                if s_amt is not None and s_amt <= 0:
                    s_amt = None
                dem = demangle_assessor_owner(owner, col_max_len=80)
                cl_own = dem.cleaned_owner or owner
                updates[bbl] = (
                    cl_own,
                    dem.owner_norm or normalize_owner_name(cl_own),
                    units,
                    stories,
                    yr,
                    s_dt,
                    s_amt,
                    cama_link,
                    bbl,
                )
            if len(feats) < 2000 and not data.get("exceededTransferLimit"):
                break
    finally:
        session.close()

    if updates:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE fl_properties AS p
                SET owner_name = CASE
                        WHEN p.owner_is_truncated IS TRUE OR LENGTH(v.own) > LENGTH(COALESCE(p.owner_name, ''))
                        THEN v.own ELSE p.owner_name
                    END,
                    owner_name_norm = CASE
                        WHEN p.owner_is_truncated IS TRUE OR LENGTH(v.own) > LENGTH(COALESCE(p.owner_name, ''))
                        THEN v.own_norm ELSE p.owner_name_norm
                    END,
                    units_res = CASE WHEN COALESCE(v.u, 0) > COALESCE(p.units_res, 0) THEN v.u ELSE p.units_res END,
                    units_total = CASE WHEN COALESCE(v.u, 0) > COALESCE(p.units_total, 0) THEN v.u ELSE p.units_total END,
                    num_floors = COALESCE(p.num_floors, v.stories),
                    year_built = COALESCE(p.year_built, v.yr),
                    last_sale_date = COALESCE(v.sdt, p.last_sale_date),
                    last_sale_price = COALESCE(v.samt, p.last_sale_price),
                    cama_site_link = v.cama_url,
                    cama_last_scraped_at = NOW(),
                    updated_at = NOW()
                FROM (VALUES %s) AS v(own, own_norm, u, stories, yr, sdt, samt, cama_url, bbl)
                WHERE p.bbl = v.bbl;
                """,
                list(updates.values()),
                template="(%s, %s, %s::numeric, %s::numeric, %s::int, %s::date, %s::numeric, %s, %s)",
                page_size=2500,
            )
        conn.commit()
    log(f"  ✓ Hillsborough HCPA CAMA scanned {scanned:,} improved parcels -> enriched {len(updates):,} `C39-%` rows.")
    return {"scanned": scanned, "matched": len(updates)}


def backfill_fl_county_portal_links(conn) -> int:
    """Populate County Property Appraiser portal links on `fl_properties` where `cama_site_link` is NULL."""
    log("Backfilling official County Property Appraiser CAMA portal links across all 67 Florida counties...")
    mapping_rows = [
        (f"C{co_no:02d}", cfg["portal_url"])
        for co_no, cfg in FL_COUNTY_CONFIG.items()
        if cfg.get("portal_url")
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            UPDATE fl_properties AS p
            SET cama_site_link = v.url
            FROM (VALUES %s) AS v(prefix, url)
            WHERE p.cama_site_link IS NULL
              AND LEFT(p.bbl, 3) = v.prefix;
            """,
            mapping_rows,
            page_size=100,
        )
        updated = cur.rowcount
    conn.commit()
    log(f"  ✓ Populated County Property Appraiser CAMA links on {updated:,} Florida parcels.")
    return updated


def enrich_fl_code_enforcement(conn) -> Dict[str, Any]:
    """Attach live Florida municipal & county code enforcement violations onto `fl_bbl_stats`:
      1. Miami-Dade County RER Code Compliance (`CodeComplianceViolation_Open_View`,
         `CodeComplianceViolation_Lien_View`, `CodeComplianceViolation_ClosedPast5Years_View`)
      2. City of Orlando Code Enforcement Cases (`data.cityoforlando.net/resource/k6e8-nw6w.json`)
      3. City of Gainesville Code Violations (`data.cityofgainesville.org/resource/vu9p-a5f7.json`)"""
    log("Enriching Florida Code Enforcement overlays (Miami-Dade RER, Orlando, Gainesville)...")

    md_by_folio: Dict[str, str] = {}
    md_by_addr: Dict[str, str] = {}
    orange_by_addr: Dict[str, str] = {}
    alachua_by_addr: Dict[str, str] = {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bbl, address, cama_site_link
            FROM fl_properties
            WHERE LEFT(bbl, 3) IN ('C23', 'C58', 'C11')
            """
        )
        for bbl, addr, cama_url in cur.fetchall():
            co3 = bbl[:3]
            anorm = normalize_link_address(addr or "")
            if co3 == "C23":
                if bbl.startswith("C23-MD-"):
                    md_by_folio[bbl[len("C23-MD-"):]] = bbl
                elif cama_url and "/folio/" in cama_url:
                    md_by_folio[cama_url.rsplit("/folio/", 1)[-1].strip()] = bbl
                if anorm:
                    md_by_addr.setdefault(anorm, bbl)
            elif co3 == "C58" and anorm:
                orange_by_addr.setdefault(anorm, bbl)
            elif co3 == "C11" and anorm:
                alachua_by_addr.setdefault(anorm, bbl)

    stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"vt": 0, "vo": 0, "last_v": None})
    counts = {"miami_dade_rer": 0, "orlando": 0, "gainesville": 0}
    session = requests.Session()
    session.headers.update(HTTP_HEADERS)

    try:
        # 1. Miami-Dade County RER Code Compliance
        md_layers = [
            ("https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/CodeComplianceViolation_Open_View/FeatureServer/0/query", True),
            ("https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/CodeComplianceViolation_Lien_View/FeatureServer/0/query", True),
            ("https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/CodeComplianceViolation_ClosedPast5Years_View/FeatureServer/0/query", False),
        ]
        for l_url, is_open_layer in md_layers:
            last_oid = -1
            while True:
                try:
                    r = session.get(
                        l_url,
                        params={
                            "where": f"OBJECTID > {last_oid}",
                            "outFields": "OBJECTID,FOLIO,ADDRESS,CASE_DATE,STAT_DESC",
                            "orderByFields": "OBJECTID ASC",
                            "resultRecordCount": 2000,
                            "returnGeometry": "false",
                            "f": "json",
                        },
                        timeout=30,
                    ).json()
                except Exception as exc:
                    log(f"  ⚠ Miami-Dade RER layer fetch warning: {exc}")
                    break
                feats = r.get("features") or []
                if not feats:
                    break
                for f in feats:
                    a = f.get("attributes") or {}
                    oid = source_int(a.get("OBJECTID")) or 0
                    if oid > last_oid:
                        last_oid = oid
                    folio = re.sub(r"[^0-9]", "", source_text(a.get("FOLIO")) or "")
                    bbl = md_by_folio.get(folio) if folio else None
                    if not bbl:
                        addr = source_text(a.get("ADDRESS"))
                        if addr and "NO ADDRESS" not in addr.upper():
                            bbl = md_by_addr.get(normalize_link_address(addr))
                    if not bbl:
                        continue
                    counts["miami_dade_rer"] += 1
                    st = stats[bbl]
                    st["vt"] += 1
                    sdesc = (source_text(a.get("STAT_DESC")) or "").upper()
                    if is_open_layer or any(k in sdesc for k in ("OPEN", "LIEN", "ACTIVE", "PENDING")):
                        st["vo"] += 1
                    cdt = _parse_any_date(a.get("CASE_DATE"))
                    if cdt and (st["last_v"] is None or cdt > st["last_v"]):
                        st["last_v"] = cdt
                if len(feats) < 2000 and not r.get("exceededTransferLimit"):
                    break

        # 2. City of Orlando Code Enforcement Cases (`k6e8-nw6w`)
        offset = 0
        while offset < 250000:
            try:
                rows = session.get(
                    "https://data.cityoforlando.net/resource/k6e8-nw6w.json",
                    params={
                        "$select": "apno,caseinfostatus,casedt,derived_address",
                        "$where": "derived_address IS NOT NULL",
                        "$order": "apno ASC",
                        "$limit": 5000,
                        "$offset": offset,
                    },
                    timeout=30,
                ).json()
            except Exception as exc:
                log(f"  ⚠ Orlando code enforcement fetch warning at offset {offset}: {exc}")
                break
            if not isinstance(rows, list) or not rows:
                break
            for r in rows:
                daddr = source_text(r.get("derived_address"))
                if not daddr:
                    continue
                # derived_address may include ", ORLANDO, FL 328..." — strip city/state suffix
                street_only = daddr.split(",")[0].strip()
                bbl = orange_by_addr.get(normalize_link_address(street_only))
                if not bbl:
                    continue
                counts["orlando"] += 1
                st = stats[bbl]
                st["vt"] += 1
                cstat = (source_text(r.get("caseinfostatus")) or "").upper()
                if cstat not in ("CLOSED", "VOID", "CANCELLED", "CANCELED", "COMPLIED", "DISMISSED"):
                    st["vo"] += 1
                cdt = _parse_any_date(r.get("casedt"))
                if cdt and (st["last_v"] is None or cdt > st["last_v"]):
                    st["last_v"] = cdt
            if len(rows) < 5000:
                break
            offset += len(rows)

        # 3. City of Gainesville Code Violations (`vu9p-a5f7`)
        offset = 0
        while offset < 150000:
            try:
                rows = session.get(
                    "https://data.cityofgainesville.org/resource/vu9p-a5f7.json",
                    params={
                        "$select": "number,address,status,infraction",
                        "$where": "address IS NOT NULL",
                        "$order": "number ASC",
                        "$limit": 5000,
                        "$offset": offset,
                    },
                    timeout=30,
                ).json()
            except Exception as exc:
                log(f"  ⚠ Gainesville code violations fetch warning at offset {offset}: {exc}")
                break
            if not isinstance(rows, list) or not rows:
                break
            for r in rows:
                addr = source_text(r.get("address"))
                if not addr or addr.startswith("0 "):
                    continue
                bbl = alachua_by_addr.get(normalize_link_address(addr.split(",")[0]))
                if not bbl:
                    continue
                counts["gainesville"] += 1
                st = stats[bbl]
                st["vt"] += 1
                cstat = (source_text(r.get("status")) or "").upper()
                if cstat in ("OPEN", "ACTIVE", "PENDING", "IN VIOLATION"):
                    st["vo"] += 1
                cdt = _parse_any_date(r.get("infraction"))
                if cdt and (st["last_v"] is None or cdt > st["last_v"]):
                    st["last_v"] = cdt
            if len(rows) < 5000:
                break
            offset += len(rows)
    finally:
        session.close()

    if stats:
        stat_rows = [
            ("fl", bbl, s["vt"], s["vo"], s["last_v"])
            for bbl, s in stats.items()
        ]
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO fl_bbl_stats (
                    jurisdiction, bbl, violations_total, violations_open, last_violation_date, updated_at
                )
                VALUES %s
                ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                    violations_total = EXCLUDED.violations_total,
                    violations_open = EXCLUDED.violations_open,
                    last_violation_date = COALESCE(EXCLUDED.last_violation_date, fl_bbl_stats.last_violation_date),
                    updated_at = NOW();
                """,
                stat_rows,
                template="(%s, %s, %s, %s, %s::date, NOW())",
                page_size=2500,
            )
        conn.commit()

    total_matched = sum(counts.values())
    open_cases = sum(s["vo"] for s in stats.values())
    details = {
        "matched_records": total_matched,
        "matched_parcels": len(stats),
        "open_violations": open_cases,
        "by_municipality": counts,
        "message": (
            f"Matched {total_matched:,} code enforcement cases across {len(stats):,} Florida parcels "
            f"(Miami-Dade County RER: {counts['miami_dade_rer']:,}, City of Orlando: {counts['orlando']:,}, "
            f"City of Gainesville: {counts['gainesville']:,})."
        ),
        "sources": [
            {"source": "Miami-Dade County RER Code Compliance (Open, Lien & Closed 5-Yr Views)", "url": "https://gis-mdc.opendata.arcgis.com/"},
            {"source": "City of Orlando Code Enforcement Cases (k6e8-nw6w)", "url": "https://data.cityoforlando.net/resource/k6e8-nw6w"},
            {"source": "City of Gainesville Code Complaints & Violations (vu9p-a5f7)", "url": "https://data.cityofgainesville.org/resource/vu9p-a5f7"},
        ],
    }
    _record_status(conn, "FL_CODE_ENFORCEMENT", "city_enforcement", details)
    log(f"  ✓ FL Code Enforcement matched {total_matched:,} cases onto {len(stats):,} parcels ({open_cases:,} open).")
    return details


def sync_fl_67_county_status(conn) -> int:
    """Compute per-county completeness stats across all 67 Florida counties (`C11..C77`)
    and upsert `FL_COUNTY_<COUNTY>` rows in `data_source_status` + `fl_prop_stats:v1` cache."""
    log("Computing 67-County Florida completeness matrix (`FL_COUNTY_*` + `fl_prop_stats:v1`)...")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                LEFT(bbl, 3) AS co_prefix,
                COUNT(*)::int AS total_properties,
                COUNT(CASE WHEN building_photo_url IS NOT NULL THEN 1 END)::int AS with_photos,
                COUNT(CASE WHEN cama_site_link IS NOT NULL THEN 1 END)::int AS with_cama,
                COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END)::int AS with_coords,
                COUNT(CASE WHEN owner_name IS NOT NULL AND UPPER(owner_name) NOT LIKE 'CURRENT OWNER%%' THEN 1 END)::int AS with_owner,
                COUNT(CASE WHEN year_built IS NOT NULL THEN 1 END)::int AS with_year_built,
                MAX(CASE WHEN last_sale_date <= CURRENT_DATE THEN last_sale_date END) AS max_sale_date
            FROM fl_properties
            WHERE bbl ~ '^C[0-9]{2}'
            GROUP BY LEFT(bbl, 3)
            ORDER BY LEFT(bbl, 3)
            """
        )
        by_prefix = {r["co_prefix"]: dict(r) for r in cur.fetchall()}

        cache_rows = []
        for co_no, cfg in sorted(FL_COUNTY_CONFIG.items()):
            prefix = f"C{co_no:02d}"
            stats = by_prefix.get(prefix, {})
            total = int(stats.get("total_properties") or 0)
            county_name = cfg["county"]
            platform = cfg["platform"]
            status = "success" if platform in ("miami_dade_pa", "hcpa_arcgis") else (
                "cama_queued" if platform in ("qpublic", "bcpa", "pbcpao", "ocpa", "pcpao", "coj_pao", "leepa", "polkpa", "bcpao") else "fdor_nal"
            )
            max_sale = stats.get("max_sale_date")
            skey = fl_county_status_key(county_name)
            details = {
                "county": county_name,
                "co_no": co_no,
                "platform": platform,
                "portal_url": cfg["portal_url"],
                "frequency": cfg["frequency"],
                "properties_count": total,
                "with_owner": int(stats.get("with_owner") or 0),
                "with_coords": int(stats.get("with_coords") or 0),
                "with_year_built": int(stats.get("with_year_built") or 0),
                "with_cama": int(stats.get("with_cama") or 0),
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
                "town": f"{county_name} COUNTY",
                "county": county_name,
                "co_no": co_no,
                "platform": platform,
                "portal_url": cfg["portal_url"],
                "frequency": cfg["frequency"],
                "refresh_status": status,
                "total_properties": total,
                "with_photos": int(stats.get("with_photos") or 0),
                "with_cama": int(stats.get("with_cama") or 0),
                "with_coords": int(stats.get("with_coords") or 0),
                "with_owner": int(stats.get("with_owner") or 0),
                "with_year_built": int(stats.get("with_year_built") or 0),
                "max_sale_date": max_sale.isoformat() if max_sale else None,
            })

        cur.execute(
            """
            INSERT INTO kv_cache (key, value, created_at)
            VALUES ('fl_prop_stats:v1', %s::jsonb, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = NOW();
            """,
            (json.dumps(cache_rows),),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()
    log(f"  ✓ Synced {len(cache_rows)} Florida County Property Appraiser rows into `data_source_status` & `fl_prop_stats:v1`.")
    return len(cache_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Florida 67-County CAMA, Miami-Dade Unification & Enforcement Pipeline")
    parser.add_argument("--skip-hillsborough", action="store_true", help="Skip live HCPA FeatureServer enrichment")
    parser.add_argument("--skip-enforcement", action="store_true", help="Skip live Florida code enforcement sync")
    parser.add_argument("--sync-baseline-only", action="store_true", help="Only compute 67-county status rows")
    parser.add_argument("--rebuild-networks", action="store_true", help="Re-link DBPR + OpenCorporates and rebuild `fl_networks`")
    args = parser.parse_args()

    conn = psycopg2.connect(_resolve_database_url())
    try:
        ensure_nationwide_parent_tables(conn)
        ensure_jurisdiction_partition(conn, "fl")
        if not args.sync_baseline_only:
            prune_fl_vacant_and_nonbuilding_lots(conn)
            unify_miami_into_fl(conn)
            if not args.skip_hillsborough:
                enrich_hillsborough_hcpa_cama(conn)
            backfill_fl_county_portal_links(conn)
            if not args.skip_enforcement:
                enrich_fl_code_enforcement(conn)
        sync_fl_67_county_status(conn)
        if args.rebuild_networks:
            enrich_fl_dbpr_apartments(conn, target_jurisdictions=("fl",))
            p_cnt, n_cnt = build_and_save_state_networks(conn, "fl", "FL", "FL_UNIFIED_67_COUNTY_PARCELS")
            log(f"✓ Rebuilt unified Florida networks: {p_cnt:,} improved parcels -> {n_cnt:,} owner networks.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
