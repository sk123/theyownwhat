#!/usr/bin/env python3
"""
ca/ingest_ca_statewide.py
=========================
California 58-County Property, Business-Tax Lessor, Code Enforcement, Eviction,
NHPD & CPRA Secured Roll Pipeline (`ca_properties` + `ca_networks`).

Implements both honest California tracks:
  - Track A (`--parcels-gz` / `--overlays-gz`): Streams improved LA County &
    San Francisco parcels enriched with SF (`g8m3-pdis`) & LA (`6rrh-rzua`)
    registered lessor/property-owner business locations, SF DBI & LADBS code
    violations, SF & LAHD eviction notices, and explicit CA Gov. Code § 7928.205
    statutory redaction markers where online county portals strip deed owners.
  - Track B (`--import-cpra-roll <COUNTY> <CSV_PATH>`): Drop-in loader for
    County Assessor CPRA Secured Assessment Roll extracts (`Rev. & Tax. Code § 408.3`),
    upgrading redacted placeholders to full assessee/deed owners!
  - Corporate Veil Piercing: Links `ca_properties` against `us_ca` (`9.42M` CA
    corporations/LLCs & `31.96M` CA Form `LLC-12` managers, members & officers)
    and syncs all 58 `CA_COUNTY_<COUNTY>` completeness rows (`ca_prop_stats:v1`).
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

csv.field_size_limit(10_000_000)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_jurisdiction_partition, ensure_nationwide_parent_tables
from api.oc_linker import demangle_assessor_owner
from api.populate_cities import (
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
from ca.ca_county_config import ALL_58_CA_COUNTIES, CA_COUNTY_CONFIG, ca_county_status_key
from states.ingest_statewide_parcels import (
    _clean_zip5,
    _parse_any_date,
    _record_status,
    build_and_save_state_networks,
)


def _resolve_database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [CA-STATEWIDE] {msg}", flush=True)


def import_ca_track_a_spot_data(
    conn,
    parcels_gz: Optional[str],
    overlays_gz: Optional[str],
) -> Dict[str, int]:
    res = {"parcels_loaded": 0, "named_lessor_parcels": 0, "redacted_7928_parcels": 0, "violations_matched": 0, "evictions_matched": 0}

    if parcels_gz and os.path.exists(parcels_gz):
        log(f"Streaming California Track A parcels from `{parcels_gz}` into `ca_properties`...")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE ca_properties;")
        conn.commit()

        batch: List[tuple] = []
        seen_bbls = set()
        insert_sql = """
            INSERT INTO ca_properties (
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
                is_redacted = owner1.upper().startswith("CA GOV CODE 7928")
                if not is_redacted and (is_public_entity(owner1) or is_excluded_third_party_entity(owner1)):
                    continue
                seen_bbls.add(bbl)
                if is_redacted:
                    res["redacted_7928_parcels"] += 1
                    cl_own = owner1
                    own_norm = owner1.upper()
                    care_of = None
                    is_trunc = False
                    mail = ""
                    mail_norm = None
                    mail_coarse = None
                    mst = None
                else:
                    res["named_lessor_parcels"] += 1
                    mail = source_text(r.get("mailing_address")) or ""
                    dem = demangle_assessor_owner(owner1, mailing_address=mail, explicit_mailing_state=source_text(r.get("mailing_state")), col_max_len=80)
                    cl_own = dem.cleaned_owner or owner1
                    own_norm = dem.owner_norm or normalize_owner_name(cl_own)
                    care_of = dem.care_of_entity
                    is_trunc = dem.is_truncated
                    cl_mail = strip_benign_care_of_prefix(mail)
                    mail_norm = normalize_link_address(cl_mail)
                    mail_coarse = normalize_mailing_address_coarse(cl_mail)
                    mst = dem.mailing_state or source_text(r.get("mailing_state"))

                units = source_int(r.get("units_res"))
                yr = source_int(r.get("year_built"))
                if yr is not None and (yr < 1800 or yr > 2030):
                    yr = None
                lat = source_float(r.get("latitude"))
                lng = source_float(r.get("longitude"))
                if lat is not None and lng is not None and not (32.45 <= lat <= 42.05 and -124.50 <= lng <= -114.10):
                    lat, lng = None, None

                batch.append((
                    "ca",
                    bbl[:80],
                    addr,
                    (source_text(r.get("borough")) or "LOS ANGELES (LOS ANGELES)").upper(),
                    _clean_zip5(r.get("zip_code")),
                    cl_own,
                    own_norm,
                    care_of,
                    is_trunc,
                    mail,
                    mail_norm,
                    mail_coarse,
                    mst,
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
                    source_text(r.get("source_system")) or "CA_OPEN_ROLL_AND_BIZ_TAX",
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
        log(
            f"  ✓ Loaded {res['parcels_loaded']:,} CA improved parcels "
            f"({res['named_lessor_parcels']:,} lessor/business-identified; "
            f"{res['redacted_7928_parcels']:,} explicitly badged under Gov. Code § 7928.205)."
        )

    if overlays_gz and os.path.exists(overlays_gz):
        log(f"Attaching California Eviction Notices & Code Violations from `{overlays_gz}`...")
        by_bbl: Set = set()
        sf_by_addr: Dict[str, str] = {}
        la_by_addr: Dict[str, str] = {}
        with conn.cursor() as cur:
            cur.execute("SELECT bbl, address, borough FROM ca_properties")
            for bbl, addr, boro in cur.fetchall():
                by_bbl.add(bbl)
                an = normalize_link_address(addr or "")
                if not an:
                    continue
                if "SAN FRANCISCO" in (boro or ""):
                    sf_by_addr.setdefault(an, bbl)
                else:
                    la_by_addr.setdefault(an, bbl)

        stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "vt": 0, "vo": 0, "last_v": None, "et": 0, "last_e": None
        })
        with gzip.open(overlays_gz, "rt", encoding="utf-8", errors="replace") as gz:
            reader = csv.DictReader(gz)
            for r in reader:
                kind = source_text(r.get("record_kind"))
                phint = source_text(r.get("parcel_hint"))
                addr = source_text(r.get("address"))
                city = (source_text(r.get("city")) or "").upper()
                bbl = phint if (phint and phint in by_bbl) else None
                if not bbl and phint and city != "SAN FRANCISCO":
                    ain_digits = re.sub(r"[^0-9]", "", phint)
                    if f"CA-LA-{ain_digits}" in by_bbl:
                        bbl = f"CA-LA-{ain_digits}"
                if not bbl and addr:
                    an = normalize_link_address(addr)
                    bbl = sf_by_addr.get(an) if "SAN FRANCISCO" in city else la_by_addr.get(an)
                if not bbl:
                    continue

                st = stats[bbl]
                edt = _parse_any_date(r.get("event_date"))
                if kind == "eviction":
                    res["evictions_matched"] += 1
                    st["et"] += 1
                    if edt and (st["last_e"] is None or edt > st["last_e"]):
                        st["last_e"] = edt
                else:
                    res["violations_matched"] += 1
                    st["vt"] += 1
                    vstat = (source_text(r.get("status")) or "").upper()
                    if vstat not in ("COMPLIED", "CLOSED", "ABATED", "CANCELLED", "CANCELED", "RESOLVED"):
                        st["vo"] += 1
                    if edt and (st["last_v"] is None or edt > st["last_v"]):
                        st["last_v"] = edt

        if stats:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO ca_bbl_stats (
                        jurisdiction, bbl, violations_total, violations_open, last_violation_date,
                        evictions_total, last_eviction_date, updated_at
                    )
                    VALUES %s
                    ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                        violations_total = EXCLUDED.violations_total,
                        violations_open = EXCLUDED.violations_open,
                        last_violation_date = COALESCE(EXCLUDED.last_violation_date, ca_bbl_stats.last_violation_date),
                        evictions_total = EXCLUDED.evictions_total,
                        last_eviction_date = COALESCE(EXCLUDED.last_eviction_date, ca_bbl_stats.last_eviction_date),
                        updated_at = NOW();
                    """,
                    [("ca", b, s["vt"], s["vo"], s["last_v"], s["et"], s["last_e"]) for b, s in stats.items()],
                    template="(%s, %s, %s, %s, %s::date, %s, %s::date, NOW())",
                    page_size=2500,
                )
            conn.commit()
        _record_status(
            conn,
            "CA_METRO_ENFORCEMENT_AND_EVICTIONS",
            "city_enforcement",
            {
                "matched_records": res["violations_matched"] + res["evictions_matched"],
                "matched_parcels": len(stats),
                "violations_matched": res["violations_matched"],
                "evictions_matched": res["evictions_matched"],
                "message": (
                    f"Matched {res['violations_matched']:,} code violations (SF DBI & LA LADBS) and "
                    f"{res['evictions_matched']:,} eviction notices (SF Rent Board & LAHD) across {len(stats):,} California parcels."
                ),
                "sources": [
                    {"source": "SF Dept. of Building Inspection Notices of Violation (nbtm-fbw5)", "url": "https://data.sfgov.org/Housing-and-Buildings/Notices-of-Violation-issued-by-the-Department-of-B/nbtm-fbw5"},
                    {"source": "SF Rent Board Eviction Notices (5cei-gny5)", "url": "https://data.sfgov.org/Housing-and-Buildings/Eviction-Notices/5cei-gny5"},
                    {"source": "LA Housing Dept. (LAHD) Eviction Notices (2u8b-eyuu)", "url": "https://data.lacity.org/Housing-and-Real-Estate/Eviction-Notices/2u8b-eyuu"},
                    {"source": "LADBS Code Enforcement Cases (u82d-eh7z)", "url": "https://data.lacity.org/"},
                ],
            },
        )
        log(f"  ✓ Matched {res['violations_matched']:,} CA code violations & {res['evictions_matched']:,} eviction notices.")
    return res


def import_ca_cpra_secured_roll(conn, county_name: str, csv_path: str) -> int:
    """Track B drop-in importer for official County Assessor CPRA Secured Assessment
    Roll CSV/TSV files (`APN/AIN`, `Owner/Assessee`, `Mailing Address`, `Situs Address`,
    `Units`, `Year Built`, `Land Value`, `Improvement Value`)."""
    cnty = county_name.strip().upper()
    log(f"Importing Track B CPRA Secured Assessment Roll for {cnty} County from `{csv_path}`...")
    opener = gzip.open if csv_path.endswith(".gz") else open
    batch: List[tuple] = []
    loaded = 0
    with opener(csv_path, "rt", encoding="utf-8", errors="replace") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        for r in reader:
            norm_r = {k.strip().upper(): v for k, v in r.items() if k}
            apn = re.sub(r"[^0-9A-Za-z-]", "", source_text(norm_r.get("APN") or norm_r.get("AIN") or norm_r.get("PARCEL_NUMBER") or norm_r.get("PARCEL")) or "")
            owner = source_text(norm_r.get("OWNER_NAME") or norm_r.get("ASSESSEE") or norm_r.get("ASSESSEE_NAME") or norm_r.get("OWNER1"))
            addr = source_text(norm_r.get("SITUS_ADDRESS") or norm_r.get("PROPERTY_ADDRESS") or norm_r.get("ADDRESS"))
            imp = source_float(norm_r.get("IMPROVEMENT_VALUE") or norm_r.get("ROLL_IMPVALUE") or norm_r.get("IMP_VAL"))
            if not apn or not owner or not addr or (imp is not None and imp <= 0):
                continue
            if is_public_entity(owner) or is_excluded_third_party_entity(owner):
                continue
            city = (source_text(norm_r.get("SITUS_CITY") or norm_r.get("CITY")) or cnty).upper()
            z5 = _clean_zip5(norm_r.get("SITUS_ZIP") or norm_r.get("ZIP"))
            mail = source_text(norm_r.get("MAILING_ADDRESS") or norm_r.get("MAIL_ADDR")) or ""
            mst = source_text(norm_r.get("MAIL_STATE") or norm_r.get("STATE"))
            dem = demangle_assessor_owner(owner, mailing_address=mail, explicit_mailing_state=mst, col_max_len=80)
            cl_own = dem.cleaned_owner or owner
            cl_mail = strip_benign_care_of_prefix(mail)
            prefix = "SF" if cnty == "SAN FRANCISCO" else ("LA" if cnty == "LOS ANGELES" else cnty[:3])
            bbl = f"CA-{prefix}-{apn}"[:80]
            lnd = source_float(norm_r.get("LAND_VALUE") or norm_r.get("ROLL_LANDVALUE")) or 0
            tot = source_float(norm_r.get("TOTAL_VALUE") or norm_r.get("NET_VALUE")) or ((imp or 0) + lnd)
            units = source_int(norm_r.get("UNITS") or norm_r.get("NUMBER_OF_UNITS"))
            yr = source_int(norm_r.get("YEAR_BUILT") or norm_r.get("YEARBUILT1"))
            batch.append((
                "ca", bbl, addr, f"{city} ({cnty})", z5, cl_own,
                dem.owner_norm or normalize_owner_name(cl_own), dem.care_of_entity, dem.is_truncated,
                mail, normalize_link_address(cl_mail), normalize_mailing_address_coarse(cl_mail),
                dem.mailing_state or mst, source_text(norm_r.get("USE_DESCRIPTION") or norm_r.get("USE_CODE")),
                source_text(norm_r.get("USE_CODE")), None, units, units, yr, tot,
                f"CA_{cnty.replace(' ', '_')}_CPRA_SECURED_ROLL",
            ))
            if len(batch) >= 10000:
                execute_values(
                    cur,
                    """
                    INSERT INTO ca_properties (
                        jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
                        owner_co_name, owner_is_truncated, mailing_address, mailing_address_norm,
                        mailing_address_coarse, mailing_state, land_use, bld_class, num_floors,
                        units_res, units_total, year_built, assessed_total, source_system
                    )
                    VALUES %s
                    ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                        owner_name = EXCLUDED.owner_name,
                        owner_name_norm = EXCLUDED.owner_name_norm,
                        owner_co_name = EXCLUDED.owner_co_name,
                        mailing_address = EXCLUDED.mailing_address,
                        mailing_address_norm = EXCLUDED.mailing_address_norm,
                        mailing_address_coarse = EXCLUDED.mailing_address_coarse,
                        mailing_state = EXCLUDED.mailing_state,
                        source_system = EXCLUDED.source_system,
                        updated_at = NOW();
                    """,
                    batch,
                    page_size=2500,
                )
                loaded += len(batch)
                batch.clear()
        if batch:
            execute_values(
                cur,
                """
                INSERT INTO ca_properties (
                    jurisdiction, bbl, address, borough, zip_code, owner_name, owner_name_norm,
                    owner_co_name, owner_is_truncated, mailing_address, mailing_address_norm,
                    mailing_address_coarse, mailing_state, land_use, bld_class, num_floors,
                    units_res, units_total, year_built, assessed_total, source_system
                )
                VALUES %s
                ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                    owner_name = EXCLUDED.owner_name,
                    owner_name_norm = EXCLUDED.owner_name_norm,
                    owner_co_name = EXCLUDED.owner_co_name,
                    mailing_address = EXCLUDED.mailing_address,
                    mailing_address_norm = EXCLUDED.mailing_address_norm,
                    mailing_address_coarse = EXCLUDED.mailing_address_coarse,
                    mailing_state = EXCLUDED.mailing_state,
                    source_system = EXCLUDED.source_system,
                    updated_at = NOW();
                """,
                batch,
                page_size=2500,
            )
            loaded += len(batch)
    conn.commit()
    log(f"  ✓ Imported/upgraded {loaded:,} {cnty} County parcels from CPRA Secured Assessment Roll.")
    return loaded


def sync_ca_58_county_status(conn) -> int:
    """Compute per-county completeness across all 58 California counties—explicitly
    reporting `with_owner` excluding `CA GOV CODE 7928.205 REDACTED%` placeholders so
    owner-name coverage is 100% epistemically honest!"""
    log("Computing 58-County California completeness matrix (`CA_COUNTY_*` + `ca_prop_stats:v1`)...")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                CASE
                    WHEN bbl LIKE 'CA-SF-%%' OR borough LIKE 'SAN FRANCISCO%%' THEN 'SAN FRANCISCO'
                    WHEN bbl LIKE 'CA-LA-%%' OR borough LIKE '%%(LOS ANGELES)' THEN 'LOS ANGELES'
                    WHEN borough LIKE '%%(%%)' THEN SUBSTRING(borough FROM '\\(([^)]+)\\)$')
                    ELSE 'LOS ANGELES'
                END AS county_key,
                COUNT(*)::int AS total_properties,
                COUNT(CASE WHEN building_photo_url IS NOT NULL THEN 1 END)::int AS with_photos,
                COUNT(CASE WHEN cama_site_link IS NOT NULL THEN 1 END)::int AS with_cama,
                COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END)::int AS with_coords,
                COUNT(CASE WHEN owner_name IS NOT NULL AND UPPER(owner_name) NOT LIKE 'CURRENT OWNER%%' AND UPPER(owner_name) NOT LIKE 'CA GOV CODE 7928%%' THEN 1 END)::int AS with_owner,
                COUNT(CASE WHEN year_built IS NOT NULL THEN 1 END)::int AS with_year_built,
                MAX(CASE WHEN last_sale_date <= CURRENT_DATE THEN last_sale_date END) AS max_sale_date
            FROM ca_properties
            GROUP BY 1
            """
        )
        by_county = {(r["county_key"] or "").strip().upper(): dict(r) for r in cur.fetchall()}

        cache_rows = []
        for cnty in ALL_58_CA_COUNTIES:
            cfg = CA_COUNTY_CONFIG[cnty]
            st = by_county.get(cnty, {})
            total = int(st.get("total_properties") or 0)
            named_owners = int(st.get("with_owner") or 0)
            plat = cfg["platform"]
            status = "success" if total > 0 else "cpra_roll_ready"
            max_sale = st.get("max_sale_date")
            skey = ca_county_status_key(cnty)
            details = {
                "county": cnty,
                "display": cfg["display"],
                "platform": plat,
                "portal_url": cfg["portal_url"],
                "frequency": cfg["frequency"],
                "properties_count": total,
                "named_owner_parcels": named_owners,
                "statutory_redaction_note": (
                    "CA Gov. Code § 7928.205 restricts unrestricted online bulk publication of Name + APN; "
                    "Track A links registered business-tax lessors + `us_ca` LLC filings, while Track B loads "
                    "County Assessor CPRA Secured Assessment Rolls (Rev. & Tax. Code § 408.3)."
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
                "with_owner": named_owners,
                "with_year_built": int(st.get("with_year_built") or 0),
                "max_sale_date": max_sale.isoformat() if max_sale else None,
            })

        cur.execute(
            """
            INSERT INTO kv_cache (key, value, created_at)
            VALUES ('ca_prop_stats:v1', %s::jsonb, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = NOW();
            """,
            (json.dumps(cache_rows),),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()
    log(f"  ✓ Synced 58 California county completeness rows (`ca_prop_stats:v1`).")
    return len(cache_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="California 58-County Track A + Track B CPRA Roll Pipeline")
    parser.add_argument("--parcels-gz", default=None, help="Path to `ca_parcels.csv.gz`")
    parser.add_argument("--overlays-gz", default=None, help="Path to `ca_overlays.csv.gz`")
    parser.add_argument("--import-cpra-roll", nargs=2, metavar=("COUNTY", "CSV_PATH"), help="Import County Assessor CPRA Secured Roll file")
    parser.add_argument("--sync-baseline-only", action="store_true", help="Only sync 58-county status rows")
    parser.add_argument("--rebuild-networks", action="store_true", help="Link `us_ca` OpenCorporates and rebuild `ca_networks`")
    args = parser.parse_args()

    conn = psycopg2.connect(_resolve_database_url())
    try:
        ensure_nationwide_parent_tables(conn)
        ensure_jurisdiction_partition(conn, "ca")
        if not args.sync_baseline_only:
            if args.parcels_gz or args.overlays_gz:
                import_ca_track_a_spot_data(conn, args.parcels_gz, args.overlays_gz)
            if args.import_cpra_roll:
                import_ca_cpra_secured_roll(conn, args.import_cpra_roll[0], args.import_cpra_roll[1])
        sync_ca_58_county_status(conn)
        if args.rebuild_networks:
            p_cnt, n_cnt = build_and_save_state_networks(conn, "ca", "CA", "CA_ASSESSOR_ROLL_AND_BIZ_TAX")
            log(f"✓ Rebuilt California networks: {p_cnt:,} parcels -> {n_cnt:,} verified owner networks.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
