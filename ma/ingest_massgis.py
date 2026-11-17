#!/usr/bin/env python3
"""
ma/ingest_massgis.py
====================
Massachusetts Statewide (`ma`) CT-style replication pipeline built on the
partitioned nationwide schema (`us_properties` / `us_networks` -> `ma_*`) and
wired to the 5-tier deterministic OpenCorporates linker (`api/oc_linker.py`).

Upstream Official Source:
  MassGIS Level 3 Assessors' Property Tax Parcels (All 351 MA Municipalities)
  https://services1.arcgis.com/hGdibHYSPO59RG1h/ArcGIS/rest/services/Massachusetts_Property_Tax_Parcels/FeatureServer/0

Modes:
  - `--pilot` (default): Major Massachusetts rental & gateway municipalities
    (Worcester, Springfield, Cambridge, Lowell, Brockton, New Bedford,
     Fall River, Lynn, Quincy, Somerville, Lawrence, Newton, Framingham,
     Haverhill, Malden, Waltham, Medford, Revere, Everett, Chelsea, Holyoke,
     Salem, Chicopee, Pittsfield, Fitchburg)
  - `--towns <CITY1> <CITY2> ...`: Explicit municipality subset
  - `--full`: Full statewide ingestion across all 351 Massachusetts towns/cities
  - `--rebuild-only`: Re-run OpenCorporates 5-tier entity linking + network
    clustering in-place over existing `ma_properties` without re-downloading GIS
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import requests

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_jurisdiction_partition
from api.oc_linker import (
    demangle_assessor_owner,
    link_jurisdiction_parcels_to_oc,
    resolve_oc_company_principals,
)
from api.populate_cities import (
    GENERIC_OWNER_NAMES,
    is_public_entity,
    load_registrar_address_substrings,
    normalize_link_address,
    normalize_owner_name,
    run_union_find_clustering,
    source_float,
    source_int,
    source_text,
)
from api.shared_utils import (
    looks_like_person_owner,
    normalize_mailing_address_coarse,
    strip_benign_care_of_prefix,
)

def _resolve_database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@localhost")
    return raw


DATABASE_URL = _resolve_database_url()

MASSGIS_PARCELS_URL = (
    "https://services1.arcgis.com/hGdibHYSPO59RG1h/ArcGIS/rest/services/"
    "Massachusetts_Property_Tax_Parcels/FeatureServer/0/query"
)

MASSGIS_OUT_FIELDS = (
    "OBJECTID,LOC_ID,MAP_PAR_ID,TOWN_ID,PROP_ID,TOTAL_VAL,FY,LS_DATE,LS_PRICE,"
    "USE_CODE,SITE_ADDR,CITY,ZIP,OWNER1,OWN_CO,OWN_ADDR,OWN_CITY,OWN_STATE,"
    "OWN_ZIP,YEAR_BUILT,UNITS,STORIES"
)

PILOT_MUNICIPALITIES = [
    "BOSTON",
    "WORCESTER",
    "SPRINGFIELD",
    "CAMBRIDGE",
    "LOWELL",
    "BROCKTON",
    "QUINCY",
    "NEW BEDFORD",
    "LYNN",
    "FALL RIVER",
    "NEWTON",
    "SOMERVILLE",
    "LAWRENCE",
    "FRAMINGHAM",
    "HAVERHILL",
    "WALTHAM",
    "MALDEN",
    "BROOKLINE",
    "PLYMOUTH",
    "MEDFORD",
    "TAUNTON",
    "CHICOPEE",
    "WEYMOUTH",
    "REVERE",
    "PEABODY",
    "METHUEN",
    "BARNSTABLE",
    "PITTSFIELD",
    "ARLINGTON",
    "EVERETT",
    "SALEM",
    "ATTLEBORO",
    "CHELSEA",
    "HOLYOKE",
    "FITCHBURG",
]

MA_BOUNDS = (41.20, 42.90, -73.55, -69.90)


def _clean_ma_zip(raw_zip: Any) -> Optional[str]:
    txt = source_text(raw_zip)
    if not txt:
        return None
    m = re.search(r"\b(\d{4,5})\b", txt)
    if m:
        d = m.group(1)
        return ("0" + d) if len(d) == 4 else d[:5]
    digits = re.sub(r"[^0-9]", "", txt)
    if len(digits) == 4:
        return "0" + digits
    if len(digits) >= 5:
        return digits[:5]
    return None


def _parse_ls_date(raw_val: Any) -> Optional[dt.date]:
    txt = source_text(raw_val)
    if not txt:
        return None
    digits = re.sub(r"[^0-9]", "", txt)
    if len(digits) == 8:
        try:
            yr, mo, dy = int(digits[:4]), int(digits[4:6]), int(digits[6:8])
            if 1750 <= yr <= 2035 and 1 <= mo <= 12 and 1 <= dy <= 31:
                return dt.date(yr, mo, dy)
        except ValueError:
            return None
    return None


def _ma_use_code_label(use_code: Optional[str]) -> Optional[str]:
    if not use_code:
        return None
    u = use_code.strip()
    if u.startswith("101"):
        return "SINGLE FAMILY"
    if u.startswith("102"):
        return "RESIDENTIAL CONDOMINIUM"
    if u.startswith("104"):
        return "TWO-FAMILY RESIDENTIAL"
    if u.startswith("105"):
        return "THREE-FAMILY RESIDENTIAL"
    if u.startswith(("111", "112")):
        return "APARTMENTS 4+ UNITS"
    if u.startswith("01") or u.startswith("03"):
        return "MIXED-USE RESIDENTIAL/COMMERCIAL"
    if u.startswith("3"):
        return "COMMERCIAL"
    if u.startswith("4"):
        return "INDUSTRIAL"
    if u.startswith("9"):
        return "EXEMPT / INSTITUTIONAL"
    return f"USE {u}"


def _feature_to_parcel_dict(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    attrs = feat.get("attributes") or {}
    town_id = source_int(attrs.get("TOWN_ID"))
    prop_id = source_text(attrs.get("PROP_ID")) or source_text(attrs.get("MAP_PAR_ID"))
    loc_id = source_text(attrs.get("LOC_ID"))
    raw_owner1 = source_text(attrs.get("OWNER1"))
    site_addr = source_text(attrs.get("SITE_ADDR"))
    if not raw_owner1 or not site_addr:
        return None

    if town_id is not None and prop_id:
        bbl = f"M{town_id:03d}-{prop_id}"
    elif loc_id:
        bbl = loc_id
    else:
        return None

    own_co = source_text(attrs.get("OWN_CO"))
    own_addr = source_text(attrs.get("OWN_ADDR"))
    own_city = source_text(attrs.get("OWN_CITY"))
    own_state = source_text(attrs.get("OWN_STATE"))
    own_zip = _clean_ma_zip(attrs.get("OWN_ZIP"))

    mail_parts = []
    if own_co:
        mail_parts.append(own_co if own_co.upper().startswith(("C/O", "ATTN", "CARE OF")) else f"C/O {own_co}")
    for comp in (own_addr, own_city, own_state, own_zip):
        if comp:
            mail_parts.append(comp)
    mailing_address = ", ".join(mail_parts)

    demangled = demangle_assessor_owner(
        raw_owner1,
        care_of=own_co,
        mailing_address=mailing_address,
        explicit_mailing_state=own_state,
        col_max_len=40,
    )
    cleaned_owner = demangled.cleaned_owner or raw_owner1
    clean_mail = strip_benign_care_of_prefix(mailing_address)
    coarse_mail = normalize_mailing_address_coarse(clean_mail)

    centroid = feat.get("centroid") or {}
    lng = source_float(centroid.get("x"))
    lat = source_float(centroid.get("y"))
    if lat is not None and lng is not None:
        min_lat, max_lat, min_lon, max_lon = MA_BOUNDS
        if not (min_lat <= lat <= max_lat and min_lon <= lng <= max_lon):
            lat, lng = None, None

    city_name = (source_text(attrs.get("CITY")) or f"TOWN {town_id or ''}").strip().upper()
    site_zip = _clean_ma_zip(attrs.get("ZIP"))
    use_code = source_text(attrs.get("USE_CODE"))
    yr_built = source_int(attrs.get("YEAR_BUILT"))
    if yr_built is not None and (yr_built <= 1600 or yr_built > 2030):
        yr_built = None
    units = source_int(attrs.get("UNITS"))
    if units is not None and units < 0:
        units = 0

    return {
        "bbl": bbl,
        "address": site_addr,
        "borough": city_name,
        "zip_code": site_zip,
        "owner_name": cleaned_owner,
        "owner_name_norm": demangled.owner_norm or normalize_owner_name(cleaned_owner),
        "owner_co_name": demangled.care_of_entity,
        "owner_is_truncated": demangled.is_truncated,
        "mailing_address": mailing_address,
        "mailing_address_norm": normalize_link_address(clean_mail),
        "mailing_address_coarse": coarse_mail,
        "mailing_state": demangled.mailing_state,
        "land_use": _ma_use_code_label(use_code),
        "bld_class": use_code,
        "num_floors": source_float(attrs.get("STORIES")),
        "units_res": units,
        "units_total": units,
        "year_built": yr_built,
        "assessed_total": source_float(attrs.get("TOTAL_VAL")),
        "last_sale_date": _parse_ls_date(attrs.get("LS_DATE")),
        "last_sale_price": source_float(attrs.get("LS_PRICE")),
        "latitude": lat,
        "longitude": lng,
        "source_system": "MASSGIS_L3_ASSESSOR",
    }


def _fetch_where_cursor(where_base: str, label: str, page_size: int = 2000) -> List[Dict[str, Any]]:
    last_oid = -1
    parcels: List[Dict[str, Any]] = []
    session = requests.Session()
    try:
        while True:
            where = f"({where_base}) AND OBJECTID > {last_oid} AND OWNER1 IS NOT NULL AND SITE_ADDR IS NOT NULL"
            params = {
                "where": where,
                "outFields": MASSGIS_OUT_FIELDS,
                "returnGeometry": "false",
                "returnCentroid": "true",
                "outSR": "4326",
                "orderByFields": "OBJECTID ASC",
                "resultRecordCount": page_size,
                "f": "json",
            }
            data: Dict[str, Any] = {}
            for attempt in range(4):
                try:
                    resp = session.get(MASSGIS_PARCELS_URL, params=params, timeout=45)
                    resp.raise_for_status()
                    data = resp.json()
                    if "error" in data:
                        raise RuntimeError(f"MassGIS ArcGIS error: {data['error']}")
                    break
                except Exception as exc:
                    if attempt == 3:
                        raise RuntimeError(f"MassGIS fetch failed for {label} after OBJECTID={last_oid}: {exc}") from exc
                    time.sleep(2 * (attempt + 1))

            features = data.get("features") or []
            if not features:
                break

            max_oid = last_oid
            for feat in features:
                oid = source_int((feat.get("attributes") or {}).get("OBJECTID"))
                if oid is not None and oid > max_oid:
                    max_oid = oid
                p = _feature_to_parcel_dict(feat)
                if p is not None:
                    parcels.append(p)

            if max_oid <= last_oid:
                break
            last_oid = max_oid
            if len(features) < page_size and not data.get("exceededTransferLimit"):
                break
    finally:
        session.close()
    print(f"  [{label}] fetched {len(parcels):,} MassGIS parcels.")
    return parcels


def fetch_massgis_parcels(towns: Optional[Sequence[str]] = None, full_statewide: bool = False) -> List[Dict[str, Any]]:
    if full_statewide:
        # Query min/max OBJECTID and shard across 8 parallel cursor workers
        meta = requests.get(
            MASSGIS_PARCELS_URL,
            params={
                "where": "1=1",
                "outStatistics": json.dumps([
                    {"statisticType": "min", "onStatisticField": "OBJECTID", "outStatisticFieldName": "min_oid"},
                    {"statisticType": "max", "onStatisticField": "OBJECTID", "outStatisticFieldName": "max_oid"},
                ]),
                "f": "json",
            },
            timeout=30,
        ).json()
        stat_attrs = ((meta.get("features") or [{}])[0].get("attributes")) or {}
        min_oid = int(stat_attrs.get("min_oid") or 1)
        max_oid = int(stat_attrs.get("max_oid") or 3000000)
        num_shards = 8
        step = max(1, (max_oid - min_oid + 1) // num_shards)
        shards = []
        for idx in range(num_shards):
            lo = min_oid + idx * step
            hi = max_oid if idx == num_shards - 1 else (lo + step - 1)
            shards.append((
                f"OBJECTID >= {lo} AND OBJECTID <= {hi} AND (USE_CODE IS NULL OR (USE_CODE NOT LIKE '9%' AND USE_CODE NOT LIKE '13%'))",
                f"MA-Shard-{idx+1}",
            ))
        all_rows: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = [pool.submit(_fetch_where_cursor, w, lbl) for w, lbl in shards]
            for fut in as_completed(futs):
                all_rows.extend(fut.result())
        return all_rows

    selected = [t.strip().upper() for t in (towns or PILOT_MUNICIPALITIES) if t.strip()]
    all_rows = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {
            pool.submit(_fetch_where_cursor, f"CITY = '{town.replace(chr(39), chr(39)*2)}'", town): town
            for town in selected
        }
        for fut in as_completed(futs):
            all_rows.extend(fut.result())
    return all_rows


def build_and_save_ma_networks(conn) -> Tuple[int, int]:
    """Run OpenCorporates 5-tier deterministic linking (if OC data loaded) and guarded
    Union-Find + corporate-principal network clustering over `ma_properties`."""
    oc_stats = link_jurisdiction_parcels_to_oc(conn, "ma", "MA", col_max_len=40)
    if oc_stats["evaluated"]:
        print(f"  OpenCorporates linker evaluated {oc_stats['evaluated']:,} MA business parcels -> {oc_stats['matched']:,} matched.")

    registrar_substrings = load_registrar_address_substrings(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT bbl, address, borough, zip_code, owner_name, owner_name_norm,
                   mailing_address, mailing_address_norm, owner_email, owner_email_norm,
                   land_use, bld_class, num_floors, units_res, units_total, year_built,
                   assessed_total, latitude, longitude,
                   oc_jurisdiction_code, oc_company_number, oc_match_tier, oc_match_confidence
            FROM ma_properties
            WHERE bbl IS NOT NULL
            """
        )
        properties = [dict(r) for r in cur.fetchall()]

    if not properties:
        return 0, 0

    network_groups = run_union_find_clustering(properties, "ma", registrar_substrings=registrar_substrings)

    # Second-stage OpenCorporates corporate/branch/beneficial-principal untangling
    # (active immediately whenever `oc_companies` / `oc_officers` are populated on Nov. 17)
    from api.oc_linker import cluster_with_oc_and_series
    oc_keys = list({
        (p["oc_jurisdiction_code"], p["oc_company_number"])
        for p in properties
        if p.get("oc_jurisdiction_code") and p.get("oc_company_number")
    })
    oc_resolved = resolve_oc_company_principals(conn, oc_keys) if oc_keys else {}

    network_groups, oc_merges = cluster_with_oc_and_series(
        properties,
        network_groups,
        oc_resolved,
        is_public_fn=is_public_entity,
        generic_owner_names=GENERIC_OWNER_NAMES,
    )
    if oc_merges:
        print(f"  [MA] Corporate registry & series untangling merged {oc_merges:,} multi-entity links.")

    db_networks = []
    for root_bbl, group in network_groups.items():
        dedup = {p["bbl"]: p for p in group if p.get("bbl")}
        group = list(dedup.values())
        if not group:
            continue

        owner_counts: Dict[str, int] = {}
        for p in group:
            owner_counts[p["owner_name"]] = owner_counts.get(p["owner_name"], 0) + 1
        display_name = max(owner_counts, key=owner_counts.get)

        human_counts: Dict[str, int] = {
            nm: cnt for nm, cnt in owner_counts.items() if looks_like_person_owner(nm)
        }
        oc_company_ids = set()
        oc_holding_entities = set()
        for p in group:
            oj, on = p.get("oc_jurisdiction_code"), p.get("oc_company_number")
            if oj and on:
                oc_company_ids.add(f"{oj}/{on}")
                info = oc_resolved.get((oj, on), {})
                for h in info.get("humans", ()):
                    human_counts[h] = human_counts.get(h, 0) + 1
                oc_holding_entities.update(info.get("holding_entities", ()))

        entity_counts = {
            nm: cnt for nm, cnt in owner_counts.items() if not looks_like_person_owner(nm)
        }
        source_humans = [
            nm for nm, _ in sorted(human_counts.items(), key=lambda kv: (-kv[1], kv[0]))
        ]
        primary_human = source_humans[0] if source_humans else None

        network_key = f"network_ma_{root_bbl.lower().replace(' ', '_')}"
        anchor_type = (
            "person"
            if not any(
                w in display_name.upper()
                for w in ("LLC", "INC", "CORP", "GROUP", "PARTNERS", "HOLDINGS", "ASSOCIATES", "TRUST", "LP", "REALTY")
            )
            else "corp"
        )
        member_names = sorted({p["owner_name"] for p in group if p.get("owner_name")} | oc_holding_entities)
        member_addrs = sorted({p["mailing_address"] for p in group if p.get("mailing_address")})
        member_addr_norms = sorted({p["mailing_address_norm"] for p in group if p.get("mailing_address_norm")})
        bbl_list = [p["bbl"] for p in group]
        borough_summary: Dict[str, int] = {}
        total_units = 0
        for p in group:
            bname = p.get("borough") or "MASSACHUSETTS"
            borough_summary[bname] = borough_summary.get(bname, 0) + 1
            total_units += int(float(p.get("units_res") or 0))

        provenance = ["MASSGIS_L3_ASSESSORS_PARCELS"]
        if oc_company_ids:
            provenance.append("OPENCORPORATES_BULK_REGISTRY")
        connection_signals = {
            "member_name_count": len(member_names),
            "member_address_count": len(member_addrs),
            "member_address_norm_count": len(member_addr_norms),
            "has_mailing_address_links": bool(member_addr_norms),
            "oc_matched_companies": sorted(oc_company_ids)[:25],
            "source_human_names": source_humans[:25],
            "primary_human_name": primary_human,
            "primary_entity_name": max(entity_counts, key=entity_counts.get) if entity_counts else None,
            "principal_status": "source_listed_person" if primary_human else "unresolved_entity",
            "provenance": provenance,
            "link_confidence": 0.99 if oc_company_ids else (1.0 if len(member_names) == 1 else 0.88),
            "data_transparency_note": (
                "Assembled strictly from official MassGIS Level 3 municipal tax-assessor parcels "
                "and verified corporate registry links where present. No values inferred or fabricated."
            ),
        }
        db_networks.append((
            "ma",
            network_key,
            anchor_type,
            display_name,
            member_names,
            member_addrs,
            [],
            bbl_list,
            len(bbl_list),
            total_units,
            json.dumps(borough_summary),
            json.dumps(connection_signals),
            sorted(oc_company_ids) if oc_company_ids else None,
        ))

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE ma_networks;")
        execute_values(
            cur,
            """
            INSERT INTO ma_networks (
                jurisdiction, network_key, anchor_type, display_name, member_names,
                member_addresses, registration_ids, bbl_list, building_count, unit_count,
                borough_summary, connection_signals, oc_company_ids
            )
            VALUES %s
            ON CONFLICT (jurisdiction, network_key) DO NOTHING;
            """,
            db_networks,
            page_size=2000,
        )
        cur.execute(
            """
            INSERT INTO data_source_status (
                source_name, source_type, last_refreshed_at, refresh_status, details
            )
            VALUES ('MA', 'state_dataset', NOW(), 'success', %s::jsonb)
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                last_refreshed_at = NOW(),
                refresh_status = 'success',
                details = EXCLUDED.details;
            """,
            (
                json.dumps({
                    "properties_count": len(properties),
                    "networks_count": len(db_networks),
                    "municipalities_loaded": len({p.get("borough") for p in properties if p.get("borough")}),
                    "oc_matched_parcels": oc_stats.get("matched", 0),
                }),
            ),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix' OR key LIKE '%ma%'")
    conn.commit()
    return len(properties), len(db_networks)


def ingest_massachusetts(
    conn,
    *,
    towns: Optional[Sequence[str]] = None,
    full_statewide: bool = False,
    rebuild_only: bool = False,
) -> Tuple[int, int]:
    ensure_jurisdiction_partition(conn, "ma")
    if not rebuild_only:
        raw_parcels = fetch_massgis_parcels(towns=towns, full_statewide=full_statewide)
        # Deduplicate multi-card CAMA records sharing `M<town_id>-<prop_id>`, keeping max units/value
        deduped: Dict[str, Dict[str, Any]] = {}
        for p in raw_parcels:
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

        rows = [
            (
                "ma",
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
        print(f"Writing {len(rows):,} deduplicated Massachusetts parcels into partitioned `ma_properties`...")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE ma_properties;")
            execute_values(
                cur,
                """
                INSERT INTO ma_properties (
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
                page_size=2500,
            )
        conn.commit()

    return build_and_save_ma_networks(conn)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Massachusetts Statewide (MassGIS L3) property parcels & networks.")
    parser.add_argument("--full", action="store_true", help="Ingest all 351 Massachusetts municipalities (~2.56M parcels).")
    parser.add_argument("--towns", nargs="+", default=None, help="Specific MA municipality names (e.g., WORCESTER SPRINGFIELD CAMBRIDGE).")
    parser.add_argument("--rebuild-only", action="store_true", help="Re-link OC entities & rebuild `ma_networks` from existing `ma_properties`.")
    args = parser.parse_args()

    t0 = time.time()
    conn = psycopg2.connect(DATABASE_URL)
    try:
        props_cnt, nets_cnt = ingest_massachusetts(
            conn,
            towns=args.towns,
            full_statewide=args.full,
            rebuild_only=args.rebuild_only,
        )
        print(
            f"✓ Massachusetts (`ma`) complete in {time.time() - t0:.1f}s: "
            f"{props_cnt:,} parcels -> {nets_cnt:,} private landlord networks."
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
