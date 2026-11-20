#!/usr/bin/env python3
"""
gcp/import_spot_expansion_v5.py
===============================
Additively imports Spot Expansion V5 artifacts from `data/spot_expansion_v5/`
into local PostgreSQL (NEVER truncating existing counties or properties):

  1. `oh_summit_v5_parcels.csv.gz`     -> `oh_properties` (+199,178 Summit County / Akron OH parcels)
  2. `hud_federal_subsidies_v5.csv.gz` ->
       a) Permanent 50-state table `federal_housing_subsidies` (`105,391` HUD LIHTC,
          HUD Multifamily Assisted + REAC inspection scores, HUD FHA-Insured MF + REAC,
          and USDA Section 515 Rural Multifamily rows)
       b) Address-matched enrichment of `property_subsidies` (CT) and
          `nhpd_subsidy` / `nhpd_program` / `nhpd_expiration` across ALL live
          nationwide `*_properties` tables (preserving any existing NHPD records).
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import datetime as dt
import gzip
import os
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
    _base_addr_key,
    _record_status,
    _resolve_db_url,
    import_additive_parcels,
)
from states.ingest_statewide_parcels import _parse_any_date

LOCAL_DIR = os.path.join(REPO_ROOT, "data", "spot_expansion_v5")

# Map 2-letter US state -> list of (table_name, jurisdiction_col_or_None)
STATE_TO_PROPERTY_TABLES: Dict[str, List[Tuple[str, Any]]] = {
    "AR": [("ar_properties", "ar")],
    "CA": [("ca_properties", "ca")],
    "CO": [("co_properties", "co")],
    "DC": [("dc_properties", None)],
    "FL": [("fl_properties", "fl"), ("miami_properties", None)],
    "IL": [("il_properties", "il"), ("chicago_properties", None)],
    "MA": [("ma_properties", "ma"), ("boston_properties", None)],
    "MD": [("baltimore_properties", None)],
    "MI": [("detroit_properties", None)],
    "MN": [("mn_properties", "mn"), ("minneapolis_properties", None)],
    "MT": [("mt_properties", "mt")],
    "NC": [("nc_properties", "nc")],
    "NJ": [("nj_properties", "nj")],
    "NY": [("ny_properties", "ny"), ("nyc_properties", None)],
    "OH": [("oh_properties", "oh")],
    "PA": [("philadelphia_properties", None)],
    "VA": [("va_properties", "va")],
    "VT": [("vt_properties", "vt")],
    "WI": [("wi_properties", "wi")],
}


def log(msg: str) -> None:
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] [IMPORT-V5] {msg}", flush=True)


def ensure_federal_subsidies_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS federal_housing_subsidies (
                id                  SERIAL PRIMARY KEY,
                source_dataset      TEXT NOT NULL,
                federal_id          TEXT NOT NULL,
                property_name       TEXT,
                address             TEXT,
                address_norm        TEXT,
                base_addr_key       TEXT,
                city                TEXT,
                state               TEXT,
                zip_code            TEXT,
                program_name        TEXT,
                subsidy_type        TEXT,
                total_units         INTEGER,
                assisted_units      INTEGER,
                expiry_date         DATE,
                reac_score          INTEGER,
                reac_date           DATE,
                mgmt_agent_name     TEXT,
                mgmt_agent_address  TEXT,
                latitude            DOUBLE PRECISION,
                longitude           DOUBLE PRECISION,
                source_url          TEXT,
                updated_at          TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (source_dataset, federal_id)
            );
            CREATE INDEX IF NOT EXISTS idx_fed_sub_state_addr ON federal_housing_subsidies (state, address_norm);
            CREATE INDEX IF NOT EXISTS idx_fed_sub_state_base ON federal_housing_subsidies (state, base_addr_key);
            CREATE INDEX IF NOT EXISTS idx_fed_sub_reac ON federal_housing_subsidies (state, reac_score) WHERE reac_score IS NOT NULL;
            """
        )
    conn.commit()


def import_federal_subsidies(conn, gz_path: str) -> Dict[str, Any]:
    if not os.path.exists(gz_path):
        log(f"Missing {gz_path}; skipping federal subsidies.")
        return {}
    ensure_federal_subsidies_table(conn)
    log(f"Loading 50-state federal housing subsidies & REAC scores from `{os.path.basename(gz_path)}`...")

    by_key: Dict[Tuple[str, str], Tuple[Any, ...]] = {}
    by_state_addr: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    by_state_base: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    reac_count = 0

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as gz:
        for r in csv.DictReader(gz):
            src = source_text(r.get("source_dataset")) or ""
            fid = source_text(r.get("federal_id")) or ""
            st = (source_text(r.get("state")) or "")[:2].upper()
            addr = source_text(r.get("address")) or ""
            if not src or not fid:
                continue
            an = normalize_link_address(addr) if addr else ""
            bn = _base_addr_key(addr) if addr else ""
            prog = source_text(r.get("program_name")) or src
            stype = source_text(r.get("subsidy_type")) or src
            tot_u = source_int(r.get("total_units"))
            ass_u = source_int(r.get("assisted_units"))
            exp_dt = _parse_any_date(r.get("expiry_date"))
            reac_sc = source_int(r.get("reac_score"))
            reac_dt = _parse_any_date(r.get("reac_date"))
            if reac_sc is not None:
                reac_count += 1
            lat = source_float(r.get("latitude"))
            lng = source_float(r.get("longitude"))
            city = source_text(r.get("city")) or ""
            zcode = (source_text(r.get("zip_code")) or "")[:5]
            surl = source_text(r.get("source_url")) or ""

            by_key[(src, fid)] = (
                src, fid, source_text(r.get("property_name")) or "",
                addr, an, bn, city, st, zcode,
                prog, stype, tot_u, ass_u, exp_dt,
                reac_sc, reac_dt,
                source_text(r.get("mgmt_agent_name")) or "",
                source_text(r.get("mgmt_agent_address")) or "",
                lat, lng, surl,
            )

            if st and (an or bn):
                label = prog
                if reac_sc is not None:
                    label = f"{prog} (REAC {reac_sc})"
                info = {
                    "program": label[:180],
                    "subsidy_type": stype[:80],
                    "units": ass_u or tot_u,
                    "expiry": exp_dt,
                    "zip": zcode,
                    "city": city.upper(),
                    "source_url": surl,
                }
                for bucket, key in ((by_state_addr[st], an), (by_state_base[st], bn)):
                    if not key:
                        continue
                    prev = bucket.get(key)
                    if prev is None:
                        bucket[key] = dict(info)
                    else:
                        if label[:60] not in prev["program"] and len(prev["program"]) < 140:
                            prev["program"] = f"{prev['program']} | {label}"[:180]
                        if exp_dt and (prev["expiry"] is None or exp_dt > prev["expiry"]):
                            prev["expiry"] = exp_dt
                        if (ass_u or 0) > (prev["units"] or 0):
                            prev["units"] = ass_u

    rows = list(by_key.values())
    with conn.cursor() as cur:
        for i in range(0, len(rows), 5000):
            execute_values(
                cur,
                """
                INSERT INTO federal_housing_subsidies (
                    source_dataset, federal_id, property_name, address, address_norm, base_addr_key,
                    city, state, zip_code, program_name, subsidy_type, total_units, assisted_units,
                    expiry_date, reac_score, reac_date, mgmt_agent_name, mgmt_agent_address,
                    latitude, longitude, source_url, updated_at
                )
                VALUES %s
                ON CONFLICT (source_dataset, federal_id) DO UPDATE SET
                    property_name = EXCLUDED.property_name,
                    address = EXCLUDED.address,
                    address_norm = EXCLUDED.address_norm,
                    base_addr_key = EXCLUDED.base_addr_key,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    program_name = EXCLUDED.program_name,
                    subsidy_type = EXCLUDED.subsidy_type,
                    total_units = EXCLUDED.total_units,
                    assisted_units = EXCLUDED.assisted_units,
                    expiry_date = EXCLUDED.expiry_date,
                    reac_score = EXCLUDED.reac_score,
                    reac_date = EXCLUDED.reac_date,
                    mgmt_agent_name = EXCLUDED.mgmt_agent_name,
                    mgmt_agent_address = EXCLUDED.mgmt_agent_address,
                    latitude = COALESCE(EXCLUDED.latitude, federal_housing_subsidies.latitude),
                    longitude = COALESCE(EXCLUDED.longitude, federal_housing_subsidies.longitude),
                    source_url = EXCLUDED.source_url,
                    updated_at = NOW();
                """,
                rows[i : i + 5000],
                template=(
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::date,"
                    "%s,%s::date,%s,%s,%s,%s,%s,NOW())"
                ),
                page_size=2500,
            )
    conn.commit()
    log(f"  ✓ Upserted {len(rows):,} rows into `federal_housing_subsidies` ({reac_count:,} with HUD REAC scores).")

    # Cross-match into CT `property_subsidies`
    ct_matched = 0
    if by_state_addr.get("CT"):
        with conn.cursor() as cur:
            cur.execute("SELECT id, location, property_city FROM properties WHERE location IS NOT NULL")
            ct_inserts = []
            seen_ct = set()
            for pid, loc, town in cur.fetchall():
                an = normalize_link_address(loc or "")
                hit = by_state_addr["CT"].get(an) if an else None
                if not hit:
                    bn = _base_addr_key(loc or "")
                    cand = by_state_base["CT"].get(bn) if bn else None
                    if cand and (not cand["city"] or not town or cand["city"] == str(town).upper().strip()):
                        hit = cand
                if hit and pid not in seen_ct:
                    seen_ct.add(pid)
                    ct_inserts.append((pid, hit["program"], hit["subsidy_type"], hit["units"], hit["expiry"], hit["source_url"]))
            if ct_inserts:
                cur.execute("SELECT DISTINCT property_id FROM property_subsidies")
                existing_ct = {x[0] for x in cur.fetchall()}
                new_ct = [x for x in ct_inserts if x[0] not in existing_ct]
                if new_ct:
                    execute_values(
                        cur,
                        """
                        INSERT INTO property_subsidies (
                            property_id, program_name, subsidy_type, units_subsidized, expiry_date, last_updated, source_url
                        )
                        VALUES %s
                        """,
                        new_ct,
                        template="(%s, %s, %s, %s, %s::date, NOW(), %s)",
                        page_size=2000,
                    )
                    ct_matched = len(new_ct)
        conn.commit()
        log(f"  ✓ Added {ct_matched:,} newly matched federal subsidized CT properties into `property_subsidies`.")

    # Cross-match into nationwide `<jur>_bbl_stats` tables (joined by address against `<jur>_properties`)
    matched_by_table: Dict[str, int] = {"ct_properties": ct_matched}
    for st, targets in STATE_TO_PROPERTY_TABLES.items():
        amap = by_state_addr.get(st) or {}
        bmap = by_state_base.get(st) or {}
        if not amap and not bmap:
            continue
        for tbl, jur_val in targets:
            stats_tbl = tbl.replace("_properties", "_bbl_stats")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (tbl,),
                )
                pcols = {x[0] for x in cur.fetchall()}
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (stats_tbl,),
                )
                scols = {x[0] for x in cur.fetchall()}
                if "address" not in pcols or "nhpd_subsidy" not in scols:
                    continue
                zip_col = "zip_code" if "zip_code" in pcols else "NULL"
                bor_col = "borough" if "borough" in pcols else "NULL"
                where_j = f"WHERE jurisdiction = '{jur_val}'" if (jur_val and "jurisdiction" in pcols) else ""
                cur.execute(f"SELECT bbl, address, {zip_col}, {bor_col} FROM {tbl} {where_j}")
                updates_by_bbl: Dict[str, Tuple[str, Any]] = {}
                for bbl, addr, zc, bor in cur.fetchall():
                    if not addr or not bbl or bbl in updates_by_bbl:
                        continue
                    an = normalize_link_address(addr)
                    hit = amap.get(an) if an else None
                    if not hit:
                        bn = _base_addr_key(addr)
                        cand = bmap.get(bn) if bn else None
                        if cand:
                            z5 = (str(zc or "").strip())[:5]
                            bor_u = str(bor or "").upper()
                            if (cand["zip"] and z5 and cand["zip"] == z5) or (cand["city"] and cand["city"] in bor_u):
                                hit = cand
                    if hit:
                        updates_by_bbl[bbl] = (hit["program"], hit["expiry"])
                updates = list(updates_by_bbl.items())
                if updates:
                    has_jur_stats = bool(jur_val and "jurisdiction" in scols)
                    for i in range(0, len(updates), 5000):
                        chunk = updates[i : i + 5000]
                        if has_jur_stats:
                            execute_values(
                                cur,
                                f"""
                                INSERT INTO {stats_tbl} (
                                    jurisdiction, bbl, nhpd_subsidy, nhpd_program, nhpd_expiration, updated_at
                                )
                                VALUES %s
                                ON CONFLICT (jurisdiction, bbl) DO UPDATE SET
                                    nhpd_subsidy = TRUE,
                                    nhpd_program = CASE
                                        WHEN COALESCE({stats_tbl}.nhpd_program, '') = '' THEN EXCLUDED.nhpd_program
                                        WHEN POSITION('REAC ' IN EXCLUDED.nhpd_program) > 0
                                             AND POSITION('REAC ' IN {stats_tbl}.nhpd_program) = 0
                                            THEN LEFT({stats_tbl}.nhpd_program || ' | ' || EXCLUDED.nhpd_program, 240)
                                        ELSE {stats_tbl}.nhpd_program
                                    END,
                                    nhpd_expiration = COALESCE({stats_tbl}.nhpd_expiration, EXCLUDED.nhpd_expiration),
                                    updated_at = NOW();
                                """,
                                [(jur_val, bbl, prog, exp) for bbl, (prog, exp) in chunk],
                                template="(%s, %s, TRUE, %s, %s::date, NOW())",
                                page_size=2500,
                            )
                        else:
                            execute_values(
                                cur,
                                f"""
                                INSERT INTO {stats_tbl} (
                                    bbl, nhpd_subsidy, nhpd_program, nhpd_expiration, updated_at
                                )
                                VALUES %s
                                ON CONFLICT (bbl) DO UPDATE SET
                                    nhpd_subsidy = TRUE,
                                    nhpd_program = CASE
                                        WHEN COALESCE({stats_tbl}.nhpd_program, '') = '' THEN EXCLUDED.nhpd_program
                                        WHEN POSITION('REAC ' IN EXCLUDED.nhpd_program) > 0
                                             AND POSITION('REAC ' IN {stats_tbl}.nhpd_program) = 0
                                            THEN LEFT({stats_tbl}.nhpd_program || ' | ' || EXCLUDED.nhpd_program, 240)
                                        ELSE {stats_tbl}.nhpd_program
                                    END,
                                    nhpd_expiration = COALESCE({stats_tbl}.nhpd_expiration, EXCLUDED.nhpd_expiration),
                                    updated_at = NOW();
                                """,
                                [(bbl, prog, exp) for bbl, (prog, exp) in chunk],
                                template="(%s, TRUE, %s, %s::date, NOW())",
                                page_size=2500,
                            )
                    conn.commit()
                matched_by_table[stats_tbl] = len(updates)
                if updates:
                    log(f"  ✓ Matched {len(updates):,} federal subsidized / REAC parcels in `{stats_tbl}`.")

    total_matched = sum(matched_by_table.values())
    _record_status(
        conn,
        "FEDERAL_HUD_USDA_SUBSIDIES",
        "federal_subsidies",
        {
            "catalog_rows": len(rows),
            "reac_scored_rows": reac_count,
            "matched_parcels_total": total_matched,
            "matched_by_table": matched_by_table,
            "message": f"Loaded {len(rows):,} HUD/USDA federal subsidy & REAC records; matched {total_matched:,} parcels across live jurisdictions.",
        },
    )
    return {"catalog": len(rows), "reac": reac_count, "matched_total": total_matched, "by_table": matched_by_table}


def main() -> int:
    parser = argparse.ArgumentParser(description="Import Spot Expansion V5 (Federal Subsidies/REAC + Summit OH).")
    parser.add_argument("--skip-oh", action="store_true")
    parser.add_argument("--skip-subsidies", action="store_true")
    args = parser.parse_args()

    conn = psycopg2.connect(_resolve_db_url())
    try:
        oh_n = 0
        if not args.skip_oh:
            oh_n = import_additive_parcels(conn, "oh", os.path.join(LOCAL_DIR, "oh_summit_v5_parcels.csv.gz"))
        sub_res = {}
        if not args.skip_subsidies:
            sub_res = import_federal_subsidies(conn, os.path.join(LOCAL_DIR, "hud_federal_subsidies_v5.csv.gz"))
        log(f"=== IMPORT-V5 COMPLETE: OH Summit +{oh_n:,}, Subsidies={sub_res.get('catalog', 0):,} catalog / {sub_res.get('matched_total', 0):,} matched ===")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
