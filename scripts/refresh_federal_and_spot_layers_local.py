#!/usr/bin/env python3
"""
scripts/refresh_federal_and_spot_layers_local.py
================================================
Lightweight, zero-GCP-cost, watermark-gated local refresher for every public
government dataset initially harvested via GCP Spot Expansion V4 & V5:

  1. `federal_subsidies` (Weekly — ~60s when updated, ~2s when unchanged):
       - HUD LIHTC (`50.5K`)
       - HUD Multifamily Assisted + REAC Physical Inspection Scores (`23.8K`)
       - HUD FHA-Insured Multifamily + REAC Physical Inspection Scores (`17.8K`)
       - USDA Rural Development Section 515 (`13.3K`)
       -> Updates `federal_housing_subsidies`, CT `property_subsidies`, and
          `nhpd_subsidy` / `nhpd_program` / `nhpd_expiration` across all live
          jurisdictions.

  2. `syracuse_ny` (Weekly — ~30s when updated, ~1s when unchanged):
       - City of Syracuse Code Violations V2 & Syracuse Rental Registry
       -> Updates `ny_bbl_stats` and `ny_properties.compliance_active`.

  3. `county_parcels` (Monthly — watermark-gated per county, skips unchanged
     rolls in <1s; streams changed rolls additively without truncating):
       - OH: Stark (Canton), Lake, Delaware, Summit (Akron)
       - MN: Washington, Scott, Carver (7-County Twin Cities Metro)
       - IL: Kane (Aurora/Elgin), McHenry (5-County Chicagoland)

  4. `geocode_backfill` (Monthly — runs ONLY if a jurisdiction has >= 500
     parcels missing WGS84 latitude/longitude):
       - DC, Baltimore, Montana, Minnesota (Dakota / Anoka / Olmsted).

All upstream metadata checks use official public ArcGIS REST `?f=pjson`
(`editingInfo.lastEditDate`) and `returnCountOnly=true` watermarks recorded in
`data_source_status.details`, requiring zero Google Cloud resources or credentials.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import json
import os
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import requests

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from gcp.import_spot_expansion_v3 import _resolve_db_url, import_additive_parcels
from gcp.import_spot_expansion_v4 import import_geocoding_backfill, import_ny_syracuse_overlays
from gcp.import_spot_expansion_v5 import import_federal_subsidies
from gcp import spot_expansion_v4_worker as v4w
from gcp import spot_expansion_v5_worker as v5w

HEADERS = {"User-Agent": "TheyOwnWhatLocalRefresher/1.0 (public-open-data-sync)"}

# Authoritative ArcGIS REST endpoints for upstream watermark probing
FEDERAL_SUBSIDY_PROBES: Dict[str, Tuple[str, str]] = {
    "HUD_LIHTC": (
        "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/LIHTC/FeatureServer/0",
        "1=1",
    ),
    "HUD_MF_ASSISTED": (
        "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/MULTIFAMILY_PROPERTIES_ASSISTED/FeatureServer/0",
        "1=1",
    ),
    "HUD_INSURED_MF": (
        "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/HUD_Insured_Multifamily_Properties/FeatureServer/0",
        "1=1",
    ),
    "USDA_RURAL_515": (
        "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/USDA_Rural_Housing_Assets/FeatureServer/0",
        "1=1",
    ),
}

SYRACUSE_PROBES: Dict[str, Tuple[str, str]] = {
    "SYRACUSE_CODE_VIOLATIONS": (
        "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Code_Violations_V2/FeatureServer/0",
        "1=1",
    ),
    "SYRACUSE_RENTAL_REGISTRY": (
        "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Syracuse_Rental_Registry/FeatureServer/0",
        "1=1",
    ),
}

# County-level Spot V4/V5 parcel layers grouped by target state jurisdiction
COUNTY_PARCEL_PROBES: Dict[str, Dict[str, Tuple[str, str]]] = {
    "oh_v4": {
        "OH_STARK": (
            "https://scgisa.starkcountyohio.gov/arcgis/rest/services/Auditor/StarkCountyParcels/FeatureServer/0",
            "APPRAISED_BUILDING_VALUE > 0",
        ),
        "OH_LAKE": (
            "https://gis.lakecountyohio.gov/arcgis/rest/services/Sharing/LCGIS_SHARE_2023/FeatureServer/0",
            "A_VAL_BLDG > 0",
        ),
        "OH_DELAWARE": (
            "https://services2.arcgis.com/ziXVKVy3BiopMCCU/arcgis/rest/services/Parcel/FeatureServer/0",
            "MARKET_IMP > 0",
        ),
    },
    "oh_summit": {
        "OH_SUMMIT": (
            "https://services3.arcgis.com/3Ukh5HzAdI6WZ3KP/arcgis/rest/services/TaxParcels_dashboard/FeatureServer/0",
            "CNTASSDVAL > LNDVALUE AND OWNERNME1 IS NOT NULL",
        ),
    },
    "mn_v4": {
        "MN_WASHINGTON": (
            "https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcels/FeatureServer/6",
            "EMV_BLDG > 0",
        ),
        "MN_SCOTT": (
            "https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcels/FeatureServer/5",
            "EMV_BLDG > 0",
        ),
        "MN_CARVER": (
            "https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcels/FeatureServer/1",
            "EMV_BLDG > 0",
        ),
    },
    "il_v4": {
        "IL_KANE": (
            "https://gistech.countyofkane.org/arcgis/rest/services/KanePINList/MapServer/0",
            "1=1",
        ),
        "IL_MCHENRY": (
            "https://services1.arcgis.com/6iYC5AXXYapRVNzl/arcgis/rest/services/McHenry_County_TaxParcels/FeatureServer/0",
            "1=1",
        ),
    },
}


def log(msg: str) -> None:
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] [LOCAL-REFRESH] {msg}", flush=True)


def epoch_ms_to_iso_date(val: Any) -> Optional[str]:
    if val is None or val == "":
        return None
    try:
        iv = int(val)
        if iv <= 0:
            return None
        d = dt.datetime.fromtimestamp(iv / 1000.0, tz=dt.timezone.utc).date()
        return d.isoformat() if 1990 <= d.year <= 2099 else None
    except Exception:
        return None


def probe_arcgis_watermark(base_url: str, where: str = "1=1", timeout: int = 20) -> Dict[str, Any]:
    """Fetch lightweight `(last_edit_ms, last_edit_date, count)` fingerprint from an ArcGIS REST layer."""
    out: Dict[str, Any] = {"ok": False, "last_edit_ms": None, "last_edit_date": None, "count": None, "error": None}
    try:
        r_meta = requests.get(f"{base_url}?f=pjson", headers=HEADERS, timeout=timeout)
        r_meta.raise_for_status()
        meta = r_meta.json()
        if "error" in meta:
            out["error"] = str(meta["error"])[:180]
            return out
        ei = meta.get("editingInfo") or {}
        ms = ei.get("dataLastEditDate") or ei.get("lastEditDate")
        out["last_edit_ms"] = int(ms) if ms else None
        out["last_edit_date"] = epoch_ms_to_iso_date(ms)

        r_cnt = requests.get(
            f"{base_url}/query",
            params={"where": where, "returnCountOnly": "true", "f": "json"},
            headers=HEADERS,
            timeout=timeout,
        )
        r_cnt.raise_for_status()
        cd = r_cnt.json()
        if "count" in cd:
            out["count"] = int(cd["count"])
        out["ok"] = True
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:150]}"
    return out


def probe_group_watermarks(probes: Dict[str, Tuple[str, str]]) -> Tuple[Dict[str, Dict[str, Any]], Optional[str]]:
    """Probe a dictionary of ArcGIS layers and return `(fingerprints, max_external_date)`."""
    fps: Dict[str, Dict[str, Any]] = {}
    dates: List[str] = []
    for key, (url, where) in probes.items():
        fp = probe_arcgis_watermark(url, where)
        fps[key] = fp
        if fp.get("last_edit_date"):
            dates.append(fp["last_edit_date"])
    return fps, (max(dates) if dates else None)


def watermarks_changed(
    previous: Optional[Dict[str, Any]],
    current: Dict[str, Dict[str, Any]],
) -> Tuple[bool, List[str]]:
    """Return `(changed, reasons)` comparing current ArcGIS watermarks against saved details.
    If all upstream probes failed, returns `(False, ['upstream_unreachable'])` to fail closed."""
    ok_keys = [k for k, v in current.items() if v.get("ok")]
    if not ok_keys:
        return False, ["all_upstream_probes_unreachable"]
    if not previous:
        return True, ["no_previous_watermark"]
    reasons: List[str] = []
    for k in ok_keys:
        cur = current[k]
        prev = previous.get(k) if isinstance(previous, dict) else None
        if not isinstance(prev, dict):
            reasons.append(f"{k}:new")
            continue
        if cur.get("last_edit_ms") and prev.get("last_edit_ms") and cur["last_edit_ms"] != prev["last_edit_ms"]:
            reasons.append(f"{k}:edit_date({prev.get('last_edit_date')}->{cur.get('last_edit_date')})")
        elif cur.get("count") is not None and prev.get("count") is not None and cur["count"] != prev["count"]:
            reasons.append(f"{k}:count({prev['count']}->{cur['count']})")
    return bool(reasons), reasons


def get_saved_watermarks(conn, source_name: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT details FROM data_source_status WHERE source_name = %s", (source_name,))
        row = cur.fetchone()
    if not row or not isinstance(row[0], dict):
        return None
    wm = row[0].get("layer_watermarks")
    return wm if isinstance(wm, dict) else None


def save_status_with_watermarks(
    conn,
    source_name: str,
    source_type: str,
    watermarks: Dict[str, Dict[str, Any]],
    external_date: Optional[str],
    extra: Optional[Dict[str, Any]] = None,
    status: str = "success",
) -> None:
    payload = dict(extra or {})
    payload["layer_watermarks"] = {
        k: {
            "last_edit_ms": v.get("last_edit_ms"),
            "last_edit_date": v.get("last_edit_date"),
            "count": v.get("count"),
        }
        for k, v in watermarks.items()
        if v.get("ok")
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status (
                source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details
            )
            VALUES (%s, %s, %s::date, NOW(), %s, %s::jsonb)
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                last_refreshed_at = NOW(),
                refresh_status = EXCLUDED.refresh_status,
                details = COALESCE(data_source_status.details, '{}'::jsonb) || EXCLUDED.details;
            """,
            (source_name, source_type, external_date, status, json.dumps(payload)),
        )
    conn.commit()


def refresh_federal_subsidies_local(conn, *, only_if_changed: bool, probe_only: bool, work_dir: str) -> bool:
    t0 = time.time()
    log("▶ Probing 4 nationwide federal HUD/USDA subsidy & REAC layers...")
    fps, max_ext_date = probe_group_watermarks(FEDERAL_SUBSIDY_PROBES)
    for k, fp in fps.items():
        log(f"    {k:18s}: ok={fp['ok']} count={fp['count']} edit_date={fp['last_edit_date']} err={fp['error']}")

    prev = get_saved_watermarks(conn, "FEDERAL_HUD_USDA_SUBSIDIES")
    changed, reasons = watermarks_changed(prev, fps)
    if probe_only:
        log(f"  [PROBE-ONLY] federal_subsidies changed={changed} reasons={reasons}")
        return False
    if only_if_changed and not changed:
        log(f"  ✓ Federal HUD/USDA subsidies & REAC scores unchanged ({reasons}); skipping download in {time.time() - t0:.1f}s.")
        return False

    log(f"  Upstream change detected ({reasons}); refreshing federal subsidies & REAC scores locally...")
    v5w.LOCAL_ONLY = True
    v5w.WORK_DIR = work_dir
    os.makedirs(work_dir, exist_ok=True)
    gz_path = v5w.harvest_federal_subsidies()
    res = import_federal_subsidies(conn, gz_path)
    save_status_with_watermarks(
        conn,
        "FEDERAL_HUD_USDA_SUBSIDIES",
        "federal_subsidies",
        fps,
        max_ext_date,
        extra={
            "catalog_rows": res.get("catalog", 0),
            "reac_scored_rows": res.get("reac", 0),
            "matched_parcels_total": res.get("matched_total", 0),
            "refresh_mode": "local_watermark_runner",
            "message": f"Refreshed {res.get('catalog', 0):,} HUD/USDA federal subsidy & REAC records locally ({res.get('matched_total', 0):,} matched parcels).",
        },
    )
    log(f"  ✓ Federal subsidies & REAC refresh complete in {time.time() - t0:.1f}s.")
    return True


def refresh_syracuse_ny_local(conn, *, only_if_changed: bool, probe_only: bool, work_dir: str) -> bool:
    t0 = time.time()
    log("▶ Probing City of Syracuse NY Code Violations V2 & Rental Registry...")
    fps, max_ext_date = probe_group_watermarks(SYRACUSE_PROBES)
    for k, fp in fps.items():
        log(f"    {k:26s}: ok={fp['ok']} count={fp['count']} edit_date={fp['last_edit_date']} err={fp['error']}")

    prev = get_saved_watermarks(conn, "NY_SYRACUSE_CODE_ENFORCEMENT")
    changed, reasons = watermarks_changed(prev, fps)
    if probe_only:
        log(f"  [PROBE-ONLY] syracuse_ny changed={changed} reasons={reasons}")
        return False
    if only_if_changed and not changed:
        log(f"  ✓ Syracuse NY code & rental feeds unchanged ({reasons}); skipping in {time.time() - t0:.1f}s.")
        return False

    log(f"  Upstream change detected ({reasons}); refreshing Syracuse NY overlays locally...")
    v4w.LOCAL_ONLY = True
    v4w.WORK_DIR = work_dir
    os.makedirs(work_dir, exist_ok=True)
    v4w.crunch_ny_syracuse_overlays()
    gz_path = os.path.join(work_dir, "ny_syracuse_overlays.csv.gz")
    res = import_ny_syracuse_overlays(conn, gz_path)
    save_status_with_watermarks(
        conn,
        "NY_SYRACUSE_CODE_ENFORCEMENT",
        "city_enforcement",
        fps,
        max_ext_date,
        extra={"refresh_mode": "local_watermark_runner", "syracuse_result": res},
    )
    log(f"  ✓ Syracuse NY refresh complete in {time.time() - t0:.1f}s.")
    return True


def refresh_county_parcels_local(
    conn,
    *,
    only_if_changed: bool,
    probe_only: bool,
    work_dir: str,
    rebuild_changed: bool,
) -> List[str]:
    changed_states: List[str] = []
    v4w.LOCAL_ONLY = True
    v4w.WORK_DIR = work_dir
    v5w.LOCAL_ONLY = True
    v5w.WORK_DIR = work_dir
    os.makedirs(work_dir, exist_ok=True)

    group_specs = [
        ("oh_v4", "oh", "COUNTY_ROLL_OH_V4", "oh_v4_parcels.csv.gz", lambda: v4w.crunch_oh_v4()),
        ("oh_summit", "oh", "COUNTY_ROLL_OH_SUMMIT", "oh_summit_v5_parcels.csv.gz", lambda: v5w.harvest_oh_summit()),
        ("mn_v4", "mn", "COUNTY_ROLL_MN_V4", "mn_v4_parcels.csv.gz", lambda: v4w.crunch_mn_v4()),
        ("il_v4", "il", "COUNTY_ROLL_IL_V4", "il_v4_parcels.csv.gz", lambda: v4w.crunch_il_v4()),
    ]

    for grp_key, jur, status_key, gz_name, runner in group_specs:
        t0 = time.time()
        log(f"▶ Probing county parcel roll `{grp_key}` ({jur.upper()})...")
        fps, max_ext_date = probe_group_watermarks(COUNTY_PARCEL_PROBES[grp_key])
        for k, fp in fps.items():
            log(f"    {k:16s}: ok={fp['ok']} count={fp['count']} edit_date={fp['last_edit_date']} err={fp['error']}")

        prev = get_saved_watermarks(conn, status_key)
        changed, reasons = watermarks_changed(prev, fps)
        if probe_only:
            log(f"  [PROBE-ONLY] {grp_key} changed={changed} reasons={reasons}")
            continue
        if only_if_changed and not changed:
            log(f"  ✓ `{grp_key}` unchanged ({reasons}); skipping in {time.time() - t0:.1f}s.")
            continue

        log(f"  Roll change detected for `{grp_key}` ({reasons}); streaming locally...")
        runner()
        gz_path = os.path.join(work_dir, gz_name)
        upserted = import_additive_parcels(conn, jur, gz_path)
        save_status_with_watermarks(
            conn,
            status_key,
            "county_parcel_roll",
            fps,
            max_ext_date,
            extra={"jurisdiction": jur, "upserted_parcels": upserted, "refresh_mode": "local_watermark_runner"},
        )
        if jur not in changed_states:
            changed_states.append(jur)
        log(f"  ✓ `{grp_key}` upserted {upserted:,} parcels in {time.time() - t0:.1f}s.")

    if rebuild_changed and changed_states and not probe_only:
        from states.ingest_statewide_parcels import build_and_save_state_networks
        tags = {
            "oh": ("OH", "OH_AUDITOR_CAMA"),
            "mn": ("MN", "MN_TWIN_CITIES_CAMA_AND_RENTAL_LICENSES"),
            "il": ("IL", "IL_CHICAGOLAND_PARCELS"),
        }
        for jur in changed_states:
            st_code, src_tag = tags[jur]
            log(f"▶ Rebuilding {st_code} networks after county roll update...")
            p_cnt, n_cnt = build_and_save_state_networks(conn, jur, st_code, src_tag)
            log(f"  ✓ [{st_code}] {p_cnt:,} parcels -> {n_cnt:,} networks.")
    return changed_states


def refresh_geocode_backfill_local(conn, *, min_missing: int, probe_only: bool, work_dir: str) -> Dict[str, int]:
    checks = [
        ("dc", "dc_properties", ""),
        ("baltimore", "baltimore_properties", ""),
        ("mt", "mt_properties", "AND jurisdiction = 'mt'"),
        ("mn", "mn_properties", "AND jurisdiction = 'mn'"),
    ]
    missing: Dict[str, int] = {}
    with conn.cursor() as cur:
        for jur, tbl, extra_w in checks:
            cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE latitude IS NULL {extra_w}")
            missing[jur] = int(cur.fetchone()[0])
    log(f"▶ Geocode missing counts: {missing} (threshold={min_missing})")
    needs_run = [j for j, c in missing.items() if c >= min_missing]
    if probe_only or not needs_run:
        log("  ✓ Geocode coverage above threshold (or probe-only); skipping backfill.")
        return {}

    v4w.LOCAL_ONLY = True
    v4w.WORK_DIR = work_dir
    os.makedirs(work_dir, exist_ok=True)
    log(f"  Backfilling WGS84 coordinates locally for {needs_run}...")
    v4w.crunch_geocoding_backfill_v4()
    return import_geocoding_backfill(conn, os.path.join(work_dir, "geocoding_backfill_v4.csv.gz"))


def seed_watermarks_from_current_upstream(conn, layers: List[str]) -> None:
    """Record current upstream watermarks in `data_source_status` without re-downloading
    data that was ALREADY freshly imported via Spot V4/V5 today."""
    if "federal_subsidies" in layers:
        fps, max_dt = probe_group_watermarks(FEDERAL_SUBSIDY_PROBES)
        save_status_with_watermarks(conn, "FEDERAL_HUD_USDA_SUBSIDIES", "federal_subsidies", fps, max_dt, extra={"watermark_seeded": True})
        log(f"  ✓ Seeded watermarks for FEDERAL_HUD_USDA_SUBSIDIES (ext_date={max_dt})")
    if "syracuse_ny" in layers:
        fps, max_dt = probe_group_watermarks(SYRACUSE_PROBES)
        save_status_with_watermarks(conn, "NY_SYRACUSE_CODE_ENFORCEMENT", "city_enforcement", fps, max_dt, extra={"watermark_seeded": True})
        log(f"  ✓ Seeded watermarks for NY_SYRACUSE_CODE_ENFORCEMENT (ext_date={max_dt})")
    if "county_parcels" in layers:
        for grp_key, status_key in [
            ("oh_v4", "COUNTY_ROLL_OH_V4"),
            ("oh_summit", "COUNTY_ROLL_OH_SUMMIT"),
            ("mn_v4", "COUNTY_ROLL_MN_V4"),
            ("il_v4", "COUNTY_ROLL_IL_V4"),
        ]:
            fps, max_dt = probe_group_watermarks(COUNTY_PARCEL_PROBES[grp_key])
            save_status_with_watermarks(conn, status_key, "county_parcel_roll", fps, max_dt, extra={"watermark_seeded": True})
            log(f"  ✓ Seeded watermarks for {status_key} (ext_date={max_dt})")


def main() -> int:
    parser = argparse.ArgumentParser(description="Zero-GCP watermark-gated local refresher for HUD/REAC & Spot V4/V5 layers.")
    parser.add_argument(
        "--layers",
        nargs="+",
        default=["federal_subsidies", "syracuse_ny"],
        choices=["federal_subsidies", "syracuse_ny", "county_parcels", "geocode_backfill"],
    )
    parser.add_argument("--only-if-changed", action="store_true", help="Skip downloading any layer whose upstream ArcGIS edit date & count are unchanged.")
    parser.add_argument("--probe-only", action="store_true", help="Only probe upstream watermarks and report whether each layer changed; zero downloads or DB writes.")
    parser.add_argument("--seed-watermarks", action="store_true", help="Record current upstream watermarks in data_source_status for already-imported Spot V4/V5 data.")
    parser.add_argument("--rebuild-changed", action="store_true", help="Rebuild state networks for any county parcel roll that updated.")
    parser.add_argument("--min-missing-geo", type=int, default=500, help="Minimum un-geocoded parcels required to trigger geocode_backfill.")
    args = parser.parse_args()

    conn = psycopg2.connect(_resolve_db_url())
    try:
        if args.seed_watermarks:
            seed_watermarks_from_current_upstream(conn, args.layers)
            return 0

        with tempfile.TemporaryDirectory(prefix="local_spot_refresh_") as work_dir:
            if "federal_subsidies" in args.layers:
                refresh_federal_subsidies_local(
                    conn, only_if_changed=args.only_if_changed, probe_only=args.probe_only, work_dir=work_dir
                )
            if "syracuse_ny" in args.layers:
                refresh_syracuse_ny_local(
                    conn, only_if_changed=args.only_if_changed, probe_only=args.probe_only, work_dir=work_dir
                )
            if "county_parcels" in args.layers:
                refresh_county_parcels_local(
                    conn,
                    only_if_changed=args.only_if_changed,
                    probe_only=args.probe_only,
                    work_dir=work_dir,
                    rebuild_changed=args.rebuild_changed,
                )
            if "geocode_backfill" in args.layers:
                refresh_geocode_backfill_local(
                    conn, min_missing=args.min_missing_geo, probe_only=args.probe_only, work_dir=work_dir
                )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
