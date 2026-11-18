#!/usr/bin/env python3
"""
scripts/rebuild_all_with_opencorporates.py
==========================================
Snapshots before/after network-untangling metrics and rebuilds live ownership
networks across `theyownwhat` using the newly populated OpenCorporates (`oc_*`)
corporate registry graph:
  1. Municipal city tables (`dc`, `boston`, `minneapolis`, `milwaukee`,
     `baltimore`, `detroit`, `miami`, `philadelphia`, `chicago`)
  2. New Jersey BHI (`nj`)
  3. New York City HPD + OpenCorporates (`nyc`)
  4. Connecticut shadow-validated refresh (`ct`)
  5. Statewide `us_*` partitions (`vt`, `co`, `wi`, `ma`, `ny`, `fl`, `nc`)
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import os
import sys
import time
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


ALL_CITIES = [
    "dc",
    "boston",
    "minneapolis",
    "baltimore",
    "detroit",
    "miami",
    "philadelphia",
    "chicago",
]

ALL_STATES = [
    ("vt", "VT", "VCGI_GRAND_LIST"),
    ("co", "CO", "CO_OIT_PARCELS"),
    ("wi", "WI", "WI_SCO_V11_PARCELS"),
    ("ma", "MA", "MASSGIS_L3_ASSESSOR"),
    ("ny", "NY", "NYS_TAX_PARCELS"),
    ("fl", "FL", "FLORIDAGIO_PARCELS"),
    ("nc", "NC", "NC_ONEMAP_PARCELS"),
    ("ar", "AR", "ARKANSAS_GIS_PARCELS"),
    ("va", "VA", "VA_STATEWIDE_PARCELS"),
]


def snapshot_metrics(db_url: str) -> Dict[str, Dict[str, int]]:
    conn = psycopg2.connect(db_url)
    out: Dict[str, Dict[str, int]] = {}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for jur in [*ALL_CITIES, "nj", "nyc", *(s[0] for s in ALL_STATES)]:
                cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"public.{jur}_networks",))
                if not cur.fetchone()["ok"]:
                    continue
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    """,
                    (f"{jur}_networks",),
                )
                cols = {r["column_name"] for r in cur.fetchall()}
                if "building_count" not in cols:
                    continue
                human_expr = (
                    "COUNT(*) FILTER (WHERE (connection_signals::jsonb)->>'principal_status' = 'source_listed_person')::int"
                    if "connection_signals" in cols
                    else ("COUNT(*) FILTER (WHERE anchor_type = 'person')::int" if "anchor_type" in cols else "0::int")
                )
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*)::int AS networks,
                        COUNT(*) FILTER (WHERE COALESCE(building_count, 0) >= 2)::int AS multi_bldg_networks,
                        {human_expr} AS human_attributed_networks
                    FROM {jur}_networks
                    """
                )
                row = dict(cur.fetchone())
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s AND column_name = 'oc_company_number'
                    ) AS has_oc
                    """,
                    (f"{jur}_properties",),
                )
                if cur.fetchone()["has_oc"]:
                    cur.execute(
                        f"SELECT COUNT(*) FILTER (WHERE oc_company_number IS NOT NULL)::int AS oc_matched FROM {jur}_properties"
                    )
                    row["oc_matched_parcels"] = cur.fetchone()["oc_matched"]
                else:
                    row["oc_matched_parcels"] = 0
                out[jur] = row

            # CT metrics
            cur.execute("SELECT COUNT(*)::int AS networks, COUNT(*) FILTER (WHERE principal_count > 0)::int AS with_principals FROM networks")
            ct_row = dict(cur.fetchone())
            out["ct"] = ct_row
    finally:
        conn.close()
    return out


def _rebuild_city_worker(args_tuple: Tuple[str, str]) -> Tuple[str, float]:
    city, db_url = args_tuple
    os.environ["DATABASE_URL"] = db_url
    t0 = time.time()
    conn = psycopg2.connect(db_url)
    try:
        from api.populate_cities import rebuild_city_networks_from_db
        rebuild_city_networks_from_db(conn, city)
    finally:
        conn.close()
    return city, time.time() - t0


def _rebuild_state_worker(args_tuple: Tuple[str, str, str, str]) -> Tuple[str, float]:
    jur, state_code, source_tag, db_url = args_tuple
    os.environ["DATABASE_URL"] = db_url
    t0 = time.time()
    conn = psycopg2.connect(db_url)
    try:
        if jur == "ma":
            from ma.ingest_massgis import build_and_save_ma_networks
            build_and_save_ma_networks(conn)
        else:
            from states.ingest_statewide_parcels import build_and_save_state_networks
            build_and_save_state_networks(conn, jur, state_code, source_tag)
    finally:
        conn.close()
    return jur, time.time() - t0


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild all `theyownwhat` networks with OpenCorporates piercing.")
    parser.add_argument("--skip-states", action="store_true", help="Skip large statewide `us_*` rebuilds")
    parser.add_argument("--skip-ct", action="store_true", help="Skip CT shadow network refresh")
    parser.add_argument("--skip-nyc", action="store_true", help="Skip NYC OpenCorporates enrichment + rebuild")
    parser.add_argument("--only", nargs="*", help="Optional explicit subset of slugs to rebuild")
    args = parser.parse_args()

    db_url = _resolve_db_url()
    os.environ["DATABASE_URL"] = db_url
    only_set = {x.lower() for x in args.only} if args.only else None

    print("Taking pre-OpenCorporates network metrics snapshot...", flush=True)
    before = snapshot_metrics(db_url)

    t_start = time.time()

    # 1. Municipal cities (`max_workers=3`)
    cities_to_run = [c for c in ALL_CITIES if (only_set is None or c in only_set)]
    if cities_to_run:
        print(f"\n=== Phase 1: Rebuilding {len(cities_to_run)} municipal city networks (workers=3) ===", flush=True)
        with ProcessPoolExecutor(max_workers=min(3, len(cities_to_run))) as pool:
            futs = {pool.submit(_rebuild_city_worker, (c, db_url)): c for c in cities_to_run}
            for fut in as_completed(futs):
                c_done, el = fut.result()
                print(f"  ✓ [{c_done}] rebuilt in {el:.1f}s", flush=True)

    # 2. New Jersey (`nj`)
    if only_set is None or "nj" in only_set:
        print("\n=== Phase 2: Rebuilding New Jersey (`nj`) BHI + OpenCorporates networks ===", flush=True)
        t_nj = time.time()
        from nj.build_nj_networks import build_nj_networks
        build_nj_networks()
        print(f"  ✓ [nj] rebuilt in {time.time() - t_nj:.1f}s", flush=True)

    # 3. NYC (`nyc`)
    if not args.skip_nyc and (only_set is None or "nyc" in only_set):
        print("\n=== Phase 3: Enriching & Rebuilding NYC (`nyc`) HPD + OpenCorporates networks ===", flush=True)
        t_nyc = time.time()
        from nyc.enrich_opencorporates import run_nyc_opencorporates_enrichment
        run_nyc_opencorporates_enrichment(rebuild_networks=True)
        print(f"  ✓ [nyc] enriched & rebuilt in {time.time() - t_nyc:.1f}s", flush=True)

    # 4. Connecticut (`ct`) shadow-validated refresh
    if not args.skip_ct and (only_set is None or "ct" in only_set):
        print("\n=== Phase 4: Running Connecticut (`ct`) Shadow-Validated Safe Refresh ===", flush=True)
        t_ct = time.time()
        sys.path.insert(0, os.path.join(REPO_ROOT, "api"))
        from api.safe_network_refresh import run_refresh
        ok = run_refresh(dry_run=False, skip_linking=True, skip_emails=False)
        if not ok:
            raise RuntimeError("Connecticut safe_network_refresh shadow validation failed or lock busy!")
        print(f"  ✓ [ct] shadow-validated & swapped in {time.time() - t_ct:.1f}s", flush=True)

    # 5. Statewide `us_*` partitions (`max_workers=1` for strict RAM safety on 2M-3.9M parcel states)
    if not args.skip_states:
        states_to_run = [s for s in ALL_STATES if (only_set is None or s[0] in only_set)]
        if states_to_run:
            print(f"\n=== Phase 5: Rebuilding {len(states_to_run)} statewide `us_*` partitions (workers=1) ===", flush=True)
            with ProcessPoolExecutor(max_workers=1) as pool:
                futs = {pool.submit(_rebuild_state_worker, (*st_cfg, db_url)): st_cfg[0] for st_cfg in states_to_run}
                for fut in as_completed(futs):
                    s_done, el = fut.result()
                    print(f"  ✓ [{s_done}] statewide partition rebuilt in {el:.1f}s", flush=True)

    # 6. Refresh kv_cache top-network caches & landing_snapshot:v1 atomically
    print("\n=== Phase 6: Refreshing cached insights & landing snapshot ===", flush=True)
    try:
        conn_cache = psycopg2.connect(db_url)
        try:
            with conn_cache.cursor() as cur:
                cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
                for jur_slug in [*ALL_CITIES, "nj", "nyc", *(s[0] for s in ALL_STATES)]:
                    if only_set is None or jur_slug in only_set:
                        cur.execute(
                            "DELETE FROM kv_cache WHERE key = %s OR key LIKE %s",
                            (f"city_stats:v1:{jur_slug}", f"city_top_nets:v1:{jur_slug}:%"),
                        )
            conn_cache.commit()
            from api.landing_cache import build_and_store_landing_snapshot
            build_and_store_landing_snapshot(conn=conn_cache)
            print("  ✓ Refreshed kv_cache city stats/top-networks & landing_snapshot:v1", flush=True)
        finally:
            conn_cache.close()
    except Exception as cache_exc:
        print(f"  ! Warning: landing snapshot refresh encountered an error: {cache_exc}", flush=True)

    after = snapshot_metrics(db_url)
    report = {
        "elapsed_sec": round(time.time() - t_start, 1),
        "before": before,
        "after": after,
    }
    out_path = os.path.join(REPO_ROOT, "data", "opencorporates", "wave1_rebuild_metrics.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)

    print(f"\n=== ALL NETWORK REBUILDS COMPLETE IN {report['elapsed_sec']}s (saved {out_path}) ===", flush=True)
    for jur in sorted(after.keys()):
        b = before.get(jur, {})
        a = after[jur]
        if jur == "ct":
            print(
                f"  {jur:13s}: networks {b.get('networks', 0):>9,d} -> {a.get('networks', 0):>9,d} | "
                f"with_principals {b.get('with_principals', 0):>8,d} -> {a.get('with_principals', 0):>8,d}",
                flush=True,
            )
        else:
            print(
                f"  {jur:13s}: oc_parcels {b.get('oc_matched_parcels', 0):>8,d} -> {a.get('oc_matched_parcels', 0):>8,d} | "
                f"human_nets {b.get('human_attributed_networks', 0):>9,d} -> {a.get('human_attributed_networks', 0):>9,d} | "
                f"multi_nets {b.get('multi_bldg_networks', 0):>7,d} -> {a.get('multi_bldg_networks', 0):>7,d}",
                flush=True,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
