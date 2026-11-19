#!/usr/bin/env python3
"""
scripts/rebuild_hardened_networks.py
====================================
Sequential, zero-OOM, DB-only ownership-network re-clustering pass across existing
statewide and municipal jurisdictions using the hardened 3rd-party registrar,
foreign-branch service-of-process, NNN lessee tax-hub, institutional stale-parcel,
natural-person homonym, and condo-building exclusion guards.
"""

from __future__ import annotations

import argparse
import gc
import os
import sys
import time
import psycopg2

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.oc_linker import refresh_empirical_registered_agent_addresses
from api.populate_cities import rebuild_city_networks_from_db
from ma.ingest_massgis import build_and_save_ma_networks
from states.ingest_statewide_parcels import build_and_save_state_networks


STATE_SOURCE_TAGS = {
    "fl": ("FL", "FL_UNIFIED_67_COUNTY_PARCELS"),
    "ny": ("NY", "NY_UNIFIED_62_COUNTY_HPD_ORPTS"),
    "il": ("IL", "IL_COOK_COLLAR_CAMA"),
    "ca": ("CA", "CA_ASSESSOR_ROLL_AND_BIZ_TAX"),
    "mn": ("MN", "MN_TWIN_CITIES_CAMA_AND_RENTAL_LICENSES"),
    "oh": ("OH", "OH_CUYAHOGA_AUDITOR_CAMA"),
    "mt": ("MT", "MT_MSDI_ORION_CADASTRAL"),
    "va": ("VA", "VA_STATEWIDE_PARCELS"),
    "nc": ("NC", "NC_ONEMAP_STATEWIDE_PARCELS"),
    "co": ("CO", "CO_OIT_PUBLIC_PARCELS"),
    "wi": ("WI", "WI_SCO_DOA_STATEWIDE_PARCELS"),
    "ar": ("AR", "ARKANSAS_GIS_PARCELS"),
    "vt": ("VT", "VT_VCGI_STANDARDIZED_PARCELS"),
}

DEFAULT_STATES = [
    "ma", "il", "ca", "mn", "oh", "mt", "fl", "ny", "va", "nc", "co", "wi", "ar", "vt"
]
DEFAULT_CITIES = ["dc", "baltimore", "detroit", "philadelphia"]


def _db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def main() -> int:
    parser = argparse.ArgumentParser(description="Sequential DB-only false-positive network hardening rebuild")
    parser.add_argument("--states", nargs="*", default=DEFAULT_STATES)
    parser.add_argument("--cities", nargs="*", default=DEFAULT_CITIES)
    parser.add_argument("--skip-ra-refresh", action="store_true")
    args = parser.parse_args()

    dsn = _db_url()
    if not args.skip_ra_refresh:
        t0 = time.time()
        conn = psycopg2.connect(dsn)
        try:
            upserted = refresh_empirical_registered_agent_addresses(conn)
            print(f"[HARDEN] Refreshed empirical RA & shared-office hubs ({upserted:,} rows) in {time.time() - t0:.1f}s.", flush=True)
        finally:
            conn.close()

    for st in args.states:
        st_l = st.lower().strip()
        t0 = time.time()
        conn = psycopg2.connect(dsn)
        try:
            if st_l == "ma":
                p_cnt, n_cnt = build_and_save_ma_networks(conn)
            else:
                st_code, src_tag = STATE_SOURCE_TAGS[st_l]
                p_cnt, n_cnt = build_and_save_state_networks(conn, st_l, st_code, src_tag)
            print(
                f"[HARDEN] ✓ [{st_l.upper()}] {p_cnt:,} parcels -> {n_cnt:,} clean networks ({time.time() - t0:.1f}s)",
                flush=True,
            )
        finally:
            conn.close()
        gc.collect()

    for city in args.cities:
        c_l = city.lower().strip()
        t0 = time.time()
        conn = psycopg2.connect(dsn)
        try:
            rebuild_city_networks_from_db(conn, c_l)
            print(f"[HARDEN] ✓ [{c_l.upper()}] city networks rebuilt ({time.time() - t0:.1f}s)", flush=True)
        finally:
            conn.close()
        gc.collect()

    try:
        os.environ.setdefault("DATABASE_URL", dsn)
        from api.landing_cache import build_and_store_landing_snapshot
        t0 = time.time()
        build_and_store_landing_snapshot()
        print(f"[HARDEN] ✓ Refreshed landing_snapshot:v1 & per-jurisdiction top-networks cache ({time.time() - t0:.1f}s)", flush=True)
    except Exception as snap_exc:
        print(f"[HARDEN] ⚠ Landing snapshot refresh skipped: {snap_exc}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
