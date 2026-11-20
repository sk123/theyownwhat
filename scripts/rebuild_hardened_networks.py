#!/usr/bin/env python3
"""
scripts/rebuild_hardened_networks.py
====================================
Rebuilds ownership networks across specified nationwide jurisdictions after
additive Spot V4/V5 parcel/subsidy/overlay imports, IRS 501(c) nonprofit
isolation updates, and universal false-bridge hardening in `api/shared_utils.py`,
then refreshes the daily landing snapshot (`landing_snapshot:v1`).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import psycopg2

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.populate_cities import rebuild_city_networks_from_db
from states.ingest_statewide_parcels import build_and_save_state_networks

STATE_META = {
    "oh": ("OH", "OH_AUDITOR_CAMA"),
    "mn": ("MN", "MN_TWIN_CITIES_CAMA_AND_RENTAL_LICENSES"),
    "il": ("IL", "IL_CHICAGOLAND_PARCELS"),
    "ny": ("NY", "NY_STATEWIDE_PARCELS"),
    "fl": ("FL", "FL_FDOR_NAL_PARCELS"),
    "nc": ("NC", "NC_ONEMAP_STATEWIDE_PARCELS"),
    "co": ("CO", "CO_STATEWIDE_PARCELS"),
    "ma": ("MA", "MA_MASSGIS_LEVEL3_PARCELS"),
    "va": ("VA", "VA_STATEWIDE_PARCELS"),
    "ar": ("AR", "AR_STATEWIDE_PARCELS"),
    "vt": ("VT", "VT_STATEWIDE_PARCELS"),
    "wi": ("WI", "WI_STATEWIDE_PARCELS"),
    "mt": ("MT", "MT_MSDI_ORION_CADASTRAL"),
    "nj": ("NJ", "NJ_DCA_BHI_REGISTRATIONS"),
}

CITIES = {"philadelphia", "chicago", "boston", "baltimore", "detroit", "miami", "minneapolis", "dc"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild hardened networks and refresh landing snapshot.")
    parser.add_argument(
        "--targets",
        nargs="+",
        default=["oh", "mn", "il", "ny", "fl", "nc", "co", "ma", "va", "ar", "philadelphia", "chicago", "boston", "baltimore", "detroit", "miami"],
    )
    parser.add_argument("--skip-landing-snapshot", action="store_true")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    t_all = time.time()
    conn = psycopg2.connect(db_url)
    try:
        for t in args.targets:
            t0 = time.time()
            tl = t.lower()
            if tl in STATE_META:
                st_code, src_tag = STATE_META[tl]
                print(f"▶ Rebuilding statewide networks for {tl.upper()}...", flush=True)
                p_cnt, n_cnt = build_and_save_state_networks(conn, tl, st_code, src_tag)
                print(f"  ✓ [{tl.upper()}] {p_cnt:,} parcels -> {n_cnt:,} networks in {time.time() - t0:.1f}s", flush=True)
            elif tl in CITIES:
                print(f"▶ Rebuilding city networks for {tl.upper()}...", flush=True)
                rebuild_city_networks_from_db(conn, tl)
                print(f"  ✓ [{tl.upper()}] rebuilt in {time.time() - t0:.1f}s", flush=True)
            else:
                print(f"⚠ Unknown target `{t}`, skipping.", flush=True)
    finally:
        conn.close()

    if not args.skip_landing_snapshot:
        print("▶ Refreshing daily landing_snapshot:v1...", flush=True)
        from api.landing_cache import refresh_landing_snapshot
        refresh_landing_snapshot()
        print(f"✓ All rebuilds + landing snapshot complete in {time.time() - t_all:.1f}s.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
