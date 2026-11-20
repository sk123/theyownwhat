#!/usr/bin/env python3
"""
scripts/clean_ca_condo_fanout.py
================================
Epistemic-honesty hygiene script for California (`ca_properties`).

Under CA Gov. Code § 7928.205, LA County and SF Assessor public downloads omit
deed owner names; our Track A pipeline attaches registered lessors from LA
Office of Finance (`6rrh-rzua`) and DataSF Registered Business Locations
(`g8m3-pdis`). When a single business registration at a building's base street
address matched a multi-AIN condominium or timeshare complex via coarse street
matching, it fanned out onto 3-815 individual unit parcels (e.g., stamping
one condo-unit licensee onto all 300 units at 3500 W Manchester Blvd, or
stamping a parking garage onto 815 SF timeshare intervals at 710 Powell St).

This script:
  1. Removes SF fractional Timeshare intervals (`bld_class = 'TS'`).
  2. Resets falsely fanned-out multi-AIN condo unit clusters (>=3 condo-like
     rows sharing one `(base_street, borough, owner_name)`) back to the honest
     statutory marker `CA GOV CODE 7928.205 REDACTED (<BBL>)` and clears the
     fanned-out `mailing_address` / `oc_company_number` so only whole-building
     multifamily/commercial parcels and exact single-parcel matches anchor
     California landlord networks.
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg2

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def run_cleanup(dry_run: bool = False) -> None:
    conn = psycopg2.connect(_resolve_db_url())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ca_properties WHERE UPPER(COALESCE(bld_class, '')) = 'TS'")
            ts_cnt = cur.fetchone()[0]

            cur.execute(
                """
                WITH base AS (
                    SELECT bbl,
                           regexp_replace(UPPER(COALESCE(address, '')), '\\s+(UNIT|APT|STE|SUITE|#|NO\\.?|PH)\\s*.*$', '') AS base_addr,
                           borough,
                           owner_name,
                           (
                               land_use IN ('Single', 'Single Family Residential', 'Hotel & Motels', 'Commercial Misc')
                               OR UPPER(COALESCE(bld_class, '')) LIKE 'Z%%'
                               OR UPPER(COALESCE(bld_class, '')) IN ('TS', 'TH', 'LZ')
                               OR address ~* '\\s(UNIT|APT|PH|#)\\s*[A-Z0-9-]+$'
                           ) AS is_condo_unit
                    FROM ca_properties
                    WHERE owner_name IS NOT NULL
                      AND owner_name NOT LIKE 'CA GOV CODE%%'
                ),
                flagged_groups AS (
                    SELECT base_addr, borough, owner_name
                    FROM base
                    WHERE is_condo_unit
                    GROUP BY 1, 2, 3
                    HAVING COUNT(*) >= 3
                ),
                target_bbls AS (
                    SELECT b.bbl
                    FROM base b
                    JOIN flagged_groups g
                      ON g.base_addr = b.base_addr
                     AND g.borough = b.borough
                     AND g.owner_name = b.owner_name
                    WHERE b.is_condo_unit
                )
                SELECT COUNT(*) FROM target_bbls;
                """
            )
            fanout_cnt = cur.fetchone()[0]
            print(f"[CA-HYGIENE] Detected {ts_cnt:,} SF timeshare (`TS`) rows and {fanout_cnt:,} fanned-out multi-AIN condo rows.", flush=True)

            if dry_run:
                print("[CA-HYGIENE] --dry-run specified; no rows modified.")
                return

            if ts_cnt:
                cur.execute("DELETE FROM ca_properties WHERE UPPER(COALESCE(bld_class, '')) = 'TS'")
                print(f"  ✓ Deleted {cur.rowcount:,} fractional timeshare (`TS`) rows from `ca_properties`.", flush=True)

            if fanout_cnt:
                cur.execute(
                    """
                    WITH base AS (
                        SELECT bbl,
                               regexp_replace(UPPER(COALESCE(address, '')), '\\s+(UNIT|APT|STE|SUITE|#|NO\\.?|PH)\\s*.*$', '') AS base_addr,
                               borough,
                               owner_name,
                               (
                                   land_use IN ('Single', 'Single Family Residential', 'Hotel & Motels', 'Commercial Misc')
                                   OR UPPER(COALESCE(bld_class, '')) LIKE 'Z%%'
                                   OR UPPER(COALESCE(bld_class, '')) IN ('TS', 'TH', 'LZ')
                                   OR address ~* '\\s(UNIT|APT|PH|#)\\s*[A-Z0-9-]+$'
                               ) AS is_condo_unit
                        FROM ca_properties
                        WHERE owner_name IS NOT NULL
                          AND owner_name NOT LIKE 'CA GOV CODE%%'
                    ),
                    flagged_groups AS (
                        SELECT base_addr, borough, owner_name
                        FROM base
                        WHERE is_condo_unit
                        GROUP BY 1, 2, 3
                        HAVING COUNT(*) >= 3
                    ),
                    target_bbls AS (
                        SELECT b.bbl
                        FROM base b
                        JOIN flagged_groups g
                          ON g.base_addr = b.base_addr
                         AND g.borough = b.borough
                         AND g.owner_name = b.owner_name
                        WHERE b.is_condo_unit
                    )
                    UPDATE ca_properties p
                    SET owner_name = 'CA GOV CODE 7928.205 REDACTED (' || p.bbl || ')',
                        owner_name_norm = '',
                        owner_co_name = NULL,
                        mailing_address = '',
                        mailing_address_norm = '',
                        mailing_address_coarse = '',
                        mailing_state = NULL,
                        oc_jurisdiction_code = NULL,
                        oc_company_number = NULL,
                        oc_match_tier = NULL,
                        oc_match_confidence = NULL,
                        updated_at = NOW()
                    FROM target_bbls t
                    WHERE p.bbl = t.bbl;
                    """
                )
                print(f"  ✓ Redacted {cur.rowcount:,} falsely fanned-out condo-unit rows back to honest § 7928.205 status.", flush=True)

            conn.commit()
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean CA multi-AIN condo/timeshare business-tax fanout.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_cleanup(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
