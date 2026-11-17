#!/usr/bin/env python3
"""
updater/enrich_baltimore_registrations.py
=========================================
Ingests official Baltimore City DHCD Non-Owner-Occupied Dwelling Registrations
and Multi-Family Rental Licenses from `baltegis.baltimorecity.gov`:
  https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/OpenGov_RegAndLicense/MapServer/0/query

Joins `BlockLot` directly to `baltimore_properties.bbl`, populates
`baltimore_hpd_registrations`, updates `baltimore_properties` compliance fields
(`compliance_active`, `compliance_record_id`, `compliance_expiration`), and backfills
missing `units_res` when official licensed dwelling/rooming/efficiency counts are present.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
from typing import Any, Dict, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from updater.enrich_city_enforcement import (
    clean_text,
    iter_arcgis_features,
    load_city_bbl_map,
    normalize_apn,
    normalize_compact,
    parse_source_date,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("enrich-baltimore-reg")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
BALTIMORE_REG_URL = (
    "https://baltegis.baltimorecity.gov/mapping/rest/services/Housing/OpenGov_RegAndLicense/MapServer/0/query"
)


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def update_status(conn, status: str, details: Dict[str, Any], external_last_updated: Optional[dt.date] = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
            VALUES ('BALTIMORE_RENTAL_REGISTRATIONS', 'city_enrichment', %s, NOW(), %s, %s)
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                last_refreshed_at = NOW(),
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
            """,
            (external_last_updated, status, Json(details)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix' OR key LIKE '%baltimore%'")
    conn.commit()


def enrich_baltimore_registrations(limit: Optional[int] = None) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        bbl_map = load_city_bbl_map(conn, "baltimore")
        registrations_by_id: Dict[str, tuple] = {}
        parcel_updates: Dict[str, Tuple[str, Optional[dt.date], int]] = {}
        latest_issued: Optional[dt.date] = None

        total_records = 0
        matched_records = 0
        today = dt.date.today()

        out_fields = (
            "OBJECTID,BlockLot,Address,DwellingCount,RoomingCount,EfficiencyCounts,"
            "LicenceOrRegistration,LicenseIssuedDate,LicenseExpirationDate,RecordNUMBER"
        )
        logger.info("Fetching Baltimore DHCD Rental Registrations & Licenses from %s...", BALTIMORE_REG_URL)
        for attrs in iter_arcgis_features(BALTIMORE_REG_URL, page_size=2000, limit=limit, out_fields=out_fields):
            total_records += 1
            raw_bl = attrs.get("BlockLot")
            bbl = bbl_map.get(normalize_compact(raw_bl)) or bbl_map.get(normalize_apn(raw_bl))
            if not bbl:
                continue

            matched_records += 1
            rec_num = clean_text(attrs.get("RecordNUMBER"))
            oid = attrs.get("OBJECTID")
            reg_id = rec_num or f"BALT-REG-{oid}"

            issued_dt = parse_source_date(attrs.get("LicenseIssuedDate"))
            exp_dt = parse_source_date(attrs.get("LicenseExpirationDate"))
            if issued_dt and (latest_issued is None or issued_dt > latest_issued):
                latest_issued = issued_dt

            lic_type = clean_text(attrs.get("LicenceOrRegistration")) or "Registered"
            is_active = bool(exp_dt is None or exp_dt >= today)

            registrations_by_id[reg_id] = (
                reg_id,
                bbl,
                None,
                clean_text(attrs.get("Address")) or None,
                "BALTIMORE",
                None,
                "BALTIMORE",
                f"{lic_type} ({'Active' if is_active else 'Expired'})",
                issued_dt,
                exp_dt,
            )

            units = sum(
                max(int(float(attrs.get(k) or 0)), 0)
                for k in ("DwellingCount", "RoomingCount", "EfficiencyCounts")
            )
            prev = parcel_updates.get(bbl)
            if prev is None or (exp_dt and (prev[1] is None or exp_dt > prev[1])):
                parcel_updates[bbl] = (reg_id, exp_dt, max(units, prev[2] if prev else 0))

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE baltimore_hpd_registrations CASCADE;")
            if registrations_by_id:
                execute_values(
                    cur,
                    """
                    INSERT INTO baltimore_hpd_registrations (
                        registration_id, bbl, bin, building_address, building_city,
                        building_zip, borough, lifecycle_stage, last_registration_date, registration_end_date
                    ) VALUES %s
                    ON CONFLICT (registration_id) DO NOTHING
                    """,
                    list(registrations_by_id.values()),
                    page_size=2000,
                )

            cur.execute(
                "UPDATE baltimore_properties SET compliance_active = FALSE, compliance_record_id = NULL, compliance_expiration = NULL WHERE compliance_active IS TRUE"
            )
            if parcel_updates:
                cur.execute(
                    "CREATE TEMP TABLE tmp_balt_reg (bbl TEXT PRIMARY KEY, rec_id TEXT, exp DATE, units INT, active BOOLEAN) ON COMMIT DROP"
                )
                execute_values(
                    cur,
                    "INSERT INTO tmp_balt_reg (bbl, rec_id, exp, units, active) VALUES %s",
                    [
                        (bbl, rec_id, exp_dt, units, bool(exp_dt is None or exp_dt >= today))
                        for bbl, (rec_id, exp_dt, units) in parcel_updates.items()
                    ],
                    page_size=2000,
                )
                cur.execute(
                    """
                    UPDATE baltimore_properties p
                    SET compliance_active = t.active,
                        compliance_record_id = t.rec_id,
                        compliance_expiration = t.exp,
                        units_res = CASE
                            WHEN (p.units_res IS NULL OR p.units_res = 0) AND t.units > 0 THEN t.units
                            ELSE p.units_res
                        END,
                        updated_at = NOW()
                    FROM tmp_balt_reg t
                    WHERE p.bbl = t.bbl
                    """
                )
        conn.commit()

        details = {
            "source_url": BALTIMORE_REG_URL,
            "source_records": total_records,
            "matched_records": matched_records,
            "unique_registrations_inserted": len(registrations_by_id),
            "matched_parcels": len(parcel_updates),
        }
        update_status(conn, "success", details, latest_issued)
        logger.info("✓ Baltimore rental registrations enrichment complete: %s", json.dumps(details, default=str))
        return details
    except Exception as exc:
        conn.rollback()
        logger.exception("Baltimore rental registrations enrichment failed: %s", exc)
        update_status(conn, "failure", {"error": str(exc)}, None)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest official Baltimore DHCD Rental Registrations & Licenses.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for smoke tests.")
    args = parser.parse_args()
    enrich_baltimore_registrations(limit=args.limit)
