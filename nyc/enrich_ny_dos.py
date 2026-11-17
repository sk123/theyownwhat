#!/usr/bin/env python3
"""
nyc/enrich_ny_dos.py
====================
Enriches NYC corporate landlord untangling using the official New York State
Department of State (NY DOS) Active Corporations dataset on NY Open Data:
  https://data.ny.gov/resource/n9v6-gdp6.json

Source-Only Invariants:
  - Uses ONLY official NY DOS public filings (`chairman_name` CEO/principal,
    `dos_process_name`, `dos_process_address_1..zip`, `registered_agent_name`).
  - Never invents principals or infers undisclosed LLC members.
  - Excludes commercial registered agents / service-of-process mills, high-frequency
    institutional entities, and NYC HPD nonprofit intermediaries (`NONPROFIT_INTERMEDIARY_CORPS`)
    so MHANY (Speliotis) and Banana Kelly (Burgess) remain 100% isolated.
  - Idempotent: tracks NY DOS-sourced contact rows via `source_tag = 'NY_DOS_ACTIVE_CORPS'`
    so `nyc_hpd_contacts` is never duplicated across nightly runs.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values
import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()

from api.shared_utils import (
    compose_address,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    load_registrar_address_substrings,
    looks_like_person_owner,
    normalize_business_name,
    normalize_mailing_address_coarse,
    normalize_person_name,
)
from nyc.build_nyc_networks import (
    CORP_MAX_USERS,
    NONPROFIT_INTERMEDIARY_CORPS,
    NYC_INSTITUTIONAL_PATTERN,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("nyc-ny-dos")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")
NY_DOS_SOCRATA_URL = "https://data.ny.gov/resource/n9v6-gdp6.json"
NY_DOS_SOURCE_TAG = "NY_DOS_ACTIVE_CORPS"

REGISTERED_AGENT_NAME_FRAGMENTS = (
    "CT CORPORATION",
    "C T CORPORATION",
    "CORPORATION SERVICE",
    "CSC LAWYERS",
    "LEGALINC",
    "LEGALZOOM",
    "NORTHWEST REGISTERED",
    "REGISTERED AGENTS INC",
    "REGISTERED AGENT",
    "UNITED STATES CORPORATION AGENTS",
    "COGENCY GLOBAL",
    "NATIONAL REGISTERED AGENTS",
    "PARACORP",
    "INCORP SERVICES",
    "CAPITOL SERVICES",
    "FILING EXPRESS",
    "ALBANY CORPORATE",
    "SECRETARY OF STATE",
    "DEPARTMENT OF STATE",
    "SPIEGEL & UTRERA",
    "BLUMBERGEXCELSIOR",
    "VRA SERVICES",
    "CORPORATE CREATIONS",
)


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def clean_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    text = " ".join(str(val).strip().split())
    if not text or text.upper() in {"NONE", "NULL", "N/A", "NA", "UNKNOWN"}:
        return None
    return text


def is_commercial_agent_name(name: Optional[str]) -> bool:
    if not name:
        return True
    upper = name.upper()
    if upper.startswith(("C/O ", "CARE OF ", "ATTN ", "THE LLC", "THE CORPORATION")):
        return True
    return any(frag in upper for frag in REGISTERED_AGENT_NAME_FRAGMENTS)


def ensure_ny_dos_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nyc_dos_entities (
                dos_id                  TEXT PRIMARY KEY,
                current_entity_name     TEXT NOT NULL,
                corporation_name_norm   TEXT NOT NULL,
                entity_type             TEXT,
                county                  TEXT,
                initial_dos_filing_date DATE,
                chairman_name           TEXT,
                chairman_name_norm      TEXT,
                chairman_address        TEXT,
                chairman_city           TEXT,
                chairman_state          TEXT,
                chairman_zip            TEXT,
                dos_process_name        TEXT,
                dos_process_address     TEXT,
                dos_process_city        TEXT,
                dos_process_state       TEXT,
                dos_process_zip         TEXT,
                registered_agent_name   TEXT,
                updated_at              TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_nyc_dos_corp_norm
                ON nyc_dos_entities(corporation_name_norm);
            ALTER TABLE nyc_hpd_contacts
                ADD COLUMN IF NOT EXISTS source_tag TEXT;
            CREATE INDEX IF NOT EXISTS idx_nyc_contacts_source_tag
                ON nyc_hpd_contacts(source_tag)
                WHERE source_tag IS NOT NULL;
            """
        )
    conn.commit()


def update_status(conn, status: str, details: Dict[str, Any], external_last_updated: Optional[dt.date] = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
            VALUES ('NY_DOS_CORPORATIONS', 'nyc', %s, NOW(), %s, %s)
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                last_refreshed_at = NOW(),
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
            """,
            (external_last_updated, status, Json(details)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def _soql_escape(val: str) -> str:
    return "'" + val.replace("'", "''") + "'"


def _parse_iso_date(val: Optional[str]) -> Optional[dt.date]:
    if not val:
        return None
    try:
        return dt.datetime.fromisoformat(val.replace("Z", "+00:00")[:26]).date()
    except Exception:
        return None


def fetch_ny_dos_batch(
    session: requests.Session,
    raw_names: Sequence[str],
    app_token: Optional[str] = None,
) -> List[Dict[str, Any]]:
    candidates: Set[str] = set()
    for name in raw_names:
        up = " ".join(name.upper().strip().split())
        if not up:
            continue
        candidates.add(up)
        # NY DOS frequently punctuates 'LLC' or 'L.L.C.' or ', LLC'
        if up.endswith(" LLC") and not up.endswith(", LLC"):
            candidates.add(up[:-4] + ", LLC")
        elif up.endswith(", LLC"):
            candidates.add(up[:-5] + " LLC")

    if not candidates:
        return []

    in_list = ",".join(_soql_escape(n) for n in sorted(candidates))
    params = {
        "$where": f"upper(current_entity_name) in ({in_list})",
        "$limit": 5000,
    }
    headers = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token

    for attempt in range(1, 4):
        try:
            resp = session.get(NY_DOS_SOCRATA_URL, params=params, headers=headers, timeout=45)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as exc:
            if attempt >= 3:
                logger.warning("NY DOS batch fetch failed after 3 attempts: %s", exc)
                return []
            time.sleep(2 ** attempt)
    return []


def run_ny_dos_enrichment(limit_corps: Optional[int] = None, rebuild_networks: bool = False) -> Dict[str, Any]:
    conn = get_conn()
    try:
        ensure_ny_dos_schema(conn)
        registrar_substrings = load_registrar_address_substrings(conn)
        app_token = clean_str(os.environ.get("NYS_SOCRATA_APP_TOKEN"))

        # Select distinct CorporateOwner entities on NYC HPD registrations
        # prioritizing multi-building / active corporate owners while excluding
        # institutional and nonprofit intermediary entities.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    corporation_name_norm,
                    MODE() WITHIN GROUP (ORDER BY corporation_name) AS raw_corp_name,
                    ARRAY_AGG(DISTINCT registration_id) FILTER (WHERE registration_id IS NOT NULL) AS reg_ids,
                    COUNT(DISTINCT registration_id) AS reg_count
                FROM nyc_hpd_contacts
                WHERE UPPER(COALESCE(contact_type, '')) = 'CORPORATEOWNER'
                  AND corporation_name_norm IS NOT NULL
                  AND corporation_name IS NOT NULL
                  AND source_tag IS NULL
                GROUP BY corporation_name_norm
                HAVING COUNT(DISTINCT registration_id) < %s
                ORDER BY COUNT(DISTINCT registration_id) DESC, corporation_name_norm ASC
                """,
                (CORP_MAX_USERS,),
            )
            corp_rows = cur.fetchall()

        eligible_corps: List[Tuple[str, str, List[str]]] = []
        for corp_norm, raw_name, reg_ids, _ in corp_rows:
            if not corp_norm or not raw_name or not reg_ids:
                continue
            if (
                corp_norm in NONPROFIT_INTERMEDIARY_CORPS
                or NYC_INSTITUTIONAL_PATTERN.search(corp_norm)
                or is_excluded_third_party_entity(corp_norm)
            ):
                continue
            if NYC_INSTITUTIONAL_PATTERN.search(raw_name) or is_excluded_third_party_entity(raw_name):
                continue
            eligible_corps.append((corp_norm, raw_name, list(reg_ids)))
            if limit_corps and len(eligible_corps) >= limit_corps:
                break

        logger.info(
            "Querying official NY DOS Active Corporations for %d eligible NYC corporate owners...",
            len(eligible_corps),
        )

        corp_by_norm: Dict[str, Tuple[str, List[str]]] = {
            c_norm: (raw_name, reg_ids) for c_norm, raw_name, reg_ids in eligible_corps
        }

        dos_entities_to_upsert: Dict[str, tuple] = {}
        matched_norms: Dict[str, Dict[str, Any]] = {}
        latest_filing_date: Optional[dt.date] = None

        batch_size = 35
        session = requests.Session()
        try:
            for idx in range(0, len(eligible_corps), batch_size):
                chunk = eligible_corps[idx : idx + batch_size]
                records = fetch_ny_dos_batch(session, [r[1] for r in chunk], app_token=app_token)
                for rec in records:
                    dos_id = clean_str(rec.get("dos_id"))
                    entity_name = clean_str(rec.get("current_entity_name"))
                    if not dos_id or not entity_name:
                        continue
                    c_norm = normalize_business_name(entity_name)
                    if not c_norm:
                        continue

                    filing_dt = _parse_iso_date(rec.get("initial_dos_filing_date"))
                    if filing_dt and (latest_filing_date is None or filing_dt > latest_filing_date):
                        latest_filing_date = filing_dt

                    chair_raw = clean_str(rec.get("chairman_name"))
                    chair_norm = normalize_person_name(chair_raw) if chair_raw and looks_like_person_owner(chair_raw) and not is_commercial_agent_name(chair_raw) else None
                    chair_addr = " ".join(
                        p for p in [clean_str(rec.get("chairman_address_1")), clean_str(rec.get("chairman_address_2"))] if p
                    ) or None
                    chair_city = clean_str(rec.get("chairman_city"))
                    chair_state = clean_str(rec.get("chairman_state"))
                    chair_zip = clean_str(rec.get("chairman_zip"))

                    proc_name = clean_str(rec.get("dos_process_name"))
                    proc_addr = " ".join(
                        p for p in [clean_str(rec.get("dos_process_address_1")), clean_str(rec.get("dos_process_address_2"))] if p
                    ) or None
                    proc_city = clean_str(rec.get("dos_process_city"))
                    proc_state = clean_str(rec.get("dos_process_state"))
                    proc_zip = clean_str(rec.get("dos_process_zip"))
                    reg_agent = clean_str(rec.get("registered_agent_name"))

                    dos_entities_to_upsert[dos_id] = (
                        dos_id,
                        entity_name,
                        c_norm,
                        clean_str(rec.get("entity_type")),
                        clean_str(rec.get("county")),
                        filing_dt,
                        chair_raw,
                        chair_norm,
                        chair_addr,
                        chair_city,
                        chair_state,
                        chair_zip,
                        proc_name,
                        proc_addr,
                        proc_city,
                        proc_state,
                        proc_zip,
                        reg_agent,
                    )
                    if c_norm in corp_by_norm and c_norm not in matched_norms:
                        matched_norms[c_norm] = {
                            "entity_name": entity_name,
                            "chair_raw": chair_raw,
                            "chair_norm": chair_norm,
                            "chair_addr": chair_addr,
                            "chair_city": chair_city,
                            "chair_state": chair_state,
                            "chair_zip": chair_zip,
                            "proc_name": proc_name,
                            "proc_addr": proc_addr,
                            "proc_city": proc_city,
                            "proc_state": proc_state,
                            "proc_zip": proc_zip,
                            "reg_agent": reg_agent,
                        }
                if (idx // batch_size + 1) % 25 == 0:
                    logger.info(
                        "  Processed %d/%d NYC corporate names (%d NY DOS entities matched)...",
                        min(idx + batch_size, len(eligible_corps)),
                        len(eligible_corps),
                        len(matched_norms),
                    )
        finally:
            session.close()

        # Build verified NY DOS contact rows for nyc_hpd_contacts
        # Only insert:
        #   1) Verified human CEO/Chairman (`chairman_name`) as HEADOFFICER
        #   2) Verified non-registrar process/executive address on CORPORATEOWNER when not a commercial agent
        new_contact_rows = []
        principals_discovered = 0
        non_registrar_process_addrs = 0

        for c_norm, info in matched_norms.items():
            raw_corp_name, reg_ids = corp_by_norm[c_norm]
            chair_raw = info["chair_raw"]
            chair_norm = info["chair_norm"]

            if (
                chair_raw
                and chair_norm
                and not NYC_INSTITUTIONAL_PATTERN.search(chair_norm)
                and not is_excluded_third_party_entity(chair_norm)
            ):
                principals_discovered += 1
                chair_composed = compose_address(
                    info["chair_addr"], info["chair_city"], info["chair_state"], info["chair_zip"]
                )
                chair_coarse = normalize_mailing_address_coarse(chair_composed) if info["chair_addr"] else ""
                addr_ok = bool(
                    info["chair_addr"]
                    and info["chair_zip"]
                    and not is_excluded_third_party_address(chair_composed, chair_coarse, registrar_substrings)
                )
                parts = chair_norm.split()
                first_nm = parts[0] if parts else None
                last_nm = parts[-1] if len(parts) > 1 else None
                for reg_id in reg_ids:
                    new_contact_rows.append(
                        (
                            reg_id,
                            "HEADOFFICER",
                            raw_corp_name,
                            c_norm,
                            first_nm,
                            last_nm,
                            chair_raw,
                            chair_norm,
                            info["chair_addr"] if addr_ok else None,
                            info["chair_city"] if addr_ok else None,
                            info["chair_state"] if addr_ok else None,
                            info["chair_zip"][:5] if (addr_ok and info["chair_zip"]) else None,
                            NY_DOS_SOURCE_TAG,
                        )
                    )

            # Non-commercial process address (only if process recipient is not a registered agent mill)
            proc_name = info["proc_name"]
            proc_addr = info["proc_addr"]
            proc_zip = info["proc_zip"]
            proc_composed = compose_address(proc_addr, info["proc_city"], info["proc_state"], proc_zip)
            proc_coarse = normalize_mailing_address_coarse(proc_composed) if proc_addr else ""
            if (
                proc_addr
                and proc_zip
                and not info["reg_agent"]
                and not is_commercial_agent_name(proc_name)
                and not is_excluded_third_party_entity(proc_name)
                and not is_excluded_third_party_address(proc_composed, proc_coarse, registrar_substrings)
            ):
                non_registrar_process_addrs += 1
                # If process recipient is a directly named human (and different from chairman), surface as HEADOFFICER
                proc_human_norm = (
                    normalize_person_name(proc_name)
                    if proc_name and looks_like_person_owner(proc_name) and not is_commercial_agent_name(proc_name)
                    else None
                )
                for reg_id in reg_ids:
                    new_contact_rows.append(
                        (
                            reg_id,
                            "HEADOFFICER" if proc_human_norm else "CORPORATEOWNER",
                            raw_corp_name,
                            c_norm,
                            None,
                            None,
                            proc_name if proc_human_norm else None,
                            proc_human_norm,
                            proc_addr,
                            info["proc_city"],
                            info["proc_state"],
                            proc_zip[:5],
                            NY_DOS_SOURCE_TAG,
                        )
                    )

        with conn.cursor() as cur:
            if dos_entities_to_upsert:
                execute_values(
                    cur,
                    """
                    INSERT INTO nyc_dos_entities (
                        dos_id, current_entity_name, corporation_name_norm, entity_type, county,
                        initial_dos_filing_date, chairman_name, chairman_name_norm,
                        chairman_address, chairman_city, chairman_state, chairman_zip,
                        dos_process_name, dos_process_address, dos_process_city, dos_process_state, dos_process_zip,
                        registered_agent_name, updated_at
                    ) VALUES %s
                    ON CONFLICT (dos_id) DO UPDATE SET
                        current_entity_name = EXCLUDED.current_entity_name,
                        corporation_name_norm = EXCLUDED.corporation_name_norm,
                        entity_type = EXCLUDED.entity_type,
                        county = EXCLUDED.county,
                        chairman_name = EXCLUDED.chairman_name,
                        chairman_name_norm = EXCLUDED.chairman_name_norm,
                        chairman_address = EXCLUDED.chairman_address,
                        chairman_city = EXCLUDED.chairman_city,
                        chairman_state = EXCLUDED.chairman_state,
                        chairman_zip = EXCLUDED.chairman_zip,
                        dos_process_name = EXCLUDED.dos_process_name,
                        dos_process_address = EXCLUDED.dos_process_address,
                        dos_process_city = EXCLUDED.dos_process_city,
                        dos_process_state = EXCLUDED.dos_process_state,
                        dos_process_zip = EXCLUDED.dos_process_zip,
                        registered_agent_name = EXCLUDED.registered_agent_name,
                        updated_at = NOW()
                    """,
                    list(dos_entities_to_upsert.values()),
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                    page_size=1000,
                )

            # Atomically replace only NY_DOS_ACTIVE_CORPS rows for matched corporations (or full run)
            if limit_corps:
                matched_corp_list = list(matched_norms.keys())
                if matched_corp_list:
                    cur.execute(
                        "DELETE FROM nyc_hpd_contacts WHERE source_tag = %s AND corporation_name_norm = ANY(%s)",
                        (NY_DOS_SOURCE_TAG, matched_corp_list),
                    )
            else:
                cur.execute("DELETE FROM nyc_hpd_contacts WHERE source_tag = %s", (NY_DOS_SOURCE_TAG,))

            if new_contact_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO nyc_hpd_contacts (
                        registration_id, contact_type, corporation_name, corporation_name_norm,
                        first_name, last_name, full_name, full_name_norm,
                        business_address, business_city, business_state, business_zip, source_tag
                    ) VALUES %s
                    """,
                    new_contact_rows,
                    page_size=2000,
                )
        conn.commit()

        details = {
            "source_url": NY_DOS_SOCRATA_URL,
            "eligible_corporate_owners_queried": len(eligible_corps),
            "ny_dos_entities_stored": len(dos_entities_to_upsert),
            "matched_corporate_owners": len(matched_norms),
            "verified_human_chairmen_linked": principals_discovered,
            "verified_non_registrar_process_addresses": non_registrar_process_addrs,
            "contact_rows_synced": len(new_contact_rows),
        }
        update_status(conn, "success", details, latest_filing_date)
        logger.info("✓ NY DOS corporate untangling complete: %s", json.dumps(details))
    except Exception as exc:
        conn.rollback()
        logger.exception("NY DOS enrichment failed: %s", exc)
        update_status(conn, "failure", {"error": str(exc)}, None)
        raise
    finally:
        conn.close()

    if rebuild_networks:
        from nyc.build_nyc_networks import build_nyc_networks
        build_nyc_networks()

    return details


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich NYC corporate owners from official NY DOS Active Corporations.")
    parser.add_argument("--limit-corps", type=int, default=None, help="Optional cap on distinct corporate owners queried.")
    parser.add_argument("--rebuild-networks", action="store_true", help="Rebuild nyc_networks after syncing NY DOS contacts.")
    args = parser.parse_args()
    run_ny_dos_enrichment(limit_corps=args.limit_corps, rebuild_networks=args.rebuild_networks)
