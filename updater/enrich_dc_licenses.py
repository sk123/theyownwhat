#!/usr/bin/env python3
"""
updater/enrich_dc_licenses.py
=============================
Ingests official Washington, D.C. Department of Licensing and Consumer Protection (DLCP)
Basic Business Licenses for Rental Housing (`Apartment`, `One Family Rental`, `Two Family Rental`)
from DC GIS Open Data:
  https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/0/query

Joins `SSL` (Square-Suffix-Lot) directly to `dc_properties.bbl`, populates:
  - `dc_hpd_registrations` (official license record, status, start/expiration dates)
  - `dc_hpd_contacts` (licensed human owners `INDIVIDUALOWNER`, entity owners `CORPORATEOWNER`,
    and registered agents `AGENT` kept strictly separate from ownership clustering)
  - `dc_properties` compliance fields (`compliance_active`, `compliance_record_id`, `compliance_expiration`)
  - Rebuilds `dc_networks` in-place (`rebuild_city_networks_from_db`) so anonymous D.C. LLCs
    sharing verified licensed human owners or non-registrar billing addresses are untangled!
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
from typing import Any, Dict, Iterator, Optional

import psycopg2
from psycopg2.extras import Json, execute_values
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.populate_cities import normalize_owner_name, rebuild_city_networks_from_db
from api.shared_utils import (
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    load_registrar_address_substrings,
    looks_like_person_owner,
    normalize_mailing_address_coarse,
    normalize_person_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("enrich-dc-licenses")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
DC_BBL_RENTAL_URL = "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/0/query"
RENTAL_CATEGORIES = ("Apartment", "One Family Rental", "Two Family Rental")


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = " ".join(str(value).strip().split())
    if not text or text.upper() in {"NAN", "NULL", "NONE", "N/A", "NA", "UNKNOWN"}:
        return None
    return text


def normalize_ssl(value: Any) -> str:
    return re.sub(r"\s+", " ", (clean_text(value) or "").upper()).strip()


def normalize_ssl_compact(value: Any) -> str:
    return re.sub(r"\s+", "", (clean_text(value) or "").upper())


def parse_dc_date(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        try:
            return dt.datetime.fromtimestamp(value / 1000, tz=dt.timezone.utc).date()
        except Exception:
            return None
    text = clean_text(value)
    if not text:
        return None
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(text[:19], fmt).date()
        except ValueError:
            pass
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")[:26]).date()
    except Exception:
        return None


def parse_city_state_zip(raw_addr: Optional[str]):
    """Extract trailing city, state, zip from a single-line billing address if present."""
    if not raw_addr:
        return None, None, None, None
    cleaned = re.sub(r"[,\s]+(?:USA|US|UNITED STATES)\s*$", "", raw_addr.upper()).strip()
    m = re.search(r"^(.*?)[,\s]+([A-Z\s]{2,25})[,\s]+([A-Z]{2})[,\s]+(\d{5})(?:-\d{4})?\s*$", cleaned)
    if m:
        return m.group(1).strip(" ,"), m.group(2).strip(" ,"), m.group(3), m.group(4)
    return raw_addr, None, None, None


def iter_dc_rental_licenses(limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    cats = ",".join(f"'{c}'" for c in RENTAL_CATEGORIES)
    base_where = f"BUSINESSACTIVITY IN ({cats}) AND SSL IS NOT NULL"
    out_fields = (
        "OBJECTID,SSL,MAR_ID,PREMISEADDRESS,WARD,"
        "CUSTOMERNUMBER,LICENSESTATUS,CATEGORYSERVICETYPE,BUSINESSACTIVITY,LICENSESTARTDATE,LICENSEENDDATE,"
        "ENTITYNAME,ENTITYTRADENAME,BUSINESSOWNERFIRSTNAME,BUSINESSOWNERLASTNAME,BILLINGADDRESS,"
        "AGENTFIRSTNAME,AGENTLASTNAME,AGENTENTITY"
    )
    last_oid = -1
    page_size = 2000
    yielded = 0
    session = requests.Session()
    try:
        while True:
            current_limit = min(page_size, limit - yielded) if limit else page_size
            if current_limit <= 0:
                break
            params = {
                "where": f"{base_where} AND OBJECTID > {last_oid}",
                "outFields": out_fields,
                "orderByFields": "OBJECTID ASC",
                "resultRecordCount": current_limit,
                "returnGeometry": "false",
                "f": "json",
            }
            data = None
            for attempt in range(1, 4):
                try:
                    resp = session.get(DC_BBL_RENTAL_URL, params=params, timeout=60)
                    resp.raise_for_status()
                    payload = resp.json()
                    if "error" in payload:
                        raise RuntimeError(f"ArcGIS error: {payload['error']}")
                    data = payload
                    break
                except Exception as exc:
                    if attempt >= 3:
                        raise
                    time.sleep(2 ** attempt)

            features = data.get("features", [])
            if not features:
                break

            max_oid = last_oid
            for feat in features:
                attrs = feat.get("attributes") or {}
                oid = attrs.get("OBJECTID")
                if isinstance(oid, (int, float)) and int(oid) > max_oid:
                    max_oid = int(oid)
                yield attrs
                yielded += 1
                if limit and yielded >= limit:
                    return

            if max_oid <= last_oid:
                break
            last_oid = max_oid
            if len(features) < current_limit and not data.get("exceededTransferLimit"):
                break
    finally:
        session.close()


def update_status(conn, status: str, details: Dict[str, Any], external_last_updated: Optional[dt.date] = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details)
            VALUES ('DC_RENTAL_LICENSES', 'city_enrichment', %s, NOW(), %s, %s)
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                last_refreshed_at = NOW(),
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
            """,
            (external_last_updated, status, Json(details)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix' OR key LIKE '%dc%'")
    conn.commit()


def enrich_dc_rental_licenses(limit: Optional[int] = None, rebuild_networks: bool = True) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        registrar_substrings = load_registrar_address_substrings(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT bbl FROM dc_properties WHERE bbl IS NOT NULL")
            bbl_rows = cur.fetchall()

        bbl_exact = {normalize_ssl(r[0]): r[0] for r in bbl_rows if r[0]}
        bbl_compact = {normalize_ssl_compact(r[0]): r[0] for r in bbl_rows if r[0]}

        registrations_by_id: Dict[str, tuple] = {}
        contacts_rows = []
        active_compliance_by_bbl: Dict[str, Tuple[str, Optional[dt.date]]] = {}
        latest_start_date: Optional[dt.date] = None

        total_fetched = 0
        matched_records = 0
        human_owners_found = 0

        for attrs in iter_dc_rental_licenses(limit=limit):
            total_fetched += 1
            raw_ssl_field = clean_text(attrs.get("SSL")) or ""
            ssl_tokens = [tok.strip() for tok in re.split(r"[,;]+", raw_ssl_field) if tok.strip()]
            matched_bbls = []
            for ssl_tok in ssl_tokens:
                b = bbl_exact.get(normalize_ssl(ssl_tok)) or bbl_compact.get(normalize_ssl_compact(ssl_tok))
                if b and b not in matched_bbls:
                    matched_bbls.append(b)
            if not matched_bbls:
                continue

            matched_records += 1
            cust_no = clean_text(attrs.get("CUSTOMERNUMBER"))
            oid = attrs.get("OBJECTID")
            lic_status = clean_text(attrs.get("LICENSESTATUS")) or "Unknown"
            start_dt = parse_dc_date(attrs.get("LICENSESTARTDATE"))
            end_dt = parse_dc_date(attrs.get("LICENSEENDDATE"))
            max_plausible_date = dt.date.today() + dt.timedelta(days=1)
            if start_dt and start_dt <= max_plausible_date and (latest_start_date is None or start_dt > latest_start_date):
                latest_start_date = start_dt

            ward_raw = clean_text(attrs.get("WARD"))
            borough = f"WARD {ward_raw}" if ward_raw and not ward_raw.upper().startswith("WARD") else (ward_raw or "DC")
            premise_addr = clean_text(attrs.get("PREMISEADDRESS"))
            _, _, _, premise_zip = parse_city_state_zip(premise_addr)
            mar_id_val = attrs.get("MAR_ID")
            mar_id_str = str(int(mar_id_val)) if isinstance(mar_id_val, (int, float)) else clean_text(mar_id_val)

            billing_addr_raw = clean_text(attrs.get("BILLINGADDRESS"))
            street, city_nm, state_cd, zip_cd = parse_city_state_zip(billing_addr_raw)

            entity_name = clean_text(attrs.get("ENTITYNAME")) or clean_text(attrs.get("ENTITYTRADENAME"))
            owner_first = clean_text(attrs.get("BUSINESSOWNERFIRSTNAME"))
            owner_last = clean_text(attrs.get("BUSINESSOWNERLASTNAME"))
            owner_full = " ".join(p for p in [owner_first, owner_last] if p).strip() or None
            agent_first = clean_text(attrs.get("AGENTFIRSTNAME"))
            agent_last = clean_text(attrs.get("AGENTLASTNAME"))
            agent_person = " ".join(p for p in [agent_first, agent_last] if p).strip() or None
            agent_entity = clean_text(attrs.get("AGENTENTITY"))

            for bbl in matched_bbls:
                reg_id = f"DC-BBL-{cust_no or oid}-{normalize_ssl_compact(bbl)}"
                if reg_id in registrations_by_id:
                    continue
                registrations_by_id[reg_id] = (
                    reg_id,
                    bbl,
                    mar_id_str,
                    premise_addr,
                    "WASHINGTON",
                    premise_zip,
                    borough,
                    lic_status,
                    start_dt,
                    end_dt,
                )

                if lic_status.upper() == "ACTIVE":
                    prev = active_compliance_by_bbl.get(bbl)
                    if prev is None or (end_dt and (prev[1] is None or end_dt > prev[1])):
                        active_compliance_by_bbl[bbl] = (cust_no or reg_id, end_dt)

                billing_coarse = normalize_mailing_address_coarse(billing_addr_raw or "") if billing_addr_raw else ""
                addr_excluded = is_excluded_third_party_address(
                    billing_addr_raw, billing_coarse, registrar_substrings
                )
                clean_street = None if addr_excluded else street
                clean_city_nm = None if addr_excluded else city_nm
                clean_state_cd = None if addr_excluded else state_cd
                clean_zip_cd = None if addr_excluded else zip_cd

                if (
                    owner_full
                    and looks_like_person_owner(owner_full)
                    and not is_excluded_third_party_entity(owner_full)
                    and not is_excluded_third_party_entity(entity_name)
                ):
                    human_owners_found += 1
                    contacts_rows.append(
                        (
                            reg_id,
                            "INDIVIDUALOWNER",
                            entity_name,
                            normalize_owner_name(entity_name) if entity_name else None,
                            owner_first,
                            owner_last,
                            owner_full,
                            normalize_person_name(owner_full),
                            clean_street,
                            clean_city_nm,
                            clean_state_cd,
                            clean_zip_cd,
                        )
                    )
                elif entity_name and not is_excluded_third_party_entity(entity_name):
                    is_person_entity = looks_like_person_owner(entity_name)
                    if is_person_entity:
                        human_owners_found += 1
                    contacts_rows.append(
                        (
                            reg_id,
                            "INDIVIDUALOWNER" if is_person_entity else "CORPORATEOWNER",
                            None if is_person_entity else entity_name,
                            None if is_person_entity else normalize_owner_name(entity_name),
                            None,
                            None,
                            entity_name if is_person_entity else None,
                            normalize_person_name(entity_name) if is_person_entity else None,
                            clean_street,
                            clean_city_nm,
                            clean_state_cd,
                            clean_zip_cd,
                        )
                    )

                if agent_person or agent_entity:
                    contacts_rows.append(
                        (
                            reg_id,
                            "AGENT",
                            agent_entity,
                            normalize_owner_name(agent_entity) if agent_entity else None,
                            agent_first,
                            agent_last,
                            agent_person,
                            normalize_person_name(agent_person) if agent_person else None,
                            None,
                            None,
                            None,
                            None,
                        )
                    )

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE dc_hpd_registrations, dc_hpd_contacts CASCADE;")
            if registrations_by_id:
                execute_values(
                    cur,
                    """
                    INSERT INTO dc_hpd_registrations (
                        registration_id, bbl, bin, building_address, building_city,
                        building_zip, borough, lifecycle_stage, last_registration_date, registration_end_date
                    ) VALUES %s
                    ON CONFLICT (registration_id) DO NOTHING
                    """,
                    list(registrations_by_id.values()),
                    page_size=2000,
                )
            if contacts_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO dc_hpd_contacts (
                        registration_id, contact_type, corporation_name, corporation_name_norm,
                        first_name, last_name, full_name, full_name_norm,
                        business_address, business_city, business_state, business_zip
                    ) VALUES %s
                    """,
                    contacts_rows,
                    page_size=2000,
                )

            cur.execute(
                "UPDATE dc_properties SET compliance_active = FALSE, compliance_record_id = NULL, compliance_expiration = NULL WHERE compliance_active IS TRUE"
            )
            if active_compliance_by_bbl:
                cur.execute(
                    "CREATE TEMP TABLE tmp_dc_compliance (bbl TEXT PRIMARY KEY, rec_id TEXT, exp DATE) ON COMMIT DROP"
                )
                execute_values(
                    cur,
                    "INSERT INTO tmp_dc_compliance (bbl, rec_id, exp) VALUES %s",
                    [(b, r_id, exp) for b, (r_id, exp) in active_compliance_by_bbl.items()],
                    page_size=2000,
                )
                cur.execute(
                    """
                    UPDATE dc_properties p
                    SET compliance_active = TRUE,
                        compliance_record_id = t.rec_id,
                        compliance_expiration = t.exp,
                        updated_at = NOW()
                    FROM tmp_dc_compliance t
                    WHERE p.bbl = t.bbl
                    """
                )
        conn.commit()

        rebuilt_networks = 0
        if rebuild_networks and not limit:
            rebuilt_networks = rebuild_city_networks_from_db(conn, "dc")

        details = {
            "source_url": DC_BBL_RENTAL_URL,
            "categories": list(RENTAL_CATEGORIES),
            "source_records": total_fetched,
            "matched_license_records": matched_records,
            "unique_registrations_inserted": len(registrations_by_id),
            "contacts_inserted": len(contacts_rows),
            "human_owner_records_found": human_owners_found,
            "active_licensed_parcels": len(active_compliance_by_bbl),
            "rebuilt_dc_networks": rebuilt_networks,
        }
        update_status(conn, "success", details, latest_start_date)
        logger.info("✓ D.C. rental license untangling complete: %s", json.dumps(details, default=str))
        return details
    except Exception as exc:
        conn.rollback()
        logger.exception("D.C. rental license enrichment failed: %s", exc)
        update_status(conn, "failure", {"error": str(exc)}, None)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich D.C. properties & networks from official DLCP Rental Licenses.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for smoke tests.")
    parser.add_argument("--skip-network-rebuild", action="store_true", help="Skip in-place dc_networks rebuild.")
    args = parser.parse_args()
    enrich_dc_rental_licenses(limit=args.limit, rebuild_networks=not args.skip_network_rebuild)
