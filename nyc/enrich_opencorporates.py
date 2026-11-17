#!/usr/bin/env python3
"""
nyc/enrich_opencorporates.py
============================
Enriches NYC HPD `CORPORATEOWNER` entities using the local OpenCorporates
corporate graph (`oc_companies`, `oc_officers`, `oc_previous_names`, `oc_branches`)
across `us_ny`, `us_nj`, `us_de`, `us_fl`, `us_ct`, `us_pa`, `us_wy`, and `us_nv`.

Source-Only & Anti-Meganet Guarantees:
  - Uses `api/oc_linker.py`'s 5-tier anti-homonym matcher (`score_oc_candidates`)
    and 3-hop bidirectional foreign-branch/holding-company resolver (`resolve_oc_company_principals`).
  - Preserves NYC HPD nonprofit intermediary separation (`NONPROFIT_INTERMEDIARY_CORPS`:
    MHANY / Banana Kelly / Neighborhood Restore HDFC).
  - Suppresses commercial registered agents, attorneys (`ESQ`), CPAs, and diffuse
    formation-service officers (`>=15` distinct corporate owners across diffuse addresses).
  - Idempotent: manages rows tagged `source_tag = 'OPENCORPORATES_BULK'` in `nyc_hpd_contacts`.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import logging
import os
import re
import sys
from typing import Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.oc_linker import (  # noqa: E402
    OCCandidate,
    demangle_assessor_owner,
    load_oc_registered_agent_addresses,
    resolve_oc_company_principals,
    score_oc_candidates,
)
from api.shared_utils import (  # noqa: E402
    compose_address,
    is_excluded_third_party_entity,
    normalize_business_name,
    normalize_mailing_address_coarse,
    normalize_person_name,
)
from nyc.build_nyc_networks import (  # noqa: E402
    CORP_MAX_USERS,
    NONPROFIT_INTERMEDIARY_CORPS,
    NYC_INSTITUTIONAL_PATTERN,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("nyc-oc-enrich")

OC_SOURCE_TAG = "OPENCORPORATES_BULK"
LEGACY_NY_DOS_TAG = "NY_DOS_ACTIVE_CORPS"
NYC_SCOPED_OC_JURS = ["us_ny", "us_nj", "us_de", "us_fl", "us_ct", "us_pa", "us_ma", "us_wy", "us_nv"]
_ATTORNEY_OR_CPA_RE = re.compile(r"\b(?:ESQ|ESQUIRE|CPA|C\.P\.A\.|ATTORNEY|COUNSEL|FILING)\b", re.IGNORECASE)


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def run_nyc_opencorporates_enrichment(*, rebuild_networks: bool = False) -> Dict[str, int]:
    db_url = _resolve_db_url()
    os.environ.setdefault("DATABASE_URL", db_url)
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE nyc_hpd_contacts ADD COLUMN IF NOT EXISTS source_tag TEXT;")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_nyc_contacts_source_tag
                ON nyc_hpd_contacts(source_tag) WHERE source_tag IS NOT NULL;
                """
            )
        conn.commit()

        ra_addrs = load_oc_registered_agent_addresses(conn)

        # 1. Load native HPD CorporateOwner records & existing human officers per registration
        corp_regs: Dict[str, Set[str]] = defaultdict(set)
        corp_raw_name: Dict[str, str] = {}
        corp_coarse_addrs: Dict[str, Set[str]] = defaultdict(set)
        corp_sample_raw_addr: Dict[str, str] = {}
        existing_reg_humans: Dict[str, Set[str]] = defaultdict(set)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT registration_id, contact_type, corporation_name, corporation_name_norm,
                       full_name_norm, business_address, business_city, business_state, business_zip
                FROM nyc_hpd_contacts
                WHERE COALESCE(source_tag, '') NOT IN (%s, %s)
                  AND registration_id IS NOT NULL
                """,
                (OC_SOURCE_TAG, LEGACY_NY_DOS_TAG),
            )
            for row in cur.fetchall():
                reg_id = row["registration_id"]
                ctype = (row.get("contact_type") or "").upper()
                if ctype in {"HEADOFFICER", "INDIVIDUALOWNER", "JOINTOWNER"} and row.get("full_name_norm"):
                    existing_reg_humans[reg_id].add(row["full_name_norm"])
                if ctype != "CORPORATEOWNER":
                    continue
                cnorm = (row.get("corporation_name_norm") or "").strip()
                craw = (row.get("corporation_name") or "").strip()
                if not cnorm or len(cnorm) < 4:
                    continue
                if cnorm in NONPROFIT_INTERMEDIARY_CORPS:
                    continue
                if is_excluded_third_party_entity(cnorm) or NYC_INSTITUTIONAL_PATTERN.search(cnorm):
                    continue
                corp_regs[cnorm].add(reg_id)
                if cnorm not in corp_raw_name and craw:
                    corp_raw_name[cnorm] = craw
                composed = compose_address(
                    row.get("business_address"),
                    row.get("business_city"),
                    row.get("business_state"),
                    row.get("business_zip"),
                )
                if composed:
                    if cnorm not in corp_sample_raw_addr:
                        corp_sample_raw_addr[cnorm] = composed
                    caddr = normalize_mailing_address_coarse(composed)
                    if caddr and len(caddr) >= 6:
                        corp_coarse_addrs[cnorm].add(caddr)

        # Exclude high-frequency institutional corps (`>= CORP_MAX_USERS` registrations)
        eligible_corps = [
            cnorm for cnorm, rids in corp_regs.items() if 1 <= len(rids) < CORP_MAX_USERS
        ]
        logger.info("Loaded %s eligible NYC HPD CorporateOwner entities.", f"{len(eligible_corps):,}")

        # Map HPD `corporation_name_norm` -> canonical `normalize_business_name`
        demangled_by_cnorm = {}
        lookup_norms: Set[str] = set()
        for cnorm in eligible_corps:
            raw_nm = corp_raw_name.get(cnorm, cnorm)
            rec = demangle_assessor_owner(raw_nm, mailing_address=corp_sample_raw_addr.get(cnorm))
            if rec.is_business and rec.owner_norm:
                demangled_by_cnorm[cnorm] = rec
                lookup_norms.add(rec.owner_norm)

        # 2. Fetch candidate `oc_companies` & `oc_previous_names` in chunks
        cands_by_norm: Dict[str, List[OCCandidate]] = defaultdict(list)
        norm_list = sorted(lookup_norms)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for idx in range(0, len(norm_list), 4000):
                chunk = norm_list[idx : idx + 4000]
                cur.execute(
                    """
                    SELECT c.*,
                           ARRAY(
                               SELECT DISTINCT o.address_coarse
                               FROM oc_officers o
                               WHERE o.jurisdiction_code = c.jurisdiction_code
                                 AND o.company_number = c.company_number
                                 AND o.role_class IN ('beneficial_principal', 'corporate_parent')
                                 AND o.address_coarse IS NOT NULL AND o.address_coarse != ''
                           ) AS officer_addrs
                    FROM oc_companies c
                    WHERE c.jurisdiction_code = ANY(%s)
                      AND c.name_norm = ANY(%s)
                    """,
                    (NYC_SCOPED_OC_JURS, chunk),
                )
                for r in cur.fetchall():
                    cands_by_norm[r["name_norm"]].append(
                        OCCandidate(
                            jurisdiction_code=r["jurisdiction_code"],
                            company_number=r["company_number"],
                            name=r["name"],
                            name_norm=r["name_norm"],
                            is_active=bool(r.get("is_active", True)),
                            registered_address_coarse=r.get("registered_address_coarse") or "",
                            mailing_address_coarse=r.get("mailing_address_coarse") or "",
                            agent_address_coarse=r.get("agent_address_coarse") or "",
                            officer_addresses_coarse=tuple(r.get("officer_addrs") or ()),
                            home_jurisdiction_code=r.get("home_jurisdiction_code"),
                            home_company_number=r.get("home_company_number"),
                        )
                    )

        # 3. Run 5-tier anti-homonym matching
        matched_key_by_cnorm: Dict[str, Tuple[str, str]] = {}
        for cnorm, rec in demangled_by_cnorm.items():
            addrs = sorted(corp_coarse_addrs.get(cnorm, ()))
            primary_coarse = addrs[0] if addrs else None
            extra_coarse = addrs[1:] if len(addrs) > 1 else ()
            res = score_oc_candidates(
                rec,
                parcel_state="NY",
                parcel_mailing_coarse=primary_coarse,
                candidates=cands_by_norm.get(rec.owner_norm, ()),
                extra_parcel_coarse_addrs=extra_coarse,
                registered_agent_addrs=ra_addrs,
            )
            if res is not None:
                matched_key_by_cnorm[cnorm] = (res.canonical_jurisdiction_code, res.canonical_company_number)

        logger.info(
            "Matched %s / %s NYC HPD corporations to OpenCorporates.",
            f"{len(matched_key_by_cnorm):,}",
            f"{len(demangled_by_cnorm):,}",
        )

        # 4. Multi-hop principal resolution (`us_ny` <-> `us_de`/`us_nj`/`us_fl` + corporate parents)
        resolved = resolve_oc_company_principals(conn, list(set(matched_key_by_cnorm.values())), max_hops=3)

        # Pre-screen diffuse formation-service / paralegal names appearing on >=12 corps with diffuse addresses
        person_corps: Dict[str, Set[str]] = defaultdict(set)
        person_addrs: Dict[str, Set[str]] = defaultdict(set)
        corp_humans: Dict[str, List[Tuple[str, str]]] = {}

        for cnorm, okey in matched_key_by_cnorm.items():
            info = resolved.get(okey) or {}
            valid_h: List[Tuple[str, str]] = []
            for h_raw in info.get("humans") or ():
                if _ATTORNEY_OR_CPA_RE.search(h_raw):
                    continue
                if is_excluded_third_party_entity(h_raw) or NYC_INSTITUTIONAL_PATTERN.search(h_raw):
                    continue
                h_norm = normalize_person_name(h_raw)
                if not h_norm or len(h_norm) < 5 or len(h_norm.split()) < 2:
                    continue
                valid_h.append((h_raw, h_norm))
                person_corps[h_norm].add(cnorm)
                person_addrs[h_norm].update(corp_coarse_addrs.get(cnorm, ()))
            if valid_h:
                corp_humans[cnorm] = valid_h

        blocked_oc_people = {
            h_norm
            for h_norm, cset in person_corps.items()
            if len(cset) >= 12 and len(person_addrs[h_norm]) >= max(5, int(len(cset) * 0.5))
        }

        new_contact_rows: List[Tuple] = []
        corps_pierced = 0
        for cnorm, h_list in corp_humans.items():
            kept = [(hr, hn) for (hr, hn) in h_list if hn not in blocked_oc_people]
            if not kept:
                continue
            corps_pierced += 1
            raw_corp = corp_raw_name.get(cnorm, cnorm)
            for reg_id in corp_regs[cnorm]:
                seen_on_reg = existing_reg_humans[reg_id]
                for h_raw, h_norm in kept:
                    if h_norm in seen_on_reg:
                        continue
                    seen_on_reg.add(h_norm)
                    parts = h_norm.split()
                    new_contact_rows.append(
                        (
                            reg_id,
                            "HEADOFFICER",
                            raw_corp,
                            cnorm,
                            parts[0],
                            parts[-1] if len(parts) > 1 else None,
                            h_raw,
                            h_norm,
                            None,
                            None,
                            None,
                            None,
                            OC_SOURCE_TAG,
                        )
                    )

        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM nyc_hpd_contacts WHERE source_tag IN (%s, %s)",
                (OC_SOURCE_TAG, LEGACY_NY_DOS_TAG),
            )
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
                    page_size=5000,
                )
            details = {
                "eligible_corporate_owners": len(eligible_corps),
                "matched_oc_corporations": len(matched_key_by_cnorm),
                "corporations_pierced_to_humans": corps_pierced,
                "blocked_diffuse_officers": len(blocked_oc_people),
                "contact_rows_synced": len(new_contact_rows),
            }
            cur.execute(
                """
                INSERT INTO data_source_status
                    (source_name, source_type, last_refreshed_at, refresh_status, details)
                VALUES ('NYC_OPENCORPORATES', 'nyc', NOW(), 'success', %s)
                ON CONFLICT (source_name) DO UPDATE SET
                    last_refreshed_at = NOW(),
                    refresh_status = 'success',
                    details = EXCLUDED.details;
                """,
                (Json(details),),
            )
        conn.commit()
        logger.info("✓ NYC OpenCorporates enrichment complete: %s", json.dumps(details))
    finally:
        conn.close()

    if rebuild_networks:
        from nyc.build_nyc_networks import build_nyc_networks
        build_nyc_networks()

    return details


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich NYC corporate owners from OpenCorporates bulk graph.")
    parser.add_argument("--rebuild-networks", action="store_true", help="Rebuild `nyc_networks` immediately after syncing.")
    args = parser.parse_args()
    run_nyc_opencorporates_enrichment(rebuild_networks=args.rebuild_networks)
