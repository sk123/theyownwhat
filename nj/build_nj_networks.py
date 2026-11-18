#!/usr/bin/env python3
"""
Build New Jersey BHI owner networks.

Signals:
  1. Shared primary owner name.
  2. Shared primary-owner mailing address, capped by distinct owner count.

Authorized agents/managers are retained as contacts and metadata, but are not
ownership-linking signals. That avoids merging unrelated owners through a large
management company or registered-agent office.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.shared_utils import (
    extract_owner_family_key,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    is_registrar_address,
    load_registrar_address_substrings,
    looks_like_business_name,
    looks_like_person_owner,
    normalize_mailing_address,
    normalize_mailing_address_coarse,
    normalize_owner_name,
    normalize_person_name,
    strip_benign_care_of_prefix,
)


DATABASE_URL = os.environ.get("DATABASE_URL")
OWNER_ADDRESS_MAX_DISTINCT_OWNERS = 10
OWNER_COARSE_BUILDING_MAX_FAMILIES = 12
PO_BOX_MAX_OWNER_FAMILIES = 4
LOGGER = logging.getLogger("nj-networks")

GENERIC_OWNER_NAMES = {
    "",
    "UNKNOWN",
    "UNKNOWN OWNER",
    "CURRENT OWNER",
    "OCCUPANT",
    "REFERENCE ONLY",
    "NO INFORMATION PROVIDED",
    "NOT PROVIDED",
    "N/A",
    "NA",
    "NONE",
    "NULL",
    "OWNER",
}

PUBLIC_ENTITY_KEYWORDS = {
    "CITY OF",
    "TOWNSHIP OF",
    "BOROUGH OF",
    "TOWN OF",
    "COUNTY OF",
    "STATE OF NEW JERSEY",
    "STATE OF NJ",
    "UNITED STATES",
    "SECRETARY OF HOUSING",
    "SECRETARY OF VETERANS",
    "FANNIE MAE",
    "FREDDIE MAC",
    "HOUSING AUTHORITY",
    "REDEVELOPMENT",
    "LAND BANK",
    "BOARD OF EDUCATION",
    "SCHOOL DISTRICT",
    "RUTGERS UNIVERSITY",
    "MONTCLAIR STATE UNIVERSITY",
    "PORT AUTHORITY",
    "NEW JERSEY TRANSIT",
    "NJ TRANSIT",
}


class UnionFind:
    def __init__(self):
        self.parent: Dict[str, str] = {}

    def add(self, value: str):
        if value not in self.parent:
            self.parent[value] = value

    def find(self, value: str) -> str:
        self.add(value)
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, a: str, b: str):
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if ra > rb:
            ra, rb = rb, ra
        self.parent[rb] = ra


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(DATABASE_URL)


def is_public_or_generic_owner(name: Optional[str]) -> bool:
    raw = (name or "").strip()
    norm = normalize_owner_name(raw)
    if norm in GENERIC_OWNER_NAMES:
        return True
    if is_excluded_third_party_entity(raw) or is_excluded_third_party_entity(norm):
        return True
    return any(keyword in norm for keyword in PUBLIC_ENTITY_KEYWORDS)


def is_generic_address(address_norm: Optional[str]) -> bool:
    text = (address_norm or "").strip().upper()
    if not text or len(text) < 8:
        return True
    generic_fragments = {
        "PO BOX 000",
        "P O BOX 000",
        "UNKNOWN",
        "NOT PROVIDED",
        "N/A",
    }
    return any(fragment in text for fragment in generic_fragments)


def network_key_for(root_bbl: str) -> str:
    clean = re.sub(r"[^a-z0-9]+", "_", root_bbl.lower()).strip("_")
    return f"network_nj_{clean}"


def ensure_nj_oc_columns(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE nj_properties ADD COLUMN IF NOT EXISTS owner_co_name TEXT;
            ALTER TABLE nj_properties ADD COLUMN IF NOT EXISTS owner_is_truncated BOOLEAN DEFAULT FALSE;
            ALTER TABLE nj_properties ADD COLUMN IF NOT EXISTS mailing_address_coarse TEXT;
            ALTER TABLE nj_properties ADD COLUMN IF NOT EXISTS mailing_state TEXT;
            ALTER TABLE nj_properties ADD COLUMN IF NOT EXISTS last_sale_date DATE;
            ALTER TABLE nj_properties ADD COLUMN IF NOT EXISTS oc_jurisdiction_code TEXT;
            ALTER TABLE nj_properties ADD COLUMN IF NOT EXISTS oc_company_number TEXT;
            ALTER TABLE nj_properties ADD COLUMN IF NOT EXISTS oc_match_tier SMALLINT;
            ALTER TABLE nj_properties ADD COLUMN IF NOT EXISTS oc_match_confidence NUMERIC(4,3);
            """
        )
    conn.commit()


def load_properties(conn) -> List[dict]:
    ensure_nj_oc_columns(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                bbl,
                address,
                owner_name,
                owner_name_norm,
                mailing_address,
                mailing_address_norm,
                authorized_agent_name,
                county,
                municipality,
                units_res,
                bhi_registration_no,
                oc_jurisdiction_code,
                oc_company_number,
                oc_match_tier,
                oc_match_confidence
            FROM nj_properties
            WHERE bbl IS NOT NULL
            """
        )
        return [dict(row) for row in cur.fetchall()]


def build_groups(
    properties: Iterable[dict],
    registrar_substrings: Optional[tuple] = None,
    oc_principals_by_key: Optional[Dict[tuple, dict]] = None,
    registered_agent_addrs: Optional[set] = None,
) -> tuple[Dict[str, List[dict]], dict]:
    all_props = [p for p in properties if p.get("bbl")]
    excluded_coarse_counts: Dict[str, int] = defaultdict(int)
    props: List[dict] = []
    excluded_parcels = 0
    oc_map = oc_principals_by_key or {}
    ra_addrs = set(registered_agent_addrs or ())
    attorney_re = re.compile(r"\b(?:ESQ|ESQUIRE|CPA|C\.P\.A\.|ATTORNEY)\b", re.IGNORECASE)

    for prop in all_props:
        raw_owner = (prop.get("owner_name") or "").strip()
        owner_norm = normalize_owner_name(raw_owner or prop.get("owner_name_norm"))
        if looks_like_business_name(raw_owner):
            owner_norm = re.sub(r"^THE\s+", "", owner_norm).strip() or owner_norm
        prop["owner_name_norm"] = owner_norm
        raw_mail = (prop.get("mailing_address") or "").strip()
        clean_mail = strip_benign_care_of_prefix(raw_mail)
        address_norm = (normalize_mailing_address(clean_mail) or prop.get("mailing_address_norm") or "").strip()
        coarse_mail = normalize_mailing_address_coarse(clean_mail)
        prop["mailing_address_norm"] = address_norm
        prop["_coarse_mail"] = coarse_mail

        if not owner_norm or is_public_or_generic_owner(raw_owner) or is_public_or_generic_owner(owner_norm):
            excluded_parcels += 1
            if coarse_mail:
                excluded_coarse_counts[coarse_mail] += 1
            continue
        props.append(prop)

    contaminated_coarse = {c for c, cnt in excluded_coarse_counts.items() if cnt >= 2}

    uf = UnionFind()
    for prop in props:
        uf.add(prop["bbl"])

    by_owner: Dict[str, List[str]] = defaultdict(list)
    by_owner_address: Dict[str, List[str]] = defaultdict(list)
    address_families: Dict[str, set] = defaultdict(set)
    coarse_families: Dict[str, set] = defaultdict(set)
    addr_to_coarse: Dict[str, str] = {}
    by_oc_entity: Dict[tuple, List[str]] = defaultdict(list)
    oc_entity_fams: Dict[tuple, set] = defaultdict(set)
    by_oc_human: Dict[str, List[str]] = defaultdict(list)
    oc_human_fams: Dict[str, set] = defaultdict(set)

    for prop in props:
        bbl = prop["bbl"]
        owner_norm = prop["owner_name_norm"]
        family = extract_owner_family_key(prop.get("owner_name")) or owner_norm
        raw_mail = prop.get("mailing_address") or ""
        address_norm = prop["mailing_address_norm"]
        coarse_mail = prop.get("_coarse_mail") or ""

        by_owner[owner_norm].append(bbl)
        if (
            not is_generic_address(address_norm)
            and (not coarse_mail or (coarse_mail not in contaminated_coarse and coarse_mail not in ra_addrs))
            and not is_excluded_third_party_address(raw_mail, address_norm, registrar_substrings or ())
        ):
            by_owner_address[address_norm].append(bbl)
            address_families[address_norm].add(family)
            if coarse_mail:
                addr_to_coarse[address_norm] = coarse_mail
                coarse_families[coarse_mail].add(family)

        oj, oc = prop.get("oc_jurisdiction_code"), prop.get("oc_company_number")
        if oj and oc:
            okey = (oj, oc)
            by_oc_entity[okey].append(bbl)
            oc_entity_fams[okey].add(family)
            oc_info = oc_map.get(okey) or {}
            for h_raw in oc_info.get("humans") or ():
                if attorney_re.search(h_raw) or is_public_or_generic_owner(h_raw):
                    continue
                h_norm = normalize_person_name(h_raw)
                if h_norm and len(h_norm.split()) >= 2:
                    by_oc_human[h_norm].append(bbl)
                    oc_human_fams[h_norm].add(family)

    for bbls in by_owner.values():
        anchor = bbls[0]
        for bbl in bbls[1:]:
            uf.union(anchor, bbl)

    skipped_address_links = 0
    for address_norm, bbls in by_owner_address.items():
        is_po_box = address_norm.upper().startswith("PO BOX ")
        max_fine = PO_BOX_MAX_OWNER_FAMILIES if is_po_box else OWNER_ADDRESS_MAX_DISTINCT_OWNERS
        fam_cnt = len(address_families[address_norm])
        coarse_key = addr_to_coarse.get(address_norm)
        coarse_cnt = len(coarse_families[coarse_key]) if coarse_key else fam_cnt
        if fam_cnt > max_fine or coarse_cnt > OWNER_COARSE_BUILDING_MAX_FAMILIES:
            skipped_address_links += 1
            continue
        anchor = bbls[0]
        for bbl in bbls[1:]:
            uf.union(anchor, bbl)

    oc_entity_merges = 0
    for okey, bbls in by_oc_entity.items():
        if len(bbls) >= 2 and len(oc_entity_fams[okey]) <= 40:
            anchor = bbls[0]
            for bbl in bbls[1:]:
                if uf.find(anchor) != uf.find(bbl):
                    uf.union(anchor, bbl)
                    oc_entity_merges += 1

    oc_human_merges = 0
    for h_norm, bbls in by_oc_human.items():
        if len(bbls) >= 2 and len(oc_human_fams[h_norm]) <= 25:
            anchor = bbls[0]
            for bbl in bbls[1:]:
                if uf.find(anchor) != uf.find(bbl):
                    uf.union(anchor, bbl)
                    oc_human_merges += 1

    groups: Dict[str, List[dict]] = defaultdict(list)
    for prop in props:
        groups[uf.find(prop["bbl"])].append(prop)

    from api.oc_linker import cluster_with_oc_and_series
    groups_merged, oc_series_merges = cluster_with_oc_and_series(
        props,
        dict(groups),
        oc_map,
        is_public_fn=is_public_or_generic_owner,
        generic_owner_names=GENERIC_OWNER_NAMES,
    )

    metadata = {
        "properties_loaded": len(all_props),
        "excluded_third_party_properties": excluded_parcels,
        "owner_signal_count": len(by_owner),
        "owner_address_signal_count": len(by_owner_address),
        "skipped_high_cardinality_owner_addresses": skipped_address_links,
        "owner_address_max_distinct_owners": OWNER_ADDRESS_MAX_DISTINCT_OWNERS,
        "oc_entity_merges": oc_entity_merges + oc_series_merges,
        "oc_human_principal_merges": oc_human_merges,
    }
    return groups_merged, metadata


def format_network_rows(
    groups: Dict[str, List[dict]],
    oc_principals_by_key: Optional[Dict[tuple, dict]] = None,
) -> List[tuple]:
    rows = []
    oc_map = oc_principals_by_key or {}
    for root_bbl, group in groups.items():
        owner_counts = Counter(p.get("owner_name") or "Unknown owner" for p in group)
        display_name = owner_counts.most_common(1)[0][0]
        anchor_type = "corp" if looks_like_business_name(display_name) else "person"

        member_names = sorted({p.get("owner_name") for p in group if p.get("owner_name")})
        member_addresses = sorted({p.get("mailing_address") for p in group if p.get("mailing_address")})
        agent_names = sorted({p.get("authorized_agent_name") for p in group if p.get("authorized_agent_name")})
        bbl_list = sorted({p["bbl"] for p in group if p.get("bbl")})
        registration_ids = bbl_list[:]
        borough_summary = Counter(p.get("county") or "UNKNOWN" for p in group)
        unit_count = sum(int(float(p.get("units_res") or 0)) for p in group)

        human_counts: Counter[str] = Counter()
        holding_entities: set[str] = set()
        oc_ids: set[str] = set()
        for p in group:
            onm = (p.get("owner_name") or "").strip()
            if onm and looks_like_person_owner(onm):
                human_counts[onm] += 2
            oj, oc = p.get("oc_jurisdiction_code"), p.get("oc_company_number")
            if oj and oc:
                oc_ids.add(f"{oj}/{oc}")
                info = oc_map.get((oj, oc)) or {}
                for h_nm in info.get("humans") or ():
                    if looks_like_person_owner(h_nm) and not is_public_or_generic_owner(h_nm):
                        human_counts[h_nm] += 1
                for he in info.get("holding_entities") or ():
                    if not is_public_or_generic_owner(he):
                        holding_entities.add(he)

        source_humans = [nm for nm, _ in sorted(human_counts.items(), key=lambda x: (-x[1], x[0]))]
        primary_human = source_humans[0] if source_humans else None

        linking_signals = ["primary_owner_name", "primary_owner_mailing_address"]
        if oc_ids:
            linking_signals.append("opencorporates_registry_graph")

        connection_signals = {
            "source_scope": "NJ DCA BHI registered active buildings + OpenCorporates corporate filings",
            "owner_names": member_names[:50],
            "owner_name_count": len(member_names),
            "owner_mailing_address_count": len(member_addresses),
            "authorized_agents_observed": agent_names[:50],
            "authorized_agents_not_used_for_linking": True,
            "source_human_names": source_humans[:25],
            "primary_human_name": primary_human,
            "principal_status": "source_listed_person" if primary_human else "unresolved_entity",
            "oc_company_ids": sorted(oc_ids)[:25],
            "oc_holding_entities": sorted(holding_entities)[:20],
            "linking_signals": linking_signals,
        }

        rows.append(
            (
                network_key_for(root_bbl),
                anchor_type,
                display_name,
                member_names,
                member_addresses,
                registration_ids,
                bbl_list,
                len(bbl_list),
                unit_count,
                json.dumps(dict(sorted(borough_summary.items()))),
                json.dumps(connection_signals),
            )
        )
    return rows


def update_status(conn, status: str, details: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, last_refreshed_at, refresh_status, details)
            VALUES ('NJ_NETWORKS', 'city_dataset', NOW(), %s, %s::jsonb)
            ON CONFLICT (source_name)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
            """,
            (status, json.dumps(details, default=str)),
        )
        if status == "success":
            cur.execute(
                "DELETE FROM kv_cache WHERE key IN ('completeness_matrix', 'city_stats:v1:nj') OR key LIKE 'city_top_nets:v1:nj:%'"
            )
        else:
            cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def build_nj_networks(conn=None):
    own_conn = conn is None
    if own_conn:
        conn = get_conn()
    assert conn is not None
    try:
        update_status(conn, "running", {"message": "Rebuilding NJ BHI owner networks"})
        ensure_nj_oc_columns(conn)
        oc_stats = {"evaluated": 0, "matched": 0}
        try:
            from api.oc_linker import link_jurisdiction_parcels_to_oc, resolve_oc_company_principals
            oc_stats = link_jurisdiction_parcels_to_oc(conn, "nj", "NJ", col_max_len=40)
            if oc_stats.get("matched"):
                LOGGER.info(
                    "OpenCorporates linker matched %s / %s NJ BHI corporate buildings",
                    f"{oc_stats['matched']:,}",
                    f"{oc_stats['evaluated']:,}",
                )
        except Exception as oc_exc:
            conn.rollback()
            LOGGER.warning("NJ OC linking skipped: %s", oc_exc)

        registrar_substrings = load_registrar_address_substrings(conn)
        properties = load_properties(conn)
        matched_oc_keys = sorted(
            {
                (p["oc_jurisdiction_code"], p["oc_company_number"])
                for p in properties
                if p.get("oc_jurisdiction_code") and p.get("oc_company_number")
            }
        )
        oc_principals_by_key = {}
        if matched_oc_keys:
            try:
                from api.oc_linker import resolve_oc_company_principals
                oc_principals_by_key = resolve_oc_company_principals(conn, matched_oc_keys, max_hops=3)
            except Exception as oc_exc:
                conn.rollback()
                LOGGER.warning("NJ OC principal resolution skipped: %s", oc_exc)

        from api.oc_linker import load_oc_registered_agent_addresses
        ra_addrs = load_oc_registered_agent_addresses(conn)
        groups, metadata = build_groups(
            properties,
            registrar_substrings=registrar_substrings,
            oc_principals_by_key=oc_principals_by_key,
            registered_agent_addrs=ra_addrs,
        )
        metadata["oc_matched_buildings"] = oc_stats.get("matched", 0)
        rows = format_network_rows(groups, oc_principals_by_key=oc_principals_by_key)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE nj_networks;")
            if rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO nj_networks (
                        network_key, anchor_type, display_name, member_names,
                        member_addresses, registration_ids, bbl_list,
                        building_count, unit_count, borough_summary, connection_signals
                    )
                    VALUES %s;
                    """,
                    rows,
                    page_size=2000,
                )
        conn.commit()
        details = {
            "message": "NJ BHI owner networks rebuilt",
            "networks": len(rows),
            **metadata,
        }
        update_status(conn, "success", details)
        LOGGER.info("NJ network build complete: %s networks", f"{len(rows):,}")
        return details
    except Exception as exc:
        LOGGER.exception("NJ network build failed")
        try:
            update_status(conn, "failure", {"message": str(exc)})
        except Exception:
            pass
        raise
    finally:
        if own_conn:
            conn.close()


def main():
    parser = argparse.ArgumentParser(description="Build NJ BHI owner networks.")
    parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    build_nj_networks()


if __name__ == "__main__":
    main()

