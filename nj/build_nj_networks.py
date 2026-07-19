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

from api.shared_utils import looks_like_business_name, normalize_owner_name


DATABASE_URL = os.environ.get("DATABASE_URL")
OWNER_ADDRESS_MAX_DISTINCT_OWNERS = 12
LOGGER = logging.getLogger("nj-networks")

GENERIC_OWNER_NAMES = {
    "",
    "UNKNOWN",
    "UNKNOWN OWNER",
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
    "HOUSING AUTHORITY",
    "BOARD OF EDUCATION",
    "SCHOOL DISTRICT",
    "RUTGERS UNIVERSITY",
    "MONTCLAIR STATE UNIVERSITY",
    "PORT AUTHORITY",
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
    norm = normalize_owner_name(name or "")
    if norm in GENERIC_OWNER_NAMES:
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


def load_properties(conn) -> List[dict]:
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
                bhi_registration_no
            FROM nj_properties
            WHERE bbl IS NOT NULL
            """
        )
        return [dict(row) for row in cur.fetchall()]


def build_groups(properties: Iterable[dict]) -> tuple[Dict[str, List[dict]], dict]:
    props = [p for p in properties if p.get("bbl")]
    uf = UnionFind()
    for prop in props:
        uf.add(prop["bbl"])

    by_owner: Dict[str, List[str]] = defaultdict(list)
    by_owner_address: Dict[str, List[str]] = defaultdict(list)
    address_owners: Dict[str, set] = defaultdict(set)

    for prop in props:
        bbl = prop["bbl"]
        owner_norm = normalize_owner_name(prop.get("owner_name_norm") or prop.get("owner_name") or "")
        prop["owner_name_norm"] = owner_norm
        address_norm = (prop.get("mailing_address_norm") or "").strip()
        prop["mailing_address_norm"] = address_norm

        if owner_norm and not is_public_or_generic_owner(owner_norm):
            by_owner[owner_norm].append(bbl)
            if not is_generic_address(address_norm):
                by_owner_address[address_norm].append(bbl)
                address_owners[address_norm].add(owner_norm)

    for bbls in by_owner.values():
        anchor = bbls[0]
        for bbl in bbls[1:]:
            uf.union(anchor, bbl)

    skipped_address_links = 0
    for address_norm, bbls in by_owner_address.items():
        if len(address_owners[address_norm]) > OWNER_ADDRESS_MAX_DISTINCT_OWNERS:
            skipped_address_links += 1
            continue
        anchor = bbls[0]
        for bbl in bbls[1:]:
            uf.union(anchor, bbl)

    groups: Dict[str, List[dict]] = defaultdict(list)
    for prop in props:
        groups[uf.find(prop["bbl"])].append(prop)

    metadata = {
        "properties_loaded": len(props),
        "owner_signal_count": len(by_owner),
        "owner_address_signal_count": len(by_owner_address),
        "skipped_high_cardinality_owner_addresses": skipped_address_links,
        "owner_address_max_distinct_owners": OWNER_ADDRESS_MAX_DISTINCT_OWNERS,
    }
    return groups, metadata


def format_network_rows(groups: Dict[str, List[dict]]) -> List[tuple]:
    rows = []
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

        connection_signals = {
            "source_scope": "NJ DCA BHI registered active buildings",
            "owner_names": member_names[:50],
            "owner_name_count": len(member_names),
            "owner_mailing_address_count": len(member_addresses),
            "authorized_agents_observed": agent_names[:50],
            "authorized_agents_not_used_for_linking": True,
            "linking_signals": ["primary_owner_name", "primary_owner_mailing_address"],
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
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def build_nj_networks(conn=None):
    own_conn = conn is None
    if own_conn:
        conn = get_conn()
    assert conn is not None
    try:
        update_status(conn, "running", {"message": "Rebuilding NJ BHI owner networks"})
        properties = load_properties(conn)
        groups, metadata = build_groups(properties)
        rows = format_network_rows(groups)
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

