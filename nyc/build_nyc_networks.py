#!/usr/bin/env python3
"""
nyc/build_nyc_networks.py
=========================
Clusters NYC HPD contacts into ownership networks using Union-Find,
then writes results to the nyc_networks table.

Clustering signals (applied in order):
  1. Shared normalized person name  (HeadOfficer / IndividualOwner full_name_norm)
  2. Shared owner corporation name  (CorporateOwner corporation_name_norm)
  3. Shared owner/officer mailing address (not Agent-only management addresses)

Mega-net prevention:
  Any cluster exceeding MAX_NETWORK_BBLS buildings is re-clustered using
  ONLY the name/corp signals (address signal dropped).  This prevents
  a single law firm or managing-agent address from collapsing thousands
  of unrelated landlords into one giant fake "portfolio".

Usage:
    DATABASE_URL=... python -m nyc.build_nyc_networks

    # Skip the slow property join (faster for testing)
    DATABASE_URL=... python -m nyc.build_nyc_networks --skip-property-join

    # Tune the mega-net threshold
    DATABASE_URL=... python -m nyc.build_nyc_networks --max-buildings 200
"""

import os
import sys
import argparse
import logging
import json
from collections import defaultdict
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("nyc-networks")

DATABASE_URL = os.environ.get("DATABASE_URL")

# Mega-net threshold — any component with more buildings than this is re-split
DEFAULT_MAX_BUILDINGS = 300

# Only anchor on person names with these contact types
PERSON_CONTACT_TYPES = {"HEADOFFICER", "INDIVIDUALOWNER", "JOINTOWNER"}
CORP_CONTACT_TYPES = {"CORPORATEOWNER"}
ADDRESS_CONTACT_TYPES = PERSON_CONTACT_TYPES | CORP_CONTACT_TYPES

# Noise person name tokens — skip anything that is only these
NOISE_NAMES = {"UNKNOWN", "VACANT", "NONE", "N A", "NA", "N/A", "TBD", "SAME"}

# Addresses used by this many or more registrations are treated as shared registered-agent
# offices and excluded from the address clustering signal entirely.
# (Law firms, CT Corporation, management company HQs, etc.)
ADDR_MAX_USERS = 10  # any address seen on ≥10 registrations → blocked

# Corporations used by this many or more registrations are treated as institutional
# entities (banks, mortgage servicers, title companies) and excluded from the
# corporation clustering signal.  Small LLCs own 1-30 buildings; a corp on 75+
# registrations is almost certainly not a mom-and-pop landlord.
CORP_MAX_USERS = 75  # any corp seen on ≥75 registrations → excluded from corp_index


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------
class UnionFind:
    def __init__(self):
        self._parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]  # path compression
            x = self._parent[x]
        return x

    def union(self, a: str, b: str):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            if ra > rb:
                ra, rb = rb, ra
            self._parent[rb] = ra

    def components(self) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = defaultdict(list)
        for node in self._parent:
            groups[self.find(node)].append(node)
        return dict(groups)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set.")
    return psycopg2.connect(DATABASE_URL)


def update_network_status(conn, status: str, details: dict):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_source_status
                    (source_name, source_type, last_refreshed_at, refresh_status, details)
                VALUES ('NYC_NETWORKS', 'nyc', NOW(), %s, %s::jsonb)
                ON CONFLICT (source_name)
                DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    last_refreshed_at = EXCLUDED.last_refreshed_at,
                    refresh_status = EXCLUDED.refresh_status,
                    details = EXCLUDED.details;
                """,
                (status, json.dumps(details or {})),
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Could not update NYC network status: {e}")


# ---------------------------------------------------------------------------
# Name-only re-clustering for mega-nets
# ---------------------------------------------------------------------------
def split_meganet(
    reg_ids: list[str],
    reg_names: dict[str, set],
    max_buildings: int,
) -> list[list[str]]:
    """
    Given a list of registration_ids that formed a mega-net (via address signal),
    re-cluster them using ONLY shared person/corp names.
    Returns a list of sub-clusters (each is a list of reg_ids).
    """
    uf2 = UnionFind()

    # Build name → reg_ids index (name signal only)
    name_idx: dict[str, list[str]] = defaultdict(list)
    for rid in reg_ids:
        uf2.find(rid)  # ensure node exists
        for name in reg_names.get(rid, set()):
            if name and len(name) >= 8 and name not in NOISE_NAMES:
                name_idx[name].append(rid)

    for name, rids in name_idx.items():
        unique = list(set(rids))
        if len(unique) < 2:
            continue
        anchor = unique[0]
        for other in unique[1:]:
            uf2.union(anchor, other)

    components = uf2.components()
    return list(components.values())


# ---------------------------------------------------------------------------
# Main clustering logic
# ---------------------------------------------------------------------------
def build_nyc_networks(skip_property_join: bool = False, max_buildings: int = DEFAULT_MAX_BUILDINGS):
    conn = get_conn()
    cur  = conn.cursor()
    update_network_status(conn, "running", {"message": "Rebuilding NYC owner-contact networks"})

    # ------------------------------------------------------------------
    # 1. Load all contacts
    # ------------------------------------------------------------------
    logger.info("Loading HPD contacts …")
    cur.execute("""
        SELECT
            c.registration_id,
            c.contact_type,
            c.full_name_norm,
            c.corporation_name_norm,
            c.business_address,
            c.business_zip
        FROM nyc_hpd_contacts c
        WHERE c.full_name_norm IS NOT NULL
           OR c.corporation_name_norm IS NOT NULL
    """)
    contacts = cur.fetchall()
    logger.info(f"  {len(contacts):,} contacts loaded.")

    # ------------------------------------------------------------------
    # 2. Load registrations
    # ------------------------------------------------------------------
    logger.info("Loading HPD registrations …")
    cur.execute("""
        SELECT registration_id, bbl, borough
        FROM nyc_hpd_registrations
        WHERE registration_id IS NOT NULL
    """)
    reg_rows = cur.fetchall()
    reg_bbl:     dict[str, Optional[str]] = {}
    reg_borough: dict[str, Optional[str]] = {}
    bbl_index:   dict[str, list[str]] = defaultdict(list)
    for reg_id, bbl, borough in reg_rows:
        reg_bbl[reg_id]     = bbl
        reg_borough[reg_id] = borough
        if bbl:
            bbl_index[bbl].append(reg_id)
    logger.info(f"  {len(reg_bbl):,} registrations loaded.")

    # ------------------------------------------------------------------
    # 3. Build per-registration metadata (names + addresses)
    # ------------------------------------------------------------------
    reg_names: dict[str, set] = defaultdict(set)   # strong owner/officer names used for linking
    reg_addrs: dict[str, set] = defaultdict(set)   # all mailing addresses for display/audit
    reg_people: dict[str, set] = defaultdict(set)  # valid person names per reg_id
    reg_corps: dict[str, set] = defaultdict(set)   # valid owner corp names per reg_id

    person_index: dict[str, list[str]] = defaultdict(list)
    corp_index:   dict[str, list[str]] = defaultdict(list)
    addr_index:   dict[str, list[str]] = defaultdict(list)

    for reg_id, contact_type, name_norm, corp_norm, biz_addr, biz_zip in contacts:
        if not reg_id or reg_id not in reg_bbl:
            continue

        if biz_addr and biz_zip:
            reg_addrs[reg_id].add(f"{biz_addr.strip()} {biz_zip.strip()}")

        # Signal 1: person name (officer types only)
        if name_norm and contact_type and contact_type.upper() in PERSON_CONTACT_TYPES:
            if len(name_norm) >= 5 and name_norm not in NOISE_NAMES:
                person_index[name_norm].append(reg_id)
                reg_people[reg_id].add(name_norm)
                reg_names[reg_id].add(name_norm)

        # Signal 2: owner corp name. AGENT corporations are management signals,
        # not ownership signals, so they should not stitch landlord networks.
        if corp_norm and len(corp_norm) >= 4 and contact_type and contact_type.upper() in CORP_CONTACT_TYPES:
            corp_index[corp_norm].append(reg_id)
            reg_corps[reg_id].add(corp_norm)
            reg_names[reg_id].add(corp_norm)

        # Signal 3: owner/officer mailing address (block high-frequency offices)
        if biz_addr and biz_zip and contact_type and contact_type.upper() in ADDRESS_CONTACT_TYPES:
            addr_key = f"{biz_addr.strip()}|{biz_zip.strip()}"
            if len(addr_key) >= 10:
                addr_index[addr_key].append(reg_id)

    # Auto-compute blocked addresses BEFORE applying address signal.
    blocked_addrs = {k for k, v in addr_index.items() if len(set(v)) >= ADDR_MAX_USERS}
    if blocked_addrs:
        logger.info(
            f"  Address blocklist: {len(blocked_addrs)} high-frequency addresses blocked "
            f"(each used by ≥{ADDR_MAX_USERS} registrations)"
        )
        for k in blocked_addrs:
            del addr_index[k]

    # Auto-compute blocked corporations BEFORE applying corp signal.
    # Corporations on too many registrations are institutional (banks, servicers)
    # and must NOT anchor clusters of unrelated landlords.
    blocked_corps = {k for k, v in corp_index.items() if len(set(v)) >= CORP_MAX_USERS}
    if blocked_corps:
        logger.info(
            f"  Corp blocklist: {len(blocked_corps)} high-frequency corps blocked "
            f"(each used by ≥{CORP_MAX_USERS} registrations): "
            + ", ".join(sorted(blocked_corps)[:10])
        )
        for k in blocked_corps:
            del corp_index[k]

    # ------------------------------------------------------------------
    # 4. Initial Union-Find (all three signals)
    # ------------------------------------------------------------------
    logger.info("Clustering (pass 1: all signals) …")
    uf = UnionFind()

    def merge_index(index: dict, label: str):
        merges = 0
        for key, reg_ids in index.items():
            unique = list(set(reg_ids))
            if len(unique) < 2:
                continue
            anchor = unique[0]
            for other in unique[1:]:
                uf.union(anchor, other)
                merges += 1
        logger.info(f"  {label}: {len(index):,} keys → {merges:,} merges")

    merge_index(person_index, "person-name signal")
    merge_index(corp_index,   "corp-name signal")
    merge_index(addr_index,   "address signal")
    merge_index(bbl_index,    "same-BBL registration signal")

    # Ensure every registration has a node
    for reg_id in reg_bbl:
        uf.find(reg_id)

    components = uf.components()
    logger.info(f"  Pass 1: {len(components):,} clusters found.")

    # ------------------------------------------------------------------
    # 5. Mega-net detection & name-only re-splitting
    # ------------------------------------------------------------------
    def member_bbl_count(members: list[str]) -> int:
        return len({reg_bbl.get(reg_id) for reg_id in members if reg_bbl.get(reg_id)})

    mega_count = sum(1 for members in components.values() if member_bbl_count(members) > max_buildings)
    logger.info(f"  Mega-nets (>{max_buildings} BBLs): {mega_count}")

    if mega_count > 0:
        logger.info(f"Pass 2: name-only re-clustering for {mega_count} mega-net(s) …")
        final_components: list[list[str]] = []

        for root, members in components.items():
            if member_bbl_count(members) <= max_buildings:
                final_components.append(members)
                continue

            # Re-cluster this mega-net with name signal only
            sub_clusters = split_meganet(members, reg_names, max_buildings)
            split_count  = len(sub_clusters)
            max_sub      = max(member_bbl_count(s) for s in sub_clusters)
            logger.info(
                f"  Mega-net '{root[:40]}' ({member_bbl_count(members)} BBLs) → "
                f"{split_count} sub-clusters (largest: {max_sub} BBLs)"
            )

            # Recursively check if any sub-cluster is still oversized.
            # If it still can't be split by name, break it into singletons
            # (one network per registration). A real portfolio will be
            # re-unified on the next ingest cycle; a fake mega-net disappears.
            for sub in sub_clusters:
                if member_bbl_count(sub) > max_buildings:
                    logger.warning(
                        f"    Sub-cluster still >{max_buildings} after name-only split "
                        f"({member_bbl_count(sub)} BBLs) — breaking into {len(sub)} singletons."
                    )
                    # Each registration becomes its own isolated network
                    for singleton in sub:
                        final_components.append([singleton])
                else:
                    final_components.append(sub)

        logger.info(
            f"  After splitting: {len(final_components):,} total clusters "
            f"(was {len(components):,})"
        )
    else:
        final_components = list(components.values())

    # Mega-net splitting can break a multi-registration BBL into more than one
    # component. Re-merge by BBL so parcel-level code/enforcement stats are not
    # attributed to multiple separate network cards.
    if final_components:
        parent = list(range(len(final_components)))

        def comp_find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        def comp_union(a: int, b: int):
            ra, rb = comp_find(a), comp_find(b)
            if ra != rb:
                parent[rb] = ra

        bbl_owner: dict[str, int] = {}
        for idx, members in enumerate(final_components):
            for bbl in {reg_bbl.get(reg_id) for reg_id in members if reg_bbl.get(reg_id)}:
                if bbl in bbl_owner:
                    comp_union(idx, bbl_owner[bbl])
                else:
                    bbl_owner[bbl] = idx

        merged_components: dict[int, list[str]] = defaultdict(list)
        for idx, members in enumerate(final_components):
            merged_components[comp_find(idx)].extend(members)
        if len(merged_components) != len(final_components):
            logger.info(
                f"  BBL re-merge: {len(final_components):,} clusters → "
                f"{len(merged_components):,} clusters"
            )
        final_components = list(merged_components.values())

    # ------------------------------------------------------------------
    # 6. Optional: load PLUTO unit counts per BBL
    # ------------------------------------------------------------------
    bbl_units: dict[str, int] = {}
    if not skip_property_join:
        logger.info("Loading PLUTO unit counts …")
        cur.execute("SELECT bbl, COALESCE(units_res, 0) FROM nyc_properties WHERE bbl IS NOT NULL")
        for bbl, units in cur.fetchall():
            bbl_units[bbl] = int(units or 0)
        logger.info(f"  {len(bbl_units):,} PLUTO lots loaded.")

    # ------------------------------------------------------------------
    # 7. Build network rows
    # ------------------------------------------------------------------
    logger.info("Building network rows …")
    BUSINESS_TOKENS = {"LLC", "CORP", "INC", "REALTY", "PROPERTIES", "HOLDINGS",
                       "MANAGEMENT", "ASSOCIATES", "PARTNERS", "GROUP", "TRUST", "LP"}

    network_rows = []
    for members in final_components:
        all_names: set = set()
        all_addrs: set = set()
        all_bbls:  set = set()
        bbl_borough: dict[str, str] = {}

        for reg_id in members:
            all_names.update(reg_names.get(reg_id, set()))
            all_addrs.update(reg_addrs.get(reg_id, set()))
            bbl     = reg_bbl.get(reg_id)
            borough = reg_borough.get(reg_id)
            if bbl:
                all_bbls.add(bbl)
                if borough and bbl not in bbl_borough:
                    bbl_borough[bbl] = borough

        borough_counts: dict[str, int] = defaultdict(int)
        for borough in bbl_borough.values():
            borough_counts[borough] += 1

        unit_count = sum(bbl_units.get(bbl, 0) for bbl in all_bbls)

        # Connection signals: the shared names/corps that actually stitched this
        # cluster together. Person names that appear on 2+ registrations within
        # this cluster are the most important evidence of real connection.
        people_counts = defaultdict(int)
        corps_counts = defaultdict(int)

        for reg_id in members:
            for p in reg_people.get(reg_id, []):
                people_counts[p] += 1
            for c in reg_corps.get(reg_id, []):
                if c not in blocked_corps:
                    corps_counts[c] += 1

        linking_people = sorted(
            [name for name, count in people_counts.items() if count >= 2],
            key=lambda n: -people_counts[n]
        )[:10]
        linking_corps = sorted(
            [corp for corp, count in corps_counts.items() if count >= 2],
            key=lambda c: -corps_counts[c]
        )[:10]

        clean_names = sorted(
            [n for n in all_names if n and len(n) >= 4],
            key=lambda n: -len(n)
        )
        display_name = (
            linking_people[0]
            if linking_people
            else (linking_corps[0] if linking_corps else (clean_names[0] if clean_names else (members[0] if members else "UNKNOWN")))
        )

        # Anchor type heuristic
        root_words = set(display_name.upper().split())
        anchor_type = "corp" if root_words & BUSINESS_TOKENS else "person"

        # Use a stable, deterministic network_key
        network_key = min(members)  # smallest registration_id string → stable

        connection_signals = json.dumps({
            "people": linking_people,
            "corps":  linking_corps,
            "signal_scope": "owner/officer contacts; agent-only management signals excluded",
        })

        network_rows.append((
            network_key,
            anchor_type,
            display_name,
            sorted(all_names)[:200],
            sorted(all_addrs)[:50],
            sorted(members),
            sorted(all_bbls),
            len(all_bbls) or len(members),
            unit_count,
            json.dumps(dict(borough_counts)),
            connection_signals,
        ))

    logger.info(f"  {len(network_rows):,} networks to write.")

    # ------------------------------------------------------------------
    # 8. Write to DB
    # ------------------------------------------------------------------
    # Ensure connection_signals column exists (safe to run on existing table)
    cur.execute("""
        ALTER TABLE nyc_networks
        ADD COLUMN IF NOT EXISTS connection_signals JSONB DEFAULT '{}'::jsonb;
    """)
    conn.commit()

    # Self-healing: make sure the id sequence and default on id exist
    cur.execute("""
        CREATE SEQUENCE IF NOT EXISTS nyc_networks_id_seq;
        ALTER TABLE nyc_networks ALTER COLUMN id SET DEFAULT nextval('nyc_networks_id_seq');
        ALTER SEQUENCE nyc_networks_id_seq OWNED BY nyc_networks.id;
    """)
    conn.commit()

    logger.info("Writing to nyc_networks …")
    cur.execute("BEGIN;")
    cur.execute("TRUNCATE TABLE nyc_networks CASCADE;")

    insert_sql = """
        INSERT INTO nyc_networks (
            network_key, anchor_type, display_name,
            member_names, member_addresses,
            registration_ids, bbl_list,
            building_count, unit_count, borough_summary,
            connection_signals
        ) VALUES %s;
    """

    batch_size = 5_000
    total_written = 0
    for i in range(0, len(network_rows), batch_size):
        batch = network_rows[i : i + batch_size]
        execute_values(cur, insert_sql, batch)
        total_written += len(batch)

    conn.commit()
    logger.info(f"=== NYC network build complete: {total_written:,} networks ===")

    # ------------------------------------------------------------------
    # 9. Summary stats
    # ------------------------------------------------------------------
    cur.execute("SELECT COUNT(*), SUM(building_count), SUM(unit_count) FROM nyc_networks")
    net_count, bld_total, unit_total = cur.fetchone()
    logger.info(
        f"Summary — Networks: {net_count:,} | "
        f"Buildings: {int(bld_total or 0):,} | "
        f"Residential Units: {int(unit_total or 0):,}"
    )

    # Mega-net check
    cur.execute("SELECT COUNT(*) FROM nyc_networks WHERE building_count > %s", (max_buildings,))
    remaining_mega = cur.fetchone()[0]
    if remaining_mega:
        logger.warning(f"⚠️  {remaining_mega} network(s) still >{max_buildings} buildings after splitting.")
    else:
        logger.info(f"✅  No mega-nets remain above {max_buildings} building threshold.")

    cur.execute("""
        SELECT display_name, building_count, unit_count, borough_summary
        FROM nyc_networks
        ORDER BY unit_count DESC NULLS LAST
        LIMIT 15
    """)
    logger.info("Top 15 networks by unit count:")
    for name, blds, units, boroughs in cur.fetchall():
        logger.info(f"  {name[:55]:<57} {int(blds or 0):>5} bldgs  {int(units or 0):>7} units  {boroughs}")

    update_network_status(conn, "success", {
        "message": "NYC owner-contact networks rebuilt",
        "networks": int(total_written or 0),
        "buildings": int(bld_total or 0),
        "units": int(unit_total or 0),
    })

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build NYC ownership networks from HPD contact clusters.")
    parser.add_argument(
        "--skip-property-join",
        action="store_true",
        help="Skip the PLUTO join (faster, but unit counts will be 0).",
    )
    parser.add_argument(
        "--max-buildings",
        type=int,
        default=DEFAULT_MAX_BUILDINGS,
        help=f"Mega-net threshold — clusters above this are re-split name-only (default: {DEFAULT_MAX_BUILDINGS}).",
    )
    args = parser.parse_args()
    build_nyc_networks(
        skip_property_join=args.skip_property_join,
        max_buildings=args.max_buildings,
    )
