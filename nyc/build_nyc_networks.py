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
import re
import sys
import argparse
import logging
import json
from collections import defaultdict
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.shared_utils import (
    THIRD_PARTY_ENTITY_REGEX_BODY,
    compose_address,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    load_registrar_address_substrings,
    normalize_mailing_address_coarse,
    strip_benign_care_of_prefix,
)

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

# Mega-net address-link threshold — any Pass-1 component with more buildings than
# this has its address-only links stripped and is re-clustered on verified person/corp
# ownership names only. Genuine NYC landlord portfolios verified by person/corp names
# may legitimately exceed 300 buildings (up to MAX_VERIFIED_PORTFOLIO_BBLS).
DEFAULT_MAX_BUILDINGS = 300
MAX_VERIFIED_PORTFOLIO_BBLS = 2500

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

# Corporations used by this many or more registrations are treated as unflagged
# public/institutional umbrellas and excluded from corp_index. Legitimate multi-building
# NYC rental LLCs/PACT entities (e.g. Townhouse Rental LLC = 248 BBLs, NYC PACT = 115 regs)
# sit below 500, while banks/servicers/TPT are filtered explicitly via regex & blocklists.
CORP_MAX_USERS = 500

# Private shareholder co-op / condominium corporate patterns (intentionally excludes HDFC
# so community affordable housing operators like MHANY, RiseBoro, and Fordham Bedford
# retain their HDFC portfolios, while third-party co-op/condo managing agents like AKAM,
# The Andrews Organization, and Daisy Management do not merge unrelated boards).
PRIVATE_COOP_CONDO_CORP_RE = re.compile(
    r"\b(?:CONDO|CONDOMINIUM|COOPERATIVE|CO-OP|"
    r"OWNERS\s+CORP(?:ORATION|ORATED)?|"
    r"OWNERS\s+INC(?:ORPORATED)?|"
    r"TENANTS\s+CORP(?:ORATION|ORATED)?|"
    r"APARTMENTS?\s+CORP(?:ORATION|ORATED)?)\b",
    re.IGNORECASE,
)

# Nonprofit intermediary corporations that hold title on behalf of NYC HPD
# (e.g. Third Party Transfer program) before transferring to community
# development organizations.  They appear as CorporateOwner on registrations
# managed by completely separate nonprofits (MHANY, Banana Kelly, etc.) and
# must NOT be used as ownership-linking signals — while preserving the real
# community operator's HeadOfficer/CorporateOwner signal on those buildings.
TPT_INTERMEDIARY_CORPS = {
    "NEIGHBORHOOD RESTORE HDFC",
    "NEIGHBORHOOD RESTORE",
    "NEIGHBORHOOD RENEWAL HDFC",
    "RESTORING COMMUNITIES HDFC",
    "RESTORING URBAN NEIGHBORHOODS LLC",
    "PRESERVING CITY NEIGHBORHOODS HDFC",
    "JOE NYC CLUSTER LLC",
}

NONPROFIT_INTERMEDIARY_CORPS = TPT_INTERMEDIARY_CORPS | {
    "CITY OF NEW YORK",
    "CITY OF NY",
    "NYC HPD",
    "NEW YORK CITY HOUSING AUTHORITY",
    "NYCHA",
}

NYC_INSTITUTIONAL_PATTERN = re.compile(
    rf"{THIRD_PARTY_ENTITY_REGEX_BODY}|"
    r"CITY OF NEW YORK|CITY OF NY|\bNYC HPD\b|DEPARTMENT OF HOUSING PRESERVATION|"
    r"DEPT OF HOUSING PRESERVATION|\bDAMP/?TIL\b|DAMPTIL|NEW YORK CITY HOUSING|"
    r"\bNYCHA\b|NYC DEPARTMENT OF|NEIGHBORHOOD RESTORE|RESTORING COMMUNITIES|"
    r"RESTORING URBAN NEIGHBORHOODS|PRESERVING CITY NEIGHBORHOODS|\bDEPT\s+VPR\b",
    re.IGNORECASE,
)

# Queens/Bronx grid addresses are written both hyphenated and solid
# ('70-33 260th Street' vs '7033 260 Street'), which are the same building.
_HOUSE_NUMBER_RANGE = re.compile(r'^(\d+)-(\d+)\b')


def normalize_contact_address(street, city, state, zip_code) -> Optional[str]:
    """Build a building-level address key for HPD owner/officer contacts.

    HPD stores the address as raw '<house> <street> <apartment>' with no
    normalization, so '188-02 64TH AVE', '188-02 64TH. AVE.' and
    '188-02 64 AVENUE' were three different keys for one office, and a suite
    number kept two contacts in the same building apart. Normalizing first also
    makes the ADDR_MAX_USERS frequency blocklist count whole offices rather
    than individual spellings.
    """
    if not street:
        return None
    cleaned_street = strip_benign_care_of_prefix(street) or street
    composed = compose_address(cleaned_street, city, state)
    norm = normalize_mailing_address_coarse(composed)
    if not norm:
        return None
    norm = _HOUSE_NUMBER_RANGE.sub(lambda m: f"{m.group(1)}{m.group(2)}", norm, count=1)
    if len(norm) < 6:
        return None
    # ZIP stays a separate key component: it is the most reliable NYC
    # discriminator when a street name repeats across boroughs.
    return f"{norm}|{(zip_code or '').strip()[:5]}"


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
            c.business_zip,
            c.business_city,
            c.business_state
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

    registrar_substrings = load_registrar_address_substrings(conn)

    # Pre-pass: identify pure non-landlord 3rd-party registrations (foreclosure banks,
    # RMBS trustees, mortgage servicers, REO field vendors, or municipal housing agencies)
    # that have ZERO private CorporateOwner/IndividualOwner/JointOwner contacts, so REO
    # employees listed as HeadOfficer (e.g. VPR coordinators at LoanCare / US Bank Trust /
    # Nationstar / Guardian Asset Management) never form fake landlord networks.
    # TPT intermediaries (e.g. Neighborhood Restore HDFC) are intentionally NOT marked as
    # pure excluded owners so community operators (Banana Kelly / MHANY) keep their buildings.
    reg_has_excluded_owner: set[str] = set()
    reg_has_private_owner: set[str] = set()
    for reg_id, contact_type, name_norm, corp_norm, *_ in contacts:
        if not reg_id or reg_id not in reg_bbl:
            continue
        ctype = (contact_type or "").upper()
        if ctype in CORP_CONTACT_TYPES and corp_norm and len(corp_norm) >= 4:
            if corp_norm in TPT_INTERMEDIARY_CORPS:
                continue
            if is_excluded_third_party_entity(corp_norm) or NYC_INSTITUTIONAL_PATTERN.search(corp_norm):
                reg_has_excluded_owner.add(reg_id)
            else:
                reg_has_private_owner.add(reg_id)
        elif ctype in {"INDIVIDUALOWNER", "JOINTOWNER"} and name_norm and name_norm not in NOISE_NAMES:
            if is_excluded_third_party_entity(name_norm) or NYC_INSTITUTIONAL_PATTERN.search(name_norm):
                reg_has_excluded_owner.add(reg_id)
            else:
                reg_has_private_owner.add(reg_id)
        elif ctype == "HEADOFFICER":
            if (
                corp_norm
                and corp_norm not in TPT_INTERMEDIARY_CORPS
                and (is_excluded_third_party_entity(corp_norm) or NYC_INSTITUTIONAL_PATTERN.search(corp_norm))
            ) or (
                name_norm
                and (is_excluded_third_party_entity(name_norm) or NYC_INSTITUTIONAL_PATTERN.search(name_norm))
            ):
                reg_has_excluded_owner.add(reg_id)

    excluded_reg_ids = reg_has_excluded_owner - reg_has_private_owner
    if excluded_reg_ids:
        logger.info(
            f"  Excluded {len(excluded_reg_ids):,} pure 3rd-party / REO / government HPD registrations "
            "from landlord network clustering."
        )
        for rid in excluded_reg_ids:
            bbl_val = reg_bbl.pop(rid, None)
            reg_borough.pop(rid, None)
            if bbl_val and bbl_val in bbl_index:
                bbl_index[bbl_val] = [x for x in bbl_index[bbl_val] if x != rid]
                if not bbl_index[bbl_val]:
                    del bbl_index[bbl_val]

    for reg_id, contact_type, name_norm, corp_norm, biz_addr, biz_zip, biz_city, biz_state in contacts:
        if not reg_id or reg_id not in reg_bbl:
            continue

        if biz_addr and biz_zip:
            reg_addrs[reg_id].add(f"{biz_addr.strip()} {biz_zip.strip()}")

        # Signal 1: person name (officer types only, excluding municipal/DAMP-TIL/3rd-party placeholders)
        if name_norm and contact_type and contact_type.upper() in PERSON_CONTACT_TYPES:
            if (
                len(name_norm) >= 5
                and name_norm not in NOISE_NAMES
                and not NYC_INSTITUTIONAL_PATTERN.search(name_norm)
                and not is_excluded_third_party_entity(name_norm)
            ):
                person_index[name_norm].append(reg_id)
                reg_people[reg_id].add(name_norm)
                reg_names[reg_id].add(name_norm)

        # Signal 2: owner corp name. AGENT corporations are management signals,
        # not ownership signals, so they should not stitch landlord networks.
        if (
            corp_norm
            and len(corp_norm) >= 4
            and contact_type
            and contact_type.upper() in CORP_CONTACT_TYPES
            and not NYC_INSTITUTIONAL_PATTERN.search(corp_norm)
            and not is_excluded_third_party_entity(corp_norm)
        ):
            corp_index[corp_norm].append(reg_id)
            reg_corps[reg_id].add(corp_norm)
            reg_names[reg_id].add(corp_norm)

        # Signal 3: owner/officer mailing address (block high-frequency offices, servicers & registrars).
        # The key is normalized to building level so that spelling variants and
        # suite numbers do not split one office into many weak keys.
        if biz_addr and biz_zip and contact_type and contact_type.upper() in ADDRESS_CONTACT_TYPES:
            raw_composed = compose_address(biz_addr, biz_city, biz_state, biz_zip)
            addr_key = normalize_contact_address(biz_addr, biz_city, biz_state, biz_zip)
            if addr_key and not is_excluded_third_party_address(
                raw_composed, addr_key.split("|")[0], registrar_substrings
            ):
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

    # Remove nonprofit intermediary corps (e.g. Neighborhood Restore HDFC) that
    # hold title on behalf of HPD and transfer to separate community orgs.
    intermediary_hits = NONPROFIT_INTERMEDIARY_CORPS & set(corp_index.keys())
    if intermediary_hits:
        logger.info(
            f"  Nonprofit intermediary blocklist: removing {len(intermediary_hits)} corps: "
            + ", ".join(sorted(intermediary_hits))
        )
        blocked_corps |= intermediary_hits
        for k in intermediary_hits:
            del corp_index[k]

    # Auto-compute blocked person names:
    #   (A) TPT intermediary officers (e.g. Neighborhood Restore officers) whose
    #       registrations are predominantly TPT intermediary corporations.
    #   (B) Third-party private co-op/condominium managing agents (e.g. AKAM Associates,
    #       The Andrews Organization, Daisy Management, MRE, Habitat, Leiter, Impact)
    #       signing as HeadOfficer across ≥10 distinct CorporateOwner entities where ≥5
    #       (and ≥35%) are shareholder co-ops or condominiums (excluding HDFCs so real
    #       nonprofit housing operators like MHANY, RiseBoro, and Fordham Bedford stay intact).
    raw_reg_owner_corps: dict[str, set[str]] = defaultdict(set)
    for reg_id, contact_type, _, corp_norm, *_ in contacts:
        if reg_id and reg_id in reg_bbl and corp_norm and contact_type and contact_type.upper() in CORP_CONTACT_TYPES:
            raw_reg_owner_corps[reg_id].add(corp_norm)

    person_owner_corps: dict[str, set[str]] = defaultdict(set)
    person_tpt_regs: dict[str, set[str]] = defaultdict(set)
    person_total_regs: dict[str, set[str]] = defaultdict(set)
    for p_name, rids in list(person_index.items()):
        unique_rids = set(rids)
        person_total_regs[p_name] = unique_rids
        for rid in unique_rids:
            owners = raw_reg_owner_corps.get(rid, set())
            person_owner_corps[p_name].update(owners)
            if owners and owners <= NONPROFIT_INTERMEDIARY_CORPS:
                person_tpt_regs[p_name].add(rid)

    blocked_people = set()
    for p_name, total_rids in person_total_regs.items():
        if len(total_rids) >= 10 and (len(person_tpt_regs[p_name]) / len(total_rids)) >= 0.5:
            blocked_people.add(p_name)
            continue
        corps = person_owner_corps.get(p_name, set()) - NONPROFIT_INTERMEDIARY_CORPS
        if len(corps) >= 10:
            coop_condo_corps = {c for c in corps if PRIVATE_COOP_CONDO_CORP_RE.search(c)}
            if len(coop_condo_corps) >= 5 and (len(coop_condo_corps) / len(corps)) >= 0.35:
                blocked_people.add(p_name)

    if blocked_people:
        logger.info(
            f"  Managing Agent / TPT Person blocklist: {len(blocked_people)} names blocked "
            f"(co-op/condo managing agents & TPT intermediary officers): "
            + ", ".join(sorted(blocked_people)[:12])
        )
        for k in blocked_people:
            del person_index[k]

    # CRITICAL: Prune blocked corps and managing-agent people from reg_names so
    # split_meganet does not re-merge mega-clusters using blocked signals!
    all_blocked_names = blocked_corps | blocked_people
    if all_blocked_names:
        for reg_id in list(reg_names.keys()):
            reg_names[reg_id].difference_update(all_blocked_names)
            reg_people[reg_id].difference_update(blocked_people)
            reg_corps[reg_id].difference_update(blocked_corps)

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
    logger.info(f"  Address mega-nets (>{max_buildings} BBLs): {mega_count}")

    verified_portfolio_cap = max(max_buildings, MAX_VERIFIED_PORTFOLIO_BBLS)

    if mega_count > 0:
        logger.info(f"Pass 2: stripping address-only links for {mega_count} >{max_buildings}-BBL cluster(s) …")
        final_components: list[list[str]] = []

        for root, members in components.items():
            if member_bbl_count(members) <= max_buildings:
                final_components.append(members)
                continue

            # Re-cluster this >max_buildings component with verified person/corp names only
            sub_clusters = split_meganet(members, reg_names, max_buildings)
            split_count  = len(sub_clusters)
            max_sub      = max(member_bbl_count(s) for s in sub_clusters)
            logger.info(
                f"  Mega-net '{root[:40]}' ({member_bbl_count(members)} BBLs) → "
                f"{split_count} sub-clusters (largest: {max_sub} BBLs)"
            )

            # Keep genuine large NYC portfolios that remain connected on verified
            # person/corp ownership signals (up to verified_portfolio_cap), only breaking
            # runaway unflagged public/institutional super-clusters above verified_portfolio_cap.
            for sub in sub_clusters:
                sub_bbls = member_bbl_count(sub)
                if sub_bbls > verified_portfolio_cap:
                    logger.warning(
                        f"    Sub-cluster exceeds hard safety cap >{verified_portfolio_cap} "
                        f"({sub_bbls} BBLs) — breaking into {len(sub)} singletons."
                    )
                    for singleton in sub:
                        final_components.append([singleton])
                else:
                    if sub_bbls > max_buildings:
                        logger.info(
                            f"    Preserving verified large NYC portfolio ({sub_bbls} BBLs) "
                            f"connected on non-agent person/corp signals."
                        )
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
        comp_bbl_sets: dict[int, set[str]] = {
            idx: {reg_bbl.get(reg_id) for reg_id in members if reg_bbl.get(reg_id)}
            for idx, members in enumerate(final_components)
        }

        def comp_find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        def comp_union(a: int, b: int):
            ra, rb = comp_find(a), comp_find(b)
            if ra != rb:
                combined = comp_bbl_sets[ra] | comp_bbl_sets[rb]
                allowed_cap = (
                    verified_portfolio_cap
                    if max(len(comp_bbl_sets[ra]), len(comp_bbl_sets[rb])) > max_buildings
                    else max_buildings
                )
                if len(combined) > allowed_cap:
                    return
                parent[rb] = ra
                comp_bbl_sets[ra] = combined

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
