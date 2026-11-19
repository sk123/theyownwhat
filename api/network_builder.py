# network_builder.py
import os
import re
import time
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from collections import Counter, deque, defaultdict
import logging
import sys

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared_utils import (
    THIRD_PARTY_ENTITY_REGEX_BODY,
    _STREET_FOLLOWER_LOOKAHEAD,
    normalize_business_name,
    normalize_person_name,
    normalize_mailing_address,
    normalize_mailing_address_coarse,
    first_normalized_mailing_address,
    is_ignored_shared_address,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    strip_benign_care_of_prefix,
    canonicalize_person_name,
    canonicalize_business_name,
    looks_like_business_name,
    looks_like_person_owner,
    get_name_variations,
    get_email_match_key,
    compose_address,
    load_registrar_address_substrings,
    is_registrar_address,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
MAX_DEPTH = 4 # Standard depth for network discovery
EMAIL_HUB_MIN_BUSINESSES = int(os.environ.get("EMAIL_HUB_MIN_BUSINESSES", "10"))
EMAIL_HUB_MIN_DISPERSED_HQS = int(os.environ.get("EMAIL_HUB_MIN_DISPERSED_HQS", "6"))
EMAIL_HUB_TOP2_HQ_MAX_SHARE = float(os.environ.get("EMAIL_HUB_TOP2_HQ_MAX_SHARE", "0.35"))
EMAIL_HUB_MIN_DISPERSED_SURNAMES = int(os.environ.get("EMAIL_HUB_MIN_DISPERSED_SURNAMES", "8"))
EMAIL_HUB_TOP2_SURNAME_MAX_SHARE = float(os.environ.get("EMAIL_HUB_TOP2_SURNAME_MAX_SHARE", "0.30"))
AMBIGUOUS_PRINCIPAL_MIN_BUSINESSES = int(os.environ.get("AMBIGUOUS_PRINCIPAL_MIN_BUSINESSES", "12"))
AMBIGUOUS_PRINCIPAL_MAX_OWNING_BUSINESSES = int(os.environ.get("AMBIGUOUS_PRINCIPAL_MAX_OWNING_BUSINESSES", "3"))
AMBIGUOUS_PRINCIPAL_MIN_EMAIL_DOMAINS = int(os.environ.get("AMBIGUOUS_PRINCIPAL_MIN_EMAIL_DOMAINS", "5"))
AMBIGUOUS_PRINCIPAL_MIN_MAIL_ADDRESSES = int(os.environ.get("AMBIGUOUS_PRINCIPAL_MIN_MAIL_ADDRESSES", "8"))
AMBIGUOUS_PRINCIPAL_TOP_EMAIL_SHARE = float(os.environ.get("AMBIGUOUS_PRINCIPAL_TOP_EMAIL_SHARE", "0.35"))
AMBIGUOUS_PRINCIPAL_MAIL_ADDRESS_SHARE = float(os.environ.get("AMBIGUOUS_PRINCIPAL_MAIL_ADDRESS_SHARE", "0.60"))
MULTI_UNIT_BUILDING_MIN_DISTINCT_UNITS = int(os.environ.get("MULTI_UNIT_BUILDING_MIN_DISTINCT_UNITS", "3"))
# Quota-free intermediary dispersion thresholds (no hard entity-count ceiling):
# An unlisted address is quarantined ONLY when >=6 mutually disjoint principal/email
# islands share it with no dominant operator (<35% share), OR when >=85% of >=30
# registered businesses at that address own zero real estate (commercial RA / maildrop).
DISPERSED_HUB_MIN_DISJOINT_ISLANDS = int(os.environ.get("DISPERSED_HUB_MIN_DISJOINT_ISLANDS", "6"))
DISPERSED_HUB_MAX_DOMINANT_SHARE = float(os.environ.get("DISPERSED_HUB_MAX_DOMINANT_SHARE", "0.35"))
NON_OWNING_RA_MIN_TOTAL_BIZ = int(os.environ.get("NON_OWNING_RA_MIN_TOTAL_BIZ", "30"))
NON_OWNING_RA_MAX_OWNING_RATIO = float(os.environ.get("NON_OWNING_RA_MAX_OWNING_RATIO", "0.15"))


def _is_dispersed_intermediary_business_hub(
    uniq_biz_ids,
    graph,
    node_to_int,
    *,
    total_registered_at_coarse: int = 0,
) -> bool:
    """Distinguish an unlisted commercial agent / CPA / attorney / virtual-mailbox hub
    from a genuine large landlord HQ (even with 500+ single-purpose LLCs) using graph
    cohesion vs. disjoint-principal dispersion and real-estate-owning footprint ratio,
    without imposing any artificial entity-count quota."""
    n_owning = len(uniq_biz_ids)
    if n_owning < 8:
        return False

    b_nodes = [node_to_int[("business", b)] for b in uniq_biz_ids if ("business", b) in node_to_int]
    connected_b = [bn for bn in b_nodes if graph.get(bn)]

    if len(connected_b) >= 8:
        par = {bn: bn for bn in connected_b}

        def _find(x):
            while par[x] != x:
                par[x] = par[par[x]]
                x = par[x]
            return x

        nbr_anchor = {}
        for bn in connected_b:
            for nb in graph.get(bn, ()):
                prev = nbr_anchor.get(nb)
                if prev is None:
                    nbr_anchor[nb] = bn
                else:
                    ra, rb = _find(prev), _find(bn)
                    if ra != rb:
                        par[ra] = rb

        island_sizes = defaultdict(int)
        for bn in connected_b:
            island_sizes[_find(bn)] += 1
        n_islands = len(island_sizes)
        top_share = max(island_sizes.values()) / len(connected_b)
        if n_islands >= DISPERSED_HUB_MIN_DISJOINT_ISLANDS and top_share < DISPERSED_HUB_MAX_DOMINANT_SHARE:
            return True
        return False

    # Anonymous / out-of-state entities lacking local officer links: check whether the
    # address is overwhelmingly a non-real-estate corporate filing hub (>85% non-owning).
    if (
        total_registered_at_coarse >= NON_OWNING_RA_MIN_TOTAL_BIZ
        and (n_owning / total_registered_at_coarse) < NON_OWNING_RA_MAX_OWNING_RATIO
    ):
        return True
    return False

# Placeholder/Generic names to skip during linking to prevent bad merges
SKIP_NAMES = {
    'CURRENT OWNER', 'UNKNOWN OWNER', 'OCCUPANT', 'OWNER',
    'UNKNOWN', 'CT', 'CONNECTICUT', 'THE', 'INC', 'LLC', 'CORP',
    'USA', 'UNITED STATES', 'NO NAME', 'N/A', 'NA', 'NONE',
    'NO INFORMATION PROVIDED', 'NOT PROVIDED', 'VACANT', 'NULL',
    'NOT AVAILABLE', '[UNKNOWN]', 'CURRENT COMPANY OWNER', 'CURRENT COMPANY-OWNER',
    'SV', 'SURVIVORSHIP', 'SURV', 'TITLE', 'LIVING', 'TRUST', 'ESTATE',
    'CORPORATION', 'INCORPORATED', 'ASSOCIATION', 'COMPANY',
    'JT', 'TIC', 'TC', 'ET AL', 'LII', 'ETAL', 'REFERENCE ONLY'
}

# Generic single-token canonical business stems that collide with assessor
# overflow fragments (e.g. 'TITLE', '(SURV)', 'LIVING', '(TRUST)', 'CORP', 'INC').
GENERIC_BUSINESS_CANON_STOPWORDS = {
    '', 'TITLE', 'SURV', 'SURVIVORSHIP', 'LIVING', 'TRUST', 'TRUSTEE', 'TRUSTEES',
    'ESTATE', 'REALTY', 'HOLDINGS', 'HOLDING', 'PROPERTIES', 'PROPERTY',
    'MANAGEMENT', 'DEVELOPMENT', 'INVESTMENTS', 'INVESTMENT', 'PARTNERS',
    'PARTNERSHIP', 'ASSOCIATES', 'ASSOCIATION', 'COMPANY', 'CORPORATION',
    'INCORPORATED', 'LIMITED', 'GROUP', 'SERVICES', 'ENTERPRISES', 'ENTERPRISE',
    'VENTURES', 'OWNER', 'OWNERS', 'APARTMENTS', 'APARTMENT', 'HOMES', 'HOME',
    'HOUSING', 'LAND', 'BUILDING', 'BUILDINGS', 'CENTER', 'SYSTEM', 'SYSTEMS',
    'CEMETERY', 'CHURCH', 'BANK', 'LIFE', 'REVOCABLE', 'IRREVOCABLE', 'FAMILY',
    'INC', 'LLC', 'CORP', 'LTD', 'LP', 'LLP', 'THE', 'CT', 'NY', 'STATE', 'CITY', 'TOWN'
}

# Institutional/Service regex to catch pseudo-hubs (banks, mortgage servicers,
# RMBS trustees, registered agents, government, utilities, hospitals, universities,
# hospitality, etc.) and exclude them from the landlord ownership graph.
INSTITUTIONAL_PATTERN = re.compile(
    rf'{THIRD_PARTY_ENTITY_REGEX_BODY}|'
    r'\b(?:PREPARATORY|BOARDING|HIGH|ELEMENTARY|MIDDLE|GRAMMAR|PAROCHIAL|CHARTER)\s+SCHOOL\b|'
    rf'\bACADEMY{_STREET_FOLLOWER_LOOKAHEAD}\b|'
    rf'\bFEDERAL{_STREET_FOLLOWER_LOOKAHEAD}\b|'
    r'CITY BUFFALO|PERFECTING TITLE|REDEVELOPMENT|'
    r'SUFFOLK COUNTY|NASSAU COUNTY|WESTCHESTER COUNTY|ALBANY COUNTY|ERIE COUNTY|MONROE COUNTY|ONONDAGA COUNTY|'
    r'\b(?:MUNICIPAL|DEPARTMENT\s+OF|AUTHORITY)\b|'
    r'LAND TRUST|HUMAN SERVICES|FOUNDATION FOR .* LIVING|'
    r'SENIOR HOUSING|ELDERLY HOUSING|INTERFAITH HOUSING|PUBLIC HOUSING|'
    r'HOUSING CORPORATION|HOUSING ASSOCIATES|HOUSING, INC|HOUSING INC|'
    r'CONGREGATE HOMES|RETIREMENT COMMUNITY|'
    r'\b(?:HOSPITAL|HEALTHCARE|HEALTH\s+CENTER|CLINIC|NEW\s+SAMARITAN)\b|'
    r'\b(?:CATHOLIC|CEMETERY|NATURE\s+CONSERVANCY)\b|'
    r'\b(?:LODGING|HOSPITALITY|HOTEL|MOTEL)\b|\bINN\s+|\bSUITES\b|'
    r'WATER COMPANY|GAS SERVICES|POWER CORPORATION|POWER CORP|'
    r'NATIONAL FUEL|CENTRAL HUDSON|ROCHESTER GAS|NYSEG|'
    r'ORANGE AND ROCKLAND|LONG ISLAND POWER|'
    r'WATER UTILITY|SEWER |YANKEE GAS|SOUTHERN CONN.* GAS|AVANGRID|BERKSHIRE GAS|CONNECTICUT NATURAL GAS|'
    r'VERIZON|FRONTIER COMMUNICATIONS|SOUTHERN NEW ENGLAND TELEPHONE|RAILROAD',
    re.IGNORECASE
)

# Care O / Attention pattern to ignore property managers/mailing addresses
CARE_OF_PATTERN = re.compile(r'^\s*(C/O|CIO|CARE OF|ATTN|ATTENTION|%)\s+', re.IGNORECASE)

# --- Database Connection ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

# --- Logic 1: Property Linking ---
def link_properties_to_entities(conn):
    """Maps properties to business_id or principal_id using robust name matching."""
    logger.info("🔗 PHASE 1: Linking properties to entities...")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. Load Business Map
        b_map = {}
        b_canon_map = {}
        cur.execute("SELECT id, name FROM businesses")
        logger.info("  - Loading business names into memory...")
        b_count = 0
        for row in cur:
            b_count += 1
            if b_count % 100000 == 0:
                logger.info(f"    Loaded {b_count:,} businesses...")
            raw_bname = row['name'] or ''
            if not raw_bname or INSTITUTIONAL_PATTERN.search(raw_bname):
                continue
            norm = normalize_business_name(raw_bname)
            if not norm or norm in SKIP_NAMES or norm in GENERIC_BUSINESS_CANON_STOPWORDS:
                continue
            # Do not allow a bare human-name DBA record (without business suffixes)
            # to hijack homeowner parcels in Pass 1.
            if not looks_like_business_name(raw_bname) and looks_like_person_owner(raw_bname):
                continue
            b_map[norm] = row['id']

            # Add canonical version as secondary match key only when sufficiently
            # specific (never single-token generic words or bare person names).
            bc = canonicalize_business_name(raw_bname)
            if (
                bc
                and bc not in GENERIC_BUSINESS_CANON_STOPWORDS
                and (len(bc.split()) >= 2 or len(bc) >= 8)
                and not looks_like_person_owner(bc)
                and bc not in b_canon_map
            ):
                b_canon_map[bc] = row['id']

        logger.info(f"  - Business map loaded ({len(b_map):,} unique normal, {len(b_canon_map):,} unique canon).")

        # 2. Load Principal Map
        #
        # unique_principals is not deduplicated by name: ~11k names are shared by
        # ~55k rows (one name is held by 146 separate principal_ids). A plain
        # name -> id dict keeps whichever row happened to be scanned last, so a
        # property owned by "MARTIN WILLIAM" was attached to an arbitrary one of
        # 118 William Martins. Ambiguous names are recorded and then skipped:
        # leaving a property unlinked is far better than crediting it to the
        # wrong person on a public accountability site.
        p_map = {}
        p_ambiguous = set()
        p_mid_init = {}

        _ROLE_SUFFIX_RE = re.compile(r'\b(?:TRUSTEES?|TR|TRS|JR|SR|II|III|IV|ESQ|ESQUIRE|MD|PHD|CPA)\b')

        def _extract_middle_initial(raw_person_name: str) -> str:
            if not raw_person_name:
                return ""
            s = str(raw_person_name).upper().strip()
            if ',' in s:
                last_part, rest = s.split(',', 1)
                s = f"{rest.strip()} {last_part.strip()}"
            if ' & ' in s or ' AND ' in s or ' / ' in s:
                s = re.split(r'\s+(?:&|AND|/)\s+', s, maxsplit=1)[0]
            s = re.sub(r"[,.'`\"]", '', s)
            s = _ROLE_SUFFIX_RE.sub(' ', s)
            tokens = [t for t in s.split() if t]
            if len(tokens) == 3 and len(tokens[1]) == 1 and tokens[1].isalpha():
                return tokens[1]
            return ""

        def register_principal(key, pid):
            if not key:
                return
            existing = p_map.get(key)
            if existing is None:
                p_map[key] = pid
            elif existing != pid:
                p_ambiguous.add(key)

        cur.execute("SELECT principal_id, name_normalized, representative_name_c FROM unique_principals")
        logger.info("  - Loading principal names into memory...")
        p_count = 0
        for row in cur:
            p_count += 1
            if p_count % 100000 == 0:
                logger.info(f"    Loaded {p_count:,} principals...")
            pid_val = row['principal_id']
            rep_c = row.get('representative_name_c') or ''
            norm_p = row.get('name_normalized') or ''
            if INSTITUTIONAL_PATTERN.search(rep_c) or INSTITUTIONAL_PATTERN.search(norm_p):
                continue
            mid = _extract_middle_initial(rep_c)
            if mid:
                p_mid_init[pid_val] = mid
            # unique_principals.name_normalized is already fairly clean, but we canonicalize it
            register_principal(norm_p, pid_val)
            register_principal(canonicalize_person_name(norm_p), pid_val)

        for key in p_ambiguous:
            p_map.pop(key, None)

        def lookup_principal(key, owner_mid=""):
            """Resolve a name to a principal, refusing ambiguous names or conflicting middle initials."""
            if not key or key in p_ambiguous:
                return None
            cand = p_map.get(key)
            if cand is not None and owner_mid:
                cand_mid = p_mid_init.get(cand, "")
                if cand_mid and cand_mid != owner_mid:
                    return None
            return cand

        logger.info(
            f"  - Principal map loaded ({len(p_map):,} unambiguous names; "
            f"{len(p_ambiguous):,} ambiguous names withheld from matching)."
        )

        # Use a separate connection for the server-side cursor to avoid closure on commit
        read_conn = get_db_connection()
        try:
            cursor_name = f"prop_linker_{int(time.time())}"
            with read_conn.cursor(name=cursor_name, cursor_factory=RealDictCursor) as sc:
                sc.execute("SELECT id, owner, co_owner FROM properties")
                updates = []
                linked_count = 0
                count = 0

                # Fetch in chunks from the server-side cursor
                while True:
                    rows = sc.fetchmany(5000)
                    if not rows: break

                    for row in rows:
                        count += 1
                        if count % 100000 == 0:
                            logger.info(f"    Processed {count:,} properties...")

                        oname = row['owner'] or ''
                        cname = row['co_owner'] or ''

                        onorm = normalize_business_name(oname)
                        cnorm = normalize_business_name(cname)

                        # Institutional Filter: If owner is institutional, do not link to business/principal
                        # This effectively removes them from the network graph
                        if INSTITUTIONAL_PATTERN.search(oname) or INSTITUTIONAL_PATTERN.search(cname):
                            continue

                        # Care-Of Filter: If name starts with C/O, ignore it (it's likely a manager or mailing address)
                        # We only skip the specific field that matches.

                        skip_owner = bool(CARE_OF_PATTERN.match(oname))
                        skip_co_owner = bool(CARE_OF_PATTERN.match(cname))

                        # Pass 1: Primary Business matches
                        # Co-owner frequently holds overflow fragments ('(SURV)', 'TITLE', 'LIVING', 'INC');
                        # require co_owner to look like a real multi-token business name before matching.
                        co_owner_biz_ok = (
                            not skip_co_owner
                            and cnorm not in SKIP_NAMES
                            and looks_like_business_name(cname)
                            and len(canonicalize_business_name(cname).split()) >= 2
                        )
                        bid = None
                        if not skip_owner and onorm not in SKIP_NAMES:
                            bid = b_map.get(onorm)
                        if not bid and co_owner_biz_ok:
                            bid = b_map.get(cnorm)

                        # Pass 2: Canonical Business matches (Suffix-stripped)
                        # Only suffix-strip owner strings that actually look like business entities,
                        # so human homeowners ('DAVID, SMITH') never match 'DAVID SMITH, INC.'
                        if not bid:
                            if not skip_owner and onorm not in SKIP_NAMES and looks_like_business_name(oname):
                                bc_owner = canonicalize_business_name(oname)
                                if bc_owner and bc_owner not in GENERIC_BUSINESS_CANON_STOPWORDS:
                                    bid = b_canon_map.get(bc_owner)
                            if not bid and co_owner_biz_ok:
                                bc_co = canonicalize_business_name(cname)
                                if bc_co and bc_co not in GENERIC_BUSINESS_CANON_STOPWORDS:
                                    bid = b_canon_map.get(bc_co)

                        # Pass 3: Principal matches (with LAST FIRST ↔ FIRST LAST reversal)
                        pid = None
                        if not bid: # Only check principal if no business found
                            # Iterate over checking owner then co-owner, skipping if C/O matches
                            for raw_name, skip in [(oname, skip_owner), (cname, skip_co_owner)]:
                                if skip: continue
                                if pid: break # Found one already

                                owner_mid = _extract_middle_initial(raw_name)
                                stripped_role = re.sub(r'\b(?:TRUSTEES?|TR|TRS)\b', ' ', raw_name, flags=re.IGNORECASE)
                                name_clean = normalize_person_name(stripped_role)
                                if not name_clean:
                                    continue
                                # Direct match
                                pid = lookup_principal(name_clean, owner_mid)
                                # Canonical match
                                if not pid:
                                    pid = lookup_principal(canonicalize_person_name(name_clean), owner_mid)
                                # LAST FIRST → FIRST LAST reversal
                                if not pid:
                                    parts = name_clean.split()
                                    if len(parts) >= 2:
                                        # Try "LAST FIRST" → "FIRST LAST"
                                        reversed_name = f"{parts[-1]} {' '.join(parts[:-1])}"
                                        pid = lookup_principal(reversed_name, owner_mid)
                                        if not pid:
                                            pid = lookup_principal(canonicalize_person_name(reversed_name), owner_mid)
                                        # Try "FIRST LAST" → "LAST FIRST"
                                        if not pid:
                                            reversed_name2 = f"{' '.join(parts[1:])} {parts[0]}"
                                            pid = lookup_principal(reversed_name2, owner_mid)
                                            if not pid:
                                                pid = lookup_principal(canonicalize_person_name(reversed_name2), owner_mid)

                        if bid or pid:
                            updates.append((bid, pid, row['id']))
                            linked_count += 1

                        if len(updates) >= 5000:
                            with conn.cursor() as up_cur: # Use the main connection for updates
                                execute_values(up_cur, """
                                    UPDATE properties AS p
                                    SET business_id = v.bid, principal_id = v.pid
                                    FROM (VALUES %s) AS v(bid, pid, id)
                                    WHERE p.id = v.id
                                """, updates)
                            conn.commit()
                            updates = []

                if updates:
                    with conn.cursor() as up_cur:
                        execute_values(up_cur, """
                            UPDATE properties AS p
                            SET business_id = v.bid, principal_id = v.pid
                            FROM (VALUES %s) AS v(bid, pid, id)
                            WHERE p.id = v.id
                        """, updates)
                    conn.commit()
        finally:
            read_conn.close()

    logger.info(f"  - Linked {linked_count:,} out of {count:,} properties.")

# --- Logic 2: Graph Building ---
def build_graph(conn, skip_emails=False):
    """Builds the entity graph using shared attributes."""
    logger.info("🕸️ PHASE 2: Building graph (Integers)...")

    # Start IDs:
    # 0 = reserved
    # Positive ints only
    node_to_int = {}
    int_to_node = [] # Index is the int ID

    def get_id(node_tuple):
        if node_tuple not in node_to_int:
            node_to_int[node_tuple] = len(int_to_node)
            int_to_node.append(node_tuple)
        return node_to_int[node_tuple]

    # Use defaultdict(list) for adjacency list as it is lighter than set (handle dupes later or ignore)
    # Using arrays would be even better but lists are standard python.
    graph = defaultdict(set) # Using set to avoid edges duplication during build

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT normalized_name FROM principal_ignore_list")
        ignore_names_raw = {row['normalized_name'] for row in cur}
        # The ignore list stores names first-last ('ZENON LANKOWSKY') while
        # unique_principals.name_normalized is last-first ('LANKOWSKY ZENON').
        # normalize_person_name preserves word order, so comparing normalized
        # forms never matched. Compare word-sorted canonical forms instead,
        # which are order-independent.
        ignore_names = set(ignore_names_raw)
        for raw in ignore_names_raw:
            ignore_names.add(normalize_person_name(raw))
        ignore_canon = {canonicalize_person_name(raw) for raw in ignore_names_raw}
        ignore_canon.discard("")

        def is_ignored_principal(name):
            if not name:
                return False
            return name in ignore_names or canonicalize_person_name(name) in ignore_canon

        logger.info(
            f"  - Loaded {len(ignore_names_raw)} ignore list entries "
            f"({len(ignore_canon)} order-independent keys)."
        )

        # Reuse module-level INSTITUTIONAL_PATTERN so build_graph and link_properties_to_entities
        # share the exact same institutional/utility/government exclusions.
        WHITELIST_PATTERN = re.compile(r'GUREVITCH|MANDY|NETZ|ACME', re.IGNORECASE)

        SERVICE_PATTERN = re.compile(r'SERVICES|AGENT|PROPERTY MGMT', re.IGNORECASE)

        # --- LOAD PROPERTY-OWNING ENTITIES FIRST (needed for hub detection) ---
        logger.info("  - Loading Property-Owning Business List...")
        cur.execute("SELECT DISTINCT business_id FROM properties WHERE business_id IS NOT NULL")
        owning_business_ids = {row['business_id'] for row in cur}
        property_owning_business_ids = set(owning_business_ids)  # Snapshot before adding bridges
        logger.info(f"    Found {len(owning_business_ids):,} businesses with properties.")

        # Keep general matching constrained to businesses that directly own property.
        # Non-owning businesses are only used later for a narrow same-family
        # co-principal bridge between principals already active in this graph.
        logger.info("    General bridge businesses disabled; matching is limited to property-owning businesses.")

        # principals with direct property ownership should also be valid seeds/nodes
        cur.execute("SELECT DISTINCT principal_id FROM properties WHERE principal_id IS NOT NULL")
        owning_principal_ids = set()
        for r in cur:
            try: owning_principal_ids.add(int(r['principal_id']))
            except: pass
        logger.info(f"    Found {len(owning_principal_ids):,} principals with direct properties.")

        # No degree-based hub caps — rely on name-based institutional/service filters only.
        # The property-owning-only filter on line 293 naturally limits the graph.
        hub_principals = set()
        hub_businesses = set()
        logger.info("  - Hub detection: NO degree caps (name-based filters only).")

        # Ambiguous principal names are a different failure mode from true hubs:
        # common names like MICHAEL SULLIVAN or CLARK ROBERT can collapse many
        # unrelated people into one unique_principals row. Only suppress the
        # diffuse, low-property-owner cases; concentrated high-count operators
        # remain eligible for matching.
        ambiguous_principal_ids = set()
        attorney_organizer_re = re.compile(r'\b(?:ESQ|ESQUIRE)\b', re.IGNORECASE)
        logger.info("  - Detecting ambiguous common-name principal bridges...")
        cur.execute(
            """
            SELECT
                hp.principal_id,
                hp.name_normalized,
                hp.representative_name_c,
                pbl.business_id,
                LOWER(TRIM(COALESCE(b.business_email_address, ''))) AS email,
                SPLIT_PART(LOWER(TRIM(COALESCE(b.business_email_address, ''))), '@', 2) AS domain,
                b.mail_address, b.mail_city, b.mail_state, b.mail_zip,
                b.business_address, b.business_city, b.business_state, b.business_zip,
                EXISTS (
                    SELECT 1 FROM properties p
                    WHERE p.business_id = pbl.business_id
                ) AS owns_property
            FROM unique_principals hp
            JOIN principal_business_links pbl ON pbl.principal_id = hp.principal_id
            JOIN businesses b ON b.id = pbl.business_id
            WHERE hp.business_count >= %s
            """,
            (AMBIGUOUS_PRINCIPAL_MIN_BUSINESSES,),
        )
        hp_stats = {}
        for r in cur:
            pid_key = r["principal_id"]
            st = hp_stats.get(pid_key)
            if st is None:
                st = {
                    "name_normalized": r.get("name_normalized") or "",
                    "representative_name_c": r.get("representative_name_c") or "",
                    "businesses": set(),
                    "owning_businesses": set(),
                    "domains": set(),
                    "emails": defaultdict(int),
                    "mail_addresses": set(),
                }
                hp_stats[pid_key] = st
            bid_val = r["business_id"]
            st["businesses"].add(bid_val)
            if r["owns_property"]:
                st["owning_businesses"].add(bid_val)
            em = r.get("email") or ""
            if em:
                st["emails"][em] += 1
            dom = r.get("domain") or ""
            if dom:
                st["domains"].add(dom)
            norm_addr = first_normalized_mailing_address(
                compose_address(r["mail_address"], r["mail_city"], r["mail_state"], r["mail_zip"]),
                compose_address(r["business_address"], r["business_city"], r["business_state"], r["business_zip"]),
                coarse=True,
            )
            if norm_addr:
                st["mail_addresses"].add(norm_addr)

        for pid_key, st in hp_stats.items():
            name = st["name_normalized"] or st["representative_name_c"]
            rep_name = st["representative_name_c"] or name
            if WHITELIST_PATTERN.search(name) or WHITELIST_PATTERN.search(rep_name):
                continue
            linked = len(st["businesses"])
            if linked < AMBIGUOUS_PRINCIPAL_MIN_BUSINESSES:
                continue
            owning = len(st["owning_businesses"])
            domains = len(st["domains"])
            mail_addresses = len(st["mail_addresses"])
            top_email = max(st["emails"].values(), default=0)
            top_email_share = (top_email / linked) if linked else 0
            mail_address_share = (mail_addresses / linked) if linked else 0

            # Suppress explicit ESQ/Esquire corporate organizer hubs bridging unrelated client portfolios
            if attorney_organizer_re.search(rep_name) and linked >= 8 and mail_addresses >= 4 and top_email_share < 0.50:
                ambiguous_principal_ids.add(pid_key)
                continue

            diffuse_email_evidence = (
                domains >= AMBIGUOUS_PRINCIPAL_MIN_EMAIL_DOMAINS
                and top_email_share < AMBIGUOUS_PRINCIPAL_TOP_EMAIL_SHARE
            )
            diffuse_address_evidence = (
                mail_addresses >= AMBIGUOUS_PRINCIPAL_MIN_MAIL_ADDRESSES
                and mail_address_share >= AMBIGUOUS_PRINCIPAL_MAIL_ADDRESS_SHARE
            )
            low_property_owner_evidence = owning <= AMBIGUOUS_PRINCIPAL_MAX_OWNING_BUSINESSES

            if low_property_owner_evidence and (diffuse_email_evidence or diffuse_address_evidence):
                ambiguous_principal_ids.add(pid_key)

        if ambiguous_principal_ids:
            logger.info(
                "  - Suppressing %s ambiguous common-name / organizer principal bridge(s).",
                f"{len(ambiguous_principal_ids):,}",
            )

        # A. Principal-Business Links
        logger.info("  - Loading Principal-Business Links (Filtered to Owners)...")

        count = 0
        property_active_principal_ids = set()
        biz_to_surnames = defaultdict(set)
        with conn.cursor(name="network_pbl_links", cursor_factory=RealDictCursor) as link_cur:
            link_cur.itersize = 50000
            link_cur.execute("""
                SELECT pbl.business_id, pbl.principal_id,
                       up.name_normalized as p_name,
                       up.representative_name_c as p_rep_name,
                       b.name as b_name
                FROM principal_business_links pbl
                JOIN unique_principals up ON up.principal_id = pbl.principal_id
                JOIN businesses b ON b.id = pbl.business_id
            """)

            for row in link_cur:
                bid = row['business_id']
                pid = row['principal_id']

                # --- FILTER: Must be an owning business ---
                if bid not in owning_business_ids:
                    continue

                if is_ignored_principal(row['p_name']) or is_ignored_principal(row['p_rep_name']): continue
                if pid in ambiguous_principal_ids: continue
                if pid in hub_principals: continue
                if bid in hub_businesses: continue

                # Extra safety: name-based filter for institutional entities that might have low degree.
                # Check BOTH name_normalized and representative_name_c because name_normalized
                # transposes word order (e.g. 'AGENTS INC NATIONAL REGISTERED' vs 'NATIONAL REGISTERED AGENTS, INC.').
                is_whitelisted = (
                    WHITELIST_PATTERN.search(row['p_name'] or "")
                    or WHITELIST_PATTERN.search(row['p_rep_name'] or "")
                    or WHITELIST_PATTERN.search(row['b_name'] or "")
                )

                if not is_whitelisted:
                    if INSTITUTIONAL_PATTERN.search(row['p_name'] or "") or INSTITUTIONAL_PATTERN.search(row['p_rep_name'] or ""):
                        continue
                    if INSTITUTIONAL_PATTERN.search(row['b_name'] or ""):
                        continue

                u_node = ('business', bid)
                v_node = ('principal', pid)

                u = get_id(u_node)
                v = get_id(v_node)

                graph[u].add(v)
                graph[v].add(u)
                rep_norm = normalize_person_name(row['p_rep_name'] or row['p_name'] or "")
                rep_toks = rep_norm.split()
                if rep_toks:
                    biz_to_surnames[bid].add(rep_toks[-1])
                try:
                    pid_int = int(pid)
                    property_active_principal_ids.add(pid_int)
                except (TypeError, ValueError):
                    pass
                count += 1
                if count % 100000 == 0:
                    logger.debug(f"    Loaded {count:,} links...")

        # A1b. OpenCorporates Multi-Hop Foreign & Holding-Company Principal Bridges
        # Pierces Delaware / NY / MA / NJ / FL / WY parent & foreign-registered LLCs
        # owning CT properties (`owning_business_ids`) using `oc_*` when populated.
        try:
            cur.execute("SAVEPOINT sp_ct_oc_bridges")
            cur.execute("SELECT to_regclass('public.oc_companies') IS NOT NULL AS has_tbl")
            has_oc_tbl = bool(cur.fetchone()["has_tbl"])
            has_oc_rows = False
            if has_oc_tbl:
                cur.execute("SELECT EXISTS (SELECT 1 FROM oc_companies LIMIT 1) AS has_rows")
                has_oc_rows = bool(cur.fetchone()["has_rows"])

            if has_oc_rows and owning_business_ids:
                from oc_linker import (
                    OCCandidate,
                    demangle_assessor_owner,
                    load_oc_registered_agent_addresses,
                    resolve_oc_company_principals,
                    score_oc_candidates,
                )

                logger.info("  - Resolving OpenCorporates multi-hop foreign/parent bridges for CT owning businesses...")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ct_oc_principal_links (
                        business_id  TEXT NOT NULL,
                        principal_id INTEGER NOT NULL,
                        PRIMARY KEY (business_id, principal_id)
                    );
                    TRUNCATE TABLE ct_oc_principal_links;
                    """
                )

                # Build conservative unambiguous `canonicalize_person_name -> principal_id` map
                canon_to_pid = {}
                canon_ambig = set()
                cur.execute("SELECT principal_id, name_normalized, representative_name_c FROM unique_principals")
                for up_row in cur:
                    pid_cand = up_row["principal_id"]
                    if pid_cand in ambiguous_principal_ids or pid_cand in hub_principals:
                        continue
                    rep_nm = up_row.get("representative_name_c") or ""
                    norm_nm = up_row.get("name_normalized") or ""
                    if is_ignored_principal(norm_nm) or is_ignored_principal(rep_nm):
                        continue
                    if attorney_organizer_re.search(rep_nm):
                        continue
                    if not (WHITELIST_PATTERN.search(norm_nm) or WHITELIST_PATTERN.search(rep_nm)):
                        if INSTITUTIONAL_PATTERN.search(norm_nm) or INSTITUTIONAL_PATTERN.search(rep_nm):
                            continue
                    ck = canonicalize_person_name(norm_nm or rep_nm)
                    if not ck or len(ck.split()) < 2:
                        continue
                    prev_pid = canon_to_pid.get(ck)
                    if prev_pid is None:
                        canon_to_pid[ck] = pid_cand
                    elif prev_pid != pid_cand:
                        canon_ambig.add(ck)
                for ak in canon_ambig:
                    canon_to_pid.pop(ak, None)

                ra_addrs = load_oc_registered_agent_addresses(conn)
                owning_list = sorted(owning_business_ids)
                ct_biz_records = {}
                lookup_norms = set()
                for b_idx in range(0, len(owning_list), 5000):
                    b_chunk = owning_list[b_idx : b_idx + 5000]
                    cur.execute(
                        """
                        SELECT id, name, mail_address, mail_city, mail_state, mail_zip,
                               business_address, business_city, business_state, business_zip
                        FROM businesses
                        WHERE id = ANY(%s)
                        """,
                        (b_chunk,),
                    )
                    for brow in cur.fetchall():
                        bid_k = brow["id"]
                        bname = brow.get("name") or ""
                        if not bname or (INSTITUTIONAL_PATTERN.search(bname) and not WHITELIST_PATTERN.search(bname)):
                            continue
                        m_addr = compose_address(brow.get("mail_address"), brow.get("mail_city"), brow.get("mail_state"), brow.get("mail_zip"))
                        b_addr = compose_address(brow.get("business_address"), brow.get("business_city"), brow.get("business_state"), brow.get("business_zip"))
                        rec = demangle_assessor_owner(bname, mailing_address=(m_addr or b_addr))
                        if not rec.is_business or not rec.owner_norm:
                            continue
                        m_coarse = normalize_mailing_address_coarse(m_addr) if m_addr else ""
                        b_coarse = normalize_mailing_address_coarse(b_addr) if b_addr else ""
                        ct_biz_records[bid_k] = (rec, m_coarse or b_coarse, b_coarse if (m_coarse and b_coarse) else "")
                        lookup_norms.add(rec.owner_norm)

                oc_cands_by_norm = defaultdict(list)
                ct_oc_jurs = ["us_ct", "us_de", "us_ny", "us_ma", "us_nj", "us_fl", "us_wy", "us_nv"]
                norm_list = sorted(lookup_norms)
                for n_idx in range(0, len(norm_list), 4000):
                    n_chunk = norm_list[n_idx : n_idx + 4000]
                    cur.execute(
                        """
                        SELECT jurisdiction_code, company_number, name, name_norm, is_active,
                               registered_address_coarse, mailing_address_coarse, agent_address_coarse,
                               home_jurisdiction_code, home_company_number
                        FROM oc_companies
                        WHERE jurisdiction_code = ANY(%s)
                          AND name_norm = ANY(%s)
                        """,
                        (ct_oc_jurs, n_chunk),
                    )
                    for oc_r in cur.fetchall():
                        oc_cands_by_norm[oc_r["name_norm"]].append(
                            OCCandidate(
                                jurisdiction_code=oc_r["jurisdiction_code"],
                                company_number=oc_r["company_number"],
                                name=oc_r["name"],
                                name_norm=oc_r["name_norm"],
                                is_active=bool(oc_r.get("is_active", True)),
                                registered_address_coarse=oc_r.get("registered_address_coarse") or "",
                                mailing_address_coarse=oc_r.get("mailing_address_coarse") or "",
                                agent_address_coarse=oc_r.get("agent_address_coarse") or "",
                                home_jurisdiction_code=oc_r.get("home_jurisdiction_code"),
                                home_company_number=oc_r.get("home_company_number"),
                            )
                        )

                matched_oc_by_bid = {}
                for bid_k, (rec, prim_c, ext_c) in ct_biz_records.items():
                    m_res = score_oc_candidates(
                        rec,
                        parcel_state="CT",
                        parcel_mailing_coarse=prim_c or None,
                        candidates=oc_cands_by_norm.get(rec.owner_norm, ()),
                        extra_parcel_coarse_addrs=(ext_c,) if ext_c else (),
                        registered_agent_addrs=ra_addrs,
                    )
                    if m_res is not None:
                        matched_oc_by_bid[bid_k] = (m_res.canonical_jurisdiction_code, m_res.canonical_company_number)

                resolved_oc = resolve_oc_company_principals(conn, list(set(matched_oc_by_bid.values())), max_hops=3)
                oc_prin_pairs = set()
                holdco_to_bids = defaultdict(list)

                for bid_k, okey in matched_oc_by_bid.items():
                    oinfo = resolved_oc.get(okey) or {}
                    for h_nm in oinfo.get("humans") or ():
                        if attorney_organizer_re.search(h_nm) or is_ignored_principal(h_nm):
                            continue
                        if INSTITUTIONAL_PATTERN.search(h_nm) and not WHITELIST_PATTERN.search(h_nm):
                            continue
                        ck = canonicalize_person_name(h_nm)
                        pid_match = canon_to_pid.get(ck)
                        if pid_match is not None:
                            u = get_id(("business", bid_k))
                            v = get_id(("principal", pid_match))
                            graph[u].add(v)
                            graph[v].add(u)
                            try:
                                pid_i = int(pid_match)
                                property_active_principal_ids.add(pid_i)
                                oc_prin_pairs.add((bid_k, pid_i))
                            except (TypeError, ValueError):
                                pass

                    for hold_nm in oinfo.get("holding_entities") or ():
                        if INSTITUTIONAL_PATTERN.search(hold_nm) or is_excluded_third_party_entity(hold_nm):
                            continue
                        h_norm = normalize_business_name(hold_nm)
                        if h_norm and len(h_norm) >= 6 and h_norm not in SKIP_NAMES:
                            holdco_to_bids[h_norm].append(bid_k)

                    for hq_addr in oinfo.get("hq_addresses") or ():
                        if hq_addr and not hq_addr.startswith("PO BOX "):
                            holdco_to_bids[f"hq:{hq_addr}"].append(bid_k)

                holdco_edges = 0
                for h_norm, h_bids in holdco_to_bids.items():
                    uniq_bids = sorted(set(h_bids))
                    if len(uniq_bids) < 2:
                        continue
                    if h_norm.startswith("hq:") and _is_dispersed_intermediary_business_hub(uniq_bids, graph, node_to_int):
                        continue
                    h_hub = get_id(("address", f"oc_holdco:{h_norm}"))
                    for b_k in uniq_bids:
                        b_int = get_id(("business", b_k))
                        graph[b_int].add(h_hub)
                        graph[h_hub].add(b_int)
                        holdco_edges += 1

                if oc_prin_pairs:
                    execute_values(
                        cur,
                        "INSERT INTO ct_oc_principal_links (business_id, principal_id) VALUES %s ON CONFLICT DO NOTHING",
                        sorted(oc_prin_pairs),
                        page_size=2000,
                    )
                logger.info(
                    "    OpenCorporates CT piercing matched %s owning businesses -> %s human principal links, %s holding-company edges.",
                    f"{len(matched_oc_by_bid):,}",
                    f"{len(oc_prin_pairs):,}",
                    f"{holdco_edges:,}",
                )
            cur.execute("RELEASE SAVEPOINT sp_ct_oc_bridges")
        except Exception as oc_ct_exc:
            cur.execute("ROLLBACK TO SAVEPOINT sp_ct_oc_bridges")
            logger.warning("  - Skipping CT OpenCorporates bridge step: %s", oc_ct_exc)

        # A2. Same-family co-principal bridges through non-owning businesses.
        #
        # This keeps the universe of address/email/business matching limited to
        # property-owning businesses, while preserving direct co-principal evidence
        # such as Menachem + Yehuda Gurevitch sharing Supplies for Pro LLC.
        # It intentionally adds principal-principal edges only; the non-owning
        # business itself is not added to the visible graph.
        logger.info("  - Loading Same-Family Co-Principal Bridges (Non-Owning Businesses)...")

        BRIDGE_PRINCIPAL_BUSINESS_TERMS = {
            'APARTMENT', 'APARTMENTS', 'ASSOC', 'ASSOCIATES', 'ASSOCIATION',
            'CO', 'COMPANY', 'CORP', 'CORPORATION', 'DEVELOPMENT', 'ENTERPRISE',
            'ENTERPRISES', 'ESTATE', 'GROUP', 'HOLDING', 'HOLDINGS', 'INC',
            'INCORPORATED', 'INVESTMENT', 'INVESTMENTS', 'LLC', 'LLP', 'LP',
            'LTD', 'MANAGEMENT', 'PARTNER', 'PARTNERS', 'PARTNERSHIP',
            'PROPERTIES', 'PROPERTY', 'REALTY', 'REAL', 'SERVICES', 'TRUST',
            'VENTURES',
        }

        def bridge_surname_key(name):
            norm = normalize_person_name(name)
            if not norm:
                return ""
            parts = norm.split()
            if len(parts) < 2 or len(parts) > 4:
                return ""
            if any(part in BRIDGE_PRINCIPAL_BUSINESS_TERMS for part in parts):
                return ""
            if len(parts[0]) < 2 or len(parts[1]) < 2:
                return ""
            # unique_principals.name_normalized is stored in LAST FIRST order.
            return parts[0]

        bridge_groups = defaultdict(lambda: defaultdict(list))
        with conn.cursor(name="network_same_family_bridges", cursor_factory=RealDictCursor) as bridge_cur:
            bridge_cur.itersize = 50000
            bridge_cur.execute("""
                SELECT pbl.business_id, pbl.principal_id,
                       up.name_normalized as p_name,
                       up.representative_name_c as p_rep_name,
                       b.name as b_name
                FROM principal_business_links pbl
                JOIN unique_principals up ON up.principal_id = pbl.principal_id
                JOIN businesses b ON b.id = pbl.business_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM properties p WHERE p.business_id = pbl.business_id
                )
            """)
            for row in bridge_cur:
                try:
                    pid = int(row['principal_id'])
                except (TypeError, ValueError):
                    continue
                principal_node = ('principal', pid)
                if pid not in property_active_principal_ids or principal_node not in node_to_int:
                    continue
                if pid in ambiguous_principal_ids:
                    continue
                if is_ignored_principal(row['p_name']) or is_ignored_principal(row['p_rep_name']) or pid in hub_principals:
                    continue

                b_name = row['b_name'] or ""
                is_whitelisted = (
                    WHITELIST_PATTERN.search(row['p_name'] or "")
                    or WHITELIST_PATTERN.search(row['p_rep_name'] or "")
                    or WHITELIST_PATTERN.search(b_name)
                )
                if not is_whitelisted:
                    if INSTITUTIONAL_PATTERN.search(row['p_name'] or "") or INSTITUTIONAL_PATTERN.search(row['p_rep_name'] or ""):
                        continue
                    if INSTITUTIONAL_PATTERN.search(b_name):
                        continue

                surname = bridge_surname_key(row['p_name'])
                if not surname:
                    continue
                bridge_groups[row['business_id']][surname].append(pid)

        bridge_business_count = 0
        bridge_edge_count = 0
        max_bridge_group = 0
        for surname_groups in bridge_groups.values():
            added_for_business = False
            for pids in surname_groups.values():
                pids = sorted(set(pids))
                if len(pids) < 2:
                    continue
                max_bridge_group = max(max_bridge_group, len(pids))
                int_ids = [node_to_int[('principal', pid)] for pid in pids]
                for i in range(len(int_ids)):
                    for j in range(i + 1, len(int_ids)):
                        u, v = int_ids[i], int_ids[j]
                        if v not in graph[u]:
                            bridge_edge_count += 1
                        graph[u].add(v)
                        graph[v].add(u)
                added_for_business = True
            if added_for_business:
                bridge_business_count += 1

        logger.info(
            "  - Added %s same-family bridge edge(s) via %s non-owning business(es); max family group=%s.",
            f"{bridge_edge_count:,}",
            f"{bridge_business_count:,}",
            max_bridge_group,
        )

        # B. Shared Emails (Filtered)
        if skip_emails:
            logger.info("  - SKIPPING Shared Emails (Diagnostic Mode).")
            return graph, node_to_int, int_to_node

        logger.info("  - Loading Shared Emails...")
        # Force a fresh cursor for rules to ensure no state leakage
        with conn.cursor(cursor_factory=RealDictCursor) as rule_cur:
            rule_cur.execute("SELECT domain, match_type FROM email_match_rules")
            email_rules = {row['domain'].lower().strip(): row['match_type'] for row in rule_cur}

        # Hardcoded fail-safe blacklist for known mega-registrars
        BLACKLIST_DOMAINS = {
            'cscinfo.com', 'cscglobal.com', 'wolterskluwer.com', 'incfile.com',
            'northwestregisteredagent.com', 'cogencyglobal.com', 'registeredagentsinc.com',
            'corporatedocfiling.com', 'corpcreations.com', 'unitedagentgroup.com',
            'primecorporateservices.com', 'starwood.com', 'rohuer.com', 'cncinfo.com',
            'ct.gov' # Institutional bypass
        }
        for d in BLACKLIST_DOMAINS:
            email_rules[d] = 'registrar'

        logger.info(f"  - Loaded {len(email_rules)} email rules.")

        # User Specific Whitelist for Custom Domains
        # (Removed: Loaded from DB)

        email_map = defaultdict(list)
        email_biz_hqs = defaultdict(Counter)
        with conn.cursor(name="network_email_businesses", cursor_factory=RealDictCursor) as email_cur:
            email_cur.itersize = 50000
            email_cur.execute("""
                SELECT id, name, business_email_address,
                       mail_address, mail_city, mail_state, mail_zip,
                       business_address, business_city, business_state, business_zip
                FROM businesses
                WHERE business_email_address IS NOT NULL
            """)
            for row in email_cur:
                # --- FILTER: Must be an owning business ---
                if row['id'] not in owning_business_ids:
                    continue

                # --- FILTER: Exclude Institutional Entities from Email Linking ---
                # If an entity is generic/institutional (Housing Authority, University),
                # its email (e.g. info@university.edu) should not bridge other entities.
                if INSTITUTIONAL_PATTERN.search(row['name'] or ""):
                    # Unless it's whitelisted
                    if not WHITELIST_PATTERN.search(row['name'] or ""):
                        continue

                key = get_email_match_key(row['business_email_address'], email_rules)
                if key:
                    email_map[key].append(row['id']) # Store string ID directly
                    m_addr = compose_address(row.get('mail_address'), row.get('mail_city'), row.get('mail_state'), row.get('mail_zip'))
                    b_addr = compose_address(row.get('business_address'), row.get('business_city'), row.get('business_state'), row.get('business_zip'))
                    hq_norm = first_normalized_mailing_address(m_addr, b_addr, coarse=True)
                    if hq_norm and not is_ignored_shared_address(hq_norm):
                        email_biz_hqs[key][hq_norm] += 1

        skipped_email_keys = 0
        for key, biz_ids in email_map.items():
            if len(biz_ids) > 1:
                # Quota-free structural cohesion test (replaces arbitrary 25/75 business caps):
                # A large landlord can legitimately share one email/domain across 100+ LLCs
                # when they share cohesive HQ addresses or principal surnames. Only skip an
                # email/domain when >= 10 businesses have BOTH dispersed HQs AND dispersed
                # unrelated principal surnames (indicating an unlisted attorney/CPA/agent).
                if len(biz_ids) >= EMAIL_HUB_MIN_BUSINESSES:
                    hq_counts = email_biz_hqs.get(key) or Counter()
                    total_hqs = sum(hq_counts.values())
                    top2_hq_share = (sum(c for _, c in hq_counts.most_common(2)) / total_hqs) if total_hqs else 1.0
                    surnames = Counter(s for b in biz_ids for s in biz_to_surnames.get(b, ()))
                    total_sur = sum(surnames.values())
                    top2_sur_share = (sum(c for _, c in surnames.most_common(2)) / total_sur) if total_sur else 1.0
                    if (
                        len(hq_counts) >= EMAIL_HUB_MIN_DISPERSED_HQS
                        and top2_hq_share < EMAIL_HUB_TOP2_HQ_MAX_SHARE
                        and len(surnames) >= EMAIL_HUB_MIN_DISPERSED_SURNAMES
                        and top2_sur_share < EMAIL_HUB_TOP2_SURNAME_MAX_SHARE
                    ):
                        skipped_email_keys += 1
                        logger.info(
                            "    Skipping dispersed intermediary email key %s (%s businesses, %s HQs top2=%.0f%%, %s surnames top2=%.0f%%)",
                            key,
                            len(biz_ids),
                            len(hq_counts),
                            top2_hq_share * 100.0,
                            len(surnames),
                            top2_sur_share * 100.0,
                        )
                        continue
                # Star-connect through hub business (O(N) edges instead of O(N^2) clique!)
                int_ids = [get_id(('business', bid)) for bid in biz_ids]
                hub_u = int_ids[0]
                for v in int_ids[1:]:
                    if hub_u != v:
                        graph[hub_u].add(v)
                        graph[v].add(hub_u)
        if skipped_email_keys:
            logger.info("  - Skipped %s high-frequency email key(s) as likely service/agent bridges.", skipped_email_keys)

        # C. Shared Mailing Addresses (using coarse normalization)
        # Safeguards: blank/placeholder filtering, curated registered-agent
        # blocklist from address_match_rules, and a frequency cap for the
        # agent offices and virtual mailboxes nobody has listed yet.
        logger.info("  - Loading Shared Mailing Addresses (Coarse Normalization)...")

        JUNK_ADDRS = {
            'NO INFORMATION PROVIDED', 'NONE', 'UNKNOWN', '', '.', 'NOT PROVIDED', 'N/A', 'NULL',
            'CONNECTICUT', 'CT', 'USA', 'UNITED STATES',
        }

        registrar_substrings = load_registrar_address_substrings(conn)
        if registrar_substrings:
            logger.info(
                "  - Loaded %s registered-agent address rule(s) from address_match_rules.",
                f"{len(registrar_substrings):,}",
            )
        else:
            logger.warning(
                "  - No registered-agent address rules loaded; falling back to the "
                "hardcoded list only. Shared-address matching will be noisier."
            )

        LAW_CARE_OF_RE = re.compile(
            r'\bC\s*/?\s*O\b.*\b(?:LAW\b|ESQ\b|ATTORNEYS?\b|C\.?P\.?A\.?\b)|GREENE\s+LAW',
            re.IGNORECASE,
        )
        LAW_OFFICE_ADDR_RE = re.compile(
            r'\bLAW\s+(?:PC|P\.C\.|PLLC|FIRM|OFFICES?|GROUP)\b|\bATTORNEYS?\s+AT\s+LAW\b',
            re.IGNORECASE,
        )
        raw_addr_records = []
        coarse_distinct_units = defaultdict(set)
        all_biz_count_by_coarse = defaultdict(int)
        addr_key_to_coarse = {}
        law_office_coarse_addrs = set()
        third_party_entity_addr_counts = defaultdict(int)
        skipped_registrar_addresses = set()
        with conn.cursor(name="network_address_businesses", cursor_factory=RealDictCursor) as addr_cur:
            addr_cur.itersize = 50000
            # City/state/zip live in sibling columns for business_address, so the
            # address must be composed before normalizing — otherwise every
            # '70 West Main Street' in the state collapses to one key.
            addr_cur.execute("""
                SELECT id, name, mail_address, mail_city, mail_state, mail_zip,
                       business_address, business_city, business_state, business_zip
                FROM businesses
            """)
            for row in addr_cur:
                b_name = row.get('name') or ''
                raw_combined = f"{row['mail_address'] or ''} {row['business_address'] or ''}"
                if LAW_CARE_OF_RE.search(raw_combined):
                    care_mail = compose_address(row['mail_address'], row['mail_city'],
                                                row['mail_state'], row['mail_zip'])
                    care_biz = compose_address(row['business_address'], row['business_city'],
                                               row['business_state'], row['business_zip'])
                    stripped_mail = re.sub(r'\bC\s*/?\s*O\b[^,\d]*', ' ', care_mail, flags=re.IGNORECASE)
                    stripped_biz = re.sub(r'\bC\s*/?\s*O\b[^,\d]*', ' ', care_biz, flags=re.IGNORECASE)
                    for candidate_addr in (care_mail, care_biz, stripped_mail, stripped_biz):
                        care_norm = normalize_mailing_address_coarse(candidate_addr)
                        if care_norm and care_norm not in JUNK_ADDRS and len(care_norm) > 4:
                            law_office_coarse_addrs.add(care_norm)

                if is_excluded_third_party_entity(b_name):
                    tp_mail = compose_address(
                        strip_benign_care_of_prefix(row['mail_address']) or row['mail_address'],
                        row['mail_city'], row['mail_state'], row['mail_zip'],
                    )
                    tp_biz = compose_address(
                        strip_benign_care_of_prefix(row['business_address']) or row['business_address'],
                        row['business_city'], row['business_state'], row['business_zip'],
                    )
                    tp_norm = first_normalized_mailing_address(tp_mail, tp_biz, coarse=True)
                    if tp_norm and tp_norm not in JUNK_ADDRS and len(tp_norm) > 4:
                        third_party_entity_addr_counts[tp_norm] += 1
                    continue

                if INSTITUTIONAL_PATTERN.search(b_name) and not WHITELIST_PATTERN.search(b_name):
                    continue

                # Count non-owning businesses per coarse building address for RA footprint ratio
                if row['id'] not in property_owning_business_ids:
                    if row['mail_address'] or row['business_address']:
                        no_mail = compose_address(row['mail_address'], row['mail_city'], row['mail_state'], row['mail_zip'])
                        no_biz = compose_address(row['business_address'], row['business_city'], row['business_state'], row['business_zip'])
                        no_norm = first_normalized_mailing_address(no_mail, no_biz, coarse=True)
                        if no_norm:
                            all_biz_count_by_coarse[no_norm] += 1
                    continue
                raw_comp_mail = compose_address(row['mail_address'], row['mail_city'],
                                                row['mail_state'], row['mail_zip'])
                raw_comp_biz = compose_address(row['business_address'], row['business_city'],
                                               row['business_state'], row['business_zip'])
                comp_mail = compose_address(
                    strip_benign_care_of_prefix(row['mail_address']) or row['mail_address'],
                    row['mail_city'], row['mail_state'], row['mail_zip'],
                )
                comp_biz = compose_address(
                    strip_benign_care_of_prefix(row['business_address']) or row['business_address'],
                    row['business_city'], row['business_state'], row['business_zip'],
                )
                norm = first_normalized_mailing_address(comp_mail, comp_biz, coarse=True)
                # Filter: coarse normalizer already rejects blanks/short/no-letter strings
                # Additional junk filter for known placeholders
                if not norm or norm in JUNK_ADDRS or len(norm) <= 4:
                    continue
                all_biz_count_by_coarse[norm] += 1
                if is_ignored_shared_address(norm):
                    continue
                if is_excluded_third_party_address(f"{raw_comp_mail} ; {raw_comp_biz}", norm, registrar_substrings):
                    skipped_registrar_addresses.add(norm)
                    continue

                if LAW_OFFICE_ADDR_RE.search(raw_combined):
                    law_office_coarse_addrs.add(norm)

                fine_raw = first_normalized_mailing_address(comp_mail, comp_biz, coarse=False)
                fine_no_zip = re.sub(r'\s+\d{5}\b\s*$', '', fine_raw).strip() if fine_raw else ""
                fine_no_zip = re.sub(r'\b(\d+)(?:ST|ND|RD|TH)\b', r'\1', fine_no_zip)
                unit_tokens = tuple(re.findall(r'#\s*([A-Z0-9][A-Z0-9-]*)', fine_no_zip))
                if unit_tokens:
                    coarse_distinct_units[norm].add(unit_tokens)

                raw_addr_records.append((row['id'], norm, fine_no_zip, bool(unit_tokens)))

        for tp_addr, tp_cnt in third_party_entity_addr_counts.items():
            if tp_cnt >= 2:
                law_office_coarse_addrs.add(tp_addr)

        addr_map = defaultdict(list)
        multi_unit_buildings = {
            c for c, units in coarse_distinct_units.items()
            if len(units) >= MULTI_UNIT_BUILDING_MIN_DISTINCT_UNITS
        }
        for bid, norm, fine_no_zip, has_unit in raw_addr_records:
            if norm in law_office_coarse_addrs:
                continue
            if norm in multi_unit_buildings:
                # Multi-unit office tower / UPS Store / PMB center: only match businesses
                # sharing the exact same unit/mailbox number; never collapse the building.
                if has_unit and fine_no_zip:
                    addr_map[fine_no_zip].append(bid)
                    addr_key_to_coarse[fine_no_zip] = norm
            else:
                addr_map[norm].append(bid)
                addr_key_to_coarse[norm] = norm

        if law_office_coarse_addrs:
            logger.info(
                "  - Excluded %s law-office care-of service address(es) from shared-address bridging.",
                f"{len(law_office_coarse_addrs):,}",
            )
        if multi_unit_buildings:
            logger.info(
                "  - Preserved unit-level separation at %s multi-unit building / PMB center address(es) (>= %s distinct units).",
                f"{len(multi_unit_buildings):,}",
                MULTI_UNIT_BUILDING_MIN_DISTINCT_UNITS,
            )

        addr_membership_edge_count = 0
        addr_pair_equivalent_count = 0
        skipped_dispersed_addresses = 0
        for addr, biz_ids in addr_map.items():
            uniq_biz = sorted(set(biz_ids))
            if len(uniq_biz) < 2:
                continue
            coarse_ref = addr_key_to_coarse.get(addr, addr)
            if _is_dispersed_intermediary_business_hub(
                uniq_biz,
                graph,
                node_to_int,
                total_registered_at_coarse=all_biz_count_by_coarse.get(coarse_ref, 0),
            ):
                skipped_dispersed_addresses += 1
                logger.info(
                    "    Skipping dispersed intermediary address %s (%s businesses; disjoint principals or <15%% property-owning ratio).",
                    addr, len(uniq_biz),
                )
                continue
            addr_node = get_id(('address', addr))
            for bid in uniq_biz:
                biz_node = get_id(('business', bid))
                graph[biz_node].add(addr_node)
                graph[addr_node].add(biz_node)
                addr_membership_edge_count += 1
            addr_pair_equivalent_count += len(uniq_biz) * (len(uniq_biz) - 1) // 2

        if skipped_registrar_addresses:
            logger.info(
                "  - Excluded %s known registered-agent address(es) via address_match_rules.",
                f"{len(skipped_registrar_addresses):,}",
            )
        if skipped_dispersed_addresses:
            logger.info(
                "  - Excluded %s address(es) failing principal-cohesion / real-estate footprint checks.",
                f"{skipped_dispersed_addresses:,}",
            )

        logger.info(
            "  - Added %s shared-address membership edges from %s unique addresses (equivalent to %s uncapped pair links).",
            f"{addr_membership_edge_count:,}",
            f"{len(addr_map):,}",
            f"{addr_pair_equivalent_count:,}",
        )

    # Convert sets to tuples for memory efficiency (sets have high overhead)
    # We use a dict instead of defaultdict for the final object to prevent accidental growth
    final_graph = {k: tuple(v) for k, v in graph.items()}
    del graph # Free the sets immediately

    logger.info(f"✅ PHASE 2 COMPLETE: Graph built with {len(final_graph):,} nodes and {len(int_to_node):,} entities.")
    return final_graph, node_to_int, int_to_node

# --- Logic 3: Depth-Limited Discovery ---
def discover_networks_depth_limited(graph_data, seed_nodes_raw):
    """Discovers networks using Iterative BFS on INTEGER graph."""
    graph, node_to_int, int_to_node = graph_data

    logger.info("🔍 PHASE 3: Discovering networks (Iterative BFS + DSU Merging on Integers)...")

    # Filter seed nodes to those present in our graph
    # (Seeds might be filtered out by hub rules, etc)
    seed_ints = []
    for s in seed_nodes_raw:
        if s in node_to_int:
            seed_ints.append(node_to_int[s])

    logger.info(f"  - Valid seeds count: {len(seed_ints):,} / {len(seed_nodes_raw):,}")

    # Initialize DSU for merging seed components
    # Map: Seed_Index -> Parent_Seed_Index
    parent = list(range(len(seed_ints)))

    def find(i):
        if parent[i] == i:
            return i
        root = i
        while parent[root] != root:
            root = parent[root]
        curr = i
        while curr != root:
            next_val = parent[curr]
            parent[curr] = root
            curr = next_val
        return root

    def union(i, j):
        root_i = find(i)
        root_j = find(j)
        if root_i != root_j:
            parent[root_i] = root_j

    # Map: Node_Int -> Seed_Index (owner)
    node_owner = {}

    processed_count = 0
    total_seeds = len(seed_ints)

    for idx, seed_id in enumerate(seed_ints):
        if idx % 10000 == 0:
            logger.info(f"  - Processing seed {idx:,}/{total_seeds:,}...")

        if seed_id in node_owner:
            continue

        # Start new component
        node_owner[seed_id] = idx
        queue = deque([(seed_id, 0)])

        while queue:
            node, depth = queue.popleft()
            processed_count += 1

            if depth >= MAX_DEPTH: continue

            for neighbor in graph.get(node, []):
                if neighbor not in node_owner:
                    node_owner[neighbor] = idx
                    queue.append((neighbor, depth + 1))
                else:
                    # Collision
                    owner_idx = node_owner[neighbor]
                    if find(idx) != find(owner_idx):
                        union(idx, owner_idx)

    # Construct discovered_networks list
    logger.info("  - Compiling network components...")
    components_int = defaultdict(list)

    for node_id, owner_idx in node_owner.items():
        root = find(owner_idx)
        components_int[root].append(node_id)

    # Convert ints back to node tuples
    discovered_networks = []
    for comp_ints in components_int.values():
        network = [int_to_node[i] for i in comp_ints]
        discovered_networks.append(network)

    logger.info(f"✅ PHASE 3 COMPLETE: Found {len(discovered_networks):,} distinct networks.")
    return discovered_networks

# --- Logic 4: Storage ---
def store_networks_shadow(conn, networks, graph_data=None):
    """Stores discovered networks into shadow tables with property-weighted human naming."""
    logger.info("💾 PHASE 4: Storing to shadow tables...")
    with conn.cursor() as cur:
        # ownership_links_shadow is truncated in setup_shadow_tables in safe_network_refresh,
        # but if running standalone it might need truncation.
        # Safe to truncate here if we assume this is the only writer.
        cur.execute("TRUNCATE networks_shadow, entity_networks_shadow, ownership_links_shadow RESTART IDENTITY;")

    # Metadata loading
    biz_names = {}; prin_names = {}
    biz_props = defaultdict(int); prin_props = defaultdict(int)
    biz_values = defaultdict(float); prin_values = defaultdict(float)
    biz_to_prins = defaultdict(list)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        logger.info("  - Loading names...")
        cur.execute("SELECT id, name FROM businesses"); biz_names = {r['id']: r['name'] for r in cur}
        cur.execute("SELECT principal_id, representative_name_c FROM unique_principals"); prin_names = {r['principal_id']: r['representative_name_c'] for r in cur}

        logger.info("  - Loading property weights...")
        cur.execute("SELECT business_id, COUNT(*) as count, SUM(COALESCE(assessed_value, 0)) as val FROM properties WHERE business_id IS NOT NULL GROUP BY business_id")
        for r in cur:
            biz_props[r['business_id']] = r['count']
            biz_values[r['business_id']] = float(r['val'])

        cur.execute("SELECT principal_id, COUNT(*) as count, SUM(COALESCE(assessed_value, 0)) as val FROM properties WHERE principal_id IS NOT NULL GROUP BY principal_id")
        for r in cur:
            try:
                pid = int(r['principal_id'])
                prin_props[pid] = r['count']
                prin_values[pid] = float(r['val'])
            except (ValueError, TypeError):
                continue

        logger.info("  - Loading principal-business links...")
        cur.execute("SELECT principal_id, business_id FROM principal_business_links")
        for r in cur:
            try:
                pid = int(r['principal_id'])
                biz_to_prins[r['business_id']].append(pid)
            except (ValueError, TypeError):
                continue
        cur.execute("SELECT to_regclass('public.ct_oc_principal_links') IS NOT NULL AS has_oc_links")
        if cur.fetchone()["has_oc_links"]:
            cur.execute("SELECT principal_id, business_id FROM ct_oc_principal_links")
            for r in cur:
                try:
                    pid = int(r['principal_id'])
                    biz_to_prins[r['business_id']].append(pid)
                except (ValueError, TypeError):
                    continue

    def is_human_like(name):
        if not name: return False
        name_upper = name.upper()
        # Heuristic: If it has biz suffixes or common buzzwords, it's probably not a "key human principal"
        for suffix in [' LLC', ' INC', ' CORP', ' LTD', ' LP ', ' LLP', ' TRUST', ' ESTATE', ' ASSOC', 'HOLDING', 'PROPERT', 'REALTY', 'MANAGEMENT', 'PARTNERS', 'VENTURES', 'DEVELOPMENT', 'INVESTMENT', 'SERVIC', 'YANKEE', 'POWER', 'LIGHT', ' GAS ', 'ELECTRIC', 'UTILITY']:
            if suffix in name_upper: return False
        return True

    owning_principals = set(prin_props.keys())
    owning_businesses = set(biz_props.keys())

    # Load principal ignore list ONCE (order-independent via canonicalize_person_name)
    with conn.cursor() as cur:
        cur.execute("SELECT normalized_name FROM principal_ignore_list")
        ignore_names_raw = {r[0] for r in cur if r[0]}
    ignore_names_set = set(ignore_names_raw) | {normalize_person_name(n) for n in ignore_names_raw}
    ignore_canon_set = {canonicalize_person_name(n) for n in ignore_names_raw}
    ignore_canon_set.discard("")

    def is_ignored_header_principal(name):
        if not name:
            return False
        if INSTITUTIONAL_PATTERN.search(name):
            return True
        norm = normalize_person_name(name)
        return norm in ignore_names_set or canonicalize_person_name(name) in ignore_canon_set

    network_rows = []
    entity_links = []

    # Prepare for edge storage
    edges_to_store = []
    graph_map = None
    node_to_int = None
    if graph_data:
        graph_map, node_to_int, _ = graph_data
        logger.info("  - Graph data provided. Will store edges.")

    for nid, group in enumerate(networks, 1):
        # 1. Identify members
        principals = []
        for n in group:
            if n[0] == 'principal':
                try: principals.append(int(n[1]))
                except: pass
        businesses = [n[1] for n in group if n[0] == 'business']

        # 2. NAMING LOGIC: Property-Weighted Human Priority
        p_weights = defaultdict(int)
        for pid in principals:
            p_weights[pid] += prin_props.get(pid, 0)

        # Add property counts from businesses owned by these principals (within this network)
        for bid in businesses:
            b_count = biz_props.get(bid, 0)
            if b_count > 0:
                for pid in biz_to_prins.get(bid, []):
                    if pid in p_weights: # Only count if the principal is actually in this network
                        p_weights[pid] += b_count

        # Header candidates MUST be active owners (direct or indirect) and not ignored agents
        active_ps = [p for p in principals if p_weights.get(p, 0) > 0 and not is_ignored_header_principal(prin_names.get(p))]

        # Sort active principals: First by human-likeness, then by property influence
        sorted_ps = sorted(
            active_ps,
            key=lambda p: (is_human_like(prin_names.get(p)), p_weights.get(p, 0)),
            reverse=True
        )

        primary_name = None
        if sorted_ps:
            primary_name = prin_names.get(sorted_ps[0])

        if not primary_name:
            # Fallback to top active business owner
            active_bs = [b for b in businesses if b in owning_businesses]
            sorted_bs = sorted(active_bs, key=lambda b: biz_props.get(b, 0), reverse=True)
            if sorted_bs:
                primary_name = biz_names.get(sorted_bs[0])
            else:
                primary_name = "Unknown Network"

        visible_p_count = len(active_ps)
        visible_b_count = len([b for b in businesses if b in owning_businesses])

        # --- DEBUG LOGGING FOR GUREVITCH / SRULOWITZ ---
        if primary_name and ("GUREVITCH" in primary_name.upper() or "SRULOWITZ" in primary_name.upper()):
            principal_sample = principals[:20]
            weight_sample = [(p, p_weights.get(p, 0)) for p in principals[:20]]
            principal_suffix = "..." if len(principals) > len(principal_sample) else ""
            logger.info(f"--- DEBUG NETWORK: {primary_name} ---")
            logger.info(f"  Principals in Net: {principal_sample}{principal_suffix}")
            logger.info(f"  Businesses in Net: {len(businesses)}")
            logger.info(f"  Owning Businesses in Net: {visible_b_count}")
            logger.info(f"  Active Principals (ActivePS): {len(active_ps)}")
            logger.info(f"  Principal Weights: {weight_sample}{principal_suffix}")
            logger.info(f"  Sample Biz Props: {[(b, biz_props.get(b,0)) for b in businesses[:5]]}")
        # -----------------------------------------------

        # Calculate totals using SQL at the end of Phase 4 to avoid memory-heavy sets
        net_id = nid
        network_rows.append((net_id, primary_name, visible_b_count, visible_p_count, 0, 0))

        if len(network_rows) >= 10000:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO networks_shadow (id, primary_name, business_count, principal_count, total_properties, total_assessed_value) VALUES %s",
                    network_rows,
                )
            conn.commit()
            network_rows = []

        # Identify principals who are connected to owning businesses (indirect ownership)
        principals_via_businesses = set()
        for bid in businesses:
            if bid in owning_businesses:
                for pid in biz_to_prins.get(bid, []):
                    principals_via_businesses.add(pid)

        # STORE NODES
        for node_type, node_id in group:
            # RESTRICTED: Only insert owning entities into the link table (sidebar)
            if node_type == 'business' and node_id not in owning_businesses:
                continue
            if node_type == 'principal':
                try:
                    pid = int(node_id)
                    # Include if directly owns property OR indirectly via business
                    if pid not in owning_principals and pid not in principals_via_businesses:
                        continue
                except: continue

            name = biz_names.get(node_id) if node_type == 'business' else prin_names.get(node_id)
            if not name: continue
            norm = normalize_business_name(name) if node_type == 'business' else normalize_person_name(name)
            entity_links.append((net_id, node_type, node_id, name, norm))

        # STORE EDGES (Optimization: Only if graph data provided)
        if graph_map:
            # Map node tuple to entity name for storage
            # We want to store edges between ANY nodes in the network, not just "visible" ones,
            # because the invisible ones (e.g. non-owning businesses) might be the bridge.

            # 1. Get Int IDs for all nodes in this network group
            group_ints = []
            addr_ints = []
            node_map = {} # Int -> Name

            for node in group:
                if node in node_to_int:
                    idx = node_to_int[node]
                    if node[0] == 'address':
                        addr_ints.append(idx)
                        continue
                    group_ints.append(idx)

                    # Resolve name
                    n_name = None
                    if node[0] == 'business': n_name = biz_names.get(node[1])
                    elif node[0] == 'principal': n_name = prin_names.get(node[1])

                    if n_name: node_map[idx] = n_name

            # 2. Check edges between entity nodes
            for u_idx in group_ints:
                u_name = node_map.get(u_idx)
                if not u_name: continue

                neighbors = graph_map.get(u_idx, [])
                for v_idx in neighbors:
                    if v_idx > u_idx and v_idx in node_map:
                        v_name = node_map.get(v_idx)
                        u_node = graph_data[2][u_idx]
                        v_node = graph_data[2][v_idx]

                        l_type = 'link'
                        if u_node[0] != v_node[0]: l_type = 'principal_link'
                        elif u_node[0] == 'business': l_type = 'shared_contact'

                        edges_to_store.append((net_id, u_name, v_name, l_type))

            # 3. Emit star edges for shared-address hub nodes so components stay connected
            for a_idx in addr_ints:
                addr_biz_names = []
                seen_addr_names = set()
                for v_idx in graph_map.get(a_idx, []):
                    if v_idx in node_map and graph_data[2][v_idx][0] == 'business':
                        b_nm = node_map[v_idx]
                        if b_nm and b_nm not in seen_addr_names:
                            seen_addr_names.add(b_nm)
                            addr_biz_names.append(b_nm)
                if len(addr_biz_names) >= 2:
                    hub_nm = addr_biz_names[0]
                    for other_nm in addr_biz_names[1:]:
                        edges_to_store.append((net_id, hub_nm, other_nm, 'shared_address'))

        if len(entity_links) >= 10000:
            with conn.cursor() as cur:
                execute_values(cur, "INSERT INTO entity_networks_shadow (network_id, entity_type, entity_id, entity_name, normalized_name) VALUES %s", entity_links)
            conn.commit(); entity_links = []

        if len(edges_to_store) >= 10000:
            with conn.cursor() as cur:
                execute_values(cur, "INSERT INTO ownership_links_shadow (network_id, from_entity, to_entity, link_type) VALUES %s", edges_to_store)
            conn.commit(); edges_to_store = []

    if network_rows:
        with conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO networks_shadow (id, primary_name, business_count, principal_count, total_properties, total_assessed_value) VALUES %s",
                network_rows,
            )
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT setval(pg_get_serial_sequence('networks_shadow', 'id'), COALESCE((SELECT MAX(id) FROM networks_shadow), 1))"
        )
    conn.commit()

    if entity_links:
        with conn.cursor() as cur:
            execute_values(cur, "INSERT INTO entity_networks_shadow (network_id, entity_type, entity_id, entity_name, normalized_name) VALUES %s", entity_links)
        conn.commit()

    if edges_to_store:
        with conn.cursor() as cur:
            execute_values(cur, "INSERT INTO ownership_links_shadow (network_id, from_entity, to_entity, link_type) VALUES %s", edges_to_store)
        conn.commit()

    logger.info("📊 Calculating and updating network total properties and values using SQL...")
    with conn.cursor() as cur:
        cur.execute("""
            WITH network_unique_properties AS (
                SELECT DISTINCT ON (en.network_id, p.location, p.property_city, p.unit)
                       en.network_id,
                       p.location,
                       p.property_city,
                       p.unit,
                       p.assessed_value
                FROM entity_networks_shadow en
                JOIN properties p ON (
                    (en.entity_type = 'business' AND p.business_id = en.entity_id)
                    OR (en.entity_type = 'business' AND UPPER(p.owner) = UPPER(en.entity_name))
                    OR (en.entity_type = 'business' AND p.owner_norm = en.normalized_name)
                    OR (en.entity_type = 'principal' AND p.principal_id = en.entity_id)
                )
                ORDER BY en.network_id, p.location, p.property_city, p.unit, p.id DESC
            ),
            network_stats AS (
                SELECT network_id,
                       COUNT(*) as total_props,
                       SUM(COALESCE(assessed_value, 0)) as total_val
                FROM network_unique_properties
                GROUP BY network_id
            )
            UPDATE networks_shadow n
            SET total_properties = ns.total_props,
                total_assessed_value = ns.total_val
            FROM network_stats ns
            WHERE n.id = ns.network_id;
        """)
    conn.commit()
    logger.info("✅ Network totals update complete.")

    logger.info("✅ PHASE 4 COMPLETE.")

def run_full_rebuild():
    conn = get_db_connection()
    try:
        # 0. Clear stale links (to remove old name-based IDs)
        logger.info("🧹 PHASE 0: Clearing stale property links...")
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE properties
                SET business_id = NULL, principal_id = NULL
                WHERE business_id IS NOT NULL OR principal_id IS NOT NULL
            """)
        conn.commit()

        link_properties_to_entities(conn)
        graph, node_to_int, int_to_node = build_graph(conn)

        # Seed nodes: Entities that own properties
        logger.info("Gathering seed nodes...")
        seeds = set()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT DISTINCT business_id FROM properties WHERE business_id IS NOT NULL")
            for r in cur: seeds.add(('business', r['business_id']))
            cur.execute("SELECT DISTINCT principal_id FROM properties WHERE principal_id IS NOT NULL")
            for r in cur:
                try:
                    pid = int(r['principal_id'])
                    seeds.add(('principal', pid))
                except (ValueError, TypeError):
                    continue

        graph_data = (graph, node_to_int, int_to_node)
        networks = discover_networks_depth_limited(graph_data, list(seeds))
        store_networks_shadow(conn, networks, graph_data=graph_data)
    finally:
        conn.close()

if __name__ == "__main__":
    run_full_rebuild()
