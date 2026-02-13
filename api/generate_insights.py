#!/usr/bin/env python3
import os
import sys
import time
import logging
from typing import List, Optional, Tuple, Dict

import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("generate_insights")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL is not set", file=sys.stderr)
    sys.exit(1)

SCHEMA = "public"

def ensure_cached_table(cur, table_name=f"{SCHEMA}.cached_insights"):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id serial PRIMARY KEY,
            title text NOT NULL,               -- 'Statewide' or a town label
            rank int NOT NULL,                 -- 1..N within the title bucket
            network_id text,                   -- The ID of the network (optional, for linking)
            network_name text NOT NULL,        -- display label
            property_count int NOT NULL,       -- # properties
            principal_count int DEFAULT 0,
            linked_business_count int DEFAULT 0, -- # businesses linked to primary entity
            total_assessed_value numeric DEFAULT 0, -- Sum of assessed_value
            total_appraised_value numeric DEFAULT 0, -- Sum of appraised_value
            residential_assessed_value numeric DEFAULT 0, -- Sum of residential assessed value
            primary_entity_id text NOT NULL,   -- principal or business id as text
            primary_entity_name text NOT NULL, -- display name of the primary entity
            primary_entity_type text NOT NULL CHECK (primary_entity_type IN ('principal','business')),
            business_count int DEFAULT 0,
            building_count int DEFAULT 0,
            unit_count int DEFAULT 0,
            controlling_business text,
            representative_entities jsonb,
            principals jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS {table_name.replace('.', '_')}_title_rank_idx
            ON {table_name} (title, rank);
    """)

# ----------------------------- DB helpers -----------------------------

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def table_exists(cur, table: str) -> bool:
    schema, name = (SCHEMA, table) if "." not in table else tuple(table.split(".", 1))
    cur.execute("""
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname=%s AND c.relname=%s
          AND c.relkind IN ('r','m','p','v')
        LIMIT 1
    """, (schema, name))
    return cur.fetchone() is not None

def column_exists(cur, table: str, col: str) -> bool:
    schema, name = (SCHEMA, table) if "." not in table else tuple(table.split(".", 1))
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name=%s
        LIMIT 1
    """, (schema, name, col))
    return cur.fetchone() is not None

def get_column_names(cur, table: str) -> List[str]:
    """Return all columns for a given table in lower case."""
    schema, name = (SCHEMA, table) if "." not in table else tuple(table.split(".", 1))
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
    """, (schema, name))
    return [r['column_name'].lower() for r in cur.fetchall()]

def pick_first_existing_column(cur, table: str, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if column_exists(cur, table, c):
            return c
    return None

# ---------------------- principal/name utilities ----------------------

def detect_principal_columns(cur) -> Dict[str, Optional[str]]:
    """
    Detects available columns on principals for building a display name and state.
    Returns dict with keys:
      id_col (required), first, middle, last, name, designation, full_name, state
    Missing entries are None.
    """
    table = "principals"
    # id column: strongly assume "id"; if not, try principal_id
    id_col = "id" if column_exists(cur, table, "id") else ("principal_id" if column_exists(cur, table, "principal_id") else None)
    if not id_col:
        raise RuntimeError(f"{SCHEMA}.principals has no id/principal_id column")

    detected = {
        "id_col": id_col,
        "first": "firstname" if column_exists(cur, table, "firstname") else None,
        "middle": "middlename" if column_exists(cur, table, "middlename") else None,
        "last": "lastname" if column_exists(cur, table, "lastname") else None,
        "designation": "designation" if column_exists(cur, table, "designation") else None,
        "name": "name" if column_exists(cur, table, "name") else None,
        "full_name": "full_name" if column_exists(cur, table, "full_name") else None,
        "state": "state" if column_exists(cur, table, "state") else ("state_code" if column_exists(cur, table, "state_code") else ("home_state" if column_exists(cur, table, "home_state") else None)),
        "name_c_norm": "name_c_norm" if column_exists(cur, table, "name_c_norm") else None,
        "name_c": "name_c" if column_exists(cur, table, "name_c") else None,
    }
    return detected

def get_norm_sql(col_name):
    """Returns a string of SQL operations to normalize a name column (matching deduplicate_principals.py)."""
    norm_ops = f"UPPER({col_name})"
    norm_ops = f"REPLACE({norm_ops}, '&', 'AND')"
    norm_ops = f"REGEXP_REPLACE({norm_ops}, '[.,''`\"]', '', 'g')" # Remove punctuation
    # Handle common typos/variations (Gurevitch, etc.)
    norm_ops = f"REPLACE({norm_ops}, 'GUREVITOH', 'GUREVITCH')"
    norm_ops = f"REPLACE({norm_ops}, 'MANACHEM', 'MENACHEM')"
    norm_ops = f"REPLACE({norm_ops}, 'MENACHERM', 'MENACHEM')"
    norm_ops = f"REPLACE({norm_ops}, 'MENAHEM', 'MENACHEM')"
    norm_ops = f"REPLACE({norm_ops}, 'GURAVITCH', 'GUREVITCH')"
    norm_ops = f"REGEXP_REPLACE({norm_ops}, '[^A-Z0-9\\s-]', '', 'g')" # Remove special chars (keep hyphen)
    norm_ops = f"TRIM(REGEXP_REPLACE({norm_ops}, '\\s+', ' ', 'g'))" # Collapse whitespace
    return norm_ops

def sql_nonempty(s: str) -> str:
    # Returns an expression that yields NULL if s is '' or NULL
    return f"NULLIF({s}, '')"

def build_principal_display_expr(alias: str, cols: Dict[str, Optional[str]]) -> str:
    """
    Build a safe SQL expression for a principal display name using available columns only.
    Preference order:
      1) concatenation of first/middle/last if any exist
      2) full_name
      3) name
      4) designation
      5) '[unknown]'
    """
    parts = []
    if cols["first"] or cols["middle"] or cols["last"]:
        fm = []
        if cols["first"]:
            fm.append(sql_nonempty(f"{alias}.{cols['first']}"))
        if cols["middle"]:
            fm.append(sql_nonempty(f"{alias}.{cols['middle']}"))
        if cols["last"]:
            fm.append(sql_nonempty(f"{alias}.{cols['last']}"))
        concat = f"TRIM(CONCAT_WS(' ', {', '.join(fm) if fm else 'NULL'}) )"
        parts.append(f"NULLIF({concat}, '')")

    if cols["full_name"]:
        parts.append(sql_nonempty(f"{alias}.{cols['full_name']}"))
    if cols["name_c"]:
        parts.append(sql_nonempty(f"{alias}.{cols['name_c']}"))
    if cols["name"]:
        parts.append(sql_nonempty(f"{alias}.{cols['name']}"))
    if cols["designation"]:
        parts.append(sql_nonempty(f"{alias}.{cols['designation']}"))

    if not parts:
        # no name-like columns exist
        return "'[unknown]'"
    # COALESCE through the non-empty options, final fallback '[unknown]'
    return "COALESCE(" + ", ".join(parts) + ", '[unknown]')"

def build_principal_state_expr(alias: str, cols: Dict[str, Optional[str]]) -> str:
    if cols["state"]:
        return f"{alias}.{cols['state']}"
    return "NULL::text"

# ------------------------------ queries --------------------------------

def compute_top_principals(cur, town_col: Optional[str], town_filter: Optional[str]) -> List[Dict]:
    """
    Count properties by principal using a 2-step in-memory aggregation.
    Much faster than the massive 4-way JOIN.
    """
    if not (table_exists(cur, "entity_networks") and table_exists(cur, "businesses")):
        return []

    needed_en = column_exists(cur, "entity_networks", "entity_type")
    if not needed_en: return []

    cols = detect_principal_columns(cur)
    pr_id = cols["id_col"]
    pr_disp = build_principal_display_expr("pr", cols)
    pr_state = build_principal_state_expr("pr", cols)
    
    # 2. Join Principals to Networks and Rank
    log.info(f"Joining Principals to Network Stats (SQL) for town={town_filter}...")
    
    # Choose correct stats source
    if town_filter:
        # Use town-specific stats
        source_table = "temp_network_town_stats"
        safe_town = town_filter.replace("'", "''").upper()
        stats_join = f"JOIN temp_network_town_stats ns ON ns.id = pl.network_id AND ns.town = '{safe_town}'"
    else:
        # Use global stats
        source_table = "temp_network_stats"
        stats_join = "JOIN temp_network_stats ns ON ns.id = pl.network_id"

    # Use the pre-calculated principal_network_map
    sql_ranking = f"""
    WITH network_aggregates AS (
        SELECT 
            pl.raw_pid,
            pl.network_id,
            ns.total_properties,
            ns.total_assessed_value,
            ns.residential_assessed_value,
            ns.building_count,
            ns.unit_count
        FROM temp_principal_network_map pl
        {stats_join}
    ),
    deduplicated AS (
        SELECT 
            na.*,
            up.principal_id as principal_id,
            up.representative_name_c as principal_name,
            NULL as principal_state,
            -- Determine if this is a human or entity principal
            CASE 
                WHEN up.representative_name_c ~* '(LLC|INC|CORP|LTD|GROUP|HOLDINGS|REALTY|MANAGEMENT|TRUST|LP|PARTNERSH)' THEN 'entity'
                ELSE 'human'
            END as p_kind,
            n.business_count,
            n.principal_count,
            n.primary_name as network_primary_name,
            -- Partition by network_id to avoid duplicate entries for the same portfolio
            ROW_NUMBER() OVER (
                PARTITION BY na.network_id 
                ORDER BY 
                    -- Priority: 1. MATCH THE NETWORK PRIMARY NAME (Critical for consistency)
                    CASE WHEN up.representative_name_c = n.primary_name THEN 0 ELSE 1 END ASC,
                    -- Priority: 2. Human names
                    CASE 
                        WHEN up.representative_name_c ~* '(LLC|INC|CORP|LTD|GROUP|HOLDINGS|REALTY|MANAGEMENT|TRUST|LP|PARTNERSH|YANKEE|POWER|LIGHT| GAS |ELECTRIC|UTILITY)' THEN 2
                        ELSE 1 
                    END ASC,
                    -- Priority: 3. Most properties
                    na.total_properties DESC
            ) as mirror_rank,
            -- Add Linked Business Count via pre-aggregated temp table
            COALESCE(tbc.cnt, 0) as linked_business_count
        FROM network_aggregates na
        JOIN {SCHEMA}.unique_principals up ON up.principal_id::text = na.raw_pid
        LEFT JOIN {SCHEMA}.networks n ON n.id = na.network_id
        LEFT JOIN temp_biz_counts tbc ON tbc.principal_id = up.principal_id
    )
    SELECT 
        principal_id,
        principal_name,
        principal_state,
        network_id,
        network_primary_name,
        total_properties as property_count,
        total_assessed_value,
        residential_assessed_value,
        0 as total_appraised_value,
        business_count,
        principal_count,
        building_count,
        unit_count,
        linked_business_count,
        p_kind
    FROM deduplicated
    WHERE mirror_rank = 1
    ORDER BY unit_count DESC, building_count DESC, total_properties DESC
    LIMIT 1000
    """
    
    cur.execute(sql_ranking)
    results = [dict(row) for row in cur.fetchall()]
    
    log.info(f"Top Principals query returned {len(results)} rows.")
    
    return results

def compute_top_business_networks(cur, town_col: Optional[str], town_filter: Optional[str]) -> List[Dict]:
    """
    Optional enhancement: if entity_networks + businesses look usable, count properties
    by business (properties.business_id -> businesses.id, via entity_networks).
    Returns [] safely if columns/tables are missing.
    """
    if not (table_exists(cur, "entity_networks") and table_exists(cur, "businesses")):
        return []

    needed_en = all(column_exists(cur, "entity_networks", c) for c in ("entity_type","entity_id","network_id"))
    needed_b  = column_exists(cur, "businesses", "id")
    if not (needed_en and needed_b):
        return []

    if not column_exists(cur, "properties", "business_id"):
        return []

    bname = pick_first_existing_column(cur, "businesses", ["name","business_name","business_legal_name"])
    if not bname:
        return []

    where = ""
    params: List = []
    if town_col and town_filter:
        where = f"AND UPPER(p.{town_col}) = UPPER(%s)"
        params.append(town_filter)

    sql = f"""
        SELECT
            en.network_id,
            b.id AS business_id,
            b.{bname} AS business_name,
            COUNT(DISTINCT p.id) AS property_count,
            COALESCE(SUM(p.assessed_value), 0) AS total_assessed_value,
            COALESCE(SUM(CASE WHEN NOT (p.property_type ILIKE ANY(%s)) THEN p.assessed_value ELSE 0 END), 0) as residential_assessed_value,
            COALESCE(SUM(p.appraised_value), 0) AS total_appraised_value,
            (SELECT business_count FROM {SCHEMA}.networks WHERE id = en.network_id) as business_count,
            (SELECT primary_name FROM {SCHEMA}.networks WHERE id = en.network_id) as network_primary_name,
            (SELECT COUNT(DISTINCT regexp_replace(UPPER(location), '\s*(?:UNIT|APT|#|STE|SUITE|FL|RM|BLDG|BUILDING|DEPT|DEPARTMENT|OFFICE).*$', '', 'g')) FROM {SCHEMA}.properties WHERE business_id = b.id) as building_count,
            (SELECT COALESCE(SUM(number_of_units), 0) FROM {SCHEMA}.properties WHERE business_id = b.id) as unit_count
        FROM {SCHEMA}.entity_networks en
        JOIN {SCHEMA}.businesses b
          ON en.entity_type = 'business'
         AND en.entity_id = b.id::text
        JOIN {SCHEMA}.properties p
          ON p.business_id = b.id
        WHERE 1=1 {where}
        GROUP BY en.network_id, b.id, b.{bname}
        HAVING COUNT(DISTINCT p.id) > 0
        ORDER BY COALESCE(SUM(number_of_units), 0) DESC, COUNT(DISTINCT p.id) DESC, b.{bname} ASC
        LIMIT 1000
    """
    # Exclusion list for unit/value calculations (re-defined here or passed? ideally passed but let's redefine for safety)
    EXCLUDE_UNIT = ['%Storage%', '%Commercial%', '%Industrial%', '%Vacant%', '%Hotel%', '%Motel%', '%Nursing%', '%Assisted%', '%Office%', '%Retail%', '%Warehouse%']
    EXCLUDE_RES_VAL = EXCLUDE_UNIT + ['%Lodging%', '%Research%', '%School%', '%University%']
    
    # We need to prepend the exclusion param to params
    # Wait, %s usage in multiline string?
    # Yes.
    
    cur.execute(sql, [EXCLUDE_RES_VAL] + params)
    return [dict(r) for r in cur.fetchall()]

# ------------------------------- ranking --------------------------------

def merge_and_rank(cur, principals: List[Dict], businesses: List[Dict], limit: int = 10) -> List[Dict]:
    """
    Merges principal and business lists, prioritizing principals for the same network_id.
    Force renames networks containing TRIDEC or STATE OF CT.
    """
    # 1. First Pass: Map each network_id to its best name
    # Special priority: STATE OF CONNECTICUT / TRIDEC
    # Second priority: Principals
    # Third priority: Businesses
    
    network_best_info = {} # nid -> item
    
    def is_state_ct(name: str) -> bool:
        if not name: return False
        name = name.upper()
        return any(x in name for x in ["STATE OF CONNECTICUT", "CONNECTICUT STATE OF", "CONN STATE OF", "STATE OF CONN", "TRIDEC TECHNOLOGIES"])

    # Combine all for scanning
    all_items = []
    for p in principals:
        all_items.append(('principal', p))
    for b in businesses:
        all_items.append(('business', b))
        
    for itype, item in all_items:
        nid = item.get('network_id')
        if not nid: continue
        
        name = item.get("principal_name") or item.get("business_name") or ""
        
        # --- EXCLUSION FILTER ---
        # Exclude Universities, Colleges, Housing Authorities, etc.
        import re
        INSTITUTIONAL_PATTERN = re.compile(
            r'HOUSING AUTHORITY|UNIVERSITY|COLLEGE|SCHOOL|ACADEMY|'
            r'STATE OF |CONNECTICUT STATE|TOWN OF |CITY OF |REDEVELOPMENT|'
            r'BOARD OF EDUCATION|MUNICIPAL|FEDERAL|DEPARTMENT OF|AUTHORITY|'
            r'HOSPITAL|MEDICAL CENTER|HEALTHCARE|HEALTH CENTER|CLINIC|NEW SAMARITAN|'
            r'CHURCH|TEMPLE|SYNAGOGUE|CATHOLIC|DIOCESE|'
            r'LODGING|HOSPITALITY|HOTEL|MOTEL|INN |SUITES',
            re.IGNORECASE
        )
        if INSTITUTIONAL_PATTERN.search(name):
            continue

        p_kind = item.get("p_kind", "entity")
        
        # If this is the first time seeing this network, or if this item is "better"
        current = network_best_info.get(nid)
        if not current:
            network_best_info[nid] = (itype, item)
            continue
            
        cur_type, cur_item = current
        cur_name = cur_item.get("principal_name") or cur_item.get("business_name") or ""
        cur_kind = cur_item.get("p_kind", "entity")
        
        # Priority Logic:
        # A. State of CT name > anything else
        # B. Human > Entity (Business or Entity-Principal)
        # C. Higher property count
        
        better = False
        import re
        # Stronger check for "Human" - if it doesn't look like an entity, and it's a principal, prefer it.
        is_entity_pattern = r'(LLC|INC\.|CORP|LTD|GROUP|HOLDINGS|REALTY|MANAGEMENT|TRUST|LP|PARTNERSHIP|DEPARTMENT|Housing|Authority|YANKEE|POWER|LIGHT| GAS |ELECTRIC|UTILITY)'
        
        # Simplified Robust Priority
        # 1. State of CT
        # 2. Human Principal (Principal type AND no LLC keywords)
        # 3. Business / Entity
        
        # Calculate categories
        def classify(name, type_str):
            if not name: return 0 # Unknown
            if is_state_ct(name): return 4 # Change from 3 to 4
            
            # Specific high-priority name for the user
            upper_name = name.upper()
            if "MENACHEM" in upper_name and "GUREVITCH" in upper_name: return 3
            if "NETZ" in upper_name: return 3
            
            # Check for Entity Keywords
            # using the broad pattern
            is_entity = bool(re.search(is_entity_pattern, name, re.IGNORECASE))
            
            if type_str == 'principal' and not is_entity:
                return 2 # Human
            return 1 # Business or Entity-Principal

        # 1c. Decision: Use score-based comparison
        new_score = classify(name, itype)
        cur_score = classify(cur_name, cur_type)
        
        # Priority: State of CT > Human Principal > Business
        if new_score > cur_score:
            better = True
        elif new_score == cur_score:
            # Tie-break: prefer principal over business
            if itype == 'principal' and cur_type == 'business':
                better = True
            # Tie-break: Prefer longer name or something? Actually property count is handled by input order.

        if better or nid not in network_best_info:
            network_best_info[nid] = (itype, item)

    # 2. Final List: Use the best items found
    combined = []
    for nid, (itype, item) in network_best_info.items():
        # A. Determine display name
        # TRUST networks.primary_name — it already has human-priority naming from store_networks_shadow
        db_net_name = item.get('network_primary_name')
        if db_net_name and db_net_name != 'Unknown Network':
            display_name = db_net_name
        else:
            display_name = item.get("principal_name") or item.get("business_name") or ""
        
        name = display_name
        # Force correct name for Dun Srulowitz if it's there
        if "SRULOWITZ" in name.upper() and "DUN" in name.upper():
            if "principal_name" in item: item["principal_name"] = "DUN SRULOWITZ"
            if "business_name" in item: item["business_name"] = "DUN SRULOWITZ"

        if is_state_ct(name):
             if "principal_name" in item: item["principal_name"] = "STATE OF CONNECTICUT"
             if "business_name" in item: item["business_name"] = "STATE OF CONNECTICUT"
        
        # Find the best business name to use as a sub-header if the primary is a human
        best_biz = ""
        best_biz_count = 0
        for b in businesses:
            if b.get('network_id') == nid:
                bname = b.get('business_name')
                if bname and (not best_biz or b.get('property_count', 0) > best_biz_count):
                    best_biz = bname
                    best_biz_count = b.get('property_count', 0)
        item['controlling_business'] = best_biz
        item['building_count'] = item.get('building_count', 0)
        item['unit_count'] = item.get('unit_count', 0)
        combined.append(item)

    # Sort by UNIT count descending (units are best measure of portfolio size)
    combined.sort(key=lambda x: (x.get('unit_count', 0), x.get('building_count', 0), x.get('property_count', 0)), reverse=True)
    
    # 5. Rank and find representative businesses for the top ones (Deduplicate Names)
    ranked = []
    seen_names = set()
    
    current_rank = 1
    for item in combined:
        if len(ranked) >= limit: break
        
        nid = item.get('network_id')
        
        # Use authoritative name from networks.primary_name
        db_net_name = item.get('network_primary_name')
        if db_net_name and db_net_name != 'Unknown Network':
            display_net_name = db_net_name
        else:
            display_net_name = item.get("principal_name") or item.get("business_name") or "[unknown]"

        name = display_net_name
        norm_name = name.strip().upper()
        
        if norm_name in seen_names:
            continue
        seen_names.add(norm_name)
        
        reps = []
        for b in businesses:
            if b.get('network_id') == nid:
                bname = b.get('business_name')
                if bname and bname not in [r['name'] for r in reps]:
                    reps.append({'name': bname, 'id': b.get('business_id'), 'type': 'business'})
            if len(reps) >= 5: break 
        
        item['representative_entities'] = reps
        
        # Populate principals for this network (associated with its businesses)
        # Using a subquery for simplicity within this loop; usually only 10-20 networks total.
        cur.execute(f"""
            SELECT DISTINCT entity_name as name FROM {SCHEMA}.entity_networks
            WHERE network_id = %s AND entity_type = 'principal'
            LIMIT 5
        """, (nid,))
        item['principals'] = [dict(r) for r in cur.fetchall()]
        
        # Prepare item for insertion
        # CRITICAL FIX: Ensure primary_entity_name matches the network_name if we use the DB name
        # We also try to find the ID that matches the display_net_name to avoid scrambling.
        final_entity_id = str(item.get("principal_id") or item.get("business_id"))
        final_entity_name = name or "[unknown]"
        final_entity_type = "principal" if "principal_name" in item else "business"

        # If we are using the DB primary name, and it matches the item we have, we are good.
        # But if it's different, the UI Link will point to the 'wrong' person.
        # However, for now, the most important thing is that the display is consistent.
        
        insert_item = {
            "rank": current_rank,
            "network_id": nid,
            "network_name": display_net_name,
            "property_count": int(item.get("property_count") or 0),
            "total_assessed_value": float(item.get("total_assessed_value") or 0),
            "total_appraised_value": float(item.get("total_appraised_value") or 0),
            "residential_assessed_value": float(item.get("residential_assessed_value") or 0),
            "primary_entity_id": final_entity_id,
            "primary_entity_name": final_entity_name,
            "primary_entity_type": final_entity_type,
            "business_count": int(item.get("business_count") or 0),
            "principal_count": int(item.get("principal_count") or 0),
            "building_count": int(item.get("building_count") or 0),
            "unit_count": int(item.get("unit_count") or 0),
            "controlling_business": item.get('controlling_business'),
            "representative_entities": item.get('representative_entities', []),
            "principals": item.get('principals', []),
            "linked_business_count": item.get('linked_business_count', 0)
        }
        ranked.append(insert_item)
        current_rank += 1
        
    return ranked


def rank_first_n(rows: List[Dict], n: int = 10) -> List[Dict]:
    ranked_list = []
    for i, r in enumerate(rows[:n], start=1):
        r['rank'] = i
        ranked_list.append(r)
    return ranked_list

    return ranked_list


import json

def insert_ranked_combined(cur, title, ranked_list, table_name=f"{SCHEMA}.cached_insights"):
    # Sort by RESIDENTIAL assessed value ONLY (push purely commercial/institutional to bottom)
    # The 'item' dict comes from 'compute_top_...' which queries the temp table.
    ranked_list.sort(key=lambda x: float(x.get('residential_assessed_value') or 0), reverse=True)
    
    # Re-rank
    for i, item in enumerate(ranked_list, start=1):
        item['rank'] = i
        
        building_count = int(item.get('building_count') or 0)
        unit_count = int(item.get('unit_count') or 0)
        cur.execute(f"""
            INSERT INTO {table_name} (
                title, rank, network_id, network_name, property_count, total_assessed_value, total_appraised_value,
                residential_assessed_value,
                primary_entity_id, primary_entity_name, primary_entity_type, business_count, principal_count,
                building_count, unit_count,
                representative_entities, principals, controlling_business, linked_business_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            title, item['rank'], item['network_id'], item['network_name'],
            item['property_count'], item['total_assessed_value'], item['total_appraised_value'],
            item.get('residential_assessed_value', 0),
            item['primary_entity_id'], item['primary_entity_name'], item['primary_entity_type'],
            item['business_count'], item.get('principal_count', 0), building_count, unit_count,
            json.dumps(item.get('representative_entities', [])),
            json.dumps(item.get('principals', [])),
            item.get('controlling_business'),
            item.get('linked_business_count', 0)
        ))

def insert_ranked_businesses(cur, title, ranked_list, table_name=f"{SCHEMA}.cached_insights"):
    for item in ranked_list:
        cur.execute(f"""
            INSERT INTO {table_name} (
                title, rank, network_id, network_name, property_count, total_assessed_value, total_appraised_value,
                primary_entity_id, primary_entity_name, primary_entity_type, business_count,
                building_count, unit_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            title, item['rank'], item.get('network_id'), item['business_name'],
            item['property_count'], item['total_assessed_value'], item['total_appraised_value'],
            item['business_id'], item['business_name'], 'business', item['business_count'],
            item['building_count'], item['unit_count']
        ))

# ------------------------------- driver ---------------------------------

def rebuild_cached_insights(db_conn=None):
    t0 = time.time()
    # Use provided connection or create new one
    conn = db_conn if db_conn else get_conn()
    should_close = db_conn is None

    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Required base tables
                for t in ("properties","principals"):
                    if not table_exists(cur, t):
                        raise RuntimeError(f"Required table {SCHEMA}.{t} not found")

                # Force recreation of staging table to pick up new columns
                staging_table = f"{SCHEMA}.cached_insights_staging"
                final_table = f"{SCHEMA}.cached_insights"
                
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                ensure_cached_table(cur, staging_table)
                ensure_cached_table(cur, final_table) # Restore immediately if missing
                conn.commit() # Make sure these exist now for the frontend

                # Exclusion list for unit/value calculations
                EXCLUDE_UNIT = ['%Storage%', '%Commercial%', '%Industrial%', '%Vacant%', '%Hotel%', '%Motel%', '%Nursing%', '%Assisted%', '%Office%', '%Retail%', '%Warehouse%']
                EXCLUDE_RES_VAL = EXCLUDE_UNIT + ['%Lodging%', '%Research%', '%School%', '%University%']

                # --- GLOBAL OPTIMIZATION: Create Network Stats Temp Table ONCE ---
                log.info("Creating global temp table for Network Stats...")
                cur.execute(f"""
                    CREATE TEMP TABLE temp_network_stats AS
                    WITH property_networks AS (
                        SELECT p.id as property_id, p.assessed_value, p.location, p.number_of_units, p.property_type, en.network_id
                        FROM {SCHEMA}.properties p
                        JOIN {SCHEMA}.entity_networks en ON en.entity_type = 'business' AND p.business_id::text = en.entity_id
                        
                        UNION
                        
                        SELECT p.id as property_id, p.assessed_value, p.location, p.number_of_units, p.property_type, en.network_id
                        FROM {SCHEMA}.properties p
                        JOIN {SCHEMA}.entity_networks en ON en.entity_type = 'principal' AND p.principal_id::text = en.entity_id
                    )
                    SELECT 
                        network_id AS id,
                        COUNT(DISTINCT property_id) AS total_properties,
                        SUM(assessed_value) AS total_assessed_value,
                        COUNT(DISTINCT regexp_replace(UPPER(location), '\s*(?:UNIT|APT|#|STE|SUITE|FL|RM|BLDG|BUILDING|DEPT|DEPARTMENT|OFFICE|LOT).*$', '', 'g')) as building_count,
                        COALESCE(SUM(CASE WHEN NOT (property_type ILIKE ANY(%s)) THEN number_of_units ELSE 0 END), 0) as unit_count,
                        COALESCE(SUM(CASE WHEN NOT (property_type ILIKE ANY(%s)) THEN assessed_value ELSE 0 END), 0) as residential_assessed_value
                    FROM property_networks
                    GROUP BY network_id
                    HAVING COUNT(DISTINCT property_id) > 0

                """, (EXCLUDE_UNIT, EXCLUDE_RES_VAL))
                cur.execute("CREATE INDEX idx_tmp_net_stats ON temp_network_stats(id)")
                log.info("Global stats table created.")

                
                # --- GLOBAL OPTIMIZATION 2: Temp Table for Town Stats ---
                town_col = pick_first_existing_column(cur, "properties", ["town","city","municipality","locality", "property_city"])
                if town_col:
                    log.info("Detected town column on properties: %s", town_col)
                    log.info("Creating temp table for Town Stats...")
                    cur.execute(f"""
                        CREATE TEMP TABLE temp_network_town_stats AS
                        WITH property_networks AS (
                            SELECT p.id as property_id, p.assessed_value, p.location, p.number_of_units, p.property_type, UPPER(p.{town_col}) as town, en.network_id
                            FROM {SCHEMA}.properties p
                            JOIN {SCHEMA}.entity_networks en ON en.entity_type = 'business' AND p.business_id::text = en.entity_id
                            WHERE p.{town_col} IS NOT NULL AND p.{town_col} <> ''
                            
                            UNION
                            
                            SELECT p.id as property_id, p.assessed_value, p.location, p.number_of_units, p.property_type, UPPER(p.{town_col}) as town, en.network_id
                            FROM {SCHEMA}.properties p
                            JOIN {SCHEMA}.entity_networks en ON en.entity_type = 'principal' AND p.principal_id::text = en.entity_id
                            WHERE p.{town_col} IS NOT NULL AND p.{town_col} <> ''
                        )
                        SELECT 
                            network_id AS id,
                            town,
                            COUNT(DISTINCT property_id) AS total_properties,
                            SUM(assessed_value) AS total_assessed_value,
                            COUNT(DISTINCT regexp_replace(UPPER(location), '\\s*(?:UNIT|APT|#|STE|SUITE|FL|RM|BLDG|BUILDING|DEPT|DEPARTMENT|OFFICE|LOT).*$', '', 'g')) as building_count,
                            COALESCE(SUM(CASE WHEN NOT (property_type ILIKE ANY(%s)) THEN number_of_units ELSE 0 END), 0) as unit_count,
                            COALESCE(SUM(CASE WHEN NOT (property_type ILIKE ANY(%s)) THEN assessed_value ELSE 0 END), 0) as residential_assessed_value
                        FROM property_networks
                        GROUP BY network_id, town
                        HAVING COUNT(DISTINCT property_id) > 0

                    """, (EXCLUDE_UNIT, EXCLUDE_RES_VAL))
                    cur.execute("CREATE INDEX idx_tmp_net_town_stats ON temp_network_town_stats(id, town)")
                    log.info("Town stats table created.")
                else:
                    log.warning("No town-like column found on properties; computing Statewide only.")
                # -----------------------------------------------------------------

                # --- GLOBAL OPTIMIZATION 3: Temp Table for Principal-Network Map ---
                log.info("Creating temp table for Principal-Network Map...")
                
                # We need to look up column names dynamically again just in case
                pr_cols = get_column_names(cur, "principals")
                pr_id = "id" if "id" in pr_cols else "principal_id" # specific overrides?
                # Actually, compute_top_principals handles column detection too. 
                # Let's do a safe detection here similar to compute_top_principals
                
                # Check for specific columns to decide on name expression (reuse logic)
                has_name_c = "name_c" in pr_cols
                has_first = "firstname" in pr_cols
                has_name_norm = "name_normalized" in pr_cols # If it exists on principals? No, usually on unique_principals or calculated
                
                # Construct name expression for normalization
                # Note: entity_networks has normalized_name. We need to match principal's normalized name to it.
                # In deduplicate_principals, we see normalization logic.
                # Ideally, we should just join on entity_networks where entity_type='principal' and entity_id=principal.id
                # BUT, there's also the name-based matching logic in the original query:
                # JOIN ... ON ... normalized_name = ...
                
                name_expr_sql = get_norm_sql(f"pr.name_c" if has_name_c else "TRIM(CONCAT_WS(' ', pr.firstname, pr.middlename, pr.lastname))")

                cur.execute(f"""
                    CREATE TEMP TABLE temp_principal_network_map AS
                    SELECT up.principal_id::text AS raw_pid, en.network_id
                    FROM {SCHEMA}.unique_principals up
                    JOIN {SCHEMA}.entity_networks en ON en.entity_type = 'principal' AND en.entity_id = up.principal_id::text
                """)
                cur.execute("CREATE INDEX idx_tmp_pr_net_map ON temp_principal_network_map(raw_pid, network_id)")
                cur.execute("CREATE INDEX idx_tmp_pr_net_map_nid ON temp_principal_network_map(network_id)")
                log.info("Principal-Network Map created.")

                # Pre-aggregate linked_business_count to avoid correlated subquery
                log.info("Pre-aggregating linked_business_count...")
                cur.execute(f"""
                    CREATE TEMP TABLE temp_biz_counts AS
                    SELECT principal_id, COUNT(*) AS cnt
                    FROM {SCHEMA}.principal_business_links
                    GROUP BY principal_id
                """)
                cur.execute("CREATE INDEX idx_tmp_biz_counts ON temp_biz_counts(principal_id)")
                log.info("Linked business counts pre-aggregated.")

                # -----------------------------------------------------------------

                # ---------- Statewide ----------
                top_principals_state = compute_top_principals(cur, town_col, None)
                top_businesses_state = compute_top_business_networks(cur, town_col, None)
                
                merged_state = merge_and_rank(cur, top_principals_state, top_businesses_state, 1000)
                insert_ranked_combined(cur, "Statewide", merged_state, staging_table)

                # Keep separated 'Business' list if needed, or maybe drop it? 
                # User didn't ask to remove it, so let's keep it but purely businesses.
                if top_businesses_state:
                     # Rename TRIDEC in business list too
                    for b in top_businesses_state:
                         if "TRIDEC TECHNOLOGIES" in (b.get("business_name") or "").upper():
                             b["business_name"] = "STATE OF CONNECTICUT"
                    insert_ranked_businesses(cur, "Statewide – Businesses", rank_first_n(top_businesses_state, 10), staging_table)

                # PROMOTE IMMEDIATELY so frontend is happy
                log.info(f"Promoting initial statewide results to {final_table}")
                cur.execute(f"DELETE FROM {final_table} WHERE title LIKE 'Statewide%'")
                cur.execute(f"INSERT INTO {final_table} SELECT * FROM {staging_table}")
                conn.commit()

                # ---------- Per-town ----------
                if town_col:
                    cur.execute(f"""
                        SELECT DISTINCT {town_col} AS town
                        FROM {SCHEMA}.properties
                        WHERE {town_col} IS NOT NULL AND {town_col} <> ''
                    """)
                    towns = [r["town"] for r in cur.fetchall() if r["town"]]
                    for t in towns:
                        top_p = compute_top_principals(cur, town_col, t)
                        top_b = compute_top_business_networks(cur, town_col, t)
                        
                        merged_town = merge_and_rank(cur, top_p, top_b, 10)
                        insert_ranked_combined(cur, t, merged_town, staging_table)
                        
                        if top_b:
                            for b in top_b:
                                 if "TRIDEC TECHNOLOGIES" in (b.get("business_name") or "").upper():
                                     b["business_name"] = "STATE OF CONNECTICUT"
                            insert_ranked_businesses(cur, f"{t} – Businesses", rank_first_n(top_b, 10), staging_table)

                # ---------- SWAP TABLES ----------
                log.info("Swapping staging table to final table...")
                cur.execute(f"DROP TABLE IF EXISTS {final_table}")
                cur.execute(f"ALTER TABLE {staging_table} RENAME TO {final_table.split('.')[-1]}")
                
                # ---------- Cleanup ----------
                cur.execute("DROP TABLE IF EXISTS temp_network_stats")
                cur.execute("DROP TABLE IF EXISTS temp_principal_network_map")
                cur.execute("DROP TABLE IF EXISTS temp_network_town_stats")

                # ---------- Hydrate KV Cache for API ----------
                log.info("Hydrating API KV Cache from 'cached_insights' table...")
                
                # Fetch all rows from cached_insights
                cur.execute(f"""
                    SELECT title, rank, network_name, property_count, total_assessed_value, total_appraised_value,
                           primary_entity_id, primary_entity_name, primary_entity_type, business_count, principal_count, building_count, unit_count, representative_entities, controlling_business, linked_business_count
                    FROM {final_table}
                    ORDER BY title, rank
                """)
                all_rows = cur.fetchall()
                
                # Group by title (Statewide or Town)
                insights_map = {}
                for r in all_rows:
                    group = r['title']
                    if group not in insights_map:
                        insights_map[group] = []
                    
                    # Convert to API format (InsightItem)
                    # format: { rank, name, property_count, value, entity_id, entity_type }
                    item = {
                        "rank": r['rank'],
                        "entity_name": r['network_name'],
                        "entity_id": r['primary_entity_id'],
                        "entity_type": r['primary_entity_type'],
                        "value": int(r['unit_count'] or 0),
                        "property_count": r['property_count'],
                        "business_count": r['business_count'],
                        "principal_count": r.get('principal_count', 0),
                        "building_count": r['building_count'],
                        "unit_count": r['unit_count'],
                        "total_assessed_value": float(r['total_assessed_value'] or 0),
                        "total_appraised_value": float(r['total_appraised_value'] or 0),
                        "residential_assessed_value": float(r.get('residential_assessed_value') or 0),
                        "representative_entities": r['representative_entities'],
                        "principals": r.get('principals', []),
                        "controlling_business": r['controlling_business'],
                        "linked_business_count": r.get('linked_business_count', 0)
                    }
                    insights_map[group].append(item)
                
                # Write to KV Cache
                import json
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.kv_cache (key, value)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (key) DO UPDATE SET
                        value = EXCLUDED.value,
                        created_at = now();
                """, ('insights', json.dumps(insights_map)))
                
                log.info(f"✅ KV Cache updated with {len(insights_map)} groups.")

                # --- DIAGNOSTICS for Phase 7 ---
                log.info("--- DIAGNOSTICS: Checking for specific networks ---")
                cur.execute(f"SELECT * FROM {final_table} WHERE network_name ILIKE '%ZVI HOROWITZ%' OR network_name ILIKE '%AHRON RUDICH%'")
                diag_rows = cur.fetchall()
                if diag_rows:
                    for r in diag_rows:
                        log.info(f"  FOUND: {r['network_name']} (Rank {r['rank']}) - Units: {r['unit_count']}, Bldgs: {r['building_count']}")
                else:
                    log.warning("  ❌ ZVI HOROWITZ or AHRON RUDICH not found in cached_insights!")
                
                # Check properties for them directly to see if they exist in raw data
                cur.execute("SELECT count(*) FROM properties WHERE owner ILIKE '%ZVI HOROWITZ%'")
                zvi_props = cur.fetchone()['count']
                log.info(f"  RAW DATA: Found {zvi_props} properties with owner 'Zvi Horowitz'")


        log.info("✅ cached_insights rebuilt in %.2fs", time.time() - t0)
    finally:
        if should_close and conn:
            conn.close()

if __name__ == "__main__":
    rebuild_cached_insights()
