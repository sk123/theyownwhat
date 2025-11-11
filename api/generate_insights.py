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

DDL_CACHED_INSIGHTS = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.cached_insights (
    id serial PRIMARY KEY,
    title text NOT NULL,               -- 'Statewide' or a town label
    rank int NOT NULL,                 -- 1..N within the title bucket
    network_name text NOT NULL,        -- display label
    property_count int NOT NULL,       -- # properties
    primary_entity_id text NOT NULL,   -- principal or business id as text
    primary_entity_name text NOT NULL, -- display name of the primary entity
    primary_entity_type text NOT NULL CHECK (primary_entity_type IN ('principal','business')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cached_insights_title_rank_idx
    ON {SCHEMA}.cached_insights (title, rank);
"""

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

def pick_first_existing_column(cur, table: str, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if column_exists(cur, table, c):
            return c
    return None

def ensure_cached_table(cur):
    cur.execute(DDL_CACHED_INSIGHTS)

def wipe_cached_insights(cur):
    cur.execute(f"TRUNCATE {SCHEMA}.cached_insights")

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
    }
    return detected

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
    Count properties by principal (joining text principal_id -> principals.id safely).
    Optionally filter by a given town (if town_col is present).
    """
    cols = detect_principal_columns(cur)
    pr_id = cols["id_col"]
    pr_disp = build_principal_display_expr("pr", cols)
    pr_state = build_principal_state_expr("pr", cols)

    where = ""
    params: List = []
    if town_col and town_filter:
        where = f"WHERE UPPER(p.{town_col}) = UPPER(%s)"
        params.append(town_filter)

    # Use LEFT JOIN so we include principals with zero properties in the base, then HAVING > 0 filters to those with properties
    sql = f"""
        SELECT
            pr.{pr_id} AS principal_id,
            {pr_disp}  AS principal_name,
            {pr_state} AS principal_state,
            COUNT(p.id) AS property_count
        FROM {SCHEMA}.principals pr
        LEFT JOIN {SCHEMA}.properties p
          ON p.principal_id ~ '^[0-9]+$'
         AND pr.{pr_id} = p.principal_id::integer
        {where}
        GROUP BY pr.{pr_id}, principal_name, principal_state
        HAVING COUNT(p.id) > 0
        ORDER BY COUNT(p.id) DESC, principal_name ASC
        LIMIT 1000
    """
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]

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
            b.id AS business_id,
            b.{bname} AS business_name,
            COUNT(DISTINCT p.id) AS property_count
        FROM {SCHEMA}.entity_networks en
        JOIN {SCHEMA}.businesses b
          ON en.entity_type = 'business'
         AND en.entity_id = b.id::text
        JOIN {SCHEMA}.properties p
          ON p.business_id = b.id
        WHERE 1=1 {where}
        GROUP BY b.id, b.{bname}
        HAVING COUNT(DISTINCT p.id) > 0
        ORDER BY COUNT(DISTINCT p.id) DESC, b.{bname} ASC
        LIMIT 1000
    """
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]

# ------------------------------- ranking --------------------------------

def rank_first_n(rows: List[Dict], n: int = 10) -> List[Tuple[int, Dict]]:
    return [(i, r) for i, r in enumerate(rows[:n], start=1)]

def insert_ranked_principals(cur, title: str, rows: List[Tuple[int, Dict]]):
    for rank, r in rows:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.cached_insights
            (title, rank, network_name, property_count,
             primary_entity_id, primary_entity_name, primary_entity_type)
            VALUES (%s, %s, %s, %s, %s, %s, 'principal')
            """,
            (
                title,
                rank,
                r.get("principal_name") or "[unknown]",
                int(r.get("property_count") or 0),
                str(r.get("principal_id")),
                r.get("principal_name") or "[unknown]",
            ),
        )

def insert_ranked_businesses(cur, title: str, rows: List[Tuple[int, Dict]]):
    for rank, r in rows:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.cached_insights
            (title, rank, network_name, property_count,
             primary_entity_id, primary_entity_name, primary_entity_type)
            VALUES (%s, %s, %s, %s, %s, %s, 'business')
            """,
            (
                title,
                rank,
                r.get("business_name") or "[unknown]",
                int(r.get("property_count") or 0),
                str(r.get("business_id")),
                r.get("business_name") or "[unknown]",
            ),
        )

# ------------------------------- driver ---------------------------------

def main():
    t0 = time.time()
    conn = get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Required base tables
                for t in ("properties","principals"):
                    if not table_exists(cur, t):
                        raise RuntimeError(f"Required table {SCHEMA}.{t} not found")

                ensure_cached_table(cur)
                wipe_cached_insights(cur)

                # Detect a town-like column
                town_col = pick_first_existing_column(cur, "properties", ["town","city","municipality","locality"])
                if town_col:
                    log.info("Detected town column on properties: %s", town_col)
                else:
                    log.warning("No town-like column found on properties; computing Statewide only.")

                # ---------- Statewide ----------
                top_principals_state = compute_top_principals(cur, town_col, None)
                insert_ranked_principals(cur, "Statewide", rank_first_n(top_principals_state, 10))

                top_businesses_state = compute_top_business_networks(cur, town_col, None)
                if top_businesses_state:
                    insert_ranked_businesses(cur, "Statewide – Businesses", rank_first_n(top_businesses_state, 10))

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
                        if top_p:
                            insert_ranked_principals(cur, t, rank_first_n(top_p, 10))
                        top_b = compute_top_business_networks(cur, town_col, t)
                        if top_b:
                            insert_ranked_businesses(cur, f"{t} – Businesses", rank_first_n(top_b, 10))

        log.info("✅ cached_insights rebuilt in %.2fs", time.time() - t0)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
