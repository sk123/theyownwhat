
import os
import sys
import psycopg2
import json
import re
from collections import defaultdict
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")

def normalize_person_name_py(name):
    return name.upper().strip()

def debug_network(entity_name):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    print(f"Debugging Network for: {entity_name}")
    
    entity_id = entity_name
    pname_norm = normalize_person_name_py(entity_name)
    
    # 1. Find Networks
    cursor.execute(
        "SELECT network_id FROM entity_networks "
        "WHERE entity_type = 'principal' AND (entity_id = %s OR normalized_name = %s)",
        (entity_id, pname_norm)
    )
    rows = cursor.fetchall()
    network_ids = [r["network_id"] for r in rows]
    print(f"Found Network IDs: {network_ids}")
    
    if not network_ids:
        print("No networks found.")
        return

    # 2. Get Businesses
    cursor.execute(
        "SELECT b.* FROM entity_networks en "
        "JOIN businesses b ON b.id::text = en.entity_id "
        "WHERE en.network_id = ANY(%s) AND en.entity_type = 'business'",
        (network_ids,)
    )
    businesses = cursor.fetchall()
    print(f"Found {len(businesses)} Businesses.")
    
    # 3. Get Principals
    cursor.execute(
        "SELECT entity_id AS principal_id, COALESCE(entity_name, entity_id) AS principal_name "
        "FROM entity_networks "
        "WHERE network_id = ANY(%s) AND entity_type = 'principal'",
        (network_ids,)
    )
    principals_in_network = cursor.fetchall()
    print(f"Found {len(principals_in_network)} Principals.")
    
    # 4. Count Properties (Stream Load Logic - Explicit)
    all_biz_ids = [b["id"] for b in businesses]
    all_raw_p_ids = [pr["principal_id"] for pr in principals_in_network]
    
    b_names = [b["name"] for b in businesses]
    p_names = [pr["principal_name"] for pr in principals_in_network]
    
    print(f"Searching properties for {len(all_biz_ids)} business IDs and {len(all_raw_p_ids)} principal IDs/Names.")
    
    query = """
        SELECT count(*) as cnt 
        FROM properties p
        WHERE 
        (business_id::text = ANY(%s) OR principal_id::text = ANY(%s) OR owner_norm = ANY(%s) OR owner = ANY(%s))
    """
    
    cursor.execute(query, (all_biz_ids, all_raw_p_ids, all_raw_p_ids, list(set(b_names + p_names))))
    count = cursor.fetchone()['cnt']
    print(f"Properties Count (Stream Load Logic - Explicit): {count}")
    
    # 5. Simulate Neighbor Fetch
    cursor.execute(f"""
        SELECT location, property_city FROM properties p
        WHERE 
        (business_id::text = ANY(%s) OR principal_id::text = ANY(%s) OR owner_norm = ANY(%s) OR owner = ANY(%s))
    """, (all_biz_ids, all_raw_p_ids, all_raw_p_ids, list(set(b_names + p_names))))
    anchors = cursor.fetchall()
    
    city_locs = defaultdict(set)
    for a in anchors:
         if a['location']:
             base = re.sub(r'\s+(UNIT|APT|#|FL|STE).*$', '', a['location'], flags=re.IGNORECASE).strip()
             if base:
                 city_locs[a['property_city']].add(base)
    
    total_expanded = 0
    for city, locs in city_locs.items():
        if not locs: continue
        patterns = [f"{l}%" for l in locs]
        cursor.execute("""
              SELECT count(*) as cnt FROM properties 
              WHERE property_city = %s AND location LIKE ANY(%s)
        """, (city, patterns))
        total_expanded += cursor.fetchone()['cnt']
        
    print(f"Properties Count (Stream Load Logic - With Neighbors): {total_expanded}")

    # 6. Count Properties (Refresh Cache Logic - Business Only)
    query_cache = """
        SELECT count(DISTINCT p.id) as cnt
        FROM properties p
        JOIN entity_networks en ON p.business_id::text = en.entity_id AND en.entity_type = 'business'
        WHERE en.network_id = ANY(%s)
    """
    cursor.execute(query_cache, (network_ids,))
    count_cache = cursor.fetchone()['cnt']
    print(f"Properties Count (Refresh Cache Logic - Business Only): {count_cache}")

    conn.close()

if __name__ == "__main__":
    debug_network("Menachem Gurevitch")
