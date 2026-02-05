import os
import psycopg2
from psycopg2.extras import RealDictCursor
import sys

# Configure logging
try:
    DATABASE_URL = os.environ.get("DATABASE_URL")
    conn = psycopg2.connect(DATABASE_URL)
except Exception as e:
    print(f"Error connecting to DB: {e}")
    sys.exit(1)

def check_network_naming(name_fragment):
    print(f"\n--- Searching for principals matching '{name_fragment}' ---")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find principals matching the name
        cur.execute("""
            SELECT principal_id, name_normalized, representative_name_c, business_count 
            FROM unique_principals 
            WHERE name_normalized LIKE %s
            ORDER BY business_count DESC
        """, (f'%{name_fragment.upper()}%',))
        
        principals = cur.fetchall()
        for p in principals:
            print(f"ID: {p['principal_id']}, Repr: {p['representative_name_c']}, Norm: {p['name_normalized']}, Count: {p['business_count']}")
            
            # Check what network this principal belongs to
            cur.execute("""
                SELECT network_id FROM entity_networks 
                WHERE entity_type = 'principal' AND entity_id = %s
            """, (str(p['principal_id']),))
            nets = cur.fetchall()
            for n in nets:
                 print(f"  -> Belong to Network ID: {n['network_id']}")
                 # Get Network Header
                 cur.execute("SELECT primary_name, business_count, principal_count FROM networks WHERE id = %s", (n['network_id'],))
                 net_head = cur.fetchone()
                 print(f"     Network Header: {net_head['primary_name']} (Biz: {net_head['business_count']}, Prin: {net_head['principal_count']})")
                 
                 # List ALL principals in this network
                 print("     All Principals in this network:")
                 cur.execute("""
                    SELECT up.name_normalized, up.representative_name_c, up.business_count
                    FROM entity_networks en
                    JOIN unique_principals up ON up.principal_id = CAST(en.entity_id AS INTEGER)
                    WHERE en.network_id = %s AND en.entity_type = 'principal'
                    ORDER BY up.business_count DESC
                 """, (n['network_id'],))
                 all_prins = cur.fetchall()
                 for ap in all_prins:
                     print(f"       - {ap['representative_name_c']} (Count: {ap['business_count']})")
                 print("")

check_network_naming('GARDEN HILL')
check_network_naming('ZVI HOROWITZ')
