import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Search in principals table
        print("=== Search in principals table ===")
        cursor.execute("""
            SELECT id, name_c, address, business_id 
            FROM principals 
            WHERE name_c ILIKE '%david mack%' 
            LIMIT 10;
        """)
        rows = cursor.fetchall()
        for r in rows:
            print(dict(r))
            
        # 2. Search in entity_networks
        print("\n=== Search in entity_networks ===")
        cursor.execute("""
            SELECT network_id, entity_id, entity_type, entity_name 
            FROM entity_networks 
            WHERE entity_name ILIKE '%david mack%' 
            OR entity_id ILIKE '%david mack%';
        """)
        rows = cursor.fetchall()
        for r in rows:
            print(dict(r))
            
        # If we got any network_id, let's look at all entity_networks with that network_id
        if rows:
            net_id = rows[0]['network_id']
            print(f"\n=== All entities in network {net_id} ===")
            cursor.execute("""
                SELECT entity_id, entity_type, entity_name 
                FROM entity_networks 
                WHERE network_id = %s;
            """, (net_id,))
            for r in cursor.fetchall():
                print(dict(r))
                
        # 3. Check principal_business_links
        print("\n=== Search in principal_business_links ===")
        # Find business links for the principal_id of any David Mack in the results
        for r in rows:
            p_id = r['entity_id']
            print(f"Links for principal_id: {p_id}")
            cursor.execute("""
                SELECT business_id, principal_id 
                FROM principal_business_links 
                WHERE principal_id::text = %s;
            """, (p_id,))
            for link in cursor.fetchall():
                print(dict(link))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
