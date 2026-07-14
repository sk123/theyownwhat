import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Search in principals table
        print("=== Principals table for DAVID MACK ===")
        cursor.execute("""
            SELECT id, name_c, address, business_id 
            FROM principals 
            WHERE name_c ILIKE '%david mack%' 
            LIMIT 5;
        """)
        for r in cursor.fetchall():
            print(dict(r))
            
        # 2. Search in unique_principals table
        print("\n=== unique_principals table ===")
        cursor.execute("""
            SELECT * 
            FROM unique_principals 
            WHERE name_normalized ILIKE '%david mack%' 
            LIMIT 5;
        """)
        for r in cursor.fetchall():
            print(dict(r))
            
        # 3. Search in principal_business_links table
        print("\n=== principal_business_links ===")
        cursor.execute("""
            SELECT * 
            FROM principal_business_links 
            WHERE business_id ILIKE '%david mack%' 
            LIMIT 5;
        """)
        for r in cursor.fetchall():
            print(dict(r))
            
        # 4. Search in entity_networks
        print("\n=== entity_networks count by network_id ===")
        cursor.execute("""
            SELECT network_id, COUNT(*) 
            FROM entity_networks 
            WHERE network_id IN (
                SELECT network_id FROM entity_networks WHERE entity_name ILIKE '%david mack%'
            )
            GROUP BY network_id;
        """)
        for r in cursor.fetchall():
            print(dict(r))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
