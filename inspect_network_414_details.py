import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Select all distinct principals from principal_business_links for the businesses in network 414
        print("=== Actual principals of businesses in network 414 ===")
        cursor.execute("""
            SELECT DISTINCT up.principal_id, up.name_normalized 
            FROM principal_business_links pbl
            JOIN unique_principals up ON up.principal_id = pbl.principal_id
            WHERE pbl.business_id IN (
                SELECT entity_id FROM entity_networks 
                WHERE network_id = 414 AND entity_type = 'business'
            )
            ORDER BY up.name_normalized;
        """)
        principals = cursor.fetchall()
        print(f"Number of actual principals: {len(principals)}")
        for p in principals[:15]:
            print(dict(p))
            
        # Is 'DAVID MACK' in this list of actual principals?
        is_in = any(p['name_normalized'] == 'DAVID MACK' for p in principals)
        print(f"\nIs 'DAVID MACK' actually one of the principals of these businesses? {is_in}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
