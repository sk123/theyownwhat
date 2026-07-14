import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query unique_principals where name is 'DAVID MACK'
        print("=== unique_principals ===")
        cursor.execute("SELECT * FROM unique_principals WHERE name_normalized = 'DAVID MACK';")
        for r in cursor.fetchall():
            print(dict(r))
            p_id = r['principal_id']
            
            # Query principal_business_links
            print(f"\n=== principal_business_links for {p_id} ===")
            cursor.execute("SELECT * FROM principal_business_links WHERE principal_id = %s;", (p_id,))
            links = cursor.fetchall()
            print(f"Count of links: {len(links)}")
            for l in links[:5]:
                print(dict(l))
                
            # Query entity_networks
            print(f"\n=== entity_networks for DAVID MACK or {p_id} ===")
            cursor.execute("""
                SELECT * FROM entity_networks 
                WHERE entity_id = %s OR entity_id = %s OR entity_name = 'DAVID MACK';
            """, (str(p_id), 'DAVID MACK'))
            for r2 in cursor.fetchall():
                print(dict(r2))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
