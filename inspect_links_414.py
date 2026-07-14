import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query ownership_links for network_id = 414 where one of the entities matches 'DAVID MACK'
        print("=== ownership_links involving DAVID MACK in network 414 ===")
        cursor.execute("""
            SELECT * FROM ownership_links 
            WHERE network_id = 414 
            AND (from_entity ILIKE '%DAVID MACK%' OR to_entity ILIKE '%DAVID MACK%');
        """)
        for r in cursor.fetchall():
            print(dict(r))
            
        # Also query principal_business_links for unique_principal named 'DAVID MACK'
        print("\n=== unique_principals count & details ===")
        cursor.execute("SELECT * FROM unique_principals WHERE name_normalized = 'DAVID MACK';")
        row = cursor.fetchone()
        if row:
            print(dict(row))
            p_id = row['principal_id']
            cursor.execute("""
                SELECT pbl.*, b.name 
                FROM principal_business_links pbl
                JOIN businesses b ON b.id = pbl.business_id
                WHERE pbl.principal_id = %s;
            """, (p_id,))
            links = cursor.fetchall()
            print(f"Number of linked businesses: {len(links)}")
            for l in links[:10]:
                print(dict(l))
                
    finally:
        conn.close()

if __name__ == "__main__":
    main()
