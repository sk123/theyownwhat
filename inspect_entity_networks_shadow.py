import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Let's count how many principal rows have numeric entity_ids in entity_networks_shadow
        cursor.execute("""
            SELECT COUNT(*) FROM entity_networks_shadow 
            WHERE entity_type = 'principal' AND entity_id ~ '^[0-9]+$';
        """)
        print("Count of numeric principal entity_ids in entity_networks_shadow:", cursor.fetchone()['count'])
        
        # Let's count total principal rows in entity_networks_shadow
        cursor.execute("""
            SELECT COUNT(*) FROM entity_networks_shadow 
            WHERE entity_type = 'principal';
        """)
        print("Total principal rows in entity_networks_shadow:", cursor.fetchone()['count'])
        
        # Let's see some samples of principal rows in entity_networks_shadow
        cursor.execute("""
            SELECT * FROM entity_networks_shadow 
            WHERE entity_type = 'principal' 
            LIMIT 10;
        """)
        print("\n=== Sample principals in entity_networks_shadow ===")
        for r in cursor.fetchall():
            print(dict(r))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
