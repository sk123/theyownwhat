import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Select all columns from entity_networks where network_id = 414 and entity_type = 'principal'
        print("=== Principals in network 414 (first 10) ===")
        cursor.execute("""
            SELECT * FROM entity_networks 
            WHERE network_id = 414 AND entity_type = 'principal'
            ORDER BY entity_id
            LIMIT 10;
        """)
        for r in cursor.fetchall():
            print(dict(r))
            
        # Select all columns from entity_networks where entity_id = 'DAVID MACK'
        print("\n=== entity_networks where entity_id = 'DAVID MACK' ===")
        cursor.execute("SELECT * FROM entity_networks WHERE entity_id = 'DAVID MACK';")
        for r in cursor.fetchall():
            print(dict(r))
            
        # Let's see if there are any principals in entity_networks with integer entity_ids
        print("\n=== Are there integer entity_ids for principals in entity_networks? ===")
        cursor.execute("""
            SELECT entity_id, entity_name 
            FROM entity_networks 
            WHERE entity_type = 'principal' AND entity_id ~ '^[0-9]+$'
            LIMIT 5;
        """)
        for r in cursor.fetchall():
            print(dict(r))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
