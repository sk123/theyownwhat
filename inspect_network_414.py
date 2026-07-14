import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Count of entities in network 414
        cursor.execute("SELECT entity_type, COUNT(*) FROM entity_networks WHERE network_id = 414 GROUP BY entity_type;")
        print("=== Count in network 414 ===")
        for r in cursor.fetchall():
            print(dict(r))
            
        # Sample of businesses in network 414
        cursor.execute("SELECT * FROM entity_networks WHERE network_id = 414 AND entity_type = 'business' LIMIT 10;")
        print("\n=== Businesses in network 414 (sample 10) ===")
        for r in cursor.fetchall():
            print(dict(r))
            
        # Sample of principals in network 414
        cursor.execute("SELECT * FROM entity_networks WHERE network_id = 414 AND entity_type = 'principal' LIMIT 10;")
        print("\n=== Principals in network 414 (sample 10) ===")
        for r in cursor.fetchall():
            print(dict(r))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
