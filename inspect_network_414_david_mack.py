import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Select all actual principals from principal_business_links for the businesses in network 414 that match 'DAVID MACK'
        cursor.execute("""
            SELECT DISTINCT up.principal_id, up.name_normalized, pbl.business_id, b.name as business_name
            FROM principal_business_links pbl
            JOIN unique_principals up ON up.principal_id = pbl.principal_id
            JOIN businesses b ON b.id = pbl.business_id
            WHERE pbl.business_id IN (
                SELECT entity_id FROM entity_networks 
                WHERE network_id = 414 AND entity_type = 'business'
            )
            AND up.name_normalized = 'DAVID MACK';
        """)
        for r in cursor.fetchall():
            print(dict(r))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
