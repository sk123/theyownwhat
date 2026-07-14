import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if 001t000000yGHC4AAO or 001t000000WoJgrAAF are in entity_networks
        print("=== Checking businesses in entity_networks ===")
        cursor.execute("""
            SELECT * FROM entity_networks 
            WHERE entity_id IN ('001t000000yGHC4AAO', '001t000000WoJgrAAF');
        """)
        for r in cursor.fetchall():
            print(dict(r))
            
        # Let's see what network_id David Mack is actually assigned to
        print("\n=== David Mack network assignments ===")
        cursor.execute("""
            SELECT * FROM entity_networks 
            WHERE entity_name = 'David Mack' OR entity_name = 'DAVID MACK';
        """)
        for r in cursor.fetchall():
            print(dict(r))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
