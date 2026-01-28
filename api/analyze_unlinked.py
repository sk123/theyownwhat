import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")

def main():
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        print("--- Top Unlinked Owners ---")
        cur.execute("""
            SELECT owner, COUNT(*) as cnt 
            FROM properties 
            WHERE business_id IS NULL AND principal_id IS NULL AND owner IS NOT NULL 
            GROUP BY owner 
            ORDER BY cnt DESC 
            LIMIT 20
        """)
        for row in cur:
            print(f"{row['cnt']:5} | {row['owner']}")
            
        print("\n--- Top Owners with 'MANDY' or 'GUREVITCH' or 'OCEAN' (including linked) ---")
        cur.execute("""
            SELECT owner, business_id, principal_id, COUNT(*) as cnt 
            FROM properties 
            WHERE (UPPER(owner) LIKE '%MANDY%' OR UPPER(owner) LIKE '%GUREVITCH%' OR UPPER(owner) LIKE '%OCEAN%')
            GROUP BY owner, business_id, principal_id
            ORDER BY cnt DESC 
            LIMIT 20
        """)
        for row in cur:
            linked = "YES" if (row['business_id'] or row['principal_id']) else "NO"
            print(f"{row['cnt']:5} | {linked:3} | {row['owner']}")

if __name__ == "__main__":
    main()
