import psycopg2
from psycopg2.extras import RealDictCursor
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

def check_arman():
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. Check normalization of the property record
        cur.execute("SELECT owner, owner_norm, principal_id FROM properties WHERE owner ILIKE '%ANDALIB ARMAN%'")
        rows = cur.fetchall()
        print(f"Found {len(rows)} properties for 'ANDALIB ARMAN':")
        for r in rows:
            print(f"  Owner: {r['owner']} | Norm: {r['owner_norm']} | PrincipalID: {r['principal_id']}")

        # 2. Check principal variations
        cur.execute("SELECT name_c, name_c_norm FROM principals WHERE name_c ILIKE '%ANDALIB ARMAN%'")
        prins = cur.fetchall()
        print(f"\nPrincipals matching 'ANDALIB ARMAN':")
        for p in prins:
            print(f"  Name: {p['name_c']} | Norm: {p['name_c_norm']}")

    conn.close()

if __name__ == "__main__":
    check_arman()
