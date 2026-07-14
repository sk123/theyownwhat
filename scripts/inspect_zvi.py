import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== Check all principals containing zvi AND horowitz ===")
        cursor.execute("SELECT id, name_c, name_c_norm, business_id FROM principals WHERE name_c ILIKE '%zvi%' AND name_c ILIKE '%horowitz%'")
        rows = cursor.fetchall()
        for r in rows:
            # check link
            cursor.execute("SELECT principal_id FROM principal_business_links WHERE business_id = %s", (r['business_id'],))
            links = [x['principal_id'] for x in cursor.fetchall()]
            
            # get names of those linked unique_principals
            up_names = []
            if links:
                cursor.execute("SELECT principal_id, name_normalized, representative_name_c FROM unique_principals WHERE principal_id IN %s", (tuple(links),))
                up_names = [dict(x) for x in cursor.fetchall()]
            
            print(f"Principal: {dict(r)} -> Linked unique_principals: {up_names}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
