
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from api.shared_utils import normalize_business_name

DATABASE_URL = os.environ.get("DATABASE_URL")

def update_normalization():
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        print("Fetching property owners...")
        cur.execute("SELECT id, owner, co_owner FROM properties")
        
        updates = []
        count = 0
        for row in cur:
            onorm = normalize_business_name(row['owner'])
            cnorm = normalize_business_name(row['co_owner'])
            updates.append((onorm, cnorm, row['id']))
            count += 1
            
            if len(updates) >= 10000:
                with conn.cursor() as write_cur:
                    execute_values(write_cur, "UPDATE properties SET owner_norm = v.onorm, co_owner_norm = v.cnorm FROM (VALUES %s) AS v(onorm, cnorm, id) WHERE properties.id = v.id", updates)
                conn.commit()
                print(f"Updated {count:,} records...")
                updates = []
        
        if updates:
            with conn.cursor() as write_cur:
                execute_values(write_cur, "UPDATE properties SET owner_norm = v.onorm, co_owner_norm = v.cnorm FROM (VALUES %s) AS v(onorm, cnorm, id) WHERE properties.id = v.id", updates)
            conn.commit()
            print(f"Final: Updated {count:,} records.")
    conn.close()

if __name__ == "__main__":
    update_normalization()
