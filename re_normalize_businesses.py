
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from api.shared_utils import normalize_business_name

DATABASE_URL = os.environ.get("DATABASE_URL")

def update_normalization():
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        print("Fetching businesses...")
        cur.execute("SELECT id, name FROM businesses WHERE name IS NOT NULL")
        
        updates = []
        count = 0
        for row in cur:
            norm = normalize_business_name(row['name'])
            updates.append((norm, row['id']))
            count += 1
            
            if len(updates) >= 10000:
                with conn.cursor() as write_cur:
                    execute_values(write_cur, "UPDATE businesses SET name_norm = v.n FROM (VALUES %s) AS v(n, id) WHERE businesses.id = v.id", updates)
                conn.commit()
                print(f"Updated {count:,} businesses...")
                updates = []
        
        if updates:
            with conn.cursor() as write_cur:
                execute_values(write_cur, "UPDATE businesses SET name_norm = v.n FROM (VALUES %s) AS v(n, id) WHERE businesses.id = v.id", updates)
            conn.commit()
            print(f"Final: Updated {count:,} businesses.")
    conn.close()

if __name__ == "__main__":
    update_normalization()
