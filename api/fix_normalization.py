import os
import psycopg2
from psycopg2.extras import execute_batch

from api.shared_utils import normalize_owner_name, normalize_person_name

DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

def run():
    print("Connecting...")
    conn = psycopg2.connect(DB_URL)
    c = conn.cursor()
    
    # 1. FIX PRINCIPALS
    print("Fetching principals...")
    c.execute("SELECT id, name_c, name_c_norm FROM principals WHERE name_c IS NOT NULL")
    updates = []
    rows = c.fetchall()
    print(f"Checking {len(rows)} principals...")
    
    for r in rows:
        pid, raw, current_norm = r
        new_norm = normalize_person_name(raw)
        if new_norm != current_norm:
            updates.append((new_norm, pid))
            
    if updates:
        print(f"Updating {len(updates)} principals...")
        execute_batch(c, "UPDATE principals SET name_c_norm = %s WHERE id = %s", updates, page_size=1000)
    else:
        print("No principal updates needed.")
    conn.commit()
        
    # 2. FIX PROPERTIES (Owner)
    print("Fetching properties (owner)...")
    c.execute("SELECT id, owner, owner_norm FROM properties WHERE owner IS NOT NULL")
    updates = []
    rows = c.fetchall()
    print(f"Checking {len(rows)} owners...")
    
    for r in rows:
        pid, raw, current_norm = r
        new_norm = normalize_owner_name(raw)
        if new_norm != current_norm:
            updates.append((new_norm, pid))
            
    if updates:
        print(f"Updating {len(updates)} owners...")
        execute_batch(c, "UPDATE properties SET owner_norm = %s WHERE id = %s", updates, page_size=1000)
    else:
        print("No owner updates needed.")
    conn.commit()

    # 3. FIX PROPERTIES (Co-Owner)
    print("Fetching properties (co_owner)...")
    c.execute("SELECT id, co_owner, co_owner_norm FROM properties WHERE co_owner IS NOT NULL")
    updates = []
    rows = c.fetchall()
    print(f"Checking {len(rows)} co_owners...")
    
    for r in rows:
        pid, raw, current_norm = r
        new_norm = normalize_owner_name(raw)
        if new_norm != current_norm:
            updates.append((new_norm, pid))
            
    if updates:
        print(f"Updating {len(updates)} co_owners...")
        execute_batch(c, "UPDATE properties SET co_owner_norm = %s WHERE id = %s", updates, page_size=1000)
    else:
        print("No co-owner updates needed.")

    conn.commit()
    print("Done.")

if __name__ == "__main__":
    run()
