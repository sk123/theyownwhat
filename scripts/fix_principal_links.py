#!/usr/bin/env python3
"""
Fix principal_id linkage on properties table.

The bug: some properties have principal_id pointing to the wrong principal
because the network builder assigned links based on positional data rather
than name matching. This script re-links by:
1. For each property with an owner name, canonicalize it
2. Find principals whose canonicalized name matches
3. Update the principal_id on the property

Run inside the ctdata_api container:
    docker exec ctdata_api python /app/scripts/fix_principal_links.py
"""

import psycopg2
import os
import sys
import re

sys.path.insert(0, '/app')
from api.shared_utils import canonicalize_person_name, normalize_person_name

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://user:password@ctdata_db:5432/ctdata')

def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()
    
    # Step 1: Build a lookup of canonicalized principal names -> principal ID
    print("Building principal name lookup...")
    cur.execute("SELECT id, name_c FROM principals WHERE name_c IS NOT NULL")
    principal_lookup = {}  # canonical_name -> principal_id
    for pid, name_c in cur.fetchall():
        canonical = canonicalize_person_name(name_c)
        if canonical:
            # Keep the one with the highest ID (most recent)
            if canonical not in principal_lookup or pid > principal_lookup[canonical]:
                principal_lookup[canonical] = pid
    print(f"  Built lookup with {len(principal_lookup)} unique canonical names")
    
    # Step 2: Find properties where owner canonicalizes to a principal
    #         but principal_id is wrong or missing
    print("Scanning properties for mislinked principal_ids...")
    cur.execute("""
        SELECT id, owner, principal_id 
        FROM properties 
        WHERE owner IS NOT NULL 
        AND owner != ''
    """)
    
    fixed = 0
    checked = 0
    batch = []
    
    for prop_id, owner, current_pid in cur.fetchall():
        checked += 1
        canonical_owner = canonicalize_person_name(owner)
        if not canonical_owner:
            continue
            
        correct_pid = principal_lookup.get(canonical_owner)
        if correct_pid and str(correct_pid) != str(current_pid):
            batch.append((str(correct_pid), prop_id))
            fixed += 1
            
        if checked % 10000 == 0:
            print(f"  Checked {checked} properties, fixed {fixed} so far...")
            
        # Batch update every 1000
        if len(batch) >= 1000:
            cur.executemany(
                "UPDATE properties SET principal_id = %s WHERE id = %s",
                batch
            )
            conn.commit()
            batch = []
    
    # Final batch
    if batch:
        cur.executemany(
            "UPDATE properties SET principal_id = %s WHERE id = %s",
            batch
        )
        conn.commit()
    
    print(f"\nDone! Checked {checked} properties, fixed {fixed} principal_id links.")
    
    # Step 3: Invalidate caches
    cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()
    print("Cache invalidated.")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
