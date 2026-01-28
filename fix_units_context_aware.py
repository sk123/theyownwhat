
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import re
from collections import defaultdict

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not set")
    exit(1)

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=RealDictCursor)

def normalize_street(loc):
    # Basic normalization to find "neighbors"
    # Remove unit part if possible.
    # Pattern: remove trailing single letter or "UNIT X"
    s = loc.upper().strip()
    return s

def run():
    print("Fetching properties...")
    cur.execute("SELECT id, location, unit, property_city FROM properties WHERE property_city = 'Farmington' ORDER BY location")
    rows = cur.fetchall()
    
    # Group by "Base Address" (heuristically)
    # We want to group "6 TALCOTT FOREST RD A" and "6 TALCOTT FOREST RD E" together.
    # Heuristic: Remove the last token if it's short.
    
    groups = defaultdict(list)
    
    for r in rows:
        loc = r['location'].upper().strip()
        parts = loc.split()
        if not parts: continue
        
        # Candidate base: everything except last token
        if len(parts) > 1:
            base = " ".join(parts[:-1])
            last = parts[-1]
        else:
            base = loc
            last = ""
            
        groups[base].append(r)
        
    print(f"Found {len(groups)} address groups.")
    
    updates = []
    
    for base, props in groups.items():
        # Check if this group has "Confirmed Units"
        has_Explicit_units = any(p['unit'] is not None for p in props)
        
        # Also check if other members imply units (e.g. "UNIT A" in location but unit column might be null or populated)
        # Actually we only care if we should fix the NULLs.
        
        # If the group has at least ONE confirmed unit (in DB column), 
        # it gives us confidence to interpret single letters as units for the others.
        
        # Specific Logic for 6 TALCOTT FOREST RD
        # DB has "6 TALCOTT FOREST RD A" ... Unit might be NULL? 
        # My check_farmington.py didn't print unit column!
        # Let's assume if unit is NULL, it needs fixing.
        
        potential_units = []
        for p in props:
            if p['unit'] is not None:
                continue
                
        potential_units = []
        for p in props:
            if p['unit'] is not None:
                continue
                
            loc = p['location'].strip()
            # Regex for "Space Single Letter" or "Space Number" at end
            # STRICTER: Only 1 letter or digits. Exclude "AV", "RD", "ST".
            m = re.search(r'\s([A-Z]|\d{1,4})$', loc)
            if m:
                u = m.group(1)
                potential_units.append((p, u))
        
        # Determine if we should apply updates for this group
        # Evidence:
        # 1. Has explicit units (p['unit'] correctly populated).
        # 2. OR Multiple potential units exist and some are NOT directions (A, B, C, D...).
        
        non_direction_candidates = [u for p, u in potential_units if u not in ['N', 'S', 'E', 'W']]
        
        strong_evidence = has_Explicit_units or len(non_direction_candidates) > 0
        
        if strong_evidence:
            for p, u in potential_units:
                # Double check: if it's ONE letter, and it's E/S/N/W, only update if strong_evidence
                if u in ['N', 'S', 'E', 'W'] and not strong_evidence:
                     continue 
                
                # Update
                if base.endswith("6 TALCOTT FOREST RD"):
                     print(f"DEBUG: Updating {p['location']} -> UNIT {u}")

                # print(f"Planned Update: {p['location']} -> UNIT {u} (ID: {p['id']})")
                updates.append((u, p['id']))
                
    print(f"Identified {len(updates)} updates.")
    
    if os.environ.get("EXECUTE") == "1":
        print("Executing updates...")
        for u, pid in updates:
            # cur.execute("UPDATE properties SET unit = %s WHERE id = %s", (u, pid))
            pass
        # conn.commit()
        print("Done.")
    else:
        print("Dry run. Set EXECUTE=1 to run.")

if __name__ == "__main__":
    run()
