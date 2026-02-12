
import psycopg2
import os
import sys

DATABASE_URL = os.environ.get("DATABASE_URL")

def check_gurevitch():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("🔍 Checking Principal Unity for Gurevitch...")
    # Find all variations
    cur.execute("SELECT principal_id, name_normalized FROM unique_principals WHERE name_normalized LIKE '%GUREVITCH%MENACHEM%' OR name_normalized LIKE '%MENACHEM%GUREVITCH%'")
    rows = cur.fetchall()
    print(f"  - Found Principals: {rows}")

    if not rows:
        print("❌ No principal found!")
        return

    p_ids = [str(r[0]) for r in rows]
    
    # Check if they are in the same network
    print(f"  - Checking network membership for IDs: {p_ids}")
    if len(p_ids) == 1:
        pid_sql = f"('{p_ids[0]}')"
    else:
        pid_sql = tuple(p_ids)
    
    cur.execute(f"SELECT network_id, entity_id FROM entity_networks WHERE entity_type='principal' AND entity_id IN {pid_sql}")
    net_rows = cur.fetchall()
    
    nets = set()
    for nr in net_rows:
        nets.add(nr[0])
        
    print(f"  - Belongs to Networks: {nets}")
    
    if len(nets) == 0:
        print("❌ Principal is NOT in any network (Orphaned?)")
    elif len(nets) > 1:
        print("❌ Fragmented! Gurevitch is split across multiple networks.")
    else:
        nid = list(nets)[0]
        print(f"✅ Unified in Network {nid}")
        
        # Check size
        cur.execute("SELECT total_properties, primary_name FROM networks WHERE id = %s", (nid,))
        res = cur.fetchone()
        if res:
            size = res[0]
            name = res[1]
            print(f"    - Network Name: {name}")
            print(f"    - Total Properties: {size}")
            
            if size > 800:
                print("    ✅ Size looks correct (>800).")
            else:
                print("    ⚠️ Size seems low (expected ~900).")
        else:
            print("    ❌ Network record not found!")

def check_greenfield():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    print("\n🔍 Checking Meganetwork Mitigation (Greenfield)...")
    
    # Check exact normalization or LIKE
    cur.execute("SELECT principal_id, name_normalized FROM unique_principals WHERE name_normalized LIKE '%GREENFIELD%ENTER%' LIMIT 1")
    row = cur.fetchone()
    
    if not row:
        print("❌ Greenfield principal not found!")
        return
        
    pid = row[0]
    name = row[1]
    print(f"  - Found Principal: {name} (ID: {pid})")
    
    # Find its network
    cur.execute("SELECT network_id FROM entity_networks WHERE entity_type='principal' AND entity_id = %s", (str(pid),))
    net_row = cur.fetchone()
    
    if not net_row:
        print("❌ Greenfield not in any network!")
        return
    
    nid = net_row[0]
    print(f"✅ Greenfield is in Network {nid}")
    
    # Check size
    cur.execute("SELECT total_properties, business_count, principal_count FROM networks WHERE id = %s", (nid,))
    res = cur.fetchone()
    if res:
        props, b_count, p_count = res
        print(f"    - Size: {props} properties")
        print(f"    - Businesses: {b_count}")
        print(f"    - Principals: {p_count}")
        
        if props > 10000:
             print("❌ STILL A MEGANETWORK! (>10000 props)")
        else:
             print("✅ Meganetwork BROKEN! (Size is reasonable)")

if __name__ == "__main__":
    check_gurevitch()
    check_greenfield()
