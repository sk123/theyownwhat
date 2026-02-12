
import psycopg2
import os
import sys

def verify_fixes():
    conn = psycopg2.connect(dsn=os.environ["DATABASE_URL"])
    cur = conn.cursor()
    
    print("--- VERIFICATION REPORT ---")
    
    # 1. CHECK MEGANETWORK SIZE
    # The old Meganetwork was Network 5 with ~$27B
    cur.execute("SELECT id, total_properties, total_assessed_value, primary_name FROM networks ORDER BY total_assessed_value DESC LIMIT 1;")
    largest = cur.fetchone()
    print(f"\n1. LARGEST NETWORK:")
    print(f"   ID: {largest[0]}")
    print(f"   Properties: {largest[1]}")
    print(f"   Value: ${float(largest[2]):,.2f}")
    print(f"   Name: {largest[3]}")
    
    if float(largest[2]) < 2000000000:
        print("   ✅ PASS: Meganetwork Broken (< $2B)")
    else:
        print("   ❌ FAIL: Meganetwork Persists (> $2B)")

    # 2. CHECK MENACHEM GUREVITCH (Should be ~$1.3B - $1.5B)
    cur.execute("""
        SELECT n.id, n.total_properties, n.total_assessed_value, n.primary_name 
        FROM entity_networks en
        JOIN networks n ON en.network_id = n.id
        WHERE en.entity_name = 'MENACHEM GUREVITCH' AND en.entity_type = 'principal'
    """)
    mg = cur.fetchone()
    if mg:
        print(f"\n2. MENACHEM GUREVITCH NETWORK:")
        print(f"   ID: {mg[0]}")
        print(f"   Properties: {mg[1]}")
        print(f"   Value: ${float(mg[2]):,.2f}")
        print(f"   Name: {mg[3]}")
        
        if 500 < mg[1] < 2000:
            print("   ✅ PASS: Network Unified (Correct Property Count)")
        else:
            print("   ❌ FAIL: Network Fragmented or Bloated")
    else:
        print("\n2. MENACHEM GUREVITCH: ❌ Not Found in Graph")

    # 3. CHECK SFR 2 DE LLC (Should link to NETZ/MANDY)
    cur.execute("""
        SELECT n.id, n.primary_name 
        FROM entity_networks en 
        JOIN networks n ON en.network_id = n.id
        WHERE en.entity_name = 'SFR 2 DE LLC'
    """)
    sfr = cur.fetchone()
    if sfr:
        sfr_id = sfr[0]
        # Check if Netz is in the same network
        cur.execute(f"SELECT COUNT(*) FROM entity_networks WHERE network_id = {sfr_id} AND entity_name LIKE '%NETZ%'")
        netz_count = cur.fetchone()[0]
        
        print(f"\n3. SFR 2 DE LLC CONTEXT:")
        print(f"   Network ID: {sfr_id}")
        print(f"   Network Name: {sfr[1]}")
        print(f"   Connected 'NETZ' Entities: {netz_count}")
        
        if netz_count > 0:
            print("   ✅ PASS: Connected to NETZ")
        else:
            print("   ❌ FAIL: Isolated from NETZ")
    else:
        print("\n3. SFR 2 DE LLC: ❌ Not Found")
        
    conn.close()

if __name__ == "__main__":
    verify_fixes()
