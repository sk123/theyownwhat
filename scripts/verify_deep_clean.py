import psycopg2
import json

def verify_deep_clean():
    conn = psycopg2.connect(dbname='ctdata', user='user', host='ctdata_db', password='password')
    cur = conn.cursor()

    print("--- VERIFICATION ---")

    # 1. Check for Institutional Entities in Networks
    print("\n1. Checking for BANNED entities in networks...")
    banned_keywords = ['UNIVERSITY', 'COLLEGE', 'HOSPITAL', 'LODGING', 'HOTEL'] # 'NEW SAMARITAN' is now checked separately
    
    for kw in banned_keywords:
        cur.execute(f"SELECT network_name, rank FROM cached_insights WHERE network_name ILIKE '%{kw}%'")
        rows = cur.fetchall()
        if rows:
            print(f"❌ FAILED: Found {kw} in top networks: {rows}")
        else:
            print(f"✅ PASSED: No top networks match '{kw}'")

    # Specific check for NEW SAMARITAN
    cur.execute(f"SELECT COUNT(*) FROM cached_insights WHERE title='Statewide' AND network_name ILIKE '%NEW SAMARITAN%'")
    if cur.fetchone()[0] == 0:
        print("✅ PASSED: No top networks match 'NEW SAMARITAN'")
    else:
        print("❌ FAILED: Found 'NEW SAMARITAN' in top networks")

    # Check for C/O, CIO, CARE OF, ATTN
    cur.execute(f"SELECT COUNT(*) FROM cached_insights WHERE title='Statewide' AND network_name ~* '^\s*(C/O|CIO|CARE OF|ATTN)'")
    count_co = cur.fetchone()[0]
    if count_co == 0:
        print("✅ PASSED: No top networks start with 'C/O' or 'ATTN'")
    else:
        print(f"❌ FAILED: Found {count_co} networks starting with 'C/O' or 'ATTN'")

    print("\n2. Checking specific network composition...")
    
    # Kathleen Wheeler - Check for Fairfield U properties
    cur.execute("""
        SELECT COUNT(*) FROM properties p
        JOIN entity_networks en ON p.business_id::text = en.entity_id
        WHERE en.network_id::text IN (SELECT network_id FROM cached_insights WHERE network_name ILIKE '%KATHLEEN WHEELER%')
        AND p.owner ILIKE '%FAIRFIELD UNIV%'
    """)
    count = cur.fetchone()[0]
    if count > 0:
        print(f"❌ FAILED: Kathleen Wheeler still linked to {count} Fairfield U properties.")
    else:
        print("✅ PASSED: Kathleen Wheeler clean of Fairfield U.")

    print("\n3. Checking Sort Order (Residential Value)...")
    cur.execute("""
        SELECT rank, network_name, total_assessed_value, residential_assessed_value
        FROM cached_insights
        WHERE title = 'Statewide'
        ORDER BY rank ASC
        LIMIT 5
    """)
    rows = cur.fetchall()
    prev_val = float('inf')
    for r in rows:
        val = float(r[3] or 0) # residential
        print(f"   Rank {r[0]}: {r[1]} - ResValue: ${val:,.0f} (Total: ${float(r[2] or 0):,.0f})")
        if val > prev_val:
            print(f"❌ FAILED: Sort order violation at Rank {r[0]}")
        prev_val = val
    
    print("\n4. AUDIT: Top 50 Networks (Visual Inspection)")
    print("-" * 60)
    print(f"{'Rank':<5} | {'Network Name':<40} | {'Res Value':<15} | {'Props'}")
    print("-" * 60)
    
    cur.execute("""
        SELECT rank, network_name, residential_assessed_value, property_count, network_id
        FROM cached_insights
        WHERE title = 'Statewide'
        ORDER BY rank ASC
        LIMIT 50
    """)
    rows = cur.fetchall()
    for r in rows:
        r_val = float(r[2] or 0)
        # Format commas first, then pad
        r_val_str = f"${r_val:,.0f}"
        print(f"{r[0]:<5} | {r[1][:40]:<40} | {r_val_str:<15} | {r[3]}")
    print("-" * 60)

    # DEBUG: Check property types for Rank 1
    if rows:
        top_id = rows[0][4]
        print(f"\n🔬 DEBUG: Property Types for Rank 1 ({rows[0][1]}) - Network ID: {top_id}")
        query = """
            SELECT p.property_type, COUNT(*), SUM(p.assessed_value)
            FROM properties p
            JOIN entity_networks en ON (
                (en.entity_type = 'business' AND p.business_id::text = en.entity_id) OR
                (en.entity_type = 'principal' AND p.principal_id::text = en.entity_id)
            )
            WHERE en.network_id = %s
            GROUP BY p.property_type
        """
        cur.execute(query, (top_id,))
        for pt in cur.fetchall():
            print(f"   - {pt[0]}: {pt[1]} props, ${float(pt[2] or 0):,.0f}")


    conn.close()

if __name__ == "__main__":
    verify_deep_clean()
