import psycopg2
import psycopg2.extras
import os
import sys

def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])

def find_bridge(p1_id, p2_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print(f"Searching for bridge between Principal {p1_id} and {p2_id}...")

    # 1. Get Businesses for P1
    cur.execute("SELECT business_id FROM principal_business_links WHERE principal_id = %s", (p1_id,))
    p1_businesses = {row['business_id'] for row in cur.fetchall()}
    print(f"P1 has {len(p1_businesses)} businesses.")

    # 2. Get Businesses for P2
    cur.execute("SELECT business_id FROM principal_business_links WHERE principal_id = %s", (p2_id,))
    p2_businesses = {row['business_id'] for row in cur.fetchall()}
    print(f"P2 has {len(p2_businesses)} businesses.")

    # 3. Check for direct business intersection (already done, but good to double check)
    common_businesses = p1_businesses.intersection(p2_businesses)
    if common_businesses:
        print(f"FOUND DIRECT SHARED BUSINESSES: {common_businesses}")
        return

    # 4. Get Principals for P1's businesses
    cur.execute(f"SELECT up.principal_id, name_normalized FROM unique_principals up JOIN principal_business_links pbl ON up.principal_id = pbl.principal_id WHERE pbl.business_id IN {tuple(p1_businesses) if p1_businesses else '(0)'}")
    p1_principals = {row['principal_id']: row['name_normalized'] for row in cur.fetchall()}
    p1_principals.pop(p1_id, None) # Remove self

    # 5. Get Principals for P2's businesses
    cur.execute(f"SELECT up.principal_id, name_normalized FROM unique_principals up JOIN principal_business_links pbl ON up.principal_id = pbl.principal_id WHERE pbl.business_id IN {tuple(p2_businesses) if p2_businesses else '(0)'}")
    p2_principals = {row['principal_id']: row['name_normalized'] for row in cur.fetchall()}
    p2_principals.pop(p2_id, None) # Remove self

    # 6. Find Intersection
    common_principals = set(p1_principals.keys()).intersection(set(p2_principals.keys()))
    
    if common_principals:
        print(f"FOUND BRIDGE PRINCIPALS ({len(common_principals)}):")
        for pid in common_principals:
            print(f"  - ID: {pid}, Name: {p1_principals[pid]}")
    else:
        print("No bridge principals found at Depth 2.")

        # Optional: Check Depth 3 (Shared Emails between businesses)
        # B1 (P1) -- email -- B2 (P2)
        # We need to check if any of P1's businesses share an email with P2's businesses
        print("Checking for shared emails between businesses...")
        
        cur.execute(f"SELECT business_email_address FROM businesses WHERE id IN {tuple(p1_businesses) if p1_businesses else '(0)'} AND business_email_address IS NOT NULL")
        p1_emails = {row['business_email_address'].strip().lower() for row in cur.fetchall()}
        
        cur.execute(f"SELECT business_email_address FROM businesses WHERE id IN {tuple(p2_businesses) if p2_businesses else '(0)'} AND business_email_address IS NOT NULL")
        p2_emails = {row['business_email_address'].strip().lower() for row in cur.fetchall()}
        
        common_emails = p1_emails.intersection(p2_emails)
        if common_emails:
             print(f"FOUND BRIDGE EMAILS: {common_emails}")
        else:
             print("No bridge emails found.")

    conn.close()

if __name__ == "__main__":
    # Gurevitch (15532) and Vigliotti (42122)
    find_bridge(15532, 42122)
