
import psycopg2
import os
import sys

DATABASE_URL = os.environ.get("DATABASE_URL")

def analyze():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # Gurevitch: 25129
    # Gottesman: 2821
    
    principals = {
        'Gurevitch': 25129,
        'Gottesman': 2821
    }
    
    for name, pid in principals.items():
        print(f"\n--- Analyzing {name} ({pid}) ---")
        cur.execute("SELECT business_id FROM principal_business_links WHERE principal_id = %s", (pid,))
        bids = [r[0] for r in cur.fetchall()]
        print(f"  - Owns {len(bids)} businesses.")
        
        # Check emails of these businesses
        domain_counts = {}
        for bid in bids:
            cur.execute("SELECT business_email_address FROM businesses WHERE id = %s", (bid,))
            res = cur.fetchone()
            if res and res[0]:
                email = res[0]
                if '@' in email:
                    domain = email.split('@')[1].lower().strip()
                    domain_counts[domain] = domain_counts.get(domain, 0) + 1
                    
        # Sort domains
        sorted_domains = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)
        print(f"  - Top Email Domains: {sorted_domains[:5]}")
        
        # Check connectivity of top domains
        for domain, count in sorted_domains[:3]:
            # Count total businesses with this domain
            cur.execute("SELECT COUNT(*) FROM businesses WHERE business_email_address LIKE %s", (f"%@{domain}",))
            total = cur.fetchone()[0]
            print(f"    -> {domain}: Used by {total} businesses globally.")

    # Check Intersection
    g_bids = set()
    gw_bids = set()
    
    cur.execute("SELECT business_id FROM principal_business_links WHERE principal_id = 25129")
    g_bids = {r[0] for r in cur.fetchall()}
    
    cur.execute("SELECT business_id FROM principal_business_links WHERE principal_id = 2821")
    gw_bids = {r[0] for r in cur.fetchall()}
    
    print(f"\nShared Businesses: {g_bids & gw_bids}")
    
    # Check Shared Specific Emails (Any domain)
    print("\n--- Shared Email Address Check ---")
    if g_bids and gw_bids:
        query = """
            SELECT business_email_address FROM businesses 
            WHERE id IN %s AND business_email_address IS NOT NULL
            INTERSECT
            SELECT business_email_address FROM businesses 
            WHERE id IN %s AND business_email_address IS NOT NULL
        """
        cur.execute(query, (tuple(g_bids), tuple(gw_bids)))
        shared = cur.fetchall()
        print(f"Shared Specific Emails: {shared}")
    else:
         print("One principal has no businesses, cannot share email.")

if __name__ == "__main__":
    analyze()
