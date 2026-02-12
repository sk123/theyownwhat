import os
import psycopg2
from collections import defaultdict
from api.db import get_db_connection
from api.shared_utils import normalize_mailing_address, get_email_match_key

def analyze_meganet():
    conn = get_db_connection()
    if hasattr(conn, '__next__'): conn = next(conn)
    cur = conn.cursor()

    NETWORK_ID = '1'

    print(f"--- Analyzing Network {NETWORK_ID} ---")
    
    # 1. Get all members
    print("Fetching members...")
    cur.execute("SELECT entity_type, entity_id FROM entity_networks WHERE network_id = %s", (NETWORK_ID,))
    members = cur.fetchall()
    print(f"Total Members: {len(members)}")
    
    principal_ids = set()
    business_ids = set()
    
    for etype, eid in members:
        if etype == 'principal':
            try:
                principal_ids.add(int(eid))
            except: pass
        else:
            business_ids.add(eid)
            
    print(f"Principals: {len(principal_ids)}, Businesses: {len(business_ids)}")

    # 2. Build Graph (Adjacency List)
    adj = defaultdict(set)
    
    def add_edge(u, v, type=''):
        adj[u].add(v)
        adj[v].add(u)

    # A. Principal-Business Links
    print("Fetching PBL links...")
    if principal_ids:
        cur.execute(f"""
            SELECT principal_id, business_id FROM principal_business_links 
            WHERE principal_id IN %s
        """, (tuple(principal_ids),))
        pbl_links = cur.fetchall()
        print(f"PBL Links found: {len(pbl_links)}")
        for pid, bid in pbl_links:
            if bid in business_ids:
                add_edge(f"P:{pid}", f"B:{bid}", type='ownership')

    # B. Shared Emails
    print("Fetching Emails...")
    if business_ids:
        cur.execute("SELECT domain, match_type FROM email_match_rules")
        email_rules = {row[0]: row[1] for row in cur.fetchall()}
        
        cur.execute(f"SELECT id, business_email_address FROM businesses WHERE id IN %s", (tuple(business_ids),))
        email_map = defaultdict(list)
        for bid, email in cur.fetchall():
            key = get_email_match_key(email, email_rules)
            if key:
                email_map[key].append(bid)
                
        print(f"Email Clusters: {len(email_map)}")
        for key, bids in email_map.items():
            if len(bids) > 1:
                # Add a dummy node for the email to see centrality
                email_node = f"Email:{key}"
                for bid in bids:
                    add_edge(f"B:{bid}", email_node, type='email')

    # C. Shared Addresses
    # DISABLED to match network_builder.py
    # print("Fetching Addresses...")
    # if business_ids:
    #     cur.execute(f"SELECT id, mail_address, business_address FROM businesses WHERE id IN %s", (tuple(business_ids),))
    #     addr_map = defaultdict(list)
    #     for bid, mail, phys in cur.fetchall():
    #         raw = mail or phys
    #         norm = normalize_mailing_address(raw)
    #         if norm and len(norm) > 10:
    #             addr_map[norm].append(bid)
    #             
    #     print(f"Address Clusters: {len(addr_map)}")
    #     for addr, bids in addr_map.items():
    #         if len(bids) > 1:
    #             addr_node = f"Addr:{addr}"
    #             for bid in bids:
    #                 add_edge(f"B:{bid}", addr_node, type='address')

    # 3. Analyze manually without networkx
    print(f"Graph Built. Nodes: {len(adj)}, Edges: {sum(len(v) for v in adj.values()) // 2}")
    
    if not adj:
        print("Empty Graph!")
        return

    print("\n--- Top 20 Nodes by Degree ---")
    degrees = sorted([(n, len(neighbors)) for n, neighbors in adj.items()], key=lambda x: x[1], reverse=True)[:20]
    for n, d in degrees:
        print(f"{n}: {d}")

    # Simple component analysis (BFS)
    visited = set()
    components = []
    for node in adj:
        if node not in visited:
            component = set()
            queue = [node]
            visited.add(node)
            while queue:
                curr = queue.pop(0)
                component.add(curr)
                for neighbor in adj[curr]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append(neighbor)
            components.append(component)
            
    components.sort(key=len, reverse=True)
    print(f"\nNumber of components within net members: {len(components)}")
    print(f"Largest component size: {len(components[0]) if components else 0}")
    
    # Check "LITTLE BRANCH" node
    cur.execute("SELECT id FROM businesses WHERE name ILIKE '%LITTLE BRANCH%'")
    lb_ids = [r[0] for r in cur.fetchall()]
    print(f"\nLittle Branch IDs: {lb_ids}")
    for bid in lb_ids:
        node = f"B:{bid}"
        if node in adj:
            print(f"Little Branch ({node}) Degree: {len(adj[node])}")
            print(f"Neighbors: {list(adj[node])[:10]}...") 

if __name__ == "__main__":
    analyze_meganet()
