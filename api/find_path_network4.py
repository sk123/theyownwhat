import psycopg2
from psycopg2.extras import RealDictCursor
from collections import deque, defaultdict
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

def find_path_network4(g_id=None, v_id=None):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print(f"🔍 Loading Network 4 entities...")
    cur.execute("SELECT entity_type, entity_id, entity_name FROM entity_networks WHERE network_id = 4")
    network_nodes = cur.fetchall()
    
    valid_principals = set()
    valid_businesses = set()
    
    node_map = {} # (type, id) -> name

    for row in network_nodes:
        etype = row['entity_type']
        eid = row['entity_id']
        name = row['entity_name']
        node_map[(etype, eid)] = name
        
        if etype == 'principal':
            valid_principals.add(int(eid))
        elif etype == 'business':
            valid_businesses.add(str(eid)) # IDs are usually strings or ints, DB is int but sometimes treated as str

    print(f"  - Loaded {len(valid_principals)} principals and {len(valid_businesses)} businesses.")

    # Build Graph
    adj = defaultdict(set)

    # 1. P-B Links
    print("  - Loading P-B links...")
    if valid_principals:
        cur.execute(f"SELECT principal_id, business_id FROM principal_business_links WHERE principal_id IN {tuple(valid_principals)}")
        for row in cur:
            pid = row['principal_id']
            bid = str(row['business_id'])
            if bid in valid_businesses:
                p_node = ('principal', str(pid))
                b_node = ('business', bid)
                adj[p_node].add(b_node)
                adj[b_node].add(p_node)

    # 2. Shared Emails
    print("  - Loading Shared Emails...")
    if valid_businesses:
        cur.execute(f"SELECT id, business_email_address FROM businesses WHERE id IN {tuple(valid_businesses) if valid_businesses else '(0)'} AND business_email_address IS NOT NULL")
        email_to_bids = defaultdict(list)
        for row in cur:
            email = row['business_email_address'].strip().lower()
            if len(email) < 5: continue
            email_to_bids[email].append(str(row['id']))
            
        for email, bids in email_to_bids.items():
            if len(bids) > 1:
                # Create a virtual node for the email to see it in the path
                email_node = ('email', email)
                for bid in bids:
                    b_node = ('business', bid)
                    adj[b_node].add(email_node)
                    adj[email_node].add(b_node)

    # 3. Shared Addresses (Test Hypothesis)
    print("  - Loading Shared Addresses...")
    if valid_businesses:
        cur.execute(f"SELECT id, business_address, mail_address FROM businesses WHERE id IN {tuple(valid_businesses) if valid_businesses else '(0)'}")
        addr_to_bids = defaultdict(list)
        for row in cur:
            # Check both fields
            addrs = []
            if row['business_address']: addrs.append(row['business_address'].strip().upper())
            if row['mail_address']: addrs.append(row['mail_address'].strip().upper())
            
            for raw in addrs:
                if len(raw) < 5: continue
                # Simple normalization to match what network_builder likely did
                # (Assuming strict string match for now, or simple normalization)
                norm = " ".join(raw.split()) # Basic timeout
                addr_to_bids[norm].append(str(row['id']))

        for addr, bids in addr_to_bids.items():
            if len(bids) > 1:
                addr_node = ('address', addr)
                for bid in bids:
                    b_node = ('business', bid)
                    adj[b_node].add(addr_node)
                    adj[addr_node].add(b_node)

    # Find Start/End Nodes
    start_nodes = [('principal', str(g_id))] if g_id and ('principal', str(g_id)) in node_map else []
    end_nodes = [('principal', str(v_id))] if v_id and ('principal', str(v_id)) in node_map else []
    
    print(f"  - Start Nodes: {len(start_nodes)} ({[node_map.get(n) for n in start_nodes]})")
    print(f"  - End Nodes: {len(end_nodes)} ({[node_map.get(n) for n in end_nodes]})")
    
    if not start_nodes or not end_nodes:
        print("❌ Could not find start or end nodes in Network 4.")
        return

    # Component Analysis
    print("🚀 Analyzing Connected Components...")
    
    def get_component(start_nodes):
        q = deque(start_nodes)
        seen_nodes = set(start_nodes)
        component_nodes = set(start_nodes)
        
        while q:
            n = q.popleft()
            for neighbor in adj[n]:
                if neighbor not in seen_nodes:
                    seen_nodes.add(neighbor)
                    component_nodes.add(neighbor)
                    q.append(neighbor)
        return component_nodes

    print("  - Tracing Component for Gurevitch...")
    comp_g = get_component(start_nodes)
    print(f"    - Size: {len(comp_g)} nodes")
    
    print("  - Tracing Component for Vigliotti...")
    comp_v = get_component(end_nodes)
    print(f"    - Size: {len(comp_v)} nodes")
    
    intersection = comp_g.intersection(comp_v)
    if intersection:
        print(f"✅ Components Intersection Size: {len(intersection)}")
        print(f"    - Sample Intersection: {list(intersection)[:5]}")
    else:
        print("❌ Components are DISJOINT in this graph model.")
        print("    This implies missing edge types.")
        
        # Analyze what kinds of nodes are in the components
        def analyze_comp(comp, name):
            types = defaultdict(int)
            for t, _ in comp: types[t] += 1
            print(f"    - {name} Composition: {dict(types)}")
            
        analyze_comp(comp_g, "Gurevitch")
        analyze_comp(comp_v, "Vigliotti")

if __name__ == "__main__":
    find_path_network4("MENACHEM GUREVITCH", "ALEX VIGLIOTTI")
