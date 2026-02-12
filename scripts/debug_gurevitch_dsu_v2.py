import os
import sys
import psycopg2
from collections import deque, defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock keys
MENACHEM = 'principal_1224'
YEHUDA = 'principal_15600'

def run_debug():
    conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
    
    # 1. Build partial graph relevant to Gurevitch
    print("Building partial graph...")
    graph = defaultdict(set)
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT principal_id, business_id 
            FROM principal_business_links 
            WHERE principal_id IN ('1224', '15600')
        """)
        for pid, bid in cur.fetchall():
            u, v = f'principal_{pid}', f'business_{bid}'
            graph[u].add(v); graph[v].add(u)
            
        bids = [n.split('_')[1] for n in graph if n.startswith('business')]
        if bids:
            cur.execute("""
                SELECT principal_id, business_id 
                FROM principal_business_links 
                WHERE business_id IN %s
            """, (tuple(bids),))
            for pid, bid in cur.fetchall():
                u, v = f'principal_{pid}', f'business_{bid}'
                graph[u].add(v); graph[v].add(u)
    
    print(f"Graph built with {len(graph)} nodes")

    # 2. Multi-Source BFS + DSU Simulation
    seed_nodes = [MENACHEM, YEHUDA]
    MAX_DEPTH = 4
    
    parent = list(range(len(seed_nodes)))
    def find(i):
        if parent[i] == i: return i
        root = i
        while parent[root] != root: root = parent[root]
        curr = i
        while curr != root:
            parent[curr], curr = root, parent[curr]
        return root

    def union(i, j):
        root_i, root_j = find(i), find(j)
        if root_i != root_j:
            print(f"  Merging seed {root_i} and {root_j}...")
            parent[root_i] = root_j

    node_owner = {}
    queue = deque()
    
    for idx, seed in enumerate(seed_nodes):
        if seed not in node_owner:
            node_owner[seed] = idx
            queue.append((seed, 0, idx))
            
    print(f"Initialized queue with {len(queue)} seeds")
    
    while queue:
        node, depth, owner_idx = queue.popleft()
        
        if depth >= MAX_DEPTH: continue
        
        for neighbor in graph.get(node, []):
            if neighbor not in node_owner:
                node_owner[neighbor] = owner_idx
                queue.append((neighbor, depth + 1, owner_idx))
            else:
                existing_owner = node_owner[neighbor]
                if find(owner_idx) != find(existing_owner):
                    print(f"  Collision at {neighbor} (depth {depth+1}). Merging {owner_idx} and {existing_owner}")
                    union(owner_idx, existing_owner)
    
    # 3. Check results
    root_m = find(0)
    root_y = find(1)
    print(f"\nMenachem Root: {root_m}")
    print(f"Yehuda Root: {root_y}")
    
    if root_m == root_y:
        print("✅ SUCCESS: They are merged!")
    else:
        print("❌ FAILURE: They are disjoint.")

if __name__ == "__main__":
    run_debug()
