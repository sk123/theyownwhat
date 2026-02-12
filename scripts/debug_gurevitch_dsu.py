import os
import sys
import psycopg2
from collections import deque, defaultdict
import logging

# Setup logging to see what's happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock keys
MENACHEM = 'principal_1224'
YEHUDA = 'principal_15600'
SHARED_BIZ = 'business_001eq00000f8703AAA'

def run_debug():
    conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
    
    # 1. Build partial graph relevant to Gurevitch
    print("Building partial graph...")
    graph = defaultdict(set)
    
    with conn.cursor() as cur:
        # Get all business links for Menachem/Yehuda
        cur.execute("""
            SELECT principal_id, business_id 
            FROM principal_business_links 
            WHERE principal_id IN ('1224', '15600')
        """)
        for pid, bid in cur.fetchall():
            u, v = f'principal_{pid}', f'business_{bid}'
            graph[u].add(v); graph[v].add(u)
            
        # Get all principals for those businesses (to get bridge connections)
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
    
    if SHARED_BIZ in graph:
        print(f"✅ Shared business {SHARED_BIZ} found in keys")
    else:
        print(f"❌ Shared business {SHARED_BIZ} NOT in keys")
        
    if MENACHEM in graph:
        print(f"✅ Menachem found in keys neighbors: {len(graph[MENACHEM])}")
    else:
        print("❌ Menachem NOT in keys")

    # 2. DSU Simulation
    parent = {}
    def find(i):
        if i not in parent:
            parent[i] = i
            return i
        root = i
        path = []
        while parent[root] != root:
            path.append(root)
            root = parent[root]
        for node in path:
            parent[node] = root
        return root

    def union(i, j):
        root_i = find(i)
        root_j = find(j)
        if root_i != root_j:
            print(f"  Merging {root_i} and {root_j} (via {i}, {j})")
            parent[root_i] = root_j

    # 3. Process Seeds
    # We simulate them as seeds
    seeds = [MENACHEM, YEHUDA]
    MAX_DEPTH = 4
    
    for seed in seeds:
        print(f"\nProcessing seed {seed}...")
        queue = deque([(seed, 0)])
        inner_seen = {seed}
        
        while queue:
            node, depth = queue.popleft()
            union(seed, node)
            
            if depth >= MAX_DEPTH: continue
            
            for neighbor in graph.get(node, []):
                if neighbor not in inner_seen:
                    inner_seen.add(neighbor)
                    queue.append((neighbor, depth + 1))
    
    # 4. Check results
    root_m = find(MENACHEM)
    root_y = find(YEHUDA)
    print(f"\nMenachem Root: {root_m}")
    print(f"Yehuda Root: {root_y}")
    
    if root_m == root_y:
        print("✅ SUCCESS: They are merged!")
    else:
        print("❌ FAILURE: They are disjoint.")

if __name__ == "__main__":
    run_debug()
