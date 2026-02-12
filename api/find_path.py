# find_path.py
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import deque, defaultdict
import os
import argparse

DATABASE_URL = os.environ.get("DATABASE_URL")

def find_path(start_name, end_name, depth=10):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print(f"Searching for path between {start_name} and {end_name} (Max Depth: {depth})...")
    
    # Load links
    cur.execute("""
        SELECT up.name_normalized, pbl.business_id
        FROM principal_business_links pbl
        JOIN unique_principals up ON up.principal_id = pbl.principal_id
    """)
    links = cur.fetchall()
    
    graph = defaultdict(set)
    for row in links:
        p = row['name_normalized']
        b = row['business_id']
        graph[('p', p)].add(('b', b))
        graph[('b', b)].add(('p', p))
    
    # BFS
    queue = deque([(('p', start_name), [('p', start_name)])])
    seen = {('p', start_name)}
    
    found = []
    while queue:
        node, path = queue.popleft()
        if len(path) > depth: continue
        
        # Check if we reached a principal containing end_name
        if node[0] == 'p' and end_name.upper() in node[1].upper():
            found.append(path)
            if len(found) > 3: break
            
        for neighbor in graph.get(node, []):
            if neighbor not in seen:
                seen.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    
    if found:
        print(f"✅ Found {len(found)} paths:")
        for p in found:
            print(" -> ".join([str(x) for x in p]))
    else:
        print("❌ No path found.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--depth", type=int, default=10)
    args = parser.parse_args()
    
    find_path(args.start, args.end, args.depth)
