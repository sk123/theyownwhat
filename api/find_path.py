# find_path.py
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import deque, defaultdict
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

def find_path(start_name, end_name):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print(f"Searching for path between {start_name} and {end_name}...")
    
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
        if len(path) > 10: continue
        
        if node[0] == 'p' and end_name in node[1]:
            found.append(path)
            if len(found) > 3: break
            
        for neighbor in graph.get(node, []):
            if neighbor not in seen:
                seen.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    
    if found:
        for p in found:
            print(" -> ".join([str(x) for x in p]))
    else:
        print("No path found.")

if __name__ == "__main__":
    find_path('PAYAM ANDALIB', 'KAZEROUNIAN')
