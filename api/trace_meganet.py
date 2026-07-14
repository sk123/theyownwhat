import psycopg2
import os
from collections import deque

def trace():
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    
    # Get network ID for the meganetwork
    cur.execute("SELECT id FROM networks WHERE primary_name = 'TCK WOOD WORK LLC' LIMIT 1")
    row = cur.fetchone()
    if not row:
        print("Meganetwork not found.")
        return
    net_id = row[0]
    print(f"Tracing network {net_id}...")
    
    # Get all entities in this network
    cur.execute("SELECT entity_type, entity_id, entity_name FROM entity_networks WHERE network_id = %s", (net_id,))
    entities = cur.fetchall()
    
    # Sample two random entities to trace between
    start = None
    end = None
    if len(entities) >= 2:
        start = (entities[0][0], entities[0][1], entities[0][2])
        end = (entities[-1][0], entities[-1][1], entities[-1][2])
    
    if not start or not end:
        print("Not enough entities to trace.")
        return
        
    print(f"Tracing from {start[2]} to {end[2]}")
    
    # Start building our link representation using the same logic as discover_networks
    # This requires running the same graph extraction
    import sys
    sys.path.append(os.path.dirname(__file__))
    from discover_networks import build_graph, build_address_edges
    
    graph, info, _ = build_graph(conn)
    addr_edges = build_address_edges(conn, graph)
    for u, v in addr_edges:
        graph[u].add(v)
        graph[v].add(u)
        
    # BFS
    # keys in graph are ('business', 'id') or ('principal', 'name')
    start_key = (start[0], start[1])
    end_key = (end[0], end[1])
    
    if start_key not in graph or end_key not in graph:
        print("Start or end not in graph.")
        return
        
    queue = deque([ (start_key, [start_key]) ])
    visited = {start_key}
    
    while queue:
        curr, path = queue.popleft()
        if curr == end_key:
            print("Found path:")
            for p in path:
                print(f" -> {info.get(p, {}).get('name', str(p))}")
            break
            
        for neighbor in graph.get(curr, []):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))

if __name__ == '__main__':
    trace()
