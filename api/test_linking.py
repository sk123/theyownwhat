# test_linking.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import sys

# Add current dir to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from shared_utils import normalize_business_name, canonicalize_business_name

DATABASE_URL = os.environ.get("DATABASE_URL")

def test():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print("Loading 100 businesses...")
    cur.execute("SELECT id, name FROM businesses LIMIT 100")
    b_map = {normalize_business_name(r['name']): r['id'] for r in cur}
    
    print("Testing 100 properties...")
    cur.execute("SELECT id, owner, co_owner FROM properties LIMIT 100")
    rows = cur.fetchall()
    
    linked = 0
    for row in rows:
        onorm = normalize_business_name(row['owner'])
        bid = b_map.get(onorm)
        if bid:
            print(f"✅ Match: {row['owner']} -> {bid}")
            linked += 1
    
    print(f"Total linked: {linked}")

if __name__ == "__main__":
    test()
