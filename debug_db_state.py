
import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata" # Adjust for host

# Since we are on host, we might need to map port?
# docker-compose exposes 5432:5432.
# So localhost:5432 should work if the container is running.
# CAUTION: The user did NOT say the container is running right now.
# But "docker reports complete and healthy" implies it IS running.

def check_status():
    try:
        conn = psycopg2.connect(
            dbname="ctdata",
            user="user",
            password="password",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check counts
        cur.execute("SELECT count(*) FROM properties")
        props = cur.fetchone()['count']
        print(f"Properties count: {props}")
        
        cur.execute("SELECT count(*) FROM entity_networks")
        nets = cur.fetchone()['count']
        print(f"Entity Networks count: {nets}")
        
        # Check cache
        cur.execute("SELECT key, created_at, length(value::text) as len FROM kv_cache")
        rows = cur.fetchall()
        print("Cache keys:")
        for r in rows:
            print(f" - {r['key']}: {r['len']} bytes (created {r['created_at']})")
            
        conn.close()
    except Exception as e:
        print(f"Error connecting/querying: {e}")

if __name__ == "__main__":
    check_status()
