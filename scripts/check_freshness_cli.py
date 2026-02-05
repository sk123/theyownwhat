import os
import psycopg2
from psycopg2.extras import RealDictCursor
import sys

# Configure logging
try:
    DATABASE_URL = os.environ.get("DATABASE_URL")
    # If not set, try to find it or default? 
    # Usually it's in env.
    conn = psycopg2.connect(DATABASE_URL)
except Exception as e:
    print(f"Error connecting to DB: {e}")
    sys.exit(1)

def check_status():
    print(f"\n--- Checking Data Source Status ---")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find stale sources (older than 30 days or never refreshed)
        # Or status != success
        cur.execute("""
            SELECT source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details
            FROM data_source_status
            ORDER BY last_refreshed_at ASC NULLS FIRST
            LIMIT 20
        """)
        
        rows = cur.fetchall()
        print(f"{'Source':<20} | {'Type':<15} | {'Last Refreshed':<20} | {'Status':<10} | {'Details'}")
        print("-" * 100)
        for r in rows:
            last = str(r['last_refreshed_at'])[:19] if r['last_refreshed_at'] else "NEVER"
            print(f"{r['source_name']:<20} | {r['source_type']:<15} | {last:<20} | {r['refresh_status']:<10} | {r['details']}")

if __name__ == "__main__":
    check_status()
