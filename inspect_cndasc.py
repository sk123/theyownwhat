
import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    return psycopg2.connect(item)

# DATABASE_URL should be set in env
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

print("Searching for CNDASC units...")
cur.execute("SELECT * FROM properties WHERE unit LIKE '%CNDASC%' LIMIT 5;")
rows = cur.fetchall()

if not rows:
    print("No exact CNDASC match in 'unit'. Checking 'location'...")
    cur.execute("SELECT * FROM properties WHERE location LIKE '%CNDASC%' LIMIT 5;")
    rows = cur.fetchall()

for r in rows:
    print(r)
