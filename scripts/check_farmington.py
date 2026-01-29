
import os
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

print("Checking units for 6 TALCOTT FOREST RD in Farmington...")
cur.execute("""
    SELECT id, location, unit, owner, business_id 
    FROM properties 
    WHERE property_city = 'Farmington' 
      AND location LIKE '6 TALCOTT FOREST RD%'
    ORDER BY location;
""")
rows = cur.fetchall()

print(f"Found {len(rows)} rows:")
for r in rows:
    print(f"{r['location']} | {r['owner']} | {r['business_id']}")
