
import os
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

print("Checking ALL rows for Unit E...")
cur.execute("""
    SELECT *
    FROM properties 
    WHERE property_city = 'Farmington' 
      AND (location ILIKE '6 TALCOTT FOREST RD%E' OR unit = 'E')
""")
rows = cur.fetchall()

print(f"Found {len(rows)} rows:")
for r in rows:
    print(f"ID: {r['id']} | Loc: {r['location']} | Unit: {r['unit']} | Owner: {r['owner']} | BizID: {r['business_id']}")
