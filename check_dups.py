
import os
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

print("Checking for duplicate 'Unit 0304' for CT MAY APARTMENTS LLC...")
cur.execute("""
    SELECT id, location, unit, property_city, assessed_value 
    FROM properties 
    WHERE owner ILIKE '%CT MAY APARTMENTS%' 
      AND (unit ILIKE '%0304%' OR location ILIKE '%0304%')
    LIMIT 20;
""")
rows = cur.fetchall()

print(f"Found {len(rows)} rows:")
for r in rows:
    print(r)

print("\nChecking exact duplicate locations count:")
cur.execute("""
    SELECT location, COUNT(*) 
    FROM properties 
    WHERE owner ILIKE '%CT MAY APARTMENTS%'
    GROUP BY location 
    HAVING COUNT(*) > 1
    LIMIT 10;
""")
dups = cur.fetchall()
for d in dups:
    print(d)
