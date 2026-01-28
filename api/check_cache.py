
import os
import psycopg2
import json

DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("SELECT rank, network_name, primary_entity_name, property_count FROM cached_insights WHERE title='Statewide' AND rank <= 10 ORDER BY rank")
rows = cur.fetchall()

print(f"{'RANK':<5} {'NETWORK NAME':<40} {'PRIMARY ENTITY':<30} {'PROPS':<5}")
print("-" * 80)
for r in rows:
    print(f"{r[0]:<5} {r[1]:<40} {r[2]:<30} {r[3]:<5}")

print("\n--- Checking Address '93' ---")
# Check if any property has address '93'
cur.execute("SELECT id, location, normalized_address, property_city FROM properties WHERE location = '93' OR location LIKE '93 %' LIMIT 5")
bad_props = cur.fetchall()
for p in bad_props:
    print(p)

conn.close()
