
import psycopg2
from psycopg2.extras import RealDictCursor
import os

DATABASE_URL = os.environ.get("DATABASE_URL") or "postgresql://user:password@db:5432/ctdata"

def check_property():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Check for the user's specific address
    print("Searching for '304%BARBOUR'...")
    cur.execute("SELECT id, location, property_city, nhpd_id, complex_name FROM properties WHERE location ILIKE '%304%BARBOUR%'")
    props = cur.fetchall()
    
    print(f"Found {len(props)} properties.")
    for p in props:
        print(f"Property: {p}")
        cur.execute("SELECT * FROM property_subsidies WHERE property_id = %s", (p['id'],))
        subsidies = cur.fetchall()
        print(f"  Subsidies ({len(subsidies)}): {subsidies}")

    # Check for the one I found earlier "314-316 BARBOUR" just in case
    print("\nSearching for '314%BARBOUR'...")
    cur.execute("SELECT id, location, property_city, nhpd_id FROM properties WHERE location ILIKE '%314%BARBOUR%'")
    props = cur.fetchall()
    for p in props:
        print(f"Property: {p}")
        cur.execute("SELECT * FROM property_subsidies WHERE property_id = %s", (p['id'],))
        subsidies = cur.fetchall()
        print(f"  Subsidies ({len(subsidies)}): {subsidies}")

    conn.close()

if __name__ == "__main__":
    check_property()
