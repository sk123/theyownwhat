
import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

def inspect_address(address):
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT id, location FROM properties WHERE location = %s", (address,))
            prop = cursor.fetchone()
            if not prop:
                print(f"Property {address} not found.")
                # Try fuzzy
                cursor.execute("SELECT id, location FROM properties WHERE location LIKE %s LIMIT 5", (f"%{address}%",))
                results = cursor.fetchall()
                if results:
                    print("Did you mean:")
                    for r in results:
                        print(f"  - {r['location']} (id: {r['id']})")
                return

            print(f"Property: {prop['id']} - {prop['location']}")
            cursor.execute("""
                SELECT * FROM property_subsidies WHERE property_id = %s
            """, (prop['id'],))
            subsidies = cursor.fetchall()
            print(f"Subsidies found: {len(subsidies)}")
            for s in subsidies:
                print(f"\nProgram: {s['program_name']}")
                print(f"Type: {s['subsidy_type']}")
                print(f"Units: {s['units_subsidized']}")
                print(f"Expiry: {s['expiry_date']}")
                print(f"Source: {s['source_url']}")

    finally:
        conn.close()

if __name__ == "__main__":
    inspect_address("150 MANHAN ST")
