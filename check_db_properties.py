
import os
import psycopg2
import sys

DATABASE_URL = os.environ.get("DATABASE_URL")

def check_props():
    if not DATABASE_URL:
        print("No DATABASE_URL set")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    address = "145 BARKER ST%"
    print(f"Checking for properties matching: {address} in Hartford")
    
    cursor.execute(
        "SELECT id, location, unit, owner, link FROM properties WHERE location ILIKE %s AND property_city = 'Hartford' ORDER BY location LIMIT 100",
        (address,)
    )
    rows = cursor.fetchall()
    print(f"Found {len(rows)} rows matching address:")
    for r in rows:
        print(r)

    print("-" * 20)
    print("Checking city names like %artford%:")
    cursor.execute("SELECT DISTINCT property_city FROM properties WHERE property_city ILIKE '%artford%'")
    rows = cursor.fetchall()
    for r in rows:
        print(r)
        
    conn.close()

if __name__ == "__main__":
    check_props()
