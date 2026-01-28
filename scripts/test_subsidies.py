
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal

DATABASE_URL = os.environ.get("DATABASE_URL")

def test_hartford_subsidies():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # 1. Find a property with subsidies
            cursor.execute("""
                SELECT p.id, p.location, p.property_city 
                FROM properties p 
                JOIN property_subsidies ps ON p.id = ps.property_id 
                WHERE p.property_city = 'Hartford' 
                LIMIT 1
            """)
            prop = cursor.fetchone()
            if not prop:
                print("No subsidized Hartford properties found.")
                return

            print(f"Testing property: {prop['id']} - {prop['location']}")

            # 2. Simulate the grouping logic
            from collections import defaultdict
            
            # This is a bit tricky because we'd need to import from main.py
            # But we can just run the query that group_properties_into_complexes uses.
            
            cursor.execute("""
                SELECT property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url
                FROM property_subsidies
                WHERE property_id = ANY(%s)
            """, ([prop['id']],))
            subsidies = cursor.fetchall()
            print(f"Subsidies found: {len(subsidies)}")
            for s in subsidies:
                print(f"  - {s['program_name']} ({s['subsidy_type']})")

    finally:
        conn.close()

if __name__ == "__main__":
    test_hartford_subsidies()
