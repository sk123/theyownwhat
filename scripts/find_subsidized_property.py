
import os
import psycopg2

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def find_example():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            SELECT p.id, p.location, p.property_city, s.program_name, s.subsidy_type 
            FROM properties p
            JOIN property_subsidies s ON p.id = s.property_id
            LIMIT 5;
        """)
        rows = cur.fetchall()
        for row in rows:
            print(f"Property ID: {row[0]}, Address: {row[1]}, {row[2]}")
            print(f"  - Program: {row[3]} ({row[4]})")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    find_example()
