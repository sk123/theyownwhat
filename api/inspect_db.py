import psycopg2
import os
from psycopg2.extras import RealDictCursor

def inspect():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set")
        return

    conn = psycopg2.connect(db_url)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    print("--- CITIES IN DB ---")
    cur.execute("SELECT property_city, COUNT(*) FROM properties GROUP BY property_city ORDER BY COUNT(*) DESC LIMIT 20")
    for row in cur.fetchall():
        print(row)

    print("\n--- STAMFORD SAMPLES ---")
    cur.execute("SELECT id, location, owner, mailing_address, property_city FROM properties WHERE UPPER(property_city) = 'STAMFORD' LIMIT 5")
    for row in cur.fetchall():
        print(row)

    print("\n--- NEW HAVEN SAMPLES ---")
    cur.execute("SELECT id, location, owner, mailing_address, property_city FROM properties WHERE UPPER(property_city) = 'NEW HAVEN' LIMIT 5")
    for row in cur.fetchall():
        print(row)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect()
