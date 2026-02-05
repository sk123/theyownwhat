
import psycopg2
from psycopg2.extras import RealDictCursor

def check_pomfret():
    try:
        conn = psycopg2.connect(
            dbname="ctdata",
            user="user",
            password="password",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT count(*) FROM properties WHERE property_city ILIKE 'POMFRET%'")
        count = cur.fetchone()['count']
        print(f"Pomfret properties: {count}")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_pomfret()
