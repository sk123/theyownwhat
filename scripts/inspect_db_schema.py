
import psycopg2
from psycopg2.extras import RealDictCursor

def inspect_schema():
    try:
        conn = psycopg2.connect(
            dbname="ctdata",
            user="user",
            password="password",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'properties';
        """)
        rows = cur.fetchall()
        print("--- Properties Table Schema ---")
        for r in rows:
            print(f"{r[0]} ({r[1]})")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_schema()
