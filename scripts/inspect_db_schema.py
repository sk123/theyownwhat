
import os
import psycopg2

def inspect():
    db_url = os.getenv('DATABASE_URL')
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'properties'")
            cols = [r[0] for r in cur.fetchall()]
            print("Columns in 'properties' table:")
            for col in sorted(cols):
                print(f" - {col}")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect()
