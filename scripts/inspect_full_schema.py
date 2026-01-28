
import os
import psycopg2
from psycopg2.extras import RealDictCursor

def inspect_tables():
    db_url = os.getenv('DATABASE_URL')
    tables = ['properties', 'data_source_status', 'complex_management']
    try:
        conn = psycopg2.connect(db_url)
        for table in tables:
            print(f"\nSchema for table: {table}")
            with conn.cursor() as cur:
                cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
                rows = cur.fetchall()
                for row in rows:
                    print(f" - {row[0]}: {row[1]}")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_tables()
