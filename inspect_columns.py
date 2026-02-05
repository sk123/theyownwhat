import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")

def inspect_table(table_name):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
        cols = cur.fetchall()
        print(f"\n--- {table_name} Columns ---")
        for col in cols:
            print(f"{col[0]} ({col[1]})")
        conn.close()
    except Exception as e:
        print(f"Error inspecting {table_name}: {e}")

inspect_table('businesses')
inspect_table('principals')
inspect_table('networks')
