
import psycopg2
from psycopg2.extras import RealDictCursor

def check_running():
    try:
        conn = psycopg2.connect(
            dbname="ctdata",
            user="user",
            password="password",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT source_name, refresh_status, last_refreshed_at FROM data_source_status WHERE refresh_status = 'running'")
        rows = cur.fetchall()
        if rows:
            print("RUNNING SCRAPERS:")
            for r in rows:
                print(f" - {r['source_name']} (started {r['last_refreshed_at']})")
        else:
            print("No scrapers are currently marked as 'running' in the database.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_running()
