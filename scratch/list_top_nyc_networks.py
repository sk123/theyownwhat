import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT id, display_name, building_count, unit_count, member_names[:3] as member_names_sample, member_addresses[:2] as member_addresses_sample
            FROM nyc_networks
            ORDER BY unit_count DESC
            LIMIT 10;
        """)
        print("=== TOP 10 NYC NETWORKS ===")
        for r in cursor.fetchall():
            print(dict(r))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
