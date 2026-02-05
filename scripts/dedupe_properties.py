import os
import psycopg2
from psycopg2.extras import RealDictCursor

DB_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

def get_db_connection():
    return psycopg2.connect(DB_URL)

def find_duplicates(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT property_city, location, cama_site_link, COUNT(*) as dup_count
            FROM properties
            GROUP BY property_city, location, cama_site_link
            HAVING COUNT(*) > 1
        """)
        return cursor.fetchall()

def dedupe(conn, dry_run=True):
    dups = find_duplicates(conn)
    print(f"Found {len(dups)} duplicate groups.")
    for group in dups:
        city = group['property_city']
        location = group['location']
        cama = group['cama_site_link']
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                SELECT *
                FROM properties
                WHERE property_city = %s AND location = %s AND cama_site_link = %s
                ORDER BY id
            """, (city, location, cama))
            rows = cursor.fetchall()
            # Skip if no rows found (avoid IndexError)
            if not rows:
                continue
            # Print all fields for inspection
            print(f"\nDuplicate group for {city}, {location}, cama_site_link={cama}:")
            for r in rows:
                print(r)
            # Keep the first row, delete the rest
            keep_id = rows[0]['id']
            delete_ids = [r['id'] for r in rows[1:]]
            if delete_ids:
                print(f"Deduping: keeping id {keep_id}, deleting {delete_ids}")
                if not dry_run:
                    cursor.execute("DELETE FROM properties WHERE id = ANY(%s)", (delete_ids,))
                    conn.commit()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Deduplicate properties table by (city, location, cama_site_link)")
    parser.add_argument('--apply', action='store_true', help='Actually delete duplicates (default is dry run)')
    args = parser.parse_args()
    conn = get_db_connection()
    try:
        dedupe(conn, dry_run=not args.apply)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
