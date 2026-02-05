import os
import sys

# Minimal psycopg2 shim to handle running in environments without it
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Error: psycopg2 module not found. Run this in the correct environment (e.g., docker).")
    sys.exit(1)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("Error: DATABASE_URL not set.")
    sys.exit(1)

def check_berlin_data():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(cama_site_link) as count_link_not_null,
                    COUNT(NULLIF(cama_site_link, '')) as count_link_nonempty,
                    COUNT(building_photo) as count_photo_not_null,
                    COUNT(NULLIF(building_photo, '')) as count_photo_nonempty
                FROM properties 
                WHERE UPPER(property_city) = 'BERLIN'
            """)
            stats = cur.fetchone()
            print("\n--- BERLIN Property Stats ---")
            print(f"Total Properties: {stats['total']}")
            print(f"CAMA Links (Not Null): {stats['count_link_not_null']}")
            print(f"CAMA Links (Non-Empty): {stats['count_link_nonempty']}")
            print(f"Building Photos (Not Null): {stats['count_photo_not_null']}")
            print(f"Building Photos (Non-Empty): {stats['count_photo_nonempty']}")
            
            # Check Status
            cur.execute("SELECT * FROM data_source_status WHERE source_name = 'BERLIN'")
            status = cur.fetchone()
            print("\n--- Data Source Status ---")
            print(status)
            
            # Sample records
            cur.execute("SELECT id, cama_site_link, building_photo FROM properties WHERE UPPER(property_city) = 'BERLIN' LIMIT 5")
            print("\n--- Sample Records ---")
            for r in cur.fetchall():
                print(r)
                
    except Exception as e:
        print(f"DB Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    check_berlin_data()
