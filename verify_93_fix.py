import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

def verify_fix():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Check for remaining "93" addresses in New Haven
            cursor.execute("""
                SELECT count(*) FROM properties 
                WHERE UPPER(property_city) = 'NEW HAVEN' 
                AND location = '93';
            """)
            count_93 = cursor.fetchone()['count']
            print(f"Properties in New Haven with location '93': {count_93}")

            # Check for properties in New Haven with valid-looking locations that were updated recently
            cursor.execute("""
                SELECT p.location, p.cama_site_link, ppl.last_processed_date 
                FROM properties p
                JOIN property_processing_log ppl ON p.id = ppl.property_id
                WHERE UPPER(p.property_city) = 'NEW HAVEN' 
                AND p.location != '93'
                AND ppl.last_processed_date >= CURRENT_DATE - INTERVAL '2 days'
                LIMIT 10;
            """)
            recent_updates = cursor.fetchall()
            print(f"Recently updated properties in New Haven (sample):")
            for prop in recent_updates:
                print(f"  - Location: {prop['location']}, URL: {prop['cama_site_link']}, Updated: {prop['last_processed_date']}")

    finally:
        conn.close()

if __name__ == "__main__":
    verify_fix()
