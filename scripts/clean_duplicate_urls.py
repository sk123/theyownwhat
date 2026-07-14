import os
import psycopg2

# Use standard environment variable or default to local development database URL
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

def main():
    print("Starting database cama_site_link clean-up script...")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cursor:
            # 1. New Haven instant PID restoration
            print("\nStep 1: Running New Haven PID URL restoration...")
            cursor.execute("""
                UPDATE properties 
                SET cama_site_link = 'https://gis.vgsi.com/newhavenct/Parcel.aspx?pid=' || SPLIT_PART(link, '.', 1) 
                WHERE UPPER(property_city) = 'NEW HAVEN' 
                  AND link IS NOT NULL 
                  AND link ~ '^[0-9]+(\.[0-9]+)?$';
            """)
            affected_rows = cursor.rowcount
            print(f"-> Successfully restored {affected_rows} New Haven properties to unique PID URLs.")

            # 2. Global de-duplication
            print("\nStep 2: Identifying duplicate cama_site_link URLs shared by more than 5 properties in the same city...")
            cursor.execute("""
                WITH duplicate_links AS (
                    SELECT property_city, cama_site_link, COUNT(*) as cnt
                    FROM properties
                    WHERE cama_site_link LIKE 'http%'
                    GROUP BY property_city, cama_site_link
                    HAVING COUNT(*) > 5
                )
                SELECT property_city, cama_site_link, cnt FROM duplicate_links ORDER BY cnt DESC;
            """)
            duplicates = cursor.fetchall()
            print(f"-> Found {len(duplicates)} unique URLs that are duplicated across multiple properties.")

            if duplicates:
                print("-> Resetting duplicate cama_site_link fields to NULL so they can be scraped correctly in Slow Path...")
                cursor.execute("""
                    WITH duplicate_links AS (
                        SELECT cama_site_link
                        FROM properties
                        WHERE cama_site_link LIKE 'http%'
                        GROUP BY property_city, cama_site_link
                        HAVING COUNT(*) > 5
                    )
                    UPDATE properties 
                    SET cama_site_link = NULL 
                    WHERE cama_site_link IN (SELECT cama_site_link FROM duplicate_links);
                """)
                nullified_rows = cursor.rowcount
                print(f"-> Successfully set cama_site_link to NULL for {nullified_rows} duplicated records.")
            else:
                print("-> No duplicate URLs found to nullify.")

            conn.commit()
            print("\nDone! Database clean-up completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error during clean-up: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
