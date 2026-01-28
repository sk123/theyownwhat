
import os
import psycopg2
from datetime import date

def check():
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("DATABASE_URL not found.")
        return
    
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.property_city, count(*) 
                FROM properties p
                JOIN property_processing_log ppl ON p.id = ppl.property_id
                WHERE ppl.last_processed_date = CURRENT_DATE 
                GROUP BY p.property_city
                ORDER BY count DESC;
            """)
            rows = cur.fetchall()
            print(f"Updates for {date.today()}:")
            for row in rows:
                print(f" - {row[0]}: {row[1]} properties")
            
            if not rows:
                print("No properties processed today yet.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check()
