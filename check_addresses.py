
import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")

try:
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        print("Checking Business Address Statistics...")
        
        cur.execute("SELECT COUNT(*) FROM businesses;")
        total = cur.fetchone()[0]
        print(f"Total Businesses: {total}")
        
        cur.execute("SELECT COUNT(*) FROM businesses WHERE mail_address IS NOT NULL AND mail_address != '';")
        has_mail = cur.fetchone()[0]
        print(f"With Mail Address: {has_mail} ({has_mail/total*100:.1f}%)")
        
        cur.execute("SELECT COUNT(*) FROM businesses WHERE business_address IS NOT NULL AND business_address != '';")
        has_biz = cur.fetchone()[0]
        print(f"With Business Address: {has_biz} ({has_biz/total*100:.1f}%)")
        
        cur.execute("SELECT COUNT(*) FROM businesses WHERE principal_address IS NOT NULL AND principal_address != '';")
        has_prin = cur.fetchone()[0]
        print(f"With Principal Address: {has_prin} ({has_prin/total*100:.1f}%)")

        print("\nTop 10 Most Common Mailing Addresses:")
        cur.execute("""
            SELECT mail_address, COUNT(*) as c 
            FROM businesses 
            WHERE mail_address IS NOT NULL AND mail_address != ''
            GROUP BY mail_address 
            ORDER BY c DESC 
            LIMIT 10;
        """)
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}")

except Exception as e:
    print(f"Error: {e}")
