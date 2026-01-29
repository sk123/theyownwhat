import os
import psycopg2
from collections import Counter

DATABASE_URL = os.environ.get("DATABASE_URL")

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("--- Top 20 Principals by Business Count ---")
    cur.execute("SELECT name_c, COUNT(*) as c FROM principals GROUP BY name_c ORDER BY c DESC LIMIT 20")
    for row in cur.fetchall():
        print(f"{row[0]}: {row[1]}")

    print("\n--- Top 20 Mailing Addresses for Businesses ---")
    cur.execute("SELECT mail_address, COUNT(*) as c FROM businesses WHERE mail_address IS NOT NULL GROUP BY mail_address ORDER BY c DESC LIMIT 20")
    for row in cur.fetchall():
        print(f"{row[0]}: {row[1]}")

    print("\n--- Investigating DT ENTERPRISE LLC ---")
    cur.execute("SELECT id FROM businesses WHERE name ILIKE 'DT ENTERPRISE%'")
    biz_ids = [r[0] for r in cur.fetchall()]
    print(f"IDs: {biz_ids}")

    if biz_ids:
        cur.execute("SELECT name_c FROM principals WHERE business_id = ANY(%s)", (biz_ids,))
        principals = [r[0] for r in cur.fetchall()]
        print(f"Principals: {Counter(principals).most_common(10)}")

        cur.execute("SELECT mail_address FROM businesses WHERE id = ANY(%s)", (biz_ids,))
        addresses = [r[0] for r in cur.fetchall()]
        print(f"Addresses: {Counter(addresses).most_common(5)}")

    conn.close()

if __name__ == '__main__':
    main()
