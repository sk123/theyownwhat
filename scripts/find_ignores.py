import os
import psycopg2
from collections import Counter
import re

DATABASE_URL = os.environ.get("DATABASE_URL")

def normalize_person_name(name):
    if not name: return ''
    n = name.upper().strip()
    n = re.sub(r"[,.'`\"]", '', n)
    return re.sub(r"\s+", ' ', n).strip()

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("--- Top 50 Domains ---")
    cur.execute("SELECT business_email_address FROM businesses WHERE business_email_address IS NOT NULL")
    domains = [r[0].split('@')[-1].lower() for r in cur.fetchall() if '@' in r[0]]
    for d, c in Counter(domains).most_common(50):
        print(f"{d}: {c}")

    print("\n--- Top 50 Principals ---")
    cur.execute("SELECT name_c, COUNT(*) as c FROM principals GROUP BY name_c ORDER BY c DESC LIMIT 50")
    for row in cur.fetchall():
        print(f"{row[0]}: {row[1]} (Norm: {normalize_person_name(row[0])})")

    conn.close()

if __name__ == '__main__':
    main()
