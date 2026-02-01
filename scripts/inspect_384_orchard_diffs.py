import os
import psycopg2
from collections import defaultdict

DB_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT * FROM properties WHERE property_city='New Haven' AND location ILIKE '%384 Orchard%' ORDER BY id")
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    diffs = defaultdict(set)
    for row in rows:
        for i, val in enumerate(row):
            diffs[colnames[i]].add(val)
    print(f"Total rows: {len(rows)}\n")
    for k, v in diffs.items():
        if len(v) > 1:
            vals = list(v)
            print(f"{k}: {len(v)} unique values")
            for val in vals[:10]:
                print(f"  - {val}")
            if len(vals) > 10:
                print("  ...")
            print()
    conn.close()

if __name__ == "__main__":
    main()
