import psycopg2
import os

DATABASE_URL = "postgresql://user:password@ctdata_db:5432/ctdata"

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("--- Data Freshness Report (Most Recently Updated) ---")
cur.execute("SELECT source_name, source_type, last_refreshed_at, refresh_status FROM data_source_status ORDER BY last_refreshed_at DESC NULLS LAST LIMIT 15")
for row in cur.fetchall():
    print(f"{row[0]} ({row[1]}): {row[2]} [{row[3]}]")

conn.close()
