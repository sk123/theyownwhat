import psycopg2
import os

DATABASE_URL = "postgresql://user:password@ctdata_db:5432/ctdata"

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("--- Actual Data (Top 10 Cities by Count) ---")
cur.execute("SELECT property_city, count(*) as cnt FROM properties GROUP BY property_city ORDER BY cnt DESC LIMIT 10")
for row in cur.fetchall():
    print(f"{row[0]}: {row[1]}")

print("\n--- Data Freshness Report (Oldest 10) ---")
cur.execute("SELECT source_name, source_type, last_refreshed_at, refresh_status FROM data_source_status ORDER BY last_refreshed_at ASC NULLS FIRST LIMIT 10")
for row in cur.fetchall():
    print(f"{row[0]} ({row[1]}): {row[2]} [{row[3]}]")

conn.close()
