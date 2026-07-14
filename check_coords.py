import psycopg2
import os

DATABASE_URL = "postgresql://user:password@ctdata_db:5432/ctdata"

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cities = ['STAMFORD', 'BRIDGEPORT', 'NEW HAVEN', 'MERIDEN']
placeholders = ', '.join(['%s'] * len(cities))
cur.execute(f"SELECT UPPER(property_city), count(*) as total, count(latitude) as with_coords, count(location) as locations FROM properties WHERE UPPER(property_city) IN ({placeholders}) GROUP BY UPPER(property_city);", cities)

print(f"{'City':<15} | {'Total':<10} | {'With Coords':<15} | {'With Location':<15}")
print("-" * 65)
for row in cur.fetchall():
    print(f"{row[0]:<15} | {row[1]:<10} | {row[2]:<15} | {row[3]:<15}")

conn.close()
