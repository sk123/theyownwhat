import os
import psycopg2

DATABASE_URL = "postgresql://user:password@127.0.0.1:5432/ctdata"
try:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = cur.fetchall()
    print("PostgreSQL Tables found:", [t[0] for t in tables])
    conn.close()
except Exception as e:
    print("Failed to connect or query PG:", e)
