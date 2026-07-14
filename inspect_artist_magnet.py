import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Select all columns from principals for business_id = '001t000000Wo9JgAAJ'
        cursor.execute("SELECT * FROM principals WHERE business_id = '001t000000Wo9JgAAJ';")
        for r in cursor.fetchall():
            print(dict(r))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
