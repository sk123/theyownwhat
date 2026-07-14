import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Let's inspect the unique_principals table schema and data types
        cursor.execute("SELECT * FROM unique_principals LIMIT 5;")
        print("=== unique_principals sample ===")
        for r in cursor.fetchall():
            print(dict(r))
            
        # Let's inspect the principals table schema and data types
        cursor.execute("SELECT * FROM principals LIMIT 5;")
        print("\n=== principals sample ===")
        for r in cursor.fetchall():
            print(dict(r))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
