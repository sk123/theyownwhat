import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # List tables
        print("=== Tables ===")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        tables = [row['table_name'] for row in cursor.fetchall()]
        print(tables)
        
        # Describe each table
        for table in tables:
            print(f"\n=== Columns of '{table}' ===")
            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s;
            """, (table,))
            for col in cursor.fetchall():
                print(f"  {col['column_name']}: {col['data_type']} (Nullable: {col['is_nullable']})")
                
    finally:
        conn.close()

if __name__ == "__main__":
    main()
