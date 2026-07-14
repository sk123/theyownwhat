import os
import psycopg2
from psycopg2.extras import RealDictCursor

def main():
    db_url = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/ctdata')
    conn = psycopg2.connect(db_url)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query unique constraints on table properties
        cursor.execute("""
            SELECT conname, pg_get_constraintdef(c.oid) as condef
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE c.conrelid = 'properties'::regclass;
        """)
        print("Constraints:")
        for r in cursor.fetchall():
            print(dict(r))
            
        # Query indexes on table properties
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'properties';
        """)
        print("\nIndexes:")
        for r in cursor.fetchall():
            print(dict(r))
            
    finally:
        conn.close()

if __name__ == "__main__":
    main()
