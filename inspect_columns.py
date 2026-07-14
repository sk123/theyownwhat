
import os
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")

try:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'properties' 
        AND column_name IN ('business_id', 'principal_id', 'assessed_value');
    """)
    
    print("Properties Columns:")
    for row in cur.fetchall():
        print(row)

    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'entity_networks' 
        AND column_name IN ('entity_id', 'entity_type');
    """)
    
    print("\nEntity Networks Columns:")
    for row in cur.fetchall():
        print(row)
        
    conn.close()

except Exception as e:
    print(f"Error: {e}")
