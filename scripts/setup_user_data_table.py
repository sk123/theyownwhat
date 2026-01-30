
import os
import psycopg2
from dotenv import load_dotenv

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def setup_db():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    print("Creating property_user_data table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS property_user_data (
            property_id INTEGER PRIMARY KEY REFERENCES properties(id) ON DELETE CASCADE,
            notes TEXT,
            photos JSONB DEFAULT '[]'::jsonb,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    print("✅ Table created.")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    setup_db()
