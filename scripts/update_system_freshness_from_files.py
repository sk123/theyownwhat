import os
import sys
import datetime
import psycopg2
import json
# Append parent dir to path so we can import 'api'
# Append parent dir to path so we can import 'api'
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from api.db import get_db_connection

# Configuration
DATA_DIR = "/app/data"
FILES_TO_CHECK = {
    "businesses.csv": "BUSINESSES",
    "principals.csv": "PRINCIPALS"
}

def update_freshness():
    print("🚀 Starting one-time freshness update from file attributes...")
    
    conn = None
    try:
        # Get connection generator and consume it
        conn_gen = get_db_connection()
        conn = next(conn_gen)
        
        with conn.cursor() as cursor:
            for filename, source_name in FILES_TO_CHECK.items():
                filepath = os.path.join(DATA_DIR, filename)
                
                # Check if file exists
                if not os.path.exists(filepath):
                    # Try local path relative to script if not in container
                    local_path = os.path.join(os.path.dirname(__file__), '..', 'data', filename)
                    if os.path.exists(local_path):
                        filepath = local_path
                    else:
                        print(f"⚠️ File not found: {filename} (checked {filepath} and {local_path})")
                        continue

                # Get modification time
                mtime = os.path.getmtime(filepath)
                dt = datetime.datetime.fromtimestamp(mtime)
                
                print(f"✅ Found {filename}: Modified at {dt}")
                
                # Update DB
                cursor.execute("""
                    INSERT INTO data_source_status (source_name, source_type, last_refreshed_at, refresh_status, details)
                    VALUES (%s, 'system', %s, 'Success', %s)
                    ON CONFLICT (source_name) 
                    DO UPDATE SET 
                        last_refreshed_at = EXCLUDED.last_refreshed_at,
                        refresh_status = 'Success',
                        details = EXCLUDED.details;
                """, (source_name, dt, json.dumps({"message": "Updated from file modification time"})))
                
            conn.commit()
            print("🎉 Database updated successfully.")

    except Exception as e:
        print(f"❌ Error updating freshness: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            # Put connection back (if using pool logic) or just close if script
            # Since get_db_connection yields, we need to be careful.
            # But here we just let the script exit.
            pass

if __name__ == "__main__":
    # Ensure api.db can be imported
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    update_freshness()
