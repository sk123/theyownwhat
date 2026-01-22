
import os
import time
import sys
import psycopg2

# Unbuffered output
sys.stdout.reconfigure(line_buffering=True)

def get_counts(cursor):
    print("Executing COUNT query...", flush=True)
    cursor.execute("SELECT COUNT(*) FROM properties WHERE latitude IS NOT NULL AND longitude IS NOT NULL;")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM properties WHERE latitude = 0 AND longitude = 0;")
    failed = cursor.fetchone()[0]
    print("Counts retrieved.", flush=True)
    
    return total, failed

def main():
    print("Script started.", flush=True)
    database_url = os.environ.get("DATABASE_URL")
    print(f"Connecting to DB...", flush=True)
    try:
        conn = psycopg2.connect(database_url)
        print("Connected.", flush=True)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    cursor = conn.cursor()
    
    start_total, start_failed = get_counts(cursor)
    print(f"Initial: Total={start_total}, Failed(0,0)={start_failed}, Success={start_total - start_failed}")
    
    sleep_time = 20
    print(f"Waiting {sleep_time} seconds...", flush=True)
    time.sleep(sleep_time)
    
    end_total, end_failed = get_counts(cursor)
    
    delta_total = end_total - start_total
    delta_failed = end_failed - start_failed
    delta_success = delta_total - delta_failed
    
    print("-" * 30)
    print(f"Change in {sleep_time}s:")
    print(f"Total Processed: +{delta_total}")
    print(f"Successful:      +{delta_success}")
    print(f"Failed (0,0):    +{delta_failed}")
    
    if delta_total > 0:
        rate = delta_total / sleep_time
        print(f"Rate: {rate:.2f} properties/sec")
        print(f"Est. hourly rate: {rate * 3600:.0f} properties/hour")
    else:
        print("No change detected.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
