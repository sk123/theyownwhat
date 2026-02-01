import psycopg2
import os
from datetime import datetime

def repair_missing_locations():
    """
    Repairs properties in New Haven and other affected towns with missing or invalid street addresses.
    Attempts to recover the property address using cama_site_link or other available data.
    """
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not DATABASE_URL:
        print("DATABASE_URL not set.")
        return
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    # Find properties in New Haven with missing or invalid location
    cur.execute("""
        SELECT id, cama_site_link, normalized_address, mailing_address
        FROM properties
        WHERE (property_city = 'New Haven' OR property_city = 'Groton')
          AND (location IS NULL OR location = '' OR location ~ '^\\d+$')
    """)
    rows = cur.fetchall()
    print(f"Found {len(rows)} properties with missing/invalid location.")
    for row in rows:
        pid, cama_link, norm_addr, mail_addr = row
        # Try to use normalized_address if it looks like a street address
        if norm_addr and not norm_addr.isdigit():
            new_loc = norm_addr
        elif mail_addr and not mail_addr.isdigit():
            new_loc = mail_addr
        else:
            new_loc = None
        if new_loc:
            cur.execute("UPDATE properties SET location = %s WHERE id = %s", (new_loc, pid))
            print(f"Updated property {pid} location to: {new_loc}")
    conn.commit()
    cur.close()
    conn.close()
    print("Repair complete.")

if __name__ == "__main__":
    repair_missing_locations()
