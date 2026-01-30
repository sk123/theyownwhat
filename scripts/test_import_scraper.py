
import os
import requests
import psycopg2
from bs4 import BeautifulSoup
from updater.update_data import scrape_individual_property_page

def test():
    url = "https://gis.vgsi.com/WinchesterCT/Parcel.aspx?pid=3678"
    print(f"Testing URL: {url}")
    
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    
    # The function expects (url, session, referer)
    referer = "https://gis.vgsi.com/winchesterct/Streets.aspx?Letter=M"
    
    db_url = os.getenv('DATABASE_URL')
    conn = psycopg2.connect(db_url)
    
    # We need the ID for this PID in the DB
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM properties WHERE property_city = 'WINCHESTER' AND location ILIKE '%MOUNTAIN%' LIMIT 1")
        row = cur.fetchone()
        if not row:
            print("Property not found in DB to update.")
            return
        db_id = row[0]
        print(f"Found DB ID {db_id} for Winchester Mountain property.")

    data = scrape_individual_property_page(url, session, referer)
    
    if data:
        print("Scraper success. Attempting DB update...")
        from updater.update_data import update_property_in_db
        success = update_property_in_db(conn, db_id, data, restricted_mode=False)
        print(f"DB Update success: {success}")
    else:
        print("FAILURE: Function returned None or empty.")
    
    conn.close()

if __name__ == "__main__":
    test()
