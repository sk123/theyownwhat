
import os
import re
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import time
from bs4 import BeautifulSoup
import urllib3
import warnings

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Config
DB_URL = os.environ.get("DATABASE_URL")
# The link column usually holds the PID for Vision towns, but let me check. 
# Based on earlier select, 'link' was indeed the PID but sometimes float (e.g. 5466.0).

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    return psycopg2.connect(DB_URL)

def scrape_address(pid):
    # Vision URL for New Haven
    url = f"https://gis.vgsi.com/newhavenct/Parcel.aspx?pid={pid}"
    try:
        resp = requests.get(url, verify=False, timeout=10)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        selectors = ['MainContent_lblLocation', 'lblLocation', 'lblGenLocation', 'ctl00_MainContent_lblLocation']
        
        for sel in selectors:
            el = soup.find(id=sel)
            if el and el.text.strip():
                return el.text.strip()
            
        # Try generic span containing common street suffixes if ID fails
        # pass
        
    except Exception as e:
        logger.error(f"Failed to scrape PID {pid}: {e}")
    return None

def fix_93_properties():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    logger.info("Fetching '93' properties in New Haven...")
    # Select properties where location is literally "93"
    cursor.execute("""
        SELECT id, link 
        FROM properties 
        WHERE property_city = 'New Haven' 
          AND location = '93'
        ORDER BY id DESC
    """)
    props = cursor.fetchall()
    logger.info(f"Found {len(props)} properties to fix.")
    
    count = 0
    updated = 0
    
    for row in props:
        prop_id = row['id']
        raw_pid = row['link']
        
        if not raw_pid:
            continue
            
        # Clean PID: "5466.0" -> "5466"
        pid = str(raw_pid).split('.')[0]
        
        count += 1
        if count % 10 == 0:
            logger.info(f"Progress: {count}/{len(props)}...")
            
        real_addr = scrape_address(pid)
        
        if real_addr:
            cursor.execute("""
                UPDATE properties 
                SET location = %s, normalized_address = NULL 
                WHERE id = %s
            """, (real_addr, prop_id))
            conn.commit()
            updated += 1
            logger.info(f"Updated PID {pid}: {real_addr}")
        else:
            logger.warning(f"Could not scrape address for PID {pid}")
            
        time.sleep(0.5) # Be polite
        
    logger.info(f"Done. Updated {updated} properties.")
    conn.close()

if __name__ == "__main__":
    fix_93_properties()
