import os
import sys
import requests
import logging
import shutil
import psycopg2
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config
DATA_DIR = "/app/data"
SOURCES = {
    "businesses": {
        "url": "https://data.ct.gov/api/views/n7gp-d28j/rows.csv?accessType=DOWNLOAD",
        "filename": "businesses.csv",
        "table": "businesses",
        "name": "BUSINESSES"
    },
    "principals": {
        "url": "https://data.ct.gov/api/views/ka36-64k6/rows.csv?accessType=DOWNLOAD",
        "filename": "principals.csv",
        "table": "principals",
        "name": "PRINCIPALS"
    }
}

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def update_status(source_name, status, details=None):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_source_status (source_name, source_type, last_refreshed_at, refresh_status, details)
                VALUES (%s, 'system', NOW(), %s, %s)
                ON CONFLICT (source_name) 
                DO UPDATE SET 
                    last_refreshed_at = NOW(),
                    refresh_status = EXCLUDED.refresh_status,
                    details = EXCLUDED.details;
            """, (source_name, status, details))
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"Failed to update status for {source_name}: {e}")

def download_file(url, target_path):
    logger.info(f"Downloading {url} to {target_path}...")
    temp_path = target_path + ".tmp"
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(temp_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        shutil.move(temp_path, target_path)
        logger.info(f"Download complete: {target_path}")
        return True
    except Exception as e:
        logger.error(f"Download failed: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False

def refresh_system_data():
    logger.info("Starting System Data Refresh...")
    
    # 1. Download Files
    for key, config in SOURCES.items():
        update_status(config["name"], "Downloading")
        target_path = os.path.join(DATA_DIR, config["filename"])
        
        if download_file(config["url"], target_path):
            update_status(config["name"], "Importing", "Download complete, starting import")
            
            # 2. Import Data
            try:
                # Import the module dynamically to avoid import errors if not in path
                sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
                from importer.update_data import main as import_main
                
                # Mock args for the import script
                logger.info(f"Running importer for {config['table']}...")
                sys.argv = ['update_data.py', config['table']]
                import_main()
                
                update_status(config["name"], "Success", "Import complete")
                logger.info(f"Import success for {config['table']}")
                
            except Exception as e:
                logger.error(f"Import failed for {config['table']}: {e}")
                update_status(config["name"], "Error", str(e))
        else:
            update_status(config["name"], "Error", "Download failed")

    # 3. Trigger Network Discovery
    logger.info("Starting Network Discovery...")
    try:
        from api.discover_networks import build_network_graph
        build_network_graph()
        logger.info("Network Discovery Complete")
    except Exception as e:
        logger.error(f"Network Discovery failed: {e}")

if __name__ == "__main__":
    refresh_system_data()
