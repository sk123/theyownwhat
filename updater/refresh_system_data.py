import os
import sys
import requests
import logging
import shutil
import psycopg2
import json
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
        if details:
            if isinstance(details, str):
                try:
                    json.loads(details)
                except:
                    details = json.dumps({"message": details})
            else:
                details = json.dumps(details, default=str)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_source_status (source_name, source_type, last_refreshed_at, refresh_status, details)
                VALUES (%s, 'system', NOW(), %s, %s::jsonb)
                ON CONFLICT (source_name) 
                DO UPDATE SET 
                    last_refreshed_at = NOW(),
                    refresh_status = EXCLUDED.refresh_status,
                    details = COALESCE(EXCLUDED.details, data_source_status.details);
            """, (source_name, status, details))
            cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"Failed to update status for {source_name}: {e}")

def download_file(url, target_path):
    os.makedirs(DATA_DIR, exist_ok=True)
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
        update_status(config["name"], "running", "Downloading")
        target_path = os.path.join(DATA_DIR, config["filename"])
        
        if download_file(config["url"], target_path):
            update_status(config["name"], "running", "Download complete, starting import")
            
            # 2. Import Data
            try:
                # Import the module dynamically to avoid import errors if not in path
                sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
                from importer.update_data import main as import_main
                
                # Set args for the import script entry point.
                logger.info(f"Running importer for {config['table']}...")
                sys.argv = ['update_data.py', config['table']]
                import_main()
                
                update_status(config["name"], "success", "Import complete")
                logger.info(f"Import success for {config['table']}")
                
            except Exception as e:
                logger.error(f"Import failed for {config['table']}: {e}")
                update_status(config["name"], "failure", str(e))
        else:
            update_status(config["name"], "failure", "Download failed")

    # 3. Trigger Network Discovery
    logger.info("Starting Network Discovery...")
    try:
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'api'))
        from discover_networks import main as build_network_graph
        original_argv = sys.argv
        sys.argv = ['discover_networks.py']
        build_network_graph()
        sys.argv = original_argv
        logger.info("Network Discovery Complete")
    except Exception as e:
        logger.error(f"Network Discovery failed: {e}")

    # 4. NYC HPD Refresh (opt-in via NYC_HPD_ENABLED=true)
    if os.environ.get("NYC_HPD_ENABLED", "").lower() == "true":
        logger.info("NYC_HPD_ENABLED=true — starting NYC HPD ingest...")
        try:
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
            from nyc.ingest_hpd import ingest_hpd
            ingest_hpd()
            logger.info("NYC HPD ingest complete.")
        except Exception as e:
            logger.error(f"NYC HPD ingest failed: {e}")

        logger.info("Building NYC networks...")
        try:
            from nyc.build_nyc_networks import build_nyc_networks
            build_nyc_networks()
            logger.info("NYC network build complete.")
        except Exception as e:
            logger.error(f"NYC network build failed: {e}")
    else:
        logger.info("NYC_HPD_ENABLED not set — skipping NYC refresh.")

if __name__ == "__main__":
    refresh_system_data()
