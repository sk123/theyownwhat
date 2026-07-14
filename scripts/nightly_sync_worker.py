
import os
import sys
import subprocess
import psycopg2
from datetime import datetime, timedelta
import time

DATABASE_URL = os.environ.get("DATABASE_URL")
SOURCE_ONLY_ENV_VAR = "THEYOWNWHAT_SOURCE_ONLY"
SUPPORTED_PROPERTY_SOURCE_TYPES = {
    "VISION",
    "VISION_APPRAISAL",
    "ARCGIS",
    "ARCGIS_CSV",
    "CT_GEODATA_CSV",
    "MAPXPRESS",
    "MAPGEO",
    "PROPERTYRECORDCARDS",
    "TIGHE_BOND",
    "AVON_STATIC",
    "HARTFORD_SCRIPT",
    "HARTFORD_ENRICHMENT",
    "WINDSOR_API",
}

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def run_script(script_path, args=None):
    cmd = ["python3", script_path]
    if args:
        cmd.extend(args)
    print(f"Running: {' '.join(cmd)}")
    env = os.environ.copy()
    env[SOURCE_ONLY_ENV_VAR] = "true"
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print(f"Error running {script_path}: {result.stderr}")
    else:
        print(f"Success: {result.stdout}")
    return result.returncode == 0

def get_configured_municipality_names():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    try:
        from api.municipal_config import MUNICIPAL_DATA_SOURCES
        return {name.upper() for name in MUNICIPAL_DATA_SOURCES.keys()}
    except Exception as exc:
        print(f"Could not load municipal source config; only indexed Vision sources will be eligible: {exc}")
        return set()

def get_outdated_sources(conn):
    """
    Returns a list of municipalities that need updating.
    Criteria:
    - Only source-backed municipal property feeds are eligible.
    - Dated sources refresh when external_last_updated > last_refreshed_at.
    - Undated sources refresh when last_refreshed_at is more than 30 days old.
    """
    query = """
    SELECT source_name, source_type, external_last_updated, last_refreshed_at 
    FROM data_source_status
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    
    outdated = []
    now = datetime.now().astimezone()
    configured_municipalities = get_configured_municipality_names()
    
    for name, stype, ext_date, last_ref in rows:
        name_upper = (name or "").upper()
        source_type = (stype or "").upper()
        if source_type not in SUPPORTED_PROPERTY_SOURCE_TYPES:
            continue
        if source_type not in {"VISION", "VISION_APPRAISAL"} and name_upper not in configured_municipalities:
            continue

        if last_ref is None:
            outdated.append(name)
            continue
        
        # If we have an external date, compare it
        if ext_date:
            # Convert ext_date to datetime for comparison
            ext_dt = datetime.combine(ext_date, datetime.min.time()).astimezone()
            if ext_dt > last_ref:
                outdated.append(name)
                continue
        
        # Stale check: 30 days
        if now - last_ref > timedelta(days=30):
            outdated.append(name)
            
    return outdated

import argparse

def main():
    parser = argparse.ArgumentParser(description="Nightly Sync Worker")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually run update scripts")
    args = parser.parse_args()

    print(f"--- Nightly Sync Worker Started at {datetime.now()} ---")
    if args.dry_run:
        print("!!! DRY RUN MODE ENABLED !!!")
    
    conn = get_db_connection()
    
    try:
        # 1. Sync dates
        print("Step 1: Syncing data source dates...")
        run_script("scripts/sync_data_dates.py")
        
        # 2. Check for outdated properties
        print("Step 2: Checking for outdated municipalities...")
        outdated_munis = get_outdated_sources(conn)
        if outdated_munis:
            print(f"Found {len(outdated_munis)} municipalities to refresh: {outdated_munis}")
            for muni in outdated_munis:
                if not args.dry_run:
                    run_script("updater/update_data.py", ["-m", muni])
                else:
                    print(f"Dry-run: Would run update_data.py for {muni}")
        else:
            print("All municipalities are up to date.")

        # 3. Nightly Business Sync
        print("Step 3: Syncing Business Registry...")
        if not args.dry_run:
            if run_script("scripts/download_business_data.py"):
                 run_script("importer/import_data.py", ["--force", "businesses", "principals"])
        else:
            print("Dry-run: Skipping business download and import")
        
        # 4. Re-generate networks
        print("Step 4: Re-generating networks and refreshing insights...")
        if not args.dry_run:
            # Use the guarded shadow-table refresh path so scheduled rebuilds
            # follow the same anti-meganet rules as production maintenance.
            run_script("api/safe_network_refresh.py")
        else:
            print("Dry-run: Skipping network re-generation (would run api/safe_network_refresh.py)")
            
        print(f"--- Nightly Sync Worker Completed at {datetime.now()} ---")
    except Exception as e:
        print(f"Critical error in nightly sync worker: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
