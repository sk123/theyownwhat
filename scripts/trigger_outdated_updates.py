#!/usr/bin/env python3
"""
Trigger Outdated Updates
========================
Helper script to dynamically identify and update any municipalities that
haven't been updated in over 2 weeks, as long as newer source data is available.
Runs updates sequentially to avoid overloading external services or database connections.
"""
import sys
import os
import subprocess
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from updater.update_data import get_vision_municipalities, get_db_connection
import datetime as dt
from datetime import timezone

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def main():
    log("Starting outdated municipalities update trigger...")
    
    # 1. Get vision municipalities and their portal dates
    try:
        vision_towns = get_vision_municipalities()
    except Exception as e:
        log(f"Failed to get Vision municipalities: {e}")
        return

    # 2. Get database status
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("select source_name, external_last_updated, last_refreshed_at from data_source_status;")
    db_stats = {row[0].upper(): (row[1], row[2]) for row in cur.fetchall()}
    conn.close()

    two_weeks_ago = dt.datetime.now(timezone.utc) - dt.timedelta(days=14)
    outdated_towns = []
    
    for town in vision_towns:
        town_upper = town.upper()
        # Skip WEST HARTFORD and SPRAGUE since they are handled separately / running
        if town_upper in ['WEST HARTFORD', 'SPRAGUE']:
            continue
            
        portal_datetime = vision_towns[town]['last_updated']
        db_ext_date, db_refreshed = db_stats.get(town_upper, (None, None))
        
        # Check if last refreshed is older than 2 weeks
        if not db_refreshed or db_refreshed < two_weeks_ago:
            is_newer = False
            if portal_datetime:
                portal_date = portal_datetime.date() if isinstance(portal_datetime, dt.datetime) else portal_datetime
                db_date = db_ext_date.date() if isinstance(db_ext_date, dt.datetime) else db_ext_date
                if not db_date or portal_date > db_date:
                    is_newer = True
            else:
                is_newer = True
                
            if is_newer:
                outdated_towns.append(town_upper)

    log(f"Found {len(outdated_towns)} outdated towns to update: {outdated_towns}")
    
    for idx, town in enumerate(outdated_towns, 1):
        log(f"[{idx}/{len(outdated_towns)}] Triggering update for {town}...")
        cmd = [sys.executable, "updater/update_data.py", "-m", town]
        try:
            start_time = time.time()
            result = subprocess.run(cmd, check=False)
            elapsed = time.time() - start_time
            log(f"Finished {town} in {elapsed:.1f}s (exit code: {result.returncode})")
        except Exception as e:
            log(f"Failed to update {town}: {e}")
            
        # Polite delay
        time.sleep(5)
        
    log("All triggered updates completed successfully.")

if __name__ == '__main__':
    main()
