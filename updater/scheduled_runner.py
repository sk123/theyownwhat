#!/usr/bin/env python3
"""
Scheduled Vision Data Updater
Runs automatically to keep property data fresh by updating "Current Owner" properties.
"""
import os
import sys
import time
import schedule
import logging
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/scheduled_updates.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Priority municipalities ordered by impact
PRIORITY_TOWNS = [
    'EAST HAMPTON',
    'CROMWELL',
    'RIDGEFIELD',
    'CHESHIRE',
    'HAMDEN',
    'CLINTON',
    'OLD LYME',
    'MIDDLETOWN',
    'ENFIELD',
    'MILFORD'
]

def run_nightly_update():
    """Run nightly update for Current Owner properties in priority towns"""
    logger.info("=" * 80)
    logger.info("Starting nightly Vision data update (Parallel)")
    logger.info("=" * 80)
    
    try:
        # Run update_data.py as a subprocess for better isolation
        import subprocess
        
        cmd = [
            sys.executable, 
            "updater/update_data.py", 
            "-m"
        ] + PRIORITY_TOWNS
        
        logger.info(f"Executing: {' '.join(cmd)}")
        # We don't use check=True to allow the scheduler to continue even if one run fails
        subprocess.run(cmd, check=False)
        
        logger.info("✓ Parallel town update process finished")

    except Exception as e:
        logger.error(f"✘ Nightly update failed: {e}")
    
    logger.info("=" * 80)
    logger.info("Nightly update completed")
    logger.info("=" * 80)

def run_weekly_full_scan():
    """Run weekly full scan for a rotating subset of towns"""
    logger.info("Starting weekly full property scan")
    
    # Rotate through towns weekly (10 towns = 10 weeks to cover all)
    week_number = datetime.now().isocalendar()[1]
    town_index = week_number % len(PRIORITY_TOWNS)
    town = PRIORITY_TOWNS[town_index]
    
    try:
        logger.info(f"Running full scan for {town} (week {week_number} rotation)")
        from updater.update_data import main as vision_main
        sys.argv = ['update_data.py', town, '--force']
        vision_main()
        logger.info(f"✓ Full scan of {town} completed")
    except Exception as e:
        logger.error(f"✗ Full scan of {town} failed: {e}")

def main():
    """Main scheduler loop"""
    logger.info("Vision Data Updater Service Starting")
    logger.info(f"Monitoring {len(PRIORITY_TOWNS)} priority municipalities")
    
    # Schedule jobs
    schedule.every().day.at("02:00").do(run_nightly_update)
    schedule.every().sunday.at("03:00").do(run_weekly_full_scan)
    
    logger.info("Scheduled jobs:")
    logger.info("  - Nightly update (Current Owner): 2:00 AM daily")
    logger.info("  - Weekly full scan: 3:00 AM Sunday")
    
    # Run immediately on startup for testing
    logger.info("Running initial update...")
    run_nightly_update()
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == '__main__':
    main()
