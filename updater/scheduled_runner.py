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

# Priority configurations
NIGHTLY_MUNI_COUNT = 15  # Process top 15 municipalities by data debt nightly
WEEKLY_MUNI_COUNT = 30   # Process top 30 municipalities more aggressively on weekends

def run_priority_update():
    """Run update for municipalities with the most 'data debt' (missing photos, units, links)"""
    logger.info("=" * 80)
    logger.info("Starting Priority Property Enrichment (Data Debt Mode)")
    logger.info("=" * 80)
    
    try:
        from updater.update_vision_data import main as vision_main
        # --priority flag triggers the data debt ranking
        # We don't specify towns; the script will fetch and rank them itself.
        # We can limit parallel municipalities to avoid rate limits.
        sys.argv = ['update_vision_data.py', '--priority', '--parallel-munis', '4']
        vision_main()
        logger.info("✓ Priority enrichment complete")
    except Exception as e:
        logger.error(f"✗ Priority enrichment failed: {e}")
    
    logger.info("=" * 80)

def run_placeholder_cleanup():
    """Focus specifically on 'Current Owner' placeholders across all towns"""
    logger.info("Starting 'Current Owner' placeholder cleanup")
    try:
        from updater.update_vision_data import main as vision_main
        sys.argv = ['update_vision_data.py', '--current-owner-only', '--parallel-munis', '6']
        vision_main()
        logger.info("✓ Placeholder cleanup complete")
    except Exception as e:
        logger.error(f"✗ Placeholder cleanup failed: {e}")

def run_weekly_full_scan():
    """Weekly full force-scan for a rotating set of priority towns"""
    logger.info("Starting weekly full property scan (Force Refresh)")
    try:
        from updater.update_vision_data import main as vision_main
        # Run with --force and --priority to refresh high-value/low-quality towns
        sys.argv = ['update_vision_data.py', '--priority', '--force', '--parallel-munis', '2']
        vision_main()
        logger.info("✓ Weekly full scan completed")
    except Exception as e:
        logger.error(f"✗ Weekly full scan failed: {e}")

def main():
    """Main scheduler loop"""
    logger.info("Comprehensive Property Updater & Enrichment Service Starting")
    
    # Schedule jobs
    # 1. Daily Data Debt Enrichment (Photos, Units, Links)
    schedule.every().day.at("01:00").do(run_priority_update)
    
    # 2. Daily Placeholder Cleanup (Current Owner)
    schedule.every().day.at("04:00").do(run_placeholder_cleanup)
    
    # 3. Weekly Force Refresh (Sundays)
    schedule.every().sunday.at("00:00").do(run_weekly_full_scan)
    
    logger.info("Scheduled jobs:")
    logger.info("  - Priority Data Enrichment: 1:00 AM daily")
    logger.info("  - 'Current Owner' Cleanup: 4:00 AM daily")
    logger.info("  - Weekly Full Refresh: Sunday 12:00 AM")
    
    # Run a quick priority check on startup for verification
    logger.info("Running initial priority scan...")
    run_priority_update()
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    main()
