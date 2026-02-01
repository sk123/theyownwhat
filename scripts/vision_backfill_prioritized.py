#!/usr/bin/env python3
"""
Prioritized Vision Data Backfill Script
========================================
Backfill Vision Appraisal data for municipalities lacking:
  1. Official record links (highest priority)
  2. Property photos  
  3. Other details (owner info, valuations, etc.)

Priority Order:
  1. Mansfield (per user request)
  2. All Tolland County municipalities
  3. Rest of CT, sorted by missing data

Features:
  - Resumable: Tracks progress in database, can be safely interrupted/restarted
  - Safe: Only updates if new data is available, never replaces newer with older
  - Network refresh: Triggers safe network rebuild after completion
  - Logging: Comprehensive logging of all operations

Usage:
  python3 scripts/vision_backfill_prioritized.py
  python3 scripts/vision_backfill_prioritized.py --dry-run  # Test without updating
  python3 scripts/vision_backfill_prioritized.py --resume  # Resume from last checkpoint
"""

import os
import sys
import json
import logging
import argparse
import subprocess
from datetime import datetime
from typing import List, Dict, Tuple

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
os.makedirs('logs', exist_ok=True)
log_file = f"logs/vision_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# Tolland County municipalities
TOLLAND_COUNTY = [
    "ANDOVER", "BOLTON", "COLUMBIA", "COVENTRY", "ELLINGTON",
    "HEBRON", "MANSFIELD", "SOMERS", "STAFFORD", "TOLLAND",
    "UNION", "VERNON", "WILLINGTON"
]

def get_db_connection():
    """Get database connection."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(DATABASE_URL)

def setup_progress_tracking(conn):
    """Create table to track backfill progress."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vision_backfill_progress (
                municipality TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                properties_updated INTEGER DEFAULT 0,
                error_message TEXT
            )
        """)
        conn.commit()
    logger.info("Progress tracking table ready")

def get_municipality_priority_list(conn) -> List[Tuple[str, int, int, int]]:
    """
    Query database to prioritize municipalities by missing data.
    Returns list of (municipality, missing_links, missing_photos, missing_details)
    """
    logger.info("Analyzing municipalities for missing Vision data...")
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                property_city as municipality,
                COUNT(*) as total_properties,
                COUNT(CASE WHEN link IS NULL OR link = '' THEN 1 END) as missing_links,
                COUNT(CASE WHEN building_photo IS NULL OR building_photo = '' THEN 1 END) as missing_photos
            FROM properties 
            WHERE property_city IS NOT NULL
            GROUP BY property_city
            HAVING 
                COUNT(CASE WHEN link IS NULL OR link = '' THEN 1 END) > 0 OR
                COUNT(CASE WHEN building_photo IS NULL OR building_photo = '' THEN 1 END) > 0
        """)
        
        results = cur.fetchall()
    
    # Convert to list of tuples and calculate priority score
    municipalities = []
    for row in results:
        municipality = row['municipality'].upper() if row['municipality'] else None
        if not municipality:
            continue
        
        missing_links = row['missing_links'] or 0
        missing_photos = row['missing_photos'] or 0
        missing_details = 0  # Not tracked in current schema
        
        # Priority score: links are most important, then photos
        priority_score = (missing_links * 100) + (missing_photos * 10)
        
        municipalities.append((
            municipality,
            missing_links,
            missing_photos,
            missing_details,
            priority_score
        ))
    
    # Sort by custom priority:
    # 1. Mansfield first (user request)
    # 2. Other Tolland County towns (sorted by priority score)
    # 3. Rest of CT (sorted by priority score)
    
    def sort_key(item):
        municipality = item[0]
        priority_score = item[4]
        
        if municipality == "MANSFIELD":
            return (0, 0)  # Highest priority
        elif municipality in TOLLAND_COUNTY:
            return (1, -priority_score)  # Second tier, sorted by score
        else:
            return (2, -priority_score)  # Third tier, sorted by score
    
    municipalities.sort(key=sort_key)
    
    logger.info(f"Found {len(municipalities)} municipalities needing updates")
    logger.info(f"  - Tolland County: {sum(1 for m in municipalities if m[0] in TOLLAND_COUNTY)}")
    logger.info(f"  - Other CT: {sum(1 for m in municipalities if m[0] not in TOLLAND_COUNTY)}")
    
    return municipalities

def get_completed_municipalities(conn) -> set:
    """Get set of municipalities already successfully processed."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT municipality 
            FROM vision_backfill_progress 
            WHERE status = 'completed'
        """)
        return {row[0] for row in cur.fetchall()}

def mark_municipality_started(conn, municipality: str):
    """Mark municipality as started in progress table."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO vision_backfill_progress 
                (municipality, status, started_at)
            VALUES (%s, 'in_progress', NOW())
            ON CONFLICT (municipality) 
            DO UPDATE SET 
                status = 'in_progress',
                started_at = NOW(),
                error_message = NULL
        """, (municipality,))
        conn.commit()

def mark_municipality_completed(conn, municipality: str, properties_updated: int):
    """Mark municipality as completed."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE vision_backfill_progress 
            SET status = 'completed',
                completed_at = NOW(),
                properties_updated = %s
            WHERE municipality = %s
        """, (properties_updated, municipality))
        conn.commit()

def mark_municipality_failed(conn, municipality: str, error: str):
    """Mark municipality as failed."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE vision_backfill_progress 
            SET status = 'failed',
                completed_at = NOW(),
                error_message = %s
            WHERE municipality = %s
        """, (error, municipality))
        conn.commit()

def run_vision_update(municipality: str, dry_run: bool = False) -> bool:
    """
    Run Vision Appraisal update for a single municipality.
    Uses the existing update_data.py script.
    """
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Starting Vision update for {municipality}")
    
    if dry_run:
        logger.info(f"[DRY RUN] Would run: python updater/update_data.py -m {municipality}")
        return True
    
    try:
        cmd = [
            sys.executable,
            "updater/update_data.py",
            "-m",
            municipality
        ]
        
        logger.info(f"Executing: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout per municipality
        )
        
        if result.returncode == 0:
            logger.info(f"✓ Vision update completed for {municipality}")
            # Log last 20 lines of output for reference
            output_lines = result.stdout.split('\n')[-20:]
            for line in output_lines:
                if line.strip():
                    logger.info(f"  {line}")
            return True
        else:
            logger.error(f"✗ Vision update failed for {municipality} (exit code: {result.returncode})")
            logger.error(f"Error output: {result.stderr[-500:]}")  # Last 500 chars of error
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"✗ Vision update timed out for {municipality}")
        return False
    except Exception as e:
        logger.error(f"✗ Vision update exception for {municipality}: {e}")
        return False

def trigger_network_refresh(dry_run: bool = False) -> bool:
    """Trigger safe network refresh using existing script."""
    logger.info("=== Triggering Network Refresh ===")
    
    if dry_run:
        logger.info("[DRY RUN] Would run: python api/safe_network_refresh.py")
        return True
    
    try:
        cmd = [sys.executable, "api/safe_network_refresh.py"]
        logger.info(f"Executing: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 min timeout
        
        if result.returncode == 0:
            logger.info("✓ Network refresh completed successfully")
            return True
        else:
            logger.error(f"✗ Network refresh failed (exit code: {result.returncode})")
            logger.error(f"Error: {result.stderr[-500:]}")
            return False
            
    except Exception as e:
        logger.error(f"✗ Network refresh exception: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Prioritized Vision Data Backfill')
    parser.add_argument('--dry-run', action='store_true', help='Test mode - no actual updates')
    parser.add_argument('--resume', action='store_true', help='Resume from last checkpoint (skip completed municipalities)')
    parser.add_argument('--skip-refresh', action='store_true', help='Skip network refresh at the end')
    parser.add_argument('--limit', type=int, help='Limit number of municipalities to process (for testing)')
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("PRIORITIZED VISION DATA BACKFILL")
    logger.info("=" * 80)
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info(f"Resume: {args.resume}")
    logger.info(f"Log file: {log_file}")
    logger.info("")
    
    conn = None
    try:
        conn = get_db_connection()
        setup_progress_tracking(conn)
        
        # Get prioritized list
        priority_list = get_municipality_priority_list(conn)
        
        if not priority_list:
            logger.info("No municipalities need updates. Exiting.")
            return
        
        # Filter out completed if resuming
        completed = get_completed_municipalities(conn) if args.resume else set()
        if completed:
            logger.info(f"Resuming: Skipping {len(completed)} already completed municipalities")
            priority_list = [m for m in priority_list if m[0] not in completed]
        
        # Apply limit if specified
        if args.limit:
            priority_list = priority_list[:args.limit]
            logger.info(f"Limited to first {args.limit} municipalities")
        
        # Display plan
        logger.info("=" * 80)
        logger.info(f"BACKFILL PLAN - {len(priority_list)} municipalities")
        logger.info("=" * 80)
        for i, (muni, links, photos, details, score) in enumerate(priority_list[:20], 1):
            county_tag = " [TOLLAND]" if muni in TOLLAND_COUNTY else ""
            logger.info(f"{i:3d}. {muni:20s}{county_tag:12s} - Links: {links:5d}, Photos: {photos:5d}, Details: {details:5d}")
        if len(priority_list) > 20:
            logger.info(f"     ... and {len(priority_list) - 20} more")
        logger.info("")
        
        # Process each municipality
        total_updated = 0
        failed_municipalities = []
        
        for i, (municipality, missing_links, missing_photos, missing_details, _) in enumerate(priority_list, 1):
            logger.info("=" * 80)
            logger.info(f"[{i}/{len(priority_list)}] Processing: {municipality}")
            logger.info(f"  Missing - Links: {missing_links}, Photos: {missing_photos}, Details: {missing_details}")
            logger.info("=" * 80)
            
            mark_municipality_started(conn, municipality)
            
            success = run_vision_update(municipality, dry_run=args.dry_run)
            
            if success:
                # Estimate properties updated (could query DB for exact count if needed)
                properties_updated = missing_links + missing_photos + missing_details
                mark_municipality_completed(conn, municipality, properties_updated)
                total_updated += properties_updated
                logger.info(f"✓ {municipality} completed ({i}/{len(priority_list)})")
            else:
                mark_municipality_failed(conn, municipality, "Vision update failed")
                failed_municipalities.append(municipality)
                logger.error(f"✗ {municipality} failed ({i}/{len(priority_list)})")
            
            logger.info("")
        
        # Summary
        logger.info("=" * 80)
        logger.info("BACKFILL SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total municipalities processed: {len(priority_list)}")
        logger.info(f"Successful: {len(priority_list) - len(failed_municipalities)}")
        logger.info(f"Failed: {len(failed_municipalities)}")
        if failed_municipalities:
            logger.info(f"Failed municipalities: {', '.join(failed_municipalities)}")
        logger.info(f"Estimated properties updated: {total_updated}")
        logger.info("")
        
        # Trigger network refresh
        if not args.skip_refresh and not args.dry_run:
            refresh_success = trigger_network_refresh(dry_run=args.dry_run)
            if not refresh_success:
                logger.warning("Network refresh failed, but backfill completed")
        
        logger.info("=" * 80)
        logger.info("BACKFILL COMPLETE")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        if conn:
            conn.close()
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
