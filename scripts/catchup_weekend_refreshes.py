#!/usr/bin/env python3
"""
catchup_weekend_refreshes.py
===========================
Executes all scheduled refreshes and pipeline jobs that were missed or interrupted
during the weekend power surge downtime.
"""

import os
import sys
import time
import logging
from datetime import datetime

# Set working directory to project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

# Configure logging
log_file = os.path.join(PROJECT_ROOT, "logs", "catchup_weekend.log")
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("catchup-refreshes")

def main():
    logger.info("=" * 80)
    logger.info(f"STARTING WEEKEND DOWNTIME CATCHUP PIPELINE — {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info("=" * 80)

    from updater.scheduled_runner import (
        run_nightly_update,
        run_nightly_nyc_update,
        run_nightly_nj_update,
        run_nightly_other_cities_update,
        run_weekly_nightly_updates,
        run_weekly_full_scan,
        run_weekly_other_cities_full_scan,
        run_weekly_app_audit,
        run_daily_usage_report
    )
    from updater.scheduled_runner import run_source_only

    steps = [
        ("1/9: Nightly CT Vision & System Data Ingest", run_nightly_update),
        ("2/9: Nightly NYC HPD Ingest & Network Pipeline", run_nightly_nyc_update),
        ("3/9: Nightly NJ BHI Ingest & Network Pipeline", run_nightly_nj_update),
        ("4/9: Nightly & Weekly Multi-City Ingest (DC, Baltimore, Boston, Detroit, Philly, Chicago, Miami, Minneapolis)", run_weekly_other_cities_full_scan),
        ("5/9: Weekly CT Municipalities & Eviction Ingest", run_weekly_nightly_updates),
        ("6/9: Weekly Rotating Full Scan", run_weekly_full_scan),
        ("7/9: Post-Scrape Safe Network Graph & Insights Rebuild", lambda: run_source_only([sys.executable, "api/safe_network_refresh.py"], check=False)),
        ("8/9: Daily Server-Side Usage Analytics Email Dispatch", run_daily_usage_report),
        ("9/9: Weekly Application Audit & Email Status Dispatch", run_weekly_app_audit),
    ]

    start_total = time.time()

    for idx, (label, func) in enumerate(steps, 1):
        logger.info("-" * 80)
        logger.info(f"▶ EXECUTING CATCHUP STEP {label}")
        logger.info("-" * 80)
        step_start = time.time()
        try:
            func()
            elapsed = time.time() - step_start
            logger.info(f"✓ FINISHED {label} in {elapsed:.2f}s")
        except Exception as e:
            elapsed = time.time() - step_start
            logger.error(f"✘ CRASHED {label} after {elapsed:.2f}s: {e}")

    total_elapsed = time.time() - start_total
    logger.info("=" * 80)
    logger.info(f"✓ CATCHUP PIPELINE COMPLETE IN {total_elapsed:.2f}s ({total_elapsed/60:.2f} minutes)")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
