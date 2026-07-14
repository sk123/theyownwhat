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
import subprocess
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

SOURCE_ONLY_ENV_VAR = "THEYOWNWHAT_SOURCE_ONLY"

# Fallback priority municipalities ordered by impact
FALLBACK_PRIORITY_TOWNS = [
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

def run_source_only(cmd, **kwargs):
    """Run a scheduled child process with source-only mode explicitly enabled."""
    env = kwargs.pop("env", None)
    child_env = os.environ.copy() if env is None else env.copy()
    child_env[SOURCE_ONLY_ENV_VAR] = "true"
    logger.info("Executing source-only command: %s", " ".join(cmd))
    return subprocess.run(cmd, env=child_env, **kwargs)

def get_dynamic_priority_towns(limit=10):
    """
    Queries the database dynamically to find the largest municipalities that are the most outdated.
    Filters to only include municipalities configured in MUNICIPAL_DATA_SOURCES or general ones.
    """
    try:
        from updater.update_data import get_db_connection
        from api.municipal_config import MUNICIPAL_DATA_SOURCES
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Get stats of property count for each town
                cur.execute("""
                    SELECT 
                        COALESCE(TRIM(UPPER(property_city)), 'UNKNOWN') as town,
                        COUNT(*) as total_properties
                    FROM properties
                    GROUP BY TRIM(UPPER(property_city))
                """)
                prop_stats = {row[0]: row[1] for row in cur.fetchall()}
                
                # Get last refreshed timestamp
                cur.execute("""
                    SELECT UPPER(source_name) as town, last_refreshed_at 
                    FROM data_source_status
                """)
                refresh_stats = {row[0]: row[1] for row in cur.fetchall()}
        finally:
            conn.close()
            
        # Get all updateable towns
        try:
            from updater.update_data import get_vision_municipalities
            vision_towns = set(get_vision_municipalities().keys())
        except Exception as e:
            logger.error(f"Failed to fetch Vision municipalities dynamically: {e}")
            vision_towns = set()

        updateable_towns = {t.upper() for t in MUNICIPAL_DATA_SOURCES.keys()} | {t.upper() for t in vision_towns}

        # Compile all potential candidates.
        all_towns = (set(prop_stats.keys()) | {t.upper() for t in MUNICIPAL_DATA_SOURCES.keys()}) & updateable_towns
        
        candidates = []
        for town in all_towns:
            town_upper = town.upper()
            if town_upper == 'UNKNOWN':
                continue
            total_props = prop_stats.get(town_upper, 0)
            last_refreshed = refresh_stats.get(town_upper)
            candidates.append({
                'town': town_upper,
                'total_properties': total_props,
                'last_refreshed': last_refreshed
            })
            
        # Sorting priority:
        # 1. Never refreshed (last_refreshed is None)
        # 2. Outdatedness (oldest last_refreshed timestamp first)
        # 3. Size (largest total_properties first)
        def sort_key(c):
            refreshed = c['last_refreshed']
            has_refreshed = 1 if refreshed is not None else 0
            refreshed_ts = refreshed.timestamp() if refreshed is not None else 0
            return (has_refreshed, refreshed_ts, -c['total_properties'])
            
        candidates.sort(key=sort_key)
        
        # Take the top N
        priority_towns = [c['town'] for c in candidates[:limit]]
        logger.info(f"Dynamically determined top {limit} priority towns based on size & outdatedness:")
        for c in candidates[:limit]:
            refreshed_str = c['last_refreshed'].strftime('%Y-%m-%d %H:%M:%S') if c['last_refreshed'] else 'NEVER'
            logger.info(f"  - {c['town']}: {c['total_properties']} properties, last refreshed: {refreshed_str}")
            
        return priority_towns
    except Exception as e:
        logger.error(f"Error dynamically determining priority towns: {e}. Falling back to default list.")
        return FALLBACK_PRIORITY_TOWNS

def run_nightly_update():
    """Run nightly update for Current Owner properties in priority towns"""
    logger.info("=" * 80)
    logger.info("Starting nightly Vision data update (Parallel)")
    logger.info("=" * 80)
    
    priority_towns = get_dynamic_priority_towns(limit=10)
    
    try:
        # 0. Refresh System Data (Businesses, Principals) & Run Network Discovery
        logger.info("Starting System Data Refresh & Network Discovery...")
        system_refresh_cmd = [sys.executable, "updater/refresh_system_data.py"]
        run_source_only(system_refresh_cmd, check=False)
        logger.info("✓ System Data Refresh finished")

        # Run update_data.py as a subprocess for better isolation
        cmd = [
            sys.executable, 
            "updater/update_data.py", 
            "-m"
        ] + priority_towns
        
        logger.info(f"Executing: {' '.join(cmd)}")
        # We don't use check=True to allow the scheduler to continue even if one run fails
        run_source_only(cmd, check=False)
        
        logger.info("Starting Hartford Code Enforcement ingestion...")
        enforcement_cmd = [sys.executable, "updater/ingest_hartford_enforcement.py"]
        run_source_only(enforcement_cmd, check=False)
        
        # 3. Refresh eviction data from CT Fair Housing / CT Judicial nightly feed
        if os.environ.get("CT_EVICTIONS_ENABLED", "true").lower() == "false":
            logger.info("⏭ CT_EVICTIONS_ENABLED=false — skipping eviction refresh")
        else:
            logger.info("Starting nightly CT Judicial eviction data refresh...")
            eviction_cmd = [sys.executable, "scripts/ingest_evictions.py"]
            run_source_only(eviction_cmd, check=False)
            logger.info("✓ Eviction data refresh finished")
        
        logger.info("✓ Nightly update process finished")

    except Exception as e:
        logger.error(f"✘ Nightly update failed: {e}")
    
    logger.info("=" * 80)
    logger.info("Nightly update completed")
    logger.info("=" * 80)

def run_weekly_full_scan():
    """Run weekly full scan for a rotating subset of towns"""
    logger.info("Starting weekly full property scan")
    
    priority_towns = get_dynamic_priority_towns(limit=10)
    if not priority_towns:
        logger.warning("No priority towns available for weekly full scan.")
        return
    
    # Rotate through towns weekly (10 towns = 10 weeks to cover all)
    week_number = datetime.now().isocalendar()[1]
    town_index = week_number % len(priority_towns)
    town = priority_towns[town_index]
    
    try:
        logger.info(f"Running full scan for {town} (week {week_number} rotation)")
        cmd = [sys.executable, "updater/update_data.py", "-m", town, "--force"]
        result = run_source_only(cmd, check=False)
        if result.returncode == 0:
            logger.info(f"✓ Full scan of {town} completed")
        else:
            logger.warning(f"⚠ Full scan of {town} exited with code {result.returncode}.")
    except Exception as e:
        logger.error(f"✗ Full scan of {town} failed: {e}")

def run_weekly_nightly_updates():
    """Run weekly updates for all municipalities with Weekly or Nightly frequency"""
    logger.info("=" * 80)
    logger.info("Starting weekly update for Weekly & Nightly-frequency municipalities...")
    logger.info("=" * 80)
    
    try:
        from api.municipal_config import MUNICIPAL_DATA_SOURCES
        target_towns = [k for k, v in MUNICIPAL_DATA_SOURCES.items() if v.get('frequency') in ['Weekly', 'Nightly']]
        if not target_towns:
            logger.info("No weekly or nightly municipalities found.")
            return

        cmd = [
            sys.executable,
            "updater/update_data.py",
            "-m"
        ] + target_towns + ["-p", "8"]
        
        logger.info(f"Executing: {' '.join(cmd)}")
        run_source_only(cmd, check=False)
        
        logger.info("Starting Hartford Code Enforcement ingestion...")
        enforcement_cmd = [sys.executable, "updater/ingest_hartford_enforcement.py"]
        run_source_only(enforcement_cmd, check=False)
        
        logger.info("Starting eviction data refresh...")
        if os.environ.get("CT_EVICTIONS_ENABLED", "true").lower() != "false":
            eviction_cmd = [sys.executable, "scripts/ingest_evictions.py"]
            run_source_only(eviction_cmd, check=False)
        else:
            logger.info("⏭ CT_EVICTIONS_ENABLED=false — skipping eviction refresh")

        # Trigger safe network refresh
        logger.info("Triggering post-scrape network and insights rebuild...")
        refresh_cmd = [sys.executable, "api/safe_network_refresh.py"]
        run_source_only(refresh_cmd, check=False)
        logger.info("✓ Post-scrape rebuild finished.")
        logger.info("✓ Weekly update process finished successfully.")
    except Exception as e:
        logger.error(f"✘ Weekly update failed: {e}")
    logger.info("=" * 80)

def run_monthly_other_updates():
    """Run monthly updates for other municipalities"""
    logger.info("=" * 80)
    logger.info("Starting monthly update for all other municipalities...")
    logger.info("=" * 80)
    
    try:
        from api.municipal_config import MUNICIPAL_DATA_SOURCES
        from updater.update_data import get_vision_municipalities

        configured_monthly_towns = [k for k, v in MUNICIPAL_DATA_SOURCES.items() if v.get('frequency') not in ['Weekly', 'Nightly']]

        try:
            vision_towns = set(get_vision_municipalities().keys())
        except Exception as e:
            logger.error(f"Failed to get Vision municipalities dynamically: {e}")
            vision_towns = set()

        unconfigured_vision_towns = [t for t in vision_towns if t.upper() not in MUNICIPAL_DATA_SOURCES]
        target_towns = list(set(configured_monthly_towns) | set(unconfigured_vision_towns))

        if not target_towns:
            logger.info("No other municipalities found.")
            return

        cmd = [
            sys.executable,
            "updater/update_data.py",
            "-m"
        ] + target_towns + ["-p", "8"]
        
        logger.info(f"Executing: {' '.join(cmd)}")
        run_source_only(cmd, check=False)
        
        # Trigger safe network refresh
        logger.info("Triggering post-scrape network and insights rebuild...")
        refresh_cmd = [sys.executable, "api/safe_network_refresh.py"]
        run_source_only(refresh_cmd, check=False)
        logger.info("✓ Post-scrape rebuild finished.")
        logger.info("✓ Monthly update process finished successfully.")
    except Exception as e:
        logger.error(f"✘ Monthly update failed: {e}")
    logger.info("=" * 80)

def run_nightly_nyc_update():
    """
    Nightly NYC HPD data pipeline — runs in parallel to CT at 3:30 AM.
    Steps (each isolated; one failure does not abort subsequent steps):
      1. Re-ingest HPD registrations + contacts + PLUTO (delta — fast)
      2. Enrich nyc_bbl_stats from Socrata violations / litigations / evictions
      3. Rebuild ownership network clusters (Union-Find + mega-net splitting)
    """
    logger.info("=" * 80)
    logger.info("Starting nightly NYC HPD data pipeline")
    logger.info("=" * 80)

    nyc_enabled = os.environ.get("NYC_HPD_ENABLED", "false").lower() == "true"
    if not nyc_enabled:
        logger.info("⏭  NYC_HPD_ENABLED is not 'true' — skipping NYC update.")
        return

    steps = [
        ("HPD Registrations + Contacts + PLUTO ingestion", [sys.executable, "-m", "nyc.ingest_hpd"]),
        ("HPD Violations / Litigations / Evictions enrichment", [sys.executable, "-m", "nyc.enrich_hpd"]),
        ("NYC Ownership Network rebuild",                    [sys.executable, "-m", "nyc.build_nyc_networks"]),
        ("NHPD subsidy + rent-stabilization enrichment",    [sys.executable, "-m", "nyc.enrich_nhpd"]),
    ]

    for label, cmd in steps:
        try:
            logger.info(f"▶  {label} …")
            result = run_source_only(cmd, check=False, capture_output=False)
            if result.returncode == 0:
                logger.info(f"✓  {label} finished.")
            else:
                logger.warning(f"⚠  {label} exited with code {result.returncode}.")
        except Exception as e:
            logger.error(f"✘  {label} crashed: {e}")

    logger.info("✓ Nightly NYC pipeline complete.")
    logger.info("=" * 80)

def run_city_enforcement_enrichment():
    """Refresh source-backed non-NYC enforcement enrichment."""
    cmd = [sys.executable, "updater/enrich_city_enforcement.py", "--city", "all"]
    result = run_source_only(cmd, check=False)
    if result.returncode == 0:
        logger.info("✓ Multi-city enforcement enrichment finished.")
    else:
        logger.warning(f"⚠ Multi-city enforcement enrichment exited with code {result.returncode}.")

def run_maryland_eviction_ingest():
    """Refresh official Maryland/Baltimore eviction event data."""
    cmd = [sys.executable, "updater/ingest_maryland_evictions.py"]
    result = run_source_only(cmd, check=False)
    if result.returncode == 0:
        logger.info("✓ Maryland eviction event ingest finished.")
    else:
        logger.warning(f"⚠ Maryland eviction event ingest exited with code {result.returncode}.")

def run_detroit_enrichment():
    """Refresh official Detroit code enforcement, compliance, and rental registrations."""
    cmd = [sys.executable, "updater/enrich_detroit.py"]
    result = run_source_only(cmd, check=False)
    if result.returncode == 0:
        logger.info("✓ Detroit enrichment pipeline finished successfully.")
    else:
        logger.warning(f"⚠ Detroit enrichment pipeline exited with code {result.returncode}.")

def run_nightly_other_cities_update():
    """
    Nightly source-only multi-city hook.
    """
    logger.info("=" * 80)
    logger.info("Checking nightly multi-city source-only update hook")
    logger.info("=" * 80)
    try:
        cmd = [sys.executable, "updater/update_cities.py"]
        result = run_source_only(cmd, check=False)
        if result.returncode == 0:
            logger.info("✓ Multi-city updates finished.")
        else:
            logger.warning(f"⚠ Multi-city updates exited with code {result.returncode}.")
        logger.info("Starting source-backed multi-city enforcement enrichment...")
        run_city_enforcement_enrichment()
        logger.info("Starting Detroit enforcement and compliance enrichment...")
        run_detroit_enrichment()
        logger.info("Starting official Maryland eviction event ingest...")
        run_maryland_eviction_ingest()
        logger.info("Starting NHPD subsidy enrichment...")
        nhpd_cmd = [sys.executable, "scripts/enrich_all_cities_nhpd.py"]
        run_source_only(nhpd_cmd, check=False)
        logger.info("Starting DC CAMA enrichment...")
        dc_cama_cmd = [sys.executable, "scripts/enrich_dc_cama.py"]
        run_source_only(dc_cama_cmd, check=False)
    except Exception as e:
        logger.error(f"✘ Multi-city updates crashed: {e}")
    logger.info("=" * 80)

def run_weekly_other_cities_full_scan():
    """
    Weekly full ingestion & network rebuild for D.C., Baltimore, Boston, Detroit, and Philadelphia.
    """
    logger.info("=" * 80)
    logger.info("Starting weekly FULL multi-city (D.C., Baltimore, Boston, Detroit, Philadelphia) data update & network rebuild")
    logger.info("=" * 80)
    try:
        cmd = [sys.executable, "updater/update_cities.py", "--full"]
        result = run_source_only(cmd, check=False)
        if result.returncode == 0:
            logger.info("✓ Weekly full multi-city updates finished.")
        else:
            logger.warning(f"⚠ Weekly full multi-city updates exited with code {result.returncode}.")
        logger.info("Starting source-backed multi-city enforcement enrichment...")
        run_city_enforcement_enrichment()
        logger.info("Starting Detroit enforcement and compliance enrichment...")
        run_detroit_enrichment()
        logger.info("Starting official Maryland eviction event ingest...")
        run_maryland_eviction_ingest()
        logger.info("Starting NHPD subsidy enrichment...")
        nhpd_cmd = [sys.executable, "scripts/enrich_all_cities_nhpd.py"]
        run_source_only(nhpd_cmd, check=False)
        logger.info("Starting DC CAMA enrichment...")
        dc_cama_cmd = [sys.executable, "scripts/enrich_dc_cama.py"]
        run_source_only(dc_cama_cmd, check=False)
    except Exception as e:
        logger.error(f"✘ Weekly full multi-city updates crashed: {e}")
    logger.info("=" * 80)

def main():
    """Main scheduler loop"""
    logger.info("Vision Data Updater Service Starting")
    
    # Schedule jobs
    schedule.every().day.at("02:00").do(run_nightly_update)
    schedule.every().day.at("03:30").do(run_nightly_nyc_update)
    schedule.every().day.at("04:00").do(run_nightly_other_cities_update)
    schedule.every().sunday.at("00:00").do(run_weekly_nightly_updates)
    schedule.every(30).days.at("01:00").do(run_monthly_other_updates)
    schedule.every().sunday.at("03:00").do(run_weekly_full_scan)
    schedule.every().sunday.at("05:00").do(run_weekly_other_cities_full_scan)
    
    logger.info("Scheduled jobs:")
    logger.info("  - Nightly CT update (Vision/Current Owner): 2:00 AM daily")
    logger.info("  - Nightly NYC update (HPD + enrichment + networks): 3:30 AM daily")
    logger.info("  - Nightly multi-city source-only hook: 4:00 AM daily")
    logger.info("  - Weekly update (Weekly/Nightly towns): 12:00 AM Sunday")
    logger.info("  - Monthly update (Other towns): 1:00 AM every 30 days")
    logger.info("  - Weekly full scan: 3:00 AM Sunday")
    logger.info("  - Weekly Multi-city full scan (D.C., Baltimore, Boston, Detroit, Philadelphia): 5:00 AM Sunday")
    logger.info("  - Multi-city and Detroit enforcement enrichment runs inside nightly/weekly multi-city hooks")
    logger.info("  - Maryland/Baltimore eviction event ingest runs inside nightly/weekly multi-city hooks")
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == '__main__':
    main()
