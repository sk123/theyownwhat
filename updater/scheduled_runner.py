#!/usr/bin/env python3
"""
Scheduled Vision Data Updater
Runs automatically to keep property data fresh by updating "Current Owner" properties.
"""
import os
import sys
import time
import threading
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

# The full network rebuild (relink every property, rebuild the graph, swap the
# tables) runs far longer than an ordinary scraper step. Killing it partway
# leaves the live network tables stale without any obvious failure, so it gets
# its own budget rather than the 2-hour default.
NETWORK_REBUILD_TIMEOUT = int(os.environ.get("NETWORK_REBUILD_TIMEOUT", str(8 * 3600)))


def run_source_only(cmd, **kwargs):
    """Run a scheduled child process with source-only mode explicitly enabled."""
    env = kwargs.pop("env", None)
    timeout = kwargs.pop("timeout", 7200) # 2-hour default timeout to protect scheduler from infinite hangs
    child_env = os.environ.copy() if env is None else env.copy()
    child_env[SOURCE_ONLY_ENV_VAR] = "true"
    logger.info("Executing source-only command: %s (timeout=%ds)", " ".join(cmd), timeout)
    try:
        return subprocess.run(cmd, env=child_env, timeout=timeout, **kwargs)
    except subprocess.TimeoutExpired:
        logger.error(f"Command timed out after {timeout}s: {' '.join(cmd)}")
        class TimeoutResult:
            returncode = 124
        return TimeoutResult()

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

def sweep_stale_locks():
    """Sweep crashed or orphaned 'running' statuses older than 12 hours."""
    try:
        from updater.update_data import get_db_connection, sweep_stale_running_statuses
        conn = get_db_connection()
        try:
            swept = sweep_stale_running_statuses(conn, max_age_hours=12)
            if swept:
                logger.warning("Swept %d stale 'running' data_source_status lock(s): %s", len(swept), swept)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("Stale running-status sweep warning: %s", e)

def run_nightly_update():
    """Run nightly update for Current Owner properties in priority towns"""
    logger.info("=" * 80)
    logger.info("Starting nightly Vision data update (Parallel)")
    logger.info("=" * 80)
    
    sweep_stale_locks()
    priority_towns = get_dynamic_priority_towns(limit=10)
    
    try:
        # 0. Refresh System Data (Businesses, Principals) & Run Network Discovery
        # This step relinks every property and rebuilds the whole ownership graph.
        # It outgrew the 2-hour default timeout when the NY parcel set landed, so
        # it was being killed mid-rebuild every night and the swap never ran —
        # leaving `networks` frozen at whatever the last completed build produced.
        logger.info("Starting System Data Refresh & Network Discovery...")
        system_refresh_cmd = [sys.executable, "updater/refresh_system_data.py"]
        result = run_source_only(system_refresh_cmd, check=False, timeout=NETWORK_REBUILD_TIMEOUT)
        if getattr(result, "returncode", 1) == 0:
            logger.info("✓ System Data Refresh finished")
        else:
            logger.error(
                "✘ System Data Refresh did NOT complete (exit=%s). Networks were not "
                "rebuilt this cycle and the live tables are now stale.",
                getattr(result, "returncode", "unknown"),
            )

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
        rebuild = run_source_only(refresh_cmd, check=False, timeout=NETWORK_REBUILD_TIMEOUT)
        if getattr(rebuild, "returncode", 1) == 0:
            logger.info("✓ Post-scrape rebuild finished.")
        else:
            logger.error(
                "✘ Post-scrape network rebuild did NOT complete (exit=%s).",
                getattr(rebuild, "returncode", "unknown"),
            )
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
        rebuild = run_source_only(refresh_cmd, check=False, timeout=NETWORK_REBUILD_TIMEOUT)
        if getattr(rebuild, "returncode", 1) == 0:
            logger.info("✓ Post-scrape rebuild finished.")
        else:
            logger.error(
                "✘ Post-scrape network rebuild did NOT complete (exit=%s).",
                getattr(rebuild, "returncode", "unknown"),
            )
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
        ("NY DOS Active Corporations untangling enrichment", [sys.executable, "-m", "nyc.enrich_ny_dos", "--limit-corps", "800"]),
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
    
    steps = [
        ("Stale running-status lock sweep", sweep_stale_locks),
        ("Multi-city property updates", lambda: run_source_only([sys.executable, "updater/update_cities.py"], check=False)),
        ("DC DLCP Basic Business Rental Licenses & untangling", lambda: run_source_only([sys.executable, "updater/enrich_dc_licenses.py"], check=False)),
        ("Baltimore DHCD Rental Registrations & Licenses", lambda: run_source_only([sys.executable, "updater/enrich_baltimore_registrations.py"], check=False)),
        ("Multi-city enforcement enrichment", run_city_enforcement_enrichment),
        ("Detroit enforcement and compliance enrichment", run_detroit_enrichment),
        ("Official Maryland eviction event ingest", run_maryland_eviction_ingest),
        ("Multi-city NHPD subsidy enrichment", lambda: run_source_only([sys.executable, "scripts/enrich_all_cities_nhpd.py"], check=False)),
        ("Connecticut NHPD subsidy enrichment", lambda: run_source_only([sys.executable, "scripts/import_nhpd.py"], check=False)),
        ("DC CAMA enrichment", lambda: run_source_only([sys.executable, "scripts/enrich_dc_cama.py"], check=False)),
    ]

    for label, fn in steps:
        try:
            logger.info(f"Starting {label}...")
            fn()
            logger.info(f"✓ {label} finished.")
        except Exception as e:
            logger.error(f"✘ {label} failed: {e}")

    logger.info("=" * 80)

def run_weekly_other_cities_full_scan():
    """
    Weekly full ingestion & network rebuild for D.C., Baltimore, Boston, Detroit, and Philadelphia.
    """
    logger.info("=" * 80)
    logger.info("Starting weekly FULL multi-city (D.C., Baltimore, Boston, Detroit, Philadelphia) data update & network rebuild")
    logger.info("=" * 80)
    
    steps = [
        ("Stale running-status lock sweep", sweep_stale_locks),
        ("Weekly full multi-city updates", lambda: run_source_only([sys.executable, "updater/update_cities.py", "--full"], check=False)),
        ("DC DLCP Basic Business Rental Licenses & untangling", lambda: run_source_only([sys.executable, "updater/enrich_dc_licenses.py"], check=False)),
        ("Baltimore DHCD Rental Registrations & Licenses", lambda: run_source_only([sys.executable, "updater/enrich_baltimore_registrations.py"], check=False)),
        ("Multi-city enforcement enrichment", run_city_enforcement_enrichment),
        ("Detroit enforcement and compliance enrichment", run_detroit_enrichment),
        ("Official Maryland eviction event ingest", run_maryland_eviction_ingest),
        ("Multi-city NHPD subsidy enrichment", lambda: run_source_only([sys.executable, "scripts/enrich_all_cities_nhpd.py"], check=False)),
        ("Connecticut NHPD subsidy enrichment", lambda: run_source_only([sys.executable, "scripts/import_nhpd.py"], check=False)),
        ("DC CAMA enrichment", lambda: run_source_only([sys.executable, "scripts/enrich_dc_cama.py"], check=False)),
    ]

    for label, fn in steps:
        try:
            logger.info(f"Starting {label}...")
            fn()
            logger.info(f"✓ {label} finished.")
        except Exception as e:
            logger.error(f"✘ {label} failed: {e}")

    logger.info("=" * 80)

def run_nightly_nj_update():
    """
    Nightly New Jersey BHI multi-family data & network pipeline — runs at 3:45 AM.
    Steps:
      1. Ingest NJ BHI landlord registrations & Mod-IV properties
      2. Rebuild NJ owner networks
    """
    logger.info("=" * 80)
    logger.info("Starting nightly NJ BHI data & network pipeline")
    logger.info("=" * 80)

    steps = [
        ("NJ BHI registrations & properties ingestion", [sys.executable, "-m", "nj.ingest_bhi"]),
        ("NJ Owner Network rebuild",                  [sys.executable, "-m", "nj.build_nj_networks"]),
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

    logger.info("✓ Nightly NJ pipeline complete.")
    logger.info("=" * 80)

def run_weekly_app_audit():
    """Weekly full application audit & status email dispatch."""
    logger.info("=" * 80)
    logger.info("Starting Weekly Full Application Audit")
    logger.info("=" * 80)
    try:
        cmd = [sys.executable, "updater/weekly_audit.py"]
        run_source_only(cmd, check=False)
        logger.info("✓ Weekly full application audit finished.")
    except Exception as e:
        logger.error(f"✘ Weekly full application audit crashed: {e}")
    logger.info("=" * 80)

def run_daily_usage_report():
    """Daily server-side local analytics report by city."""
    logger.info("=" * 80)
    logger.info("Starting Daily Usage & City Analytics Report Dispatch")
    logger.info("=" * 80)
    try:
        cmd = [sys.executable, "updater/daily_usage_report.py"]
        run_source_only(cmd, check=False)
        logger.info("✓ Daily usage report finished.")
    except Exception as e:
        logger.error(f"✘ Daily usage report crashed: {e}")
    logger.info("=" * 80)

def run_scheduled_jurisdiction_optimization():
    """Ledger-driven jurisdiction rotation & functional/data-reality optimization battery.
    Runs nightly during Cycle 1 (until all 6 cohorts complete) and weekly from Cycle 2 onward."""
    logger.info("=" * 80)
    logger.info("Checking Scheduled Jurisdiction Optimization Battery (--cohort due --if-due)")
    logger.info("=" * 80)
    try:
        cmd = [
            sys.executable,
            "scripts/run_jurisdiction_battery.py",
            "run",
            "--cohort", "due",
            "--if-due",
            "--heal",
            "--write-report",
            "--advance",
            "--send-email",
        ]
        result = run_source_only(cmd, check=False, timeout=3600)
        if getattr(result, "returncode", 1) == 0:
            logger.info("✓ Scheduled Jurisdiction Optimization Battery completed cleanly.")
        else:
            logger.warning(
                "⚠ Scheduled Jurisdiction Optimization Battery exited with code %s.",
                getattr(result, "returncode", "unknown"),
            )
    except Exception as e:
        logger.error(f"✘ Scheduled Jurisdiction Optimization Battery crashed: {e}")
    logger.info("=" * 80)

def run_weekly_statewide_and_opencorporates_update():
    """Weekly automated update path for ALL statewide OpenCorporates-linked jurisdictions
    (`vt`, `co`, `ny`, `fl`, `wi`, `nc`, `ar`, `ma`, `va`) + OpenCorporates SFTP snapshot poller.
    Rotates through 3 statewide GIS partitions per week (full 9-state GIS cycle every 3 weeks)
    while polling OpenCorporates SFTP & refreshing NHPD + daily landing/completeness caches weekly."""
    logger.info("=" * 80)
    logger.info("Starting Weekly OpenCorporates SFTP Poll & Statewide Jurisdiction Refresh")
    logger.info("=" * 80)
    sweep_stale_locks()

    # 1. Poll OpenCorporates SFTP for any newer dated bulk snapshot & sync OC_US_* status
    try:
        oc_cmd = [sys.executable, "scripts/sync_opencorporates_scheduled.py", "--poll-sftp"]
        oc_res = run_source_only(oc_cmd, check=False, timeout=NETWORK_REBUILD_TIMEOUT)
        if getattr(oc_res, "returncode", 1) == 0:
            logger.info("✓ OpenCorporates SFTP check & completeness sync finished.")
        else:
            logger.warning("⚠ OpenCorporates SFTP check exited with code %s.", getattr(oc_res, "returncode", "unknown"))
    except Exception as e:
        logger.error("✘ OpenCorporates SFTP sync failed: %s", e)

    # 2. Rotate 3 statewide GIS partitions per week across all 9 statewide datasets
    cohorts = [
        [("states", ["vt", "co", "wi"])],
        [("states", ["ny", "nc", "ar"])],
        [("states", ["fl"]), ("ma", None), ("va", None)],
    ]
    week_idx = datetime.now().isocalendar()[1] % len(cohorts)
    for kind, payload in cohorts[week_idx]:
        try:
            if kind == "states" and payload:
                cmd = [sys.executable, "states/ingest_statewide_parcels.py", "--states"] + payload
            elif kind == "ma":
                cmd = [sys.executable, "ma/ingest_massgis.py", "--full"]
            elif kind == "va":
                cmd = [sys.executable, "states/ingest_va_parcels.py", "--refresh-gdb"]
            else:
                continue
            logger.info("▶ Statewide cohort refresh (week_slot=%d): %s", week_idx, " ".join(cmd))
            res = run_source_only(cmd, check=False, timeout=NETWORK_REBUILD_TIMEOUT)
            if getattr(res, "returncode", 1) == 0:
                logger.info("✓ Statewide step finished: %s", " ".join(cmd))
            else:
                logger.warning("⚠ Statewide step exited with code %s: %s", getattr(res, "returncode", "unknown"), " ".join(cmd))
        except Exception as e:
            logger.error("✘ Statewide refresh step failed (%s): %s", kind, e)

    # 3. Refresh NHPD subsidy overlays & landing/completeness caches across all 20 jurisdictions
    try:
        run_source_only([sys.executable, "scripts/enrich_all_cities_nhpd.py"], check=False, timeout=3600)
        run_daily_landing_cache_refresh()
    except Exception as e:
        logger.error("✘ Post-statewide NHPD/cache refresh failed: %s", e)

    logger.info("=" * 80)

def run_daily_landing_cache_refresh():
    """Daily pre-warm of cached_insights, landing_snapshot:v1 & per-city stats/top-networks kv_cache."""
    logger.info("=" * 80)
    logger.info("Refreshing Daily Landing Page & Jurisdiction Insights Snapshot Cache")
    logger.info("=" * 80)
    try:
        ins_cmd = [sys.executable, "api/generate_insights.py"]
        ins_res = run_source_only(ins_cmd, check=False, timeout=1800)
        if getattr(ins_res, "returncode", 1) == 0:
            logger.info("✓ CT cached_insights + landing_snapshot refreshed cleanly.")
        else:
            logger.warning("⚠ generate_insights exited with code %s; running standalone landing_cache fallback.", getattr(ins_res, "returncode", "unknown"))
            cmd = [sys.executable, "-m", "api.landing_cache"]
            result = run_source_only(cmd, check=False, timeout=1800)
            if getattr(result, "returncode", 1) == 0:
                logger.info("✓ Daily landing snapshot cache refreshed cleanly.")
            else:
                logger.warning("⚠ Daily landing snapshot cache refresh exited with code %s.", getattr(result, "returncode", "unknown"))
    except Exception as e:
        logger.error("✘ Daily landing snapshot cache refresh crashed: %s", e)
    logger.info("=" * 80)

def _daily_report_thread():
    """Independent daemon thread for daily usage report & daily landing cache warmup.
    Runs on its own schedule.Scheduler() instance so it is never blocked by long-running
    nightly syncs that monopolize the main schedule loop for hours."""
    daily_schedule = schedule.Scheduler()
    daily_schedule.every().day.at("06:15").do(run_daily_landing_cache_refresh)
    daily_schedule.every().day.at("08:00").do(run_daily_usage_report)
    logger.info("[Daily Report & Cache Thread] Started — landing cache at 06:15 UTC, usage report at 08:00 UTC daily")
    while True:
        daily_schedule.run_pending()
        time.sleep(30)

def _optimization_rotation_thread():
    """Independent daemon thread for rotating jurisdiction tests & source-only optimization.
    Runs on its own schedule.Scheduler() instance at 11:30 UTC daily (after the 5:15 AM ET
    Antigravity agent automation window) so multi-hour CT scrapes never starve verification
    and --if-due acts as a zero-duplicate self-healing fallback."""
    opt_schedule = schedule.Scheduler()
    opt_schedule.every().day.at("11:30").do(run_scheduled_jurisdiction_optimization)
    logger.info("[Jurisdiction Optimization Thread] Started — checks due cohort at 11:30 UTC daily")
    while True:
        opt_schedule.run_pending()
        time.sleep(30)

def main():
    """Main scheduler loop"""
    logger.info("Vision Data Updater Service Starting")
    
    # Start the daily usage report in a SEPARATE daemon thread
    # so it fires reliably even when nightly syncs block the main loop for hours
    report_thread = threading.Thread(target=_daily_report_thread, daemon=True, name="daily-report")
    report_thread.start()

    # Start the ledger-driven jurisdiction optimization battery in its own daemon thread
    opt_thread = threading.Thread(target=_optimization_rotation_thread, daemon=True, name="jurisdiction-optimization")
    opt_thread.start()
    
    # Schedule heavy jobs on the main loop
    schedule.every().day.at("02:00").do(run_nightly_update)
    schedule.every().day.at("03:30").do(run_nightly_nyc_update)
    schedule.every().day.at("03:45").do(run_nightly_nj_update)
    schedule.every().day.at("04:00").do(run_nightly_other_cities_update)
    schedule.every().saturday.at("01:30").do(run_weekly_statewide_and_opencorporates_update)
    schedule.every().sunday.at("00:00").do(run_weekly_nightly_updates)
    schedule.every(30).days.at("01:00").do(run_monthly_other_updates)
    schedule.every().sunday.at("03:00").do(run_weekly_full_scan)
    schedule.every().sunday.at("05:00").do(run_weekly_other_cities_full_scan)
    schedule.every().sunday.at("06:00").do(run_weekly_app_audit)
    
    logger.info("Scheduled jobs:")
    logger.info("  - Nightly CT update (Vision/Current Owner): 2:00 AM daily")
    logger.info("  - Nightly NYC update (HPD + enrichment + networks): 3:30 AM daily")
    logger.info("  - Nightly NJ update (BHI + networks): 3:45 AM daily")
    logger.info("  - Nightly multi-city source-only hook: 4:00 AM daily")
    logger.info("  - Weekly OpenCorporates SFTP poll & 9-Statewide GIS rotation (VT/CO/WI/NY/NC/AR/FL/MA/VA): 1:30 AM Saturday")
    logger.info("  - Rotating Jurisdiction Optimization Battery (Cycle 1 nightly -> Cycle 2+ weekly): 11:30 AM UTC daily [INDEPENDENT THREAD]")
    logger.info("  - Daily Server-Side Usage Analytics Email (salmunk@gmail.com): 8:00 AM daily [INDEPENDENT THREAD]")
    logger.info("  - Weekly update (Weekly/Nightly towns): 12:00 AM Sunday")
    logger.info("  - Monthly update (Other towns): 1:00 AM every 30 days")
    logger.info("  - Weekly full scan: 3:00 AM Sunday")
    logger.info("  - Weekly Multi-city full scan (D.C., Baltimore, Boston, Detroit, Philadelphia): 5:00 AM Sunday")
    logger.info("  - Weekly Full App Audit & Email Report (salmunk@gmail.com): 6:00 AM Sunday")
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == '__main__':
    main()

