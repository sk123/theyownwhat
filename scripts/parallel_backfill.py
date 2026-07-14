#!/usr/bin/env python3
"""
Parallel Backfill Orchestrator
==============================
Groups municipalities by platform type and runs one per platform concurrently 
to avoid rate-limiting any single service. Prioritizes bigger cities first.

Uses --current-owner-only mode to only fill gaps (properties with owner='Current Owner',
missing photos, or never-scraped) without overwriting enriched data like geocoded addresses.

Usage:
    python scripts/parallel_backfill.py                  # Full backfill
    python scripts/parallel_backfill.py --dry-run        # Preview what would run
    python scripts/parallel_backfill.py --platforms vision_appraisal MAPXPRESS  # Specific platforms only
"""
import os
import sys
import subprocess
import threading
import time
import argparse
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from api.municipal_config import MUNICIPAL_DATA_SOURCES

# --- CT Population estimates for priority ordering (bigger cities first) ---
# Source: US Census Bureau 2020, approximate
CT_POPULATIONS = {
    'BRIDGEPORT': 148529, 'NEW HAVEN': 134023, 'STAMFORD': 135470, 'HARTFORD': 121054,
    'WATERBURY': 114403, 'NORWALK': 91184, 'DANBURY': 86518, 'NEW BRITAIN': 74135,
    'WEST HARTFORD': 64083, 'BRISTOL': 60833, 'MERIDEN': 60850, 'MILFORD': 55387,
    'MANCHESTER': 59713, 'WEST HAVEN': 55584, 'MIDDLETOWN': 47717, 'ENFIELD': 45246,
    'HAMDEN': 61169, 'STRATFORD': 52355, 'SHELTON': 41693, 'TORRINGTON': 36383,
    'NORWICH': 40125, 'WALLINGFORD': 44396, 'SOUTHINGTON': 44416, 'NEWINGTON': 30836,
    'NAUGATUCK': 31862, 'RIDGEFIELD': 25011, 'GLASTONBURY': 35159, 'GUILFORD': 22375,
    'SIMSBURY': 24517, 'CHESHIRE': 29261, 'NORTH HAVEN': 24253, 'FARMINGTON': 25866,
    'WILTON': 18503, 'WETHERSFIELD': 27173, 'VERNON': 30215, 'BLOOMFIELD': 21535,
    'ROCKY HILL': 20845, 'WINDHAM': 25268, 'WINDSOR': 29492, 'NEWTOWN': 27560,
    'STONINGTON': 18627, 'EAST HAVEN': 28847, 'SUFFIELD': 15752, 'ELLINGTON': 16426,
    'SEYMOUR': 16748, 'AVON': 18302, 'COLCHESTER': 16068, 'COVENTRY': 12435,
    'OLD SAYBROOK': 10481, 'CLINTON': 13185, 'SOUTHBURY': 19879, 'PLAINVILLE': 17711,
    'KILLINGLY': 17777, 'MONTVILLE': 18387, 'OXFORD': 13141, 'WATERTOWN': 22105,
    'WOLCOTT': 16680, 'BERLIN': 20175, 'WOODBRIDGE': 9087, 'EAST HAMPTON': 12717,
    'CROMWELL': 14225, 'DERBY': 12902, 'WOODBURY': 10137, 'PROSPECT': 9646,
    'ANSONIA': 18918, 'MARLBOROUGH': 6418, 'PLYMOUTH': 12243, 'LITCHFIELD': 8192,
    'ESSEX': 6733, 'OLD LYME': 7628, 'LYME': 2372, 'WOODSTOCK': 8221,
    'MANSFIELD': 25892, 'POMFRET': 4266, 'BEACON FALLS': 6049, 'BETHANY': 5563,
    'BETHLEHEM': 3607, 'BROOKFIELD': 17069, 'BURLINGTON': 9475, 'CANTON': 10292,
    'SALEM': 4205, 'MIDDLEBURY': 7574, 'WINDSOR LOCKS': 12613, 'DURHAM': 7388,
    'ASHFORD': 4240, 'BOZRAH': 2627, 'BRIDGEWATER': 1727, 'CANAAN': 1234,
    'CHESTER': 3994, 'COLEBROOK': 1471, 'COLUMBIA': 5379, 'EASTFORD': 1749,
    'EASTON': 7605, 'FRANKLIN': 1922, 'HADDAM': 8452, 'HEBRON': 9686,
    'KILLINGWORTH': 6525, 'NEW CANAAN': 20622, 'NORFOLK': 1588, 'NORTH CANAAN': 3315,
    'NORTH STONINGTON': 5152, 'ROXBURY': 2260, 'SALISBURY': 3741, 'SCOTLAND': 1672,
    'SHERMAN': 3527, 'VOLUNTOWN': 2603, 'WARREN': 1461, 'WASHINGTON': 3646,
    'WESTON': 10354,
}

# Estimated population fallback for unlisted towns
DEFAULT_POPULATION = 5000

LOG_LOCK = threading.Lock()
LOG_FILE = None

def log(msg, platform=None):
    """Thread-safe logging."""
    ts = datetime.now().strftime("%H:%M:%S")
    prefix = f"[{platform}]" if platform else "[MAIN]"
    line = f"{ts} {prefix} {msg}"
    with LOG_LOCK:
        print(line, flush=True)
        if LOG_FILE:
            LOG_FILE.write(line + "\n")
            LOG_FILE.flush()


def group_by_platform(configs):
    """Group municipalities by their platform type."""
    groups = defaultdict(list)
    for muni, cfg in configs.items():
        ptype = cfg.get('type', 'unknown')
        pop = CT_POPULATIONS.get(muni, DEFAULT_POPULATION)
        groups[ptype].append((muni, pop))
    
    # Sort each group by population descending (bigger cities first)
    for ptype in groups:
        groups[ptype].sort(key=lambda x: x[1], reverse=True)
    
    return dict(groups)


def run_scrape_for_town(town_name, platform, dry_run=False):
    """Run update_data.py for a single town in current-owner-only mode."""
    cmd = [
        sys.executable, "updater/update_data.py",
        "-m", town_name,
        "-c",  # Current-owner-only mode (non-destructive)
        "-p", "1"  # Single municipality, internal parallelism handles the rest
    ]
    
    log(f"{'[DRY RUN] Would run' if dry_run else 'Starting'}: {town_name} (pop: {CT_POPULATIONS.get(town_name, '?')})", platform)
    
    if dry_run:
        return town_name, 0, True
    
    try:
        start = time.time()
        result = subprocess.run(
            cmd,
            cwd=os.path.join(os.path.dirname(__file__), '..'),
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour max per town
            env={**os.environ, 'DATABASE_URL': os.environ.get('DATABASE_URL', 'postgresql://user:password@ctdata_db:5432/ctdata')}
        )
        elapsed = time.time() - start
        
        success = result.returncode == 0
        status = "✅" if success else "❌"
        log(f"{status} {town_name} completed in {elapsed:.0f}s (exit={result.returncode})", platform)
        
        if not success and result.stderr:
            # Log last 3 lines of stderr for diagnostics
            err_lines = result.stderr.strip().split('\n')[-3:]
            for line in err_lines:
                log(f"  STDERR: {line}", platform)
        
        return town_name, elapsed, success
    except subprocess.TimeoutExpired:
        log(f"⏰ {town_name} TIMED OUT after 1 hour", platform)
        return town_name, 3600, False
    except Exception as e:
        log(f"❌ {town_name} ERROR: {e}", platform)
        return town_name, 0, False

    
def process_platform_queue(platform, towns, dry_run=False):
    """Process all towns for a single platform sequentially (to avoid rate-limiting that platform)."""
    log(f"Starting queue: {len(towns)} towns", platform)
    results = []
    for town_name, pop in towns:
        result = run_scrape_for_town(town_name, platform, dry_run)
        results.append(result)
        
        # Brief pause between towns on the same platform to be polite
        if not dry_run:
            time.sleep(2)
    
    successes = sum(1 for _, _, ok in results if ok)
    log(f"Queue complete: {successes}/{len(towns)} succeeded", platform)
    return results


def main():
    parser = argparse.ArgumentParser(description='Parallel Backfill Orchestrator')
    parser.add_argument('--dry-run', action='store_true', help='Preview what would run without executing')
    parser.add_argument('--platforms', nargs='*', help='Only run specific platform types (e.g., vision_appraisal MAPXPRESS)')
    parser.add_argument('--max-platforms', type=int, default=6, help='Max platforms running concurrently (default: 6)')
    parser.add_argument('--log-file', type=str, default='backfill_parallel.log', help='Log file path')
    args = parser.parse_args()
    
    global LOG_FILE
    log_path = os.path.join(os.path.dirname(__file__), '..', args.log_file)
    LOG_FILE = open(log_path, 'a')
    
    log("=" * 70)
    log(f"PARALLEL BACKFILL STARTED at {datetime.now().isoformat()}")
    log(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    log("=" * 70)
    
    # Group towns by platform
    platform_groups = group_by_platform(MUNICIPAL_DATA_SOURCES)
    
    # Filter platforms if requested
    if args.platforms:
        platform_groups = {k: v for k, v in platform_groups.items() if k in args.platforms}
    
    # Print summary
    log(f"\nPlatform Groups ({len(platform_groups)} platforms, {sum(len(v) for v in platform_groups.values())} towns total):")
    for ptype, towns in sorted(platform_groups.items(), key=lambda x: len(x[1]), reverse=True):
        top_towns = [t[0] for t in towns[:5]]
        log(f"  {ptype}: {len(towns)} towns (top: {', '.join(top_towns)}{'...' if len(towns) > 5 else ''})")
    log("")
    
    # Run all platform queues in parallel (one thread per platform type)
    all_results = []
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=args.max_platforms) as executor:
        future_to_platform = {
            executor.submit(process_platform_queue, ptype, towns, args.dry_run): ptype
            for ptype, towns in platform_groups.items()
        }
        
        for future in as_completed(future_to_platform):
            ptype = future_to_platform[future]
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                log(f"❌ Platform {ptype} failed entirely: {e}")
    
    total_time = time.time() - start_time
    total_success = sum(1 for _, _, ok in all_results if ok)
    total_towns = len(all_results)
    
    log("")
    log("=" * 70)
    log(f"BACKFILL COMPLETE in {total_time:.0f}s ({total_time/60:.1f} min)")
    log(f"Results: {total_success}/{total_towns} towns succeeded")
    
    # Report failures
    failures = [(name, t, ok) for name, t, ok in all_results if not ok]
    if failures:
        log(f"\nFailed towns ({len(failures)}):")
        for name, elapsed, _ in failures:
            log(f"  ❌ {name} ({elapsed:.0f}s)")
    
    log("=" * 70)
    
    if LOG_FILE:
        LOG_FILE.close()

if __name__ == '__main__':
    main()
