#!/usr/bin/env python3
"""
Queue Next Scrapes & Rebuild Network
===================================
Orchestrates targeted scrapes for the next tier of municipalities:
- Stuck/Failed previously: NEW HAVEN, WATERBURY, GUILFORD, CROMWELL
- Stale (>=30 days): DANBURY, NEW BRITAIN, MANCHESTER, WEST HAVEN, STRATFORD, WALLINGFORD

Groups them by platform to run sequentially per platform and concurrently across platforms,
then triggers a safe network refresh.
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

# Priority target list of municipalities
TARGET_TOWNS = [
    'NEW HAVEN', 'WATERBURY', 'GUILFORD', 'CROMWELL',
    'DANBURY', 'NEW BRITAIN', 'MANCHESTER', 'WEST HAVEN',
    'STRATFORD', 'WALLINGFORD'
]

CT_POPULATIONS = {
    'NEW HAVEN': 134023, 'WATERBURY': 114403, 'DANBURY': 86518,
    'NEW BRITAIN': 74135, 'MANCHESTER': 59713, 'WEST HAVEN': 55584,
    'STRATFORD': 52355, 'WALLINGFORD': 44396, 'GUILFORD': 22375,
    'CROMWELL': 14225
}

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
    """Group our targeted municipalities by their platform type."""
    groups = defaultdict(list)
    for muni in TARGET_TOWNS:
        cfg = configs.get(muni, {})
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
    """Process all towns for a platform sequentially to avoid rate limiting."""
    log(f"Starting platform queue: {len(towns)} towns", platform)
    results = []
    for town_name, pop in towns:
        result = run_scrape_for_town(town_name, platform, dry_run)
        results.append(result)
        if not dry_run:
            time.sleep(2)
    successes = sum(1 for _, _, ok in results if ok)
    log(f"Platform queue complete: {successes}/{len(towns)} succeeded", platform)
    return results

def main():
    parser = argparse.ArgumentParser(description='Queue Next Scrapes & Rebuild Network')
    parser.add_argument('--dry-run', action='store_true', help='Preview what would run without executing')
    parser.add_argument('--max-platforms', type=int, default=4, help='Max platforms running concurrently')
    parser.add_argument('--log-file', type=str, default='logs/queue_next_scrapes.log', help='Log file path')
    args = parser.parse_args()
    
    global LOG_FILE
    log_path = os.path.join(os.path.dirname(__file__), '..', args.log_file)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    LOG_FILE = open(log_path, 'a')
    
    log("=" * 70)
    log(f"QUEUE TARGETED SCRAPES STARTED at {datetime.now().isoformat()}")
    log(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    log("=" * 70)
    
    platform_groups = group_by_platform(MUNICIPAL_DATA_SOURCES)
    
    log(f"\nPlatform Groups ({len(platform_groups)} platforms, {sum(len(v) for v in platform_groups.values())} towns targeted):")
    for ptype, towns in sorted(platform_groups.items(), key=lambda x: len(x[1]), reverse=True):
        muni_names = [t[0] for t in towns]
        log(f"  {ptype}: {len(towns)} towns ({', '.join(muni_names)})")
    log("")
    
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
    log(f"SCRAPES COMPLETE in {total_time:.0f}s ({total_time/60:.1f} min)")
    log(f"Results: {total_success}/{total_towns} towns succeeded")
    
    failures = [(name, t, ok) for name, t, ok in all_results if not ok]
    if failures:
        log(f"\nFailed towns ({len(failures)}):")
        for name, elapsed, _ in failures:
            log(f"  ❌ {name} ({elapsed:.0f}s)")
    log("=" * 70)
    
    # Run resilient network rebuild
    log("\n🔄 Initiating resilient network rebuild...")
    if args.dry_run:
        log("[DRY RUN] Would run: python api/safe_network_refresh.py")
    else:
        try:
            start_rebuild = time.time()
            rebuild_cmd = [sys.executable, "api/safe_network_refresh.py"]
            result = subprocess.run(
                rebuild_cmd,
                cwd=os.path.join(os.path.dirname(__file__), '..'),
                capture_output=True,
                text=True,
                env={**os.environ, 'DATABASE_URL': os.environ.get('DATABASE_URL', 'postgresql://user:password@ctdata_db:5432/ctdata')}
            )
            elapsed_rebuild = time.time() - start_rebuild
            if result.returncode == 0:
                log(f"✅ Network rebuild completed successfully in {elapsed_rebuild:.0f}s.")
            else:
                log(f"❌ Network rebuild failed with exit code {result.returncode}.")
                if result.stderr:
                    log(f"  STDERR: {result.stderr.strip()}")
        except Exception as e:
            log(f"❌ Network rebuild failed: {e}")
            
    log("=" * 70)
    if LOG_FILE:
        LOG_FILE.close()

if __name__ == '__main__':
    main()
