#!/usr/bin/env python3
"""
Overnight Batch Enrichment Runner
Processes ~26,953 properties across 19 municipalities to extract unit numbers
Estimated runtime: 4-6 hours
"""
import subprocess
import time
from datetime import datetime

# Priority municipalities with configured data sources
# Ordered by missing unit count (highest first)
TOWNS_TO_PROCESS = [
    'MANCHESTER',      # 2,385 missing (99.7%) - Vision Appraisal
    'NORWICH',         # 2,350 missing (98.4%) - Vision Appraisal  
    'BRISTOL',         # 2,014 missing (95.5%) - MapXpress
    'NEW LONDON',      # 1,939 missing (99.4%) - PropertyRecordCards
    'MIDDLETOWN',      # 1,659 missing (95.5%) - Vision Appraisal
    'HAMDEN',          # 1,545 missing (96.9%) - MapXpress
    'WEST HARTFORD',   # 1,274 missing (99.5%) - PropertyRecordCards
    'ENFIELD',         # 1,082 missing (94.9%) - Vision Appraisal
    'MILFORD',         # 969 missing (99.2%) - Vision Appraisal
    'SOUTHINGTON',     # 734 missing (80.1%) - Vision Appraisal
    'WINCHESTER',      # 618 missing (99.7%) - PropertyRecordCards
    'TRUMBULL',        # 366 missing (96.3%) - Vision Appraisal
    'MERIDEN',         # 344 missing (97.7%) - PropertyRecordCards
    'CLINTON',         # 292 missing (97.0%) - PropertyRecordCards
    'STRATFORD',       # 220 missing (99.1%) - Vision Appraisal
    'OLD LYME',        # 136 missing (94.4%) - PropertyRecordCards
    'HARTFORD',        # 113 missing (92.6%) - Hartford CAMA (auto-runs)
    'ESSEX',           # 108 missing (52.7%) - PropertyRecordCards
]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def main():
    log("=" * 80)
    log(f"OVERNIGHT BATCH ENRICHMENT - {len(TOWNS_TO_PROCESS)} MUNICIPALITIES")
    log("=" * 80)
    log("Estimated properties to process: ~26,953")
    log("Estimated runtime: 4-6 hours")
    log("")
    
    start_time = time.time()
    completed = 0
    failed = 0
    
    for i, town in enumerate(TOWNS_TO_PROCESS, 1):
        log(f"[{i}/{len(TOWNS_TO_PROCESS)}] Starting: {town}")
        
        try:
            # Run enrichment on HOST (updater not mounted in container)
            # Assumes virtual environment or system Python has dependencies
            cmd = [
                'python3', 'updater/update_vision_data.py',
                town, '--force'
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour max per town
                cwd='/home/sk/dev/theyownwhat'  # Run from project root
            )
            
            if result.returncode == 0:
                log(f"  ✅ {town} completed successfully")
                completed += 1
            else:
                log(f"  ❌ {town} failed (exit code {result.returncode})")
                if result.stderr:
                    log(f"     Error: {result.stderr[:200]}")
                failed += 1
        
        except subprocess.TimeoutExpired:
            log(f"  ⏱️  {town} timed out after 1 hour - skipping")
            failed += 1
        except Exception as e:
            log(f"  ❌ {town} error: {e}")
            failed += 1
        
        # Brief pause between towns
        if i < len(TOWNS_TO_PROCESS):
            time.sleep(30)
    
    # Summary
    duration_hours = (time.time() - start_time) / 3600
    log("")
    log("=" * 80)
    log(f"COMPLETE - {duration_hours:.2f} hours")
    log(f"Completed: {completed}/{len(TOWNS_TO_PROCESS)}")
    log(f"Failed: {failed}/{len(TOWNS_TO_PROCESS)}")
    log("=" * 80)
    
    return failed == 0

if __name__ == '__main__':
    import sys
    sys.exit(0 if main() else 1)
