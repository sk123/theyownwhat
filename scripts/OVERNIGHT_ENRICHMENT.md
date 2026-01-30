# Overnight Unit Number Enrichment

Target: **26,953 properties** across 18 municipalities lacking unit information

## Towns To Process (In Order)

1. **MANCHESTER** - 2,385 missing (99.7%) - Vision Appraisal
2. **NORWICH** - 2,350 missing (98.4%) - Vision Appraisal
3. **BRISTOL** - 2,014 missing (95.5%) - MapXpress
4. **NEW LONDON** - 1,939 missing (99.4%) - PropertyRecordCards
5. **MIDDLETOWN** - 1,659 missing (95.5%) - Vision Appraisal
6. **HAMDEN** - 1,545 missing (96.9%) - MapXpress 
7. **WEST HARTFORD** - 1,274 missing (99.5%) - PropertyRecordCards
8. **ENFIELD** - 1,082 missing (94.9%) - Vision Appraisal
9. **MILFORD** - 969 missing (99.2%) - Vision Appraisal
10. **SOUTHINGTON** - 734 missing (80.1%) - Vision Appraisal
11. **WINCHESTER** - 618 missing (99.7%) - PropertyRecordCards
12. **TRUMBULL** - 366 missing (96.3%) - Vision Appraisal
13. **MERIDEN** - 344 missing (97.7%) - PropertyRecordCards
14. **CLINTON** - 292 missing (97.0%) - PropertyRecordCards
15. **STRATFORD** - 220 missing (99.1%) - Vision Appraisal
16. **OLD LYME** - 136 missing (94.4%) - PropertyRecordCards
17. **HARTFORD** - 113 missing (92.6%) - Hartford CAMA
18. **ESSEX** - 108 missing (52.7%) - PropertyRecordCards

## Runtime Estimate

- **Duration**: 4-6 hours
- **Per town**: 15-20 minutes average
- **Pause between towns**: 30 seconds

## How to Run

> **Note**: The `updater` directory is not mounted in the Docker container, so these scripts run from the host machine.

### Option 1: Start Now (Background)
```bash
cd /home/sk/dev/theyownwhat
nohup python3 scripts/overnight_enrichment.py > logs/overnight_enrichment_$(date +%Y%m%d).log 2>&1 &
```

### Option 2: Schedule for Tonight (2 AM)
```bash
(crontab -l 2>/dev/null; echo "0 2 * * * cd /home/sk/dev/theyownwhat && python3 scripts/overnight_enrichment.py > logs/overnight_enrichment_\$(date +\%Y\%m\%d).log 2>&1") | crontab -
```

### Option 3: Run Interactively (See Progress)
```bash
cd /home/sk/dev/theyownwhat
python3 scripts/overnight_enrichment.py | tee logs/overnight_$(date +%Y%m%d).log
```

## Monitor Progress

```bash
# Watch live log
tail -f logs/overnight_enrichment_*.log

# Check current status
python3 scripts/vision_enrichment_status.py
```

## What It Does

For each town:
1. Connects to town's assessor website (Vision Appraisal, MapXpress, or PropertyRecordCards)
2. Scrapes property details for all multi-unit properties
3. Extracts unit numbers from property records
4. Updates database with unit information
5. Logs progress and errors

## Expected Results

After completion:
- ~26,953 properties updated with unit numbers
- 18 municipalities with complete unit data
- Multi-unit properties properly identified in searches
- Network analysis improved with accurate unit counts
