---
description: Monitor and manage Vision data enrichment process
---

# Vision Data Enrichment Workflow

This workflow manages the automated enrichment of property data across Connecticut municipalities.

## Quick Status Check

```bash
# Check current enrichment status
python3 scripts/vision_enrichment_status.py
```

## Manual Enrichment

### Run for Specific Town (Current Owner only)
```bash
docker exec ctdata_api python updater/update_vision_data.py "EAST HAMPTON" --current-owner-only
```

### Force Full Town Refresh
```bash
docker exec ctdata_api python updater/update_vision_data.py "CROMWELL" --force
```

### Run Multiple Towns
```bash
# Priority list
for town in "EAST HAMPTON" "CROMWELL" "RIDGEFIELD" "CHESHIRE"; do
    docker exec ctdata_api python updater/update_vision_data.py "$town" --current-owner-only
    sleep 60  # Pause between towns
done
```

## Automated Scheduling

### Enable Automated Updates (Recommended)
```bash
# Add vision_updater service to docker-compose.yml
docker compose up -d vision_updater
```

This will:
- Run nightly at 2 AM to update "Current Owner" properties
- Run weekly full scans on rotating towns
- Log all activity to `logs/scheduled_updates.log`

### Check Scheduler Logs
```bash
docker logs -f ctdata_vision_updater
```

## Monitoring

### View Enrichment Progress
```bash
python3 scripts/vision_enrichment_status.py
```

### Check Recent Database Updates
```bash
docker exec ctdata_api python -c "
import psycopg2
conn = psycopg2.connect('postgresql://user:password@ctdata_db:5432/ctdata')
cur = conn.cursor()
cur.execute('''
    SELECT property_city, COUNT(*) 
    FROM properties 
    WHERE owner != 'Current Owner' 
    GROUP BY property_city 
    ORDER BY COUNT(*) DESC 
    LIMIT 10
''')
for row in cur.fetchall():
    print(f'{row[0]:<20}: {row[1]:>6,} enriched properties')
"
```

## Troubleshooting

### Enrichment Process Stuck
```bash
# Check for active Python processes
docker exec ctdata_api ps aux | grep update_vision_data

# Kill stuck process if needed
docker exec ctdata_api pkill -f update_vision_data

# Restart enrichment
docker exec -d ctdata_api python updater/update_vision_data.py "TOWN_NAME" --current-owner-only
```

### Database Connection Issues
```bash
# Verify database is accessible
docker exec ctdata_api python -c "import psycopg2; psycopg2.connect('postgresql://user:password@ctdata_db:5432/ctdata'); print('✓ Connected')"
```

### Scraper Failures
```bash
# Test scraper for specific town
docker exec ctdata_api python -c "
from updater.update_vision_data import MUNICIPAL_DATA_SOURCES
print(MUNICIPAL_DATA_SOURCES.get('EAST HAMPTON'))
"
```

## Priority Towns

Current top priorities based on "Current Owner" count:
1. **EAST HAMPTON**: 6,175 properties (100%)
2. **CROMWELL**: 6,027 properties (100%)
3. **RIDGEFIELD**: 303 properties (3.3%)
4. **CHESHIRE**: 141 properties (1.3%)

## Update Frequency

- **Nightly**: Current Owner properties for all priority towns
- **Weekly**: Full refresh of one rotating priority town
- **Monthly**: All towns with "Current Owner" > 50
