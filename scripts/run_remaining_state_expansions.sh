#!/bin/bash
set -euo pipefail
cd /home/sk/dev/theyownwhat
export DATABASE_URL="${DATABASE_URL:-postgresql://user:password@127.0.0.1:5432/ctdata}"

echo "=== [1/7] Streaming OpenCorporates registries for us_ca, us_mt, us_oh ==="
python3 scripts/ingest_opencorporates_bulk.py --append-clean --jurisdictions us_ca us_mt us_oh --workers 3 --shards 3
python3 scripts/sync_opencorporates_scheduled.py --sync-status-only

echo "=== [2/7] Ingesting & building ILLINOIS (1.98M Cook + DuPage parcels + Chicago Building Violations) ==="
python3 il/scrape_il_cama.py \
  --parcels-gz data/spot_parcels_v2/il_parcels.csv.gz \
  --overlays-gz data/spot_parcels_v2/il_overlays.csv.gz \
  --rebuild-networks

echo "=== [3/7] Ingesting & building CALIFORNIA (Track A LA/SF Rolls + Business Lessors + Evictions/Violations + us_ca) ==="
python3 ca/ingest_ca_statewide.py \
  --parcels-gz data/spot_parcels_v2/ca_parcels.csv.gz \
  --overlays-gz data/spot_parcels_v2/ca_overlays.csv.gz \
  --rebuild-networks

echo "=== [4/7] Ingesting & building MONTANA (56 counties), MINNESOTA (Twin Cities + Mpls unified), and OHIO (Cuyahoga) ==="
python3 states/ingest_spot_bonus_states.py --states mt mn oh

echo "=== [5/7] Enriching Federal NHPD Subsidies across fl, ny, il, ca, mt, mn, oh ==="
python3 scripts/enrich_all_cities_nhpd.py --city fl --city ny --city il --city ca --city mt --city mn --city oh

echo "=== [6/7] Re-clustering Existing Statewide & Municipal Networks with Hardened False-Positive Guards ==="
python3 scripts/rebuild_hardened_networks.py --skip-ra-refresh

echo "=== [7/7] Syncing Lightweight Upstream Freshness Telemetry & Rebuilding Landing Snapshot ==="
python3 updater/lightweight_state_refresher.py --no-deltas
python3 -m api.landing_cache

echo "=== ALL STATE EXPANSIONS, FALSE-POSITIVE HARDENING & SNAPSHOTS COMPLETE ==="
