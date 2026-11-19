#!/bin/bash
set -euo pipefail
exec > /var/log/mega-cruncher.log 2>&1
echo "[SPOT] Booting theyownwhat-mega-cruncher..."

apt-get update -qq && apt-get install -y -qq python3-requests curl >/dev/null 2>&1 || true

gcloud storage cp gs://theyownwhat-oc-staging-20261117/scripts/mega_cruncher_worker.py /tmp/mega_cruncher_worker.py --quiet
python3 /tmp/mega_cruncher_worker.py || true

gcloud storage cp /var/log/mega-cruncher.log gs://theyownwhat-oc-staging-20261117/spot_parcels_v2/mega-cruncher.log --quiet || true
echo "[SPOT] Self-deleting VM..."
ZONE=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
gcloud compute instances delete "$(hostname)" --zone="${ZONE}" --quiet || poweroff
