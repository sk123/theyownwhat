#!/bin/bash
set -euo pipefail
exec > /var/log/spot-expansion-v5.log 2>&1
BUCKET="renter-help-staging-20261119"
echo "[SPOT-V5] Booting renter-help-expansion-v5..."

apt-get update -qq && apt-get install -y -qq python3-requests curl >/dev/null 2>&1 || true

gcloud storage cp "gs://${BUCKET}/scripts/spot_expansion_v5_worker.py" /tmp/spot_expansion_v5_worker.py --quiet
GCS_BUCKET="${BUCKET}" python3 /tmp/spot_expansion_v5_worker.py || true

gcloud storage cp /var/log/spot-expansion-v5.log "gs://${BUCKET}/spot_expansion_v5/spot-expansion-v5.log" --quiet || true
echo "[SPOT-V5] Self-deleting VM..."
ZONE=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
gcloud compute instances delete "$(hostname)" --zone="${ZONE}" --quiet || poweroff
