#!/bin/bash
set -euo pipefail
exec > /var/log/mega-cruncher.log 2>&1
BUCKET="${GCS_BUCKET:-renter-help-staging-20261119}"
WORKER_NAME="${WORKER_SCRIPT:-mega_cruncher_worker.py}"
echo "[SPOT] Booting renter-help Spot cruncher (bucket=${BUCKET}, worker=${WORKER_NAME})..."

apt-get update -qq && apt-get install -y -qq python3-requests curl >/dev/null 2>&1 || true

gcloud storage cp "gs://${BUCKET}/scripts/${WORKER_NAME}" "/tmp/${WORKER_NAME}" --quiet
GCS_BUCKET="${BUCKET}" python3 "/tmp/${WORKER_NAME}" || true

gcloud storage cp /var/log/mega-cruncher.log "gs://${BUCKET}/spot_parcels_v2/mega-cruncher.log" --quiet || true
echo "[SPOT] Self-deleting VM..."
ZONE=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
gcloud compute instances delete "$(hostname)" --zone="${ZONE}" --quiet || poweroff
