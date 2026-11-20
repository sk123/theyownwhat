#!/usr/bin/env python3
"""
gcp/pull_nightly_cloud_outputs.py
=================================
Pulls the compact nightly watermark manifest & 14-day enforcement deltas
produced by the Always-Free `theyownwhat-nightly-upstream-sync` Cloud Run Job
from `gs://renter-help-staging-20261119/nightly/` into `data/nightly_cloud/`.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOCAL_DIR = os.path.join(REPO_ROOT, "data", "nightly_cloud")
GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine"
GCLOUD_CONFIG = os.environ.get("GCLOUD_CONFIG", "storrsmagic")
PROJECT = os.environ.get("GCP_PROJECT", "renter-help")
BUCKET = os.environ.get("GCS_BUCKET", "renter-help-staging-20261119")


def main() -> int:
    os.makedirs(LOCAL_DIR, exist_ok=True)
    subprocess.run(
        [
            "docker", "run", "--rm",
            "-v", f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
            "-v", f"{os.path.join(REPO_ROOT, 'data')}:/data",
            "-e", f"CLOUDSDK_ACTIVE_CONFIG_NAME={GCLOUD_CONFIG}",
            "-e", f"CLOUDSDK_CORE_PROJECT={PROJECT}",
            GCLOUD_IMAGE,
            "gcloud", "storage", "cp",
            f"gs://{BUCKET}/nightly/*",
            "/data/nightly_cloud/",
            "--quiet",
        ],
        check=True,
        timeout=180,
    )
    wm = os.path.join(LOCAL_DIR, "upstream_watermarks.json")
    if os.path.exists(wm):
        with open(wm, encoding="utf-8") as f:
            data = json.load(f)
        print(
            f"✓ Pulled nightly cloud manifest ({data.get('generated_utc')}): "
            f"{data.get('probes_ok')}/{data.get('probes_total')} upstream probes OK, "
            f"{data.get('recent_delta_rows', 0):,} 14-day delta rows in {data.get('elapsed_sec')}s."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
