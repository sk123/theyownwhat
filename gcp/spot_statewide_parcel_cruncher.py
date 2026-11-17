#!/usr/bin/env python3
"""
gcp/spot_statewide_parcel_cruncher.py
=====================================
Provisions an ephemeral, hardware-TTL Google Compute Engine Spot VM
(`e2-standard-8`, `--provisioning-model=SPOT`, `--max-run-duration=2h`,
`--instance-termination-action=DELETE`) on GCP project `theyownwhat` to crunch
statewide property parcel FeatureServers over Google's 10 Gbps backbone, upload
compressed `.csv.gz` parcel spines to `gs://theyownwhat-oc-staging-20261117/spot_parcels/`,
immediately delete the Spot VM upon completion, and import the new statewide
partition (`ar` — Arkansas 2.15M statewide parcels across all 75 counties, wired
to OpenCorporates `us_ar`) into local PostgreSQL `us_properties` / `us_networks`.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extras import execute_values

csv.field_size_limit(10_000_000)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_jurisdiction_partition  # noqa: E402
from api.oc_linker import demangle_assessor_owner  # noqa: E402
from api.populate_cities import normalize_link_address, normalize_owner_name, source_float, source_text  # noqa: E402
from api.shared_utils import normalize_mailing_address_coarse, strip_benign_care_of_prefix  # noqa: E402

GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine"
DEFAULT_PROJECT = "theyownwhat"
DEFAULT_ZONE = "us-east1-b"
DEFAULT_BUCKET = "theyownwhat-oc-staging-20261117"
VM_NAME = "theyownwhat-spot-parcel-cruncher"

# Self-contained Python worker executed inside the ephemeral Spot VM
SPOT_STARTUP_SCRIPT = r"""#!/bin/bash
set -euo pipefail
exec > /var/log/parcel-cruncher.log 2>&1
echo "[SPOT] Starting Arkansas 2.15M-parcel parallel ArcGIS cruncher..."

apt-get update -qq && apt-get install -y -qq python3-requests curl google-cloud-cli >/dev/null 2>&1 || true

cat << 'EOF' > /tmp/crunch_ar.py
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv, gzip, json, time, requests

URL = "https://gis.arkansas.gov/arcgis/rest/services/FEATURESERVICES/Planning_Cadastre/FeatureServer/6/query"
OUT_FIELDS = "objectid,countyid,parcelid,ownername,adrlabel,pstrnam,adrcity,adrzip5,parceltype,totalvalue,assessvalue,county"

def get_oid_bounds():
    r = requests.get(URL, params={
        "where": "1=1",
        "outStatistics": json.dumps([
            {"statisticType": "min", "onStatisticField": "objectid", "outStatisticFieldName": "min_oid"},
            {"statisticType": "max", "onStatisticField": "objectid", "outStatisticFieldName": "max_oid"},
        ]),
        "f": "json",
    }, timeout=30).json()
    attrs = ((r.get("features") or [{}])[0].get("attributes")) or {}
    return int(attrs.get("min_oid") or 1), int(attrs.get("max_oid") or 2500000)

def fetch_shard(lo, hi, sid):
    sess = requests.Session()
    cur_oid = lo - 1
    rows = []
    while True:
        where = f"objectid > {cur_oid} AND objectid <= {hi} AND ownername IS NOT NULL"
        params = {
            "where": where,
            "outFields": OUT_FIELDS,
            "returnGeometry": "false",
            "returnCentroid": "true",
            "outSR": "4326",
            "orderByFields": "objectid ASC",
            "resultRecordCount": 2000,
            "f": "json",
        }
        data = {}
        for att in range(5):
            try:
                resp = sess.get(URL, params=params, timeout=45)
                resp.raise_for_status()
                data = resp.json()
                if "error" not in data:
                    break
            except Exception:
                pass
            time.sleep(2 * (att + 1))
        feats = data.get("features") or []
        if not feats:
            break
        max_seen = cur_oid
        for f in feats:
            a = f.get("attributes") or {}
            oid = int(a.get("objectid") or 0)
            if oid > max_seen:
                max_seen = oid
            owner = str(a.get("ownername") or "").strip()
            pid = str(a.get("countyid") or a.get("parcelid") or "").strip()
            if not owner or not pid:
                continue
            addr = str(a.get("adrlabel") or a.get("pstrnam") or "").strip()
            city = str(a.get("adrcity") or "").strip()
            z = str(a.get("adrzip5") or "").strip()[:5]
            cnty = str(a.get("county") or "").strip().upper()
            val = a.get("totalvalue") if a.get("totalvalue") is not None else a.get("assessvalue")
            ptype = str(a.get("parceltype") or "").strip()
            cent = f.get("centroid") or {}
            lon, lat = cent.get("x"), cent.get("y")
            rows.append((pid, addr, cnty, city, z, owner, ptype, val, lat, lon))
        if max_seen <= cur_oid:
            break
        cur_oid = max_seen
        if len(feats) < 2000 and not data.get("exceededTransferLimit"):
            break
    print(f"[Shard {sid}] fetched {len(rows):,} AR parcels", flush=True)
    return rows

min_o, max_o = get_oid_bounds()
n_shards = 16
step = max(1, (max_o - min_o + 1) // n_shards)
ranges = [(min_o + i*step, max_o if i == n_shards-1 else min_o + (i+1)*step - 1, i+1) for i in range(n_shards)]

dedup = {}
with ThreadPoolExecutor(max_workers=16) as pool:
    futs = [pool.submit(fetch_shard, lo, hi, sid) for lo, hi, sid in ranges]
    for fut in as_completed(futs):
        for r in fut.result():
            dedup[r[0]] = r

with gzip.open("/tmp/ar_parcels.csv.gz", "wt", encoding="utf-8", newline="") as gz:
    w = csv.writer(gz)
    w.writerow(["bbl", "address", "county", "city", "zip_code", "owner_name", "bld_class", "assessed_total", "latitude", "longitude"])
    w.writerows(dedup.values())

with open("/tmp/ar_done.json", "w") as jf:
    json.dump({"state": "ar", "parcels": len(dedup), "timestamp": time.time()}, jf)
print(f"[SPOT] Wrote {len(dedup):,} deduplicated AR parcels!", flush=True)
EOF

python3 /tmp/crunch_ar.py
gcloud storage cp /tmp/ar_parcels.csv.gz gs://__BUCKET__/spot_parcels/ar_parcels.csv.gz
gcloud storage cp /tmp/ar_done.json gs://__BUCKET__/spot_parcels/ar_done.json
echo "[SPOT] Upload complete; shutting down."
shutdown -h now || true
"""


def _gcloud(cmd_str: str, project: str, mount_data: bool = True, timeout: int = 600) -> str:
    vols = ["-v", f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud"]
    if mount_data:
        vols.extend(["-v", f"{os.path.join(REPO_ROOT, 'data')}:/data"])
    proc = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            *vols,
            "-e",
            f"CLOUDSDK_CORE_PROJECT={project}",
            GCLOUD_IMAGE,
            "bash",
            "-c",
            cmd_str,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"gcloud failed ({proc.returncode}): {proc.stderr[-1200:]}")
    return (proc.stdout or "") + (proc.stderr or "")


def run_spot_vm_and_download(project: str, zone: str, bucket: str) -> str:
    spot_dir = os.path.join(REPO_ROOT, "data", "opencorporates", "spot_parcels")
    os.makedirs(spot_dir, exist_ok=True)
    local_gz = os.path.join(spot_dir, "ar_parcels.csv.gz")
    if os.path.exists(local_gz) and os.path.getsize(local_gz) > 5_000_000:
        print(f"[SPOT] Existing `{local_gz}` found ({os.path.getsize(local_gz)/1e6:.1f} MB); skipping VM spin-up.")
        return local_gz

    startup_path = os.path.join(spot_dir, "startup_ar.sh")
    with open(startup_path, "w", encoding="utf-8") as f:
        f.write(SPOT_STARTUP_SCRIPT.replace("__BUCKET__", bucket))

    print(f"[SPOT] Enabling Compute Engine API & launching Spot VM `{VM_NAME}` (2h hard auto-DELETE TTL)...", flush=True)
    _gcloud("gcloud services enable compute.googleapis.com", project, mount_data=False)

    # Clean any stale done marker or prior VM
    _gcloud(
        f"""
        gcloud storage rm gs://{bucket}/spot_parcels/ar_done.json >/dev/null 2>&1 || true
        gcloud compute instances delete {VM_NAME} --zone={zone} --quiet >/dev/null 2>&1 || true
        gcloud compute instances create {VM_NAME} \
            --zone={zone} \
            --machine-type=e2-standard-8 \
            --provisioning-model=SPOT \
            --instance-termination-action=DELETE \
            --max-run-duration=2h \
            --scopes=https://www.googleapis.com/auth/devstorage.read_write \
            --image-family=debian-12 \
            --image-project=debian-cloud \
            --boot-disk-size=20GB \
            --metadata-from-file=startup-script=/data/opencorporates/spot_parcels/startup_ar.sh
        """,
        project,
        timeout=300,
    )
    print(f"[SPOT] `{VM_NAME}` launched! Waiting for `gs://{bucket}/spot_parcels/ar_done.json`...", flush=True)

    t0 = time.time()
    try:
        while time.time() - t0 < 2700:
            time.sleep(20)
            chk = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
                    "-e",
                    f"CLOUDSDK_CORE_PROJECT={project}",
                    GCLOUD_IMAGE,
                    "gcloud",
                    "storage",
                    "ls",
                    f"gs://{bucket}/spot_parcels/ar_done.json",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if chk.returncode == 0:
                print(f"[SPOT] `ar_done.json` appeared after {time.time() - t0:.1f}s! Downloading `ar_parcels.csv.gz`...", flush=True)
                _gcloud(
                    f"gcloud storage cp gs://{bucket}/spot_parcels/ar_parcels.csv.gz /data/opencorporates/spot_parcels/ar_parcels.csv.gz",
                    project,
                    timeout=300,
                )
                break
        else:
            raise TimeoutError("Spot VM did not produce `ar_done.json` within 45 minutes.")
    finally:
        print(f"[SPOT] Deleting `{VM_NAME}` immediately so zero idle billing accrues...", flush=True)
        _gcloud(f"gcloud compute instances delete {VM_NAME} --zone={zone} --quiet >/dev/null 2>&1 || true", project, mount_data=False)

    return local_gz


def import_ar_into_postgres(gz_path: str) -> Tuple[int, int]:
    db_url = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in db_url and not os.path.exists("/.dockerenv"):
        db_url = db_url.replace("@ctdata_db", "@127.0.0.1")

    # 1. First ensure `us_ar` is loaded in `oc_*` (`788k` companies, `2.69M` officers!)
    with psycopg2.connect(db_url) as chk_conn:
        with chk_conn.cursor() as chk_cur:
            chk_cur.execute("SELECT EXISTS (SELECT 1 FROM oc_companies WHERE jurisdiction_code = 'us_ar' LIMIT 1)")
            has_ar_oc = bool(chk_cur.fetchone()[0])
    if not has_ar_oc:
        subprocess.run(
            [
                sys.executable,
                os.path.join(REPO_ROOT, "scripts", "ingest_opencorporates_bulk.py"),
                "--jurisdictions",
                "us_ar",
                "--append-clean",
                "--shards",
                "6",
                "--workers",
                "6",
            ],
            check=True,
        )

    conn = psycopg2.connect(db_url)
    try:
        ensure_jurisdiction_partition(conn, "ar")
        batch: List[Tuple] = []
        total = 0
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE ar_properties;")
            with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace", newline="") as gz:
                for r in csv.DictReader(gz):
                    bbl = source_text(r.get("bbl"))
                    owner = source_text(r.get("owner_name"))
                    if not bbl or not owner:
                        continue
                    addr_base = source_text(r.get("address")) or "UNNUMBERED PARCEL"
                    city = source_text(r.get("city"))
                    full_addr = f"{addr_base}, {city}" if city and city.upper() not in addr_base.upper() else addr_base
                    borough = (source_text(r.get("county")) or city or "ARKANSAS").upper()
                    z = (source_text(r.get("zip_code")) or "")[:5] or None
                    lat = source_float(r.get("latitude"))
                    lon = source_float(r.get("longitude"))
                    if lat is not None and lon is not None:
                        if not (32.95 <= lat <= 36.55 and -94.65 <= lon <= -89.60):
                            lat, lon = None, None
                    dem = demangle_assessor_owner(owner, col_max_len=50)
                    batch.append(
                        (
                            "ar",
                            bbl,
                            full_addr,
                            borough,
                            z,
                            dem.cleaned_owner or owner,
                            dem.owner_norm or normalize_owner_name(owner),
                            dem.care_of_entity,
                            dem.is_truncated,
                            None,
                            None,
                            None,
                            None,
                            source_text(r.get("bld_class")),
                            source_float(r.get("assessed_total")),
                            lat,
                            lon,
                            "ARKANSAS_GIS_PARCELS",
                        )
                    )
                    if len(batch) >= 10000:
                        execute_values(
                            cur,
                            """
                            INSERT INTO ar_properties (
                                jurisdiction, bbl, address, borough, zip_code,
                                owner_name, owner_name_norm, owner_co_name, owner_is_truncated,
                                mailing_address, mailing_address_norm, mailing_address_coarse, mailing_state,
                                bld_class, assessed_total, latitude, longitude, source_system
                            ) VALUES %s ON CONFLICT (jurisdiction, bbl) DO NOTHING;
                            """,
                            batch,
                            page_size=3000,
                        )
                        total += len(batch)
                        batch.clear()
            if batch:
                execute_values(
                    cur,
                    """
                    INSERT INTO ar_properties (
                        jurisdiction, bbl, address, borough, zip_code,
                        owner_name, owner_name_norm, owner_co_name, owner_is_truncated,
                        mailing_address, mailing_address_norm, mailing_address_coarse, mailing_state,
                        bld_class, assessed_total, latitude, longitude, source_system
                    ) VALUES %s ON CONFLICT (jurisdiction, bbl) DO NOTHING;
                    """,
                    batch,
                    page_size=3000,
                )
                total += len(batch)
                batch.clear()
        conn.commit()
        print(f"✓ Inserted {total:,} Arkansas parcels into `ar_properties`. Running OC linker + network builder...", flush=True)

        from states.ingest_statewide_parcels import build_and_save_state_networks
        return build_and_save_state_networks(conn, "ar", "AR", "ARKANSAS_GIS_PARCELS")
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Crunch Arkansas 2.15M statewide parcels on GCE Spot VM & ingest into `ar_*`.")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--zone", default=DEFAULT_ZONE)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--fetch-only", action="store_true", help="Only run GCE Spot VM fetch + download, skip DB import")
    parser.add_argument("--import-only", action="store_true", help="Import already-downloaded `ar_parcels.csv.gz` into PostgreSQL")
    args = parser.parse_args()

    spot_gz = os.path.join(REPO_ROOT, "data", "opencorporates", "spot_parcels", "ar_parcels.csv.gz")
    if not args.import_only:
        spot_gz = run_spot_vm_and_download(args.project, args.zone, args.bucket)
    if not args.fetch_only:
        p_cnt, n_cnt = import_ar_into_postgres(spot_gz)
        print(f"✓ Arkansas (`ar`) complete: {p_cnt:,} parcels -> {n_cnt:,} networks!", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
