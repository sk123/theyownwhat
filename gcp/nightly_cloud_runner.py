#!/usr/bin/env python3
"""
gcp/nightly_cloud_runner.py
===========================
Stateless, Always-Free-tier Google Cloud Run Job runner executed nightly on
GCP project `renter-help` (`us-central1`) via Cloud Scheduler.

Runs directly inside `gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine`
using Python 3 stdlib only (zero pip installs, zero Artifact Registry image
storage fees, ~15-35s runtime = <0.5% of Cloud Run Jobs' 240,000 vCPU-sec/mo
Always-Free quota).

What it offloads from local home hardware:
  1. Concurrent upstream open-data freshness & row-count watermark probing
     across all 20 TheyOwnWhat jurisdictions over Google's backbone.
  2. Lightweight 14-day rolling public eviction & code-enforcement delta
     extraction (SF, LA, Chicago, Orlando, Gainesville) into compact `.csv.gz`
     under `gs://renter-help-staging-20261119/nightly/` (14-day bucket TTL).

Epistemic honesty (`docs/ASSURANCE_POLICY.md`):
  - Only queries authoritative public government APIs; never sends user data
    or private documents to the cloud; exposes exact HTTP status & errors.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime as dt
import gzip
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Tuple

BUCKET = os.environ.get("GCS_BUCKET", "renter-help-staging-20261119")
PREFIX = os.environ.get("GCS_NIGHTLY_PREFIX", "nightly")
WORK_DIR = "/tmp/nightly_cloud"
os.makedirs(WORK_DIR, exist_ok=True)

USER_AGENT = "Mozilla/5.0 (compatible; TheyOwnWhatNightlyCloudRunner/1.0)"

# Authoritative upstream public-data watermark probes across active jurisdictions
UPSTREAM_PROBES: List[Dict[str, str]] = [
    {"jur": "CA", "source": "SF_ASSESSOR_ROLL", "kind": "socrata", "url": "https://data.sfgov.org/api/views/wv5m-vpq2.json"},
    {"jur": "CA", "source": "SF_BIZ_LOCATIONS", "kind": "socrata", "url": "https://data.sfgov.org/api/views/g8m3-pdis.json"},
    {"jur": "CA", "source": "SF_EVICTIONS", "kind": "socrata", "url": "https://data.sfgov.org/api/views/5cei-gny5.json"},
    {"jur": "CA", "source": "SF_DBI_VIOLATIONS", "kind": "socrata", "url": "https://data.sfgov.org/api/views/nbtm-fbw5.json"},
    {"jur": "CA", "source": "SF_RENT_BOARD_INVENTORY", "kind": "socrata", "url": "https://data.sfgov.org/api/views/gdc7-dmcn.json"},
    {"jur": "CA", "source": "LA_ACTIVE_BUSINESSES", "kind": "socrata", "url": "https://data.lacity.org/api/views/6rrh-rzua.json"},
    {"jur": "CA", "source": "LA_EVICTION_NOTICES", "kind": "socrata", "url": "https://data.lacity.org/api/views/2u8b-eyuu.json"},
    {"jur": "CA", "source": "LA_CODE_ENFORCEMENT", "kind": "socrata", "url": "https://data.lacity.org/api/views/u82d-eh7z.json"},
    {"jur": "CA", "source": "LA_COUNTY_PARCELS", "kind": "arcgis", "url": "https://public.gis.lacounty.gov/public/rest/services/LACounty_Cache/LACounty_Parcel/MapServer/0?f=json"},
    {"jur": "IL", "source": "COOK_ASSESSOR_ADDRESSES", "kind": "socrata", "url": "https://datacatalog.cookcountyil.gov/api/views/3723-97qp.json"},
    {"jur": "IL", "source": "CHICAGO_BLDG_VIOLATIONS", "kind": "socrata", "url": "https://data.cityofchicago.org/api/views/22u3-xenr.json"},
    {"jur": "IL", "source": "DUPAGE_PARCELS", "kind": "arcgis", "url": "https://gis.dupageco.org/arcgis/rest/services/DuPage_County_IL/ParcelsWithRealEstateCC/FeatureServer/0?f=json"},
    {"jur": "IL", "source": "LAKE_COUNTY_PARCELS", "kind": "arcgis", "url": "https://services3.arcgis.com/HESxeTbDliKKvec2/arcgis/rest/services/OpenData_ParcelPolygons/FeatureServer/0?f=json"},
    {"jur": "FL", "source": "ORLANDO_CODE_CASES", "kind": "socrata", "url": "https://data.cityoforlando.net/api/views/k6e8-nw6w.json"},
    {"jur": "FL", "source": "GAINESVILLE_CODE_CASES", "kind": "socrata", "url": "https://data.cityofgainesville.org/api/views/vu9p-a5f7.json"},
    {"jur": "NY", "source": "NYC_HPD_REGISTRATIONS", "kind": "socrata", "url": "https://data.cityofnewyork.us/api/views/tesw-yqqr.json"},
    {"jur": "NY", "source": "NYC_HPD_VIOLATIONS", "kind": "socrata", "url": "https://data.cityofnewyork.us/api/views/wvxf-dwi5.json"},
    {"jur": "NY", "source": "MONROE_COUNTY_PARCELS", "kind": "arcgis", "url": "https://services.arcgis.com/04HiymDgLlsbhaV4/arcgis/rest/services/MONROE_COUNTY_PARCELS_2025/FeatureServer/0?f=json"},
    {"jur": "NY", "source": "SYRACUSE_RENTAL_REGISTRY", "kind": "arcgis", "url": "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Syracuse_Rental_Registry/FeatureServer/0?f=json"},
    {"jur": "NY", "source": "ROCHESTER_CODE_ENFORCEMENT", "kind": "arcgis", "url": "https://maps.cityofrochester.gov/arcgis/rest/services/NBD/Building_Blocks_Code_Enforcement/MapServer/0?f=json"},
    {"jur": "OH", "source": "FRANKLIN_COUNTY_AUDITOR", "kind": "arcgis", "url": "https://gis.franklincountyohio.gov/hosting/rest/services/ParcelFeatures/Parcel_Features/MapServer/0?f=json"},
    {"jur": "OH", "source": "HAMILTON_COUNTY_CAGIS", "kind": "arcgis", "url": "https://services.arcgis.com/JyZag7oO4NteHGiq/arcgis/rest/services/Parcel_Polygons/FeatureServer/0?f=json"},
    {"jur": "MN", "source": "DAKOTA_COUNTY_PARCELS", "kind": "arcgis", "url": "https://gis2.co.dakota.mn.us/arcgis/rest/services/DCGIS_OL_PropertyInformation/MapServer/71?f=json"},
    {"jur": "MN", "source": "ANOKA_METC_PARCELS", "kind": "arcgis", "url": "https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcels/FeatureServer/0?f=json"},
    {"jur": "AR", "source": "ARKANSAS_GIS_PARCELS", "kind": "arcgis", "url": "https://gis.arkansas.gov/arcgis/rest/services/FEATURESERVICES/Planning_Cadastre/FeatureServer/6?f=json"},
]


def _http_get_json(url: str, timeout: int = 20) -> Tuple[int, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        code = getattr(resp, "status", 200)
        body = resp.read().decode("utf-8", errors="replace")
        return code, json.loads(body)


def probe_one(spec: Dict[str, str]) -> Dict[str, Any]:
    t0 = time.time()
    out: Dict[str, Any] = {
        "jur": spec["jur"],
        "source": spec["source"],
        "kind": spec["kind"],
        "url": spec["url"],
        "checked_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    try:
        code, data = _http_get_json(spec["url"], timeout=20)
        out["http_status"] = code
        out["latency_ms"] = int((time.time() - t0) * 1000)
        if spec["kind"] == "socrata" and isinstance(data, dict):
            ts = data.get("rowsUpdatedAt") or data.get("viewLastModified")
            if isinstance(ts, (int, float)) and ts > 0:
                out["upstream_updated_utc"] = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat()
            out["dataset_name"] = data.get("name")
            out["ok"] = True
        elif spec["kind"] == "arcgis" and isinstance(data, dict):
            if "error" in data:
                out["ok"] = False
                out["error"] = str(data["error"])[:200]
            else:
                ed = (data.get("editingInfo") or {}).get("lastEditDate")
                if isinstance(ed, (int, float)) and ed > 10_000_000_000:
                    out["upstream_updated_utc"] = dt.datetime.fromtimestamp(ed / 1000.0, tz=dt.timezone.utc).isoformat()
                out["layer_name"] = data.get("name")
                out["max_record_count"] = data.get("maxRecordCount")
                out["ok"] = True
        else:
            out["ok"] = True
    except Exception as exc:
        out["ok"] = False
        out["latency_ms"] = int((time.time() - t0) * 1000)
        out["error"] = f"{type(exc).__name__}: {str(exc)[:180]}"
    return out


def fetch_recent_socrata_deltas(days_back: int = 14) -> List[List[str]]:
    since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00")
    feeds = [
        ("ca", "eviction", "data.sfgov.org", "5cei-gny5", "eviction_id,address,zip,file_date", f"file_date >= '{since}'", "eviction_id", "address", "SAN FRANCISCO", "zip", "file_date", "FILED"),
        ("ca", "violation", "data.sfgov.org", "nbtm-fbw5", "primary_key,date_filed,street_number,street_name,street_suffix,status,zipcode", f"date_filed >= '{since}'", "primary_key", "", "SAN FRANCISCO", "zipcode", "date_filed", "status"),
        ("il", "violation", "data.cityofchicago.org", "22u3-xenr", "id,violation_date,violation_status,address", f"violation_date >= '{since}'", "id", "address", "CHICAGO", "", "violation_date", "violation_status"),
        ("fl", "violation", "data.cityoforlando.net", "k6e8-nw6w", "apno,caseinfostatus,casename,casedt,parcel_id", f"casedt >= '{since}'", "apno", "casename", "ORLANDO", "", "casedt", "caseinfostatus"),
        ("fl", "violation", "data.cityofgainesville.org", "vu9p-a5f7", "number,address,status,infraction,parcel", f"infraction >= '{since}'", "number", "address", "GAINESVILLE", "", "infraction", "status"),
    ]
    rows: List[List[str]] = []
    for jur, kind, dom, ds, sel, wh, id_k, addr_k, city, zip_k, dt_k, st_k in feeds:
        qs = urllib.parse.urlencode({"$select": sel, "$where": wh, "$limit": 5000})
        url = f"https://{dom}/resource/{ds}.json?{qs}"
        try:
            _, data = _http_get_json(url, timeout=25)
            if not isinstance(data, list):
                continue
            for r in data:
                rid = str(r.get(id_k) or "").strip()
                if not rid:
                    continue
                if addr_k:
                    addr = str(r.get(addr_k) or "").strip()
                else:
                    addr = " ".join(str(r.get(k) or "").strip() for k in ("street_number", "street_name", "street_suffix") if r.get(k))
                status = str(r.get(st_k) or st_k).strip() if st_k in r else st_k
                ev_dt = str(r.get(dt_k) or "")[:10]
                zp = str(r.get(zip_k) or "")[:5] if zip_k else ""
                parcel = str(r.get("parcel_id") or r.get("parcel") or "").strip()
                rows.append([jur, kind, f"{ds}:{rid}", parcel, addr, city, zp, status, ev_dt])
        except Exception as exc:
            print(f"[WARN] Delta feed {dom}/{ds} skipped: {exc}", flush=True)
    return rows


def main() -> int:
    t0 = time.time()
    print(f"[NIGHTLY-CLOUD] Probing {len(UPSTREAM_PROBES)} upstream endpoints + 14-day enforcement deltas...", flush=True)
    probes: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futs = [pool.submit(probe_one, spec) for spec in UPSTREAM_PROBES]
        for fut in as_completed(futs):
            probes.append(fut.result())
    probes.sort(key=lambda x: (x["jur"], x["source"]))
    ok_cnt = sum(1 for p in probes if p.get("ok"))

    delta_rows = fetch_recent_socrata_deltas(days_back=14)
    delta_path = os.path.join(WORK_DIR, "recent_deltas.csv.gz")
    with gzip.open(delta_path, "wt", encoding="utf-8", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(["jur", "record_kind", "record_id", "parcel_hint", "address", "city", "zip_code", "status", "event_date"])
        w.writerows(delta_rows)

    manifest = {
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - t0, 2),
        "probes_ok": ok_cnt,
        "probes_total": len(probes),
        "recent_delta_rows": len(delta_rows),
        "probes": probes,
    }
    wm_path = os.path.join(WORK_DIR, "upstream_watermarks.json")
    with open(wm_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    gcs_mount_dir = os.environ.get("GCS_MOUNT_DIR", "/mnt/gcs")
    mounted_out_dir = os.path.join(gcs_mount_dir, PREFIX)
    if os.path.isdir(gcs_mount_dir):
        os.makedirs(mounted_out_dir, exist_ok=True)
        for local_p, name in ((wm_path, "upstream_watermarks.json"), (delta_path, "recent_deltas.csv.gz")):
            dst_p = os.path.join(mounted_out_dir, name)
            with open(local_p, "rb") as src_f, open(dst_p, "wb") as dst_f:
                dst_f.write(src_f.read())
            print(f"  -> Wrote {dst_p} via Cloud Storage FUSE ({os.path.getsize(local_p):,} bytes)", flush=True)
    else:
        for local_p, name in ((wm_path, "upstream_watermarks.json"), (delta_path, "recent_deltas.csv.gz")):
            dst = f"gs://{BUCKET}/{PREFIX}/{name}"
            subprocess.run(["gcloud", "storage", "cp", local_p, dst, "--quiet"], check=True, timeout=120)
            print(f"  -> Uploaded {dst} ({os.path.getsize(local_p):,} bytes)", flush=True)

    print(
        f"[NIGHTLY-CLOUD] Done in {manifest['elapsed_sec']}s: "
        f"{ok_cnt}/{len(probes)} upstream probes OK, {len(delta_rows):,} recent 14-day delta rows.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
