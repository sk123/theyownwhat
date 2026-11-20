#!/usr/bin/env python3
"""
gcp/bq_preserve_and_cross_state.py
==================================
Billed strictly to `renter-help` (`--configuration=storrsmagic`, expiring credits)
reading `theyownwhat:opencorporates_20261117` via least-privilege `READER` ACL
before its Dec. 8–10, 2026 table expiration.

Creates a 10-day auto-expiring scratch dataset `renter-help:oc_preserve_tmp`
(auto-deletes before Dec. 12 credit expiry), runs 5 permanent high-value
analytical extracts across all 51 US jurisdictions, exports compact `.csv.gz`
files through `gs://renter-help-staging-20261119/bq_preserve/` (14-day bucket
auto-delete lifecycle), and downloads them permanently into:
  `/home/sk/dev/theyownwhat/data/opencorporates/bq_preserved_20261117/`
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT_DIR = os.path.join(REPO_ROOT, "data", "opencorporates", "bq_preserved_20261117")
GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine"
PROJECT = "renter-help"
CONFIG = "storrsmagic"
BUCKET = "renter-help-staging-20261119"
SCRATCH_DS = "oc_preserve_tmp"
SRC_DS = "theyownwhat.opencorporates_20261117"

ACTIVE_20_STATES = [
    "us_ar", "us_ca", "us_co", "us_ct", "us_dc", "us_fl", "us_il", "us_ma",
    "us_md", "us_mi", "us_mn", "us_mt", "us_nc", "us_nj", "us_ny", "us_oh",
    "us_pa", "us_va", "us_vt", "us_wi",
]


def _get_token() -> str:
    cmd = [
        "docker", "run", "--rm",
        "-e", "CLOUDSDK_METRICS_REQUEST_ATTRIBUTION=agent-skills/1.0 (skill:gcloud)",
        "-v", f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
        GCLOUD_IMAGE,
        "gcloud", "auth", "print-access-token", f"--configuration={CONFIG}", "--quiet",
    ]
    return subprocess.check_output(cmd, text=True).strip()


def _bq_req(token: str, method: str, path: str, body: Dict[str, Any] | None = None) -> Dict[str, Any]:
    url = f"https://bigquery.googleapis.com/bigquery/v2/{path.lstrip('/')}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"BQ {method} {path} -> HTTP {exc.code}: {err_body[:600]}") from exc


def _ensure_scratch_dataset(token: str) -> None:
    # 10-day TTL so everything auto-expires well ahead of Dec. 12 credit expiry
    ten_days_ms = str(10 * 24 * 3600 * 1000)
    try:
        _bq_req(token, "GET", f"projects/{PROJECT}/datasets/{SCRATCH_DS}")
        print(f"✓ Scratch dataset {PROJECT}:{SCRATCH_DS} already exists.")
        return
    except RuntimeError:
        pass
    _bq_req(
        token,
        "POST",
        f"projects/{PROJECT}/datasets",
        {
            "datasetReference": {"projectId": PROJECT, "datasetId": SCRATCH_DS},
            "location": "US",
            "defaultTableExpirationMs": ten_days_ms,
            "description": "10-day auto-expiring scratch dataset for pre-expiry OpenCorporates archive extracts",
        },
    )
    print(f"✓ Created 10-day-TTL scratch dataset {PROJECT}:{SCRATCH_DS} (US).")


def _wait_bq_job(token: str, job_id: str, location: str = "US") -> Dict[str, Any]:
    while True:
        res = _bq_req(token, "GET", f"projects/{PROJECT}/jobs/{job_id}?location={location}")
        state = (res.get("status") or {}).get("state")
        if state == "DONE":
            err = (res.get("status") or {}).get("errorResult")
            if err:
                raise RuntimeError(f"BQ job {job_id} failed: {err}")
            return res
        time.sleep(2.5)


def _run_query_to_table(token: str, dest_table: str, sql: str) -> Dict[str, Any]:
    t0 = time.time()
    print(f"▶ [BQ Query] Building {PROJECT}.{SCRATCH_DS}.{dest_table} ...", flush=True)
    job = _bq_req(
        token,
        "POST",
        f"projects/{PROJECT}/jobs",
        {
            "jobReference": {"projectId": PROJECT, "location": "US"},
            "configuration": {
                "query": {
                    "query": sql,
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": PROJECT,
                        "datasetId": SCRATCH_DS,
                        "tableId": dest_table,
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        },
    )
    jid = job["jobReference"]["jobId"]
    done = _wait_bq_job(token, jid, "US")
    stats = (done.get("statistics") or {}).get("query") or {}
    gb_billed = int(stats.get("totalBytesBilled") or 0) / (1024**3)
    print(f"  ✓ Built {dest_table} in {time.time() - t0:.1f}s (billed {gb_billed:.2f} GB)", flush=True)
    return done


def _export_table_to_local(
    token: str,
    src_project: str,
    src_dataset: str,
    src_table: str,
    local_prefix: str,
    dest_format: str = "CSV",
) -> List[str]:
    ext = "jsonl.gz" if dest_format == "NEWLINE_DELIMITED_JSON" else "csv.gz"
    gcs_uri = f"gs://{BUCKET}/bq_preserve/{local_prefix}_*.{ext}"
    print(f"▶ [BQ Export] {src_project}.{src_dataset}.{src_table} -> {gcs_uri}", flush=True)
    extract_cfg: Dict[str, Any] = {
        "sourceTable": {
            "projectId": src_project,
            "datasetId": src_dataset,
            "tableId": src_table,
        },
        "destinationUris": [gcs_uri],
        "destinationFormat": dest_format,
        "compression": "GZIP",
    }
    if dest_format == "CSV":
        extract_cfg["printHeader"] = True
    job = _bq_req(
        token,
        "POST",
        f"projects/{PROJECT}/jobs",
        {
            "jobReference": {"projectId": PROJECT, "location": "US"},
            "configuration": {"extract": extract_cfg},
        },
    )
    _wait_bq_job(token, job["jobReference"]["jobId"], "US")

    os.makedirs(OUT_DIR, exist_ok=True)
    dl_cmd = [
        "docker", "run", "--rm",
        "-e", "CLOUDSDK_METRICS_REQUEST_ATTRIBUTION=agent-skills/1.0 (skill:gcloud)",
        "-v", f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
        "-v", f"{OUT_DIR}:/out",
        GCLOUD_IMAGE,
        "gcloud", "storage", "cp",
        f"gs://{BUCKET}/bq_preserve/{local_prefix}_*.{ext}",
        "/out/",
        f"--project={PROJECT}", f"--configuration={CONFIG}", "--quiet",
    ]
    subprocess.check_call(dl_cmd)
    saved = sorted(f for f in os.listdir(OUT_DIR) if f.startswith(f"{local_prefix}_") and f.endswith(f".{ext}"))
    total_mb = sum(os.path.getsize(os.path.join(OUT_DIR, f)) for f in saved) / (1024 * 1024)
    print(f"  ✓ Downloaded {len(saved)} shard(s) ({total_mb:.1f} MB) -> {OUT_DIR}/{local_prefix}_*.{ext}", flush=True)
    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description="Preserve & extract permanent cross-state OpenCorporates tables from BigQuery before Dec. 8-10 expiry.")
    parser.add_argument("--steps", nargs="+", default=["ra_blocklist", "unique_principals", "dispersed_hq_51", "cross_state_hq", "de_wy_nv_pierce"])
    args = parser.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    token = _get_token()
    _ensure_scratch_dataset(token)

    # 1. Preserve existing `derived_ct_ra_coarse_blocklist` (expires 2026-12-10)
    if "ra_blocklist" in args.steps:
        _export_table_to_local(token, "theyownwhat", "opencorporates_20261117", "derived_ct_ra_coarse_blocklist", "bq_ct_ra_coarse_blocklist")

    # 2. Preserve all 20 already-computed `derived_ct_us_*_unique_principals` in one unified JSONL.GZ export
    if "unique_principals" in args.steps:
        unions = "\nUNION ALL\n".join(
            f"SELECT * FROM `{SRC_DS}.derived_ct_{st}_unique_principals`"
            for st in ACTIVE_20_STATES
        )
        _run_query_to_table(token, "all_20state_unique_principals", unions)
        _export_table_to_local(
            token, PROJECT, SCRATCH_DS, "all_20state_unique_principals", "bq_20state_unique_principals",
            dest_format="NEWLINE_DELIMITED_JSON",
        )
    elif "unique_principals_export" in args.steps:
        _export_table_to_local(
            token, PROJECT, SCRATCH_DS, "all_20state_unique_principals", "bq_20state_unique_principals",
            dest_format="NEWLINE_DELIMITED_JSON",
        )

    # 3. 51-US-State Empirical Dispersed HQ / CMRA / Virtual-Office Blocklist
    if "dispersed_hq_51" in args.steps:
        sql_disp = f"""
        WITH addr_clean AS (
          SELECT
            jurisdiction_code,
            company_number,
            REGEXP_REPLACE(
              UPPER(REGEXP_REPLACE(TRIM(COALESCE(NULLIF(reg_in_full, ''), CONCAT(COALESCE(reg_street_address,''), ' ', COALESCE(reg_locality,''), ' ', COALESCE(reg_region,''), ' ', SUBSTR(COALESCE(reg_postal_code,''), 1, 5)))), r'[,.#]+', ' ')),
              r'\\s+', ' '
            ) AS addr_norm
          FROM `{SRC_DS}.companies`
          WHERE STARTS_WITH(jurisdiction_code, 'us_')
        ),
        hq_non_reg AS (
          SELECT
            jurisdiction_code,
            company_number,
            REGEXP_REPLACE(
              UPPER(REGEXP_REPLACE(TRIM(COALESCE(NULLIF(in_full, ''), CONCAT(COALESCE(street_address,''), ' ', COALESCE(locality,''), ' ', COALESCE(region,''), ' ', SUBSTR(COALESCE(postal_code,''), 1, 5)))), r'[,.#]+', ' ')),
              r'\\s+', ' '
            ) AS addr_norm
          FROM `{SRC_DS}.non_reg_addresses`
          WHERE STARTS_WITH(jurisdiction_code, 'us_')
        ),
        all_addrs AS (
          SELECT jurisdiction_code, company_number, addr_norm FROM addr_clean WHERE LENGTH(addr_norm) >= 12
          UNION DISTINCT
          SELECT jurisdiction_code, company_number, addr_norm FROM hq_non_reg WHERE LENGTH(addr_norm) >= 12
        ),
        clean_off AS (
          SELECT
            jurisdiction_code,
            company_number,
            UPPER(TRIM(name)) AS off_name
          FROM `{SRC_DS}.officers`
          WHERE STARTS_WITH(jurisdiction_code, 'us_')
            AND COALESCE(end_date, '') = ''
            AND TRIM(COALESCE(name, '')) != ''
            AND NOT REGEXP_CONTAINS(LOWER(COALESCE(position, '')), r'\\b(agent|incorporator|organizer|process)\\b')
        ),
        by_addr_off AS (
          SELECT
            a.addr_norm,
            COUNT(DISTINCT CONCAT(a.jurisdiction_code, '/', a.company_number)) AS total_companies,
            COUNT(DISTINCT a.jurisdiction_code) AS distinct_states,
            COUNT(DISTINCT o.off_name) AS distinct_officers
          FROM all_addrs a
          LEFT JOIN clean_off o
            ON o.jurisdiction_code = a.jurisdiction_code AND o.company_number = a.company_number
          GROUP BY 1
          HAVING total_companies >= 25
        )
        SELECT *
        FROM by_addr_off
        WHERE distinct_officers >= 15 OR total_companies >= 75
        ORDER BY total_companies DESC
        """
        _run_query_to_table(token, "us_51_dispersed_hq_blocklist", sql_disp)
        _export_table_to_local(token, PROJECT, SCRATCH_DS, "us_51_dispersed_hq_blocklist", "bq_51state_dispersed_hq_blocklist")

    # 4. Cross-State HQ & Non-RA Operating Footprint Bridges across ALL 51 US States
    if "cross_state_hq" in args.steps:
        sql_cross = f"""
        WITH ra_hubs AS (
          SELECT DISTINCT addr_norm FROM `{SRC_DS}.derived_national_ra_hubs`
          UNION DISTINCT
          SELECT DISTINCT addr_norm FROM `{PROJECT}.{SCRATCH_DS}.us_51_dispersed_hq_blocklist`
        ),
        co_hq AS (
          SELECT
            c.jurisdiction_code,
            c.company_number,
            TRIM(c.name) AS company_name,
            COALESCE(c.company_type, '') AS company_type,
            REGEXP_REPLACE(
              UPPER(REGEXP_REPLACE(TRIM(COALESCE(NULLIF(n.in_full, ''), NULLIF(c.reg_in_full, ''))), r'[,.#]+', ' ')),
              r'\\s+', ' '
            ) AS hq_addr_norm
          FROM `{SRC_DS}.companies` c
          LEFT JOIN `{SRC_DS}.non_reg_addresses` n
            ON n.jurisdiction_code = c.jurisdiction_code AND n.company_number = c.company_number
          WHERE STARTS_WITH(c.jurisdiction_code, 'us_')
            AND NOT REGEXP_CONTAINS(LOWER(COALESCE(c.company_type, '')), r'(non[- ]?profit|not[- ]?for[- ]?profit|non[- ]?stock|charit|relig|church|co-?op|condo)')
        ),
        clean_hq AS (
          SELECT DISTINCT jurisdiction_code, company_number, company_name, hq_addr_norm
          FROM co_hq
          WHERE LENGTH(hq_addr_norm) >= 14
            AND REGEXP_CONTAINS(hq_addr_norm, r'^\\d+\\s+')
            AND hq_addr_norm NOT IN (SELECT addr_norm FROM ra_hubs)
        ),
        multi_state_hqs AS (
          SELECT
            hq_addr_norm,
            COUNT(DISTINCT jurisdiction_code) AS state_count,
            COUNT(DISTINCT CONCAT(jurisdiction_code, '/', company_number)) AS company_count
          FROM clean_hq
          GROUP BY 1
          HAVING state_count >= 2 AND company_count BETWEEN 2 AND 15
        )
        SELECT
          m.hq_addr_norm,
          m.state_count,
          m.company_count,
          c.jurisdiction_code,
          c.company_number,
          c.company_name
        FROM multi_state_hqs m
        JOIN clean_hq c ON c.hq_addr_norm = m.hq_addr_norm
        """
        _run_query_to_table(token, "us_51_cross_state_hq_bridges", sql_cross)
        _export_table_to_local(token, PROJECT, SCRATCH_DS, "us_51_cross_state_hq_bridges", "bq_51state_cross_state_hq_bridges")

    # 5. Deep Delaware / Wyoming / Nevada Holding-LLC Crosswalk (matching DE/WY/NV entities
    #    to operating-state foreign registrations by exact normalized name + home_jurisdiction)
    if "de_wy_nv_pierce" in args.steps:
        sql_haven = f"""
        WITH haven_cos AS (
          SELECT
            jurisdiction_code AS haven_jur,
            company_number AS haven_num,
            TRIM(name) AS haven_name,
            REGEXP_REPLACE(UPPER(REGEXP_REPLACE(TRIM(name), r'[^A-Z0-9 ]+', ' ')), r'\\s+', ' ') AS name_key
          FROM `{SRC_DS}.companies`
          WHERE jurisdiction_code IN ('us_de', 'us_wy', 'us_nv')
            AND TRIM(COALESCE(name, '')) != ''
            AND NOT REGEXP_CONTAINS(LOWER(COALESCE(company_type, '')), r'(non[- ]?profit|not[- ]?for[- ]?profit|non[- ]?stock|charit|relig)')
        ),
        op_foreign AS (
          SELECT
            c.jurisdiction_code AS op_jur,
            c.company_number AS op_num,
            TRIM(c.name) AS op_name,
            LOWER(TRIM(COALESCE(c.home_jurisdiction_code, ''))) AS home_jur,
            TRIM(COALESCE(c.home_jurisdiction_company_number, '')) AS home_num,
            REGEXP_REPLACE(UPPER(REGEXP_REPLACE(TRIM(c.name), r'[^A-Z0-9 ]+', ' ')), r'\\s+', ' ') AS name_key
          FROM `{SRC_DS}.companies` c
          WHERE c.jurisdiction_code IN ({','.join("'" + s + "'" for s in ACTIVE_20_STATES)})
            AND LOWER(TRIM(COALESCE(c.home_jurisdiction_code, ''))) IN ('us_de', 'us_wy', 'us_nv')
        ),
        op_officers AS (
          SELECT
            jurisdiction_code,
            company_number,
            ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TRIM(name) LIMIT 12), ' | ') AS op_principals
          FROM `{SRC_DS}.officers`
          WHERE jurisdiction_code IN ({','.join("'" + s + "'" for s in ACTIVE_20_STATES)})
            AND COALESCE(end_date, '') = ''
            AND TRIM(COALESCE(name, '')) != ''
            AND NOT REGEXP_CONTAINS(LOWER(COALESCE(position, '')), r'\\b(agent|incorporator|organizer|process)\\b')
            AND NOT REGEXP_CONTAINS(UPPER(name), r'\\b(CORPORATION SERVICE|CT CORPORATION|C T CORPORATION|LEGALZOOM|COGENCY|REGISTERED AGENT|INCORP SERVICES|PARACORP)\\b')
          GROUP BY 1, 2
        )
        SELECT
          f.op_jur,
          f.op_num,
          f.op_name,
          f.home_jur AS haven_jur,
          COALESCE(NULLIF(f.home_num, ''), h.haven_num) AS haven_num,
          o.op_principals
        FROM op_foreign f
        LEFT JOIN haven_cos h
          ON h.haven_jur = f.home_jur AND h.name_key = f.name_key
        LEFT JOIN op_officers o
          ON o.jurisdiction_code = f.op_jur AND o.company_number = f.op_num
        WHERE o.op_principals IS NOT NULL OR h.haven_num IS NOT NULL
        """
        _run_query_to_table(token, "de_wy_nv_holding_pierce", sql_haven)
        _export_table_to_local(token, PROJECT, SCRATCH_DS, "de_wy_nv_holding_pierce", "bq_de_wy_nv_holding_pierce")

    print("✅ All BigQuery preservation & cross-state exports downloaded permanently to:", OUT_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
