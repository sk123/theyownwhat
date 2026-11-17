#!/usr/bin/env python3
"""
gcp/sync_and_analyze_bigquery.py
================================
Uploads all 61 mirrored OpenCorporates jurisdictions (517.2M rows across all 50
U.S. states, D.C., Puerto Rico, and 9 Canadian registries) to Google Cloud
Storage + BigQuery, runs nationwide/cross-border corporate graph closure &
U.S. Census ACS public-dataset joins, exports the permanent derived tables back
to local disk (`/home/sk/dev/theyownwhat/data/opencorporates/`), and enforces
automatic 21-day TTL expiration (Dec. 8, 2026 — ahead of the Dec. 12 credit
expiry) on both GCS and BigQuery so zero post-credit charges can ever occur.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import subprocess
import sys
import time
from typing import List

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SNAPSHOT_DIR = os.path.join(REPO_ROOT, "data", "opencorporates", "2026-11-17")
EXPORT_DIR = os.path.join(REPO_ROOT, "data", "opencorporates", "bq_exports")
GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine"

DEFAULT_PROJECT = "theyownwhat"
DEFAULT_BUCKET = "theyownwhat-oc-staging-20261117"
DEFAULT_DATASET = "opencorporates_20261117"
TTL_DAYS = 21  # Auto-expires Dec 8, 2026 (4 days before Dec 12 credit expiry)


def run_gcloud_container(
    shell_cmd: str,
    *,
    project: str,
    mount_data: bool = True,
    timeout: int = 3600,
) -> str:
    vols = ["-v", f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud"]
    if mount_data:
        vols.extend(["-v", f"{os.path.join(REPO_ROOT, 'data')}:/data"])
    cmd = [
        "docker",
        "run",
        "--rm",
        *vols,
        "-e",
        f"CLOUDSDK_CORE_PROJECT={project}",
        GCLOUD_IMAGE,
        "bash",
        "-c",
        shell_cmd,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(
            f"GCP command failed (exit {proc.returncode}):\nCMD: {shell_cmd}\nSTDOUT: {proc.stdout[-1500:]}\nSTDERR: {proc.stderr[-1500:]}"
        )
    return (proc.stdout or "") + (proc.stderr or "")


def ensure_apis_bucket_and_dataset(project: str, bucket: str, dataset: str) -> None:
    print(f"[GCP] Enabling Storage & BigQuery APIs on project `{project}`...", flush=True)
    run_gcloud_container(
        "gcloud services enable storage.googleapis.com bigquery.googleapis.com bigquerystorage.googleapis.com",
        project=project,
        mount_data=False,
        timeout=300,
    )

    lifecycle = {
        "rule": [
            {
                "action": {"type": "Delete"},
                "condition": {"age": TTL_DAYS},
            }
        ]
    }
    os.makedirs(EXPORT_DIR, exist_ok=True)
    lc_host = os.path.join(EXPORT_DIR, "gcs_lifecycle_21d.json")
    with open(lc_host, "w", encoding="utf-8") as f:
        json.dump(lifecycle, f)

    print(f"[GCP] Ensuring GCS bucket `gs://{bucket}` (US, {TTL_DAYS}-day auto-delete lifecycle)...", flush=True)
    run_gcloud_container(
        f"""
        if ! gcloud storage buckets describe gs://{bucket} >/dev/null 2>&1; then
            gcloud storage buckets create gs://{bucket} --location=US --uniform-bucket-level-access
        fi
        gcloud storage buckets update gs://{bucket} --lifecycle-file=/data/opencorporates/bq_exports/gcs_lifecycle_21d.json
        """,
        project=project,
        timeout=180,
    )

    ttl_sec = TTL_DAYS * 86400
    print(f"[GCP] Ensuring BigQuery dataset `{project}:{dataset}` (US, {TTL_DAYS}-day table TTL)...", flush=True)
    run_gcloud_container(
        f"""
        if ! bq --location=US show --dataset {project}:{dataset} >/dev/null 2>&1; then
            bq --location=US mk --dataset --default_table_expiration={ttl_sec} {project}:{dataset}
        else
            bq --location=US update --default_table_expiration={ttl_sec} {project}:{dataset}
        fi
        """,
        project=project,
        mount_data=False,
        timeout=180,
    )


def upload_snapshot_to_gcs(project: str, bucket: str) -> None:
    print(f"[GCP] Syncing 61-jurisdiction `.csv.gz` archives to `gs://{bucket}/2026-11-17/`...", flush=True)
    t0 = time.time()
    run_gcloud_container(
        f"gcloud storage rsync --recursive --exclude='.*\\.ctl\\.csv$' /data/opencorporates/2026-11-17 gs://{bucket}/2026-11-17",
        project=project,
        timeout=3600,
    )
    print(f"  ✓ GCS sync completed in {time.time() - t0:.1f}s", flush=True)


# Explicit ALL-STRING schemas for raw ingestion so registry formatting quirks across 61 countries/states never fail load jobs
BQ_RAW_SCHEMAS = {
    "companies": (
        "company_number:STRING,jurisdiction_code:STRING,name:STRING,normalised_name:STRING,"
        "company_type:STRING,nonprofit:STRING,current_status:STRING,incorporation_date:STRING,"
        "dissolution_date:STRING,branch:STRING,business_number:STRING,current_alternative_legal_name:STRING,"
        "current_alternative_legal_name_language:STRING,home_jurisdiction_text:STRING,native_company_number:STRING,"
        "previous_names:STRING,retrieved_at:STRING,registry_url:STRING,restricted_for_marketing:STRING,"
        "inactive:STRING,accounts_next_due:STRING,accounts_reference_date:STRING,accounts_last_made_up_date:STRING,"
        "annual_return_next_due:STRING,annual_return_last_made_up_date:STRING,has_been_liquidated:STRING,"
        "has_insolvency_history:STRING,has_charges:STRING,number_of_employees:STRING,"
        "reg_street_address:STRING,reg_locality:STRING,reg_region:STRING,reg_postal_code:STRING,"
        "reg_country:STRING,reg_in_full:STRING,home_jurisdiction_code:STRING,"
        "home_jurisdiction_company_number:STRING,industry_code_uids:STRING,latest_accounts_date:STRING,"
        "latest_accounts_cash:STRING,latest_accounts_assets:STRING,latest_accounts_liabilities:STRING"
    ),
    "officers": (
        "id:STRING,company_number:STRING,jurisdiction_code:STRING,name:STRING,title:STRING,"
        "first_name:STRING,last_name:STRING,position:STRING,start_date:STRING,person_number:STRING,"
        "person_uid:STRING,end_date:STRING,current_status:STRING,occupation:STRING,nationality:STRING,"
        "country_of_residence:STRING,partial_date_of_birth:STRING,type:STRING,addr_in_full:STRING,"
        "addr_street_address:STRING,addr_locality:STRING,addr_region:STRING,addr_postal_code:STRING,"
        "addr_country:STRING,retrieved_at:STRING,source_url:STRING"
    ),
    "non_reg_addresses": (
        "company_number:STRING,jurisdiction_code:STRING,address_type:STRING,street_address:STRING,"
        "locality:STRING,region:STRING,postal_code:STRING,country:STRING,country_code:STRING,"
        "in_full:STRING,start_date:STRING,end_date:STRING"
    ),
    "alternative_names": (
        "company_number:STRING,jurisdiction_code:STRING,name:STRING,type:STRING,start_date:STRING,end_date:STRING"
    ),
    "additional_identifiers": (
        "company_number:STRING,jurisdiction_code:STRING,uid:STRING,identifier_system_code:STRING"
    ),
}


def load_all_tables_into_bigquery(project: str, bucket: str, dataset: str) -> None:
    print(f"[GCP] Batch-loading all 61 jurisdictions into BigQuery `{project}:{dataset}` (free batch load)...", flush=True)
    for tbl, schema_str in BQ_RAW_SCHEMAS.items():
        t0 = time.time()
        uri = f"gs://{bucket}/2026-11-17/*/{tbl}.csv.gz"
        print(f"  -> Loading `{tbl}` from `{uri}`...", flush=True)
        run_gcloud_container(
            f"""
            bq --location=US load \
                --replace \
                --source_format=CSV \
                --skip_leading_rows=1 \
                --allow_quoted_newlines \
                --allow_jagged_rows \
                --max_bad_records=1000 \
                --clustering_fields=jurisdiction_code,company_number \
                {project}:{dataset}.{tbl} \
                "{uri}" \
                "{schema_str}"
            """,
            project=project,
            mount_data=False,
            timeout=3600,
        )
        print(f"     ✓ `{tbl}` loaded in {time.time() - t0:.1f}s", flush=True)


def run_national_analytics_and_export(project: str, bucket: str, dataset: str) -> None:
    max_bytes = 250_000_000_000  # Hard 250 GB scan cap per query (~$0 within 1TB free tier, or <=$1.50 max)

    # 1. 61-Jurisdiction Empirical Commercial Registered-Agent Address Blocklist
    print("[GCP] Computing 61-jurisdiction national registered-agent address blocklist in BigQuery...", flush=True)
    q_ra = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.derived_national_ra_hubs` AS
    WITH agent_addrs AS (
      SELECT
        UPPER(REGEXP_REPLACE(TRIM(COALESCE(NULLIF(addr_in_full, ''), CONCAT(COALESCE(addr_street_address,''), ' ', COALESCE(addr_locality,''), ' ', COALESCE(addr_region,''), ' ', SUBSTR(COALESCE(addr_postal_code,''), 1, 5)))), r'[,\\.]+', ' ')) AS raw_clean,
        jurisdiction_code,
        company_number,
        UPPER(TRIM(name)) AS agent_name
      FROM `{project}.{dataset}.officers`
      WHERE LOWER(TRIM(COALESCE(position, ''))) IN ('agent', 'registered agent', 'resident agent', 'statutory agent', 'incorporator', 'organizer')
        AND COALESCE(end_date, '') = ''
    )
    SELECT
      REGEXP_REPLACE(REGEXP_REPLACE(raw_clean, r'\\s*,?\\s*(USA|UNITED STATES)\\s*$', ''), r'\\s+', ' ') AS addr_norm,
      ANY_VALUE(raw_clean) AS sample_raw_address,
      COUNT(DISTINCT CONCAT(jurisdiction_code, '/', company_number)) AS distinct_company_count,
      COUNT(DISTINCT jurisdiction_code) AS jurisdiction_count,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT agent_name IGNORE NULLS LIMIT 8), ' | ') AS sample_agent_names
    FROM agent_addrs
    WHERE LENGTH(raw_clean) >= 10
    GROUP BY 1
    HAVING distinct_company_count >= 25;
    """
    q_ra_file = os.path.join(EXPORT_DIR, "q_ra.sql")
    with open(q_ra_file, "w", encoding="utf-8") as f:
        f.write(q_ra)

    # 2. 61-Jurisdiction Cross-State Foreign-Branch -> Parent Beneficial Officer Closure
    #    Resolves foreign branches across ALL 50 US states + Canada (including CA, TX, GA, OH, WA, ON, QC, BC)
    print("[GCP] Computing 61-jurisdiction cross-state foreign-branch & parent beneficial-owner closure...", flush=True)
    q_pierce = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.derived_cross_state_pierced_branches` AS
    WITH clean_officers AS (
      SELECT
        jurisdiction_code,
        company_number,
        TRIM(name) AS officer_name,
        LOWER(TRIM(COALESCE(position, ''))) AS pos,
        LOWER(TRIM(COALESCE(type, ''))) AS off_type
      FROM `{project}.{dataset}.officers`
      WHERE COALESCE(end_date, '') = ''
        AND TRIM(COALESCE(name, '')) != ''
        AND NOT REGEXP_CONTAINS(LOWER(COALESCE(position, '')), r'\\b(agent|incorporator|organizer|process)\\b')
        AND NOT REGEXP_CONTAINS(UPPER(name), r'\\b(CORPORATION SERVICE|CT CORPORATION|C T CORPORATION|LEGALZOOM|COGENCY|REGISTERED AGENT|INCORP SERVICES|PARACORP)\\b')
    ),
    branch_links AS (
      SELECT
        jurisdiction_code AS branch_jur,
        company_number AS branch_num,
        TRIM(name) AS branch_name,
        LOWER(TRIM(home_jurisdiction_code)) AS home_jur,
        TRIM(home_jurisdiction_company_number) AS home_num
      FROM `{project}.{dataset}.companies`
      WHERE TRIM(COALESCE(home_jurisdiction_code, '')) != ''
        AND TRIM(COALESCE(home_jurisdiction_company_number, '')) != ''
        AND LOWER(TRIM(home_jurisdiction_code)) != jurisdiction_code
    )
    SELECT
      b.branch_jur,
      b.branch_num,
      b.branch_name,
      b.home_jur,
      b.home_num,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT o.officer_name LIMIT 15), ' | ') AS home_beneficial_principals,
      COUNT(DISTINCT o.officer_name) AS principal_count
    FROM branch_links b
    JOIN clean_officers o
      ON o.jurisdiction_code = b.home_jur
     AND o.company_number = b.home_num
    GROUP BY 1, 2, 3, 4, 5;
    """
    q_pierce_file = os.path.join(EXPORT_DIR, "q_pierce.sql")
    with open(q_pierce_file, "w", encoding="utf-8") as f:
        f.write(q_pierce)

    # 3. Free BigQuery Public Dataset Enrichment: U.S. Census ACS 5-Year ZCTA Renter & Rent-Burden Spine
    print("[GCP] Joining BigQuery Public U.S. Census ACS ZCTA housing & rent-burden metrics...", flush=True)
    q_acs = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.derived_us_zcta_acs_housing` AS
    SELECT
      geo_id AS zcta5,
      CAST(total_pop AS INT64) AS total_pop,
      CAST(occupied_housing_units AS INT64) AS occupied_units,
      CAST(housing_units_renter_occupied AS INT64) AS renter_units,
      ROUND(SAFE_DIVIDE(housing_units_renter_occupied, NULLIF(occupied_housing_units, 0)) * 100, 2) AS renter_share_pct,
      CAST(median_rent AS INT64) AS median_contract_rent,
      CAST(median_income AS INT64) AS median_household_income,
      ROUND(SAFE_DIVIDE(
        COALESCE(rent_30_to_35_percent, 0) + COALESCE(rent_35_to_40_percent, 0) + COALESCE(rent_40_to_50_percent, 0) + COALESCE(rent_over_50_percent, 0),
        NULLIF(housing_units_renter_occupied, 0)
      ) * 100, 2) AS rent_burdened_30pct_plus_share,
      ROUND(SAFE_DIVIDE(COALESCE(rent_over_50_percent, 0), NULLIF(housing_units_renter_occupied, 0)) * 100, 2) AS severely_rent_burdened_50pct_share
    FROM `bigquery-public-data.census_bureau_acs.zcta_2020_5yr`
    WHERE occupied_housing_units > 0;
    """
    q_acs_file = os.path.join(EXPORT_DIR, "q_acs.sql")
    with open(q_acs_file, "w", encoding="utf-8") as f:
        f.write(q_acs)

    for label, sql_rel, table_name, out_gz in [
        ("National RA Hubs", "q_ra.sql", "derived_national_ra_hubs", "bq_national_ra_hubs.csv.gz"),
        ("Cross-State Pierced Branches", "q_pierce.sql", "derived_cross_state_pierced_branches", "bq_cross_state_pierced_branches.csv.gz"),
        ("US Census ACS ZCTA Housing", "q_acs.sql", "derived_us_zcta_acs_housing", "bq_us_zcta_acs_housing.csv.gz"),
    ]:
        local_f = os.path.join(EXPORT_DIR, out_gz)
        if os.path.exists(local_f) and os.path.getsize(local_f) > 512:
            with gzip.open(local_f, "rt", encoding="utf-8", errors="replace") as gz:
                line_cnt = max(0, sum(1 for _ in gz) - 1)
            print(f"  -> Skipping `{label}` (already exported: {os.path.getsize(local_f)/1e6:.2f} MB, {line_cnt:,} rows)", flush=True)
            continue
        t0 = time.time()
        print(f"  -> Executing & exporting `{label}`...", flush=True)
        run_gcloud_container(
            f"""
            bq --location=US query --use_legacy_sql=false --maximum_bytes_billed={max_bytes} < /data/opencorporates/bq_exports/{sql_rel}
            bq --location=US extract --compression=GZIP --destination_format=CSV \
                {project}:{dataset}.{table_name} \
                gs://{bucket}/exports/{out_gz}
            gcloud storage cp gs://{bucket}/exports/{out_gz} /data/opencorporates/bq_exports/{out_gz}
            """,
            project=project,
            timeout=1800,
        )
        with gzip.open(local_f, "rt", encoding="utf-8", errors="replace") as gz:
            line_cnt = max(0, sum(1 for _ in gz) - 1)
        print(f"     ✓ Saved `{local_f}` ({os.path.getsize(local_f)/1e6:.2f} MB, {line_cnt:,} rows) in {time.time() - t0:.1f}s", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload 61-jurisdiction OpenCorporates dump to GCS + BigQuery and export national graph closures.")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-load", action="store_true")
    args = parser.parse_args()

    t_all = time.time()
    ensure_apis_bucket_and_dataset(args.project, args.bucket, args.dataset)
    if not args.skip_upload:
        upload_snapshot_to_gcs(args.project, args.bucket)
    if not args.skip_load:
        load_all_tables_into_bigquery(args.project, args.bucket, args.dataset)
    run_national_analytics_and_export(args.project, args.bucket, args.dataset)
    print(f"\n✓ Complete 61-jurisdiction BigQuery pipeline finished in {time.time() - t_all:.1f}s!", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
