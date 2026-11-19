#!/usr/bin/env python3
"""
gcp/sync_bq_ct_to_local.py
==========================
Bridges BigQuery's full-registry CT-Treatment tables (`derived_ct_{jur}_*`) back
into local PostgreSQL (`oc_ct_principal_links`, `oc_ct_company_edges`,
`oc_ct_dispersed_hq_blocklist`) strictly scoped to each jurisdiction's
parcel-linked corporate subgraph (4-hop closure computed inside BigQuery).

Why this architecture works cleanly at nationwide scale:
  - BigQuery computes statewide empirical homonym dispersion, self-agent-safe RA
    quarantine, dispersed-intermediary HQ blocking, and corporate-parent/principal
    edges across ALL 9M+ companies & 32M+ officers per state in seconds.
  - Only the parcel-relevant 4-hop reachable subgraph (typically 10K-300K rows
    instead of 30M+ rows) is exported back to local PostgreSQL, keeping local
    SSD & RAM lean while giving every state full CT-grade corporate untangling.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import os
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Set, Tuple

import psycopg2

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from gcp.bq_ct_treatment import (
    DEFAULT_BUCKET,
    DEFAULT_DATASET,
    DEFAULT_PROJECT,
    EXPORT_DIR,
    GCLOUD_IMAGE,
    JURISDICTION_PROFILES,
    run_bq_sql,
)

csv.field_size_limit(10_000_000)

# Map OC jurisdiction -> local Postgres property tables that hold parcel-linked OC seeds
OC_TO_LOCAL_PROP_TABLES: Dict[str, List[Tuple[str, str]]] = {
    "us_ca": [("ca_properties", "ca")],
    "us_fl": [("fl_properties", "fl"), ("miami_properties", "miami")],
    "us_ma": [("ma_properties", "ma")],
    "us_ny": [("ny_properties", "ny"), ("nyc_properties", "nyc")],
    "us_il": [("il_properties", "il")],
    "us_dc": [("dc_properties", "dc")],
    "us_md": [("baltimore_properties", "baltimore")],
    "us_mi": [("detroit_properties", "detroit")],
    "us_pa": [("philadelphia_properties", "philadelphia")],
    "us_nj": [("nj_properties", "nj")],
    "us_co": [("co_properties", "co")],
    "us_vt": [("vt_properties", "vt")],
    "us_wi": [("wi_properties", "wi")],
    "us_nc": [("nc_properties", "nc")],
    "us_ar": [("ar_properties", "ar")],
    "us_va": [("va_properties", "va")],
    "us_mt": [("mt_properties", "mt")],
    "us_mn": [("mn_properties", "mn")],
    "us_oh": [("oh_properties", "oh")],
}


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def ensure_local_ct_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS oc_ct_principal_links (
                jurisdiction_code       TEXT NOT NULL,
                company_number          TEXT NOT NULL,
                canonical_principal_id  TEXT NOT NULL,
                canonical_name          TEXT NOT NULL,
                pair_key                TEXT,
                mid_init                TEXT,
                is_homonymous_guarded   BOOLEAN DEFAULT FALSE,
                corroboration_mode      TEXT,
                position                TEXT,
                addr_coarse             TEXT,
                statewide_company_count INTEGER DEFAULT 1,
                name_variants           TEXT[],
                updated_at              TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (jurisdiction_code, company_number, canonical_principal_id)
            );
            CREATE INDEX IF NOT EXISTS idx_oc_ct_pl_company
                ON oc_ct_principal_links (jurisdiction_code, company_number);
            CREATE INDEX IF NOT EXISTS idx_oc_ct_pl_pid
                ON oc_ct_principal_links (canonical_principal_id);

            CREATE TABLE IF NOT EXISTS oc_ct_company_edges (
                jurisdiction_code   TEXT NOT NULL,
                src_company_number  TEXT NOT NULL,
                dst_company_number  TEXT NOT NULL,
                edge_type           TEXT NOT NULL,
                bridge_key          TEXT,
                updated_at          TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (jurisdiction_code, src_company_number, dst_company_number, edge_type)
            );
            CREATE INDEX IF NOT EXISTS idx_oc_ct_edges_src
                ON oc_ct_company_edges (jurisdiction_code, src_company_number);
            CREATE INDEX IF NOT EXISTS idx_oc_ct_edges_dst
                ON oc_ct_company_edges (jurisdiction_code, dst_company_number);

            CREATE TABLE IF NOT EXISTS oc_ct_dispersed_hq_blocklist (
                jurisdiction_code    TEXT NOT NULL,
                addr_coarse          TEXT NOT NULL,
                total_companies      INTEGER DEFAULT 0,
                distinct_principals  INTEGER DEFAULT 0,
                top_principal_share  NUMERIC(6,4),
                quarantine_reason    TEXT,
                updated_at           TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (jurisdiction_code, addr_coarse)
            );
            """
        )
    conn.commit()


def collect_local_seed_cnums(conn, oc_jur: str) -> List[str]:
    tables = OC_TO_LOCAL_PROP_TABLES.get(oc_jur, [])
    seeds: Set[str] = set()
    with conn.cursor() as cur:
        for tbl, _app_j in tables:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{tbl}",))
            if not cur.fetchone()[0]:
                continue
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s AND column_name = 'oc_company_number'
                )
                """,
                (tbl,),
            )
            if not cur.fetchone()[0]:
                continue
            cur.execute(
                f"""
                SELECT DISTINCT oc_company_number
                FROM {tbl}
                WHERE oc_jurisdiction_code = %s
                  AND oc_company_number IS NOT NULL
                  AND oc_company_number != ''
                """,
                (oc_jur,),
            )
            for (cnum,) in cur.fetchall():
                seeds.add(str(cnum).strip())
        # Also include CT owning businesses matched to us_ct when syncing us_ct
        if oc_jur == "us_ct":
            cur.execute(
                """
                SELECT DISTINCT company_number
                FROM oc_companies
                WHERE jurisdiction_code = 'us_ct' AND COALESCE(is_active, TRUE) = TRUE
                LIMIT 250000
                """
            )
            for (cnum,) in cur.fetchall():
                seeds.add(str(cnum).strip())
    return sorted(seeds)


def _docker_bq_gcloud(shell_cmd: str, *, project: str, timeout: int = 900) -> str:
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
        "-v", f"{os.path.join(REPO_ROOT, 'data')}:/data",
        "-e", f"CLOUDSDK_CORE_PROJECT={project}",
        GCLOUD_IMAGE,
        "bash", "-c", shell_cmd,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed (exit {proc.returncode}):\n{proc.stdout[-1500:]}\n{proc.stderr[-1500:]}")
    return proc.stdout


def extract_bq_table_to_local_gz(
    *,
    project: str,
    dataset: str,
    bucket: str,
    table_name: str,
    local_dir: str,
) -> List[str]:
    os.makedirs(local_dir, exist_ok=True)
    gcs_prefix = f"gs://{bucket}/bq_ct_exports/{table_name}"
    # Clear old shards in GCS & local dir
    for f in os.listdir(local_dir):
        if f.startswith(f"{table_name}_") and f.endswith(".csv.gz"):
            os.unlink(os.path.join(local_dir, f))
    rel_dir = os.path.relpath(local_dir, os.path.join(REPO_ROOT, "data"))
    _docker_bq_gcloud(
        f"""
        gcloud storage rm "{gcs_prefix}_*.csv.gz" --quiet 2>/dev/null || true
        bq --project_id={project} extract --compression=GZIP --destination_format=CSV \
            {project}:{dataset}.{table_name} "{gcs_prefix}_*.csv.gz"
        gcloud storage cp "{gcs_prefix}_*.csv.gz" "/data/{rel_dir}/" --quiet
        """,
        project=project,
    )
    return sorted(
        os.path.join(local_dir, f)
        for f in os.listdir(local_dir)
        if f.startswith(f"{table_name}_") and f.endswith(".csv.gz")
    )


def sync_jurisdiction(
    conn,
    oc_jur: str,
    *,
    project: str,
    dataset: str,
    bucket: str,
    max_hops: int = 4,
) -> Dict[str, int]:
    t0 = time.time()
    seeds = collect_local_seed_cnums(conn, oc_jur)
    print(f"\n=== [SYNC-BQ-CT] {oc_jur.upper()}: {len(seeds):,} local parcel-linked OC seed companies ===", flush=True)
    if not seeds:
        print(f"  No local OC seeds found for {oc_jur}; skipping.", flush=True)
        return {"seeds": 0, "principals": 0, "edges": 0, "dispersed_hqs": 0}

    os.makedirs(EXPORT_DIR, exist_ok=True)
    seed_csv = os.path.join(EXPORT_DIR, f"_seed_{oc_jur}.csv")
    with open(seed_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["company_number"])
        for cnum in seeds:
            w.writerow([cnum])
    rel_seed = os.path.relpath(seed_csv, os.path.join(REPO_ROOT, "data"))

    # 1. Load seed CSV into BigQuery
    _docker_bq_gcloud(
        f"bq --project_id={project} load --replace --source_format=CSV --skip_leading_rows=1 "
        f"{project}:{dataset}._seed_{oc_jur} /data/{rel_seed} company_number:STRING",
        project=project,
    )

    # 2. Compute 4-hop parcel-relevant subgraph in BigQuery
    #    Each hop expands ONLY over clean `derived_ct_{oc_jur}_company_edges`, keeping
    #    exported tables small, fast, and 100% relevant to real-estate portfolios.
    hop_ctes = [
        f"h0 AS (SELECT DISTINCT company_number FROM `{project}.{dataset}._seed_{oc_jur}`)"
    ]
    for h in range(1, max_hops + 1):
        prev = f"h{h-1}"
        hop_ctes.append(
            f"""h{h} AS (
              SELECT company_number FROM {prev}
              UNION DISTINCT
              SELECT e.dst_company_number AS company_number
              FROM `{project}.{dataset}.derived_ct_{oc_jur}_company_edges` e
              JOIN {prev} p ON p.company_number = e.src_company_number
              UNION DISTINCT
              SELECT e.src_company_number AS company_number
              FROM `{project}.{dataset}.derived_ct_{oc_jur}_company_edges` e
              JOIN {prev} p ON p.company_number = e.dst_company_number
            )"""
        )

    # Rename src_cnum/dst_cnum -> src_company_number/dst_company_number cleanly
    subgraph_sql = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}._reach_{oc_jur}` AS
    WITH edges AS (
      SELECT src_cnum AS src_company_number, dst_cnum AS dst_company_number
      FROM `{project}.{dataset}.derived_ct_{oc_jur}_company_edges`
    ),
    h0 AS (SELECT DISTINCT company_number FROM `{project}.{dataset}._seed_{oc_jur}`),
    h1 AS (
      SELECT company_number FROM h0
      UNION DISTINCT SELECT e.dst_company_number FROM edges e JOIN h0 p ON p.company_number = e.src_company_number
      UNION DISTINCT SELECT e.src_company_number FROM edges e JOIN h0 p ON p.company_number = e.dst_company_number
    ),
    h2 AS (
      SELECT company_number FROM h1
      UNION DISTINCT SELECT e.dst_company_number FROM edges e JOIN h1 p ON p.company_number = e.src_company_number
      UNION DISTINCT SELECT e.src_company_number FROM edges e JOIN h1 p ON p.company_number = e.dst_company_number
    ),
    h3 AS (
      SELECT company_number FROM h2
      UNION DISTINCT SELECT e.dst_company_number FROM edges e JOIN h2 p ON p.company_number = e.src_company_number
      UNION DISTINCT SELECT e.src_company_number FROM edges e JOIN h2 p ON p.company_number = e.dst_company_number
    ),
    h4 AS (
      SELECT company_number FROM h3
      UNION DISTINCT SELECT e.dst_company_number FROM edges e JOIN h3 p ON p.company_number = e.src_company_number
      UNION DISTINCT SELECT e.src_company_number FROM edges e JOIN h3 p ON p.company_number = e.dst_company_number
    )
    SELECT DISTINCT company_number FROM h4;
    """
    run_bq_sql(subgraph_sql, project=project)

    # 3. Materialize compact export tables for this jurisdiction's reachable subgraph
    exp_pl_sql = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}._exp_pl_{oc_jur}` AS
    SELECT
      pl.jurisdiction_code,
      pl.company_number,
      pl.canonical_principal_id,
      pl.canonical_name,
      pl.pair_key,
      pl.mid_init,
      pl.is_homonymous_guarded,
      pl.corroboration_mode,
      COALESCE(pl.position, '') AS position,
      COALESCE(pl.addr_coarse, '') AS addr_coarse,
      COALESCE(up.distinct_companies, 1) AS statewide_company_count,
      ARRAY_TO_STRING(up.name_variants, ' | ') AS name_variants_pipe
    FROM `{project}.{dataset}.derived_ct_{oc_jur}_principal_links` pl
    JOIN `{project}.{dataset}._reach_{oc_jur}` r ON r.company_number = pl.company_number
    LEFT JOIN `{project}.{dataset}.derived_ct_{oc_jur}_unique_principals` up
      ON up.canonical_principal_id = pl.canonical_principal_id;
    """
    run_bq_sql(exp_pl_sql, project=project)

    exp_edges_sql = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}._exp_edges_{oc_jur}` AS
    SELECT
      e.jurisdiction_code,
      e.src_cnum AS src_company_number,
      e.dst_cnum AS dst_company_number,
      e.edge_type,
      COALESCE(e.bridge_key, '') AS bridge_key
    FROM `{project}.{dataset}.derived_ct_{oc_jur}_company_edges` e
    JOIN `{project}.{dataset}._reach_{oc_jur}` r1 ON r1.company_number = e.src_cnum
    JOIN `{project}.{dataset}._reach_{oc_jur}` r2 ON r2.company_number = e.dst_cnum;
    """
    run_bq_sql(exp_edges_sql, project=project)

    exp_disp_sql = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}._exp_disp_{oc_jur}` AS
    SELECT
      jurisdiction_code,
      addr_coarse,
      total_companies,
      distinct_principals,
      top_principal_share,
      quarantine_reason
    FROM `{project}.{dataset}.derived_ct_{oc_jur}_dispersed_hq_blocklist`;
    """
    run_bq_sql(exp_disp_sql, project=project)

    # 4. Extract to GCS -> local gzipped CSVs -> COPY into local Postgres
    pl_files = extract_bq_table_to_local_gz(
        project=project, dataset=dataset, bucket=bucket, table_name=f"_exp_pl_{oc_jur}", local_dir=EXPORT_DIR
    )
    edge_files = extract_bq_table_to_local_gz(
        project=project, dataset=dataset, bucket=bucket, table_name=f"_exp_edges_{oc_jur}", local_dir=EXPORT_DIR
    )
    disp_files = extract_bq_table_to_local_gz(
        project=project, dataset=dataset, bucket=bucket, table_name=f"_exp_disp_{oc_jur}", local_dir=EXPORT_DIR
    )

    counts = {"seeds": len(seeds), "principals": 0, "edges": 0, "dispersed_hqs": 0}
    with conn.cursor() as cur:
        cur.execute("DELETE FROM oc_ct_principal_links WHERE jurisdiction_code = %s", (oc_jur,))
        cur.execute("DELETE FROM oc_ct_company_edges WHERE jurisdiction_code = %s", (oc_jur,))
        cur.execute("DELETE FROM oc_ct_dispersed_hq_blocklist WHERE jurisdiction_code = %s", (oc_jur,))

        # Stream principal links
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        n_buf = 0
        def _flush_pl():
            nonlocal n_buf
            if not n_buf:
                return
            buf.seek(0)
            cur.copy_expert(
                """
                COPY oc_ct_principal_links (
                    jurisdiction_code, company_number, canonical_principal_id, canonical_name,
                    pair_key, mid_init, is_homonymous_guarded, corroboration_mode,
                    position, addr_coarse, statewide_company_count, name_variants
                ) FROM STDIN WITH (FORMAT csv)
                """,
                buf,
            )
            buf.seek(0)
            buf.truncate(0)
            n_buf = 0

        seen_pl = set()
        for fp in pl_files:
            with gzip.open(fp, "rt", encoding="utf-8", errors="replace", newline="") as gz:
                for r in csv.DictReader(gz):
                    k = (r["jurisdiction_code"], r["company_number"], r["canonical_principal_id"])
                    if k in seen_pl:
                        continue
                    seen_pl.add(k)
                    vars_list = [v.strip() for v in (r.get("name_variants_pipe") or "").split("|") if v.strip()]
                    pg_arr = "{" + ",".join('"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"' for v in vars_list) + "}"
                    w.writerow([
                        r["jurisdiction_code"],
                        r["company_number"],
                        r["canonical_principal_id"],
                        r["canonical_name"],
                        r.get("pair_key") or "",
                        r.get("mid_init") or "",
                        "t" if str(r.get("is_homonymous_guarded")).lower() == "true" else "f",
                        r.get("corroboration_mode") or "",
                        r.get("position") or "",
                        r.get("addr_coarse") or "",
                        int(float(r.get("statewide_company_count") or 1)),
                        pg_arr,
                    ])
                    n_buf += 1
                    counts["principals"] += 1
                    if n_buf >= 25000:
                        _flush_pl()
        _flush_pl()

        # Stream edges
        buf_e = io.StringIO()
        we = csv.writer(buf_e, lineterminator="\n")
        ne_buf = 0
        def _flush_e():
            nonlocal ne_buf
            if not ne_buf:
                return
            buf_e.seek(0)
            cur.copy_expert(
                """
                COPY oc_ct_company_edges (
                    jurisdiction_code, src_company_number, dst_company_number, edge_type, bridge_key
                ) FROM STDIN WITH (FORMAT csv)
                """,
                buf_e,
            )
            buf_e.seek(0)
            buf_e.truncate(0)
            ne_buf = 0

        seen_e = set()
        for fp in edge_files:
            with gzip.open(fp, "rt", encoding="utf-8", errors="replace", newline="") as gz:
                for r in csv.DictReader(gz):
                    k = (r["jurisdiction_code"], r["src_company_number"], r["dst_company_number"], r["edge_type"])
                    if k in seen_e:
                        continue
                    seen_e.add(k)
                    we.writerow([k[0], k[1], k[2], k[3], r.get("bridge_key") or ""])
                    ne_buf += 1
                    counts["edges"] += 1
                    if ne_buf >= 25000:
                        _flush_e()
        _flush_e()

        # Stream dispersed HQ blocklist
        buf_d = io.StringIO()
        wd = csv.writer(buf_d, lineterminator="\n")
        nd_buf = 0
        def _flush_d():
            nonlocal nd_buf
            if not nd_buf:
                return
            buf_d.seek(0)
            cur.copy_expert(
                """
                COPY oc_ct_dispersed_hq_blocklist (
                    jurisdiction_code, addr_coarse, total_companies, distinct_principals,
                    top_principal_share, quarantine_reason
                ) FROM STDIN WITH (FORMAT csv)
                """,
                buf_d,
            )
            buf_d.seek(0)
            buf_d.truncate(0)
            nd_buf = 0

        seen_d = set()
        for fp in disp_files:
            with gzip.open(fp, "rt", encoding="utf-8", errors="replace", newline="") as gz:
                for r in csv.DictReader(gz):
                    k = (r["jurisdiction_code"], r["addr_coarse"])
                    if k in seen_d or not r.get("addr_coarse"):
                        continue
                    seen_d.add(k)
                    wd.writerow([
                        k[0],
                        k[1],
                        int(float(r.get("total_companies") or 0)),
                        int(float(r.get("distinct_principals") or 0)),
                        r.get("top_principal_share") or 0,
                        r.get("quarantine_reason") or "",
                    ])
                    nd_buf += 1
                    counts["dispersed_hqs"] += 1
                    if nd_buf >= 25000:
                        _flush_d()
        _flush_d()

    conn.commit()
    print(
        f"  ✓ Synced {oc_jur.upper()} in {time.time() - t0:.1f}s: "
        f"{counts['principals']:,} deduped principal links, {counts['edges']:,} 4-hop edges, "
        f"{counts['dispersed_hqs']:,} dispersed HQ hubs blocked.",
        flush=True,
    )
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync BigQuery CT-treatment parcel-relevant subgraphs into local Postgres.")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--jurisdictions", nargs="+", default=["us_ca"])
    args = parser.parse_args()

    jurs = list(JURISDICTION_PROFILES.keys()) if "all" in args.jurisdictions else args.jurisdictions
    conn = psycopg2.connect(_resolve_db_url())
    try:
        ensure_local_ct_tables(conn)
        for j in jurs:
            sync_jurisdiction(conn, j, project=args.project, dataset=args.dataset, bucket=args.bucket)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
