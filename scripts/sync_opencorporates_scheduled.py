#!/usr/bin/env python3
"""
scripts/sync_opencorporates_scheduled.py
========================================
Automated OpenCorporates SFTP snapshot monitor, streaming loader orchestrator,
and `data_source_status` completeness registrar for `theyownwhat`:

  1. `--sync-status-only`:
     Reads live `oc_companies`, `oc_officers`, `oc_registered_agent_addresses`,
     and `oc_cross_state_pierced_branches` row counts & `MAX(retrieved_at)` dates
     per `jurisdiction_code`, and upserts `OC_<JUR>` (`OC_US_MA`, `OC_US_NC`,
     `OC_US_AR`, `OC_US_VA`, `OC_US_FL`, etc.) + `OPENCORPORATES_BULK` into
     `data_source_status` so every jurisdiction in `/api/completeness`
     (`FreshnessModal.jsx`) displays its exact corporate registry status.

  2. `--poll-sftp`:
     Connects to `OPENCORPORATES_SFTP_HOST`, inspects top-level `YYYY-MM-DD/`
     snapshot directories, and whenever a newer snapshot than the last ingested
     one in `data_source_status['OPENCORPORATES_BULK']` is staged:
       a) Mirrors & MD5-verifies Wave-1 priority archives (`mirror_opencorporates_sftp.py`)
       b) Streams updated `oc_*` corporate/officer tables (`ingest_opencorporates_bulk.py`)
       c) Rebuilds live ownership networks (`rebuild_all_with_opencorporates.py`)
       d) Refreshes `data_source_status` + `landing_snapshot:v1`!
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import Json, RealDictCursor

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.mirror_opencorporates_sftp import PersistentSFTPSession, load_env_file

SNAPSHOT_DIR_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def _detect_local_snapshot() -> str:
    oc_dir = os.path.join(REPO_ROOT, "data", "opencorporates")
    if os.path.isdir(oc_dir):
        snaps = sorted(
            d for d in os.listdir(oc_dir)
            if os.path.isdir(os.path.join(oc_dir, d)) and SNAPSHOT_DIR_RE.fullmatch(d)
        )
        if snaps:
            return snaps[-1]
    return dt.date.today().isoformat()


def sync_oc_completeness_status(db_url: str, snapshot_label: Optional[str] = None) -> Dict[str, Any]:
    """Record per-registry `OC_US_*` and global `OPENCORPORATES_BULK` rows in `data_source_status`."""
    snap = snapshot_label or _detect_local_snapshot()
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    jurisdiction_code,
                    COUNT(*)::bigint AS companies_count,
                    MAX(retrieved_at)::date AS max_retrieved_date
                FROM oc_companies
                GROUP BY jurisdiction_code
                ORDER BY jurisdiction_code
                """
            )
            co_rows = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT
                    jurisdiction_code,
                    COUNT(*)::bigint AS officers_count
                FROM oc_officers
                GROUP BY jurisdiction_code
                """
            )
            off_by_jur = {r["jurisdiction_code"]: int(r["officers_count"] or 0) for r in cur.fetchall()}

            cur.execute("SELECT COUNT(*)::bigint AS cnt FROM oc_registered_agent_addresses")
            ra_cnt = int(cur.fetchone()["cnt"] or 0)

            cspb_cnt = 0
            cur.execute("SELECT to_regclass('public.oc_cross_state_pierced_branches') IS NOT NULL AS ok")
            if cur.fetchone()["ok"]:
                cur.execute("SELECT COUNT(*)::bigint AS cnt FROM oc_cross_state_pierced_branches")
                cspb_cnt = int(cur.fetchone()["cnt"] or 0)

            total_companies = 0
            total_officers = 0
            latest_ext_date: Optional[dt.date] = None

            for r in co_rows:
                jur = r["jurisdiction_code"]
                c_cnt = int(r["companies_count"] or 0)
                o_cnt = off_by_jur.get(jur, 0)
                ext_dt = r.get("max_retrieved_date")
                total_companies += c_cnt
                total_officers += o_cnt
                if ext_dt and (latest_ext_date is None or ext_dt > latest_ext_date):
                    latest_ext_date = ext_dt

                src_name = f"OC_{jur.upper()}"
                details = {
                    "snapshot": snap,
                    "jurisdiction_code": jur,
                    "companies_count": c_cnt,
                    "officers_count": o_cnt,
                    "license": "ODbL v1.0 (OpenCorporates)",
                    "url": f"https://opencorporates.com/registers/{jur}",
                    "source_url": f"https://opencorporates.com/registers/{jur}",
                }
                cur.execute(
                    """
                    INSERT INTO data_source_status (
                        source_name, source_type, external_last_updated,
                        last_refreshed_at, refresh_status, details
                    )
                    VALUES (%s, 'corporate_registry', %s, NOW(), 'success', %s)
                    ON CONFLICT (source_name) DO UPDATE SET
                        source_type = EXCLUDED.source_type,
                        external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                        last_refreshed_at = NOW(),
                        refresh_status = 'success',
                        details = EXCLUDED.details
                    """,
                    (src_name, ext_dt, Json(details)),
                )

            global_details = {
                "snapshot": snap,
                "registries_loaded": len(co_rows),
                "total_companies": total_companies,
                "total_officers": total_officers,
                "registered_agent_hubs_suppressed": ra_cnt,
                "cross_state_pierced_branches": cspb_cnt,
                "license": "ODbL v1.0 (OpenCorporates)",
            }
            cur.execute(
                """
                INSERT INTO data_source_status (
                    source_name, source_type, external_last_updated,
                    last_refreshed_at, refresh_status, details
                )
                VALUES ('OPENCORPORATES_BULK', 'system', %s, NOW(), 'success', %s)
                ON CONFLICT (source_name) DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                    last_refreshed_at = NOW(),
                    refresh_status = 'success',
                    details = EXCLUDED.details
                """,
                (latest_ext_date, Json(global_details)),
            )
            cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
        conn.commit()
        print(
            f"✓ Recorded OpenCorporates status for {len(co_rows)} registries "
            f"({total_companies:,} companies, {total_officers:,} officers, snapshot={snap})."
        )
        return global_details
    finally:
        conn.close()


def _get_last_ingested_snapshot(db_url: str) -> Optional[str]:
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT details FROM data_source_status WHERE source_name = 'OPENCORPORATES_BULK'")
            row = cur.fetchone()
            if row and isinstance(row.get("details"), dict):
                return row["details"].get("snapshot")
    except Exception:
        pass
    finally:
        conn.close()
    return None


def discover_remote_snapshots(env: Dict[str, str]) -> List[str]:
    host = env.get("OPENCORPORATES_SFTP_HOST") or os.environ.get("OPENCORPORATES_SFTP_HOST")
    user = env.get("OPENCORPORATES_SFTP_USER") or os.environ.get("OPENCORPORATES_SFTP_USER")
    pw = env.get("OPENCORPORATES_SFTP_PASSWORD") or os.environ.get("OPENCORPORATES_SFTP_PASSWORD")
    port = env.get("OPENCORPORATES_SFTP_PORT") or os.environ.get("OPENCORPORATES_SFTP_PORT") or "22"
    if not (host and user and pw):
        print("⏭ SFTP credentials not configured; skipping remote OpenCorporates poll.")
        return []

    sess = PersistentSFTPSession(host=host, port=port, user=user, password=pw)
    try:
        sess.connect()
        out = sess.cmd("ls -1", timeout=30.0)
        found = sorted(set(SNAPSHOT_DIR_RE.findall(out)))
        return found
    finally:
        sess.close()


def poll_and_ingest_if_newer(db_url: str, force_snapshot: Optional[str] = None) -> int:
    env = load_env_file(os.path.join(REPO_ROOT, ".env"))
    last_snap = _get_last_ingested_snapshot(db_url) or _detect_local_snapshot()
    if force_snapshot:
        target_snap = force_snapshot
    else:
        remote_snaps = discover_remote_snapshots(env)
        if not remote_snaps:
            sync_oc_completeness_status(db_url, last_snap)
            return 0
        latest_remote = remote_snaps[-1]
        if latest_remote <= last_snap:
            print(f"✓ OpenCorporates SFTP is already current (latest remote={latest_remote}, loaded={last_snap}).")
            sync_oc_completeness_status(db_url, last_snap)
            return 0
        target_snap = latest_remote

    print(f"▶ New OpenCorporates snapshot detected: {target_snap} (replacing {last_snap})!")
    child_env = os.environ.copy()
    child_env["DATABASE_URL"] = db_url
    child_env["THEYOWNWHAT_SOURCE_ONLY"] = "true"

    steps = [
        ("Mirror & MD5-verify Wave-1 SFTP snapshot", [sys.executable, "scripts/mirror_opencorporates_sftp.py", "--snapshot", target_snap, "--only-wave1"]),
        ("Stream-ingest OpenCorporates bulk archives", [sys.executable, "scripts/ingest_opencorporates_bulk.py", "--snapshot", target_snap, "--bulk-reset"]),
        ("Rebuild live ownership networks across jurisdictions", [sys.executable, "scripts/rebuild_all_with_opencorporates.py"]),
    ]
    for label, cmd in steps:
        print(f"▶ {label}: {' '.join(cmd)}", flush=True)
        res = subprocess.run(cmd, cwd=REPO_ROOT, env=child_env, check=False)
        if res.returncode != 0:
            raise RuntimeError(f"{label} failed with exit code {res.returncode}")

    sync_oc_completeness_status(db_url, target_snap)
    subprocess.run([sys.executable, "-m", "api.landing_cache"], cwd=REPO_ROOT, env=child_env, check=False)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync OpenCorporates SFTP snapshots & Data Completeness status.")
    parser.add_argument("--sync-status-only", action="store_true", help="Sync `OC_US_*` & `OPENCORPORATES_BULK` into `data_source_status` without SFTP download.")
    parser.add_argument("--poll-sftp", action="store_true", help="Poll OpenCorporates SFTP for any newer `YYYY-MM-DD` snapshot and auto-ingest if found.")
    parser.add_argument("--force-snapshot", help="Explicit `YYYY-MM-DD` snapshot to mirror, ingest, and rebuild.")
    args = parser.parse_args()

    db_url = _resolve_db_url()
    if args.sync_status_only or not (args.poll_sftp or args.force_snapshot):
        sync_oc_completeness_status(db_url)
        return 0
    return poll_and_ingest_if_newer(db_url, force_snapshot=args.force_snapshot)


if __name__ == "__main__":
    raise SystemExit(main())
