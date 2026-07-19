#!/usr/bin/env python3
"""
nyc/enrich_hpd.py
=================
Enriches nyc_bbl_stats with:
  1. HPD Housing Maintenance Code Violations  (wvxf-dwi5)
  2. HPD Housing Court Litigations            (59kj-x8nc)
  3. DOI Marshal-Executed Evictions           (6z8x-wfk4)

All three datasets are queried via Socrata SoQL GROUP BY — we fetch
pre-aggregated counts per BBL rather than raw rows, keeping data transfer
to a minimum (~1-2 MB total instead of hundreds of MB).

Usage:
    DATABASE_URL=... python -m nyc.enrich_hpd
    DATABASE_URL=... python -m nyc.enrich_hpd --violations-only
    DATABASE_URL=... python -m nyc.enrich_hpd --limit 10000   # dev/test
"""

import os
import sys
import argparse
import logging
import time
from collections import defaultdict
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("nyc-enrich")

DATABASE_URL     = os.environ.get("DATABASE_URL")
SOCRATA_TOKEN    = os.environ.get("NYS_SOCRATA_APP_TOKEN", "")
SOCRATA_BASE     = "https://data.cityofnewyork.us/resource"

VIOLATIONS_DS    = "wvxf-dwi5"   # HPD Housing Maintenance Code Violations
LITIGATIONS_DS   = "59kj-x8nc"   # HPD Housing Litigations
EVICTIONS_DS     = "6z8x-wfk4"   # DOI Marshal Evictions

PAGE_SIZE        = 50_000         # Socrata max per request
SOCRATA_TIMEOUT  = (10, 180)      # connect timeout, read timeout
SOCRATA_RETRIES  = 5
SOCRATA_RETRY_STATUSES = {429, 500, 502, 503, 504}


# ---------------------------------------------------------------------------
# Socrata helpers
# ---------------------------------------------------------------------------
def _headers():
    h = {"Accept": "application/json"}
    if SOCRATA_TOKEN:
        h["X-App-Token"] = SOCRATA_TOKEN
    return h


def is_valid_nyc_bbl(bbl: str) -> bool:
    return len(bbl) == 10 and bbl.isdigit() and bbl[0] in "12345"


def _retry_delay(attempt: int, response: Optional[requests.Response] = None) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), 60.0)
            except ValueError:
                pass
    return min(2 ** attempt, 30.0)


def soql_get(dataset_id: str, params: dict) -> list[dict]:
    url = f"{SOCRATA_BASE}/{dataset_id}.json"
    last_error: Optional[Exception] = None
    for attempt in range(1, SOCRATA_RETRIES + 1):
        response: Optional[requests.Response] = None
        try:
            response = requests.get(url, params=params, headers=_headers(), timeout=SOCRATA_TIMEOUT)
            if response.ok:
                return response.json()

            should_retry = response.status_code in SOCRATA_RETRY_STATUSES
            if not should_retry or attempt == SOCRATA_RETRIES:
                logger.error(f"Socrata error {response.status_code}: {response.text[:300]}")
                response.raise_for_status()

            delay = _retry_delay(attempt, response)
            logger.warning(
                f"Socrata {dataset_id} returned {response.status_code}; "
                f"retrying in {delay:.1f}s ({attempt}/{SOCRATA_RETRIES})"
            )
            time.sleep(delay)
        except (requests.RequestException, ValueError) as e:
            last_error = e
            if attempt == SOCRATA_RETRIES:
                raise
            delay = _retry_delay(attempt, response)
            logger.warning(
                f"Socrata {dataset_id} request failed: {e}; "
                f"retrying in {delay:.1f}s ({attempt}/{SOCRATA_RETRIES})"
            )
            time.sleep(delay)

    raise RuntimeError(f"Socrata request failed after retries: {last_error}")


def soql_get_all(dataset_id: str, params: dict, order_by: str, limit: int = 0) -> list[dict]:
    """Fetch every grouped Socrata result page with a stable ordering."""
    rows: list[dict] = []
    offset = 0
    while True:
        page_limit = min(PAGE_SIZE, limit - len(rows)) if limit else PAGE_SIZE
        if page_limit <= 0:
            break
        page_params = {
            "$limit": page_limit,
            "$offset": offset,
            "$order": order_by,
            **params,
        }
        page = soql_get(dataset_id, page_params)
        rows.extend(page)
        if len(page) < page_limit:
            break
        offset += len(page)
        logger.info(f"  ... {len(rows):,} grouped rows fetched")
        time.sleep(0.2)
    return rows


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set.")
    return psycopg2.connect(DATABASE_URL)


def apply_schema(conn):
    """Ensure nyc_bbl_stats table exists."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("Schema applied.")


def update_status(conn, status: str, details: Optional[dict] = None):
    try:
        next_details = dict(details or {})
        with conn.cursor() as cur:
            cur.execute("""
                SELECT refresh_status, last_refreshed_at, details
                FROM data_source_status
                WHERE source_name = 'NYC_HPD_ENRICHMENT'
                LIMIT 1
            """)
            previous = cur.fetchone()
            if previous and status != "success":
                previous_status, previous_refreshed, previous_details = previous
                if isinstance(previous_details, str):
                    try:
                        import json
                        previous_details = json.loads(previous_details)
                    except Exception:
                        previous_details = {}
                previous_details = previous_details if isinstance(previous_details, dict) else {}

                if previous_status == "success" and previous_refreshed:
                    next_details.setdefault("last_success_at", previous_refreshed.isoformat())
                    next_details.setdefault("last_success_details", previous_details)
                else:
                    for key in ("last_success_at", "last_success_details"):
                        if key in previous_details:
                            next_details.setdefault(key, previous_details[key])

            cur.execute(
                """
                INSERT INTO data_source_status
                    (source_name, source_type, last_refreshed_at, refresh_status, details)
                VALUES ('NYC_HPD_ENRICHMENT', 'nyc', NOW(), %s, %s::jsonb)
                ON CONFLICT (source_name)
                DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    last_refreshed_at = EXCLUDED.last_refreshed_at,
                    refresh_status = EXCLUDED.refresh_status,
                    details = EXCLUDED.details;
                """,
                (status, json_dumps(next_details)),
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Could not update NYC enrichment status: {e}")


def json_dumps(value: dict) -> str:
    import json
    return json.dumps(value)


def collect_status_details(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (
                    WHERE bbl ~ '^[1-5][0-9]{9}$'
                      AND (
                          COALESCE(violations_total, 0) > 0
                       OR COALESCE(violations_open, 0) > 0
                       OR COALESCE(violations_open_c, 0) > 0
                      )
                ) AS bbls_with_records,
                SUM(COALESCE(violations_total, 0)) FILTER (WHERE bbl ~ '^[1-5][0-9]{9}$') AS total_violations,
                SUM(COALESCE(violations_open, 0)) FILTER (WHERE bbl ~ '^[1-5][0-9]{9}$') AS open_violations,
                SUM(COALESCE(violations_open_c, 0)) FILTER (WHERE bbl ~ '^[1-5][0-9]{9}$') AS open_violations_c,
                SUM(COALESCE(litigations_total, 0)) FILTER (WHERE bbl ~ '^[1-5][0-9]{9}$') AS total_litigations,
                SUM(COALESCE(evictions_total, 0)) FILTER (WHERE bbl ~ '^[1-5][0-9]{9}$') AS evictions_total,
                MAX(last_violation_date) FILTER (WHERE bbl ~ '^[1-5][0-9]{9}$') AS last_violation_date,
                COUNT(*) FILTER (WHERE bbl !~ '^[1-5][0-9]{9}$') AS invalid_bbl_rows
            FROM nyc_bbl_stats
        """)
        row = cur.fetchone()

    keys = (
        "bbls_with_records",
        "total_violations",
        "open_violations",
        "open_violations_c",
        "total_litigations",
        "evictions_total",
        "last_violation_date",
        "invalid_bbl_rows",
    )
    details = dict(zip(keys, row or []))
    return {
        key: (value.isoformat() if hasattr(value, "isoformat") else int(value or 0))
        for key, value in details.items()
    }


# ---------------------------------------------------------------------------
# 1. HPD Violations — aggregated per BBL
# ---------------------------------------------------------------------------
def fetch_violations(limit: int = 0) -> dict[str, dict]:
    """
    Returns {bbl: {total, open, class_c, class_b, class_a, open_c, last_date}}
    Uses server-side GROUP BY via Socrata params.
    """
    logger.info("Fetching HPD Violations (aggregated by BBL) …")
    stats: dict[str, dict] = defaultdict(lambda: {
        "total": 0, "open": 0,
        "class_c": 0, "class_b": 0, "class_a": 0,
        "open_c": 0, "last_date": None,
    })

    base_params = {
        "$select": "bbl, class, COUNT(*) AS cnt, MAX(inspectiondate) AS last_date",
        "$where":  "bbl IS NOT NULL AND bbl != '0' AND class IS NOT NULL",
        "$group":  "bbl, class",
    }
    rows = soql_get_all(VIOLATIONS_DS, base_params, "bbl, class", limit)
    logger.info(f"  Violations total rows: {len(rows):,}")

    skipped_invalid = 0
    for r in rows:
        bbl = (r.get("bbl") or "").strip()
        cls = (r.get("class") or "").upper().strip()
        cnt = int(float(r.get("cnt", 0)))
        last = (r.get("last_date") or "")[:10]
        if not is_valid_nyc_bbl(bbl):
            skipped_invalid += cnt
            continue
        s = stats[bbl]
        s["total"] += cnt
        if cls == "C":
            s["class_c"] += cnt
        elif cls == "B":
            s["class_b"] += cnt
        elif cls == "A":
            s["class_a"] += cnt
        if last and (not s["last_date"] or last > s["last_date"]):
            s["last_date"] = last

    # Open counts
    open_params = {
        "$select": "bbl, class, COUNT(*) AS cnt",
        "$where":  "bbl IS NOT NULL AND bbl != '0' AND class IS NOT NULL AND violationstatus = 'Open'",
        "$group":  "bbl, class",
    }
    rows_open = soql_get_all(VIOLATIONS_DS, open_params, "bbl, class", limit)
    logger.info(f"  Violations open rows: {len(rows_open):,}")

    skipped_invalid_open = 0
    for r in rows_open:
        bbl = (r.get("bbl") or "").strip()
        cls = (r.get("class") or "").upper().strip()
        cnt = int(float(r.get("cnt", 0)))
        if not is_valid_nyc_bbl(bbl):
            skipped_invalid_open += cnt
            continue
        stats[bbl]["open"] += cnt
        if cls == "C":
            stats[bbl]["open_c"] += cnt

    if skipped_invalid or skipped_invalid_open:
        logger.warning(
            f"  Skipped invalid HPD violation BBL rows: "
            f"{skipped_invalid:,} total / {skipped_invalid_open:,} open"
        )
    logger.info(f"  {len(stats):,} unique BBLs with violations.")
    return dict(stats)


# ---------------------------------------------------------------------------
# 2. HPD Litigations — aggregated per BBL
# ---------------------------------------------------------------------------
def fetch_litigations(limit: int = 0) -> dict[str, dict]:
    """Returns {bbl: {total, open, harassment, last_date}}"""
    logger.info("Fetching HPD Litigations (aggregated by BBL) …")
    stats: dict[str, dict] = defaultdict(lambda: {
        "total": 0, "open": 0, "harassment": 0, "last_date": None
    })

    params = {
        "$select": "bbl, casestatus, findingofharassment, COUNT(*) AS cnt, MAX(caseopendate) AS last_date",
        "$where":  "bbl IS NOT NULL AND bbl != '0'",
        "$group":  "bbl, casestatus, findingofharassment",
    }
    rows = soql_get_all(LITIGATIONS_DS, params, "bbl, casestatus, findingofharassment", limit)
    logger.info(f"  Litigations rows: {len(rows):,}")

    skipped_invalid = 0
    for r in rows:
        bbl = (r.get("bbl") or "").strip()
        cnt = int(float(r.get("cnt", 0)))
        status = (r.get("casestatus") or "").upper()
        harassment = (r.get("findingofharassment") or "").upper()
        last = (r.get("last_date") or "")[:10]
        if not is_valid_nyc_bbl(bbl):
            skipped_invalid += cnt
            continue
        s = stats[bbl]
        s["total"] += cnt
        if "OPEN" in status:
            s["open"] += cnt
        if harassment == "YES":
            s["harassment"] += cnt
        if last and (not s["last_date"] or last > s["last_date"]):
            s["last_date"] = last

    if skipped_invalid:
        logger.warning(f"  Skipped invalid litigation BBL rows: {skipped_invalid:,}")
    logger.info(f"  {len(stats):,} unique BBLs with litigations.")
    return dict(stats)


# ---------------------------------------------------------------------------
# 3. DOI Marshal Evictions — aggregated per BBL
# ---------------------------------------------------------------------------
def fetch_evictions(limit: int = 0) -> dict[str, dict]:
    """Returns {bbl: {total, last_date}}"""
    logger.info("Fetching DOI Evictions (aggregated by BBL) …")
    stats: dict[str, dict] = defaultdict(lambda: {"total": 0, "last_date": None})

    params = {
        "$select": "bbl, COUNT(*) AS cnt, MAX(executed_date) AS last_date",
        "$where":  "bbl IS NOT NULL AND bbl != '0' AND (residential_commercial_ind = 'Residential' OR residential_commercial_ind = 'RESIDENTIAL')",
        "$group":  "bbl",
    }
    rows = soql_get_all(EVICTIONS_DS, params, "bbl", limit)
    logger.info(f"  Evictions rows: {len(rows):,}")

    skipped_invalid = 0
    for r in rows:
        bbl = (r.get("bbl") or "").strip()
        cnt = int(float(r.get("cnt", 0)))
        last = (r.get("last_date") or "")[:10]
        if not is_valid_nyc_bbl(bbl):
            skipped_invalid += cnt
            continue
        stats[bbl]["total"] += cnt
        if last and (not stats[bbl]["last_date"] or last > stats[bbl]["last_date"]):
            stats[bbl]["last_date"] = last

    if skipped_invalid:
        logger.warning(f"  Skipped invalid eviction BBL rows: {skipped_invalid:,}")
    logger.info(f"  {len(stats):,} unique BBLs with evictions.")
    return dict(stats)


# ---------------------------------------------------------------------------
# Write to nyc_bbl_stats
# ---------------------------------------------------------------------------
def write_stats(conn, violations: Optional[dict], litigations: Optional[dict], evictions: Optional[dict]):
    logger.info("Writing nyc_bbl_stats …")

    all_bbls = set(violations or {}) | set(litigations or {}) | set(evictions or {})
    logger.info(f"  {len(all_bbls):,} total BBLs with any enrichment data.")
    full_refresh = violations is not None and litigations is not None and evictions is not None

    rows = []
    for bbl in all_bbls:
        if not is_valid_nyc_bbl(bbl):
            continue
        v = (violations or {}).get(bbl, {}) if violations is not None else None
        l = (litigations or {}).get(bbl, {}) if litigations is not None else None
        e = (evictions or {}).get(bbl, {}) if evictions is not None else None
        rows.append((
            bbl,
            v.get("total", 0) if v is not None else None,
            v.get("open", 0) if v is not None else None,
            v.get("class_c", 0) if v is not None else None,
            v.get("class_b", 0) if v is not None else None,
            v.get("class_a", 0) if v is not None else None,
            v.get("open_c", 0) if v is not None else None,
            (v.get("last_date") or None) if v is not None else None,
            l.get("total", 0) if l is not None else None,
            l.get("open", 0) if l is not None else None,
            l.get("harassment", 0) if l is not None else None,
            (l.get("last_date") or None) if l is not None else None,
            e.get("total", 0) if e is not None else None,
            (e.get("last_date") or None) if e is not None else None,
        ))
    logger.info(f"  {len(rows):,} valid NYC BBLs ready to write.")
    if full_refresh and not rows:
        raise RuntimeError("Refusing to prune nyc_bbl_stats: full refresh produced zero valid BBL rows.")

    upsert_sql = """
        INSERT INTO nyc_bbl_stats (
            bbl,
            violations_total, violations_open,
            violations_class_c, violations_class_b, violations_class_a,
            violations_open_c, last_violation_date,
            litigations_total, litigations_open, litigations_harassment,
            last_litigation_date,
            evictions_total, last_eviction_date,
            updated_at
        ) VALUES %s
        ON CONFLICT (bbl) DO UPDATE SET
            violations_total    = COALESCE(EXCLUDED.violations_total, nyc_bbl_stats.violations_total),
            violations_open     = COALESCE(EXCLUDED.violations_open, nyc_bbl_stats.violations_open),
            violations_class_c  = COALESCE(EXCLUDED.violations_class_c, nyc_bbl_stats.violations_class_c),
            violations_class_b  = COALESCE(EXCLUDED.violations_class_b, nyc_bbl_stats.violations_class_b),
            violations_class_a  = COALESCE(EXCLUDED.violations_class_a, nyc_bbl_stats.violations_class_a),
            violations_open_c   = COALESCE(EXCLUDED.violations_open_c, nyc_bbl_stats.violations_open_c),
            last_violation_date = COALESCE(EXCLUDED.last_violation_date, nyc_bbl_stats.last_violation_date),
            litigations_total   = COALESCE(EXCLUDED.litigations_total, nyc_bbl_stats.litigations_total),
            litigations_open    = COALESCE(EXCLUDED.litigations_open, nyc_bbl_stats.litigations_open),
            litigations_harassment = COALESCE(EXCLUDED.litigations_harassment, nyc_bbl_stats.litigations_harassment),
            last_litigation_date = COALESCE(EXCLUDED.last_litigation_date, nyc_bbl_stats.last_litigation_date),
            evictions_total     = COALESCE(EXCLUDED.evictions_total, nyc_bbl_stats.evictions_total),
            last_eviction_date  = COALESCE(EXCLUDED.last_eviction_date, nyc_bbl_stats.last_eviction_date),
            updated_at          = NOW()
    """

    # Inject NOW() as a value placeholder
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"

    batch_size = 5_000
    total = 0
    with conn.cursor() as cur:
        cur.execute("DELETE FROM nyc_bbl_stats WHERE bbl !~ '^[1-5][0-9]{9}$'")
        if cur.rowcount:
            logger.warning(f"  Removed {cur.rowcount:,} invalid BBL row(s) from nyc_bbl_stats.")

        if full_refresh:
            cur.execute("CREATE TEMP TABLE _nyc_current_bbls (bbl TEXT PRIMARY KEY) ON COMMIT DROP")
            execute_values(
                cur,
                "INSERT INTO _nyc_current_bbls (bbl) VALUES %s ON CONFLICT DO NOTHING",
                [(row[0],) for row in rows],
                template="(%s)",
                page_size=10_000,
            )
            cur.execute("""
                DELETE FROM nyc_bbl_stats s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM _nyc_current_bbls current
                    WHERE current.bbl = s.bbl
                )
            """)
            if cur.rowcount:
                logger.info(f"  Removed {cur.rowcount:,} stale BBL stat row(s) not present in current sources.")
        conn.commit()

        for i in range(0, len(rows), batch_size):
            batch = rows[i: i + batch_size]
            execute_values(cur, upsert_sql, batch, template=template)
            conn.commit()
            total += len(batch)
            logger.info(f"  Wrote {total:,}/{len(rows):,} BBL stats …")

    logger.info(f"  Done — {total:,} BBLs enriched.")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
def print_summary(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*)                                        AS bbls,
                SUM(violations_total)                           AS violations,
                SUM(violations_open)                            AS open_violations,
                SUM(violations_open_c)                          AS open_c,
                SUM(litigations_total)                          AS litigations,
                SUM(litigations_harassment)                     AS harassment,
                SUM(evictions_total)                            AS evictions
            FROM nyc_bbl_stats
        """)
        row = cur.fetchone()
        bbls, vt, vo, vc, lt, lh, et = row
        logger.info("=== Enrichment Summary ===")
        logger.info(f"  BBLs enriched:             {int(bbls or 0):>10,}")
        logger.info(f"  Total violations:           {int(vt  or 0):>10,}")
        logger.info(f"  Open violations:            {int(vo  or 0):>10,}")
        logger.info(f"  Open Class-C violations:    {int(vc  or 0):>10,}")
        logger.info(f"  Total litigations:          {int(lt  or 0):>10,}")
        logger.info(f"  Harassment findings:        {int(lh  or 0):>10,}")
        logger.info(f"  Executed evictions (2017+): {int(et  or 0):>10,}")

        # Top 10 worst BBLs by open Class-C violations
        cur.execute("""
            SELECT s.bbl, p.address, p.borough,
                   s.violations_open_c, s.violations_open, s.evictions_total
            FROM nyc_bbl_stats s
            LEFT JOIN nyc_properties p ON p.bbl = s.bbl
            WHERE s.violations_open_c > 0
            ORDER BY s.violations_open_c DESC
            LIMIT 10
        """)
        logger.info("Top 10 BBLs by open Class-C violations:")
        for r in cur.fetchall():
            logger.info(
                f"  {r[0]:<12} {(r[1] or '')[:40]:<42} {(r[2] or ''):<15} "
                f"OpenC={r[3]:>4}  OpenAll={r[4]:>4}  Evictions={r[5]:>3}"
            )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich nyc_bbl_stats from Socrata.")
    parser.add_argument("--violations-only", action="store_true")
    parser.add_argument("--litigations-only", action="store_true")
    parser.add_argument("--evictions-only", action="store_true")
    parser.add_argument("--limit", type=int, default=0,
                        help="Row limit for dev/test (0=all).")
    args = parser.parse_args()

    conn = get_conn()
    apply_schema(conn)

    do_all = not (args.violations_only or args.litigations_only or args.evictions_only)

    try:
        update_status(conn, "running", {"message": "Refreshing HPD violations, litigations, and evictions"})
        violations  = fetch_violations(args.limit)  if (do_all or args.violations_only)  else None
        litigations = fetch_litigations(args.limit) if (do_all or args.litigations_only) else None
        evictions   = fetch_evictions(args.limit)   if (do_all or args.evictions_only)   else None

        write_stats(conn, violations, litigations, evictions)
        print_summary(conn)
        status_details = collect_status_details(conn)
        status_details["message"] = "HPD enrichment refreshed"
        update_status(conn, "success", status_details)
    except Exception as e:
        update_status(conn, "failure", {"message": str(e)[:500]})
        raise

    conn.close()
    logger.info("Enrichment complete.")
