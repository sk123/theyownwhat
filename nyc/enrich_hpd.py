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


# ---------------------------------------------------------------------------
# Socrata helpers
# ---------------------------------------------------------------------------
def _headers():
    h = {"Accept": "application/json"}
    if SOCRATA_TOKEN:
        h["X-App-Token"] = SOCRATA_TOKEN
    return h


def soql_get(dataset_id: str, params: dict) -> list[dict]:
    url = f"{SOCRATA_BASE}/{dataset_id}.json"
    all_params = {"$limit": PAGE_SIZE, **params}
    r = requests.get(url, params=all_params, headers=_headers(), timeout=90)
    if not r.ok:
        logger.error(f"Socrata error {r.status_code}: {r.text[:300]}")
        r.raise_for_status()
    return r.json()


# (paginated_soql kept for reference but not used — all queries return ≤50k rows)


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
        "$where":  "bbl IS NOT NULL AND class IS NOT NULL",
        "$group":  "bbl, class",
    }
    if limit:
        base_params["$limit"] = limit

    rows = soql_get(VIOLATIONS_DS, base_params)
    logger.info(f"  Violations total rows: {len(rows):,}")

    for r in rows:
        bbl = (r.get("bbl") or "").strip()
        cls = (r.get("class") or "").upper().strip()
        cnt = int(float(r.get("cnt", 0)))
        last = (r.get("last_date") or "")[:10]
        if not bbl:
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
        "$where":  "bbl IS NOT NULL AND class IS NOT NULL AND violationstatus = 'Open'",
        "$group":  "bbl, class",
    }
    if limit:
        open_params["$limit"] = limit

    rows_open = soql_get(VIOLATIONS_DS, open_params)
    logger.info(f"  Violations open rows: {len(rows_open):,}")

    for r in rows_open:
        bbl = (r.get("bbl") or "").strip()
        cls = (r.get("class") or "").upper().strip()
        cnt = int(float(r.get("cnt", 0)))
        if not bbl:
            continue
        stats[bbl]["open"] += cnt
        if cls == "C":
            stats[bbl]["open_c"] += cnt

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
        "$where":  "bbl IS NOT NULL",
        "$group":  "bbl, casestatus, findingofharassment",
    }
    if limit:
        params["$limit"] = limit

    rows = soql_get(LITIGATIONS_DS, params)
    logger.info(f"  Litigations rows: {len(rows):,}")

    for r in rows:
        bbl = (r.get("bbl") or "").strip()
        cnt = int(float(r.get("cnt", 0)))
        status = (r.get("casestatus") or "").upper()
        harassment = (r.get("findingofharassment") or "").upper()
        last = (r.get("last_date") or "")[:10]
        if not bbl:
            continue
        s = stats[bbl]
        s["total"] += cnt
        if "OPEN" in status:
            s["open"] += cnt
        if harassment == "YES":
            s["harassment"] += cnt
        if last and (not s["last_date"] or last > s["last_date"]):
            s["last_date"] = last

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
        "$where":  "bbl IS NOT NULL AND (residential_commercial_ind = 'Residential' OR residential_commercial_ind = 'RESIDENTIAL')",
        "$group":  "bbl",
    }
    if limit:
        params["$limit"] = limit

    rows = soql_get(EVICTIONS_DS, params)
    logger.info(f"  Evictions rows: {len(rows):,}")

    for r in rows:
        bbl = (r.get("bbl") or "").strip()
        cnt = int(float(r.get("cnt", 0)))
        last = (r.get("last_date") or "")[:10]
        if not bbl:
            continue
        stats[bbl]["total"] += cnt
        if last and (not stats[bbl]["last_date"] or last > stats[bbl]["last_date"]):
            stats[bbl]["last_date"] = last

    logger.info(f"  {len(stats):,} unique BBLs with evictions.")
    return dict(stats)


# ---------------------------------------------------------------------------
# Write to nyc_bbl_stats
# ---------------------------------------------------------------------------
def write_stats(conn, violations: dict, litigations: dict, evictions: dict):
    logger.info("Writing nyc_bbl_stats …")

    all_bbls = set(violations) | set(litigations) | set(evictions)
    logger.info(f"  {len(all_bbls):,} total BBLs with any enrichment data.")

    rows = []
    for bbl in all_bbls:
        v = violations.get(bbl, {})
        l = litigations.get(bbl, {})
        e = evictions.get(bbl, {})
        rows.append((
            bbl,
            v.get("total", 0),
            v.get("open", 0),
            v.get("class_c", 0),
            v.get("class_b", 0),
            v.get("class_a", 0),
            v.get("open_c", 0),
            v.get("last_date") or None,
            l.get("total", 0),
            l.get("open", 0),
            l.get("harassment", 0),
            l.get("last_date") or None,
            e.get("total", 0),
            e.get("last_date") or None,
        ))

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
            violations_total    = EXCLUDED.violations_total,
            violations_open     = EXCLUDED.violations_open,
            violations_class_c  = EXCLUDED.violations_class_c,
            violations_class_b  = EXCLUDED.violations_class_b,
            violations_class_a  = EXCLUDED.violations_class_a,
            violations_open_c   = EXCLUDED.violations_open_c,
            last_violation_date = EXCLUDED.last_violation_date,
            litigations_total   = EXCLUDED.litigations_total,
            litigations_open    = EXCLUDED.litigations_open,
            litigations_harassment = EXCLUDED.litigations_harassment,
            last_litigation_date = EXCLUDED.last_litigation_date,
            evictions_total     = EXCLUDED.evictions_total,
            last_eviction_date  = EXCLUDED.last_eviction_date,
            updated_at          = NOW()
    """

    # Inject NOW() as a value placeholder
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"

    batch_size = 5_000
    total = 0
    with conn.cursor() as cur:
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

    violations  = fetch_violations(args.limit)  if (do_all or args.violations_only)  else {}
    litigations = fetch_litigations(args.limit) if (do_all or args.litigations_only) else {}
    evictions   = fetch_evictions(args.limit)   if (do_all or args.evictions_only)   else {}

    write_stats(conn, violations, litigations, evictions)
    print_summary(conn)

    conn.close()
    logger.info("Enrichment complete.")
