"""
nyc/enrich_nhpd.py
──────────────────
Enriches nyc_bbl_stats with:

  1. Rent-stabilization proxy flag — derived from our existing nyc_properties
     (MapPLUTO) data: buildings with 6+ residential units built before 1974
     are flagged is_rent_stabilized = True.  This follows the standard NYC
     housing-research heuristic used by UNHP, RPA, and City Limits.
     NOTE: This is a best-effort proxy; official DHCR registration lists
     (published annually by NYS HCR) would be more precise but are not
     available as a BBL-joinable open dataset.

  2. NHPD subsidy data — National Housing Preservation Database authenticated
     bulk download (CSV for New York state), joined to our BBL table by
     normalised address string.

Run:
    python -m nyc.enrich_nhpd

Required env vars:
    DATABASE_URL      — PostgreSQL connection string
    NHPD_USER         — NHPD account e-mail
    NHPD_PASS         — NHPD account password
"""

import io
import csv
import logging
import os
import time

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
NHPD_USER    = os.environ.get("NHPD_USER", "")
NHPD_PASS    = os.environ.get("NHPD_PASS", "")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _conn():
    return psycopg2.connect(DATABASE_URL)


def _ensure_columns(cur):
    stmts = [
        "ALTER TABLE nyc_bbl_stats ADD COLUMN IF NOT EXISTS is_rent_stabilized BOOLEAN DEFAULT FALSE",
        "ALTER TABLE nyc_bbl_stats ADD COLUMN IF NOT EXISTS rs_units           INTEGER DEFAULT 0",
        "ALTER TABLE nyc_bbl_stats ADD COLUMN IF NOT EXISTS nhpd_subsidy       BOOLEAN DEFAULT FALSE",
        "ALTER TABLE nyc_bbl_stats ADD COLUMN IF NOT EXISTS nhpd_program       TEXT",
        "ALTER TABLE nyc_bbl_stats ADD COLUMN IF NOT EXISTS nhpd_expiration    DATE",
    ]
    for s in stmts:
        try:
            cur.execute(s)
        except Exception as e:
            log.warning(f"Column alter skipped: {e}")


# ---------------------------------------------------------------------------
# Part 1 — Rent-stabilisation proxy from PLUTO
# ---------------------------------------------------------------------------
RS_UNIT_THRESHOLD = 6
RS_YEAR_CUTOFF    = 1974   # standard research cutoff; 421-a/J-51 buildings are excluded (conservative)

def apply_rent_stabilized_proxy():
    """
    Flag BBLs in nyc_bbl_stats as likely rent-stabilised based on PLUTO data:
        units_res >= 6  AND  year_built < 1974
    Also picks up the residential unit count as rs_units.
    """
    log.info("=" * 60)
    log.info("Part 1 — Rent-stabilisation proxy (PLUTO heuristic)")
    log.info(f"  Criteria: units_res >= {RS_UNIT_THRESHOLD} AND year_built < {RS_YEAR_CUTOFF}")
    log.info("=" * 60)

    with _conn() as conn:
        cur = conn.cursor()
        _ensure_columns(cur)

        # Reset first so reruns are idempotent
        cur.execute("""
            UPDATE nyc_bbl_stats
            SET    is_rent_stabilized = FALSE,
                   rs_units           = 0
            WHERE  is_rent_stabilized = TRUE
        """)

        # Join nyc_bbl_stats ← nyc_properties (PLUTO source) by BBL
        cur.execute(f"""
            UPDATE nyc_bbl_stats s
            SET    is_rent_stabilized = TRUE,
                   rs_units           = p.units_res,
                   updated_at         = NOW()
            FROM (
                SELECT DISTINCT ON (bbl)
                       bbl,
                       units_res,
                       year_built
                FROM   nyc_properties
                WHERE  units_res  >= {RS_UNIT_THRESHOLD}
                  AND  year_built IS NOT NULL
                  AND  year_built::integer < {RS_YEAR_CUTOFF}
                ORDER  BY bbl
            ) p
            WHERE  s.bbl = p.bbl
        """)
        flagged = cur.rowcount
        conn.commit()
        log.info(f"  → {flagged:,} BBLs flagged as likely rent-stabilised")


# ---------------------------------------------------------------------------
# Part 2 — NHPD subsidy data
# ---------------------------------------------------------------------------
NHPD_LOGIN_URL    = "https://preservationdatabase.org/wp-login.php"
NHPD_DOWNLOAD_URL = "https://preservationdatabase.org/wp-content/uploads/nhpd/ny_nhpd.csv"

NYC_COUNTY_FIPS = {"005", "047", "061", "081", "085",
                   "36005", "36047", "36061", "36081", "36085"}


def fetch_nhpd_nyc_csv() -> list:
    """Authenticate with NHPD and download the NY state CSV, returning NYC rows."""
    if not NHPD_USER or not NHPD_PASS:
        log.warning("NHPD_USER / NHPD_PASS not set — skipping NHPD enrichment.")
        return []

    log.info("Authenticating with NHPD…")
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; research-bot/1.0)"

    try:
        session.get(NHPD_LOGIN_URL, timeout=30)
        r = session.post(NHPD_LOGIN_URL, data={
            "log": NHPD_USER, "pwd": NHPD_PASS,
            "wp-submit": "Log In", "redirect_to": "/nhpd-downloads/", "testcookie": "1",
        }, timeout=30, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        log.error(f"NHPD login failed: {e}")
        return []

    if "login" in r.url:
        log.error("NHPD login rejected — check credentials.")
        return []

    log.info("NHPD login successful. Downloading NY dataset…")
    try:
        r = session.get(NHPD_DOWNLOAD_URL, timeout=120, stream=True)
        r.raise_for_status()
        content = r.content.decode("utf-8", errors="replace")
    except Exception as e:
        log.error(f"NHPD CSV download failed: {e}")
        return []

    log.info(f"  Downloaded {len(content):,} bytes — parsing NYC rows…")
    records = []
    for row in csv.DictReader(io.StringIO(content)):
        county = str(row.get("county_fips_code") or row.get("county") or "").strip()
        if county not in NYC_COUNTY_FIPS:
            continue
        addr = str(row.get("normalized_address") or row.get("property_address") or "").strip().upper()
        prog = str(row.get("primary_subsidy_program") or row.get("program_name") or "").strip()
        exp  = str(row.get("earliest_expiration_date") or "").strip() or None
        if addr:
            records.append({"address": addr, "program": prog, "expiration": exp})

    log.info(f"  → {len(records):,} NYC NHPD records found")
    return records


def apply_nhpd(records: list):
    if not records:
        log.info("No NHPD records to apply — skipping.")
        return

    with _conn() as conn:
        cur = conn.cursor()
        _ensure_columns(cur)

        nhpd_map = {r["address"]: (r["program"], r["expiration"]) for r in records}

        cur.execute("""
            SELECT bbl, UPPER(TRIM(address)) AS addr
            FROM   nyc_properties
            WHERE  bbl IS NOT NULL
        """)
        rows = cur.fetchall()

        matches = [(bbl, *nhpd_map[addr]) for bbl, addr in rows if addr in nhpd_map]
        log.info(f"  → {len(matches):,} address matches to BBLs")
        if not matches:
            return

        cur.execute("CREATE TEMP TABLE IF NOT EXISTS _nhpd (bbl TEXT, program TEXT, expiration DATE)")
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO _nhpd VALUES %s",
            [(bbl, prog, exp) for bbl, prog, exp in matches],
            page_size=2000,
        )
        cur.execute("""
            UPDATE nyc_bbl_stats s
            SET    nhpd_subsidy    = TRUE,
                   nhpd_program    = t.program,
                   nhpd_expiration = t.expiration,
                   updated_at      = NOW()
            FROM   _nhpd t
            WHERE  s.bbl = t.bbl
        """)
        log.info(f"  → {cur.rowcount:,} BBLs enriched with NHPD subsidy data")
        conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 70)
    log.info("NYC NHPD + Rent-Stabilisation Enrichment")
    log.info("=" * 70)

    try:
        apply_rent_stabilized_proxy()
    except Exception as e:
        log.error(f"Rent-stabilisation step failed: {e}", exc_info=True)

    try:
        records = fetch_nhpd_nyc_csv()
        apply_nhpd(records)
    except Exception as e:
        log.error(f"NHPD step failed: {e}", exc_info=True)

    log.info("=" * 70)
    log.info("Enrichment complete.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
